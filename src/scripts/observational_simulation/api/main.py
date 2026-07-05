import os, time, sqlite3, uvicorn
from sqlalchemy import create_engine, Column, Integer, String, Float
from sqlalchemy.orm import sessionmaker, declarative_base
from fastapi import FastAPI, HTTPException, Depends
from prometheus_fastapi_instrumentator import Instrumentator

# --- OpenTelemetry Observability Setup ---
# 設定 OTel exporter 將 telemetry 發送到本地的 OpenTelemetry Collector
# ( 假設您在 k8s 中部署了 otel-collector，或者在本地運行 jaeger/tempo )
from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor

# 初始化 Tracer Provider
trace.set_tracer_provider(TracerProvider())
tracer = trace.get_tracer(__name__)

# 設定 OTLP Exporter (指向 Tempo 或 OTEL Collector)
# 在 K8s 中，這通常是 otel-collector.observability.svc.cluster.local:4317
otlp_endpoint = os.getenv("OTEL_EXPORTER_OTLP_ENDPOINT", "http://localhost:4317")
span_exporter = OTLPSpanExporter(endpoint=otlp_endpoint, insecure=True)
span_processor = BatchSpanProcessor(span_exporter)
trace.get_tracer_provider().add_span_processor(span_processor)
# -------------------------------------------------------

# --- SQLite Setup ---
DB_PATH = os.getenv("DB_PATH", "./data/orders.db")
# 確保數據目錄存在
os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
DATABASE_URL = f"sqlite:///{DB_PATH}"

Base = declarative_base()


# 定義 Order Model
class Order(Base):
    __tablename__ = "orders"
    id = Column(Integer, primary_key=True, index=True)
    item_name = Column(String, index=True)
    amount = Column(Float)
    customer_id = Column(Integer)


# 創建資料庫表
engine = create_engine(DATABASE_URL, connect_args={"timeout": 15})  # 增加連接超時
Base.metadata.create_all(bind=engine)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

# --- FastAPI App Setup ---
app = FastAPI(title="Order Service with Fault Injection")

# **[關鍵埋點]** 自動收集 Prometheus Metrics
Instrumentator().instrument(app).expose(app)

# **[關鍵埋點]** 自動收集 Distributed Traces
FastAPIInstrumentor.instrument_app(app)


# Dependencyinjection session
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


# 全局變量用於故障注入
FAULT_INJECTED = False
FAULT_DURATION = 0  # seconds


@app.get("/")
def read_root():
    return {"Service": "Order Service", "status": "running"}


@app.post("/orders/", response_model=dict)
@tracer.start_as_current_span("create_order")  # 手動創建一個 span
def create_order(
    item_name: str, amount: float, customer_id: int, db: SessionLocal = Depends(get_db)
):
    # **[故障注入點]** 模擬 I/O Hysteresis
    if FAULT_INJECTED:
        print(f"⚠️ Fault Injected! Sleeping for {FAULT_DURATION}s")
        time.sleep(FAULT_DURATION)

    # 手動創建一個針對 DB 操作的 span (Child Span)
    with tracer.start_as_current_span("sqlite_insert"):
        try:
            new_order = Order(
                item_name=item_name, amount=amount, customer_id=customer_id
            )
            db.add(new_order)
            db.commit()
            db.refresh(new_order)
            return {"status": "success", "order_id": new_order.id}
        except Exception as e:
            # 在 Trace 中記錄錯誤
            current_span = trace.get_current_span()
            current_span.record_exception(e)
            raise HTTPException(status_code=500, detail=str(e))


@app.get("/orders/{customer_id}")
def get_orders(customer_id: int, db: SessionLocal = Depends(get_db)):
    # **[故障注入點]** 同樣應用延遲
    if FAULT_INJECTED:
        time.sleep(FAULT_DURATION)

    with tracer.start_as_current_span("sqlite_select"):
        orders = db.query(Order).filter(Order.customer_id == customer_id).all()
    return orders


# --- Chaos/Admin API ---
@app.post("/admin/inject-fault")
def inject_fault(duration_seconds: int = 5):
    """
    API Endpoint 用於手動開啟故障注入
    在實戰中，您會用 Chaos Mesh 代替這個 API
    """
    global FAULT_INJECTED, FAULT_DURATION
    FAULT_INJECTED = True
    FAULT_DURATION = duration_seconds
    print(f"‼️ ADMIN: Fault injected for {duration_seconds}s")
    return {"status": "fault_injected", "duration": f"{duration_seconds}s"}


@app.post("/admin/clear-fault")
def clear_fault():
    """
    API Endpoint 用於手動清除故障
    """
    global FAULT_INJECTED
    FAULT_INJECTED = False
    print("✅ ADMIN: Fault cleared")
    return {"status": "fault_cleared"}


if __name__ == "__main__":
    # 啟動服務，監聽 8000 埠
    uvicorn.run(app, host="0.0.0.0", port=8000)

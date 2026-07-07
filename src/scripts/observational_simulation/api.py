import os, time, logging, asyncio, uvicorn
from fastapi import FastAPI, HTTPException, Depends
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from pythonjsonlogger import jsonlogger

# 觀測性套件
from prometheus_fastapi_instrumentator import Instrumentator
from opentelemetry import trace
from opentelemetry.trace import Status, StatusCode
from opentelemetry.instrumentation.logging import LoggingInstrumentor
from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor
from opentelemetry.instrumentation.sqlite3 import SQLite3Instrumentor
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter

# 業務邏輯模組
from table.order import Base, Order
from utils.normal import init_database
from utils.constant import DB_DIR, DB_PATH, DATABASE_URL


# TODO [1] Logging：採用 JSON 格式，便於 Loki 解析
logger = logging.getLogger(__name__)
logHandler = logging.StreamHandler()
formatter = jsonlogger.JsonFormatter(
    "%(asctime)s %(levelname)s %(name)s %(trace_id)s %(span_id)s %(message)s"
)
logHandler.setFormatter(formatter)
logger.addHandler(logHandler)
logger.setLevel(logging.INFO)
LoggingInstrumentor().instrument(set_logging_format=True)

# TODO [2] Tracer & Exporter 初始化
trace.set_tracer_provider(TracerProvider())
tracer = trace.get_tracer(__name__)
otlp_endpoint = os.getenv("OTEL_EXPORTER_OTLP_ENDPOINT", "http://localhost:4317")
span_processor = BatchSpanProcessor(
    OTLPSpanExporter(endpoint=otlp_endpoint, insecure=True)
)
trace.get_tracer_provider().add_span_processor(span_processor)

# TODO [3] DB: SQLite 設定
init_database(DB_DIR, DATABASE_URL)
engine = create_engine(DATABASE_URL, connect_args={"timeout": 15})
Base.metadata.create_all(bind=engine)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

# TODO [4] FastAPI 初始化
app = FastAPI(title="Order Service with Fault Injection")
Instrumentator().instrument(app).expose(app)  # 自動收集 Prometheus Metrics
FastAPIInstrumentor.instrument_app(app)  # 自動收集 Distributed Traces
SQLite3Instrumentor().instrument()  # 自動埋點 不需要每一行 DB 操作手動加 with tracer.start_as_current_span


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


# 故障注入控制器 (全域變數)
fault = {"injected": False, "duration": 0}


@app.get("/health")
async def health_check():
    return {"status": "ok"}


@app.post("/orders/")
async def create_order(
    item_name: str, amount: float, customer_id: int, db=Depends(get_db)
):
    # 注入故障 (非阻塞)
    if fault["injected"]:
        logger.warning(
            "⚠️ Fault Injected: Delaying I/O", extra={"delay": fault["duration"]}
        )
        await asyncio.sleep(fault["duration"])

    # with tracer.start_as_current_span("sqlite_insert") as span:
    try:
        # span.set_attribute("db.statement", "INSERT INTO orders ...")
        new_order = Order(item_name=item_name, amount=amount, customer_id=customer_id)
        db.add(new_order)
        db.commit()
        return {"status": "success", "order_id": new_order.id}
    except Exception as e:
        trace.get_current_span().set_status(Status(StatusCode.ERROR))
        logger.error("Database operation failed", extra={"error": str(e)})
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/admin/inject-fault")
async def inject_fault(duration_seconds: int = 5):
    fault["injected"] = True
    fault["duration"] = duration_seconds
    return {"status": "injected"}


@app.get("/orders/{customer_id}")
async def get_orders(customer_id: int, db=Depends(get_db)):
    if fault["injected"]:
        await asyncio.sleep(fault["duration"])
    with tracer.start_as_current_span("sqlite_select"):
        return db.query(Order).filter(Order.customer_id == customer_id).all()


if __name__ == "__main__":
    uvicorn.run(
        "src.scripts.observational_simulation.api:app",
        host="0.0.0.0",
        port=8000,
        reload=True,
    )

import os, time, logging, asyncio, uvicorn, logging_loki
from rich.console import Console
from rich.logging import RichHandler
from fastapi import FastAPI, HTTPException, Depends, Request
from starlette.middleware.base import BaseHTTPMiddleware
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
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import ConsoleSpanExporter, BatchSpanProcessor
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter

# 業務邏輯模組
from table.order import Base, Order
from utils.normal import init_database, TraceIdAliasFilter
from utils.constant import DB_DIR, DB_PATH, DATABASE_URL
from logging_test import ConsoleDataFormatter

# TODO [1] Tracer & Exporter 初始化
resource = Resource(attributes={"service.name": "order-service"})
provider = TracerProvider(resource=resource)
trace.set_tracer_provider(provider)
tracer = trace.get_tracer(__name__)

otlp_endpoint = os.getenv("OTLP_EXPORTER_OTLP_ENDPOINT", "http://127.0.0.1:4317")

# OTLP Exporter (發往 Tempo/Collector)
otlp_processor = BatchSpanProcessor(
    OTLPSpanExporter(endpoint=otlp_endpoint, insecure=True)
)
provider.add_span_processor(otlp_processor)
# provider.add_span_processor(BatchSpanProcessor(ConsoleSpanExporter())) # 每次 Span 產生時，終端機會吐出一堆追蹤數據

# TODO LoggingInstrumentor: 讀取 TracerProvider 並正確注入 TraceID
LoggingInstrumentor().instrument(set_logging_format=True)

# TODO 徹底靜音 Uvicorn 的預設 Log
uvicorn_access = logging.getLogger("uvicorn.access")
uvicorn_access.handlers = []  # 清空處理器
uvicorn_access.propagate = False  # 禁止傳遞到 Root Logger
uvicorn_error = logging.getLogger("uvicorn.error")
uvicorn_error.handlers = []  # 清空處理器
uvicorn_error.propagate = False  # 禁止傳遞到 Root Logger


# TODO [2] Logging：採用 JSON 格式，便於 Loki 解析
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
logger.handlers = []  # TODO 移除 Uvicorn 預設 handler
logger.propagate = False  # TODO 不 log 冒泡到 root logger，避免重複輸出
if logger.hasHandlers():  # 避免重複觸發 logger
    logger.handlers.clear()


console = Console(
    force_terminal=True,  # 強制開啟終端機模式 (即使不是互動式環境)
    # force_interactive=True,   # 強制開啟互動模式
    color_system="truecolor",  # 強制開啟 24-bit 顏色支援
    width=300,  # 設定寬度，避免太早換行
)
rich_handler = RichHandler(
    console=console,
    show_time=True,
    show_path=False,
    show_level=True,
    rich_tracebacks=True,
    markup=True,
    omit_repeated_times=True,  # 重複的時間點自動隱藏
)
console_formatter = ConsoleDataFormatter(
    fmt="%(message)s", datefmt="[%Y-%m-%d %H:%M:%S]"
)
rich_handler.setFormatter(console_formatter)
rich_handler.setLevel(logging.DEBUG)  # 開發細節
logger.addHandler(rich_handler)  # 將 Rich Handler 加入

# TODO 觀測專用: 設定 LokiHandler (僅送往 Loki，供觀測平台解析)
loki_formatter = (
    jsonlogger.JsonFormatter()
)  # 移除硬編碼的 fmt 參數 → 會自動把所有屬性全部包進去
loki_endpoint = os.getenv("LOKI_ENDPOINT", "http://127.0.0.1:3100")
loki_handler = logging_loki.LokiHandler(
    url=f"{loki_endpoint}/loki/api/v1/push",
    tags={"app": "fastapi-ide", "env": "development"},
    version="1",
)
loki_handler.setFormatter(loki_formatter)  # 強制指定給 Loki 格式
loki_handler.setLevel(logging.INFO)  # 層級設高
logger.addHandler(loki_handler)
logger.addFilter(TraceIdAliasFilter())


# TODO [3] DB: SQLite 設定
init_database(DB_DIR, DATABASE_URL)
engine = create_engine(DATABASE_URL, connect_args={"timeout": 15})
Base.metadata.create_all(bind=engine)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)


# TODO [4] FastAPI 初始化
class UnifiedLoggingMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request, call_next):
        # 定義排除清單
        # excluded_paths = ["/metrics", "/health"]
        excluded_paths = ["/metrics"]

        start_time = time.perf_counter()
        status_code = 500  # 預設狀態
        error_occurred = False
        try:
            response = await call_next(request)
            status_code = response.status_code

        except Exception as e:
            status_code = 500
            request.state.error_msg = str(e)  # 將錯誤存入 state 供 logger 使用
            error_occurred = True
            raise e

        finally:
            if request.url.path not in excluded_paths:
                process_time = (time.perf_counter() - start_time) * 1000

                msg = getattr(
                    request.state, "log_msg", "API Request Accessed"
                )  # 取得訊息
                extra_data = getattr(request.state, "extra_data", {})  # 取得 extra_data
                level_name = getattr(request.state, "log_level", "info")  # 取得層級
                method = request.method
                url_path = request.url.path

                # 如有捕捉到異常，強制提升層級為 error
                if error_occurred or hasattr(request.state, "error_msg"):
                    level_name = "error"
                    extra_data["error"] = getattr(
                        request.state, "error_msg", "Unknown Error"
                    )

                # TODO 監控延遲狀況
                if process_time > 200:  # 設定門檻值
                    extra_data["latency_alert"] = "high"

                injected_status = getattr(globals(), "fault", {}).get("injected", False)
                injected_status = "True" if injected_status else "False"
                url_path += f"?injected={injected_status}"

                # 統一輸出
                log_func = getattr(logger, level_name.lower(), logger.info)
                extra_data.update(
                    {
                        "method": method,
                        "path": url_path,
                        "status": status_code,
                        "duration_ms": f"{process_time:.2f}",
                    }
                )
                log_func(msg, extra=extra_data)

        return response


app = FastAPI(title="Order Service with Fault Injection")
app.add_middleware(UnifiedLoggingMiddleware)  # 系統層級行為
Instrumentator().instrument(app).expose(app)  # 自動收集 Prometheus Metrics
FastAPIInstrumentor.instrument_app(app)  # 自動收集 Distributed Traces
SQLite3Instrumentor().instrument()  # 自動埋點 不需要每一行 DB 操作手動加 with tracer.start_as_current_span


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


# 故障注入控制器
fault = {"injected": False, "duration": 0}


@app.get("/health")
async def health_check(request: Request):
    request.state.log_level = "info"
    request.state.log_msg = "Health"
    return {
        "status": "ok",
        "path": "/health",
    }


@app.post("/orders/")
async def create_order(
    request: Request,
    item_name: str,
    amount: float,
    customer_id: int,
    db=Depends(get_db),
):
    # 注入故障 (非阻塞)
    if fault["injected"]:
        with tracer.start_as_current_span("fault_injection_delay") as span:
            # logger.warning(
            #     "⚠️ Fault Injected: Delaying I/O", extra={"delay": fault["duration"]}
            # )
            span.set_attribute("delay.duration", fault["duration"])
            await asyncio.sleep(fault["duration"])

    try:
        new_order = Order(item_name=item_name, amount=amount, customer_id=customer_id)
        db.add(new_order)
        db.commit()

        request.state.log_level = "info"
        request.state.log_msg = f"Orders: {new_order.id}"
        return {
            "status": "success",
            "path": "/orders",
        }

    except Exception as e:
        trace.get_current_span().set_status(Status(StatusCode.ERROR))
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/admin/remove-inject")
async def remove_inject(request: Request):
    fault["injected"] = False
    fault["duration"] = 0
    request.state.log_level = "info"
    request.state.log_msg = "Admin/Remove-Inject"
    return {
        "status": "remove-inject",
        "path": "/admin/remove-inject",
    }


@app.post("/admin/inject-fault")
async def inject_fault(request: Request, duration_seconds: int = 5):
    fault["injected"] = True
    fault["duration"] = duration_seconds
    request.state.log_level = "info"
    request.state.log_msg = "Admin/Inject-Fault"
    return {
        "status": "inject-fault",
        "path": "/admin/inject-fault",
    }


@app.get("/orders/{customer_id}")
async def get_orders(request: Request, customer_id: int, db=Depends(get_db)):
    if fault["injected"]:
        with tracer.start_as_current_span("fault_injection_delay") as span:
            # logger.warning(
            #     "⚠️ Fault Injected: Delaying I/O", extra={"delay": fault["duration"]}
            # )
            span.set_attribute("delay.duration", fault["duration"])
            await asyncio.sleep(fault["duration"])

    request.state.log_msg = f"Orders/{customer_id}"
    request.state.log_level = "info"
    request.state.extra_data = {
        "status": "success",
        "path": f"/orders/{customer_id}",
    }
    with tracer.start_as_current_span("sqlite_select"):
        return db.query(Order).filter(Order.customer_id == customer_id).all()


if __name__ == "__main__":
    uvicorn.run(
        "src.scripts.observational_simulation.api:app",
        host="0.0.0.0",
        port=8000,
        reload=True,
    )

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


# TODO 開發專用: 設定 RichHandler (僅輸出到 Console)
class ConsoleDataFormatter(logging.Formatter):
    # 定義標準屬性清單 (logging 內建) => 不顯示
    STANDARD_ATTRS = {
        "name",
        "msg",
        "args",
        "levelname",
        "levelno",
        "pathname",
        "filename",
        "module",
        "exc_info",
        "exc_text",
        "stack_info",
        "lineno",
        "funcName",
        "created",
        "msecs",
        "relativeCreated",
        "thread",
        "threadName",
        "processName",
        "process",
        "taskName",
        "otelSpanID",
        "otelTraceID",
        "otelTraceSampled",
        "otelServiceName",
        "span_id",
        "message",
        # 'trace_id',
    }

    def format(self, record):
        msg = super().format(record)  # 取得主訊息
        extra = {
            k: v for k, v in record.__dict__.items() if k not in self.STANDARD_ATTRS
        }  # 取出「不在」標準屬性清單中的鍵值
        if extra:  # 統一處理顯示
            data_str = " | ".join([f"[dim]{k}={v}[/]" for k, v in extra.items()])
            return f"{msg} [dim][{data_str}][/]"
        return msg


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
)  # 移除硬編碼的 fmt 參數 => 會自動把所有屬性全部包進去
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
        start_time = time.perf_counter()

        # 排除掉不需要記錄的頻繁路徑
        if request.url.path in ["/metrics"]:
            # if request.url.path in ["/health", "/metrics"]:
            return await call_next(request)

        response = await call_next(request)
        process_time = (time.perf_counter() - start_time) * 1000

        # TODO 防呆機制
        # 取得自定義訊息，如果 API 沒設，給予預設值
        msg = getattr(request.state, "log_msg", "API Request Processed")

        # 取得層級
        level_name = getattr(request.state, "log_level", "info")

        # 輸出日誌(統一入口)
        log_func = getattr(logger, level_name.lower(), logger.info)
        log_func(
            msg,
            extra={
                "method": request.method,
                "path": request.url.path,
                "status": response.status_code,
                "duration_ms": f"{process_time:.2f}",
            },
        )
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

# with tracer.start_as_current_span("test-span"):
#     logger.info("Trace generated!")


@app.get("/health")
async def health_check(request: Request):
    request.state.log_level = "info"
    request.state.log_msg = "Health => Check Accessed"
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
    start_time = time.perf_counter()  # 記錄開始時間

    # 注入故障 (非阻塞)
    if fault["injected"]:
        logger.warning(
            "⚠️ Fault Injected: Delaying I/O", extra={"delay": fault["duration"]}
        )
        await asyncio.sleep(fault["duration"])

    try:
        new_order = Order(item_name=item_name, amount=amount, customer_id=customer_id)
        db.add(new_order)
        db.commit()

        duration_ms = (time.perf_counter() - start_time) * 1000  # 計算耗時

        # logger.info(
        #     "Orders => Check Accessed",
        #     extra={
        #         "status": "success",
        #         "order_id": new_order.id,
        #         "path": "/orders",
        #         "duration_ms": round(duration_ms, 2),  # 延遲注入
        #     },
        # )
        request.state.log_level = "info"
        request.state.log_msg = "Orders => Check Accessed"
        return {
            "status": "success",
            "order_id": new_order.id,
            "path": "/orders",
            "duration_ms": round(duration_ms, 2),
        }

    except Exception as e:
        trace.get_current_span().set_status(Status(StatusCode.ERROR))
        # FIXME
        logger.error("Database Operation Failed[/bold red]", extra={"error": str(e)})
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/admin/remove-inject")
async def remove_inject(request: Request):
    fault["injected"] = False
    fault["duration"] = 0
    # logger.info(
    #     "Admin/Remove-Inject => Check Accessed",
    #     extra={"status": "remove-inject", "path": "/admin/remove-inject"},
    # )
    request.state.log_level = "info"
    request.state.log_msg = "Admin/Remove-Inject => Check Accessed"
    return {
        "status": "remove-inject",
        "path": "/admin/remove-inject",
    }


@app.post("/admin/inject-fault")
async def inject_fault(request: Request, duration_seconds: int = 5):
    fault["injected"] = True
    fault["duration"] = duration_seconds
    # logger.info(
    #     "Admin/Inject-Fault => Check Accessed",
    #     extra={"status": "inject-fault", "path": "/admin/inject-fault"},
    # )
    request.state.log_level = "info"
    request.state.log_msg = "Admin/Inject-Fault => Check Accessed"
    return {
        "status": "inject-fault",
        "path": "/admin/inject-fault",
    }


@app.get("/orders/{customer_id}")
async def get_orders(request: Request, customer_id: int, db=Depends(get_db)):
    if fault["injected"]:
        logger.warning(
            "⚠️ Fault Injected: Delaying I/O", extra={"delay": fault["duration"]}
        )
        await asyncio.sleep(fault["duration"])

    with tracer.start_as_current_span("sqlite_select"):
        logger.info(
            f"Orders/{customer_id} => Check Accessed",
            extra={
                "level_name": "info",
                "status": "success",
                "order_id": customer_id,
                "path": f"/orders/{customer_id}",
            },
        )
        # FIXME
        return db.query(Order).filter(Order.customer_id == customer_id).all()


if __name__ == "__main__":
    uvicorn.run(
        "src.scripts.observational_simulation.api:app",
        host="0.0.0.0",
        port=8000,
        reload=True,
    )

import logging
from rich.theme import Theme
from rich.console import Console
from rich.logging import RichHandler


# TODO 開發專用: 設定 RichHandler (僅輸出到 Console)
class ConsoleDataFormatter(logging.Formatter):
    # 定義標準屬性清單 (logging 內建) → 不顯示
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


# 定義主題
# rich_theme = Theme({
#     "logging.level.info":     "bold white",
#     "logging.level.warning":  "bold yellow",
#     "logging.level.error":    "bold red",
#     "logging.level.critical": "bold white on red",
# })


logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
if logger.hasHandlers():  # 避免重複觸發 logger
    logger.handlers.clear()

# TODO 設定 RichHandler (僅輸出到 Console，供開發檢視)
console = Console(
    force_terminal=True,  # 強制開啟終端機模式 (即使不是互動式環境)
    # force_interactive=True,   # 強制開啟互動模式
    color_system="truecolor",  # 強制開啟 24-bit 顏色支援
    width=300,  # 設定寬度，避免太早換行
)
rich_handler = RichHandler(
    # console=Console(theme=rich_theme),
    console=console,
    show_time=True,
    show_path=False,
    show_level=True,
    rich_tracebacks=True,
    markup=False,
    # markup=True,
)
logger.addHandler(rich_handler)  # 將 Rich Handler 加入

logger.debug("Debug ( 通常是灰色或不明顯 )")
logger.info("Info ( 通常是白色 )")
logger.warning("Warning ( 應該會變黃色 )")
logger.error("Error ( 應該會變紅色 )")
logger.critical("Critical ( 應該會變紅底色 )")

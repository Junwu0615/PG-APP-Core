import logging
from rich.theme import Theme
from rich.console import Console
from rich.logging import RichHandler


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
rich_handler = RichHandler(
    # console=Console(theme=rich_theme),
    console=Console(),
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

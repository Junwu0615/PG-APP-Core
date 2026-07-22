import sys, time, logging, psycopg2
from rich.theme import Theme
from rich.console import Console
from rich.logging import RichHandler
from utils.normal import find_project_root


# 自動取得專案根目錄 + 將專案根目錄動態加入系統路徑
PROJECT_ROOT = find_project_root()
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))
from src.scripts.observational_simulation.logging_test import ConsoleDataFormatter


logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
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
    markup=False,
)
console_formatter = ConsoleDataFormatter(
    fmt="%(message)s", datefmt="[%Y-%m-%d %H:%M:%S]"
)
rich_handler.setFormatter(console_formatter)
rich_handler.setLevel(logging.DEBUG)  # 開發細節
logger.addHandler(rich_handler)  # 將 Rich Handler 加入

DB_CONFIG = {
    "host": "postgresql.k8s.local",
    "port": "5432",
    "dbname": "pgdatabase",
    "user": "migration_user",
    "password": "migration_pwd"
}


def monitor_roles(interval=1):
    logger.info("[Script B] 開始監控 PostgreSQL 動態角色(過濾條件: v - token - % ) ... 按 Ctrl + C 結束")
    try:
        while True:
            conn = psycopg2.connect(**DB_CONFIG)
            cursor = conn.cursor()

            # 查詢符合 Vault 動態產生的帳號
            cursor.execute("SELECT rolname, rolvaliduntil FROM pg_roles WHERE rolname LIKE 'v-token-%';")
            rows = cursor.fetchall()

            timestamp = time.strftime("%Y-%m-%d %H:%M:%S")
            if rows:
                logger.info(f"[Script B] 發現現存動態帳號:")
                for row in rows:
                    logger.warning(f"  - 帳號: {row[0]} | 有效期限: {row[1]}")
            else:
                logger.info(f"[Script B] 目前無任何 v-token 動態帳號存在 (已自動清空或尚未建立)")

            cursor.close()
            conn.close()
            time.sleep(interval)

    except KeyboardInterrupt:
        logger.warning("[Script B] 監控已停止 ...")

    except Exception as e:
        logger.error("[Script B] 發生錯誤", exc_info=True)


if __name__ == "__main__":
    monitor_roles(interval=1)
import time, logging, requests, psycopg2
from rich.theme import Theme
from rich.console import Console
from rich.logging import RichHandler
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

VAULT_ADDR = "http://127.0.0.1:8200"
VAULT_TOKEN = "S0lCbeweu9mm7++U5IXaHp8b1qbIhwQXCGhJmQenwXE="
ROLE_NAME = "dynamic-app-role"
DB_HOST = "postgresql.k8s.local"
DB_PORT = "5432"
DB_NAME = "pgdatabase"


def get_vault_credentials():
    url = f"{VAULT_ADDR}/v1/database/creds/{ROLE_NAME}"
    headers = {"X-Vault-Token": VAULT_TOKEN}

    logger.info("[Script A] 正在向 Vault 請求動態資料庫憑證 ...")
    response = requests.get(url, headers=headers)
    if response.status_code != 200:
        raise Exception(f"無法取得 Vault 憑證: {response.text}")

    data = response.json()["data"]
    return data["username"], data["password"]


def main():
    try:
        # 1. 向 Vault 索取暫時帳密
        username, password = get_vault_credentials()
        logger.info(f"[Script A] 成功取得動態帳號: {username}")

        # 2. 使用動態憑證連線 PostgreSQL
        logger.info("[Script A] 正在使用動態憑證連線資料庫 ...")
        conn = psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            dbname=DB_NAME,
            user=username,
            password=password
        )

        cursor = conn.cursor()
        cursor.execute("SELECT current_user, version();")
        db_user, db_version = cursor.fetchone()
        logger.info(f"[Script A] 查詢成功 → 當前身分: {db_user}")

        # 保持連線 5 秒模擬業務操作
        time.sleep(5)

        # 3. 關閉連線
        cursor.close()
        conn.close()
        logger.info("[Script A] 業務操作結束，已主動斷開連線 ...")

    except Exception as e:
        logger.error("[Script A] 發生錯誤", exc_info=True)


if __name__ == "__main__":
    main()
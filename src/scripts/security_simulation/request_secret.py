import sys, time, logging, requests, psycopg2
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

VAULT_ADDR = "http://127.0.0.1:8200"
VAULT_TOKEN = "root"
ROLE_NAME = "dynamic-app-role"
DB_HOST = "postgresql.k8s.local"
DB_PORT = "5432"
DB_NAME = "pgdatabase"


# 輪巡設定
LOOP_INTERVAL = 5  # 每次循環間隔秒數
MAX_ITERATIONS = 3  # 測試輪次

# 建立共用 HTTP Session (優化高頻請求的 TCP 連線重複使用)
http_session = requests.Session()
http_session.headers.update({"X-Vault-Token": VAULT_TOKEN})


def get_vault_credentials():
    url = f"{VAULT_ADDR}/v1/database/creds/{ROLE_NAME}"

    # 使用 session 取代每次新建 requests，大幅減少 TCP 負擔
    response = http_session.get(url, timeout=3)
    if response.status_code != 200:
        raise Exception(f"無法取得 Vault 憑證: {response.text}")

    data = response.json()["data"]
    return data["username"], data["password"]


def run_simulation_cycle(cycle_id):
    conn = None
    try:
        logger.info(f"[Script A] ─── 第 {cycle_id} 輪測試開始 ───")

        # 1. 向 Vault 索取暫時帳密
        username, password = get_vault_credentials()
        logger.debug(f"[Script A] 成功取得動態帳號: {username}")

        # 2. 使用動態憑證連線 PostgreSQL
        conn = psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            dbname=DB_NAME,
            user=username,
            password=password,
            connect_timeout=3
        )

        with conn.cursor() as cursor:
            cursor.execute("SELECT current_user;")
            db_user = cursor.fetchone()[0]
            logger.info(f"[Script A] 查詢成功 → 動態身分: {db_user}")

        time.sleep(1)

    except Exception as e:
        logger.error(f"[Script A] 第 {cycle_id} 輪發生錯誤", exc_info=True)

    finally:
        # 3. 確保關閉連線，防止資源洩漏
        if conn:
            conn.close()
            logger.info(f"[Script A] ─── 第 {cycle_id} 輪結束，連線已釋放 ───\n")


def main():
    iteration = 1
    try:
        while MAX_ITERATIONS is None or iteration <= MAX_ITERATIONS:
            run_simulation_cycle(iteration)
            iteration += 1
            time.sleep(LOOP_INTERVAL)

    except KeyboardInterrupt:
        logger.warning("[Script A] 收到中斷訊號，安全退出輪巡。")


if __name__ == "__main__":
    main()
import os, sys, logging
from pathlib import Path


class TraceIdAliasFilter(logging.Filter):
    def filter(self, record):
        """
        TODO 若存在 otelTraceID，則複製一份給 trace_id
        """
        if hasattr(record, "otelTraceID"):
            record.trace_id = record.otelTraceID
        if hasattr(record, "otelSpanID"):
            record.span_id = record.otelSpanID
        return True


def find_project_root(marker_names=(".git", "README.md")):
    """
    從當前檔案所在的目錄開始向上搜尋，直到找到包含指定標記（如 .git 或 README.md）的資料夾為止
    """
    current_path = Path(__file__).resolve().parent
    # 向上搜尋直到根目錄 (/)
    for parent in [current_path] + list(current_path.parents):
        if any((parent / marker).exists() for marker in marker_names):
            return parent
    # 若找不到，退而求其次回傳當前腳本的父目錄
    return current_path


def init_database(db_dir: str, database_url: str):
    if not os.path.exists(db_dir):
        os.makedirs(db_dir)
        print(f"Created directory: {db_dir}")
    print(f"Connecting to SQLite at: {database_url}")


# def get_db():
#     """
#     Dependencyinjection Session
#     """
#     db = SessionLocal()
#     try:
#         yield db
#     finally:
#         db.close()

import os, logging


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

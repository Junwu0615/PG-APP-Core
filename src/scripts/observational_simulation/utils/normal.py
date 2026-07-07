import os


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

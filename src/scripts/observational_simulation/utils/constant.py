import os

DB_DIR = "./data"
DB_PATH = os.getenv("DB_PATH", os.path.join(DB_DIR, "orders.db"))
DATABASE_URL = f"sqlite:///{DB_PATH}"

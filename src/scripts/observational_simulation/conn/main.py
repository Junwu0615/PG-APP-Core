import os
from sqlalchemy import create_engine, Column, Integer, String, Float
from sqlalchemy.orm import sessionmaker, declarative_base


# 定義 SQLite 資料庫路徑
# 在 Docker/K8s 中，這通常是一個掛載點，例如 /app/data/orders.db
DB_DIR = "./data"
DB_PATH = os.path.join(DB_DIR, "orders.db")


# 確保目錄存在
if not os.path.exists(DB_DIR):
    os.makedirs(DB_DIR)
    print(f"Created directory: {DB_DIR}")

DATABASE_URL = f"sqlite:///{DB_PATH}"
print(f"Connecting to SQLite at: {DATABASE_URL}")

Base = declarative_base()


# 定義 Order Model (與 app.py 保持一致)
class Order(Base):
    __tablename__ = "orders"
    id = Column(Integer, primary_key=True, index=True)
    item_name = Column(String, index=True)
    amount = Column(Float)
    customer_id = Column(Integer)


def init_database():
    try:
        # 創建 engine
        engine = create_engine(DATABASE_URL)

        # 創建表 (如果表已存在則不做任何動作)
        Base.metadata.create_all(bind=engine)
        print("✅ Database and tables initialized successfully.")

        # 驗證連接
        conn = engine.connect()
        conn.close()
        print("✅ Database connection verified.")

    except Exception as e:
        print(f"❌ Failed to initialize database: {e}")
        exit(1)


if __name__ == "__main__":
    init_database()

import os, time
from sqlalchemy import create_engine
from table.order import Base
from utils.normal import init_database
from utils.constant import (
    DB_DIR,
    DB_PATH,
    DATABASE_URL,
)


def conn_database():
    conn = None
    try:
        engine = create_engine(DATABASE_URL)
        Base.metadata.create_all(bind=engine)
        print("✅ Database and tables initialized successfully.")

        while True:
            try:
                conn = engine.connect()
                print("✅ Database connection verified.")
                time.sleep(1)
                conn.close()
                time.sleep(1)

            except Exception as e:
                print(f"❌ Database connection failed: {e}. Retrying in 5 seconds ...")
                time.sleep(5)
                continue

    except Exception as e:
        print(f"❌ Failed to initialize database: {e}")

    finally:
        if conn:
            conn.close()
            print("✅ Database connection close ... successfully.")
        exit(0)


if __name__ == "__main__":
    init_database(DB_DIR, DATABASE_URL)
    conn_database()

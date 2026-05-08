# -*- coding: utf-8 -*-
"""
Update Date: 2026-03-24
Description: creates partition tables for machine_status_logs based on the month of log_time
"""
import sys, os; sys.path.insert(0, os.getcwd())

import psycopg2
from shared.modules.log import Logger
from shared.utils.env_config import GET_PATH_ROOT, get_logger_name
from shared.utils.postgres_tools import close_conn

console_name = get_logger_name(__file__, GET_PATH_ROOT)
logging = Logger(console_name=console_name)


def _get_sql_script(table_name: str) -> str:
    # file_path = f'../sql/scripts/{table_name}/auto_partition.sql'
    file_path = f'dags/sql/auto_partition/{table_name}.sql'

    with open(file_path, 'r', encoding='utf-8') as f:
        sql_script = f.read()
    return sql_script


def main():
    conn, cursor = None, None
    try:
        conn = psycopg2.connect(
            host='127.0.0.1',
            database='pgdatabase',
            user='migration_user',
            password='migration_pwd'
        )
        cursor = conn.cursor()

        for table_name in [
            'machine_status_logs',
            'production_records',
            'fact_production'
        ]:
            sql = _get_sql_script(table_name)
            cursor.execute(sql)
            logging.notice(f"Created partition table for '{table_name}' successfully.")

        conn.commit()

    except Exception as e:
        logging.error('Exception: ', exc_info=True)
        conn.rollback()

    finally:
        close_conn(conn, cursor, logging)
        return 0

if __name__ == '__main__':
    exit_code = main()
    sys.exit(exit_code)
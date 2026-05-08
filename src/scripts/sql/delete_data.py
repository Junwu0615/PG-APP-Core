# -*- coding: utf-8 -*-
"""
Update Date: 2026-03-24
Description: deletes all data from the specified tables
"""
import sys, os; sys.path.insert(0, os.getcwd())

import psycopg2
from shared.modules.log import Logger
from shared.utils.env_config import GET_PATH_ROOT, get_logger_name
from shared.utils.postgres_tools import close_conn

console_name = get_logger_name(__file__, GET_PATH_ROOT)
logging = Logger(console_name=console_name)


TARGET_LIST = [
    # OLTP Tables
    'oltp.machine_status_logs',
    # 'oltp.machine_events', # not used
    'oltp.production_records',
    'oltp.production_orders',
    # 'oltp.machine_status_logs_2026_03',
    # 'oltp.machine_status_logs_2026_04',
    # 'oltp.machine_status_logs_2026_05',
    # 'oltp.product',
    # 'oltp.machine',
    # OLAP Tables
    # 'olap.dim_time',
    # 'olap.dim_product',
    # 'olap.dim_machine',
    # 'olap.fact_production',
    # 'olap.fact_machine_status',
]

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

        for table in TARGET_LIST:
            sql = f"""
            SET ROLE oltp_owner;

            TRUNCATE TABLE {table} RESTART IDENTITY CASCADE;

            RESET ROLE;
            """
            cursor.execute(sql)
            logging.info(f'TRUNCATE TABLE DATA FROM {table} ...')

        conn.commit()
        logging.notice('All data deleted successfully.')

    except Exception as e:
        logging.error('Exception: ', exc_info=True)
        conn.rollback()

    finally:
        close_conn(conn, cursor, logging)
        return 0

if __name__ == '__main__':
    exit_code = main()
    sys.exit(exit_code)
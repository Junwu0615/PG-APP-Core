# -*- coding: utf-8 -*-
"""
Update Date: 2026-03-24
Description: drop the specified tables
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
    # 'oltp.machine_events', # not used
    'oltp.machine_status_logs',
    # 'oltp.machine_status_logs_2026_04',
    # 'oltp.machine_status_logs_2026_05',
    'oltp.production_records',
    # 'oltp.production_records_2026_04',
    # 'oltp.production_records_2026_05',
    'oltp.production_orders',
    'oltp.machine',
    'oltp.product',

    # OLAP Tables
    'olap.fact_production',
    # 'olap.fact_production_2026_04',
    # 'olap.fact_production_2026_05',
    'olap.fact_machine_status',
    'olap.dim_date',
    'olap.dim_product',
    'olap.dim_machine',
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
            schema_owner = f"{table.split('.')[0]}_owner"

            sql = f"""
            -- SET ROLE {schema_owner};
            
            TRUNCATE TABLE {table} RESTART IDENTITY CASCADE;
            
            -- RESET ROLE;
            """
            cursor.execute(sql)
            logging.notice(f'#1 TRUNCATE TABLE {table} ...')

            sql = f"""
            -- SET ROLE {schema_owner};
            
            DROP TABLE {table};
            
            -- RESET ROLE;
            """
            cursor.execute(sql)
            logging.notice(f'#2 DROP TABLE {table} ...')

        conn.commit()
        logging.notice('All drop table successfully.')

    except Exception as e:
        logging.error('Exception: ', exc_info=True)
        conn.rollback()

    finally:
        close_conn(conn, cursor, logging)
        return 0

if __name__ == '__main__':
    exit_code = main()
    sys.exit(exit_code)
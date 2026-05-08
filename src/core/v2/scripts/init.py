# -*- coding: utf-8 -*-
import sys, os; sys.path.insert(0, os.getcwd())

from shared.utils.tools import *
from shared.utils.env_config import GET_PATH_ROOT, get_logger_name
from shared.utils.postgres_tools import get_conn, close_conn, table_exists
from shared.modules.log import Logger


console_name = get_logger_name(__file__, GET_PATH_ROOT)
logging = Logger(console_name=console_name)


YAML_VERSION = 'v2'
YAML_PATH = os.path.join('./src/core', f'{YAML_VERSION}', 'factory_config.yaml')
config = parsing_yaml(YAML_PATH)

db = config['database']
init_data = config['init_data']
simulate = config['simulate']


def generate_machines(conn, cursor):
    """
    TODO 數據生成邏輯
        - 機台名稱需要按照獲得機種順序依序遞增
        - 確認 oltp.machine 是否已建表，若有取得機台號碼
        - 生成靜態表
    """
    record_count = {} # 記錄編碼
    if table_exists(cursor, 'oltp', 'machine'):
        cursor.execute("""
        SELECT DISTINCT ON (machine_type)
        machine_name
        FROM oltp.machine
        ORDER BY machine_type, machine_id DESC;
        """)
        machines = cursor.fetchall()
        record_count = {i[0].split('-')[1]:int(i[0].split('-')[-1]) for i in machines}

    count = 0
    for line, machines in init_data['machine_layout'].items():
        for m in machines:
            if m not in record_count:
                record_count[m] = 0
            record_count[m] += 1

            cursor.execute("""
            INSERT INTO oltp.machine
            (machine_name, machine_type, line_no)
            VALUES (%s, %s, %s)
            """,
            (
                f'M-{m}-{record_count[m]}',
                m,
                line
            ))
            count += 1

    conn.commit()
    logging.info(f"[{count}] oltp.machine generated ...")


def generate_products(conn, cursor):
    """
    TODO 數據生成邏輯
        - product_type 綁 machine_type ( 指定訂單只能指定機種生產 )
        - 需要去撈 oltp.machine 確認目前有什麼機台種類，基於該種類進行訂單生成
    """
    cursor.execute("""
    SELECT DISTINCT ON (machine_type)
    machine_type
    FROM oltp.machine;
    """)
    mach_type = cursor.fetchall()
    record_content = [i[0] for i in mach_type] # 記錄編碼

    for i in range(init_data['products']):
        _target_qty = random.randint(simulate['target_qty_min'], simulate['target_qty_max'])
        _get_type = random.choice(record_content)

        cursor.execute("""
        INSERT INTO oltp.product
        (product_name, product_type, target_qty)
        VALUES (%s, %s, %s)
        """,
        (
            f'{_get_type}-{random.randint(0, 999999):06d}',
            _get_type,
            _target_qty,
        ))
    conn.commit()
    logging.info(f"[{init_data['products']}] oltp.product generated ...")


def main():
    """
    TODO 動作事項
        - OLTP : 初始化「機台」,「訂單」數據
    """
    conn, cursor = None, None
    logging.notice('Starting Init Factory Data ...')
    try:
        conn = get_conn(db, logging)
        cursor = conn.cursor()

        generate_machines(conn, cursor)
        generate_products(conn, cursor)


    except psycopg2.DatabaseError as e:
        logging.error(f'[# Rollback] Exception [Code: {e.pgcode}]', exc_info=True)
        conn.rollback()

    except Exception as e:
        logging.error('[# Other] Exception', exc_info=True)

    finally:
        close_conn(conn, cursor, logging)
        return 0


if __name__ == '__main__':
    exit_code = main()
    sys.exit(exit_code)
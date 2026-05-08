# -*- coding: utf-8 -*-
"""
TODO
    Update Date: 2026-04-04
    Description:
        - Supporting Contexts: OFF_PEAK, NORMAL, PEAK
    Notice:
        - 提交方式
            - [基準] cursor.execute : 每次執行後都需等待資料庫回應，適合單筆交易，但在大量交易時可能導致性能瓶頸
            - [速度提升有限] cursor.executemany : 類似 execute_batch，但在某些情況下可能效率較低，適合中等量的交易
                - 實際邏輯是將多筆交易組合成一個大的 SQL 語句，對資料庫造成較大負擔，且在某些情況下可能導致記憶體問題
                - ex: for row: execute()
            - [5~50x] cursor.execute_batch : 將多筆交易一次性發送給資料庫，減少往返次數，適合批量插入，需注意記憶體使用
            - [最快] cursor.execute_values : 類似 execute_batch，但使用 VALUES 語法，對於大量插入特別有效，需注意記憶體使用
"""
import sys, os; sys.path.insert(0, os.getcwd())

from shared.configs import time, random, collections, psycopg2
from shared.configs.constant import *
from shared.utils.tools import *
from shared.utils.env_config import GET_PATH_ROOT, get_logger_name
from shared.utils.postgres_tools import get_conn, close_conn
from shared.modules.log import Logger
from src.core.models.simulator import MachineStatusSimulator


console_name = get_logger_name(__file__, GET_PATH_ROOT)
logging = Logger(console_name=console_name, is_logstash=False)

mss = MachineStatusSimulator()

YAML_VERSION = 'v1'
with open(os.path.join('./src/core', f'{YAML_VERSION}', 'factory_config.yaml')) as f:
    config = yaml.safe_load(f)

db = config['database']
simulate = config['simulate']
load_cfg = config['load_profile']

BATCH_SIZE = simulate['batch_size']
NUM_ORDERS = simulate['orders']


def check_is_create_order(cursor, event_dict: dict, prob: float) -> int:
    """
    TODO 基於機率檢查是否要新增生產訂單
    """
    if random.random() < prob:
        ret = insert_production_order(cursor, event_dict)
        return ret
    return 0


def update_order_status(cursor, event_dict: dict) -> int:
    """
    TODO 檢查是否有訂單完成，若完成則更新訂單狀態並從訂單列表移除
    """
    ret = 0
    try:
        if len(event_dict['order_dict'].keys()) > 0:
            _target_list = copy.deepcopy(list(event_dict['order_dict'].keys()))
            for _order_id in _target_list:
                detail = event_dict['detail'][_order_id]
                if detail['produced_qty'] >= detail['target_qty']:
                    # TODO 1. 更新訂單結束時間
                    cursor.execute("""
                    UPDATE oltp.production_orders
                    SET end_at = %s
                    WHERE order_id = %s
                    """, (
                        get_now(hours=8, tzinfo=TZ_UTC_8),
                        _order_id
                    ))
                    ret += 1

                    # 取得 mach_id 資訊
                    _machine_id = event_dict['detail'][_order_id]['mach_id']

                    # TODO 2. 更新機台狀態 : RUNNING -> IDLE
                    cursor.execute("""
                    INSERT INTO oltp.machine_status_logs
                    (machine_id, status, event_time)
                    VALUES (%s, %s, %s)
                    """,
                    (
                        _machine_id,
                        'IDLE',
                        get_now(hours=8, tzinfo=TZ_UTC_8),
                    ))
                    ret += 1

                    # 從訂單字典移除
                    del event_dict['order_dict'][_order_id]

                    # 同時移除訂單詳情
                    del event_dict['detail'][_order_id]

                    # 清空機台持單狀態 + 狀態轉 IDLE
                    event_dict['machine_status'][_machine_id] = {
                        'status': 'IDLE',
                        'order_id': None,
                    }

                    logging.notice(f'[order_id={_order_id}] have been completed. '
                    f'( produced_qty: {detail['produced_qty']} >= target_qty: {detail['target_qty']} )')

    finally:
        return ret


def insert_production_order(cursor, event_dict: dict) -> int:
    """
    TODO 建立生產訂單: 若是命中，則瞬間生成大量訂單
    """
    ret = 0
    for _ in range(NUM_ORDERS):
        _prod_name = random.choice(list(event_dict['product_dict'].keys()))
        _prod_id = event_dict['product_dict'][_prod_name]['prod_id']
        _prod_type = event_dict['product_dict'][_prod_name]['prod_type']
        _target_qty = event_dict['product_dict'][_prod_name]['target_qty']

        cursor.execute("""
        INSERT INTO oltp.production_orders
        (product_id, quantity, created_at)
        VALUES (%s, %s, %s)
        RETURNING order_id
        """, (
            _prod_id,
            _target_qty,
            get_now(hours=8, tzinfo=TZ_UTC_8),
        ))

        _order_id = cursor.fetchone()[0]

        # 取得訂單後 塞入佇列
        event_dict['order_queue'][_prod_type] += [_order_id]

        # 建立訂單 ID 對照產品 ID
        event_dict['order_dict'][_order_id] = _prod_id
        event_dict['detail'][_order_id] = {
            'product_id': _prod_id,
            'target_qty': _target_qty,
            'produced_qty': 0
        }

        ret += 1
    else:
        return ret


def insert_production_record(cursor, event_dict: dict, _machine_id: int, efficiency: int) -> int:
    """
    TODO 插入實時生產記錄
        - 狀況 1 : 第一次生產匹配
        - 狀況 2 : 持續生產
    """
    ret, _status = 1, None

    # TODO 1. 取得機台類型
    _machine_type = event_dict['machine_dict'].get(_machine_id)

    # TODO 2. 從指定機型佇列中取出第一順位訂單 (而非隨機挑選) ; 或是持續生產
    if event_dict['order_queue'][_machine_type] and event_dict['machine_status'][_machine_id]['status'] == 'IDLE':
        # 須確認是否已經訂單在身，若無取新訂單
        _order_id = event_dict['order_queue'][_machine_type].popleft()
        _status = 'RUNNING'
        event_dict['machine_status'][_machine_id]['status'] = _status
        event_dict['machine_status'][_machine_id]['order_id'] = _order_id

        # TODO 2.1 更新訂單開始作業時間
        cursor.execute("""
        UPDATE oltp.production_orders
        SET start_at = %s
        WHERE order_id = %s
        """, (
            get_now(hours=8, tzinfo=TZ_UTC_8),
            _order_id
        ))
        ret += 1
        logging.info(f'[{_machine_type}: {_machine_id}] Production Begins Based on the Order [{_order_id}].')

        # TODO 2.2 更新機台狀態 : IDLE -> RUNNING
        cursor.execute("""
        INSERT INTO oltp.machine_status_logs
        (machine_id, status, event_time)
        VALUES (%s, %s, %s)
        """,
        (
            _machine_id,
            _status,
            get_now(hours=8, tzinfo=TZ_UTC_8),
        ))
        ret += 1

    elif event_dict['machine_status'][_machine_id]['order_id'] is not None:
        # 須確認是否已經訂單在身，若有先完成既有訂單
        _order_id = event_dict['machine_status'][_machine_id]['order_id']
        _status = event_dict['machine_status'][_machine_id]['status']

    else:
        # logging.notice(f'[{_machine_type}] Not Have Order in Queue, Machine [{_machine_id}] IDLE ...')
        return

    if _status != 'RUNNING':
        return 0

    # TODO 3. 用訂單 ID 取得產品 ID
    _product_id = event_dict['order_dict'].get(_order_id)

    # TODO 4. 根據效率增加生產數量
    for _ in range(efficiency):
        # TODO 5. 隨機生產數
        _quantity = random.randint(simulate['prod_qty_min'], simulate['prod_qty_max'])

        # TODO 6. 插入交易日誌
        cursor.execute("""
        INSERT INTO oltp.production_records
        (order_id, machine_id, product_id, quantity, event_time)
        VALUES (%s, %s, %s, %s, %s)
        """,
        (
            _order_id,
            _machine_id,
            _product_id,
            _quantity,
            get_now(hours=8, tzinfo=TZ_UTC_8),
        ))

        # TODO 7. 更新事務字典中的訂單計數狀況 + mach_id 資訊
        event_dict['detail'][_order_id]['produced_qty'] += _quantity
        event_dict['detail'][_order_id]['mach_id'] = _machine_id
        ret += 1

    return ret


def insert_machine_status(cursor, event_dict: dict, _machine_id: int) -> int:
    """
    TODO 插入機台狀態 : 在此實施隨機邏輯，可基於權重機率調整
        - MAINTENANCE # 1 # process: [1 -> 2]
        - IDLE # 2 # process: [2 -> 1], [2 -> 3]
        - RUNNING # 3 # process: [3 -> 2], [3 -> 4]
        - ALARM # 4 # process: [4 -> 3]
    """
    _random = None

    # 1. 取得當前狀態
    _event_status = event_dict['machine_status'][_machine_id]['status']

    # 直接返回且不更新狀態
    if _event_status is None:
        return 0

    # 2. 實施隨機邏輯
    _random = mss.get_next_status(_event_status)

    # 直接返回且不更新狀態
    if _event_status == _random:
        return 0

    # 3. 更新當前狀態
    event_dict['machine_status'][_machine_id]['status'] = _random

    # 4. 提交狀態更新
    cursor.execute("""
    INSERT INTO oltp.machine_status_logs
    (machine_id, status, event_time)
    VALUES (%s, %s, %s)
    """,
    (
        _machine_id,
        _random,
        get_now(hours=8, tzinfo=TZ_UTC_8),
    ))
    return 1


def init_transaction_dict(conn, cursor) -> dict:
    """
    TODO 初始化事務字典 : 用字典記錄必要變數，包含機台列表、產品列表、訂單列表 ...etc.
        - 從資料庫讀取產品資訊
        - 產品完成後 移除訂單索引
    """
    event_dict = {
        # TODO 過程不異動
        'machine_dict': [],  # 機台字典 key: mach_id, value: mach_type
        'product_dict': {},  # 產品字典 key: prod_id, value: prod_type

        # TODO 過程會異動
        'order_dict': {},    # 訂單字典 key: order_id, value: prod_id
        'machine_status': {},  # 記錄機台持單狀態 # None / Not None (order_id)
        'detail': {}, # 訂單詳情字典 key: order_id, value: dict (product_id, target_qty, produced_qty, mach_id)
        'order_queue': {}, # 訂單隊列 # 按照順序依序給予機器運轉
        # 'order_count': 0,  # 總訂單數 ; 已完成訂單數: 總訂單數 - 尚生產數
    }

    try:
        # 取得機台列表
        cursor.execute("""
        SELECT machine_id, machine_type
        FROM oltp.machine
        """)
        machines = cursor.fetchall()
        event_dict['machine_dict'] = {i[0]:i[1] for i in machines}
        event_dict['machine_status'] = {i:
            {
                'status': 'IDLE',
                'order_id': None,
        } for i in list(event_dict['machine_dict'].keys())}

        # 取得產品列表
        cursor.execute("""
        SELECT product_name, product_id, product_type, target_qty
        FROM oltp.product
        """)
        products = cursor.fetchall()
        event_dict['product_dict'] = {i[0]:{
            'prod_id': i[1],
            'prod_type': i[2],
            'target_qty': i[3],
        } for i in products}
        event_dict['order_queue'] = {i: collections.deque() for i in list(set(i[2] for i in products))}

        # 1. 更新機台狀態 : IDLE
        for _machine_id in event_dict['machine_dict'].keys():
            cursor.execute("""
            INSERT INTO oltp.machine_status_logs
            (machine_id, status, event_time)
            VALUES (%s, %s, %s)
            """,
            (
                _machine_id,
                'IDLE',
                get_now(hours=8, tzinfo=TZ_UTC_8),
            ))

        # 2. 初始化第一批訂單
        _ct = insert_production_order(cursor, event_dict)

        conn.commit()

    except psycopg2.DatabaseError as e:
        logging.error(f'[# Rollback] Exception [Code: {e.pgcode}]', exc_info=True)
        conn.rollback()

    except Exception as e:
        logging.error('[# Other] Exception', exc_info=True)

    return event_dict


def simulate_stream(conn, cursor, event_dict: dict):
    data_qty, done_qty, batch_ct = 0, 0, 0
    last_commit_time = time.time()
    while True:
        try:
            now = get_now(hours=8, tzinfo=TZ_UTC_8)
            mode = mss.get_load_profile(now.hour)
            load_setting = load_cfg[mode]

            prob = load_setting['prob']
            efficiency = load_setting['efficiency']

            _ct = check_is_create_order(cursor, event_dict, prob)
            batch_ct, data_qty = batch_ct + _ct, data_qty + _ct

            # TODO 所有機台皆要進行判斷狀態更新
            for _machine_id in event_dict['machine_status'].keys():
                _ct = insert_production_record(cursor, event_dict, _machine_id, efficiency)
                if isinstance(_ct, int):
                    batch_ct, data_qty = batch_ct + _ct, data_qty + _ct

                # TODO 隨機更新指定狀態
                _ct = insert_machine_status(cursor, event_dict, _machine_id)
                batch_ct, data_qty = batch_ct + _ct, data_qty + _ct

            _ct = update_order_status(cursor, event_dict)
            done_qty, batch_ct, data_qty = done_qty + _ct, batch_ct + _ct, data_qty + _ct

            # TODO 根據 BATCH_SIZE 或 時間間隔 (30s) 提交事務
            if batch_ct >= BATCH_SIZE or (time.time() - last_commit_time) > 30:
                conn.commit()
                batch_ct = 0
                last_commit_time = time.time()

            # TODO 輸出當前模擬狀態
            _order_qty = len(event_dict['order_dict'].keys())
            temp_dict = {}
            for k, v in event_dict['machine_status'].items():
                key = v['status']
                if key not in temp_dict:
                    temp_dict[key] = 0
                temp_dict[key] += 1
            _sum = sum(temp_dict.values())
            ret_1 = ' | '.join([f'{k}=[{v}/{_sum}]' for k, v in temp_dict.items()])
            ret_2 = ' | '.join([f'{k}[{len(v)}]' for k,v in event_dict['order_queue'].items()])

            logging.info(
                f'\n整體の概要 : '
                f'MODE={mode} | '
                f'ORDER_IN_PROGRESS={_order_qty} | '
                f'DONE_QTY={done_qty} | '
                f'DATA_QTY={data_qty} | '
                f'BATCH=[{batch_ct}/{BATCH_SIZE}]\n'
                f'當前機台の狀態 : {ret_1}\n'
                f'等機台任領の訂單 : {ret_2}'
            )

            time.sleep(1)

        # except psycopg2.OperationalError as e:
        except psycopg2.InterfaceError as e:
            logging.error('[# Re-Connect] Exception', exc_info=True)
            close_conn(conn, cursor, logging)
            conn = get_conn(db, logging)
            cursor = conn.cursor()

        except psycopg2.DatabaseError as e:
            logging.error(f'[# Rollback] Exception [Code: {e.pgcode}]', exc_info=True)
            conn.rollback()

        except Exception as e:
            logging.error('[# Other] Exception', exc_info=True)


def main():
    conn, cursor = None, None
    logging.notice('Starting Factory Stream Simulation...')
    try:
        conn = get_conn(db, logging)
        cursor = conn.cursor()

        event_dict = init_transaction_dict(conn, cursor)

        if event_dict['detail'] != {}:
            simulate_stream(conn, cursor, event_dict)

    except KeyboardInterrupt:
        logging.warning('偵測到 Ctrl+C，正在關閉連線 ...')
        conn.commit()
        logging.warning('已落實最後一次事務提交 ...')

    except Exception as e:
        logging.error('Exception', exc_info=True)

    finally:
        close_conn(conn, cursor, logging)
        return 0

if __name__ == '__main__':
    exit_code = main()
    sys.exit(exit_code)
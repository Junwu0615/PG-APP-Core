# -*- coding: utf-8 -*-
"""
TODO
    Update Date: 2026-04-28
    Description:
    Notice:
"""
import sys, os; sys.path.insert(0, os.getcwd())

from shared.configs import (
    load_dotenv,
    psycopg2,
)
from shared.configs.constant import *
from shared.utils.tools import *
from shared.utils.env_config import GET_PATH_ROOT, get_logger_name
from shared.utils.postgres_tools import get_conn, close_conn
from shared.modules.log import Logger
from shared.modules.entry import EntryPoint
from shared.modules.mqtt import MqttServer, DEFAULT_BROKER, DEFAULT_BROKER_PORT
from src.core.models.simulator import MachineStatusSimulator


class Application(EntryPoint):
    def __init__(self):
        super().__init__(dotenv_path=f'{'/'.join(__file__.split('/')[:-1])}/.env')

        _YAML_VERSION = os.getenv('YAML_VERSION', 'v2')
        _YAML_CONFIGS = parsing_yaml(os.path.join('./src/core', f'{_YAML_VERSION}', 'factory_config.yaml'))
        _SIMULATE = _YAML_CONFIGS['simulate']
        _LOAD_CFG = _YAML_CONFIGS['load_profile']
        _BATCH_SIZE = _SIMULATE['batch_size']
        _BATCH_INTERVAL = _SIMULATE['batch_interval']
        _NUM_ORDERS = _SIMULATE['orders']

        self.env['DB'] = {
            'host': os.getenv('POSTGRES_HOST', None),
            'port': os.getenv('POSTGRES_PORT', None),
            'database': os.getenv('POSTGRES_DATABASE', None),
            'user': os.getenv('POSTGRES_USER', None),
            'password': os.getenv('POSTGRES_PWD', None),
        }

        self.env['NUM_ORDERS'] = _NUM_ORDERS
        self.env['LOAD_CFG'] = _LOAD_CFG
        self.env['BATCH_SIZE'] = _BATCH_SIZE
        self.env['BATCH_INTERVAL'] = _BATCH_INTERVAL
        self.env['_MAIN_NAME'] = f'# CP'

        logging = Logger(
            console_name=get_logger_name(__file__, GET_PATH_ROOT),
            # file_name='main',
            # file_path=f'logs/CP/main.logs',
            backup_count=10,
            **{
                'app_name': 'ooud',
                'service_type': 'command platform',
            }
        )

        self.configure_setting(logging=logging)  # TODO 完成 EntryPoint 必要後續初始化

        self.event_dict = None
        self.conn = None
        self.cursor = None
        self._load_configs()  # 冗長設定


    def _load_configs(self):
        self.ms = MqttServer(
            logging=self.logging,
            log_main_name=self.env['_MAIN_NAME'],
            stop_event=self._stop_event,
            broker_host=os.getenv('MQTT_HOST', None),
            broker_port=os.getenv('MQTT_PORT', None),
            max_workers=1,
            username=os.getenv('MQTT_USER', None),
            password=os.getenv('MQTT_PWD', None),
        )
        self.mss = MachineStatusSimulator()
        self.conn = get_conn(self.env['DB'], self.logging)
        self.cursor = self.conn.cursor()


    def _check_is_create_order(self, prob: float, **kwargs) -> int:
        """
        TODO 基於機率檢查是否要新增生產訂單
        """
        if random.random() < prob:
            ret = self._insert_production_order()
            return ret
        return 0


    def _insert_production_order(self, **kwargs) -> int:
        """
        TODO 建立生產訂單:
            1. 若是命中，則瞬間生成大量訂單
            2. 要把訂單訊息發佈到 Kafka ( MQTT )，而非入 DB 供查詢
        """
        ret = 0
        for _ in range(self.env['NUM_ORDERS']):
            _prod_name = random.choice(list(self.event_dict['product_dict'].keys()))
            _prod_id = self.event_dict['product_dict'][_prod_name]['prod_id']
            _prod_type = self.event_dict['product_dict'][_prod_name]['prod_type']
            _target_qty = self.event_dict['product_dict'][_prod_name]['target_qty']

            _mach_name = random.choice(list(
                k for k,v in self.event_dict['machine_dict'].items() if v['mach_type'] == _prod_type
            ))
            _now_time = get_now(hours=8, tzinfo=TZ_UTC_8)

            self.cursor.execute("""
            INSERT INTO oltp.production_orders (product_id, quantity, created_at)
            VALUES (%s, %s, %s)
            RETURNING order_id
            """, (
                _prod_id,
                _target_qty,
                _now_time,
            ))

            _order_id = self.cursor.fetchone()[0]

            payload = {
                'time': _now_time.isoformat(),
                'mach_name': _mach_name,
                'mach_id': self.event_dict['machine_dict'][_mach_name]['mach_id'],
                # 'mach_type': self.event_dict['machine_dict'][_mach_name]['mach_type'],
                'order_id': _order_id,
                'prod_name': _prod_name,
                'prod_id': _prod_id,
                # 'prod_type': _prod_type,
                'target_qty': _target_qty,
            }
            self.ms.add_content(topic=f'cp/mach-order/{_mach_name}', payload=payload, qos=1)

            ret += 1
        else:
            return ret


    def _init_transaction_dict(self, **kwargs):
        """
        TODO 初始化事務字典 : 用字典記錄必要變數，包含機台列表、產品列表、訂單列表 ...etc.
            - 從資料庫讀取產品資訊
            - 產品完成後 移除訂單索引
        """
        self.event_dict = {
            # TODO 過程不異動
            'machine_dict': [],  # 機台字典 key: mach_id, value: mach_type
            'product_dict': {},  # 產品字典 key: prod_id, value: prod_type
        }
        try:
            # 取得機台列表
            self.cursor.execute("""
            SELECT machine_name, machine_id, machine_type
            FROM oltp.machine
            """)
            machines = self.cursor.fetchall()
            self.event_dict['machine_dict'] = {i[0]:{
                'mach_id': i[1],
                'mach_type': i[2],
            # } for i in machines}
            } for i in machines if i[0] == 'M-CNC-30'} # FIXME DEV

            # 取得產品列表
            self.cursor.execute("""
            SELECT product_name, product_id, product_type, target_qty
            FROM oltp.product
            """)
            products = self.cursor.fetchall()
            self.event_dict['product_dict'] = {i[0]:{
                'prod_id': i[1],
                'prod_type': i[2],
                'target_qty': i[3],
            # } for i in products}
            } for i in products if i[2] == 'CNC'} # FIXME DEV

            # 初始化第一批訂單
            _ct = self._insert_production_order()

            self.conn.commit()

        except psycopg2.DatabaseError as e:
            self.logging.error(f'[# Rollback] Exception [Code: {e.pgcode}]', exc_info=True)
            self.conn.rollback()

        except Exception as e:
            self.logging.error('[# Other] Exception', exc_info=True)


    def _command_platform_stream(self, **kwargs):
        batch_ct = 0
        last_commit_time = time.time()
        while not self._stop_event.is_set():
            try:
                now = get_now(hours=8, tzinfo=TZ_UTC_8)
                mode = self.mss.get_load_profile(now.hour)
                load_setting = self.env['LOAD_CFG'][mode]
                prob = load_setting['prob']

                _ct = self._check_is_create_order(prob)
                batch_ct += _ct

                # TODO 根據 BATCH_SIZE 或 時間間隔 提交事務
                if batch_ct >= self.env['BATCH_SIZE'] or (time.time() - last_commit_time) > self.env['BATCH_INTERVAL']:
                    self.conn.commit()
                    batch_ct = 0
                    last_commit_time = time.time()

                time.sleep(1)

            except psycopg2.InterfaceError as e:
                self.logging.error('[# Re-Connect] Exception', exc_info=True)
                close_conn(self.conn, self.cursor)
                self.conn = get_conn(self.env['DB'])
                self.cursor = self.conn.cursor()

            except psycopg2.DatabaseError as e:
                self.logging.error(f'[# Rollback] Exception [Code: {e.pgcode}]', exc_info=True)
                self.conn.rollback()

            except Exception as e:
                self.logging.error('[# Other] Exception', exc_info=True)


    def __exit__(self, exc_type, exc_val, exc_tb):
        """上下文管理器 : 結束"""
        self.conn.commit()
        self.logging.warning('已落實最後一次事務提交 ...')
        close_conn(self.conn, self.cursor)
        return False


    def run(self):
        """
        TODO 動作事項
            - 實例 : 1
            \
            - MQTT ( Kafka ) : 「傳送」訊息
                - 生產訂單
            - OLTP R (僅初始化):
                - 「機台規格」
                - 「產品規格」
            - OLTP W (Real-time) :
                - 「生產訂單」
        """
        self.logging.notice(f'[{self.env['_MAIN_NAME']}] Starting Factory Stream Simulation ...')
        self._init_transaction_dict()
        self.start_service(self.ms.publisher_server, **{
            'title': f'推送訊息至 [{DEFAULT_BROKER}:{DEFAULT_BROKER_PORT}] MQTT 服務',
        })
        self.start_service(self._command_platform_stream, **{
            'title': '主控台串流服務',
        })
        while not self._stop_event.is_set():
            time.sleep(1)


if __name__ == '__main__':
    app = Application()
    app.main()
# -*- coding: utf-8 -*-
"""
TODO
    Update Date: 2026-05-14
    Description:
    Notice:
        [加入 sqlite 提升 HA 消費訂單事務]:
            - [thread 1] kafka -> consumer -> sqlite ( N 個實例 = N 個 sqlite 實例 ; 用 Loki 監控是否正常消費 )
            - [thread 2] sqlite ( 每次斷掉重啟由此開始 唯一事實 ; 須建立狀態表 ) -> producer -> kafka -> kafka connection sink
"""
import sys, os; sys.path.insert(0, os.getcwd())

from shared.configs import (
    json,
    time,
    random,
    sqlite3,
    load_dotenv,
)
from shared.configs.constant import *
from shared.utils.tools import *
from shared.utils.postgres_tools import close_conn
from shared.utils.env_config import GET_PATH_ROOT, get_logger_name
from shared.modules.log import Logger
from shared.modules.entry import EntryPoint
from shared.modules.kafka_consumer import KafkaConsumerManager
from shared.modules.kafka_producer import KafkaProducerManager
from src.core.models.sink_format import *
from src.core.models.simulator import MachineStatusSimulator


class Application(EntryPoint):
    def __init__(self):
        super().__init__(dotenv_path=f'{'/'.join(__file__.split('/')[:-1])}/.env')

        _YAML_VERSION = os.getenv('YAML_VERSION', 'v2')
        _CONSUMER_ORDER_TOPIC = os.getenv('CONSUMER_ORDER_TOPIC', 'source.cp.mach-order')
        _CONSUMER_GROUP_ID = os.getenv('CONSUMER_GROUP_ID', 'iot-data-mach-processor')
        _KAFKA_HOST = os.getenv('KAFKA_HOST', '127.0.0.1:9092')
        _KAFKA_AUTO_OFFSET_RESET = os.getenv('KAFKA_AUTO_OFFSET_RESET', 'earliest')
        _KAFKA_ENABLE_AUTO_COMMIT = os.getenv('KAFKA_ENABLE_AUTO_COMMIT', False)
        _KAFKA_SCHEMA_REGISTRY_HOST = os.getenv('KAFKA_SCHEMA_REGISTRY_HOST', '127.0.0.1:8081')

        _YAML_CONFIGS = parsing_yaml(os.path.join('./src/core', f'{_YAML_VERSION}', 'factory_config.yaml'))
        _SIMULATE = _YAML_CONFIGS['simulate']
        _LOAD_CFG = _YAML_CONFIGS['load_profile']
        _BATCH_SIZE = _SIMULATE['batch_size']
        _BATCH_INTERVAL = _SIMULATE['batch_interval']

        _SQLITE_DB_NAME = os.getenv('SQLITE_DB_NAME', 'kafka_consumer_local.db')
        self.mach_id = int(os.getenv('MACH_ID', '67'))
        self.mach_name = os.getenv('MACH_NAME', 'M-CNC-30')

        self.status = 'IDLE'
        self.last_status = None
        self.order_id = None
        self.raw_data = None

        # 訂單處理狀態字典 ; value (product_id, target_qty, produced_qty, start_at, end_at)
        self.event_dict = {}

        _MAIN_NAME = f'#{self.mach_name}'

        self.env['CONSUMER_ORDER_TOPIC'] = _CONSUMER_ORDER_TOPIC
        self.env['CONSUMER_GROUP_ID'] = _CONSUMER_GROUP_ID
        self.env['KAFKA_HOST'] = _KAFKA_HOST
        self.env['KAFKA_SCHEMA_REGISTRY_HOST'] = _KAFKA_SCHEMA_REGISTRY_HOST
        self.env['KAFKA_AUTO_OFFSET_RESET'] = _KAFKA_AUTO_OFFSET_RESET
        self.env['KAFKA_ENABLE_AUTO_COMMIT'] = _KAFKA_ENABLE_AUTO_COMMIT
        self.env['SIMULATE'] = _SIMULATE
        self.env['LOAD_CFG'] = _LOAD_CFG
        self.env['BATCH_SIZE'] = _BATCH_SIZE
        self.env['BATCH_INTERVAL'] = _BATCH_INTERVAL
        self.env['SQLITE_DB_NAME'] = _SQLITE_DB_NAME
        self.env['_MAIN_NAME'] = _MAIN_NAME

        logging = Logger(
            console_name=get_logger_name(__file__, GET_PATH_ROOT),
            # file_name=self.mach_name,
            # file_path=f'logs/INSTANCE/{self.mach_name}.logs',
            backup_count=10,
            **{
                'app_name': 'pg',
                'service_type': 'instance',
                'inst_id': self.mach_name,
            }
        )

        self.configure_setting(logging=logging) # TODO 完成 EntryPoint 必要後續初始化
        self._load_configs() # 冗長設定

    
    def _load_configs(self):
        self._init_sqlite()
        self._cleanup_data()
        self._recover_stuck_orders()

        self.mss = MachineStatusSimulator()
        self.kcm = KafkaConsumerManager(
            logging=self.logging,
            log_main_name=self.env['_MAIN_NAME'],
            topic=self.env['CONSUMER_ORDER_TOPIC'],
            topic_key=f'{'/'.join(self.env['CONSUMER_ORDER_TOPIC'].split('.')[1:])}/{self.mach_name}',
            config={
                'bootstrap.servers': self.env['KAFKA_HOST'],
                'group.id': self.env['CONSUMER_GROUP_ID'],
                'auto.offset.reset': self.env['KAFKA_AUTO_OFFSET_RESET'],
                'enable.auto.commit': self.env['KAFKA_ENABLE_AUTO_COMMIT']
            },
        )
        self.kpm = KafkaProducerManager(
            logging=self.logging,
            log_main_name=self.env['_MAIN_NAME'],
            bootstrap_servers=self.env['KAFKA_HOST'],
            sr_url=self.env['KAFKA_SCHEMA_REGISTRY_HOST'],
            schemas_list=[SINK_MACH_STATUS_LOGS, SINK_PROD_ORDERS, SINK_PROD_RECORDS],
        )


    def _init_sqlite(self):
        """初始化資料庫連線 ( +運行參數設置 ) 與建表邏輯"""
        try:
            self.conn = sqlite3.connect(self.env['SQLITE_DB_NAME'], check_same_thread=False)

            # 1. 為了讀寫不互相阻塞： 提升效能，允許讀寫並行
            self.conn.execute('PRAGMA journal_mode=WAL;')

            # 2. 不犧牲安全的前提下極大化寫入速度
            self.conn.execute('PRAGMA synchronous=NORMAL;')

            # 3. 限制 WAL 文件大小，防止硬碟空間被 log 檔案吃光
            # 當 WAL 檔案達到 5MB 時會自動觸發 checkpoint 寫回主文件
            self.conn.execute('PRAGMA wal_autocheckpoint=1000;')

            # 4. 暫存記憶體大小
            # 設定快取大小為 2000 頁 ( 約 8MB )，提升查詢效率
            self.conn.execute('PRAGMA cache_size=2000;')

            # 5. 開啟外鍵約束 ( 假設未來有連集刪除需求 )
            # self.conn.execute('PRAGMA foreign_keys=ON;')

            self.cursor = self.conn.cursor()

            # 執行 DDL
            self.cursor.executescript("""
            CREATE TABLE IF NOT EXISTS sync_orders (
                order_id INTEGER PRIMARY KEY,        -- 訂單唯一性，防止重複消費
                raw_data JSON,                       -- 完整原始數據
                status INTEGER DEFAULT 0,            -- 0:待處理, 1:處理中, 2:已完成
                create_at DATETIME DEFAULT (DATETIME('now', 'localtime')),
                update_at DATETIME DEFAULT (DATETIME('now', 'localtime'))
            );
            
            -- 索引： Thread 查詢「待處理」訂單時速度會極快
            CREATE INDEX IF NOT EXISTS idx_order_status ON sync_orders (status);
            """)
            self.conn.commit()
            self.logging.info('SQLite 初始化完成 ( 表結構已就緒 ) ...')

        except Exception as e:
            self.logging.error('SQLite 初始化失敗', exc_info=True)
            raise e


    def _save_to_sqlite(self, data) -> bool:
        """儲存來自 Consumer 訂單訊息 ( Kafka 恐會重複入庫 所以需要 CONFLICT 防呆 )"""
        try:
            sql = """
            INSERT INTO sync_orders 
            (order_id, raw_data, update_at)
            VALUES (?, ?, DATETIME('now', 'localtime'))
            ON CONFLICT(order_id) DO UPDATE SET
            raw_data = excluded.raw_data,
            update_at = DATETIME('now', 'localtime')
            """
            params = (data['order_id'], json.dumps(data))

            # 當 with 區塊內的程式碼全部成功執行完畢時，自動呼叫 self.conn.commit()
            # => 確保資料被永久寫入硬碟

            # 如果區塊內任何一行 SQL 噴出 Exception (如硬碟滿、JSON 格式錯誤、型別不合)，自動呼叫 self.conn.rollback()
            # => 撤銷該次事務中所有已執行的操作

            # 原子性： 保證資料庫不會出現半成品
            with self.conn:
                self.cursor.execute(sql, params)
            return True

        except Exception as e:
            self.conn.rollback()
            self.logging.error('儲存 Consumer 訂單失敗', exc_info=True)
            return False


    def _recover_stuck_orders(self):
        """將上次執行到一半 (status=1) 的訂單恢復為待處理 (status=0)"""
        try:
            with self.conn:
                self.cursor.execute("""
                    UPDATE sync_orders 
                    SET status=0, update_at=DATETIME('now', 'localtime')
                    WHERE status=1
                """)
                if self.cursor.rowcount > 0:
                    self.logging.info(f'已將 {self.cursor.rowcount} 筆處理中的訂單重置為待處理狀態')

        except Exception as e:
            self.logging.error('修復殘留訂單失敗', exc_info=True)


    def _get_next_pending_order(self):
        """
        1. 優先查詢 status=0 的訂單 => 取出排隊順位第一個訂單，並立即標記為處理中 (status=1)
            # 如果系統健壯，status=1 應該在程式運行時被即時轉換為 2
            # 若程式重啟，status=1 應已透過 _recover_stuck_orders 轉為 0
        2. 採用原子操作，確保不會有同時抓到同一張單
        3. SQL 語句
            # 找到 order_id 最小的待處理訂單(Subquery)
            # 將其狀態改為 1
            # 回傳該筆的所有欄位內容
        """
        sql = """
            UPDATE sync_orders
            SET status=1, update_at=DATETIME('now', 'localtime')
            WHERE order_id = (
                SELECT order_id FROM sync_orders 
                WHERE status=0 
                ORDER BY order_id ASC 
                LIMIT 1
            )
            RETURNING order_id, raw_data;
        """
        try:
            with self.conn:
                self.cursor.execute(sql)
                row = self.cursor.fetchone()

                if row:
                    return {
                        'order_id': row[0],
                        'raw_data': json.loads(row[1])
                    }
                return None  # 代表目前沒有排隊中的訂單

        except Exception as e:
            self.logging.error('獲取下一筆訂單失敗', exc_info=True)
            return None


    def _mark_order_done(self):
        """完成訂單並標記狀態"""
        try:
            with self.conn:
                self.cursor.execute("""
                    UPDATE sync_orders 
                    SET status=2, update_at=DATETIME('now', 'localtime')
                    WHERE order_id = ?
                """, (self.order_id,))

            # 清除當前訂單相關變數
            self.order_id = None
            self.raw_data = None
            self.event_dict = {}

        except Exception as e:
            self.logging.error('更新狀態失敗', exc_info=True)


    def _cleanup_data(self):
        """刪除 7 天前已完成的訂單，保持資料庫輕量"""
        try:
            with self.conn:
                self.cursor.execute("""
                    DELETE FROM sync_orders 
                    WHERE status = 2 
                    AND update_at < DATETIME('now', '-7 days')
                """)
        except Exception as e:
            self.logging.error('清除失敗', exc_info=True)


    def _update_order_status(self, **kwargs) -> int:
        """檢查是否有訂單完成，若完成則更新訂單狀態並從訂單列表移除"""
        ret, _payload = 0, None
        try:
            # 1.1 插入交易日誌 (基本)
            _now_time = get_now(hours=8, tzinfo=TZ_UTC_8)
            timestamp_ms = int(_now_time.timestamp() * 1000)
            _payload = {
                'order_id': self.order_id,
                'machine_id': self.mach_id,
                'product_id': self.raw_data['prod_id'],
                'quantity': self.event_dict['produced_qty'],
                'event_time': timestamp_ms,
            }

            if self.event_dict['produced_qty'] >= self.event_dict['target_qty']:
                # 1.2 校正最大數量值
                _payload['quantity'] = self.event_dict['target_qty']
                self.event_dict['end_at'] = timestamp_ms

                # 2. 更新訂單結束時間
                payload = {
                    'order_id': self.order_id,
                    'start_at': self.event_dict['start_at'],
                    'end_at': self.event_dict['end_at'],
                }
                self.kpm.send_message(topic='inst.prod-orders', key=self.mach_name, payload=payload)
                ret += 1

                # 3. 更新機台狀態 : RUNNING -> IDLE
                self.status = 'IDLE'
                payload = {
                    'machine_id': self.mach_id,
                    'status': self.status,
                    'event_time': self.event_dict['end_at'],
                }
                self.kpm.send_message(topic='inst.status-logs', key=self.mach_name, payload=payload)
                ret += 1

                # TODO 處理完成，標記為已完成 (status=2)
                self._mark_order_done()

                self.logging.notice(f'[order_id={self.order_id}] have been completed. '
                f'( produced_qty: {self.event_dict['produced_qty']} >= target_qty: {self.event_dict['target_qty']} )')

        finally:
            self.kpm.send_message(topic='inst.prod-records', key=self.mach_name, payload=_payload)
            ret += 1
            return ret


    def _insert_production_record(self, efficiency: int, **kwargs) -> int:
        """插入實時生產記錄"""
        ret = 1

        # 1. 確認當前狀態是否正常 否則不進行後續邏輯 ( 可能中突為 Alarm... )
        if self.status != 'RUNNING':
            return 0

        # 2. 根據效率增加生產數量
        for _ in range(efficiency):

            # 3. 隨機生產數
            _quantity = random.randint(
                self.env['SIMULATE']['prod_qty_min'],
                self.env['SIMULATE']['prod_qty_max']
            )

            # TODO 4. 更新事務字典中的訂單計數狀況 + 同時檢查是否完成訂單 ( 非外部迴圈判斷 )
            self.event_dict['produced_qty'] += _quantity
            ret += self._update_order_status()

        return ret


    def _insert_machine_status(self, **kwargs) -> int:
        """
        TODO 插入機台狀態 : 在此實施隨機邏輯，可基於權重機率調整
            - MAINTENANCE # 1 # process: [1 -> 2]
            - IDLE        # 2 # process: [2 -> 1], [2 -> 3]
            - RUNNING     # 3 # process: [3 -> 2], [3 -> 4]
            - ALARM       # 4 # process: [4 -> 3]
        """
        if self.last_status is not None:
            # 1. 實施隨機邏輯
            _status = self.mss.get_next_status(self.status)

            # 2. 直接返回且不更新狀態
            if self.status == _status:
                return 0
            else:
                # 3. 更新當前狀態
                self.status = _status

        self.last_status = self.status

        # 4. 提交狀態更新
        _now_time = get_now(hours=8, tzinfo=TZ_UTC_8)
        timestamp_ms = int(_now_time.timestamp() * 1000)
        payload = {
            'machine_id': self.mach_id,
            'status': self.status,
            'event_time': timestamp_ms,
        }
        self.kpm.send_message(topic='inst.status-logs', key=self.mach_name, payload=payload)
        return 1


    def _order_start(self) -> int:
        """訂單初始狀態賦予"""
        ret = 0
        _now_time = get_now(hours=8, tzinfo=TZ_UTC_8)
        timestamp_ms = int(_now_time.timestamp() * 1000)

        self.status = 'RUNNING'
        self.event_dict = {
            'product_id': self.raw_data['prod_id'],
            'target_qty': self.raw_data['target_qty'],
            'produced_qty': 0,
            'start_at': timestamp_ms,
            'end_at': None,
        }

        payload = {
            'order_id': self.order_id,
            'start_at': self.event_dict['start_at'],
            'end_at': self.event_dict['end_at'],
        }
        self.kpm.send_message(topic='inst.prod-orders', key=self.mach_name, payload=payload)
        ret += 1

        self.logging.info(f'Production Begins Based on the Order [{self.order_id}].')

        # 更新機台狀態 : IDLE -> RUNNING
        payload = {
            'machine_id': self.mach_id,
            'status': self.status,
            'event_time': self.event_dict['start_at'],
        }
        self.kpm.send_message(topic='inst.status-logs', key=self.mach_name, payload=payload)
        ret += 1

        return ret


    def _producer_message(self, **kwargs):
        """生產者配置"""
        batch_ct = 0
        last_commit_time = time.time()
        try:
            while not self._stop_event.is_set():
                try:
                    now = get_now(hours=8, tzinfo=TZ_UTC_8)
                    mode = self.mss.get_load_profile(now.hour)
                    load_setting = self.env['LOAD_CFG'][mode]

                    # TODO 隨機更新指定狀態
                    batch_ct += self._insert_machine_status()

                    # TODO 從資料庫拿取「下一張待處理訂單」
                    if self.order_id is None and self.raw_data is None:
                        current_task = self._get_next_pending_order()

                        if current_task is None:
                            # 當前無訂單 => 空轉
                            time.sleep(1)
                            continue
                        else:
                            # TODO 起始變數更新
                            self.order_id = current_task['order_id']
                            self.raw_data = current_task['raw_data']
                            batch_ct += self._order_start()

                    # TODO 進行判斷狀態更新 + 同時檢查是否完成訂單
                    if self.event_dict != {}:
                        batch_ct += self._insert_production_record(load_setting['efficiency'])

                    # 根據 BATCH_SIZE 或 時間間隔 提交事務
                    if batch_ct >= self.env['BATCH_SIZE'] \
                            or (time.time() - last_commit_time) > self.env['BATCH_INTERVAL']:
                        self.kpm.poll(0)
                        batch_ct = 0
                        last_commit_time = time.time()

                    ret = ''
                    if self.event_dict != {}:
                        ret += f'{self.event_dict['produced_qty']}/{self.event_dict['target_qty']}'

                    # 輸出當前模擬狀態
                    self.logging.info(
                        f'[{self.env['_MAIN_NAME']}] 整體の概要 : '
                        f'MODE={mode} | '
                        f'[PROGRESS #{self.order_id}]=[{ret}] | '
                        f'BATCH=[{batch_ct}/{self.env['BATCH_SIZE']}] | '
                        f'機台の狀態 : {self.status}\n'
                    )

                    time.sleep(1)

                except Exception as e:
                    self.logging.error('[# Other] Exception', exc_info=True)

        finally:
            self.kpm.flush(sec=10)
            self.logging.notice(f'[{self.env['_MAIN_NAME']}] '
                f'已強制將緩衝區中所有尚未發送的訊息傳送到 Kafka Broker ...', stack_level=0)


    def _consumer_message(self, **kwargs):
        """消費者配置"""
        try:
            while not self._stop_event.is_set():
                try:
                    self._stop_event.wait(timeout=0.1)

                    msg = self.kcm.poll(1.0)
                    if msg is None:
                        continue

                    # key = msg.key().decode('utf-8') if msg.key() else 'N/A'
                    data = json.loads(msg.value().decode('utf-8'))

                    if data.get('mach_name') != self.mach_name:
                        continue  # 同 Partition 鄰居資料直接無視

                    try:
                        # self.logging.info(f"[{self.env['_MAIN_NAME']}] 收到來自 {key}: {data}")

                        _status = self._save_to_sqlite(data)
                        if _status:
                            # TODO 資料庫寫入成功 => 提交 Offset
                            self.kcm.commit(asynchronous=False)
                        else:
                            # TODO 資料庫寫入失敗 => 不提交 下次重試
                            self.logging.error('資料庫寫入失敗不提交 ...', exc_info=False)

                    except Exception as e:
                        self.logging.error(f"[{self.env['_MAIN_NAME']}] 消費失敗不提交 ...", exc_info=True)


                except Exception as e:
                    self.logging.error('Exception', exc_info=True)

        finally:
            self.kcm.close()


    def __exit__(self, exc_type, exc_val, exc_tb):
        """上下文管理器 : 結束"""
        self.conn.commit()
        self.logging.warning('已落實最後一次事務提交 ...')
        close_conn(self.conn, self.cursor)
        return False


    def run(self):
        """
        TODO 動作事項
            - 實例 : N
            \
            - MQTT ( Kafka ) : 「消費」/「傳送」訊息
                - 消費 : source.cp.mach-order 訂單訊息
                - 傳送 : ...
            - Offset 儲存：Kafka 根據 Key 紀錄消費數字 ; KEY => ( group.id + Topic + Partition ID )
        """
        self.logging.notice(f'[{self.env['_MAIN_NAME']}] Starting Factory Stream Simulation ...')
        self.start_service(self._consumer_message, **{
            'title': '消費「主控訂單」訊息服務',
        })
        self.start_service(self._producer_message, **{
            'title': '生產「邊緣數據」訊息服務',
        })
        while not self._stop_event.is_set():
            time.sleep(1)


if __name__ == '__main__':
    app = Application()
    app.main()
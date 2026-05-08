# -*- coding: utf-8 -*-
"""
TODO
    Update Date: 2026-05-06
    Description:
    Notice:
        [加入 sqlite 提升 HA 消費訂單事務]:
            - [thread 1] kafka -> consumer -> sqlite ( N 個實例 = N 個 sqlite 實例 ; 用 ELK 監控是否正常消費 )
            - [thread 2] sqlite ( 每次斷掉重啟由此開始 唯一事實 ; 須建立狀態表 ) -> producer -> kafka -> kafka connection sink
"""
import sys, os; sys.path.insert(0, os.getcwd())

from shared.configs import (
    load_dotenv,
)
from shared.configs.constant import *
from shared.utils.tools import *
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

        self.mach_name = os.getenv('TARGET_MACH', 'M-CNC-30')
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
        self.env['_MAIN_NAME'] = _MAIN_NAME

        logging = Logger(
            console_name=get_logger_name(__file__, GET_PATH_ROOT),
            # file_name=self.mach_name,
            # file_path=f'logs/INSTANCE/{self.mach_name}.logs',
            backup_count=10,
            **{
                'app_name': 'ooud',
                'service_type': 'instance',
                'inst_id': self.mach_name,
            }
        )

        self.configure_setting(logging=logging) # TODO 完成 EntryPoint 必要後續初始化
        self._load_configs() # 冗長設定
        
    
    def _load_configs(self):
        self.event_dict = {
            'mach_id': None, # FIXME 應該初始就要有 ID 而非拿值 ; 等 consumer 拿到訂單資訊後，才會有 mach_id 資訊 ; 初始值 None
            'order_dict': {}, # 訂單字典 | key: order_id, value: prod_id
            'order_queue': collections.deque(), # 訂單隊列
            'detail': {},  # 訂單詳情字典 | key: order_id, value: dict (product_id, target_qty, produced_qty)
            # 記錄機台持單狀態
            'machine_status': {
                'status': 'IDLE',
                'order_id': None,  # None / not None
            },
        }

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


    def _update_order_status(self, **kwargs) -> int:
        """
        TODO 檢查是否有訂單完成，若完成則更新訂單狀態並從訂單列表移除
        """
        ret = 0
        try:
            if len(self.event_dict['order_dict'].keys()) > 0:
                _target_list = copy.deepcopy(list(self.event_dict['order_dict'].keys()))
                for _order_id in _target_list:
                    detail = self.event_dict['detail'][_order_id]
                    if detail['produced_qty'] >= detail['target_qty']:
                        _now_time = get_now(hours=8, tzinfo=TZ_UTC_8)
                        timestamp_ms = int(_now_time.timestamp() * 1000)
                        self.event_dict['detail'][_order_id]['end_at'] = timestamp_ms

                        # 1. 更新訂單結束時間
                        payload = {
                            'order_id': _order_id,
                            'start_at': self.event_dict['detail'][_order_id]['start_at'],
                            'end_at': self.event_dict['detail'][_order_id]['end_at'],
                        }
                        self.kpm.send_message(topic='inst.prod-orders', key=self.mach_name, payload=payload)

                        ret += 1

                        # 2. 更新機台狀態 : RUNNING -> IDLE
                        _status = 'IDLE'
                        payload = {
                            'machine_id': self.event_dict['mach_id'],
                            'status': _status,
                            'event_time': timestamp_ms,
                        }
                        self.kpm.send_message(topic='inst.status-logs', key=self.mach_name, payload=payload)

                        ret += 1

                        # 從訂單字典移除
                        del self.event_dict['order_dict'][_order_id]

                        # 同時移除訂單詳情
                        del self.event_dict['detail'][_order_id]

                        # 清空機台持單狀態 + 狀態轉 IDLE
                        self.event_dict['machine_status'] = {
                            'status': 'IDLE',
                            'order_id': None,
                        }

                        self.logging.notice(f'[order_id={_order_id}] have been completed. '
                        f'( produced_qty: {detail['produced_qty']} >= target_qty: {detail['target_qty']} )')

        finally:
            if ret > 0:
                return ret, 1
            else:
                return ret, 0


    def _insert_production_record(self, efficiency: int, **kwargs) -> int:
        """
        TODO 插入實時生產記錄
            - 狀況 1 : 第一次生產匹配
            - 狀況 2 : 持續生產
        """
        ret, _status = 1, None

        # 1. 從指定機型佇列中取出第一順位訂單 (而非隨機挑選) ; 或是持續生產
        if self.event_dict['order_queue'] and self.event_dict['machine_status']['status'] == 'IDLE':
            # 須確認是否已經訂單在身，若無取新訂單
            _data = self.event_dict['order_queue'].popleft()
            _order_id = _data['order_id']
            _status = 'RUNNING'
            self.event_dict['machine_status']['status'] = _status
            self.event_dict['machine_status']['order_id'] = _order_id
            self.event_dict['detail'][_data['order_id']] = {
                'product_id': _data['prod_id'],
                'target_qty': _data['target_qty'],
                'produced_qty': 0
            }

            _now_time = get_now(hours=8, tzinfo=TZ_UTC_8)

            # 1.1 更新訂單開始作業時間
            timestamp_ms = int(_now_time.timestamp() * 1000)
            self.event_dict['detail'][_order_id]['start_at'] = timestamp_ms
            payload = {
                'order_id': _order_id,
                'start_at': self.event_dict['detail'][_order_id]['start_at'],
                'end_at': None,
            }
            self.kpm.send_message(topic='inst.prod-orders', key=self.mach_name, payload=payload)

            ret += 1
            self.logging.info(f'Production Begins Based on the Order [{_order_id}].')


            # 1.2 更新機台狀態 : IDLE -> RUNNING
            payload = {
                'machine_id': self.event_dict['mach_id'],
                'status': _status,
                'event_time': timestamp_ms,
            }
            self.kpm.send_message(topic='inst.status-logs', key=self.mach_name, payload=payload)

            ret += 1

        elif self.event_dict['machine_status']['order_id'] is not None:
            # 須確認是否已經訂單在身，若有先完成既有訂單
            _order_id = self.event_dict['machine_status']['order_id']
            _status = self.event_dict['machine_status']['status']

        else:
            return

        if _status != 'RUNNING':
            return 0

        # 2. 用訂單 ID 取得產品 ID
        _product_id = self.event_dict['order_dict'].get(_order_id).get('prod_id')

        # 3. 根據效率增加生產數量
        for _ in range(efficiency):

            # 4. 隨機生產數
            _quantity = random.randint(self.env['SIMULATE']['prod_qty_min'], self.env['SIMULATE']['prod_qty_max'])

            _now_time = get_now(hours=8, tzinfo=TZ_UTC_8)
            timestamp_ms = int(_now_time.timestamp() * 1000)

            # 5. 更新事務字典中的訂單計數狀況
            self.event_dict['detail'][_order_id]['produced_qty'] += _quantity

            # 6. 插入交易日誌
            payload = {
                'order_id': _order_id,
                'machine_id': self.event_dict['mach_id'],
                'product_id': _product_id,
                'quantity': self.event_dict['detail'][_order_id]['produced_qty'],
                'event_time': timestamp_ms,
            }
            self.kpm.send_message(topic='inst.prod-records', key=self.mach_name, payload=payload)
            ret += 1

        return ret


    def _insert_machine_status(self, **kwargs) -> int:
        """
        TODO 插入機台狀態 : 在此實施隨機邏輯，可基於權重機率調整
            - MAINTENANCE # 1 # process: [1 -> 2]
            - IDLE        # 2 # process: [2 -> 1], [2 -> 3]
            - RUNNING     # 3 # process: [3 -> 2], [3 -> 4]
            - ALARM       # 4 # process: [4 -> 3]
        """
        _status = None

        # 1. 取得當前狀態
        _event_status = self.event_dict['machine_status']['status']

        # 直接返回且不更新狀態
        if _event_status is None:
            return 0

        # 2. 實施隨機邏輯
        _status = self.mss.get_next_status(_event_status)

        # 直接返回且不更新狀態
        if _event_status == _status:
            return 0

        # 3. 更新當前狀態
        self.event_dict['machine_status']['status'] = _status

        _now_time = get_now(hours=8, tzinfo=TZ_UTC_8)
        timestamp_ms = int(_now_time.timestamp() * 1000)

        # 4. 提交狀態更新
        payload = {
            'machine_id': self.event_dict['mach_id'],
            'status': _status,
            'event_time': timestamp_ms,
        }
        self.kpm.send_message(topic='inst.status-logs', key=self.mach_name, payload=payload)
        return 1


    def _producer_message(self, **kwargs):
        """
        TODO 生產者配置
        """
        batch_ct, done_qty = 0, 0
        last_commit_time = time.time()
        try:
            while not self._stop_event.is_set():
                try:
                    now = get_now(hours=8, tzinfo=TZ_UTC_8)
                    mode = self.mss.get_load_profile(now.hour)
                    load_setting = self.env['LOAD_CFG'][mode]

                    efficiency = load_setting['efficiency']

                    if self.event_dict['mach_id'] is not None:
                        # TODO 進行判斷狀態更新
                        _ct = self._insert_production_record(efficiency)
                        if isinstance(_ct, int):
                            batch_ct = batch_ct + _ct

                        # TODO 隨機更新指定狀態
                        _ct = self._insert_machine_status()
                        batch_ct = batch_ct + _ct

                        _ct, _ct_2 = self._update_order_status()
                        done_qty, batch_ct = done_qty + _ct_2, batch_ct + _ct

                        # TODO 根據 BATCH_SIZE 或 時間間隔 提交事務
                        if batch_ct >= self.env['BATCH_SIZE'] or (time.time() - last_commit_time) > self.env['BATCH_INTERVAL']:
                            self.kpm.poll(0)
                            batch_ct = 0
                            last_commit_time = time.time()

                        # TODO 輸出當前模擬狀態
                        ret = ''
                        _order_id = self.event_dict['machine_status']['order_id']
                        if _order_id is not None:
                            _detail = self.event_dict['detail'][_order_id]
                            ret += f'{_detail['produced_qty']}/{_detail['target_qty']}'
                        self.logging.info(
                            f'[{self.env['_MAIN_NAME']}] 整體の概要 : '
                            f'MODE={mode} | '
                            f'訂單完成={done_qty}\n'
                            f'[PROGRESS #{_order_id}]=[{ret}] | '
                            f'BATCH=[{batch_ct}/{self.env['BATCH_SIZE']}] | '
                            f'機台の狀態 : {self.event_dict['machine_status']['status']} | '
                            f'排隊の訂單 : {len(self.event_dict['order_queue'])}\n'
                        )

                    time.sleep(1)

                except Exception as e:
                    self.logging.error('[# Other] Exception', exc_info=True)

        finally:
            self.kpm.flush(sec=10)
            self.logging.notice(f'[{self.env['_MAIN_NAME']}] '
                f'已強制將緩衝區中所有尚未發送的訊息傳送到 Kafka Broker ...', stack_level=0)


    def _consumer_message(self, **kwargs):
        """
        TODO 消費者配置
        """
        try:
            while not self._stop_event.is_set():
                try:
                    self._stop_event.wait(timeout=0.1)

                    msg = self.kcm.poll(1.0)
                    if msg is None:
                        continue

                    key = msg.key().decode('utf-8') if msg.key() else 'N/A'
                    data = json.loads(msg.value().decode('utf-8'))

                    if data.get('mach_name') != self.mach_name:
                        continue  # 同 Partition 鄰居資料直接無視

                    # TODO 處理業務邏輯
                    try:
                        # self.logging.info(f"[{self.env['_MAIN_NAME']}] 收到來自 {key}: {data}")
                        self.event_dict['mach_id'] = data['mach_id']
                        self.event_dict['order_queue'] += [data]
                        self.event_dict['order_dict'][data['order_id']] = data

                        self.kcm.commit(asynchronous=False) # TODO 處理成功，手動提交 Offset

                    except Exception as e:
                        self.logging.error(f"[{self.env['_MAIN_NAME']}] 消費失敗不提交，下次從 offset 繼續開始 ...", exc_info=True)


                except Exception as e:
                    self.logging.error('Exception', exc_info=True)

        finally:
            self.kcm.close()


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
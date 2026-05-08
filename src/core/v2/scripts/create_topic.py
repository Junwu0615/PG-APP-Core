# -*- coding: utf-8 -*-
import sys, os; sys.path.insert(0, os.getcwd())

from shared.configs import os, time, json, load_dotenv
from shared.utils.tools import *
from shared.utils.env_config import GET_PATH_ROOT, get_logger_name
from shared.modules.log import Logger
from confluent_kafka.admin import (
    AdminClient,
    NewTopic,
    NewPartitions,
    ConfigResource,
    ResourceType,
)

console_name = get_logger_name(__file__, GET_PATH_ROOT)
logging = Logger(console_name=console_name, is_logstash=False)
load_dotenv(dotenv_path=f'{'/'.join(__file__.split('/')[:-1])}/.env')
KAFKA_HOST = os.getenv('KAFKA_HOST', '127.0.0.1:9092')
JSON_PATH = os.path.join('./src/core', f'{os.getenv('YAML_VERSION', 'v2')}', 'scripts', 'topics_config.json')


def sync_kafka_infrastructure(config_file):
    with open(config_file, 'r') as f:
        target_cfg = json.load(f)

    admin_client = AdminClient({'bootstrap.servers': KAFKA_HOST})


    # 1. 取得目前 Kafka 存在的 Topic
    try:
        current_topics = admin_client.list_topics(timeout=10).topics
    except Exception as e:
        logging.error(f"無法連線至 Kafka Broker", exc_info=True)
        return


    for t_cfg in target_cfg['topics']:
        topic_name = t_cfg['name']
        try:
            # TODO 情況 A: Topic 不存在 -> 建立
            if topic_name not in current_topics:
                logging.info(f"偵測到新管道，準備建立: {topic_name}")
                new_topic = NewTopic(
                    topic=topic_name,
                    num_partitions=t_cfg['partitions'],
                    replication_factor=t_cfg['replication_factor'],
                    config=t_cfg.get('configs', {})
                )
                fs = admin_client.create_topics([new_topic])
                fs[topic_name].result()  # 等待建立完成
                logging.notice(f"管道 {topic_name} 建立成功")

            # TODO 情況 B: Topic 已存在 -> 檢查並對齊狀態
            else:
                metadata = current_topics[topic_name]

                # 2. 檢查 Partition 數量
                current_p_count = len(metadata.partitions)
                target_p_count = t_cfg['partitions']

                if target_p_count > current_p_count:
                    logging.warning(f"管道 {topic_name} 分區不足: {current_p_count} -> {target_p_count}")
                    new_parts = [NewPartitions(topic_name, target_p_count)]
                    fs = admin_client.create_partitions(new_parts)
                    fs[topic_name].result()
                    logging.notice(f"管道 {topic_name} 分區擴展完成")
                elif target_p_count < current_p_count:
                    logging.error(f"拒絕縮減管道 {topic_name} 分區 (Kafka 不支援縮減分區)", exc_info=False)

                # 3. 檢查 Configs (對齊 Retention, Compression 等)
                resource = [ConfigResource(ResourceType.TOPIC, topic_name)]
                desc_fs = admin_client.describe_configs(resource)
                current_configs = desc_fs[resource[0]].result()

                # 比對需要更新的配置
                target_configs = t_cfg.get('configs', {})
                updates = {}
                for key, val in target_configs.items():

                    # 只有當目前值不等於預期值時才加入更新清單
                    if key not in current_configs or current_configs[key].value != str(val):
                        updates[key] = str(val)

                if updates:
                    logging.info(f'管道 {topic_name} 配置不一致，準備同步: {updates}')
                    new_resource = ConfigResource(ResourceType.TOPIC, topic_name, set_config=updates)

                    # 注意: alter_configs 在新版建議使用 incremental_alter_configs，此處使用標準 alter
                    fs = admin_client.alter_configs([new_resource])

                    fs[new_resource].result()
                    logging.notice(f'管道 {topic_name} 配置同步成功')

                else:
                    logging.info(f'管道 {topic_name} 已對齊，無需更動')

        except Exception as e:
            logging.error(f'處理管道 {topic_name} 時發生異常', exc_info=True)


if __name__ == '__main__':
    try:
        logging.notice('開始 Kafka 基礎建設同步腳本')
        time.sleep(1)
        sync_kafka_infrastructure(JSON_PATH)

    except Exception as e:
        logging.error('Exception', exc_info=True)

    finally:
        sys.exit(0)
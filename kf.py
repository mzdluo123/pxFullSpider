from kafka import KafkaProducer
from kafka import KafkaConsumer
from conf import CONF
import json

TOPIC = "PixivImgProcess"
GROUP = "PixivDownload"

consumer = KafkaConsumer(TOPIC, group_id=GROUP, bootstrap_servers=[CONF.KAFKA_SERVER],
                         value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                         auto_offset_reset='latest',enable_auto_commit=False)
producer = KafkaProducer(bootstrap_servers=[CONF.KAFKA_SERVER],
                         value_serializer=lambda m: json.dumps(m).encode('utf-8'))

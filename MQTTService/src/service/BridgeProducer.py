import json
import os
from datetime import datetime
from kafka import KafkaProducer

class BridgeProducer:
    def __init__(self):
        self.__producer = KafkaProducer(
            bootstrap_servers=[os.getenv("KAFKA_SERVER"),os.getenv("KAFKA_SERVER_2"),os.getenv("KAFKA_SERVER_3")],
            client_id="MQTT Kafka Bridge",
            value_serializer=lambda x:json.dumps(x).encode('utf-8'),
            acks=1,
        )

    def send_message(self,kafka_topic,payload,mqtt_topic):
        data={'body' : {'topic':mqtt_topic,'content': payload}}
        json.dumps(data).encode('utf-8')
        print(data)
        self.__producer.send(kafka_topic, value=(data))

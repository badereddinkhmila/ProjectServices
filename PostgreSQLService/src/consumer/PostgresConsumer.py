import json
import os
from datetime import datetime
from kafka import KafkaConsumer
from src.consumer.PostgreSQLHandler import PostgreSQLHandler


class PostgresConsumer:
    def __init__(self, pg_handler: PostgreSQLHandler):
        servers=[os.getenv("KAFKA_SERVER"), os.getenv("KAFKA_SERVER_2"),os.getenv("KAFKA_SERVER_3")]
        #servers=[os.getenv("KAFKA_SERVER")]
        self.pg_handler=pg_handler
        self.__consumer = KafkaConsumer(
            bootstrap_servers=servers,
            client_id="PostgreSQL Client",
            group_id="PostgreSQL Client",
            value_deserializer= lambda x: json.loads(x.decode('utf-8')),
            enable_auto_commit=True)
        """self.producer = KafkaProducer(
            bootstrap_servers=servers,
            client_id="Cassandra Client",
            value_serializer=lambda x:json.dumps(x).encode('utf-8'),
            acks=1,
            )"""
    def save_to_postgresql(self,msg):
        body=msg['body']
        func_dict = {
            os.getenv("MQTT_TOPIC_GLUCOSE") : self.pg_handler.InsertGlucose,
            os.getenv("MQTT_TOPIC_OXYGEN") : self.pg_handler.InsertOxygen,
            os.getenv("MQTT_TOPIC_BLOODPRESSURE") : self.pg_handler.InsertBloodPressure,
            os.getenv("MQTT_TOPIC_TEMPERATURE") : self.pg_handler.InsertTemperature,
            os.getenv("MQTT_TOPIC_WEIGHT") : self.pg_handler.InsertWeight,
        }
        func_dict[body['topic']](body['content'])

    def consumes(self):
        func_dict = {os.getenv("KAFKA_TOPIC_IOT") : self.save_to_postgresql}
        try:
            self.__consumer.subscribe([os.getenv("KAFKA_TOPIC_IOT")])
        except:
            print('unable to subscribe to topics')
        self.__consumer.poll()
        for message in self.__consumer:
            if message is None:
                continue
            func_dict[message.topic](message.value)

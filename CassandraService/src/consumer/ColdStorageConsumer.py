import json
import os
from datetime import datetime
from kafka import KafkaConsumer, KafkaProducer

from src.consumer.CassandraHandler import CassandraHandler
from cassandra.cluster import Cluster


class ColdStorageConsumer:
    def __init__(self, cassandra_handler: CassandraHandler):
        servers=[os.getenv("KAFKA_SERVER"), os.getenv("KAFKA_SERVER_2"),os.getenv("KAFKA_SERVER_3")]
        #servers=[os.getenv("KAFKA_SERVER")]
        self.cassandra_handler=cassandra_handler
        self.__consumer = KafkaConsumer(
            bootstrap_servers=servers,
            client_id="Cassandra Client",
            group_id="Cassandra Client",
            value_deserializer= lambda x: json.loads(x.decode('utf-8')),
            enable_auto_commit=True,
            reconnect_backoff_ms=3000)
        self.producer = KafkaProducer(
            bootstrap_servers=servers,
            client_id="Cassandra Client",
            value_serializer=lambda x:json.dumps(x).encode('utf-8'),
            acks=1,
            )

    def process_coldstorage_req(self,msg):
        try:
       		body=json.loads(msg['body'])
       		content=body['content']
       		print(content)
       		gateway=content['gateway_id']
       		start=(content['start'])['date']
       		end=(content['end'])['date']
       		my_resp = self.cassandra_handler.ReadColdStorageData(gateway,start,end,'context')
       		data={'body' : {'type':'cold_storage', 'content': my_resp, 'gateway': gateway }}
       		print(data)
       		json.dumps(data['body']).encode('utf-8')
       		self.producer.send('coldstorage', value=(data))
        except:
            print('A sended response is consumed') 
            
    def process_error_values_msg(self,msg):
        try:
            self.cassandra_handler.InsertError(msg)
        except Exception:
            print("ERROR: ",Exception)
    
    def save_to_cassandra(self,msg):
        body=msg['body']
        func_dict = { 
            os.getenv("MQTT_TOPIC_GLUCOSE") : self.cassandra_handler.InsertGlucose,
            os.getenv("MQTT_TOPIC_OXYGEN") : self.cassandra_handler.InsertOxygen,
            os.getenv("MQTT_TOPIC_BLOODPRESSURE") : self.cassandra_handler.InsertBloodPressure,
            os.getenv("MQTT_TOPIC_TEMPERATURE") : self.cassandra_handler.InsertTemperature,
            os.getenv("MQTT_TOPIC_WEIGHT") : self.cassandra_handler.InsertWeight,
            os.getenv("MQTT_TOPIC_ERRORS") : self.cassandra_handler.InsertError,
        }
        
        func_dict[body['topic']](body['content']) 

    def consumes(self):
        func_dict = { os.getenv("KAFKA_TOPIC_SYMFONY") : self.process_coldstorage_req,
                os.getenv("KAFKA_TOPIC_CASSANDRA") : self.save_to_cassandra,
                os.getenv("KAFKA_TOPIC_ERRORS") : self.process_error_values_msg} 
        try:
            self.__consumer.subscribe([os.getenv("KAFKA_TOPIC_SYMFONY"),os.getenv("KAFKA_TOPIC_CASSANDRA"),os.getenv("KAFKA_TOPIC_ERRORS")])
        except:
            print('unable to subscribe to topics')
        
        for message in self.__consumer:
            print(message)
            if message is None:
                continue
            func_dict[message.topic](message.value) 

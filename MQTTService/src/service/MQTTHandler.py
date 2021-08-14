import json
import os
import paho.mqtt.client as paho
from datetime import datetime
from kafka import KafkaProducer
from src.service.BridgeProducer import BridgeProducer

class MQTTHandler:
    active_instance = None
    def __init__(self,bridge_producer):
        #### important ####
        MQTTHandler.active_instance = self
        #### Simple ####
        self.bridge_producer = bridge_producer
        self.mqtt_client=paho.Client(client_id="Kafka_Client", protocol=paho.MQTTv31)
        self.mqtt_client.on_connect = MQTTHandler.on_connect
        self.mqtt_client.on_subscribe = MQTTHandler.on_subscribe
        self.mqtt_client.on_message = MQTTHandler.on_message
        self.mqtt_client.connect(os.getenv("MQTT_LOAD_BALANCER"),1883,60)


    
    @staticmethod
    def on_connect(client, userdata, flags, rc):
        print("Result from connect: {}".format(
        paho.connack_string(rc)))
        if rc == paho.CONNACK_ACCEPTED:
            client.subscribe([(os.getenv("MQTT_TOPIC_TEMPERATURE"), 2), (os.getenv("MQTT_TOPIC_GLUCOSE"), 2),
                              (os.getenv("MQTT_TOPIC_OXYGEN"), 2),(os.getenv("MQTT_TOPIC_BLOOD_PRESSURE"), 2),
                              (os.getenv("MQTT_TOPIC_WEIGHT"), 2),(os.getenv("MQTT_TOPIC_ERRORS"), 0)
                            ])

    @staticmethod
    def on_subscribe(client, userdata, mid, granted_qos):
        print("I've subscribed with QoS: {}".format(granted_qos[0]))
    
    @staticmethod
    def on_message(client, userdata, msg):
        producer = MQTTHandler.active_instance.bridge_producer
        topics=[os.getenv("MQTT_TOPIC_TEMPERATURE"),
                os.getenv("MQTT_TOPIC_GLUCOSE"),
                os.getenv("MQTT_TOPIC_OXYGEN"),
                os.getenv("MQTT_TOPIC_BLOOD_PRESSURE"),
                os.getenv("MQTT_TOPIC_WEIGHT")]

        message=json.loads(msg.payload)
        if msg.topic in topics:
            producer.send_message(os.getenv("KAFKA_TOPIC_CASSANDRA"),message,msg.topic)            
        
        if msg.topic in [os.getenv("MQTT_TOPIC_ERRORS")]:
            producer.send_message(os.getenv("KAFKA_TOPIC_ERRORS"),message,msg.topic)
             
                 
    def process_incoming_messages(self):
        self.mqtt_client.loop()
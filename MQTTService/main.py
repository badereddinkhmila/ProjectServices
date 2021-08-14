import configparser
import os

from src.service.MQTTHandler import MQTTHandler
from src.service.BridgeProducer import BridgeProducer

if __name__ == "__main__":
    configuration = configparser.ConfigParser()
    configuration.read("./application.ini")

    environment: str = os.getenv("ENVIRONMENT", "DEV")
    print(configuration.sections())
    for key, value in configuration[environment].items():
        if not os.getenv(key.upper(), None):
            os.environ[key.upper()] = str(value)
    bridge_producer=BridgeProducer()
    mqtt_handler = MQTTHandler(bridge_producer)
    
    while True:
        mqtt_handler.process_incoming_messages()
        




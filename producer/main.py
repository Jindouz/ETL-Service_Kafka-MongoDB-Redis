"""
Produces random events and sends them to a Kafka topic.
"""
import json
import logging
import time
import yaml
from confluent_kafka import Producer
from modules.event_factory import EventFactory


logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class EventProducer:
    """
    This class generates and sends random events to a Kafka topic.
    """
    def __init__(self, config_path='config.yaml'):
        with open(config_path, 'r', encoding='utf-8') as f:
            self.config = yaml.safe_load(f)
        self.producer = Producer({'bootstrap.servers': self.config['kafka']['bootstrap_servers']})
        self.topic = self.config['kafka']['topic']
        self.sleep_time = self.config['producer']['sleep_time']
        self.event_factory = EventFactory(config_path)

    def produce_event(self):
        """
        Continuously generates and sends events to the Kafka topic.
        """
        while True:
            # Use the event factory to create a random event (output is a dictionary)
            event = self.event_factory.create_event()
            # Serialize the event into JSON (as string pairs) and send it to the Kafka topic 
            # (in binary format) using the confluent_kafka Driver.
            self.producer.produce(self.topic, json.dumps(event))
            logging.info(f"Event with reporterId[{event['reporterId']}] sent to topic {self.topic}")
            time.sleep(self.sleep_time)

if __name__ == "__main__":
    event_producer = EventProducer()
    event_producer.produce_event()

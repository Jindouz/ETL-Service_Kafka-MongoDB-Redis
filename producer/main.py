""" 
Produces random events and sends them to a Kafka topic.
"""
import json
import random
import time
from datetime import datetime
import pytz
import yaml
from confluent_kafka import Producer


class EventProducer:
    """
    This class generates and sends random events to a Kafka topic.
    """
    def __init__(self, config_path='config.yaml'):
        with open(config_path, 'r', encoding='utf-8') as f:
            config = yaml.safe_load(f)
        self.producer = Producer({'bootstrap.servers': config['kafka']['bootstrap_servers']})
        self.topic = config['kafka']['topic']
        self.reporter_id = config['reporter']['start_id']
        self.increment = config['reporter']['increment']

    def delivery_report(self, err, msg):
        """
        Callback function for delivery reports from the Kafka producer.
        """
        if err is not None:
            print(f"Message delivery failed: {err}")
        else:
            print(f"Message delivered to {msg.topic()} [{msg.partition()}]")

    def produce_event(self):
        """
        Continuously generates and sends random events to the Kafka topic.
        """
        while True:
            event = {
                "reporterId": self.reporter_id,
                "timestamp": datetime.now(pytz.utc).isoformat(),
                "metricId": random.randint(1, 10),
                "metricValue": random.randint(1, 100),
                "message": "Hello World"
            }
            self.producer.produce(self.topic, json.dumps(event), callback=self.delivery_report)
            self.producer.poll(1)
            self.reporter_id += self.increment
            time.sleep(1)


if __name__ == "__main__":
    event_producer = EventProducer()
    event_producer.produce_event()

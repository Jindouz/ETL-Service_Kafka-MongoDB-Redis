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
        self.reporter_id = config['producer']['start_id']
        self.increment = config['producer']['increment']
        self.message = config['producer']['message']
        self.prod_poll = config['producer']['prod_poll']
        self.sleep_time = config['producer']['sleep_time']
        self.metricValue_min = config['producer']['metricValue_min']
        self.metricValue_max = config['producer']['metricValue_max']
        self.metricId_min = config['producer']['metricId_min']
        self.metricId_max = config['producer']['metricId_max']

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
                "metricId": random.randint(self.metricId_min, self.metricId_max),
                "metricValue": random.randint(self.metricValue_min, self.metricValue_max),
                "message": self.message
            }
            self.producer.produce(self.topic, json.dumps(event), callback=self.delivery_report)
            self.producer.poll(self.prod_poll) # Wait for pending messages to be delivered before sending a new one
            self.reporter_id += self.increment
            time.sleep(self.sleep_time)


if __name__ == "__main__":
    event_producer = EventProducer()
    event_producer.produce_event()

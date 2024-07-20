""" 
Consumes events from a Kafka topic, transforms them, and stores them in MongoDB.
"""

import json
from datetime import datetime
from confluent_kafka import Consumer, KafkaException
from pymongo import MongoClient
import yaml

class EventConsumer:
    """
    Consumes Kafka events, parses them, and inserts them into a MongoDB collection.
    """
    def __init__(self, config_path='config.yaml'):
        """
        Initializes the consumer with configuration, connects to Kafka and MongoDB.
        """
        with open(config_path, 'r', encoding='utf-8') as f:
            config = yaml.safe_load(f)
        self.consumer = Consumer({
            'bootstrap.servers': config['kafka']['bootstrap_servers'],
            'group.id': config['kafka']['group_id'],
            'auto.offset.reset': 'earliest'
        })
        self.topic = config['kafka']['topic']
        self.consumer.subscribe([self.topic])
        self.poll_timeout = config['consumer']['poll_timeout']

        # MongoDB setup
        self.mongo_client = MongoClient(config['mongo']['uri'])
        self.mongo_db = self.mongo_client[config['mongo']['database']]
        self.mongo_collection = self.mongo_db[config['mongo']['collection']]

    def consume_events(self):
        """
        Continuously polls for messages from the Kafka topic and writes them to MongoDB.
        """
        try:
            while True:
                msg = self.consumer.poll(timeout=self.poll_timeout) # message polling at the specified frequency
                if msg is None:
                    continue
                if msg.error():
                    raise KafkaException(msg.error())

                event = json.loads(msg.value().decode('utf-8'))
                event['timestamp'] = datetime.fromisoformat(event['timestamp'])
                self.mongo_collection.insert_one(event)
                print(f"Consumed event from topic {msg.topic()}: {event}")

        except KeyboardInterrupt:
            pass
        finally:
            self.consumer.close()


if __name__ == "__main__":
    event_consumer = EventConsumer()
    event_consumer.consume_events()

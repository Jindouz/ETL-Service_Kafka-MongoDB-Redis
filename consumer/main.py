import json
from confluent_kafka import Consumer, KafkaException
from pymongo import MongoClient
import yaml
from datetime import datetime
import pytz

class EventConsumer:
    def __init__(self, config_path='config.yaml'):
        with open(config_path, 'r') as f:
            config = yaml.safe_load(f)
        self.consumer = Consumer({
            'bootstrap.servers': config['kafka']['bootstrap_servers'],
            'group.id': config['kafka']['group_id'],
            'auto.offset.reset': 'earliest'
        })
        self.topic = config['kafka']['topic']
        self.consumer.subscribe([self.topic])

        # MongoDB setup
        self.mongo_client = MongoClient(config['mongo']['uri'])
        self.mongo_db = self.mongo_client[config['mongo']['database']]
        self.mongo_collection = self.mongo_db[config['mongo']['collection']]

    def consume_events(self):
        try:
            while True:
                msg = self.consumer.poll(timeout=1.0)
                if msg is None:
                    continue
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        continue
                    else:
                        raise KafkaException(msg.error())
                else:
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

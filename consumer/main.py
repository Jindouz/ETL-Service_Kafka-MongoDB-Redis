""" 
Consumes events from a Kafka topic, transforms them, and stores them in MongoDB.
"""

import json
from datetime import datetime
import logging
from confluent_kafka import Consumer, KafkaException
from pymongo import MongoClient
import yaml

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class EventConsumer:
    """
    Consumes queued up Kafka events, parses them, and inserts them into a MongoDB collection.
    """
    def __init__(self, config_path='config.yaml'):
        """
        Initializes the consumer with configuration, connects to Kafka and MongoDB.
        """
        with open(config_path, 'r', encoding='utf-8') as f:
            config = yaml.safe_load(f)
        
        # Kafka setup
        self.consumer = Consumer({
            'bootstrap.servers': config['kafka']['bootstrap_servers'],
            'group.id': config['kafka']['group_id'], # The group ID is used for Consumer identification and offset tracking.

            # Tracks the last message processed, and starts from there like a bookmark (earliest = read from beginning/bookmark)
            'auto.offset.reset': config['kafka']['offset_reset'] # Set to 'earliest' as default (can also be realtime if 'latest')
            # An offset allows the consumer to continue reading from where it left off in case it crashes or is restarted.
        })
        self.topic = config['kafka']['topic']
        self.consumer.subscribe([self.topic])

        # MongoDB setup
        self.mongo_client = MongoClient(config['mongo']['uri'])
        self.mongo_db = self.mongo_client[config['mongo']['database']]
        self.mongo_collection = self.mongo_db[config['mongo']['collection']]

        self.poll_timeout = config['consumer']['poll_timeout']
        
    def consume_events(self):
        """
        Continuously polls for messages from the Kafka topic and writes them to MongoDB.
        """
        try:
            while True:
                # Consumes a message, checks for errors and return events (which are JSON strings) and sets an offset automatically
                msg = self.consumer.poll(timeout=self.poll_timeout) # Check for messages and errors for 1 second (blocking call)
                if msg is None:
                    continue # If no message, skips to the next loop iteration
                if msg.error():
                    raise KafkaException(msg.error())
                
                # Deserializes the event message (from JSON string) into a Python dictionary
                event = json.loads(msg.value().decode('utf-8'))
                # Makes sure that the timestamp string is in the correct format for MongoDB (datetime object in ISO 8601)
                event['timestamp'] = datetime.fromisoformat(event['timestamp'])

                # Inserts the event into the MongoDB collection as BSON (binary JSON) using the PyMongo Driver
                self.mongo_collection.insert_one(event)
                logging.info("Consumed event from topic %s: %s", msg.topic(), event)

        except KeyboardInterrupt:
            pass
        finally:
            self.consumer.close()


if __name__ == "__main__":
    event_consumer = EventConsumer()
    event_consumer.consume_events()

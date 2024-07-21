""" 
Extracts data from MongoDB, transforms it, and loads it to Redis (ETL process) 
"""
import json
import time
import threading
import logging
from datetime import datetime
from pymongo import MongoClient
import redis
import yaml

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class MongoToRedisETL:
    """
    Fetches data from MongoDB, transforms it, and stores it in Redis.
    """
    def __init__(self, config_path='config.yaml'):
        """
        Initializes the ETL service with MongoDB and Redis connections, and configuration.
        """
        with open(config_path, 'r', encoding='utf-8') as f:
            config = yaml.safe_load(f)

            # MongoDB setup
            self.mongo_client = MongoClient(config['mongo']['uri'])
            self.mongo_db = self.mongo_client[config['mongo']['database']]
            self.mongo_collection = self.mongo_db[config['mongo']['collection']]

            # Redis setup
            self.redis_client = redis.StrictRedis(
                host=config['redis']['host'],
                port=config['redis']['port'],
                decode_responses=True
            )
            self.redis_key = config['redis']['last_timestamp_key']
            self.max_entries = config['etl']['max_entries']
            self.max_entries_gt = config['etl']['max_entries_gt']
            self.sleep_time = config['etl']['sleep_time']
            self.event_count = config['etl']['event_count']
            self.event_count_incr = config['etl']['event_count_incr']


    def get_last_timestamp(self):
        """
        Get the last processed timestamp (KEY:last_timestamp) from Redis (if it exists)
        """
        last_timestamp = self.redis_client.get(self.redis_key)
        if last_timestamp:
            return datetime.strptime(last_timestamp, '%Y/%m/%d_%H:%M:%S')
        return None  # If no key, return None


    def extract_new_events(self, last_timestamp):
        """
        Extracts new events from MongoDB based on the last processed timestamp.
        """
        if not last_timestamp:  # If there is no last timestamp (initial run or Redis wiped)
            # Find every entry {} and fetch data limited to max_entries
            query = {}
            return self.mongo_collection.find(query).limit(self.max_entries)
        
        # Fetch Mongo entries with timestamp that is greater than the last processed timestamp
        query = {'timestamp': {'$gt': last_timestamp}}
        # Fetch entries from last_timestamp up to max_entries_gt
        new_events = self.mongo_collection.find(query).limit(self.max_entries_gt)
        return new_events


    def transform_event(self, event):
        """
        Transforms a MongoDB event document for storage in Redis.
        """
        event['_id'] = str(event['_id'])
        #strftime is used to convert datetime object to string
        event['timestamp'] = event['timestamp'].strftime('%Y/%m/%d_%H:%M:%S')
        reporter_id = event['reporterId']
        timestamp = event['timestamp']
        key = f"{reporter_id}:{timestamp}"
        value = json.dumps(event)
        return key, value


    def run_etl(self):
        """
        Continuously extracts, transforms, and loads data from MongoDB to Redis.
        """
        while True:
            last_timestamp = self.get_last_timestamp()
            # Extract
            new_events = self.extract_new_events(last_timestamp)
            event_count = self.event_count  # logger event count
            key_value_pairs = {}  # dictionary to hold all key-value pairs for MSET
            for event in new_events:
                # Transform
                key, value = self.transform_event(event)
                key_value_pairs[key] = value
                # Update last timestamp for future runs
                last_event_timestamp = event['timestamp']
                event_count += self.event_count_incr  # Increment logger event counter
            # Load all key-value pairs to Redis atomically
            if key_value_pairs:
                key_value_pairs[self.redis_key] = last_event_timestamp
                self.redis_client.mset(key_value_pairs)
            # Logs the amount of documents pulled per loop
            logging.info("Documents pulled in this loop: %s", event_count)
            time.sleep(self.sleep_time)


if __name__ == "__main__":
    etl = MongoToRedisETL()
    #threading allows the program to run multiple processes at the same time
    etl_thread = threading.Thread(target=etl.run_etl)
    etl_thread.start()

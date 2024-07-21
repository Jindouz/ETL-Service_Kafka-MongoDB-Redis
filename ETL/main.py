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
            self.sleep_time = config['etl']['sleep_time']


    def get_last_timestamp(self):
        """
        Get the last processed timestamp (KEY:last_timestamp) from Redis (if it exists)
        """
        last_timestamp = self.redis_client.get(self.redis_key)
        if last_timestamp:
            return datetime.strptime(last_timestamp, '%Y/%m/%d_%H:%M:%S')
        return None  # If no key, return None


    def set_last_timestamp(self, timestamp):
        """
        Sets (and updates) the last processed timestamp in Redis.
        """
        if isinstance(timestamp, str):
            # if timestamp is a string, convert it to datetime object
            timestamp = datetime.strptime(timestamp, '%Y/%m/%d_%H:%M:%S')
        self.redis_client.set(self.redis_key, timestamp.strftime('%Y/%m/%d_%H:%M:%S'))


    def extract_new_events(self, last_timestamp):
        """
        Extracts new events from MongoDB based on the last processed timestamp.
        """
        if not last_timestamp:  # If there is no last timestamp (initial run or Redis wiped)
            # Fetch entries sorted by timestamp (descending)
            query = {}
            sort_query = {'timestamp': -1}
            return self.mongo_collection.find(query, sort=sort_query).limit(self.max_entries) #avoids overfetching the entire database
        # Fetch Mongo entries with timestamp that is greater than the last processed timestamp
        query = {'timestamp': {'$gt': last_timestamp}} #gt is greater than
        return self.mongo_collection.find(query)


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


    def load_to_redis(self, key, value):
        """
        Loads the transformed event data into a Redis key-value pair.
        """
        self.redis_client.set(key, value)
        self.redis_client.zadd('last_timestamp_set', {key: time.time()})


    def run_etl(self):
        """
        Continuously extracts, transforms, and loads data from MongoDB to Redis.
        """
        while True:
            last_timestamp = self.get_last_timestamp()
            # Extract
            new_events = self.extract_new_events(last_timestamp)
            event_count = 0  # logger event count
            for event in new_events:
                # Transform
                key, value = self.transform_event(event)
                # Load
                self.load_to_redis(key, value)
                # Update last timestamp for future runs
                self.set_last_timestamp(event['timestamp'])
                event_count += 1  # Increment logger event counter
            # Logs the amount of documents pulled per loop
            logging.info("Documents pulled in this loop: %s", event_count)
            time.sleep(self.sleep_time)


if __name__ == "__main__":
    etl = MongoToRedisETL()
    #threading allows the program to run multiple processes at the same time
    etl_thread = threading.Thread(target=etl.run_etl)
    etl_thread.start()

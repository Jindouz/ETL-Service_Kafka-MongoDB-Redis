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
            self.redis_timestamp_key = config['redis']['last_timestamp_key']
            self.max_entries = config['etl']['max_entries']
            self.max_entries_gt = config['etl']['max_entries_gt']
            self.sleep_time = config['etl']['sleep_time']
            self.event_count = config['etl']['event_count']
            self.event_count_incr = config['etl']['event_count_incr']


    def get_last_timestamp(self):
        """
        Get the last processed timestamp string (KEY:last_timestamp) from Redis (if it exists)
        and parse it into a datetime object so MongoDB query can be used.
        """
        last_timestamp = self.redis_client.get(self.redis_timestamp_key)
        if last_timestamp:
            # parse the Redis last_timestamp value string back into a datetime object so MongoDB query can be used
            timestamp_time_object = datetime.strptime(last_timestamp, '%Y/%m/%d_%H:%M:%S')
            return timestamp_time_object
        return None  # If no key, return None


    def extract_new_events(self, last_timestamp):
        """
        Extracts new events (Binary JSON documents) from MongoDB based on the last processed timestamp.
        """
        if not last_timestamp:  # If there is no last timestamp (initial run or Redis wiped)
            # Find every entry and fetch data limited to max_entries (as default its sorted by insertion order, oldest to newest)
            # (SQL Equivalent: SELECT * FROM mongo_collection LIMIT 5000)
            new_events = self.mongo_collection.find().limit(self.max_entries)
            return new_events
        
        # Fetch Mongo entries with a timestamp that is greater than the last processed timestamp 
        # (SQL Equivalent: SELECT * FROM mongo_collection WHERE timestamp > last_timestamp LIMIT 3000)
        query = {'timestamp': {'$gt': last_timestamp}}
        # Fetch entries from last_timestamp up to max_entries_gt
        new_events = self.mongo_collection.find(query).limit(self.max_entries_gt)
        # Example: if no timestamp get max 5k then 3k each loop until it reaches the latest then 30 per loop
        return new_events


    def transform_event(self, event):
        """
        Transforms a MongoDB event document (BSON) for storage as JSON string in Redis.
        """
        # MongoDB objectId is not JSON serializable so convert _id to string
        event['_id'] = str(event['_id']) 
        # MongoDB datetime object is not JSON serializable so convert timestamp to string
        event['timestamp'] = event['timestamp'].strftime('%Y/%m/%d_%H:%M:%S')

        # names each key as the value of reporterId and timestamp (reporterId:timestamp)
        reporter_id = event['reporterId']
        timestamp = event['timestamp']
        redis_key = f"{reporter_id}:{timestamp}"

        # Serialize the entire MongoDB event document to JSON string (a JSON that is compatible with Redis)
        redis_value = json.dumps(event)
        return redis_key, redis_value


    def run_etl(self):
        """
        Continuously extracts, transforms, and loads data from MongoDB to Redis.
        """
        while True:
            last_timestamp = self.get_last_timestamp()
            # Extract events from MongoDB
            new_events = self.extract_new_events(last_timestamp)
            event_count = self.event_count  # logger event count
            key_value_pairs = {}  # dictionary to hold all key-value pairs for MSET

            for event in new_events:
                # Transform
                key, value = self.transform_event(event)
                key_value_pairs[key] = value
                last_event_timestamp = event['timestamp']
                event_count += self.event_count_incr  # Increment logger event counter

            # Load all key-value pairs and update the last timestamp to Redis in one MSET
            if key_value_pairs:
                # Put the latest timestamp value into the last_timestamp key in Redis
                key_value_pairs[self.redis_timestamp_key] = last_event_timestamp
                # MSET is used to set multiple key-value pairs at once (pairs are treated as one unit of work)
                self.redis_client.mset(key_value_pairs)

            # Logs the amount of documents pulled per loop
            logging.info("Documents pulled in this loop: %s", event_count)
            time.sleep(self.sleep_time)


if __name__ == "__main__":
    etl = MongoToRedisETL()
    #threading allows the program to run multiple processes at the same time
    etl_thread = threading.Thread(target=etl.run_etl)
    etl_thread.start()

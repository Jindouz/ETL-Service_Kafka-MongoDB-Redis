""" 
Extracts data from MongoDB, transforms it, and loads it to Redis (ETL process) 
"""
import time
import threading
import logging
import yaml
from modules.mongo_utils import MongoUtils
from modules.redis_utils import RedisUtils

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class MongoToRedisETL:
    """
    Fetches data from MongoDB, transforms it, and stores it in Redis.
    """
    def __init__(self, config_path='config.yaml'):
        """
        Initializes the ETL service with MongoDB and Redis modules and configurations.
        """
        with open(config_path, 'r', encoding='utf-8') as f:
            config = yaml.safe_load(f)

            # MongoDB module
            self.mongo_utils = MongoUtils(config)
            # Redis module
            self.redis_utils = RedisUtils(config)

            self.sleep_time = config['etl']['sleep_time']
            self.event_count = config['etl']['event_count']
            self.event_count_incr = config['etl']['event_count_incr']
            self.pairs_dict = config['etl']['pairs_dict']


    def run_etl(self):
        """
        Continuously extracts, transforms, and loads data from MongoDB to Redis.
        """
        while True:
            last_timestamp = self.redis_utils.get_last_timestamp()
            # Extract events from MongoDB
            new_events = self.mongo_utils.extract_new_events(last_timestamp)
            event_count = self.event_count  # logger event count
            key_value_pairs = self.pairs_dict  # dictionary to hold all key-value pairs for MSET

            for event in new_events:
                # Transform
                key, value = self.redis_utils.transform_event(event)
                key_value_pairs[key] = value
                last_event_timestamp = event['timestamp']
                event_count += self.event_count_incr  # Increment logger event counter

            # Load all key-value pairs and update the last timestamp to Redis in one MSET
            if key_value_pairs:
                self.redis_utils.mset_load_data(key_value_pairs, last_event_timestamp)

            # Logs the amount of documents pulled per loop
            logging.info("Documents pulled in this loop: %s", event_count)
            time.sleep(self.sleep_time)


if __name__ == "__main__":
    etl = MongoToRedisETL()
    #threading allows the program to run multiple processes at the same time
    etl_thread = threading.Thread(target=etl.run_etl)
    etl_thread.start()

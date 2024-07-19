import json
import time
import threading
from datetime import datetime
from pymongo import MongoClient
import redis
import yaml

class MongoToRedisETL:
    def __init__(self, config_path='config.yaml'):
        with open(config_path, 'r') as f:
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

    def get_last_timestamp(self):
        last_timestamp = self.redis_client.get(self.redis_key)
        if last_timestamp:
            return datetime.strptime(last_timestamp, '%Y/%m/%d:%H:%M:%S')
        else:
            return datetime.min

    def set_last_timestamp(self, timestamp):
        if isinstance(timestamp, str):
            timestamp = datetime.strptime(timestamp, '%Y/%m/%d:%H:%M:%S')
        self.redis_client.set(self.redis_key, timestamp.strftime('%Y/%m/%d:%H:%M:%S'))

    def extract_new_events(self, last_timestamp):
        query = {'timestamp': {'$gt': last_timestamp}}
        return self.mongo_collection.find(query)

    def transform_event(self, event):
        event['_id'] = str(event['_id'])
        event['timestamp'] = event['timestamp'].strftime('%Y/%m/%d:%H:%M:%S')
        reporter_id = event['reporterId']
        timestamp = event['timestamp']
        key = f"{reporter_id}:{timestamp}"
        value = json.dumps(event)
        return key, value

    def load_to_redis(self, key, value):
        self.redis_client.set(key, value)

    def run_etl(self):
        while True:
            last_timestamp = self.get_last_timestamp()
            new_events = self.extract_new_events(last_timestamp)
            for event in new_events:
                key, value = self.transform_event(event)
                self.load_to_redis(key, value)
                self.set_last_timestamp(event['timestamp'])
            time.sleep(30)

if __name__ == "__main__":
    etl = MongoToRedisETL()
    etl_thread = threading.Thread(target=etl.run_etl)
    etl_thread.start()

"""
Utility class for interacting with Redis.
"""
import json
from datetime import datetime
import redis


class RedisUtils:
    """
    This class provides methods for interacting with Redis, such as setting data,
    and handling the last processed timestamp.
    """
    def __init__(self, config):
        """
        Initializes the RedisUtils class with connections, and configuration.
        """
        # Redis setup
        self.redis_client = redis.StrictRedis(
                host=config['redis']['host'],
                port=config['redis']['port'],
                decode_responses=True # decodes Redis bytes to Python strings
        )
        self.redis_timestamp_key = config['redis']['last_timestamp_key']

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

    def mset_load_data(self, key_value_pairs, last_event_timestamp):
        """
        Loads multiple key-value pairs and updates the last_timestamp in Redis using MSET.
        """
        # Put the latest timestamp value into the last_timestamp key in Redis
        key_value_pairs[self.redis_timestamp_key] = last_event_timestamp
        # MSET is used to set multiple key-value pairs at once (pairs are treated as one unit of work)
        self.redis_client.mset(key_value_pairs)

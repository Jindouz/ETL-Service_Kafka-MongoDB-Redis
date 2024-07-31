"""
Utility class for interacting with MongoDB.
"""
from pymongo import MongoClient


class MongoUtils:
    """
    This class provides methods for interacting with MongoDB.
    """
    def __init__(self, config):
        """
        Initializes the MongoUtils class with connections, and configuration.
        """
        # MongoDB setup
        self.mongo_client = MongoClient(config['mongo']['uri'])
        self.mongo_db = self.mongo_client[config['mongo']['database']]
        self.mongo_collection = self.mongo_db[config['mongo']['collection']]

        self.max_entries = config['etl']['max_entries']
        self.max_entries_gt = config['etl']['max_entries_gt']



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

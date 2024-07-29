"""
An event class for the producer.
"""
from datetime import datetime
import pytz


class Event:
    """
    Receives event parameters and transforms it into a dictionary. (with a current timestamp)
    """
    def __init__(self, reporter_id, metric_id, metric_value, message):
        self.reporter_id = reporter_id
        self.metric_id = metric_id
        self.metric_value = metric_value
        self.message = message
        self.timestamp = datetime.now(pytz.utc).isoformat()

    def to_dict(self):
        """
        Returns the event as a dictionary. (prepares it for JSON serialization)
        """
        event_dict = {
            "reporterId": self.reporter_id,
            "metricId": self.metric_id,
            "metricValue": self.metric_value,
            "message": self.message,
            "timestamp": self.timestamp
        }
        return event_dict

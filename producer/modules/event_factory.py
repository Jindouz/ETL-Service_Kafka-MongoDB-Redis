"""
EventFactory class responsible for generating random events.
"""
import random
import yaml
from .event import Event


class EventFactory:
    """
     This class generates events with randomized data.
    """
    def __init__(self, config_path):
        with open(config_path, 'r', encoding='utf-8') as f:
            self.config = yaml.safe_load(f)

        self.reporter_id = self.config['producer']['start_id']
        self.message = self.config['producer']['message']

        self.metric_value_min = self.config['producer']['metricValue_min']
        self.metric_value_max = self.config['producer']['metricValue_max']
        self.metric_id_min = self.config['producer']['metricId_min']
        self.metric_id_max = self.config['producer']['metricId_max']
        self.increment = self.config['producer']['increment']

    def create_event(self):
        """
        Creates an event with random metric values, reporter ID, and message.
        Puts the event object in a dictionary format and returns it. (with a current timestamp)
        """
        # Create an event object
        event = Event(
            reporter_id=self.reporter_id,
            metric_id=random.randint(self.metric_id_min, self.metric_id_max), # 1-10
            metric_value=random.randint(self.metric_value_min, self.metric_value_max), # 1-100
            message=self.message
        )
        # Increment the reporter ID
        self.reporter_id += self.increment
        # Convert the event object to a dictionary
        event = event.to_dict()
        return event

import json
from confluent_kafka import Consumer, KafkaException
import yaml

class EventConsumer:
    def __init__(self, config_path='config.yaml'):
        with open(config_path, 'r') as f:
            config = yaml.safe_load(f)
        self.consumer = Consumer({
            'bootstrap.servers': config['kafka']['bootstrap_servers'],
            'group.id': config['kafka']['group_id'],
            'auto.offset.reset': 'earliest'
        })
        self.topic = config['kafka']['topic']
        self.consumer.subscribe([self.topic])

    def consume_events(self):
        try:
            while True:
                msg = self.consumer.poll(timeout=1.0)
                if msg is None:
                    continue
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        continue
                    else:
                        raise KafkaException(msg.error())
                else:
                    event = json.loads(msg.value().decode('utf-8'))
                    print(f"Consumed event from topic {msg.topic()}: {event}")

        except KeyboardInterrupt:
            pass
        finally:
            self.consumer.close()


if __name__ == "__main__":
    event_consumer = EventConsumer()
    event_consumer.consume_events()

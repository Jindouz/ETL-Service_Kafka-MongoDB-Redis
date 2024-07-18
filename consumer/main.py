from kafka import KafkaConsumer

def main():
    consumer = KafkaConsumer(
        'test-topic',
        bootstrap_servers='localhost:9092',
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        group_id='my-group'
    )

    for message in consumer:
        print(f'Received: {message.decode("utf-8")}')

if __name__ == "__main__":
    main()

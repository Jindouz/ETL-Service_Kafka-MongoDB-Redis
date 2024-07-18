FROM apache/kafka:3.7.1


EXPOSE 9092

ENV KAFKA_BROKER_ID=1
ENV KAFKA_LISTENERS=PLAINTEXT://:9092
ENV KAFKA_ZOOKEEPER_CONNECT=zookeeper:2181

CMD ["kafka-server-start.sh", "config/server.properties"]
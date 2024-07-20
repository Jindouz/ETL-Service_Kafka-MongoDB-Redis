# Kafka to MongoDB to Redis ETL Service

## Overview

This project implements an ETL (Extract, Transform, Load) pipeline using multiple microservices, each encapsulated in Docker containers.  
The pipeline processes event data generated by a Kafka producer, passes it through a Kafka queue, consumes it with a Kafka consumer, stores it in MongoDB, and finally transfers the data to Redis.

## Features

- **Event Generation**: Randomly generates event data every second and sends it to Kafka.
- **Data Queueing**: Uses Apache Kafka for queuing events.
- **Data Storage**: Consumes Kafka events and stores them in MongoDB.
- **ETL Process**: Periodically transfers data from MongoDB to Redis, ensuring no duplicate entries.

## Requirements

- Docker
- Docker Desktop

## Project Structure

The project consists of the following microservices:

1. **Kafka Producer**: Generates events and sends them to Kafka.
2. **Kafka (and Zookeeper) Server**: Handles message queuing.
3. **Kafka Consumer**: Consumes events from Kafka and stores them in MongoDB.
4. **MongoDB Server**: Stores the consumed events.
5. **Mongo to Redis ETL**: Transfers data from MongoDB to Redis.
6. **Redis Server**: Stores the final data for fast retrieval.


## Service Details

### Apache Kafka
Producer: Generates and sends events to Kafka.  
Server: Manages message queuing.  
Consumer: Consumes events and inserts them into MongoDB.  

### MongoDB 
Stores events consumed from Kafka in the EVENTS collection.

### Redis
Stores the final transformed data from MongoDB.

### ETL Service
Periodically extracts new events from MongoDB and loads them into Redis.  

### Running the ETL Service
The ETL service runs inside a Docker container and executes every 30 seconds by default, as configured in the config.yaml file.

## Author

Made by Maor S (2024)
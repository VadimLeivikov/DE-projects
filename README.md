# Purpose of the project
The goal of the project is to implement a real-time data generation system using the KafkaPython client (kafka-python) and Spark Structured Streaming.
A Python script was used as a producer for demonstration purposes, generating a continuous stream of messages that are registered in the Kafka broker.
Spark Structured Streaming was used as the message consumer.

# In this project:
Two Docker containers from Bitnami were deployed for Spark and Kafka.
- A **docker-compose.yml** file was created to organize a unified network for information exchange between the containers, with internal and external Kafka listeners configured.
- A script named **producer.py** was written.
- Port forwarding was configured, allowing messages from the producer to be received through the host into the Kafka broker.
- A consumer script, named **structure_streaming_kafka.py**, for subscribing to the Kafka topic was written.
- Message reception has been parsed and configured.
- A dataset with static information was added.
- A test join of static and dynamic streams was configured.
- Aggregate processing was applied to count the number of messages in each batch.

# Result
The origin stream from producer:
![Producer stream](https://github.com/VadimLeivikov/DE-projects/blob/kafka-spark-streaming/Producer%20stream.png)

Spark is reading topic after joining static and dynamic streams:
![Spark is reading topic after joining streams](https://github.com/VadimLeivikov/DE-projects/blob/kafka-spark-streaming/join%20streams_1.JPG)

Spark is aggregating the stream:
![The result of aggregations](https://github.com/VadimLeivikov/DE-projects/blob/kafka-spark-streaming/aggregations.JPG)

# Conclusion
This project demonstrates the integration of Kafka and Spark for real-time data processing.

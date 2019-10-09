# Demo app to stream from Twitter to Elasticsearch through Apache Kafka

I built this app to learn about Kafka. This was an assignment from [Stephane Maarek's Kafka course](https://www.linkedin.com/learning/learn-apache-kafka-for-beginners/kafka-connect-introduction). 
The app has the following three modules:
- kafka-basics: it contains demos for creating Kafka producers and consumers
- kafka-producer-twitter: it contains the code to get streams from twitter and produce them to kafka
- kafka-consumer-elasticsearch: it contains the code to poll streams from kafka (through a kafka consumer) and upload it into Elasticsearch
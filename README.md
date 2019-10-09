# Demo app to stream from Twitter to Elasticsearch through Apache Kafka

I built this app to learn about Kafka. This was an assignment from [Stephane Maarek's Kafka course](https://www.linkedin.com/learning/learn-apache-kafka-for-beginners/kafka-connect-introduction). 
The app has the following four modules:
- kafka-basics: it contains demos for creating Kafka producers and consumers
- kafka-producer-twitter: it contains the code to get streams from twitter and produce them to kafka
- kafka-consumer-elasticsearch: it contains the code to poll streams from kafka (through a kafka consumer) and upload it into Elasticsearch
- kafka-streams-filter-tweets: it contains the code sample to show how to filter streams. In this case, tweets from users with more than 10,000 followers are filtered out to a new topic (i.e., "important_tweets")

I included .classpath and .project files and .settings folder so you see how vscode structures a maven project. I run into issues trying to understand just that
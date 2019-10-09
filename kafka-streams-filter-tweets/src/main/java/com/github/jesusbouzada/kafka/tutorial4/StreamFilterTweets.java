package com.github.jesusbouzada.kafka.tutorial4;

import java.util.Properties;

import com.google.gson.JsonParser;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;

public class StreamFilterTweets {
    
    public static void main(String[] args) {

        String bootstrapServers = "127.0.0.1:9092";

        // create properties
        Properties properties = new Properties();
        properties.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "demo-kafka-streams");
        properties.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());
        properties.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());

        // create a topology
        StreamsBuilder streamsBuilder = new StreamsBuilder();

        // input topic
        KStream<String,String> inputTopic = streamsBuilder.stream("twitter_tweets");
        KStream<String,String> filteredStream = inputTopic.filter(
            (k, jsonTweet) -> {
                Integer followers_count = 0;
                try {
                    followers_count = JsonParser.parseString(jsonTweet)
                                                .getAsJsonObject()
                                                .get("user")
                                                .getAsJsonObject()
                                                .get("followers_count")
                                                .getAsInt();
                }
                catch (NullPointerException e) {
                    //e.printStackTrace();
                }
                return followers_count > 10000;
            }
                    // filter for tweets which has a user of over 10000 followers
        );
        filteredStream.to("important_tweets");

        // build the topology
        KafkaStreams kafkaStreams = new KafkaStreams(streamsBuilder.build(), properties);
        
        // start our stream application
        kafkaStreams.start();
    }
}
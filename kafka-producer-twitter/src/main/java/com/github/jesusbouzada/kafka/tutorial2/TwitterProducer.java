package com.github.jesusbouzada.kafka.tutorial2;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import com.google.common.collect.Lists;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TwitterProducer {

    private Logger logger = LoggerFactory.getLogger(TwitterProducer.class.getName());

    public static void main(String[] args) throws InterruptedException {

        // args[0] -> apiKey
        // args[1] -> apiSecretKey
        // args[2] ->  accessToken
        // args[3] ->  accessSecretToken

        new TwitterProducer().run(args[0], args[1], args[2], args[3]);
    }

    public void run(String apiKey, String apiSecretKey, String accessToken, String accessSecretToken) {

        logger.info("Setup");

        /**
         * Set up your blocking queues: Be sure to size these properly based on expected
         * TPS of your stream
         */
        BlockingQueue<String> msgQueue = new LinkedBlockingQueue<String>(10000);



        // create a twitter client
        Client hosebirdClient = createTwitterClient(msgQueue, apiKey, apiSecretKey, accessToken, accessSecretToken);

        // Attempts to establish a connection.
        hosebirdClient.connect();

        // create a kafka producer
        KafkaProducer<String, String> producer = createKafkaProducer();

        // loop to send tweets to kafka

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Stopping application...");
            logger.info("Shutting down client from Twitter...");
            hosebirdClient.stop();
            logger.info("Closing producer...");
            producer.close();
            logger.info("Done!");
        }));

        // on a different thread, or multiple different threads....
        while (!hosebirdClient.isDone()) {
            String msg = null;
            try {
                msg = msgQueue.poll(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                e.printStackTrace();
                hosebirdClient.stop();
            }
            if (msg != null) {
                // logger.info(msg);

                // create a producer record
                ProducerRecord<String, String> record = new ProducerRecord<String, String>("twitter_tweets", msg);

                // send messages - asynchronous
                producer.send(record, new Callback(){
                    @Override
                    public void onCompletion(RecordMetadata metadata, Exception exception) {
                        // executes if record successfully sent or exception thrown
                        if(exception != null) {
                            logger.error("Error while producing", exception);
                        }
                    }
                });
            }
        }

        logger.info("End of application");
    }

    KafkaProducer<String, String> createKafkaProducer() {
        // create producer properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // create safe producer - idempotence producers have lower throughput and higher latency, use them it your use case allows (test for that)
        properties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true"); // only available if kafka > 0.11
        // Following three properties are set automatically for idempotence producers, we force it to remark what idempotence do
        properties.setProperty(ProducerConfig.ACKS_CONFIG, "all");
        properties.setProperty(ProducerConfig.RETRIES_CONFIG, Integer.toString(Integer.MAX_VALUE));
        properties.setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "5"); // if Kafka > 0.11 use 5 else use 1

        // High trhoughput settings (at the expense of a little latency and CPU usage)
        properties.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");
        properties.setProperty(ProducerConfig.LINGER_MS_CONFIG, "20");
        properties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, Integer.toString(32*1024)); // 32 KB batch size

        // return producer
        return new KafkaProducer<String, String>(properties);
    }

    Client createTwitterClient(BlockingQueue<String> msgQueue, String apiKey, String apiSecretKey, String accessToken,
            String accessSecretToken) {

        /**
         * Declare the host you want to connect to, the endpoint, and authentication
         * (basic auth or oauth)
         */
        Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
        StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();
        List<String> terms = Lists.newArrayList("Kafka", "Confluent");
        hosebirdEndpoint.trackTerms(terms);

        Authentication hosebirdAuth = new OAuth1(apiKey, apiSecretKey, accessToken, accessSecretToken);

        ClientBuilder builder = new ClientBuilder()
                                        .name("Hosebird-Client-01")
                                        .hosts(hosebirdHosts)
                                        .authentication(hosebirdAuth)
                                        .endpoint(hosebirdEndpoint)
                                        .processor(new StringDelimitedProcessor(msgQueue));

        return builder.build();
    }
}
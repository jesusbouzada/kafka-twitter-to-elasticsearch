package com.github.jesusbouzada.kafka.tutorial3;

import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonParser;

import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ElasticSearchConsumer {

    public static void main(String[] args) throws InterruptedException, IOException {

        // args[0] -> hostname
        // args[1] -> username
        // args[2] -> password
        
        new ElasticSearchConsumer().run(args[0], args[1], args[2]);
    }

    public ElasticSearchConsumer() {
    }

    public void run(String hostname, String username, String password) throws IOException {

        String bootstrapServers = "127.0.0.1:9092";
        String groupId = "kafka-demo-elasticsearch";
        String topic = "twitter_tweets";

        Logger logger = LoggerFactory.getLogger(ElasticSearchConsumer.class.getName());

        // latch for dealing with multiple threads
        CountDownLatch latch = new CountDownLatch(1);

        // create the consumer runnable
        logger.info("Creating the consumer thread");
        Runnable myConsumerRunnable = new ConsumerRunnable(bootstrapServers, groupId, topic, hostname, username,
                password, latch);

        // start the thread
        Thread myThread = new Thread(myConsumerRunnable);
        myThread.start();

        // add a shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Caught shutdown hook");
            ((ConsumerRunnable) myConsumerRunnable).shutdown();
            try {
                latch.await();
            } catch (InterruptedException e) {
                logger.error("Application got interrupted", e);
            } finally {
                logger.info("Application has exited");
            }
        }));

        try {
            latch.await();
        } catch (InterruptedException e) {
            logger.error("Application got interrupted", e);
        } finally {
            logger.info("Application is closing");
        }
    }

    public class ConsumerRunnable implements Runnable {

        private CountDownLatch latch;
        private KafkaConsumer<String, String> consumer;
        RestHighLevelClient elasticSearchClient;
        private Logger logger = LoggerFactory.getLogger(ConsumerRunnable.class.getName());

        public ConsumerRunnable(String bootstrapServers, String groupId, String topic, String hostname, String username,
                String password, CountDownLatch latch) throws IOException {

            this.latch = latch;

            // create consumer
            this.consumer = createKafkaConsumer(bootstrapServers, groupId, topic);

            // connect to ElasticSearch
            this.elasticSearchClient = createClient(hostname, username, password);
        }

        @Override
        public void run() {

            try {
                // poll for new data
                while (true) {
                    BulkRequest bulkRequest = new BulkRequest();

                    ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                    Integer recordCount = records.count();
                    logger.info(recordCount + "records received");

                    for (ConsumerRecord<String, String> record : records) {

                        // 2 strategies to generate ids
                        // kafka generic id
                        // String id = record.topic() + "_" + record.partition() + "_" +
                        // record.offset();

                        try {
                            // We can do better with Twitter feed specific id
                            String id = JsonParser.parseString(record.value()).getAsJsonObject().get("id_str").getAsString();

                            IndexRequest request = new IndexRequest("twitter");
                            request.id(id); // this is to make our consumer idempotent
                            request.source(record.value(), XContentType.JSON);
                            
                            // Bulk processing
                            bulkRequest.add(request);

                            // If we wanted to process item by item, less efficient than doing it on bulk
                            // try {
                            //     IndexResponse response = this.elasticSearchClient.index(request, RequestOptions.DEFAULT);
                            //     logger.info(response.getId());
                            //     Thread.sleep(10);
                            // } catch (IOException | InterruptedException e) {
                            //     e.printStackTrace();
                            // }
                        }
                        catch (NullPointerException nullPointerException) {
                            logger.warn("skipping bad data: " + record.value());
                        }
                    }
                    if(recordCount > 0)
                    {
                        try {
                            BulkResponse bulkResponse = this.elasticSearchClient.bulk(bulkRequest, RequestOptions.DEFAULT);
                        } catch (IOException ioException) {
                            ioException.printStackTrace();
                        }
                        logger.info("Commiting the offsets...");
                        consumer.commitSync();
                        logger.info("Offsets have been commited!");
                        try {
                            Thread.sleep(1000);
                        } catch (InterruptedException interruptedException) {
                            interruptedException.printStackTrace();
                        }
                    }
                }
            } catch (WakeupException e) {
                logger.info("Received shutdown signal!");
            } finally {
                this.consumer.close();

                // Close client gracefully
                try {
                    this.elasticSearchClient.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
                // tell our main code we are done with the consumer
                latch.countDown();
            }
        }

        public RestHighLevelClient createClient(String hostname, String username, String password) {
    
            final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
            credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(username, password));
    
            RestClientBuilder builder = RestClient.builder(new HttpHost(hostname, 443, "https"));
            builder.setHttpClientConfigCallback(new RestClientBuilder.HttpClientConfigCallback(){
                @Override
                public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpClientBuilder) {
                    return httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider);  
                }
            });
    
            return new RestHighLevelClient(builder);
        }

        private KafkaConsumer<String, String> createKafkaConsumer(String bootstrapServers, String groupId, String topic) {
    
            // create consumer configs
            Properties properties = new Properties();
            properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
            properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
            properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
            // disable auto commit of offset so we control when to commit (i.e.: it's not done asynchronously)
            properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false"); 
            // properties.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "100"); 
    
            KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);

            // subscribe consumer to our topic(s)
            // consumer.subscribe(Collections.singleton(topic));
            consumer.subscribe(Arrays.asList(topic));

            // return consumer
            return consumer;
        }

        public void shutdown() {
            // wakeup() method is a special method to interrupt consumer.poll()
            // it will through WakeupException
            consumer.wakeup();
            
        }
    }

}
package com.github.jesusbouzada.kafka.tutorial1;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProducerDemoKeys {
    public static void main(String[] args) throws InterruptedException, ExecutionException {

        Logger logger = LoggerFactory.getLogger(ProducerDemoKeys.class.getName());

        // create producer properties
        Properties properties = new Properties();
        properties.setProperty( ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // create producer
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

        for (int i = 0; i < 10; i++) {
            
            String topic = "first_topic";
            String value = "Hello world " + Integer.toString(i);
            String key = "id_" + Integer.toString(i);

            // create a producer record (by providing key, we guarantee same key goes always to same partition)
            ProducerRecord<String, String> record = new ProducerRecord<String,String>(topic, key, value);
            
            logger.info("Key: " + key);
            // Key 0 - part 1
            // Key 1 - part 0
            // Key 2 - part 2
            // Key 3 - part 0
            // Key 4 - part 2
            // Key 5 - part 2
            // Key 6 - part 0
            // Key 7 - part 2
            // Key 8 - part 1
            // Key 9 - part 2
            
            // send messages - asynchronous
            producer.send(record, new Callback(){
                @Override
                public void onCompletion(RecordMetadata metadata, Exception exception) {
                    // executes if record successfully sent or exception thrown
                    if(exception == null) {
                        // record successfully sent
                        logger.info("Received new metadata. \n" + 
                        "Topic: " + metadata.topic() + "\n" + 
                        "Partition: " + metadata.partition() + "\n" + 
                        "Offset: " + metadata.offset() + "\n" +
                        "Timestamp: " + metadata.timestamp());
                    } else {
                        logger.error("Error while producing", exception);
                    }
                }
            }).get(); // block .send() to make it synchronous - don't do this in production!!
        }

        // flush data
        producer.flush();
        // flush and close producer
        producer.close();
    }
}
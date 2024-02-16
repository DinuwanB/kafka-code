package org.nod;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoWithCallback {

    private static final Logger log = LoggerFactory.getLogger(ProducerDemoWithCallback.class.getSimpleName());

    public static void main(String[] args) {
        log.info("I'm Kafka Producer with Callback!");

        // Set producer properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // Create the producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);


        // send the data - asynchronous
        for (int i = 0; i < 10; i++) {
            // create a producer record
            ProducerRecord<String, String> producerRecord = new ProducerRecord<>("demo_java", "hello Kushani mm " + i);

            producer.send(producerRecord, new Callback() {
                @Override
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    // execute every time a record is successfully sent or exception is thrown
                    if (e == null) {
                        log.info("Received new meta data / \n"
                                + "Topic : " + recordMetadata.topic() + "\n"
                                + "Partition : " + recordMetadata.partition() + "\n"
                                + "Offset : " + recordMetadata.offset() + "\n"
                                + "Timestamp : " + recordMetadata.timestamp());
                    } else {
                        log.error("Error while message producing", e);
                    }
                }
            });
        }


        // flush data - synchronous
        producer.flush();

        //flush and close producer
        producer.close();

    }
}
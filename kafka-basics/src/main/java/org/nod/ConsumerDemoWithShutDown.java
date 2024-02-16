package org.nod;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.Properties;

public class ConsumerDemoWithShutDown {

    private static final Logger log = LoggerFactory.getLogger(ConsumerDemoWithShutDown.class.getSimpleName());

    public static void main(String[] args) {
        log.info("I'm Kafka Consumer with Shutdown!");

        String boostrapServer = "127.0.0.1:9092";
        String groupId = "my-group-two-application";
        String topic = "demo_java";

        // Set producer properties
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, boostrapServer);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        // Create the consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);

        // Get a reference to the current thread
        final Thread mainThread = Thread.currentThread();

        // Adding the shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
                log.info("Detected a shutdown, let's exit by calling consumer wakeup()...");
                consumer.wakeup();

                // Join the main thread to allow the execution of the code in the main thread
                try {
                    mainThread.join();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }

        });

        try {
            // Subscribe consumer to our topic(s)
            consumer.subscribe(List.of(topic));

            // poll for new data
            while (true) {
                ConsumerRecords<String, String> consumerRecords = consumer.poll(Duration.ofMillis(1000));

                for (ConsumerRecord<String, String> record : consumerRecords) {
                    log.info("Key : " + record.key() + ", Value : " + record.value());
                    log.info("Partition : " + record.partition() + ", Offset : " + record.offset());
                }
            }
        } catch (WakeupException e) {
            log.info("Wakeup Exception!");
            // we ignore this as this an expected exception when closing a consumer
        } catch (Exception e) {
            log.error("Unexpected exception!");
        } finally {
            consumer.close();
            log.info("The consumer is now closed!");
        }


    }
}
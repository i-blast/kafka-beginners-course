package org.course.kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class ConsumerDemoWithShutdown {

    private static final Logger log = LoggerFactory.getLogger(ConsumerDemoWithShutdown.class.getSimpleName());

    public static void main(String[] args) {
        log.info("I'm a Kafka Consumer!");

        final var groupId = "my-third-application";
        final var topic = "demo_java";

        // Create consumer configuration
        var properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, System.getenv("KAFKA_BOOTSTRAP_SERVERS"));
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        var consumer = new KafkaConsumer<String, String>(properties);

        // Adding shutdown hook
        final var mainThread = Thread.currentThread();
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("Detected a shutdown...");
            consumer.wakeup();

            try {
                mainThread.join();
            } catch (InterruptedException exc) {
                exc.printStackTrace();
            }
        }));

        try {
            consumer.subscribe(Collections.singletonList(topic));

            // Poll the new data
            while (true) {
                log.info("Polling...");

                var records = consumer.poll(Duration.ofMillis(1000L));
                for (var record : records) {
                    log.info("Key: " + record.key() + ", Value: " + record.value());
                    log.info("Partition: " + record.key() + ", Offset: " + record.offset());
                }
            }
        } catch (WakeupException exc) {
            log.info("Caught WakeupException!");
            // Ignore as this is an expected exception.
        } catch (Exception exc) {
            log.error("Unexpected exception", exc);
        } finally {
            consumer.close();
            log.info("The consumer is gracefully closed");
        }
    }
}

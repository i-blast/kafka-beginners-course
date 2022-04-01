package org.course.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemo {

    private static final Logger log = LoggerFactory.getLogger(ProducerDemo.class.getSimpleName());

    public static void main(String[] args) {
        log.info("Hello Kafka!");

        // Create producer properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, System.getenv("KAFKA_BOOTSTRAP_SERVERS"));
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        /**
         * Create producer:
         * zookeeper-server-start.sh ~/dev/kafka_2.13-3.1.0/config/zookeeper.properties
         * and
         * kafka-server-start.sh ~/dev/kafka_2.13-3.1.0/config/server.properties
         * under Debian WSL2
         */
        var producer = new KafkaProducer<String, String>(properties);

        // Create a producer record
        var producerRecord = new ProducerRecord<String, String>("demo_java", "hello world");

        // Send the data - asynchronous operation
        producer.send(producerRecord);
        // Flush and close producer
        producer.flush();
        producer.close();
    }
}

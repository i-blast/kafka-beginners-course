package org.course.kafka;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoWithCallback {

    private static final Logger log = LoggerFactory.getLogger(ProducerDemoWithCallback.class.getSimpleName());

    public static void main(String[] args) {
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

        for (int i = 0; i < 10; i++) {
            // Create a producer record
            var producerRecord = new ProducerRecord<String, String>("demo_java", "hello world" + i);

            // Send the data - asynchronous operation
            producer.send(producerRecord, (metadata, exc) -> {
                if (exc == null) {
                    var logMsg = new StringBuilder().append("Received new metadata/ \n")
                            .append("Topic: ").append(metadata.topic()).append("\n")
                            .append("Partition: ").append(metadata.partition()).append("\n")
                            .append("Offset: ").append(metadata.offset()).append("\n")
                            .append("Timestamp: ").append(metadata.timestamp());
                    log.info(logMsg.toString());
                } else {
                    log.error("Error while producing", exc);
                }
            });

            try {
                Thread.sleep(1000);
            } catch (InterruptedException exc) {
                exc.printStackTrace();
            }
        }

        // Flush and close producer
        producer.flush();
        producer.close();
    }
}

package org.course.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoKeys {

    private static final Logger log = LoggerFactory.getLogger(ProducerDemoKeys.class.getSimpleName());

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
            String topic = "demo_java";
            String value = "hello_world" + i;
            String key = "id_" + i;

            // Create a producer record
            var producerRecord = new ProducerRecord<String, String>(topic, key, value);

            // Send the data - asynchronous operation
            producer.send(producerRecord, (metadata, exc) -> {
                if (exc == null) {
                    var logMsg = new StringBuilder().append("Received new metadata/ \n")
                            .append("Topic: ").append(metadata.topic()).append("\n")
                            .append("Key: ").append(producerRecord.key()).append("\n")
                            .append("Partition: ").append(metadata.partition()).append("\n")
                            .append("Offset: ").append(metadata.offset()).append("\n")
                            .append("Timestamp: ").append(metadata.timestamp());
                    log.info(logMsg.toString());
                } else {
                    log.error("Error while producing", exc);
                }
            });
        }

        // Flush and close producer
        producer.flush();
        producer.close();
    }
}

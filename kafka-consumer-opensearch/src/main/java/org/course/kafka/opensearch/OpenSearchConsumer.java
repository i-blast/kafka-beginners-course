package org.course.kafka.opensearch;

import com.google.gson.JsonParser;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.DefaultConnectionKeepAliveStrategy;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.opensearch.action.bulk.BulkRequest;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.RestClient;
import org.opensearch.client.RestClientBuilder;
import org.opensearch.client.RestHighLevelClient;
import org.opensearch.client.indices.CreateIndexRequest;
import org.opensearch.client.indices.GetIndexRequest;
import org.opensearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class OpenSearchConsumer {

    public static final String WIKIMEDIA_INDEX = "wikimedia";

    public static RestHighLevelClient createOpenSearchClient() {
        // we build a URI from the connection string
        final var connectionString = "http://localhost:9200";
        var connUri = URI.create(connectionString);

        // extract login information if it exists
        var userInfo = connUri.getUserInfo();

        RestHighLevelClient restHighLevelClient;
        if (userInfo == null) {
            // REST client without security
            var restClientBuilder = RestClient
                    .builder(new HttpHost(connUri.getHost(), connUri.getPort(), "http"));
            restHighLevelClient = new RestHighLevelClient(restClientBuilder);

        } else {
            // REST client with security
            String[] auth = userInfo.split(":");

            var cp = new BasicCredentialsProvider();
            cp.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(auth[0], auth[1]));

            RestClientBuilder.HttpClientConfigCallback callback = builder -> builder
                    .setDefaultCredentialsProvider(cp)
                    .setKeepAliveStrategy(new DefaultConnectionKeepAliveStrategy());
            var restClientBuilder = RestClient
                    .builder(new HttpHost(connUri.getHost(), connUri.getPort(), connUri.getScheme()))
                    .setHttpClientConfigCallback(callback);
            restHighLevelClient = new RestHighLevelClient(restClientBuilder);
        }

        return restHighLevelClient;
    }

    private static KafkaConsumer<String, String> createKafkaConsumer() {
        final var groupId = "consumer-opensearch-demo";

        var properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, System.getenv("KAFKA_BOOTSTRAP_SERVERS"));
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

        return new KafkaConsumer<>(properties);
    }

    private static String extractId(String json) {
        return JsonParser.parseString(json)
                .getAsJsonObject()
                .get("meta")
                .getAsJsonObject()
                .get("id")
                .getAsString();
    }

    public static void main(String[] args) {
        Logger log = LoggerFactory.getLogger(OpenSearchConsumer.class.getSimpleName());

        var openSearchClient = createOpenSearchClient();

        var consumer = createKafkaConsumer();
        final var topic = "wikimedia.recentchange";
        consumer.subscribe(Collections.singletonList(topic));

        try (openSearchClient; consumer) {
            var isIndexExists = openSearchClient.indices()
                    .exists(new GetIndexRequest(WIKIMEDIA_INDEX), RequestOptions.DEFAULT);

            if (!isIndexExists) {
                openSearchClient.indices()
                        .create(new CreateIndexRequest(WIKIMEDIA_INDEX), RequestOptions.DEFAULT);

                log.info("The Wikimedia index has been created.");
            } else {
                log.info("The Wikimedia index already exists.");
            }

            while (true) {
                var records = consumer.poll(Duration.ofMillis(3000L));
                var recordCount = records.count();

                log.info("Received " + recordCount + " record(s).");

                var bulkRequest = new BulkRequest();

                for (var record : records) {
                    try {
                        var recordId = extractId(record.value());

                        var indexRequest = new IndexRequest(WIKIMEDIA_INDEX)
                                .source(record.value(), XContentType.JSON)
                                .id(recordId);
//                        var indexResponse = openSearchClient.index(indexRequest, RequestOptions.DEFAULT);

                        bulkRequest.add(indexRequest);

//                        log.info(indexResponse.getId());
                    } catch (Exception exc) {
                        log.error("Error during indexing.", exc);
                    }
                }

                if (bulkRequest.numberOfActions() > 0) {
                    var bulkResponse = openSearchClient.bulk(bulkRequest, RequestOptions.DEFAULT);
                    log.info("Inserted " + bulkResponse.getItems().length + " record(s).");

                    try {
                        Thread.sleep(1000L);
                    } catch (InterruptedException exc) {
                        log.error("Thread was interrupted.", exc);
                    }

                    consumer.commitSync();
                    log.info("Offsets have been committed.");
                }
            }
        } catch (IOException exc) {
            log.error("Error while creating index.", exc);
        }
    }

}

package org.nod;

import com.google.gson.JsonParser;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.DefaultConnectionKeepAliveStrategy;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.index.IndexResponse;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.RestClient;
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
    public static RestHighLevelClient createOpenSearchClient() {
        String connectionString = "http://localhost:9200";

        //build a URI from the connection string
        RestHighLevelClient restHighLevelClient;
        URI conUri = URI.create(connectionString);

        //extract login information if it's exists
        String userInfo = conUri.getUserInfo();

        if (userInfo == null) {
            //REST client without security
            restHighLevelClient = new RestHighLevelClient(RestClient
                    .builder(new HttpHost(
                            conUri.getHost(),
                            conUri.getPort(),
                            "http")));

        } else {
            //Rest client with security
            String[] auth = userInfo.split(":");

            //Establish credentials to use basic authentication.
            //Only for demo purposes. Don't specify your credentials in code.
            final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
            credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(auth[0], auth[1]));

            restHighLevelClient = new RestHighLevelClient(RestClient.builder(new HttpHost(
                            conUri.getHost(),
                            conUri.getPort(),
                            conUri.getScheme()))
                    .setHttpClientConfigCallback(httpAsyncClientBuilder ->
                            httpAsyncClientBuilder
                                    .setDefaultCredentialsProvider(credentialsProvider)
                                    .setKeepAliveStrategy(new DefaultConnectionKeepAliveStrategy())));

        }

        return restHighLevelClient;
    }

    public static void main(String[] args) throws IOException {

        Logger log = LoggerFactory.getLogger(OpenSearchConsumer.class.getSimpleName());

        // create an opensearch client
        RestHighLevelClient openSearchClient = createOpenSearchClient();

        // create kafka client
        KafkaConsumer<String, String> consumer = createKafkaConsumer();

        // create the index on OpenSearch if it doesn't exist already
        try (openSearchClient; consumer) {

            boolean indexExists = openSearchClient
                    .indices()
                    .exists(new GetIndexRequest("wikimedia"), RequestOptions.DEFAULT);

            if (!indexExists) {

                CreateIndexRequest createIndexRequest = new CreateIndexRequest("wikimedia");
                openSearchClient.indices().create(createIndexRequest, RequestOptions.DEFAULT);
                log.info("The Wikimedia Index has been created!");

            } else {
                log.info("The Wikimedia Index already exists !");
            }

            // subscribe the consumer
            consumer.subscribe(Collections.singleton("wikimedia.recentchange"));

            while (true) {

                // if there is no data, block this code for 3 sec
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(3000));

                int recordCount = records.count();
                log.info("Received " + recordCount + " record(s)");

                for (ConsumerRecord<String, String> record : records) {

                    // idempotence
                    // strategy 1 - define an ID using kafka record coordinates -  this is unique
                    // String id = record.topic() + "_" + record.partition() + "_" + record.offset();

                    try {
                        // strategy 2 - extract id from the JSON value
                        String id = extractId(record.value());

                        // save record in OpenSearch
                        IndexRequest indexRequest = new IndexRequest("wikimedia")
                                .source(record.value(), XContentType.JSON)
                                .id(id);

                        IndexResponse response = openSearchClient.index(indexRequest, RequestOptions.DEFAULT);
                        log.info(response.getId());
                    } catch (Exception e) {
                        log.error(String.valueOf(e));
                    }

                }
            }
        }
    }

    private static KafkaConsumer<String, String> createKafkaConsumer() {

        String boostrapServer = "127.0.0.1:9092";
        String groupId = "consumer-opensearch-demo";

        // Set consumer properties
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, boostrapServer);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");

        // create consumer $ return
        return new KafkaConsumer<>(properties);
    }

    private static String extractId(String value) {
        // gson library
        return JsonParser.parseString(value)
                .getAsJsonObject()
                .get("meta")
                .getAsJsonObject()
                .get("id")
                .getAsString();
    }
}
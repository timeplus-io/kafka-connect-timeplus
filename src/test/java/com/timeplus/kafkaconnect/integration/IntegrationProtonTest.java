package com.timeplus.kafkaconnect.integration;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;

import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.output.OutputFrame;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.utility.DockerImageName;

import com.github.dockerjava.api.command.CreateContainerCmd;

import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.json.JSONObject;
import org.json.JSONArray;

import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;


public class IntegrationProtonTest {
    private final static String TOPIC_NAME = "test-topic";
    private final static String STREAM = "test_kafka_connect_proton_sink";

    Integer protonMappedPort = 0;

    Network network = Network.newNetwork();
    JSONObject eventJson = new JSONObject();

    @Container
    public GenericContainer proton = new GenericContainer(DockerImageName.parse("ghcr.io/timeplus-io/proton:latest"))
            .withExposedPorts(3218)
            .withNetworkAliases("proton")
            .withNetwork(network);

    @Container
    public KafkaContainer kafka = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.0.1")).withKraft()
            .withNetwork(network);

    @Container
    public GenericContainer kafkaConect = new GenericContainer(
            DockerImageName.parse("timeplus-kafkaconnect-sink:dev"))
            .withEnv("CONNECT_REST_ADVERTISED_HOST_NAME", "localhost")
            .withEnv("CONNECT_GROUP_ID", "cg_connect_timeplus")
            .withEnv("CONNECT_CONFIG_STORAGE_TOPIC", "connect_timeplus_config")
            .withEnv("CONNECT_OFFSET_STORAGE_TOPIC", "connect_timeplus_offsets")
            .withEnv("CONNECT_STATUS_STORAGE_TOPIC", "connect_timeplus_status")
            .withEnv("CONNECT_CONFIG_STORAGE_REPLICATION_FACTOR", "1")
            .withEnv("CONNECT_OFFSET_STORAGE_REPLICATION_FACTOR", "1")
            .withEnv("CONNECT_STATUS_STORAGE_REPLICATION_FACTOR", "1")
            .withEnv("CONNECT_KEY_CONVERTER", "org.apache.kafka.connect.converters.ByteArrayConverter")
            .withEnv("CONNECT_VALUE_CONVERTER", "org.apache.kafka.connect.converters.ByteArrayConverter")
            .withEnv("CONNECT_PLUGIN_PATH", "/app/libs")
            .withEnv("CONNECT_TASK_SHUTDOWN_GRACEFUL_TIMEOUT_MS", "30000")
            .withEnv("CONNECT_OFFSET_FLUSH_INTERVAL_MS", "900000")
            .withEnv("CONNECT_CONSUMER_MAX_POLL_RECORDS", "500")
            .withExposedPorts(8083)
            .withNetwork(network);

    @BeforeEach
    public void setup() {
        proton.start();
        String protonHost = proton.getHost();
        String protonName = proton.getContainerName();
        System.out.println("proton is running at host " + protonHost);
        System.out.println("proton is running at name " + protonName);
        protonMappedPort = proton.getMappedPort(3218);
        System.out.println("proton is running at port " + protonMappedPort);

        try {
            deleteStream();
        } catch (Exception e) {
            System.out.println("failed to delete streaming " + e);
        }

        kafka.start();
        String brokers = kafka.getNetworkAliases().get(0) + ":9092";
        System.out.println("kafka is running at " + brokers);

        kafkaConect.withEnv("CONNECT_BOOTSTRAP_SERVERS", kafka.getNetworkAliases().get(0) + ":9092");
        kafkaConect.start();

        eventJson.put("id", 123);
        eventJson.put("name", "test");
    }

    @AfterEach
    public void tearDown() {
        kafkaConect.stop();
        kafka.stop();
        proton.stop();
    }

    void produce() throws InterruptedException, ExecutionException {
        // Set up Kafka producer properties
        Properties properties = new Properties();
        properties.put("bootstrap.servers", kafka.getBootstrapServers());
        properties.put("key.serializer", StringSerializer.class.getName());
        properties.put("value.serializer", StringSerializer.class.getName());

        // Create Kafka producer instance
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        // Create sample event to send to Kafka

        String eventString = eventJson.toString();

        // Create Kafka producer record
        ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC_NAME, eventString);

        // Send event to Kafka and do not wait for acknowledgement
        for (int i = 0; i < 3; i++) {
            producer.send(record);
        }

        // Close Kafka producer instance
        producer.close();
    }

    void consume() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "my-group");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        // Set auto.offset.reset to earliest
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        // Create a KafkaConsumer instance
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);

        // Subscribe to a topic
        consumer.subscribe(Collections.singletonList(TOPIC_NAME));

        // Start polling for messages
        for (int i = 0; i < 3; i++) {
            consumer.poll(Duration.ofMillis(100)).forEach(record -> {
                System.out.printf("Received message with key=%s and value=%s%n", record.key(), record.value());
            });
        }

        consumer.close(Duration.ofMillis(3000));
    }

    void createConnector(String address, Integer port) {
        OkHttpClient client = new OkHttpClient();

        MediaType mediaType = MediaType.parse("application/json");

        JSONObject requestBodyJson = new JSONObject();
        requestBodyJson.put("connector.class", "com.timeplus.kafkaconnect.TimeplusSinkConnector");
        requestBodyJson.put("tasks.max", 1);
        requestBodyJson.put("topics", TOPIC_NAME);
        requestBodyJson.put("timeplus.sink.address", "http://proton:3218");
        requestBodyJson.put("timeplus.sink.workspace", "");
        requestBodyJson.put("timeplus.sink.stream", STREAM);
        requestBodyJson.put("timeplus.sink.createStream", true);
        requestBodyJson.put("timeplus.sink.dataFormat", "raw");
        requestBodyJson.put("key.converter", "org.apache.kafka.connect.storage.StringConverter");
        requestBodyJson.put("value.converter", "org.apache.kafka.connect.storage.StringConverter");

        RequestBody body = RequestBody.create(requestBodyJson.toString(), mediaType);

        Request request = new Request.Builder()
                .url("http://" + address + ":" + port + "/connectors/timeplus-sink/config")
                .put(body)
                .addHeader("Content-Type", "application/json")
                .build();

        Response response = null;
        try {
            response = client.newCall(request).execute();
            if (response != null && response.isSuccessful()) {
                System.out.println(response.body().string());
                System.out.println("connector is created!");
            } else {

            }
        } catch (IOException ex) {
            Assertions.fail(ex);
        } finally {
            if (response != null) {
                response.close();
            }
        }
    }

    void deleteConnector(String address, Integer port) throws IOException {
        OkHttpClient client = new OkHttpClient();

        MediaType mediaType = MediaType.parse("application/json");
        RequestBody body = RequestBody.create("", mediaType);

        Request request = new Request.Builder()
                .url("http://" + address + ":" + port + "/connectors/timeplus-sink")
                .delete(body)
                .addHeader("Content-Type", "application/json")
                .build();

        Response response = client.newCall(request).execute();
        System.out.println(response.body().string());
    }

    void deleteStream() throws IOException {
        OkHttpClient client = new OkHttpClient();
        String url = "http://localhost:" + protonMappedPort + "/proton/v1/ddl/streams/" + STREAM;

        Request request = new Request.Builder()
                .url(url)
                .delete()
                .addHeader("Content-Type", "application/json")
                .build();

        Response response = client.newCall(request).execute();
        System.out.println(response.body().string());
    }

    JSONArray query() {
        OkHttpClient client = new OkHttpClient();
        String url = "http://localhost:" + protonMappedPort + "/proton/v1/search";

        MediaType mediaType = MediaType.parse("application/json");
        JSONObject requestBodyJson = new JSONObject();
        requestBodyJson.put("query", "select * from table(" + STREAM + ") where _tp_time > now() -1h");

        RequestBody body = RequestBody.create(requestBodyJson.toString(), mediaType);

        Request request = new Request.Builder()
                .url(url)
                .post(body)
                .addHeader("Content-Type", "application/json")
                .build();

        try {
            Response response = client.newCall(request).execute();
            if (response.isSuccessful()) {
                System.out.println("POST request successful");
                String responseBody = response.body().string();
                JSONObject repsonseObject = new JSONObject(responseBody);
                JSONArray data = repsonseObject.getJSONArray("data");
                return data;
            } else {
                System.out.println("POST request failed");
                System.out.println("Response code: " + response.code());
                System.out.println("Response message: " + response.message());
                System.out.println("Response: " + response.body().string());
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        return null;
    }

    @Test
    public void testKafkaConnectSink() {
        String address = kafkaConect.getHost();
        Integer port = kafkaConect.getFirstMappedPort();

        Assertions.assertNotNull(address);
        Assertions.assertNotNull(port);

        System.out.println("kafka connect is running at " + address);
        System.out.println("kafka connect is running at " + port);

        // send 3 test event and validate them
        try {
            produce();
            consume();
        } catch (Exception exception) {
            Assertions.fail(exception);
        }

        // te kafka timeplus sink connector
        createConnector(address, port);

        System.out.println("Connecter created!");
        // wait connector to be created
        try {
            Thread.sleep(10000); // sleep for 10 seconds
        } catch (InterruptedException e) {
            Assertions.fail(e);
        }

        // final String logs = kafkaConect.getLogs();
        // System.out.println(logs);

        // query timeplus to make sure event has been ingested
        JSONArray queryResult = query();

        int queryResultSize = queryResult.length();

        Assertions.assertEquals(queryResultSize, 3, "the query should contain 3 events");

        for (Object events : queryResult) {
            System.out.println("get a result : " + events.toString());
        }

        // delete the connectors
        try {
            deleteConnector(address, port);
        } catch (Exception exception) {
            Assertions.fail(exception);
        }
    }
}

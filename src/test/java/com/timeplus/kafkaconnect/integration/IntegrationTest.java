package com.timeplus.kafkaconnect.integration;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;

import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.images.PullPolicy;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.utility.DockerImageName;

import okhttp3.internal.sse.RealEventSource;
import okhttp3.sse.EventSource;
import okhttp3.sse.EventSourceListener;
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
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class IntegrationTest {
    private final static String TOPIC_NAME = "test-topic";
    private final static String STREAM = "test_kafka_connect_timeplus_sink";

    Network network = Network.newNetwork();
    JSONObject eventJson = new JSONObject();

    @Container
    public KafkaContainer kafka = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.0.1")).withKraft()
            .withNetwork(network);;

    @Container
    public GenericContainer kafkaConect = new GenericContainer(
            DockerImageName.parse("timeplus/timeplus-kafkaconnect-sink:latest"))
            .withImagePullPolicy(PullPolicy.alwaysPull())
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

    void createConnector(String address, Integer port) throws IOException {
        OkHttpClient client = new OkHttpClient();

        MediaType mediaType = MediaType.parse("application/json");

        JSONObject requestBodyJson = new JSONObject();
        requestBodyJson.put("connector.class", "com.timeplus.kafkaconnect.TimeplusSinkConnector");
        requestBodyJson.put("tasks.max", 1);
        requestBodyJson.put("topics", TOPIC_NAME);
        requestBodyJson.put("timeplus.sink.address", System.getenv("TIMEPLUS_ADDRESS"));
        requestBodyJson.put("timeplus.sink.workspace", System.getenv("TIMEPLUS_WORKSPACE"));
        requestBodyJson.put("timeplus.sink.apikey", System.getenv("TIMEPLUS_API_KEY"));
        requestBodyJson.put("timeplus.sink.stream", STREAM);
        requestBodyJson.put("timeplus.sink.createStream", true);
        requestBodyJson.put("timeplus.sink.dataFormat", "json");
        requestBodyJson.put("key.converter", "org.apache.kafka.connect.storage.StringConverter");
        requestBodyJson.put("value.converter", "org.apache.kafka.connect.storage.StringConverter");

        RequestBody body = RequestBody.create(requestBodyJson.toString(), mediaType);

        Request request = new Request.Builder()
                .url("http://" + address + ":" + port + "/connectors/timeplus-sink/config")
                .put(body)
                .addHeader("Content-Type", "application/json")
                .build();

        Response response = client.newCall(request).execute();
        System.out.println(response.body().string());
        System.out.println("connector is created!");
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
        String url = System.getenv("TIMEPLUS_ADDRESS") + "/" + System.getenv("TIMEPLUS_WORKSPACE")
                + "/api/v1beta1/streams/" + STREAM;

        Request request = new Request.Builder()
                .url(url)
                .delete()
                .addHeader("Content-Type", "application/json")
                .addHeader("X-API-KEY", System.getenv("TIMEPLUS_API_KEY"))
                .build();

        Response response = client.newCall(request).execute();
        System.out.println(response.body().string());
    }

    List<JSONArray> query() {
        OkHttpClient client = new OkHttpClient();
        String url = System.getenv("TIMEPLUS_ADDRESS") + "/" + System.getenv("TIMEPLUS_WORKSPACE")
                + "/api/v1beta2/queries/";

        MediaType mediaType = MediaType.parse("application/json");
        JSONObject requestBodyJson = new JSONObject();
        requestBodyJson.put("sql", "select * from table(" + STREAM + ")");

        RequestBody body = RequestBody.create(requestBodyJson.toString(), mediaType);

        Request request = new Request.Builder()
                .url(url)
                .post(body)
                .addHeader("Content-Type", "application/json")
                .addHeader("X-API-KEY", System.getenv("TIMEPLUS_API_KEY"))
                .build();

        List<JSONArray> result = new ArrayList<JSONArray>();

        EventSourceListener listener = new EventSourceListener() {
            @Override
            public void onEvent(EventSource eventSource, String id, String type, String data) {
                if (type == null) {
                    JSONArray events = new JSONArray(data);
                    result.add(events);
                }
            }

            @Override
            public void onOpen(EventSource eventSource, Response response) {
            }

            @Override
            public void onClosed(EventSource eventSource) {
            }

            @Override
            public void onFailure(EventSource eventSource, Throwable e,
                    Response response) {
                System.out.println(response);
                System.out.println(e);
                System.out.println(eventSource);
                Assertions.fail(e);
            }
        };

        RealEventSource eventSource = new RealEventSource(request, listener);
        eventSource.connect(client);

        try {
            Thread.sleep(5000); // sleep for 15 seconds
        } catch (InterruptedException e) {
            Assertions.fail(e);
        }

        eventSource.cancel();
        return result;
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

        // create kafka timeplus sink connector
        try {
            createConnector(address, port);
        } catch (Exception exception) {
            Assertions.fail(exception);
        }

        System.out.println("Connecter created!");
        // wait event being handled
        try {
            Thread.sleep(30000); // sleep for 15 seconds
        } catch (InterruptedException e) {
            Assertions.fail(e);
        }

        final String logs = kafkaConect.getLogs();
        System.out.println(logs);

        // // query timeplus to make sure event has been ingested
        // List<JSONArray> queryResult = query();

        // int queryResultSize = queryResult.stream().map(n -> n.length()).reduce(0, (a,
        // b) -> a + b);
        // Assertions.assertEquals(queryResultSize, 3, "the query should contain 3
        // events");

        // for (JSONArray events : queryResult) {
        // for (Object obj : events) {
        // JSONArray row = new JSONArray(obj.toString());
        // Assertions.assertEquals(row.getString(0), eventJson.getString("name"));
        // Assertions.assertEquals(row.getInt(1), eventJson.getInt("id"));
        // }
        // }

        // // delete the connectors
        // try {
        // deleteConnector(address, port);
        // } catch (Exception exception) {
        // Assertions.fail(exception);
        // }
    }
}

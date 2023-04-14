package com.timeplus.kafkaconnect.integration;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;

import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.utility.DockerImageName;

public class IntegrationTest {

    @Container
    public KafkaContainer kafka = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.0.1")).withKraft();

    @Container
    public GenericContainer kafkaConect = new GenericContainer(
            DockerImageName.parse("timeplus/timeplus-kafkaconnect-sink:dev_9bf5f82"))
            .withExposedPorts(8083);

    @BeforeEach
    public void setup() {
        kafka.start();
        kafkaConect.start();
    }

    @AfterEach
    public void tearDown() {
        kafkaConect.stop();
        kafka.stop();
    }

    @Test
    public void testKafkaReady() {
        System.out.println("kafka is running at " + kafka.getBootstrapServers());
    }

}

package com.tarmac.dispatch;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.tarmac.dispatch.repository.DeadLetterRepository;
import com.tarmac.dispatch.repository.IncidentRepository;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

/**
 * The whole choreography against real infrastructure: embedded Kafka broker,
 * real MongoDB in a container. Good events flow through to storage and the
 * assigned topic; poison events land in the DLT with a recorded reason,
 * queryable over REST — and never block the partition.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@EmbeddedKafka(partitions = 1,
        topics = {"incidents.reported", "incidents.reported.DLT", "incidents.assigned"})
@Testcontainers
class DispatchFlowTest {

    @Container
    @ServiceConnection
    static MongoDBContainer mongo = new MongoDBContainer("mongo:7");

    @Autowired
    private EmbeddedKafkaBroker broker;

    @Autowired
    private IncidentRepository incidentRepository;

    @Autowired
    private DeadLetterRepository deadLetterRepository;

    @Autowired
    private TestRestTemplate rest;

    private static Producer<String, String> producer;

    @BeforeAll
    static void beforeAll() {
        // created lazily against the running broker in the first test
    }

    private Producer<String, String> producer() {
        if (producer == null) {
            Map<String, Object> props = KafkaTestUtils.producerProps(broker);
            props.put("key.serializer", StringSerializer.class);
            props.put("value.serializer", StringSerializer.class);
            producer = new KafkaProducer<>(props);
        }
        return producer;
    }

    @AfterAll
    static void afterAll() {
        if (producer != null) {
            producer.close();
        }
    }

    @Test
    void goodEventIsStoredAssignedAndReemitted() {
        String json = """
                {
                  "incidentId": "inc-001",
                  "airport": "ATL",
                  "equipment": "XRAY_SCANNER",
                  "severity": "CRITICAL",
                  "laneName": "Lane 2",
                  "description": "tunnel sensor fault",
                  "reportedAt": "2026-07-04T10:15:30Z"
                }
                """;
        producer().send(new ProducerRecord<>("incidents.reported", "ATL", json));

        await().atMost(Duration.ofSeconds(30)).untilAsserted(() -> {
            var stored = incidentRepository.findById("inc-001");
            assertThat(stored).isPresent();
            assertThat(stored.get().getStatus()).isEqualTo("ASSIGNED");
            assertThat(stored.get().getAssignedCrew()).isEqualTo("rapid-response-1");
        });

        // the assignment event went back out for anyone else who cares
        Map<String, Object> consumerProps = KafkaTestUtils.consumerProps("assigned-checker", "true", broker);
        consumerProps.put("key.deserializer", StringDeserializer.class);
        consumerProps.put("value.deserializer", StringDeserializer.class);
        try (Consumer<String, String> consumer =
                new DefaultKafkaConsumerFactory<String, String>(consumerProps).createConsumer()) {
            broker.consumeFromAnEmbeddedTopic(consumer, "incidents.assigned");
            ConsumerRecord<String, String> record = KafkaTestUtils.getSingleRecord(
                    consumer, "incidents.assigned", Duration.ofSeconds(10));
            assertThat(record.value()).contains("inc-001");
            assertThat(record.value()).contains("rapid-response-1");
        }

        // and it's queryable over the REST side
        ResponseEntity<List> byAirport = rest.getForEntity("/api/v1/incidents?airport=atl", List.class);
        assertThat(byAirport.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(byAirport.getBody()).isNotEmpty();

        ResponseEntity<Map> byId = rest.getForEntity("/api/v1/incidents/inc-001", Map.class);
        assertThat(byId.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(byId.getBody().get("assignedCrew")).isEqualTo("rapid-response-1");
    }

    @Test
    void poisonMessagesGoToDeadLetterTopicNotIntoRetryHell() {
        long before = deadLetterRepository.count();

        // unparseable garbage → deserialization failure → DLT, no retries
        producer().send(new ProducerRecord<>("incidents.reported", "ATL", "this is not json"));
        // valid JSON but unprocessable → IllegalArgumentException → DLT, no retries
        producer().send(new ProducerRecord<>("incidents.reported", "ATL",
                "{\"description\": \"an event with no id, airport or severity\"}"));

        await().atMost(Duration.ofSeconds(30)).untilAsserted(() ->
                assertThat(deadLetterRepository.count()).isGreaterThanOrEqualTo(before + 2));

        ResponseEntity<List> deadLetters = rest.getForEntity("/api/v1/deadletters", List.class);
        assertThat(deadLetters.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(deadLetters.getBody()).isNotEmpty();

        // and neither poison message became an incident
        ResponseEntity<Map> missing = rest.getForEntity("/api/v1/incidents/does-not-exist", Map.class);
        assertThat(missing.getStatusCode()).isEqualTo(HttpStatus.NOT_FOUND);
    }
}

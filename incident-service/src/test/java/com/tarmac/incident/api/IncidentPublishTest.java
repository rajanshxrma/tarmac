package com.tarmac.incident.api;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.Map;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;

/**
 * Boots the real service against an embedded Kafka broker (no Docker needed),
 * reports an incident over HTTP, and consumes the topic to prove the event
 * actually landed — key, payload and all.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@EmbeddedKafka(partitions = 3, topics = {"incidents.reported"})
class IncidentPublishTest {

    @Autowired
    private TestRestTemplate rest;

    @Autowired
    private EmbeddedKafkaBroker broker;

    private static Consumer<String, String> consumer;

    @BeforeAll
    static void setUpClass() {
        // consumer is created per-broker in the first test that needs it
    }

    private Consumer<String, String> testConsumer() {
        if (consumer == null) {
            Map<String, Object> props = KafkaTestUtils.consumerProps("test-group", "true", broker);
            props.put("key.deserializer", StringDeserializer.class);
            props.put("value.deserializer", StringDeserializer.class);
            consumer = new DefaultKafkaConsumerFactory<String, String>(props).createConsumer();
            broker.consumeFromAnEmbeddedTopic(consumer, "incidents.reported");
        }
        return consumer;
    }

    @AfterAll
    static void tearDownClass() {
        if (consumer != null) {
            consumer.close();
        }
    }

    @Test
    void reportedIncidentIsPublishedWithAirportAsKey() {
        ResponseEntity<Map> response = rest.postForEntity("/api/v1/incidents", Map.of(
                "airport", "atl",
                "equipment", "BAGGAGE_CONVEYOR",
                "severity", "HIGH",
                "laneName", "Lane 4",
                "description", "belt jammed at intake"), Map.class);

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.ACCEPTED);
        String incidentId = (String) response.getBody().get("incidentId");
        assertThat(incidentId).isNotBlank();

        ConsumerRecord<String, String> record = KafkaTestUtils.getSingleRecord(
                testConsumer(), "incidents.reported", Duration.ofSeconds(10));

        assertThat(record.key()).isEqualTo("ATL"); // normalized + used as partition key
        assertThat(record.value()).contains(incidentId);
        assertThat(record.value()).contains("BAGGAGE_CONVEYOR");
        assertThat(record.value()).contains("HIGH");
    }

    @Test
    @SuppressWarnings("unchecked")
    void invalidReportIsRejectedWithFieldErrors() {
        ResponseEntity<Map> response = rest.postForEntity("/api/v1/incidents", Map.of(
                "airport", "atlanta",
                "severity", "HIGH"), Map.class);

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);
        Map<String, Object> fieldErrors = (Map<String, Object>) response.getBody().get("fieldErrors");
        assertThat(fieldErrors).containsKey("airport");
        assertThat(fieldErrors).containsKey("equipment");
    }

    @Test
    void unknownEnumIsA400NotA500() {
        ResponseEntity<Map> response = rest.postForEntity("/api/v1/incidents", Map.of(
                "airport", "ATL",
                "equipment", "TELEPORTER",
                "severity", "HIGH"), Map.class);

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);
    }
}

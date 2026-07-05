package com.tarmac.incident.api;

import com.tarmac.incident.event.IncidentReportedEvent;
import jakarta.validation.Valid;
import java.time.Instant;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/v1/incidents")
public class IncidentController {

    private static final Logger log = LoggerFactory.getLogger(IncidentController.class);

    // Boot's auto-configured template is KafkaTemplate<Object, Object>; asking
    // for narrower generics here would fail Spring's generic type matching
    private final KafkaTemplate<Object, Object> kafkaTemplate;
    private final String topic;

    public IncidentController(KafkaTemplate<Object, Object> kafkaTemplate,
            @Value("${tarmac.topics.incidents-reported}") String topic) {
        this.kafkaTemplate = kafkaTemplate;
        this.topic = topic;
    }

    /**
     * Accepts an incident report and publishes it. The airport is the message
     * key, so all events for one airport land on the same partition and stay
     * ordered relative to each other — cross-airport ordering doesn't matter.
     */
    @PostMapping
    public ResponseEntity<?> report(@Valid @RequestBody ReportIncidentRequest request) {
        IncidentReportedEvent event = new IncidentReportedEvent(
                UUID.randomUUID().toString(),
                request.airport().toUpperCase(),
                request.equipment().name(),
                request.severity().name(),
                request.laneName(),
                request.description(),
                Instant.now());

        try {
            // block briefly for the broker ack (acks=all) so the caller gets an
            // honest answer: 202 means the event is durably in Kafka, not "maybe"
            kafkaTemplate.send(topic, event.airport(), event).get(5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return brokerUnavailable(event.incidentId(), e);
        } catch (Exception e) {
            return brokerUnavailable(event.incidentId(), e);
        }

        log.info("Published incident {} for {} ({} {})", event.incidentId(), event.airport(),
                event.severity(), event.equipment());

        return ResponseEntity.status(HttpStatus.ACCEPTED).body(Map.of(
                "incidentId", event.incidentId(),
                "status", "REPORTED"));
    }

    private ResponseEntity<?> brokerUnavailable(String incidentId, Exception e) {
        log.error("Could not publish incident {}", incidentId, e);
        return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE).body(Map.of(
                "error", "event broker unavailable, try again",
                "incidentId", incidentId));
    }
}

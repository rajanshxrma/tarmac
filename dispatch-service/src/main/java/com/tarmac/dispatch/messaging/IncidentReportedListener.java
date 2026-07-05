package com.tarmac.dispatch.messaging;

import com.tarmac.dispatch.domain.IncidentDocument;
import com.tarmac.dispatch.event.IncidentAssignedEvent;
import com.tarmac.dispatch.event.IncidentReportedEvent;
import com.tarmac.dispatch.repository.IncidentRepository;
import com.tarmac.dispatch.service.CrewAssigner;
import java.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Component
public class IncidentReportedListener {

    private static final Logger log = LoggerFactory.getLogger(IncidentReportedListener.class);

    private final IncidentRepository incidentRepository;
    private final CrewAssigner crewAssigner;
    private final KafkaTemplate<Object, Object> kafkaTemplate;
    private final String assignedTopic;

    public IncidentReportedListener(IncidentRepository incidentRepository, CrewAssigner crewAssigner,
            KafkaTemplate<Object, Object> kafkaTemplate,
            @Value("${tarmac.topics.incidents-assigned}") String assignedTopic) {
        this.incidentRepository = incidentRepository;
        this.crewAssigner = crewAssigner;
        this.kafkaTemplate = kafkaTemplate;
        this.assignedTopic = assignedTopic;
    }

    /**
     * At-least-once processing: the document id IS the event's incidentId, so
     * a redelivered event overwrites the same document instead of duplicating
     * it — idempotency through natural keys rather than dedupe bookkeeping.
     * Events that can never be processed (missing required fields) throw
     * IllegalArgumentException, which is configured as not-retryable and goes
     * straight to the dead-letter topic.
     */
    @KafkaListener(topics = "${tarmac.topics.incidents-reported}")
    public void onIncidentReported(IncidentReportedEvent event) {
        if (event.incidentId() == null || event.airport() == null || event.severity() == null) {
            throw new IllegalArgumentException("event is missing required fields: " + event);
        }

        String crew = crewAssigner.assign(event.severity());
        Instant now = Instant.now();

        IncidentDocument document = new IncidentDocument();
        document.setId(event.incidentId());
        document.setAirport(event.airport());
        document.setEquipment(event.equipment());
        document.setSeverity(event.severity());
        document.setLaneName(event.laneName());
        document.setDescription(event.description());
        document.setReportedAt(event.reportedAt());
        document.setStatus("ASSIGNED");
        document.setAssignedCrew(crew);
        document.setAssignedAt(now);
        incidentRepository.save(document);

        kafkaTemplate.send(assignedTopic, event.airport(),
                new IncidentAssignedEvent(event.incidentId(), event.airport(), crew, now));

        log.info("Incident {} at {} assigned to {}", event.incidentId(), event.airport(), crew);
    }
}

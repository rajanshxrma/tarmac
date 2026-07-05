package com.tarmac.dispatch.messaging;

import com.tarmac.dispatch.domain.DeadLetterDocument;
import com.tarmac.dispatch.repository.DeadLetterRepository;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.stereotype.Component;

/**
 * Watches the dead-letter topic and persists every poisoned message with the
 * failure reason the error handler attached — so bad events are queryable
 * (GET /api/v1/deadletters) instead of rotting invisibly in a topic.
 */
@Component
public class DeadLetterListener {

    private static final Logger log = LoggerFactory.getLogger(DeadLetterListener.class);

    private final DeadLetterRepository deadLetterRepository;

    public DeadLetterListener(DeadLetterRepository deadLetterRepository) {
        this.deadLetterRepository = deadLetterRepository;
    }

    @KafkaListener(topics = "${tarmac.topics.incidents-reported}.DLT",
            containerFactory = "dltContainerFactory")
    public void onDeadLetter(ConsumerRecord<String, String> record) {
        String reason = headerAsString(record, KafkaHeaders.DLT_EXCEPTION_MESSAGE);
        deadLetterRepository.save(new DeadLetterDocument(
                record.topic(),
                record.value(),
                reason != null ? reason : "unknown failure",
                Instant.now()));
        log.warn("Dead letter captured from {}: {}", record.topic(), reason);
    }

    private String headerAsString(ConsumerRecord<String, String> record, String headerName) {
        Header header = record.headers().lastHeader(headerName);
        return header == null ? null : new String(header.value(), StandardCharsets.UTF_8);
    }
}

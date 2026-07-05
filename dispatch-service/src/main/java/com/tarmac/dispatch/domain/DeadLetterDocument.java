package com.tarmac.dispatch.domain;

import java.time.Instant;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;

@Document("dead_letters")
public class DeadLetterDocument {

    @Id
    private String id;
    private String topic;
    private String payload;
    private String reason;
    private Instant failedAt;

    public DeadLetterDocument() {
    }

    public DeadLetterDocument(String topic, String payload, String reason, Instant failedAt) {
        this.topic = topic;
        this.payload = payload;
        this.reason = reason;
        this.failedAt = failedAt;
    }

    public String getId() {
        return id;
    }

    public String getTopic() {
        return topic;
    }

    public String getPayload() {
        return payload;
    }

    public String getReason() {
        return reason;
    }

    public Instant getFailedAt() {
        return failedAt;
    }
}

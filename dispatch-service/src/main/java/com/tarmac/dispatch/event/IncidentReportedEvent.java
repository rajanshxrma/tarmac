package com.tarmac.dispatch.event;

import java.time.Instant;

/**
 * Consumer-side copy of the incidents.reported contract. Deliberately not a
 * shared jar with incident-service — tolerant-reader: unknown fields are
 * ignored on deserialization, so the producer can add fields without breaking
 * this service.
 */
public record IncidentReportedEvent(
        String incidentId,
        String airport,
        String equipment,
        String severity,
        String laneName,
        String description,
        Instant reportedAt) {
}

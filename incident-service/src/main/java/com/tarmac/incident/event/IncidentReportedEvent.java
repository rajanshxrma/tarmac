package com.tarmac.incident.event;

import java.time.Instant;

/**
 * The contract published to incidents.reported. The dispatch service keeps its
 * own copy of this shape — each service owns its contract instead of sharing a
 * jar, so one side can evolve without force-upgrading the other (tolerant
 * reader). In a bigger system this would live in a schema registry.
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

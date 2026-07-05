package com.tarmac.dispatch.event;

import java.time.Instant;

public record IncidentAssignedEvent(
        String incidentId,
        String airport,
        String assignedCrew,
        Instant assignedAt) {
}

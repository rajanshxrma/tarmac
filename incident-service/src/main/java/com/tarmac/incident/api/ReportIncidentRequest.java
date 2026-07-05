package com.tarmac.incident.api;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Pattern;
import jakarta.validation.constraints.Size;

public record ReportIncidentRequest(
        @NotBlank(message = "airport is required")
        @Pattern(regexp = "[A-Za-z]{3}", message = "airport must be a 3-letter IATA code")
        String airport,

        @NotNull(message = "equipment is required")
        Equipment equipment,

        @NotNull(message = "severity is required")
        Severity severity,

        String laneName,

        @Size(max = 1000, message = "description is capped at 1000 characters")
        String description) {

    public enum Equipment {
        XRAY_SCANNER,
        METAL_DETECTOR,
        BODY_SCANNER,
        EXPLOSIVE_TRACE_DETECTOR,
        BAGGAGE_CONVEYOR
    }

    public enum Severity {
        LOW,
        MEDIUM,
        HIGH,
        CRITICAL
    }
}

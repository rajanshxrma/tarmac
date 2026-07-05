package com.tarmac.dispatch.service;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.springframework.stereotype.Component;

/**
 * Deliberately simple dispatch rules: criticals always get the rapid-response
 * crew, highs alternate between rapid-response backup and the day shift,
 * everything else round-robins across day crews. Simple, deterministic,
 * unit-testable — the point is the event flow around it.
 */
@Component
public class CrewAssigner {

    private static final String RAPID_RESPONSE_PRIMARY = "rapid-response-1";
    private static final List<String> HIGH_ROTATION = List.of("rapid-response-2", "day-crew-1");
    private static final List<String> DAY_ROTATION = List.of("day-crew-1", "day-crew-2", "day-crew-3");

    private final AtomicInteger highCounter = new AtomicInteger();
    private final AtomicInteger dayCounter = new AtomicInteger();

    public String assign(String severity) {
        if ("CRITICAL".equals(severity)) {
            return RAPID_RESPONSE_PRIMARY;
        }
        if ("HIGH".equals(severity)) {
            return HIGH_ROTATION.get(Math.floorMod(highCounter.getAndIncrement(), HIGH_ROTATION.size()));
        }
        return DAY_ROTATION.get(Math.floorMod(dayCounter.getAndIncrement(), DAY_ROTATION.size()));
    }
}

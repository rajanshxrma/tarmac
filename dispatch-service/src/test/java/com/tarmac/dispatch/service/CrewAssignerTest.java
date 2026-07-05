package com.tarmac.dispatch.service;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class CrewAssignerTest {

    @Test
    void criticalAlwaysGetsRapidResponse() {
        CrewAssigner assigner = new CrewAssigner();
        assertThat(assigner.assign("CRITICAL")).isEqualTo("rapid-response-1");
        assertThat(assigner.assign("CRITICAL")).isEqualTo("rapid-response-1");
        assertThat(assigner.assign("CRITICAL")).isEqualTo("rapid-response-1");
    }

    @Test
    void highSeverityAlternatesBetweenBackupAndDayShift() {
        CrewAssigner assigner = new CrewAssigner();
        String first = assigner.assign("HIGH");
        String second = assigner.assign("HIGH");
        String third = assigner.assign("HIGH");

        assertThat(first).isEqualTo("rapid-response-2");
        assertThat(second).isEqualTo("day-crew-1");
        assertThat(third).isEqualTo(first);
    }

    @Test
    void everythingElseRoundRobinsAcrossDayCrews() {
        CrewAssigner assigner = new CrewAssigner();
        assertThat(assigner.assign("LOW")).isEqualTo("day-crew-1");
        assertThat(assigner.assign("MEDIUM")).isEqualTo("day-crew-2");
        assertThat(assigner.assign("LOW")).isEqualTo("day-crew-3");
        assertThat(assigner.assign("LOW")).isEqualTo("day-crew-1");
    }
}

package com.tarmac.incident.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

/**
 * Topics are declared in code so a fresh broker gets the right layout on
 * first boot. 3 partitions on the main topic: airport is the message key, so
 * partition count = how much consumer parallelism dispatch can scale to.
 */
@Configuration
public class TopicConfig {

    @Bean
    public NewTopic incidentsReportedTopic(
            @Value("${tarmac.topics.incidents-reported}") String topic) {
        return TopicBuilder.name(topic).partitions(3).replicas(1).build();
    }

    @Bean
    public NewTopic incidentsReportedDltTopic(
            @Value("${tarmac.topics.incidents-reported}") String topic) {
        return TopicBuilder.name(topic + ".DLT").partitions(3).replicas(1).build();
    }

    @Bean
    public NewTopic incidentsAssignedTopic(
            @Value("${tarmac.topics.incidents-assigned}") String topic) {
        return TopicBuilder.name(topic).partitions(3).replicas(1).build();
    }
}

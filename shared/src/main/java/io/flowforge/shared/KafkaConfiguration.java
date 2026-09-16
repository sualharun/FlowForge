package io.flowforge.shared;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.FixedBackOff;

@Configuration
public class KafkaConfiguration {
    @Bean KafkaAdmin.NewTopics flowforgeTopics(@Value("${flowforge.topic-partitions:12}") int partitions,
            @Value("${flowforge.topic-replicas:1}") int replicas) {
        return new KafkaAdmin.NewTopics(java.util.stream.Stream.of(Topics.READY,Topics.RESULTS,Topics.RETRY,Topics.DEAD_LETTER)
                .map(name -> TopicBuilder.name(name).partitions(partitions).replicas(replicas).build()).toArray(NewTopic[]::new));
    }
    /** Never commit past transient infrastructure failures. Invalid external records require operator remediation. */
    @Bean DefaultErrorHandler kafkaErrorHandler() {
        var handler = new DefaultErrorHandler(new FixedBackOff(1000L, FixedBackOff.UNLIMITED_ATTEMPTS));
        handler.addRetryableExceptions(Exception.class);
        return handler;
    }
}

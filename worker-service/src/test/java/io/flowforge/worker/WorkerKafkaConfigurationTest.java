package io.flowforge.worker;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.autoconfigure.kafka.KafkaAutoConfiguration;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.DefaultErrorHandler;

class WorkerKafkaConfigurationTest {
    @Test
    void sharedRecordAckDefaultCannotOverrideDurableClaimAcknowledgment() {
        new ApplicationContextRunner()
                .withConfiguration(AutoConfigurations.of(KafkaAutoConfiguration.class))
                .withUserConfiguration(WorkerKafkaConfiguration.class)
                .withBean(DefaultErrorHandler.class, DefaultErrorHandler::new)
                .withPropertyValues("spring.kafka.listener.ack-mode=record")
                .run(context -> {
                    assertThat(context).hasNotFailed();
                    var factory = context.getBean("kafkaListenerContainerFactory", ConcurrentKafkaListenerContainerFactory.class);
                    assertThat(factory.getContainerProperties().getAckMode())
                            .isEqualTo(ContainerProperties.AckMode.MANUAL_IMMEDIATE);
                });
    }
}

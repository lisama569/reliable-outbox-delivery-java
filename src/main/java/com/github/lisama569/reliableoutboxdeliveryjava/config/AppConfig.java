package com.github.lisama569.reliableoutboxdeliveryjava.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.lisama569.reliableoutboxdeliveryjava.outbox.OutboxProperties;
import com.github.lisama569.reliableoutboxdeliveryjava.queue.FailureSimulator;
import com.github.lisama569.reliableoutboxdeliveryjava.queue.LocalMessageQueue;
import com.github.lisama569.reliableoutboxdeliveryjava.queue.MessageQueue;
import com.github.lisama569.reliableoutboxdeliveryjava.queue.QueueProperties;
import com.github.lisama569.reliableoutboxdeliveryjava.support.JsonSupport;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
@EnableConfigurationProperties({OutboxProperties.class, QueueProperties.class})
public class AppConfig {

    @Bean
    public JsonSupport jsonSupport(ObjectMapper objectMapper) {
        return new JsonSupport(objectMapper);
    }

    @Bean
    public FailureSimulator failureSimulator() {
        return new FailureSimulator();
    }

    @Bean
    public MessageQueue messageQueue(QueueProperties queueProperties,
                                     FailureSimulator failureSimulator) {
        return new LocalMessageQueue(queueProperties, failureSimulator);
    }
}

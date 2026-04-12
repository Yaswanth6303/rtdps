package me.yswnthdev.rtdps.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.util.backoff.FixedBackOff;

@Configuration
public class KafkaConfig {

    // ======================== TOPICS ========================

    @Bean
    public NewTopic jobRequestsTopic() {
        return TopicBuilder.name("job-requests")
                .partitions(3)
                .replicas(1)
                .build();
    }

    @Bean
    public NewTopic jobEventsTopic() {
        return TopicBuilder.name("job-events")
                .partitions(3)
                .replicas(1)
                .build();
    }

    @Bean
    public NewTopic jobRequestsDlqTopic() {
        return TopicBuilder.name("job-requests.DLT")
                .partitions(1)
                .replicas(1)
                .build();
    }

    // ======================== ERROR HANDLING ========================

    @Bean
    public DefaultErrorHandler errorHandler(KafkaTemplate<String, Object> kafkaTemplate) {
        // Failed messages go to Dead Letter Topic after 3 retries with 1-second intervals
        DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(kafkaTemplate);
        return new DefaultErrorHandler(recoverer, new FixedBackOff(1000L, 3));
    }
}

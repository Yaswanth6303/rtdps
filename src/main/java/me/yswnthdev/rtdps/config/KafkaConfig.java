package me.yswnthdev.rtdps.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.support.serializer.DeserializationException;
import org.springframework.util.backoff.FixedBackOff;

import java.util.HashMap;
import java.util.Map;

@Configuration
public class KafkaConfig {

    @Value("${app.kafka.replicas:1}")
    private short replicas;

    @Value("${app.kafka.min-insync-replicas:1}")
    private String minInSyncReplicas;

    // ======================== TOPICS ========================

    @Bean
    public NewTopic jobRequestsTopic() {
        return TopicBuilder.name("job-requests")
                .partitions(3)
                .replicas(replicas)
                .config(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, minInSyncReplicas)
                .build();
    }

    @Bean
    public NewTopic jobEventsTopic() {
        return TopicBuilder.name("job-events")
                .partitions(3)
                .replicas(replicas)
                .config(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, minInSyncReplicas)
                .build();
    }

    @Bean
    public NewTopic jobRequestsDlqTopic() {
        return TopicBuilder.name("job-requests.DLT")
                .partitions(1)
                .replicas(replicas)
                .config(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, minInSyncReplicas)
                .build();
    }

    // ======================== ERROR HANDLING ========================

    @Bean
    public DefaultErrorHandler errorHandler(KafkaTemplate<String, Object> kafkaTemplate) {
        // Failed messages go to Dead Letter Topic after 3 retries with 1-second intervals.
        // Negative partition lets Kafka pick one — the default resolver reuses the source
        // partition, which doesn't exist on the 1-partition DLT for records from partitions 1–2.
        DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(kafkaTemplate,
                (record, ex) -> new TopicPartition(record.topic() + ".DLT", -1));
        DefaultErrorHandler handler = new DefaultErrorHandler(recoverer, new FixedBackOff(1000L, 3));
        // Don't waste retries on permanent failures — send straight to the DLT.
        handler.addNotRetryableExceptions(
                DeserializationException.class,
                IllegalArgumentException.class
        );
        return handler;
    }

    // ======================== DLT CONSUMER FACTORY ========================

    /**
     * Bytes-in, bytes-out factory used by {@code JobRequestDltConsumer}. The
     * DLT can contain payloads that the main JsonDeserializer couldn't parse,
     * so we deserialize defensively as {@code byte[]} and inspect manually.
     */
    @Bean
    public ConsumerFactory<String, byte[]> byteArrayConsumerFactory(
            @Value("${spring.kafka.bootstrap-servers:localhost:9092}") String bootstrap) {
        Map<String, Object> config = new HashMap<>();
        config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        return new DefaultKafkaConsumerFactory<>(config);
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, byte[]> dltKafkaListenerContainerFactory(
            ConsumerFactory<String, byte[]> byteArrayConsumerFactory) {
        ConcurrentKafkaListenerContainerFactory<String, byte[]> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(byteArrayConsumerFactory);
        return factory;
    }
}

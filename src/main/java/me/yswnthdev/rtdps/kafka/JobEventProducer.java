package me.yswnthdev.rtdps.kafka;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import me.yswnthdev.rtdps.dto.JobEvent;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
@Slf4j
public class JobEventProducer {

    private static final String TOPIC = "job-events";
    private final KafkaTemplate<String, Object> kafkaTemplate;

    public void publishJobEvent(JobEvent event) {
        log.info("[JobEventProducer] Publishing job event — tracker: {}, status: {}",
                event.getTrackerId(), event.getStatus());

        kafkaTemplate.send(TOPIC, event.getTrackerId(), event)
                .whenComplete((result, ex) -> {
                    if (ex != null) {
                        log.error("[JobEventProducer] Failed to publish event — tracker: {}",
                                event.getTrackerId(), ex);
                    } else {
                        log.info("[JobEventProducer] Event published — tracker: {}, partition: {}, offset: {}",
                                event.getTrackerId(),
                                result.getRecordMetadata().partition(),
                                result.getRecordMetadata().offset());
                    }
                });
    }
}

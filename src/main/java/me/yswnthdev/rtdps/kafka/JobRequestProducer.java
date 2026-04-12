package me.yswnthdev.rtdps.kafka;

import java.time.LocalDateTime;
import java.util.concurrent.CompletableFuture;

import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import me.yswnthdev.rtdps.dto.JobRequest;
import me.yswnthdev.rtdps.repository.JobTrackerRepository;

@Component
@RequiredArgsConstructor
@Slf4j
public class JobRequestProducer {

    private static final String TOPIC = "job-requests";
    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final JobTrackerRepository jobTrackerRepository;

    public void sendJobRequest(JobRequest request) {
        log.info("[JobRequestProducer] Sending job request — tracker: {}, type: {}",
                request.getTrackerId(), request.getJobType());

        CompletableFuture<SendResult<String, Object>> future =
                kafkaTemplate.send(TOPIC, request.getTrackerId(), request);

        future.whenComplete((result, ex) -> {
            if (ex != null) {
                log.error("[JobRequestProducer] Failed to send message — tracker: {}",
                        request.getTrackerId(), ex);
                jobTrackerRepository.findById(request.getTrackerId()).ifPresent(tracker -> {
                    tracker.setStatus("FAILED");
                    tracker.setErrorMessage("Failed to queue job: " + ex.getMessage());
                    tracker.setUpdatedAt(LocalDateTime.now());
                    jobTrackerRepository.save(tracker);
                });
            } else {
                log.info("[JobRequestProducer] Message sent — tracker: {}, partition: {}, offset: {}",
                        request.getTrackerId(),
                        result.getRecordMetadata().partition(),
                        result.getRecordMetadata().offset());
            }
        });
    }
}

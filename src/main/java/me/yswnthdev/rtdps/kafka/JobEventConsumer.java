package me.yswnthdev.rtdps.kafka;

import lombok.extern.slf4j.Slf4j;
import me.yswnthdev.rtdps.dto.JobEvent;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
@Slf4j
public class JobEventConsumer {

    @KafkaListener(topics = "job-events", groupId = "rtdps-events-group")
    public void consumeJobEvent(JobEvent event) {
        log.info("========================================================");
        log.info("[JobEventConsumer] JOB EVENT RECEIVED");
        log.info("  Tracker ID  : {}", event.getTrackerId());
        log.info("  Execution ID: {}", event.getExecutionId());
        log.info("  Job Name    : {}", event.getJobName());
        log.info("  Status      : {}", event.getStatus());
        log.info("  Read Count  : {}", event.getReadCount());
        log.info("  Write Count : {}", event.getWriteCount());
        log.info("  Skip Count  : {}", event.getSkipCount());
        log.info("  Duration    : {} ms", event.getDurationMs());
        log.info("  Completed At: {}", event.getCompletedAt());
        log.info("========================================================");

        // In a real-world scenario, this is where you would:
        // - Send email notifications
        // - Update a dashboard via WebSocket
        // - Trigger downstream data pipelines
        // - Write to an audit log
    }
}

package me.yswnthdev.rtdps.alerting;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

/**
 * Always-on publisher: emits a structured ERROR log that log shippers
 * (Loki, Datadog, Splunk) can route to alert channels.
 */
@Component
@Slf4j
public class LoggingAlertPublisher implements AlertPublisher {

    @Override
    public void publish(DltAlert a) {
        log.error("[DLT_ALERT] trackerId={} origin={}-{}@{} exception={} message=\"{}\" payload=\"{}\"",
                a.trackerId(),
                a.originalTopic(), a.originalPartition(), a.originalOffset(),
                a.exceptionClass(),
                sanitize(a.exceptionMessage()),
                sanitize(a.payloadPreview()));
    }

    private String sanitize(String s) {
        if (s == null) return "";
        return s.replace('\n', ' ').replace('\r', ' ').replace('"', '\'');
    }
}

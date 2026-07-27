package me.yswnthdev.rtdps.alerting;

/**
 * Fan-out target for poison-message alerts. The DLT consumer iterates every
 * registered {@code AlertPublisher} bean — implementations are expected to be
 * non-blocking or fail fast; the consumer thread is the same one polling
 * Kafka.
 */
public interface AlertPublisher {
    void publish(DltAlert alert);
}

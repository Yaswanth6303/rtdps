package me.yswnthdev.rtdps.alerting;

import java.time.Instant;

/**
 * Snapshot of a poison message that landed on the DLT. Designed for both
 * structured logging and JSON serialization to a webhook.
 */
public record DltAlert(
        String trackerId,
        String originalTopic,
        Integer originalPartition,
        Long originalOffset,
        String exceptionClass,
        String exceptionMessage,
        String payloadPreview,   // first 512 chars of the raw value; bytes that decode poorly become '?'
        Instant detectedAt
) { }

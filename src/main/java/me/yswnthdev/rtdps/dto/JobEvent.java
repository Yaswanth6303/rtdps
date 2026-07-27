package me.yswnthdev.rtdps.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class JobEvent {
    private String trackerId;
    private Long executionId;
    private String jobName;
    private String status;
    private long readCount;
    private long writeCount;
    private long skipCount;

    // Honest split of writeCount — populated for IMPORT jobs by ConflictAwareCustomerWriter.
    private long insertedCount;
    private long updatedCount;
    private long conflictCount;

    // Rows dropped by the processor (CustomerProcessor returning null on invalid email).
    private long filteredCount;

    private long durationMs;
    private LocalDateTime completedAt;
}

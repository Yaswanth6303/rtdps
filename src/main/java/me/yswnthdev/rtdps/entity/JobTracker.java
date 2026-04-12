package me.yswnthdev.rtdps.entity;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;

@Entity
@Table(name = "job_tracker")
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class JobTracker {

    @Id
    private String id;

    @Column(nullable = false)
    private String jobType;

    private String filePath;

    @Column(nullable = false)
    private String status;

    private Long executionId;

    @Builder.Default
    private long readCount = 0;

    @Builder.Default
    private long writeCount = 0;

    @Builder.Default
    private long skipCount = 0;

    @Builder.Default
    private long durationMs = 0;

    @Column(columnDefinition = "TEXT")
    private String errorMessage;

    @Column(nullable = false)
    private LocalDateTime createdAt;

    private LocalDateTime updatedAt;
}

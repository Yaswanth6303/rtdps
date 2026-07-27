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
public class JobRequest {
    private String trackerId;
    private String jobType;          // IMPORT / EXPORT
    private String filePath;         // durable storage location (s3://… or local path)
    private String conflictPolicy;   // SKIP / UPDATE — only meaningful for IMPORT; null => SKIP
    private LocalDateTime requestedAt;
}

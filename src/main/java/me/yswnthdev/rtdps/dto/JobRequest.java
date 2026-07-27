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
    private String jobType;          // IMPORT
    private String filePath;         // local path to the CSV to import
    private LocalDateTime requestedAt;
}

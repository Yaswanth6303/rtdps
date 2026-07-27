package me.yswnthdev.rtdps.storage;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.core.io.InputStreamResource;
import org.springframework.core.io.Resource;
import org.springframework.stereotype.Component;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.S3Exception;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;

/**
 * S3-backed storage. Locations are returned as {@code s3://bucket/key}.
 * <p>
 * Credentials use the AWS SDK's {@link DefaultCredentialsProvider} chain:
 * environment variables → system properties → web identity → profile file →
 * container/IMDS. In production, prefer an attached IAM role over baked-in keys.
 */
@Component
@Slf4j
@ConditionalOnProperty(name = "app.storage.type", havingValue = "s3")
public class S3StorageBackend implements StorageBackend {

    private final String bucket;
    private final String uploadPrefix;
    private final String exportPrefix;
    private final Region region;
    private final URI endpointOverride;

    private S3Client s3;

    public S3StorageBackend(
            @Value("${app.storage.s3.bucket}") String bucket,
            @Value("${app.storage.s3.upload-prefix:uploads/}") String uploadPrefix,
            @Value("${app.storage.s3.export-prefix:exports/}") String exportPrefix,
            @Value("${app.storage.s3.region:us-east-1}") String region,
            @Value("${app.storage.s3.endpoint:}") String endpoint) {
        this.bucket = bucket;
        this.uploadPrefix = endWithSlash(uploadPrefix);
        this.exportPrefix = endWithSlash(exportPrefix);
        this.region = Region.of(region);
        this.endpointOverride = (endpoint == null || endpoint.isBlank()) ? null : URI.create(endpoint);
    }

    @PostConstruct
    void init() {
        var builder = S3Client.builder()
                .region(region)
                .credentialsProvider(DefaultCredentialsProvider.create());
        if (endpointOverride != null) {
            builder.endpointOverride(endpointOverride).forcePathStyle(true); // MinIO / LocalStack
        }
        this.s3 = builder.build();
        log.info("[S3StorageBackend] bucket={}, region={}, endpoint={}", bucket, region.id(),
                endpointOverride == null ? "default" : endpointOverride);
    }

    @PreDestroy
    void shutdown() {
        if (s3 != null) s3.close();
    }

    @Override
    public String store(InputStream content, String originalFilename) throws IOException {
        String key = uploadPrefix + System.currentTimeMillis() + "_" + sanitize(originalFilename);
        // We don't know the content length up-front; buffer into a temp file so the
        // SDK can use a known-length PutObject (avoids chunked encoding edge-cases on some S3-compatible servers).
        Path tmp = Files.createTempFile("rtdps-upload-", ".csv");
        try {
            Files.copy(content, tmp, StandardCopyOption.REPLACE_EXISTING);
            s3.putObject(PutObjectRequest.builder().bucket(bucket).key(key).build(),
                    RequestBody.fromFile(tmp));
        } finally {
            Files.deleteIfExists(tmp);
        }
        return "s3://" + bucket + "/" + key;
    }

    @Override
    public Path materialize(String location) throws IOException {
        S3Ref ref = parse(location);
        Path tmp = Files.createTempFile("rtdps-materialized-", suffixOf(ref.key()));
        try (var stream = s3.getObject(GetObjectRequest.builder()
                .bucket(ref.bucket()).key(ref.key()).build())) {
            Files.copy(stream, tmp, StandardCopyOption.REPLACE_EXISTING);
        }
        return tmp;
    }

    @Override
    public String finalizeFromLocal(Path localPath, String filenameHint) {
        String key = exportPrefix + filenameHint;
        s3.putObject(PutObjectRequest.builder().bucket(bucket).key(key).build(),
                RequestBody.fromFile(localPath));
        return "s3://" + bucket + "/" + key;
    }

    @Override
    public Resource asResource(String location) {
        S3Ref ref = parse(location);
        InputStream stream = s3.getObject(GetObjectRequest.builder()
                .bucket(ref.bucket()).key(ref.key()).build());
        return new InputStreamResource(stream);
    }

    @Override
    public void delete(String location) {
        if (location == null) return;
        S3Ref ref = parse(location);
        try {
            s3.deleteObject(DeleteObjectRequest.builder()
                    .bucket(ref.bucket()).key(ref.key()).build());
        } catch (S3Exception e) {
            log.warn("[S3StorageBackend] delete failed for {}: {}", location, e.getMessage());
        }
    }

    @Override
    public void releaseLocal(Path materializedPath, String originalLocation) throws IOException {
        if (materializedPath != null) Files.deleteIfExists(materializedPath);
    }

    @Override
    public Path allocateLocalExportPath(String trackerId) throws IOException {
        return Files.createTempFile("rtdps-export-" + trackerId + "-", ".csv");
    }

    // ---------- helpers ----------

    private record S3Ref(String bucket, String key) { }

    private S3Ref parse(String location) {
        if (location == null || !location.startsWith("s3://")) {
            throw new IllegalArgumentException("Not an s3:// location: " + location);
        }
        String rest = location.substring("s3://".length());
        int slash = rest.indexOf('/');
        if (slash < 0) throw new IllegalArgumentException("Missing key in: " + location);
        return new S3Ref(rest.substring(0, slash), rest.substring(slash + 1));
    }

    private static String endWithSlash(String s) {
        if (s == null || s.isBlank()) return "";
        return s.endsWith("/") ? s : s + "/";
    }

    private static String suffixOf(String key) {
        int dot = key.lastIndexOf('.');
        return dot >= 0 ? key.substring(dot) : ".tmp";
    }

    private static String sanitize(String name) {
        if (name == null || name.isBlank()) return "upload.csv";
        return name.replaceAll("[^A-Za-z0-9._-]", "_");
    }
}

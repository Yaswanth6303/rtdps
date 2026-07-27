package me.yswnthdev.rtdps.storage;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.io.Resource;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;

/**
 * Filesystem-backed storage. Active by default; suitable for single-instance
 * deployments, dev, and tests. Multi-instance deployments must switch to
 * {@code app.storage.type=s3}.
 */
@Component
@Slf4j
@ConditionalOnProperty(name = "app.storage.type", havingValue = "local", matchIfMissing = true)
public class LocalStorageBackend implements StorageBackend {

    private final Path uploadDir;
    private final Path exportDir;

    public LocalStorageBackend(@Value("${app.storage.upload-dir}") String uploadDir,
                               @Value("${app.storage.export-dir}") String exportDir) throws IOException {
        this.uploadDir = Paths.get(uploadDir);
        this.exportDir = Paths.get(exportDir);
        Files.createDirectories(this.uploadDir);
        Files.createDirectories(this.exportDir);
        log.info("[LocalStorageBackend] uploads={}, exports={}", this.uploadDir, this.exportDir);
    }

    @Override
    public String store(InputStream content, String originalFilename) throws IOException {
        String safeName = System.currentTimeMillis() + "_" + sanitize(originalFilename);
        Path target = uploadDir.resolve(safeName);
        Files.copy(content, target, StandardCopyOption.REPLACE_EXISTING);
        return target.toString();
    }

    @Override
    public Path materialize(String location) {
        return Paths.get(location);   // already local; no copy needed
    }

    @Override
    public String finalizeFromLocal(Path localPath, String filenameHint) throws IOException {
        Path target = exportDir.resolve(filenameHint);
        Files.createDirectories(exportDir);
        Files.move(localPath, target, StandardCopyOption.REPLACE_EXISTING);
        return target.toString();
    }

    @Override
    public Resource asResource(String location) {
        return new FileSystemResource(location);
    }

    @Override
    public void delete(String location) throws IOException {
        if (location == null) return;
        Files.deleteIfExists(Paths.get(location));
    }

    @Override
    public void releaseLocal(Path materializedPath, String originalLocation) {
        // identity-materialized — nothing to release
    }

    @Override
    public Path allocateLocalExportPath(String trackerId) throws IOException {
        Path tmpRoot = Paths.get(System.getProperty("java.io.tmpdir"), "rtdps-exports");
        Files.createDirectories(tmpRoot);
        return tmpRoot.resolve(trackerId + ".csv");
    }

    private String sanitize(String name) {
        if (name == null || name.isBlank()) return "upload.csv";
        return name.replaceAll("[^A-Za-z0-9._-]", "_");
    }
}

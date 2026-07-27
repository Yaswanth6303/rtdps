package me.yswnthdev.rtdps.storage;

import org.springframework.core.io.Resource;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;

/**
 * Abstraction over the file substrate the pipeline reads from and writes to.
 * <p>
 * Two implementations: {@code LocalStorageBackend} for single-node / dev,
 * {@code S3StorageBackend} for multi-instance prod. The choice is made at
 * boot via {@code app.storage.type} — no code outside this package needs to know.
 * <p>
 * Every "location" returned by the backend is an opaque string. Callers must
 * not parse it; round-trip it through the backend methods below.
 */
public interface StorageBackend {

    /**
     * Persist {@code content} under a name derived from {@code originalFilename}.
     * Returns the durable location to record on the {@code JobTracker}.
     */
    String store(InputStream content, String originalFilename) throws IOException;

    /**
     * Make {@code location} readable as a local {@link Path}. For the local
     * backend this is an identity operation; for S3 it downloads to a temp file.
     * The caller is responsible for calling {@link #releaseLocal(Path, String)}
     * when done.
     */
    Path materialize(String location) throws IOException;

    /**
     * Take a locally-written file and persist it as a new durable location.
     * For local backend: moves it under {@code export-dir} and returns the path.
     * For S3: uploads and returns {@code s3://bucket/key}. The local source
     * may be removed after this returns.
     */
    String finalizeFromLocal(Path localPath, String filenameHint) throws IOException;

    /** Stream the content at {@code location} as a Spring {@link Resource}. */
    Resource asResource(String location) throws IOException;

    /** Delete the object at {@code location}. Idempotent. */
    void delete(String location) throws IOException;

    /**
     * Drop any temp file produced by {@link #materialize(String)}. No-op when
     * {@code materializedPath} equals {@code originalLocation}.
     */
    void releaseLocal(Path materializedPath, String originalLocation) throws IOException;

    /** Allocate a fresh local path the batch writer can target during an export. */
    Path allocateLocalExportPath(String trackerId) throws IOException;
}

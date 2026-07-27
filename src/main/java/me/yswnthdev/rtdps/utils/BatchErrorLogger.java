package me.yswnthdev.rtdps.utils;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Per-trackerId, append-mode error log for rows that Spring Batch skips.
 * <p>
 * Lifecycle:
 * <ol>
 *   <li>{@link #initRun(String)} opens a fresh {@link BufferedWriter} and registers it.</li>
 *   <li>{@link #log(String, String)} writes a line through the held-open writer
 *       under a per-trackerId {@link ReentrantLock} — so many batch worker
 *       threads can write concurrently without serializing on a single global
 *       monitor, and we never pay the open/close cost per row.</li>
 *   <li>{@link #closeRun(String)} flushes and closes the writer; called from
 *       {@code JobCompletionListener.afterJob} so the file is durable before
 *       the tracker is marked terminal.</li>
 * </ol>
 * The previous implementation opened a fresh {@code FileWriter} on every single
 * skipped row inside a static {@code synchronized} method — both a contention
 * hot-spot under the multi-threaded step and an I/O bottleneck under high skip
 * counts. This version pays one open and one close per run.
 */
public final class BatchErrorLogger {

    private static final String LOG_DIR = System.getProperty("user.dir") + "/batch-error-logs";
    private static final ConcurrentMap<String, RunLog> RUNS = new ConcurrentHashMap<>();

    private BatchErrorLogger() {}

    /**
     * Open (or replace) the log writer for {@code trackerId}. Truncates any
     * leftover file from a previous run so the operator only sees the current
     * attempt's skipped rows.
     */
    public static void initRun(String trackerId) {
        try {
            Files.createDirectories(Paths.get(LOG_DIR));
            Path file = Paths.get(LOG_DIR, trackerId + "_missing_columns.log");
            BufferedWriter writer = Files.newBufferedWriter(
                    file,
                    StandardCharsets.UTF_8,
                    StandardOpenOption.CREATE,
                    StandardOpenOption.TRUNCATE_EXISTING,
                    StandardOpenOption.WRITE);

            RunLog previous = RUNS.put(trackerId, new RunLog(writer));
            if (previous != null) {
                previous.closeQuietly();   // stale entry from a crashed prior run
            }
        } catch (IOException e) {
            throw new RuntimeException("Error initializing batch log file for tracker: " + trackerId, e);
        }
    }

    /**
     * Append {@code message} as a line to the run's log file. Silently ignored
     * if {@link #initRun(String)} was never called for this {@code trackerId};
     * skipping rather than throwing keeps a transient bookkeeping miss from
     * derailing the whole step.
     */
    public static void log(String trackerId, String message) {
        RunLog run = RUNS.get(trackerId);
        if (run == null) return;
        run.write(message);
    }

    /**
     * Flush and close the run's writer. Idempotent — safe to call from
     * lifecycle hooks even if the run never logged anything.
     */
    public static void closeRun(String trackerId) {
        RunLog run = RUNS.remove(trackerId);
        if (run != null) run.closeQuietly();
    }

    // ------------------------------------------------------------------ //

    private static final class RunLog {
        private final BufferedWriter writer;
        private final ReentrantLock lock = new ReentrantLock();

        RunLog(BufferedWriter writer) {
            this.writer = writer;
        }

        void write(String message) {
            lock.lock();
            try {
                writer.write(message);
                writer.newLine();
            } catch (IOException e) {
                throw new RuntimeException("Error writing batch log", e);
            } finally {
                lock.unlock();
            }
        }

        void closeQuietly() {
            lock.lock();
            try {
                try { writer.flush(); } catch (IOException ignored) { }
                try { writer.close(); } catch (IOException ignored) { }
            } finally {
                lock.unlock();
            }
        }
    }
}

package me.yswnthdev.rtdps.utils;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;

public class BatchErrorLogger {

    private static final String LOG_DIR = System.getProperty("user.dir") + "/batch-error-logs";

    /**
     * Called once before each job run. Creates a fresh, empty log file for this run.
     */
    public static void initRun(String trackerId) {
        try {
            new java.io.File(LOG_DIR).mkdirs();
            new FileWriter(logFile(trackerId), false).close();
        } catch (IOException e) {
            throw new RuntimeException("Error initializing batch log file for tracker: " + trackerId, e);
        }
    }

    public static synchronized void log(String trackerId, String message) {
        try (BufferedWriter bw = new BufferedWriter(new FileWriter(logFile(trackerId), true))) {
            bw.write(message);
            bw.newLine();
        } catch (IOException e) {
            throw new RuntimeException("Error writing batch log", e);
        }
    }

    private static String logFile(String trackerId) {
        return LOG_DIR + "/" + trackerId + "_missing_columns.log";
    }
}


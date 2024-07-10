package com.ecubelabs.utils.k8s;

import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Instant;

public class HealthCheckApp {
    private static final String HEALTH_CHECK_FILE_PATH = "/tmp/healthy";

    public static void execute(Runnable logic) throws IOException {
        try {
            generateHealthyFile();
            logic.run();

            // NOTE: 앱 종료 시, HealthyFile 삭제
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
//                try {
////                    clearHealthyFile();
//                } catch (IOException e) {}
            }));
        } catch (Exception e) {}
    }
    private static void generateHealthyFile() throws IOException {
        try (FileWriter writer = new FileWriter(HEALTH_CHECK_FILE_PATH)) {
            writer.write(String.valueOf(Instant.now().getEpochSecond()));
        }
    }

    private static void clearHealthyFile() throws IOException {
        Files.deleteIfExists(Paths.get(HealthCheckApp.HEALTH_CHECK_FILE_PATH));
    }
}

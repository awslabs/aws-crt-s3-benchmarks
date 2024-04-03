package com.example.s3benchrunner;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.Semaphore;

public abstract class BenchmarkRunner {

    public BenchmarkConfig config;
    public String bucket;
    public String region;

    public byte[] randomDataForUpload;
    private static final int MAX_CONCURRENCE = 1000;
    public Semaphore concurrency_semaphore = new Semaphore(MAX_CONCURRENCE, true);

    public BenchmarkRunner(BenchmarkConfig config, String bucket, String region) {
        this.config = config;
        this.bucket = bucket;
        this.region = region;
        if (!config.filesOnDisk) {
            this.randomDataForUpload = Util.generateRandomData();
        }
    }

    abstract public void run();

    public void prepareRun() throws Exception {
        /* Preparation work between runs */
        for (var task : config.tasks) {
            if (task.action.equals("download")) {
                if (!Path.of(task.key).getParent().toFile().exists()) {
                    Files.createDirectories(Path.of(task.key).getParent());
                }
                if (Files.exists(Path.of(task.key))) {
                    Files.delete(Path.of(task.key));
                }
            }
        }
    };
}

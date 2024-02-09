package com.example.s3benchrunner;

import java.nio.file.Files;
import java.nio.file.Path;

public abstract class BenchmarkRunner {

    public BenchmarkConfig config;
    public String bucket;
    public String region;

    public byte[] randomDataForUpload;

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
                if (Files.exists(Path.of(task.key))) {
                    Files.delete(Path.of(task.key));
                }
            }
        }
    };
}

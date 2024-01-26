package com.example.s3benchrunner.sdkjava;

import com.example.s3benchrunner.BenchmarkConfig;
import com.example.s3benchrunner.BenchmarkRunner;
import com.example.s3benchrunner.Util;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3AsyncClient;

import java.util.ArrayList;

import static com.example.s3benchrunner.Util.bytesFromMiB;

public class SDKJavaBenchmarkRunner implements BenchmarkRunner {
    public BenchmarkConfig config;
    String bucket;
    String region;

    S3AsyncClient s3AsyncClient;
    byte[] payload;

    public SDKJavaBenchmarkRunner(BenchmarkConfig config, String bucket, String region, double targetThroughputGbps) {
        this.config = config;
        this.bucket = bucket;
        this.region = region;

        s3AsyncClient = S3AsyncClient.crtBuilder()
                .region(Region.of(region))
                .targetThroughputInGbps(targetThroughputGbps)
                .minimumPartSizeInBytes(bytesFromMiB(8))
                .build();
        if (!config.filesOnDisk) {
            this.payload = Util.generateRandomData();
        }
    }

    // A benchmark can be run repeatedly
    public void run() {
        // kick off all tasks
        var runningTasks = new ArrayList<SDKJavaTask>(config.tasks.size());
        for (int i = 0; i < config.tasks.size(); ++i) {
            runningTasks.add(new SDKJavaTask(this, i));
        }

        // wait until all tasks are done
        for (var task : runningTasks) {
            task.waitUntilDone();
        }
    }

    // TODO: close resources?

}

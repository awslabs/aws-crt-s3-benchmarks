package com.example.s3benchrunner.sdkjava;

import com.example.s3benchrunner.BenchmarkConfig;
import com.example.s3benchrunner.BenchmarkRunner;
import com.example.s3benchrunner.TaskConfig;
import com.example.s3benchrunner.Util;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.transfer.s3.S3TransferManager;

import java.nio.file.Path;
import java.util.ArrayList;

import static com.example.s3benchrunner.Util.exitWithSkipCode;

public class SDKJavaBenchmarkRunner implements BenchmarkRunner {
    public BenchmarkConfig config;
    String bucket;
    String region;

    S3AsyncClient s3AsyncClient;
    // if uploading, and filesOnDisk is false, then upload this
    byte[] randomDataForUpload;

    S3TransferManager transferManager;
    String transferAction;
    Path transferPath;
    String transferKey;

    public SDKJavaBenchmarkRunner(BenchmarkConfig config, String bucket, String region, double targetThroughputGbps,
            boolean useTransferManager) {
        this.config = config;
        this.bucket = bucket;
        this.region = region;

        s3AsyncClient = S3AsyncClient.crtBuilder()
                .region(Region.of(region))
                .targetThroughputInGbps(targetThroughputGbps)
                .build();

        if (useTransferManager) {
            if (!config.filesOnDisk) {
                exitWithSkipCode("TransferManager cannot run task unless they're on disk");
            }

            TaskConfig firstTask = config.tasks.get(0);
            this.transferAction = firstTask.action;
            if (config.tasks.size() == 1) {
                this.transferKey = firstTask.key;
                this.transferPath = Path.of(firstTask.key);
            } else {
                this.transferKey = null;
                this.transferPath = Path.of(firstTask.key).getParent();
                if (this.transferPath == null) {
                    exitWithSkipCode("TransferManager cannot run tasks unless all keys are in a directory");
                }
                for (TaskConfig task : config.tasks) {
                    if (!firstTask.action.equals(task.action)) {
                        exitWithSkipCode("TransferManager cannot run tasks unless all actions are the same");
                    }
                    Path task_path = Path.of(task.key);
                    while (!task_path.startsWith(this.transferPath)) {
                        this.transferPath = this.transferPath.getParent();
                        if (this.transferPath == null) {
                            exitWithSkipCode(
                                    "TransferManager cannot run tasks unless all keys are in the same directory");
                        }
                    }
                }
                /* TODO: Check the common root dir contains ONLY the files from the tasks */
            }
            transferManager = S3TransferManager.builder()
                    .s3Client(s3AsyncClient)
                    .build();

        } else {
            if (!config.filesOnDisk) {
                this.randomDataForUpload = Util.generateRandomData();
            }
            transferManager = null;
        }
    }

    // A benchmark can be run repeatedly
    public void run() {
        // kick off all
        if (this.transferManager != null) {
            SDKJavaTask task = new SDKJavaTaskTransferManager(this);
            task.waitUntilDone();
        } else {
            var runningTasks = new ArrayList<SDKJavaTask>(config.tasks.size());
            for (int i = 0; i < config.tasks.size(); ++i) {
                runningTasks.add(new SDKJavaTaskAsyncClient(this, i));
            }
            // wait until all tasks are done
            for (var task : runningTasks) {
                task.waitUntilDone();
            }
        }
    }

    // TODO: close resources?

}

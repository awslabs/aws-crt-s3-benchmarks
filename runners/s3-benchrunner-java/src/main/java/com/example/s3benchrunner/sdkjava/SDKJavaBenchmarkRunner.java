package com.example.s3benchrunner.sdkjava;

import com.example.s3benchrunner.BenchmarkConfig;
import com.example.s3benchrunner.BenchmarkRunner;
import com.example.s3benchrunner.TaskConfig;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.paginators.ListObjectsV2Publisher;
import software.amazon.awssdk.transfer.s3.S3TransferManager;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.concurrent.CompletableFuture;

import static com.example.s3benchrunner.Util.exitWithError;
import static com.example.s3benchrunner.Util.exitWithSkipCode;

public class SDKJavaBenchmarkRunner extends BenchmarkRunner {

    S3AsyncClient s3AsyncClient;

    // The rest of these variables are only used when useTransferManager==true
    S3TransferManager transferManager;
    String transferAction;
    Path transferPath;
    String transferKey;

    public SDKJavaBenchmarkRunner(BenchmarkConfig config, String bucket, String region, double targetThroughputGbps,
            boolean useTransferManager, boolean useCRT) {
        super(config, bucket, region);

        if (useCRT) {
            s3AsyncClient = S3AsyncClient.crtBuilder()
                    .region(Region.of(region))
                    .targetThroughputInGbps(targetThroughputGbps)
                    .build();
        } else {
            /**
             * TODO: SDKs don't support multipart download yet. But, they do have a
             * workaround to fallback for transfer manager.
             * So, use multipart for transfer manager, and default one for client directly.
             */
            s3AsyncClient = S3AsyncClient.builder().multipartEnabled(useTransferManager).region(Region.of(region))
                    .build();
        }

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
                try {
                    /* Check the common root dir contains ONLY the files from the tasks */
                    checkTaskPath();
                } catch (Exception e) {
                    exitWithSkipCode(
                            "TransferManager cannot run tasks unless all keys are in the same directory");
                }
            }

            transferManager = S3TransferManager.builder()
                    .s3Client(s3AsyncClient)
                    .build();

        } else {
            transferManager = null;
        }
    }

    private void checkTaskPath() throws IOException {
        ArrayList<String> taskPath = new ArrayList<>();
        for (var task : config.tasks) {
            taskPath.add(task.key);
        }
        if (this.transferAction.equals("upload")) {
            Files.walk(this.transferPath).forEach(path -> {
                if (!path.toFile().isDirectory() && !taskPath.remove(path.relativize(this.transferPath).toString())) {
                    /* The file in the parent directory is not in the task */
                    exitWithError(
                            "The directory:%s contains file:%s that's not part of the the task"
                                    .formatted(transferPath.toString(), path.toString()));
                }
            });
        } else {
            ListObjectsV2Request request = ListObjectsV2Request.builder().bucket(this.bucket).prefix(transferPath + "/")
                    .build();
            ListObjectsV2Publisher publisher = s3AsyncClient.listObjectsV2Paginator(request);
            CompletableFuture<Void> subscribe = publisher.subscribe(response -> {
                response.contents().forEach(content -> {
                    if (!taskPath.remove(content.key())) {
                        /* The file in the parent directory is not in the task */
                        exitWithError(
                                "The directory:%s contains file:%s that's not part of the the task"
                                        .formatted(transferPath.toString(), content.key()));
                    }
                });
            });
            subscribe.join();
        }
        if (!taskPath.isEmpty()) {
            exitWithError(
                    "The tasks contains %d files not included in the path:%s".formatted(taskPath.size(),
                            transferPath.toString()));
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

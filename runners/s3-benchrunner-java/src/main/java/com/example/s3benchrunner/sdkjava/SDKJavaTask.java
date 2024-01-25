package com.example.s3benchrunner.sdkjava;


import com.example.s3benchrunner.TaskConfig;
import com.example.s3benchrunner.Util;
import software.amazon.awssdk.core.async.AsyncRequestBody;
import software.amazon.awssdk.core.async.AsyncResponseTransformer;

import java.nio.file.Path;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

public class SDKJavaTask {

    private final TaskConfig config;
    SDKJavaBenchmarkRunner runner;
    int taskI;
    CompletableFuture<Void> doneFuture;

    SDKJavaTask(SDKJavaBenchmarkRunner runner, int taskI) {
        this.runner = runner;
        this.taskI = taskI;
        this.config = runner.config.tasks.get(taskI);

        if (config.action.equals("upload")) {
            AsyncRequestBody data;

            if (runner.config.filesOnDisk) {
                data = AsyncRequestBody.fromFile(Path.of(config.key));
            } else {
                data = AsyncRequestBody.fromBytes(runner.randomDataForUpload);
            }
            runner.s3AsyncClient.putObject(req -> req.bucket(this.runner.bucket).key(config.key), data)
                    .whenComplete((result, failure) -> {
                        complete(failure);
                    });
        } else if (config.action.equals("download")) {
            if (runner.config.filesOnDisk) {
                runner.s3AsyncClient.getObject(req -> req.bucket(this.runner.bucket)
                        .key(config.key), AsyncResponseTransformer.toFile(Path.of(config.key)))
                        .whenComplete((result, failure) -> {
                            complete(failure);
                        });
            } else {
                runner.s3AsyncClient.getObject(req -> req.bucket(this.runner.bucket)
                        .key(config.key), AsyncResponseTransformer.toBytes())
                        .whenComplete((result, failure) -> {
                            complete(failure);
                        });
            }
        } else {
            throw new RuntimeException("Unknown task action: " + config.action);
        }
    }

    void waitUntilDone() {
        try {
            doneFuture.get();
        } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException(e);
        }
    }

    private void complete(Throwable failure){
        if (failure != null) {
            // Task failed. Report error and kill program...
            System.err.printf("Task[%d] failed. actions:%s key:%s exception:%s/n",
                    taskI, config.action, config.key, failure);
            Util.exitWithError("S3MetaRequest failed");
        }
        this.doneFuture.complete(null);
    }
}

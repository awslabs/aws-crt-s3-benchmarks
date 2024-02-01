package com.example.s3benchrunner.sdkjava;

import com.example.s3benchrunner.TaskConfig;
import com.example.s3benchrunner.Util;
import software.amazon.awssdk.core.FileTransformerConfiguration;
import software.amazon.awssdk.core.async.AsyncRequestBody;
import software.amazon.awssdk.core.async.AsyncResponseTransformer;
import software.amazon.awssdk.utils.async.SimplePublisher;

import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;

public class SDKJavaTaskAsyncClient implements SDKJavaTask {

    private final TaskConfig config;
    SDKJavaBenchmarkRunner runner;
    int taskI;
    CompletableFuture<Void> doneFuture;

    SDKJavaTaskAsyncClient(SDKJavaBenchmarkRunner runner, int taskI) {
        this.runner = runner;
        this.taskI = taskI;
        this.config = runner.config.tasks.get(taskI);
        doneFuture = new CompletableFuture<Void>();

        if (config.action.equals("upload")) {
            if (runner.config.filesOnDisk) {
                runner.s3AsyncClient
                        .putObject(req -> req.bucket(this.runner.bucket).key(config.key), Path.of(config.key))
                        .whenComplete((result, failure) -> {
                            this.complete(failure);
                        });
            } else {
                SimplePublisher<ByteBuffer> publisher = new SimplePublisher<>();
                Thread uploadThread = Executors.defaultThreadFactory().newThread(() -> {
                    long remaining = config.size;
                    long perPartLen = runner.payload.length;
                    while (remaining > 0) {
                        long amtToTransfer = Math.min(remaining, perPartLen);
                        publisher.send(ByteBuffer.wrap(runner.payload, 0, (int) amtToTransfer));
                        remaining -= amtToTransfer;
                    }
                    publisher.complete();
                });

                runner.s3AsyncClient
                        .putObject(req -> req.bucket(this.runner.bucket).key(config.key),
                                AsyncRequestBody.fromPublisher(publisher))
                        .whenComplete((result, failure) -> {
                            this.complete(failure);
                        });
                uploadThread.start();
            }
        } else if (config.action.equals("download")) {
            if (runner.config.filesOnDisk) {
                runner.s3AsyncClient.getObject(req -> req.bucket(this.runner.bucket)
                        .key(config.key),
                        AsyncResponseTransformer.toFile(Path.of(config.key),
                                FileTransformerConfiguration.defaultCreateOrReplaceExisting()))
                        .whenComplete((result, failure) -> {
                            this.complete(failure);
                        });
            } else {
                runner.s3AsyncClient.getObject(req -> req.bucket(this.runner.bucket)
                        .key(config.key), AsyncResponseTransformer.toPublisher()).whenComplete((result, failure) -> {
                            if (failure != null) {
                                this.complete(failure);
                            } else {
                                result.subscribe((bufferResult) -> {
                                    /* Throw the result away */
                                }).whenComplete((subResult, subFailure) -> {
                                    this.complete(subFailure);
                                });
                            }
                        });
            }
        } else {
            throw new RuntimeException("Unknown task action: " + config.action);
        }
    }

    public void waitUntilDone() {
        try {
            doneFuture.get();
        } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException(e);
        }
    }

    private void complete(Throwable failure) {
        if (failure != null) {
            // Task failed. Report error and kill program...
            System.err.printf("Task[%d] failed. actions:%s key:%s exception:%s/n",
                    taskI, config.action, config.key, failure);
            failure.printStackTrace();
            Util.exitWithError("S3MetaRequest failed");
        }
        this.doneFuture.complete(null);
    }
}

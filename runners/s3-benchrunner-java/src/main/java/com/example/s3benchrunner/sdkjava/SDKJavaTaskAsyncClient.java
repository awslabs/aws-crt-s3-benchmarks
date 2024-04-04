package com.example.s3benchrunner.sdkjava;

import com.example.s3benchrunner.TaskConfig;
import software.amazon.awssdk.core.FileTransformerConfiguration;
import software.amazon.awssdk.core.async.AsyncRequestBody;
import software.amazon.awssdk.core.async.AsyncResponseTransformer;
import software.amazon.awssdk.utils.async.SimplePublisher;

import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;

public class SDKJavaTaskAsyncClient extends SDKJavaTask {

    SDKJavaTaskAsyncClient(SDKJavaBenchmarkRunner runner, int taskI) {
        super(runner);
        TaskConfig config = runner.config.tasks.get(taskI);
        doneFuture = new CompletableFuture<Void>();

        if (config.action.equals("upload")) {
            if (runner.config.filesOnDisk) {
                runner.s3AsyncClient
                        .putObject(req -> req.bucket(runner.bucket).key(config.key), Path.of(config.key))
                        .whenComplete((result, failure) -> {
                            completeHelper(runner, failure);
                        });
            } else {
                SimplePublisher<ByteBuffer> publisher = new SimplePublisher<>();
                Thread uploadThread = Executors.defaultThreadFactory().newThread(() -> {
                    long remaining = config.size;
                    long perPartLen = runner.randomDataForUpload.length;
                    // Enqueue all the ByteBuffers right away without waiting for demand. This isn't
                    // very "reactive", but it's simple. We're just enqueuing the same byte[] over
                    // and over, so this doesn't actually consume much memory.
                    while (remaining > 0) {
                        long amtToTransfer = Math.min(remaining, perPartLen);
                        publisher.send(ByteBuffer.wrap(runner.randomDataForUpload, 0, (int) amtToTransfer));
                        remaining -= amtToTransfer;
                    }
                    publisher.complete();
                });

                runner.s3AsyncClient
                        .putObject(req -> req.bucket(runner.bucket).key(config.key).contentLength(config.size),
                                AsyncRequestBody.fromPublisher(publisher))
                        .whenComplete((result, failure) -> {
                            completeHelper(runner, failure);
                        });
                uploadThread.start();
            }
        } else if (config.action.equals("download")) {
            if (runner.config.filesOnDisk) {
                runner.s3AsyncClient.getObject(req -> req.bucket(runner.bucket)
                        .key(config.key),
                        AsyncResponseTransformer.toFile(Path.of(config.key),
                                FileTransformerConfiguration.defaultCreateOrReplaceExisting()))
                        .whenComplete((result, failure) -> {
                            completeHelper(runner, failure);
                        });
            } else {
                runner.s3AsyncClient.getObject(req -> req.bucket(runner.bucket)
                        .key(config.key), AsyncResponseTransformer.toPublisher()).whenComplete((result, failure) -> {
                            if (failure != null) {
                                completeHelper(runner, failure);
                            } else {
                                result.subscribe((bufferResult) -> {
                                    /* Throw the result away */
                                }).whenComplete((subResult, subFailure) -> {
                                    completeHelper(runner, subFailure);
                                });
                            }
                        });
            }
        } else {
            throw new RuntimeException("Unknown task action: " + config.action);
        }
    }
}

package com.example.s3benchrunner.sdkjava;

import com.example.s3benchrunner.Util;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

public class SDKJavaTask {

    protected CompletableFuture<Void> doneFuture;

    SDKJavaTask(SDKJavaBenchmarkRunner runner) {
        doneFuture = new CompletableFuture<Void>();
        try {
            runner.concurrency_semaphore.acquire();
        } catch (InterruptedException e) {
            completeHelper(runner, e);
        }
    }

    public void waitUntilDone() {
        try {
            doneFuture.get();
        } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException(e);
        }
    }

    protected void completeHelper(SDKJavaBenchmarkRunner runner, Throwable failure) {
        if (failure != null) {
            // Task failed. Report error and kill program...
            failure.printStackTrace();
            exitWithErrorHelper(runner);
        }
        runner.concurrency_semaphore.release();
        this.doneFuture.complete(null);
    }

    protected void exitWithErrorHelper(SDKJavaBenchmarkRunner runner) {
        Util.exitWithError(
                String.format("Transfer with action:%s to path:%s failed", runner.transferAction, runner.transferPath));
    }
}

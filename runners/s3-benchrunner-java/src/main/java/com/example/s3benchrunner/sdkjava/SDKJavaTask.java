package com.example.s3benchrunner.sdkjava;

import com.example.s3benchrunner.Util;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

public class SDKJavaTask {

    protected CompletableFuture<Void> doneFuture;

    SDKJavaTask() {
        doneFuture = new CompletableFuture<Void>();
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
            existWithErrorHelper(runner);
        }
        this.doneFuture.complete(null);
    }

    protected void existWithErrorHelper(SDKJavaBenchmarkRunner runner) {
        Util.exitWithError(
                String.format("Transfer with action:%s to path:%s failed", runner.transferAction, runner.transferPath));
    }
}

package com.example.s3benchrunner.crtjava;

import com.example.s3benchrunner.BenchmarkRunner;
import software.amazon.awssdk.crt.auth.credentials.CredentialsProvider;
import software.amazon.awssdk.crt.auth.credentials.DefaultChainCredentialsProvider;
import software.amazon.awssdk.crt.auth.signing.AwsSigningConfig;
import software.amazon.awssdk.crt.io.*;
import software.amazon.awssdk.crt.s3.S3Client;
import software.amazon.awssdk.crt.s3.S3ClientOptions;

import java.util.ArrayList;

import com.example.s3benchrunner.BenchmarkConfig;
import com.example.s3benchrunner.Main;
import com.example.s3benchrunner.Util;

public class CrtJavaBenchmarkRunner implements BenchmarkRunner {
    BenchmarkConfig config;
    String bucket;
    String region;

    // CRT boilerplate
    EventLoopGroup eventLoopGroup;
    HostResolver hostResolver;
    ClientBootstrap clientBootstrap;
    TlsContext tlsCtx;
    CredentialsProvider credentialsProvider;
    S3Client s3Client;

    byte[] payload;

    public CrtJavaBenchmarkRunner(BenchmarkConfig config, String bucket, String region, double targetThroughputGbps) {
        this.config = config;
        this.bucket = bucket;
        this.region = region;

        eventLoopGroup = new EventLoopGroup(0, 0);

        hostResolver = new HostResolver(eventLoopGroup);

        clientBootstrap = new ClientBootstrap(eventLoopGroup, hostResolver);

        TlsContextOptions tlsCtxOpts = TlsContextOptions.createDefaultClient();
        tlsCtx = new TlsContext(tlsCtxOpts);

        credentialsProvider = new DefaultChainCredentialsProvider.DefaultChainCredentialsProviderBuilder()
                .withClientBootstrap(clientBootstrap)
                .build();

        AwsSigningConfig signingConfig = AwsSigningConfig.getDefaultS3SigningConfig(region, credentialsProvider);

        var s3ClientOpts = new S3ClientOptions()
                .withRegion(region)
                .withThroughputTargetGbps(targetThroughputGbps)
                .withClientBootstrap(clientBootstrap)
                .withTlsContext(tlsCtx)
                .withSigningConfig(signingConfig);

        // If writing data to disk, enable backpressure.
        // This prevents us from running out of memory due to downloading
        // data faster than we can write it to disk.
        if (config.filesOnDisk) {
            s3ClientOpts.withReadBackpressureEnabled(true);
            s3ClientOpts.withInitialReadWindowSize(Util.bytesFromMiB(Main.BACKPRESSURE_INITIAL_READ_WINDOW_MiB));
        } else {
            this.payload = Util.generateRandomData();
        }

        s3Client = new S3Client(s3ClientOpts);
    }

    // A benchmark can be run repeatedly
    public void run() {
        // kick off all tasks
        var runningTasks = new ArrayList<CrtJavaTask>(config.tasks.size());
        for (int i = 0; i < config.tasks.size(); ++i) {
            runningTasks.add(new CrtJavaTask(this, i));
        }

        // wait until all tasks are done
        for (var task : runningTasks) {
            task.waitUntilDone();
        }
    }

    // TODO: close resources?
}

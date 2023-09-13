package com.example.s3benchrunner.crtjava;

import software.amazon.awssdk.crt.auth.credentials.CredentialsProvider;
import software.amazon.awssdk.crt.auth.credentials.DefaultChainCredentialsProvider;
import software.amazon.awssdk.crt.auth.signing.AwsSigningConfig;
import software.amazon.awssdk.crt.io.*;
import software.amazon.awssdk.crt.s3.S3Client;
import software.amazon.awssdk.crt.s3.S3ClientOptions;

import java.util.ArrayList;
import java.util.Random;

class Benchmark {
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

    // if uploading, and filesOnDisk is false, then upload this
    byte randomDataForUpload[];

    Benchmark(BenchmarkConfig config, String bucket, String region, double targetThroughputGbps) {
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
            // 256MiB is Java Transfer Mgr v2 default.
            // TODO: Investigate. At time of writing, this noticeably impacts performance.
            s3ClientOpts.withInitialReadWindowSize(Util.bytesFromMiB(256));
        }

        s3Client = new S3Client(s3ClientOpts);

        // If we're uploading, and not using files on disk,
        // then generate an in-memory buffer of random data to upload.
        // All uploads will use this same buffer, so make it big enough for the largest file.
        if (!config.filesOnDisk) {
            long largestUpload = 0;
            for (var task : config.tasks) {
                if (task.action.equals("upload")) {
                    largestUpload = Math.max(largestUpload, task.size);
                }
            }

            // NOTE: if this raises an exception, either the size > Integer.MAX_VALUE
            // or we failed allocating such a large buffer.
            // So we need a new technique.
            // Either generate random data within sendRequestBody() (may impact performance).
            // Or just use a smaller buffer that we send repeatedly.
            randomDataForUpload = new byte[Math.toIntExact(largestUpload)];
            new Random().nextBytes(randomDataForUpload);
        }
    }

    // A benchmark can be run repeatedly
    void run() {
        // kick off all tasks
        var runningTasks = new ArrayList<Task>(config.tasks.size());
        for (int i = 0; i < config.tasks.size(); ++i) {
            runningTasks.add(new Task(this, i));
        }

        // wait until all tasks are done
        for (var task : runningTasks) {
            task.waitUntilDone();
        }
    }

    // TODO: close resources?
}

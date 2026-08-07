package com.example.s3benchrunner.crtjava;

import com.example.s3benchrunner.BenchmarkRunner;
import software.amazon.awssdk.crt.auth.credentials.CredentialsProvider;
import software.amazon.awssdk.crt.auth.credentials.DefaultChainCredentialsProvider;
import software.amazon.awssdk.crt.auth.signing.AwsSigningConfig;
import software.amazon.awssdk.crt.io.*;
import software.amazon.awssdk.crt.s3.S3Client;
import software.amazon.awssdk.crt.s3.S3ClientOptions;
import software.amazon.awssdk.crt.s3.S3DirectBufferPool;

import java.util.ArrayList;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.example.s3benchrunner.BenchmarkConfig;
import com.example.s3benchrunner.Main;
import com.example.s3benchrunner.Util;

public class CRTJavaBenchmarkRunner extends BenchmarkRunner {

    // CRT boilerplate
    EventLoopGroup eventLoopGroup;
    HostResolver hostResolver;
    ClientBootstrap clientBootstrap;
    TlsContext tlsCtx;
    CredentialsProvider credentialsProvider;
    S3Client s3Client;
    // Optional. Non-null iff Main.USE_DBZ. Held as a field so the JVM GC does
    // not collect the pool while aws-c-s3 still holds slot references via JNI.
    S3DirectBufferPool directBufferPool;

    // derived from bucket and region (e.g. mybucket.s3.us-west-2.amazonaws.com)
    String endpoint;

    public CRTJavaBenchmarkRunner(BenchmarkConfig config, String bucket, String region, double targetThroughputGbps) {

        super(config, bucket, region);

        // S3 Express buckets look like "mybucket--usw2-az3--x-s3"
        Matcher s3ExpressMatcher = Pattern.compile("--(.*)--x-s3$").matcher(bucket);
        boolean isS3Express = s3ExpressMatcher.find();
        if (isS3Express) {
            // extract the "usw2-az3" from "mybucket--usw2-az3--x-s3"
            String azID = s3ExpressMatcher.group(1);

            // Endpoint looks like:
            // mybucket--usw2-az3--x-s3.s3express-usw2-az3.us-west-2.amazonaws.com
            endpoint = bucket + ".s3express-" + azID + "." + region + ".amazonaws.com";
        } else {
            // Standard S3 endpoint looks like: mybucket.s3.us-west-2.amazonaws.com
            endpoint = bucket + ".s3." + region + ".amazonaws.com";
        }

        eventLoopGroup = new EventLoopGroup(0);

        hostResolver = new HostResolver(eventLoopGroup);

        clientBootstrap = new ClientBootstrap(eventLoopGroup, hostResolver);

        TlsContextOptions tlsCtxOpts = TlsContextOptions.createDefaultClient();
        tlsCtx = new TlsContext(tlsCtxOpts);

        credentialsProvider = new DefaultChainCredentialsProvider.DefaultChainCredentialsProviderBuilder()
                .withClientBootstrap(clientBootstrap)
                .build();

        AwsSigningConfig signingConfig = AwsSigningConfig.getDefaultS3SigningConfig(region, credentialsProvider);

        if (isS3Express) {
            signingConfig.setAlgorithm(AwsSigningConfig.AwsSigningAlgorithm.SIGV4_S3EXPRESS);
        }

        var s3ClientOpts = new S3ClientOptions()
                .withRegion(region)
                .withThroughputTargetGbps(targetThroughputGbps)
                .withClientBootstrap(clientBootstrap)
                .withTlsContext(tlsCtx)
                .withEnableS3Express(isS3Express)
                .withSigningConfig(signingConfig);

        // If a backpressure window is configured, enable backpressure regardless of
        // filesOnDisk.
        // This lets us match the SDK's always-on backpressure behavior for fair
        // comparison.
        // Set via -Daws.crt.backpressure.window_mib=<size>. Default 0 = disabled.
        if (Main.BACKPRESSURE_INITIAL_READ_WINDOW_MiB != 0) {
            s3ClientOpts.withReadBackpressureEnabled(true);
            s3ClientOpts.withInitialReadWindowSize(Util.bytesFromMiB(Main.BACKPRESSURE_INITIAL_READ_WINDOW_MiB));
        }

        // Override CRT's default max active connections (which it derives from the
        // throughput target). Set via -Daws.s3.max_connections=<N>. Useful for
        // exploring the tradeoff between parallelism and per-connection throughput /
        // memory footprint.
        if (Main.MAX_CONNECTIONS != null) {
            s3ClientOpts.withMaxConnections(Main.MAX_CONNECTIONS);
        }

        // Opt into the Direct Byte Zero-Copy (DBZ) pool.
        //
        // Enabled via -Daws.crt.s3.use_dbz=true. Default false = baseline path
        // (aws-c-s3 native buffer pool + byte[] response body callback).
        //
        // When enabled, the runner:
        //   1. Builds a JVM-tracked S3DirectBufferPool via
        //      S3DirectBufferPool.create(s3ClientOpts). This factory auto-scales
        //      the pool capacity to match aws-c-s3's default buffer pool sizing
        //      (2-24 GiB by throughputTargetGbps), so DBZ vs baseline memory
        //      footprint is a fair comparison at the same target throughput.
        //   2. Attaches the pool via S3ClientOptions.withDirectByteBufferPool.
        //      The client derives useDirectByteBufferPool from this and routes
        //      every meta-request's response body through the NewDirectByteBuffer
        //      callback path, eliminating the per-part 8 MiB byte[] allocation.
        //
        // The pool must be constructed AFTER withThroughputTargetGbps has been
        // set on s3ClientOpts, since create() reads that value for auto-sizing.
        if (Main.USE_DBZ) {
            directBufferPool = S3DirectBufferPool.create(s3ClientOpts);
            s3ClientOpts.withDirectByteBufferPool(directBufferPool);
        }

        s3Client = new S3Client(s3ClientOpts);
    }

    // A benchmark can be run repeatedly
    public void run() {
        // kick off all tasks
        var runningTasks = new ArrayList<CRTJavaTask>(config.tasks.size());
        for (int i = 0; i < config.tasks.size(); ++i) {
            runningTasks.add(new CRTJavaTask(this, i));
        }

        // wait until all tasks are done
        for (var task : runningTasks) {
            task.waitUntilDone();
        }
    }

    // TODO: close resources?
}

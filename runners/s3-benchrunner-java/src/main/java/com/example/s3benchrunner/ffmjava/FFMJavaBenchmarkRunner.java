package com.example.s3benchrunner.ffmjava;

import com.example.s3benchrunner.BenchmarkRunner;
import software.amazon.awssdk.crt.auth.credentials.CredentialsProvider;
import software.amazon.awssdk.crt.auth.credentials.DefaultChainCredentialsProvider;
import software.amazon.awssdk.crt.auth.signing.AwsSigningConfig;
import software.amazon.awssdk.crt.io.*;
import software.amazon.awssdk.crt.s3.S3Client;
import software.amazon.awssdk.crt.s3.S3ClientOptions;

import java.util.ArrayList;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.example.s3benchrunner.BenchmarkConfig;
import com.example.s3benchrunner.Main;
import com.example.s3benchrunner.Util;

/**
 * Benchmark runner that uses the FFM (Foreign Function & Memory) backed version
 * of aws-crt-java (requires Java 22+). The public API is identical to the JNI
 * backed CRTJavaBenchmarkRunner; the difference is that this runner is built
 * against an aws-crt-java branch that uses FFM instead of JNI under the hood.
 * <p>
 * The {@link FFMJavaTask.CopyMode} controls what happens with downloaded data:
 * <ul>
 *   <li>{@link FFMJavaTask.CopyMode#NONE} — zero-copy, data discarded</li>
 *   <li>{@link FFMJavaTask.CopyMode#HEAP_COPY} — copy to GC-managed byte[]</li>
 *   <li>{@link FFMJavaTask.CopyMode#OFFHEAP_COPY} — copy to pre-allocated off-heap buffer</li>
 * </ul>
 */
public class FFMJavaBenchmarkRunner extends BenchmarkRunner {

    /**
     * Controls what happens with downloaded data in the FFM response body callback.
     * Exposed as a public nested enum so that {@code Main.java} (in a different
     * package) can reference it without needing access to the package-private
     * {@code FFMJavaTask} class.
     */
    public enum CopyMode {
        /** Discard data immediately — zero-copy, no allocation. */
        NONE,
        /** Copy into a GC-managed {@code byte[]} on every callback. */
        HEAP_COPY,
        /** Copy into a pre-allocated off-heap {@link java.lang.foreign.MemorySegment}. */
        OFFHEAP_COPY,
    }

    // CRT boilerplate
    EventLoopGroup eventLoopGroup;
    HostResolver hostResolver;
    ClientBootstrap clientBootstrap;
    TlsContext tlsCtx;
    CredentialsProvider credentialsProvider;
    S3Client s3Client;

    // derived from bucket and region (e.g. mybucket.s3.us-west-2.amazonaws.com)
    String endpoint;

    // Controls what happens with downloaded data in the response body callback
    CopyMode copyMode;

    public FFMJavaBenchmarkRunner(BenchmarkConfig config, String bucket, String region, double targetThroughputGbps) {
        this(config, bucket, region, targetThroughputGbps, CopyMode.NONE);
    }

    public FFMJavaBenchmarkRunner(BenchmarkConfig config, String bucket, String region, double targetThroughputGbps,
            CopyMode copyMode) {

        super(config, bucket, region);

        this.copyMode = copyMode;

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

        // If writing data to disk, enable backpressure.
        // This prevents us from running out of memory due to downloading
        // data faster than we can write it to disk.
        if (config.filesOnDisk && Main.BACKPRESSURE_INITIAL_READ_WINDOW_MiB != 0) {
            s3ClientOpts.withReadBackpressureEnabled(true);
            s3ClientOpts.withInitialReadWindowSize(Util.bytesFromMiB(Main.BACKPRESSURE_INITIAL_READ_WINDOW_MiB));
        }

        s3Client = new S3Client(s3ClientOpts);
    }

    // A benchmark can be run repeatedly
    public void run() {
        // kick off all tasks
        var runningTasks = new ArrayList<FFMJavaTask>(config.tasks.size());
        for (int i = 0; i < config.tasks.size(); ++i) {
            runningTasks.add(new FFMJavaTask(this, i, copyMode));
        }

        // wait until all tasks are done
        for (var task : runningTasks) {
            task.waitUntilDone();
        }
    }

    // TODO: close resources?
}

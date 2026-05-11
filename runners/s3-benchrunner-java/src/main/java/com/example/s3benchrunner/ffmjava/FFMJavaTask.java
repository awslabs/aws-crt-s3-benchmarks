package com.example.s3benchrunner.ffmjava;

import com.example.s3benchrunner.TaskConfig;
import com.example.s3benchrunner.Util;
import software.amazon.awssdk.crt.CRT;
import software.amazon.awssdk.crt.http.HttpHeader;
import software.amazon.awssdk.crt.http.HttpRequest;
import software.amazon.awssdk.crt.http.HttpRequestBodyStream;
import software.amazon.awssdk.crt.s3.*;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

/**
 * A single transfer task (upload or download) executed via the FFM-backed
 * aws-crt-java S3Client.
 * <p>
 * This task uses {@link S3MetaRequestOptions#withUseFFM(boolean) useFFM=true}
 * so that:
 * <ul>
 *   <li><b>Downloads:</b> response body chunks are delivered as
 *       {@link MemorySegment} views of native memory (zero-copy, no
 *       {@code byte[]} allocation).</li>
 *   <li><b>Uploads:</b> the upload stream writes directly into the native
 *       buffer via {@link MemorySegment} (no {@code DirectByteBuffer} wrapper
 *       object allocation).</li>
 * </ul>
 * <p>
 * The {@link FFMJavaBenchmarkRunner.CopyMode} controls what happens with
 * downloaded data in the response body callback:
 * <ul>
 *   <li>{@link FFMJavaBenchmarkRunner.CopyMode#NONE} — data is discarded
 *       (zero-copy, benchmark measures pure download throughput)</li>
 *   <li>{@link FFMJavaBenchmarkRunner.CopyMode#HEAP_COPY} — data is copied
 *       into a GC-managed {@code byte[]}</li>
 *   <li>{@link FFMJavaBenchmarkRunner.CopyMode#OFFHEAP_COPY} — data is copied
 *       into a pre-allocated off-heap {@link MemorySegment}</li>
 * </ul>
 */
class FFMJavaTask implements S3MetaRequestResponseHandler {

    FFMJavaBenchmarkRunner runner;
    int taskI;
    TaskConfig config;
    S3MetaRequest metaRequest;
    CompletableFuture<Void> doneFuture;
    final FFMJavaBenchmarkRunner.CopyMode copyMode;

    /**
     * Pre-allocated off-heap buffer for {@link CopyMode#OFFHEAP_COPY}.
     * Sized to the maximum expected chunk size (8 MiB = typical CRT part size).
     * Reused across all callbacks for this task to avoid repeated allocation.
     */
    private final MemorySegment offheapCopyBuffer;
    private static final long OFFHEAP_BUFFER_SIZE = 8L * 1024 * 1024; // 8 MiB

    FFMJavaTask(FFMJavaBenchmarkRunner runner, int taskI, FFMJavaBenchmarkRunner.CopyMode copyMode) {
        this.runner = runner;
        this.taskI = taskI;
        this.config = runner.config.tasks.get(taskI);
        this.copyMode = copyMode;
        doneFuture = new CompletableFuture<Void>();

        // Pre-allocate off-heap buffer if needed
        if (copyMode == FFMJavaBenchmarkRunner.CopyMode.OFFHEAP_COPY) {
            offheapCopyBuffer = Arena.ofAuto().allocate(OFFHEAP_BUFFER_SIZE);
        } else {
            offheapCopyBuffer = null;
        }

        var options = new S3MetaRequestOptions();

        options.withResponseHandler(this);

        // Enable FFM mode: zero-copy downloads, direct-write uploads
        options.withUseFFM(true);

        String httpMethod;
        String httpPath = "/" + config.key;
        HttpRequestBodyStream requestUploadStream = null;
        var headers = new ArrayList<HttpHeader>();
        headers.add(new HttpHeader("Host", runner.endpoint));

        if (config.action.equals("upload")) {
            options.withMetaRequestType(S3MetaRequestOptions.MetaRequestType.PUT_OBJECT);
            httpMethod = "PUT";

            headers.add(new HttpHeader("Content-Length", Long.toString(config.size)));
            headers.add(new HttpHeader("Content-Type", "application/octet-stream"));

            if (runner.config.filesOnDisk) {
                options.withRequestFilePath(Path.of(config.key));
            } else {
                requestUploadStream = new UploadFromRamStream(runner.randomDataForUpload, config.size);
            }

        } else if (config.action.equals("download")) {
            options.withMetaRequestType(S3MetaRequestOptions.MetaRequestType.GET_OBJECT);
            httpMethod = "GET";

            headers.add(new HttpHeader("Content-Length", "0"));

            if (runner.config.filesOnDisk) {
                options.withResponseFilePath(Path.of(config.key));
            }
        } else {
            throw new RuntimeException("Unknown task action: " + config.action);
        }

        if (runner.config.checksum != null) {
            options.withChecksumConfig(new ChecksumConfig()
                    .withChecksumAlgorithm(runner.config.checksum)
                    .withChecksumLocation(ChecksumConfig.ChecksumLocation.HEADER)
                    .withValidateChecksum(true));
        }

        HttpHeader[] headersArray = headers.toArray(new HttpHeader[0]);
        options.withHttpRequest(new HttpRequest(httpMethod, httpPath, headersArray, requestUploadStream));

        // work around API-gotcha where callbacks can fire on other threads
        // before makeMetaRequest() has returned
        synchronized (this) {
            metaRequest = runner.s3Client.makeMetaRequest(options);
        }
    }

    void waitUntilDone() {
        try {
            doneFuture.get();
        } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * FFM download path: body chunk delivered as a zero-copy {@link MemorySegment}
     * view of native memory.
     * <p>
     * Behaviour depends on {@link #copyMode}:
     * <ul>
     *   <li>{@link CopyMode#NONE} — data is discarded immediately (zero-copy)</li>
     *   <li>{@link CopyMode#HEAP_COPY} — data is copied into a new {@code byte[]}
     *       on the Java GC heap</li>
     *   <li>{@link CopyMode#OFFHEAP_COPY} — data is copied into a pre-allocated
     *       off-heap {@link MemorySegment} (no GC pressure)</li>
     * </ul>
     */
    @Override
    public int onResponseBody(MemorySegment bodyBytesIn, long objectRangeStart, long objectRangeEnd) {
        switch (copyMode) {
            case NONE:
                // Zero-copy: discard data, no allocation.
                break;

            case HEAP_COPY:
                // Copy into a GC-managed byte[]. This simulates an application
                // that needs a Java-owned copy of the data. The byte[] is
                // immediately eligible for GC after this callback returns.
                @SuppressWarnings("unused")
                byte[] heapCopy = bodyBytesIn.toArray(ValueLayout.JAVA_BYTE);
                break;

            case OFFHEAP_COPY:
                // Copy into the pre-allocated off-heap buffer. This simulates
                // an application that needs an owned copy but wants to avoid
                // GC pressure. The buffer is reused across callbacks.
                long chunkSize = bodyBytesIn.byteSize();
                MemorySegment dest = offheapCopyBuffer.asSlice(0, chunkSize);
                MemorySegment.copy(bodyBytesIn, 0, dest, 0, chunkSize);
                break;
        }
        return 0;
    }

    @Override
    public void onFinished(S3FinishedResponseContext context) {
        if (context.getErrorCode() != 0) {
            // FFMJavaTask failed. Report error and kill program...
            System.err.printf("FFMJavaTask[%d] failed. action:%s key:%s error_code:%s%n",
                    taskI, config.action, config.key, CRT.awsErrorName(context.getErrorCode()));

            if (context.getResponseStatus() != 0) {
                System.err.println("Status-Code: " + context.getResponseStatus());
            }

            if (context.getErrorPayload().length > 0) {
                System.err.println(new String(context.getErrorPayload(), StandardCharsets.UTF_8));
            }

            Util.exitWithError("S3MetaRequest failed");
        } else {
            // FFMJavaTask succeeded. Clean up...
            // work around API-gotcha where callbacks can fire on other threads
            // before makeMetaRequest() has returned
            synchronized (this) {
                metaRequest.close();
            }

            doneFuture.complete(null);
        }
    }

    /**
     * FFM upload stream: writes random data directly into the native buffer via
     * {@link MemorySegment}, avoiding the {@code DirectByteBuffer} wrapper object
     * that the JNI path allocates on every call.
     * <p>
     * Returns the number of bytes written. Returning {@code 0} signals
     * end-of-stream to the native layer.
     */
    static class UploadFromRamStream implements HttpRequestBodyStream {
        final long size;
        long bytesWritten;
        byte[] randomData;

        UploadFromRamStream(byte[] randomData, long size) {
            this.randomData = randomData;
            this.size = size;
        }

        /**
         * FFM path: write directly into the native buffer at {@code address}.
         * Returns bytes written; 0 signals end-of-stream.
         */
        @Override
        public int sendRequestBody(long address, long length) {
            long remaining = size - bytesWritten;
            if (remaining <= 0) {
                return 0; // end-of-stream
            }

            long toWrite = Math.min(remaining, length);

            // Wrap the native destination buffer as a MemorySegment (zero-copy).
            MemorySegment dest = MemorySegment.ofAddress(address)
                    .reinterpret(toWrite, Arena.ofAuto(), null);

            // Copy from randomData (looping) into the native segment.
            long written = 0;
            while (written < toWrite) {
                int chunk = (int) Math.min(toWrite - written, randomData.length);
                MemorySegment.copy(MemorySegment.ofArray(randomData), ValueLayout.JAVA_BYTE, 0,
                        dest, ValueLayout.JAVA_BYTE, written, chunk);
                written += chunk;
            }

            bytesWritten += written;
            return (int) written;
        }

        @Override
        public boolean resetPosition() {
            bytesWritten = 0;
            return true;
        }

        @Override
        public long getLength() {
            return size;
        }
    }
}

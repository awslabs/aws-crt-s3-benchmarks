package com.example.s3benchrunner.crtjava;

import software.amazon.awssdk.crt.CRT;
import software.amazon.awssdk.crt.http.HttpHeader;
import software.amazon.awssdk.crt.http.HttpRequest;
import software.amazon.awssdk.crt.http.HttpRequestBodyStream;
import software.amazon.awssdk.crt.s3.*;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

class Task implements S3MetaRequestResponseHandler {

    Benchmark benchmark;
    int taskI;
    TaskConfig config;
    S3MetaRequest metaRequest;
    CompletableFuture<Void> doneFuture;
    ReadableByteChannel uploadFileChannel;
    WritableByteChannel downloadFileChannel;

    Task(Benchmark benchmark, int taskI) {
        this.benchmark = benchmark;
        this.taskI = taskI;
        this.config = benchmark.config.tasks.get(taskI);
        doneFuture = new CompletableFuture<Void>();

        var options = new S3MetaRequestOptions();

        options.withResponseHandler(this);

        String httpMethod;
        String httpPath = "/" + config.key;
        HttpRequestBodyStream requestUploadStream = null;
        var headers = new ArrayList<HttpHeader>();
        headers.add(new HttpHeader("Host", benchmark.bucket + ".s3." + benchmark.region + ".amazonaws.com"));

        if (config.action.equals("upload")) {
            options.withMetaRequestType(S3MetaRequestOptions.MetaRequestType.PUT_OBJECT);
            httpMethod = "PUT";

            headers.add(new HttpHeader("Content-Length", Long.toString(config.size)));
            headers.add(new HttpHeader("Content-Type", "application/octet-stream"));

            if (benchmark.config.filesOnDisk) {
                options.withRequestFilePath(Path.of(config.key));
            } else {
                requestUploadStream = new UploadFromRamStream(benchmark.randomDataForUpload, config.size);
            }

        } else if (config.action.equals("download")) {
            options.withMetaRequestType(S3MetaRequestOptions.MetaRequestType.GET_OBJECT);
            httpMethod = "GET";

            headers.add(new HttpHeader("Content-Length", "0"));

            if (benchmark.config.filesOnDisk) {
                try {
                    downloadFileChannel = FileChannel.open(Path.of(config.key),
                            StandardOpenOption.CREATE, StandardOpenOption.WRITE);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        } else {
            throw new RuntimeException("Unknown task action: " + config.action);
        }

        if (benchmark.config.checksum != ChecksumAlgorithm.NONE) {
            options.withChecksumConfig(new ChecksumConfig()
                    .withChecksumAlgorithm(benchmark.config.checksum)
                    .withChecksumLocation(ChecksumConfig.ChecksumLocation.HEADER)
                    .withValidateChecksum(true));
        }

        HttpHeader[] headersArray = headers.toArray(new HttpHeader[0]);
        options.withHttpRequest(new HttpRequest(httpMethod, httpPath, headersArray, requestUploadStream));

        // work around API-gotcha where callbacks can fire on other threads
        // before makeMetaRequest() has returned
        synchronized (this) {
            metaRequest = benchmark.s3Client.makeMetaRequest(options);
        }
    }

    void waitUntilDone() {
        try {
            doneFuture.get();
        } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public int onResponseBody(ByteBuffer bodyBytesIn, long objectRangeStart, long objectRangeEnd) {
        int amountReceived = bodyBytesIn.remaining();

        if (downloadFileChannel != null) {
            try {
                downloadFileChannel.write(bodyBytesIn);
            } catch (IOException e) {
                Util.exitWithError("Failed writing to file: " + e.toString());
            }
        }

        return amountReceived;
    }

    @Override
    public void onFinished(S3FinishedResponseContext context) {
        if (context.getErrorCode() != 0) {
            // Task failed. Report error and kill program...
            System.err.printf("Task[%d] failed. actions:%s key:%s error_code:%s%n",
                    taskI, config.key, CRT.awsErrorName(context.getErrorCode()));

            if (context.getResponseStatus() != 0) {
                System.err.println("Status-Code: " + context.getResponseStatus());
            }

            if (context.getErrorPayload().length > 0) {
                System.err.println(new String(context.getErrorPayload(), StandardCharsets.UTF_8));
            }

            Util.exitWithError("S3MetaRequest failed");
        } else {
            // Task succeeded. Clean up...
            try {
                if (downloadFileChannel != null) {
                    downloadFileChannel.close();
                }
                if (uploadFileChannel != null) {
                    uploadFileChannel.close();
                }
            } catch (IOException e) {
                Util.exitWithError("Failed closing file: " + e.toString());
            }

            // work around API-gotcha where callbacks can fire on other threads
            // before makeMetaRequest() has returned
            synchronized (this) {
                metaRequest.close();
            }

            doneFuture.complete(null);
        }
    }

    class UploadFromRamStream implements HttpRequestBodyStream {
        byte body[];
        final int size;
        int bytesWritten;

        UploadFromRamStream(byte body[], long size) {
            this.body = body;
            this.size = Math.toIntExact(size);
        }

        @Override
        public boolean sendRequestBody(ByteBuffer dstBuf) {
            int bufferSpaceAvailable = dstBuf.remaining();
            int bodyBytesAvailable = size - bytesWritten;
            int amountToWrite = Math.min(bufferSpaceAvailable, bodyBytesAvailable);

            dstBuf.put(body, bytesWritten, amountToWrite);

            bytesWritten += amountToWrite;
            boolean isComplete = bytesWritten == size;
            return isComplete;
        }
    }
}

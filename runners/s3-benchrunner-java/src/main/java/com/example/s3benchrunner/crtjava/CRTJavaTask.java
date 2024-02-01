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

import com.example.s3benchrunner.TaskConfig;
import com.example.s3benchrunner.Util;

class CRTJavaTask implements S3MetaRequestResponseHandler {

    CRTJavaBenchmarkRunner runner;
    int taskI;
    TaskConfig config;
    S3MetaRequest metaRequest;
    CompletableFuture<Void> doneFuture;
    ReadableByteChannel uploadFileChannel;
    WritableByteChannel downloadFileChannel;

    CRTJavaTask(CRTJavaBenchmarkRunner runner, int taskI) {
        this.runner = runner;
        this.taskI = taskI;
        this.config = runner.config.tasks.get(taskI);
        doneFuture = new CompletableFuture<Void>();

        var options = new S3MetaRequestOptions();

        options.withResponseHandler(this);

        String httpMethod;
        String httpPath = "/" + config.key;
        HttpRequestBodyStream requestUploadStream = null;
        var headers = new ArrayList<HttpHeader>();
        headers.add(new HttpHeader("Host", runner.bucket + ".s3." + runner.region + ".amazonaws.com"));

        if (config.action.equals("upload")) {
            options.withMetaRequestType(S3MetaRequestOptions.MetaRequestType.PUT_OBJECT);
            httpMethod = "PUT";

            headers.add(new HttpHeader("Content-Length", Long.toString(config.size)));
            headers.add(new HttpHeader("Content-Type", "application/octet-stream"));

            if (runner.config.filesOnDisk) {
                options.withRequestFilePath(Path.of(config.key));
            } else {
                requestUploadStream = new UploadFromRamStream(runner.payload, config.size);
            }

        } else if (config.action.equals("download")) {
            options.withMetaRequestType(S3MetaRequestOptions.MetaRequestType.GET_OBJECT);
            httpMethod = "GET";

            headers.add(new HttpHeader("Content-Length", "0"));

            if (runner.config.filesOnDisk) {
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
            // CRTJavaTask failed. Report error and kill program...
            System.err.printf("CRTJavaTask[%d] failed. actions:%s key:%s error_code:%s/n",
                    taskI, config.action, config.key, CRT.awsErrorName(context.getErrorCode()));

            if (context.getResponseStatus() != 0) {
                System.err.println("Status-Code: " + context.getResponseStatus());
            }

            if (context.getErrorPayload().length > 0) {
                System.err.println(new String(context.getErrorPayload(), StandardCharsets.UTF_8));
            }

            Util.exitWithError("S3MetaRequest failed");
        } else {
            // CRTJavaTask succeeded. Clean up...
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

    static class UploadFromRamStream implements HttpRequestBodyStream {
        final long size;
        long bytesWritten;
        byte[] body;

        UploadFromRamStream(byte[] body, long size) {
            this.body = body;
            this.size = size;
        }

        @Override
        public boolean sendRequestBody(ByteBuffer dstBuf) {
            long bufferSpaceAvailable = dstBuf.remaining();
            long bodyBytesAvailable = size - bytesWritten;
            long amountToWrite = Math.min(bufferSpaceAvailable, bodyBytesAvailable);

            while (bytesWritten < size && dstBuf.remaining() > 0) {
                long amtToTransfer = Math.min(size - bytesWritten, (long) dstBuf.remaining());
                amtToTransfer = Math.min(amtToTransfer, (long) body.length);
                dstBuf.put(body, 0, (int) amtToTransfer);
                bytesWritten += (int) amtToTransfer;
            }

            return bytesWritten == size;
        }
    }
}

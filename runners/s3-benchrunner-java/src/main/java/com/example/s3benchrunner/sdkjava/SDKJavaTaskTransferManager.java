package com.example.s3benchrunner.sdkjava;

import software.amazon.awssdk.services.s3.model.S3Object;
import software.amazon.awssdk.transfer.s3.config.DownloadFilter;
import software.amazon.awssdk.transfer.s3.model.*;

import java.nio.file.Path;

public class SDKJavaTaskTransferManager extends SDKJavaTask {

    SDKJavaTaskTransferManager(SDKJavaBenchmarkRunner runner) {
        super(runner);

        if (runner.transferKey != null) {
            /* Transfer a single file */
            if (runner.transferAction.equals("upload")) {
                UploadFileRequest uploadFileRequest = UploadFileRequest.builder()
                        .putObjectRequest(req -> req.bucket(runner.bucket).key(runner.transferKey))
                        .source(runner.transferPath)
                        .build();
                FileUpload transfer = runner.transferManager.uploadFile(uploadFileRequest);
                transfer.completionFuture().whenComplete(
                        (result, failure) -> {
                            completeHelper(runner, failure);
                        });
            } else if (runner.transferAction.equals("download")) {
                DownloadFileRequest downloadFileRequest = DownloadFileRequest.builder()
                        .getObjectRequest(req -> req.bucket(runner.bucket).key(runner.transferKey))
                        .destination(runner.transferPath)
                        .build();
                FileDownload transfer = runner.transferManager.downloadFile(downloadFileRequest);
                transfer.completionFuture().whenComplete(
                        (result, failure) -> {
                            completeHelper(runner, failure);
                        });
            } else {
                throw new RuntimeException("Unknown task action: " + runner.transferAction);
            }
        } else {
            /* Transfer multiple files */
            if (runner.transferAction.equals("upload")) {
                DirectoryUpload directoryUpload = runner.transferManager
                        .uploadDirectory(UploadDirectoryRequest.builder()
                                .source(runner.transferPath)
                                .bucket(runner.bucket)
                                .build());

                directoryUpload.completionFuture().whenComplete(
                        (result, failure) -> {
                            if (result != null && !result.failedTransfers().isEmpty()) {
                                result.failedTransfers().forEach(System.out::println);
                                this.exitWithErrorHelper(runner);
                            }
                            completeHelper(runner, failure);
                        });
            } else if (runner.transferAction.equals("download")) {
                /*
                 * Use the current working directly as destination for download. Transfer
                 * manager will create the file based on the path in the bucket.
                 */
                DirectoryDownload directoryUpload = runner.transferManager
                        .downloadDirectory(DownloadDirectoryRequest.builder()
                                .destination(Path.of(""))
                                .bucket(runner.bucket)
                                .filter(new DownloadFilter() {
                                    @Override
                                    public boolean test(S3Object s3Object) {
                                        return s3Object.key().startsWith(runner.transferPath.toString());
                                    }
                                })
                                .build());

                directoryUpload.completionFuture().whenComplete(
                        (result, failure) -> {
                            if (result != null && !result.failedTransfers().isEmpty()) {
                                result.failedTransfers().forEach(System.out::println);
                                this.exitWithErrorHelper(runner);
                            }
                            completeHelper(runner, failure);
                        });
            } else {
                throw new RuntimeException("Unknown task action: " + runner.transferAction);
            }
        }
    }
}

package com.example.s3benchrunner;

import java.util.Random;

public class Util {
    public static double nanoToSecs(long nanoseconds) {
        return ((double) nanoseconds) / 1_000_000_000;
    }

    public static long bytesFromKiB(long kibibytes) {
        return kibibytes * 1024;
    }

    public static long bytesFromMiB(long mebibytes) {
        return mebibytes * 1024 * 1024;
    }

    public static long bytesFromGiB(long gibibytes) {
        return gibibytes * 1024 * 1024 * 1024;
    }

    public static double bytesToMiB(long bytes) {
        return ((double) bytes) / (1024 * 1024);
    }

    public static double bytesToGiB(long bytes) {
        return ((double) bytes) / (1024 * 1024 * 1024);
    }

    public static double bytesToMegabit(long bytes) {
        return ((double) bytes * 8) / 1_000_000;
    }

    public static double bytesToGigabit(long bytes) {
        return ((double) bytes * 8) / 1_000_000_000;
    }

    public static void exitWithError(String msg) {
        System.err.println("FAIL - " + msg);
        System.exit(255);
    }

    public static void exitWithSkipCode(String msg) {
        System.err.println("Skipping benchmark - " + msg);
        System.exit(123);
    }

    public static byte[] generateRandomData(BenchmarkConfig config) {

        long largestUpload = 0;
        byte[] randomDataForUpload;
        for (var task : config.tasks) {
            if (task.action.equals("upload")) {
                largestUpload = Math.max(largestUpload, task.size);
            }
        }

        // NOTE: if this raises an exception, either the size > Integer.MAX_VALUE
        // or we failed allocating such a large buffer.
        // So we need a new technique.
        // Either generate random data within sendRequestBody()
        // (may impact performance).
        // Or just use a smaller buffer that we send repeatedly.
        randomDataForUpload = new byte[Math.toIntExact(largestUpload)];
        new Random().nextBytes(randomDataForUpload);
        return randomDataForUpload;
    }
}

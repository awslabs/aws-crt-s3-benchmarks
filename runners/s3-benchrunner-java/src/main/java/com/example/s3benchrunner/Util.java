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
        // We are not using System.exit because it can get blocked as it tries to do a
        // proper shutdown by calling the shutdown sequence.
        Runtime.getRuntime().halt(255);
    }

    public static void exitWithSkipCode(String msg) {
        System.err.println("Skipping benchmark - " + msg);
        Runtime.getRuntime().halt(123);
    }

    public static byte[] generateRandomData() {
        byte[] randomDataForUpload = new byte[Math.toIntExact(bytesFromMiB(8))];
        new Random().nextBytes(randomDataForUpload);
        return randomDataForUpload;
    }
}

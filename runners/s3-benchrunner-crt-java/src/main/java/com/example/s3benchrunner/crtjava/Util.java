package com.example.s3benchrunner.crtjava;

class Util {
    static double nanoToSecs(long nanoseconds) {
        return ((double) nanoseconds) / 1_000_000_000;
    }

    static long bytesFromKiB(long kibibytes) {
        return kibibytes * 1024;
    }

    static long bytesFromMiB(long mebibytes) {
        return mebibytes * 1024 * 1024;
    }

    static long bytesFromGiB(long gibibytes) {
        return gibibytes * 1024 * 1024 * 1024;
    }

    static double bytesToMiB(long bytes) {
        return ((double) bytes) / (1024 * 1024);
    }

    static double bytesToGiB(long bytes) {
        return ((double) bytes) / (1024 * 1024 * 1024);
    }

    static double bytesToMegabit(long bytes) {
        return ((double) bytes * 8) / 1_000_000;
    }

    static double bytesToGigabit(long bytes) {
        return ((double) bytes * 8) / 1_000_000_000;
    }

    static void exitWithError(String msg) {
        System.err.println("FAIL - " + msg);
        System.exit(255);
    }

    static void exitWithSkipCode(String msg) {
        System.err.println("Skipping benchmark - " + msg);
        System.exit(123);
    }
}

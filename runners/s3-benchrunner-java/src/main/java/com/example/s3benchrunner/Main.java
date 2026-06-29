package com.example.s3benchrunner;

import java.util.ArrayList;
import java.util.List;

import com.example.s3benchrunner.crtjava.CRTJavaBenchmarkRunner;
import com.example.s3benchrunner.sdkjava.SDKJavaBenchmarkRunner;

public class Main {

    /////////////// BEGIN ARBITRARY HARDCODED VALUES ///////////////

    // 256MiB is Java Transfer Mgr v2 default.
    // This benchmark can turn off backpressure and rely solely on the
    // memory-limiter.
    public static final int BACKPRESSURE_INITIAL_READ_WINDOW_MiB = 0;

    /////////////// END ARBITRARY HARD-CODED VALUES ///////////////

    private static void printStats(long bytesPerRun, List<Double> durations) {
        int n = durations.size();
        List<Double> sortedDurations = new ArrayList<>(durations);
        java.util.Collections.sort(sortedDurations);

        List<Double> throughputs = new ArrayList<>();
        for (Double d : durations) {
            throughputs.add(Util.bytesToGigabit(bytesPerRun) / d);
        }
        List<Double> sortedThroughputs = new ArrayList<>(throughputs);
        java.util.Collections.sort(sortedThroughputs);

        double[] dStats = calcStats(sortedDurations);
        double[] tStats = calcStats(sortedThroughputs);

        long peakRssKiB = 0;
        try {
            // Read peak RSS from /proc/self/status on Linux
            String status = new String(java.nio.file.Files.readAllBytes(
                    java.nio.file.Path.of("/proc/self/status")));
            for (String line : status.split("\n")) {
                if (line.startsWith("VmHWM:")) {
                    peakRssKiB = Long.parseLong(line.replaceAll("[^0-9]", ""));
                    break;
                }
            }
        } catch (Exception e) {
            // Not on Linux or can't read - use Runtime as fallback
            Runtime rt = Runtime.getRuntime();
            peakRssKiB = (rt.totalMemory() - rt.freeMemory()) / 1024;
        }
        double peakRssMiB = peakRssKiB / 1024.0;

        System.out.printf(
                "STATS:{\"runs\":%d,\"bytes_per_run\":%d,\"peak_rss_mib\":%.1f"
                        + ",\"duration\":{\"median\":%.6f,\"mean\":%.6f,\"min\":%.6f,\"max\":%.6f,\"stddev\":%.6f}"
                        + ",\"throughput_gbps\":{\"median\":%.6f,\"mean\":%.6f,\"min\":%.6f,\"max\":%.6f,\"stddev\":%.6f}}%n",
                n, bytesPerRun, peakRssMiB,
                dStats[0], dStats[1], dStats[2], dStats[3], dStats[4],
                tStats[0], tStats[1], tStats[2], tStats[3], tStats[4]);
    }

    // Returns [median, mean, min, max, stddev]
    private static double[] calcStats(List<Double> sorted) {
        int n = sorted.size();
        double min = sorted.get(0);
        double max = sorted.get(n - 1);
        double mean = sorted.stream().mapToDouble(Double::doubleValue).sum() / n;
        double median;
        if (n % 2 == 1) {
            median = sorted.get(n / 2);
        } else {
            median = (sorted.get(n / 2 - 1) + sorted.get(n / 2)) / 2.0;
        }
        double variance = sorted.stream().mapToDouble(v -> (v - mean) * (v - mean) / n).sum();
        double stddev = Math.sqrt(variance);
        return new double[] { median, mean, min, max, stddev };
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 5) {
            throw new RuntimeException("expected args: S3_CLIENT WORKLOAD BUCKET REGION TARGET_THROUGHPUT");
        }
        String s3ClientId = args[0];
        String configJsonFilepath = args[1];
        String bucket = args[2];
        String region = args[3];
        double targetThroughputGbps = Double.parseDouble(args[4]);

        BenchmarkConfig config = BenchmarkConfig.fromJson(configJsonFilepath);
        BenchmarkRunner runner = switch (s3ClientId) {
            case "crt-java" -> new CRTJavaBenchmarkRunner(config, bucket, region, targetThroughputGbps);
            case "sdk-java-client-crt" ->
                new SDKJavaBenchmarkRunner(config, bucket, region, targetThroughputGbps, false, true);
            case "sdk-java-tm-crt" ->
                new SDKJavaBenchmarkRunner(config, bucket, region, targetThroughputGbps, true, true);
            case "sdk-java-client-classic" ->
                new SDKJavaBenchmarkRunner(config, bucket, region, targetThroughputGbps, false, false);
            case "sdk-java-tm-classic" ->
                new SDKJavaBenchmarkRunner(config, bucket, region, targetThroughputGbps, true, false);
            default -> throw new RuntimeException(
                    "Unsupported S3_CLIENT. Options are: crt-java, sdk-java-client-crt, sdk-java-tm-crt, sdk-java-client-classic, sdk-java-tm-classic");
        };

        long bytesPerRun = config.bytesPerRun();

        List<Double> durations = new ArrayList<>();
        // Repeat benchmark until we exceed maxRepeatCount or maxRepeatSecs
        long appStartNs = System.nanoTime();
        for (int runI = 0; runI < config.maxRepeatCount; runI++) {
            runner.prepareRun();

            long runStartNs = System.nanoTime();

            runner.run();

            long runDurationNs = System.nanoTime() - runStartNs;
            double runSecs = Util.nanoToSecs(runDurationNs);
            durations.add(runSecs);
            System.out.printf("Run:%d Secs:%f Gb/s:%f%n",
                    runI + 1,
                    runSecs,
                    Util.bytesToGigabit(bytesPerRun) / runSecs);

            // break out if we've exceeded maxRepeatSecs
            double appDurationSecs = Util.nanoToSecs(System.nanoTime() - appStartNs);
            if (appDurationSecs >= config.maxRepeatSecs) {
                break;
            }
        }

        printStats(bytesPerRun, durations);
    }
}

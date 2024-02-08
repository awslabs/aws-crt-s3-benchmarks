package com.example.s3benchrunner;

import java.util.ArrayList;
import java.util.List;

import com.example.s3benchrunner.crtjava.CRTJavaBenchmarkRunner;
import com.example.s3benchrunner.sdkjava.SDKJavaBenchmarkRunner;

public class Main {

    /////////////// BEGIN ARBITRARY HARDCODED VALUES ///////////////

    // 256MiB is Java Transfer Mgr v2 default.
    // TODO: Investigate. At time of writing, this noticeably impacts performance.
    public static final int BACKPRESSURE_INITIAL_READ_WINDOW_MiB = 256;

    /////////////// END ARBITRARY HARD-CODED VALUES ///////////////

    private static void printStats(long bytesPerRun, List<Double> durations) {
        double n = durations.size();
        double durationMean = 0;
        for (Double duration : durations) {
            durationMean += duration / n;
        }

        double durationVariance = 0;
        for (Double duration : durations) {
            durationVariance += (duration - durationMean) * (duration - durationMean) / n;
        }

        double mbsMean = Util.bytesToMegabit(bytesPerRun) / durationMean;
        double mbsVariance = Util.bytesToMegabit(bytesPerRun) / durationVariance;

        System.out.printf(
                "Overall stats; Throughput Mean:%.1f Mb/s Throughput Variance:%.1f Mb/s Duration Mean:%.3f s Duration Variance:%.3f s %n",
                mbsMean,
                mbsVariance,
                durationMean,
                durationVariance);
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
                new SDKJavaBenchmarkRunner(config, bucket, region, targetThroughputGbps, false);
            case "sdk-java-tm-crt" -> new SDKJavaBenchmarkRunner(config, bucket, region, targetThroughputGbps, true);
            default -> throw new RuntimeException(
                    "Unsupported S3_CLIENT. Options are: crt-java, sdk-java-client-crt, sdk-java-tm-crt");
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
            System.out.printf("Run:%d Secs:%.3f Gb/s:%.1f Mb/s:%.1f GiB/s:%.1f MiB/s:%.1f%n",
                    runI + 1,
                    runSecs,
                    Util.bytesToGigabit(bytesPerRun) / runSecs,
                    Util.bytesToMegabit(bytesPerRun) / runSecs,
                    Util.bytesToGiB(bytesPerRun) / runSecs,
                    Util.bytesToMiB(bytesPerRun) / runSecs);

            // break out if we've exceeded maxRepeatSecs
            double appDurationSecs = Util.nanoToSecs(System.nanoTime() - appStartNs);
            if (appDurationSecs >= config.maxRepeatSecs) {
                break;
            }
        }

        printStats(bytesPerRun, durations);
    }
}

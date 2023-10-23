package com.example.s3benchrunner.crtjava;

import java.util.ArrayList;
import java.util.List;

public class Main {

    /////////////// BEGIN ARBITRARY HARDCODED VALUES ///////////////

    // 256MiB is Java Transfer Mgr v2 default.
    // TODO: Investigate. At time of writing, this noticeably impacts performance.
    static final int BACKPRESSURE_INITIAL_READ_WINDOW_MiB = 256;

    /////////////// END ARBITRARY HARD-CODED VALUES ///////////////

    private static void printStats(long bytesPerRun, List<Double> durations) {
        double n = durations.size();
        double durationMean = 0; 
        for (int i = 0; i < n; ++i) {
            durationMean += durations.get(i) / n; 
        }

        double durationVariance = 0; 
        for (int i = 0; i < n; ++i) {
            durationVariance += (durations.get(i) - durationMean) * (durations.get(i) - durationMean) / n; 
        }

        double gbsMean = Util.bytesToGigabit(bytesPerRun) / durationMean;
        
        System.out.printf("Overall stats; Duration Mean:%.3f s Duration Variance:%.3f s Throughput Mean:%.1f Gb/s \n",
                    durationMean,
                    durationVariance,
                    gbsMean);
    }

    public static void main(String[] args) {
        if (args.length != 4) {
            throw new RuntimeException("expected args: BENCHMARK BUCKET REGION TARGET_THROUGHPUT");
        }
        String configJsonFilepath = args[0];
        String bucket = args[1];
        String region = args[2];
        double targetThroughputGbps = Double.parseDouble(args[3]);

        BenchmarkConfig config = BenchmarkConfig.fromJson(args[0]);
        var benchmark = new Benchmark(config, bucket, region, targetThroughputGbps);
        long bytesPerRun = config.bytesPerRun();

        List<Double> durations = new ArrayList<>();
        // Repeat benchmark until we exceed maxRepeatCount or maxRepeatSecs
        long appStartNs = System.nanoTime();
        for (int runI = 0; runI < config.maxRepeatCount; runI++) {
            long runStartNs = System.nanoTime();

            benchmark.run();

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

package com.example.s3benchrunner.crtjava;

public class Main {
    public static void main(String[] args) {
        if (args.length != 4) {
            throw new RuntimeException("expected args: <config.json> <bucket> <region> <target-throughput-Gbps>");
        }
        String configJsonFilepath = args[0];
        String bucket = args[1];
        String region = args[2];
        double targetThroughputGbps = Double.parseDouble(args[3]);

        BenchmarkConfig config = BenchmarkConfig.fromJson(args[0]);
        var benchmark = new Benchmark(config, bucket, region, targetThroughputGbps);
        long bytesPerRun = config.bytesPerRun();

        // Repeat benchmark until we exceed maxRepeatCount or maxRepeatSecs
        long appStartNs = System.nanoTime();
        for (int runI = 0; runI < config.maxRepeatCount; runI++) {
            long runStartNs = System.nanoTime();

            benchmark.run();

            long runDurationNs = System.nanoTime() - runStartNs;
            double runSecs = Util.nanoToSecs(runDurationNs);
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
    }
}
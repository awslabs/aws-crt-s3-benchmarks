package com.example.s3benchrunner;

import java.lang.management.BufferPoolMXBean;
import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.MemoryPoolMXBean;
import java.lang.management.MemoryUsage;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import com.example.s3benchrunner.crtjava.CRTJavaBenchmarkRunner;
import com.example.s3benchrunner.sdkjava.SDKJavaBenchmarkRunner;

public class Main {

    /////////////// BEGIN ARBITRARY HARDCODED VALUES ///////////////

    // Backpressure read-window size (MiB). 0 = disabled.
    // May be overridden via system property `aws.crt.backpressure.window_mib`.
    // 256MiB is Java Transfer Mgr v2 default.
    // See CRTJavaBenchmarkRunner for the actual use of this value.
    public static int BACKPRESSURE_INITIAL_READ_WINDOW_MiB = 0;

    // SDK-side initial read buffer size (MiB). null = SDK default (partSize * 10 = 80 MiB).
    // May be overridden via system property `aws.sdk.s3.initial_read_buffer_mib`.
    public static Long SDK_INITIAL_READ_BUFFER_MiB = null;

    /////////////// END ARBITRARY HARD-CODED VALUES ///////////////

    static {
        // Enable CRT memory tracing by default (level 1: totals only, low overhead).
        // This is required for CRT.nativeMemory() to return non-zero values.
        // Users can override via -Daws.crt.memory.tracing=<0|1|2>.
        if (System.getProperty("aws.crt.memory.tracing") == null) {
            System.setProperty("aws.crt.memory.tracing", "1");
        }
        String bpStr = System.getProperty("aws.crt.backpressure.window_mib");
        if (bpStr != null) {
            BACKPRESSURE_INITIAL_READ_WINDOW_MiB = Integer.parseInt(bpStr);
        }
        String sdkBufStr = System.getProperty("aws.sdk.s3.initial_read_buffer_mib");
        if (sdkBufStr != null) {
            SDK_INITIAL_READ_BUFFER_MiB = Long.parseLong(sdkBufStr);
        }
    }

    /**
     * Captures a point-in-time snapshot of JVM allocation counters and GC counts.
     * Deltas between two snapshots quantify the allocation + GC work done during
     * a benchmark run.
     */
    private static class AllocSnapshot {
        long totalHeapAllocatedBytes;
        long youngGCs;
        long youngGCTimeMs;
        long oldGCs;
        long oldGCTimeMs;

        static AllocSnapshot capture() {
            AllocSnapshot s = new AllocSnapshot();
            com.sun.management.ThreadMXBean tb =
                (com.sun.management.ThreadMXBean) ManagementFactory.getThreadMXBean();
            // Cumulative across all threads (including terminated ones) since JVM start
            s.totalHeapAllocatedBytes = tb.getTotalThreadAllocatedBytes();
            for (GarbageCollectorMXBean gc : ManagementFactory.getGarbageCollectorMXBeans()) {
                long count = Math.max(0, gc.getCollectionCount());
                long time = Math.max(0, gc.getCollectionTime());
                if (isYoungGen(gc.getName())) {
                    s.youngGCs += count;
                    s.youngGCTimeMs += time;
                } else {
                    s.oldGCs += count;
                    s.oldGCTimeMs += time;
                }
            }
            return s;
        }

        AllocSnapshot deltaFrom(AllocSnapshot start) {
            AllocSnapshot d = new AllocSnapshot();
            d.totalHeapAllocatedBytes = this.totalHeapAllocatedBytes - start.totalHeapAllocatedBytes;
            d.youngGCs = this.youngGCs - start.youngGCs;
            d.youngGCTimeMs = this.youngGCTimeMs - start.youngGCTimeMs;
            d.oldGCs = this.oldGCs - start.oldGCs;
            d.oldGCTimeMs = this.oldGCTimeMs - start.oldGCTimeMs;
            return d;
        }
    }

    /**
     * Background thread that samples volatile memory metrics (CRT native, JVM direct)
     * during benchmark runs so we can report peak-observed values, not just end-of-run
     * snapshots.
     */
    private static class MemorySampler extends Thread {
        private final AtomicBoolean stop = new AtomicBoolean(false);
        private final AtomicLong crtNativePeakBytes = new AtomicLong(0);
        private final AtomicLong jvmDirectPeakBytes = new AtomicLong(0);
        private final long sampleIntervalMs;

        MemorySampler(long sampleIntervalMs) {
            super("bench-memory-sampler");
            this.sampleIntervalMs = sampleIntervalMs;
            setDaemon(true);
        }

        @Override
        public void run() {
            while (!stop.get()) {
                long crtNative = 0;
                try {
                    crtNative = software.amazon.awssdk.crt.CRT.nativeMemory();
                } catch (Throwable t) {
                    // CRT not loaded or tracing disabled; leave at 0
                }
                if (crtNative > crtNativePeakBytes.get()) {
                    crtNativePeakBytes.set(crtNative);
                }
                long directUsed = 0;
                for (BufferPoolMXBean bpb :
                    ManagementFactory.getPlatformMXBeans(BufferPoolMXBean.class)) {
                    if ("direct".equals(bpb.getName())) {
                        directUsed = bpb.getMemoryUsed();
                        break;
                    }
                }
                if (directUsed > jvmDirectPeakBytes.get()) {
                    jvmDirectPeakBytes.set(directUsed);
                }
                try {
                    Thread.sleep(sampleIntervalMs);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        }

        long crtNativePeakMiB() {
            return crtNativePeakBytes.get() / (1024 * 1024);
        }

        long jvmDirectPeakMiB() {
            return jvmDirectPeakBytes.get() / (1024 * 1024);
        }

        void stopSampling() {
            stop.set(true);
            try {
                join(500);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    private static boolean isYoungGen(String gcName) {
        String lower = gcName.toLowerCase();
        return lower.contains("young") || lower.contains("scavenge")
                || lower.contains("parnew") || lower.contains("copy")
                || lower.contains("nursery");
    }

    /**
     * Reads peak resident set size (RSS) from /proc/self/status VmHWM.
     * Falls back to JVM heap usage on non-Linux platforms.
     */
    private static double peakRssMiB() {
        long peakRssKiB = 0;
        try {
            String status = new String(java.nio.file.Files.readAllBytes(
                    java.nio.file.Path.of("/proc/self/status")));
            for (String line : status.split("\n")) {
                if (line.startsWith("VmHWM:")) {
                    peakRssKiB = Long.parseLong(line.replaceAll("[^0-9]", ""));
                    break;
                }
            }
        } catch (Exception e) {
            Runtime rt = Runtime.getRuntime();
            peakRssKiB = (rt.totalMemory() - rt.freeMemory()) / 1024;
        }
        return peakRssKiB / 1024.0;
    }

    /**
     * Sums peak-observed JVM heap usage across all heap pools.
     * MemoryPoolMXBean tracks per-pool high-water marks for free.
     */
    private static long jvmHeapUsedPeakMiB() {
        long peak = 0;
        for (MemoryPoolMXBean pool : ManagementFactory.getMemoryPoolMXBeans()) {
            if (pool.getType() == java.lang.management.MemoryType.HEAP) {
                MemoryUsage peakUsage = pool.getPeakUsage();
                if (peakUsage != null) {
                    peak += peakUsage.getUsed();
                }
            }
        }
        return peak / (1024 * 1024);
    }

    private static long jvmNonHeapUsedMiB() {
        MemoryUsage nonHeap = ManagementFactory.getMemoryMXBean().getNonHeapMemoryUsage();
        return nonHeap.getUsed() / (1024 * 1024);
    }

    /**
     * Prints a human-readable memory + GC summary block.
     * This appears before the machine-readable STATS: line so users see it inline
     * with the rest of the runner's output, before monitor-resource.sh takes over.
     */
    private static void printMemorySummary(AllocSnapshot delta, MemorySampler sampler,
                                            double peakRss, long heapPeak, long nonHeap,
                                            boolean crtTracingEffective) {
        double allocGiB = delta.totalHeapAllocatedBytes / 1024.0 / 1024.0 / 1024.0;
        System.out.printf("=== JVM Memory & Allocation ===%n");
        System.out.printf("Peak RSS (total process):    %10.1f MiB%n", peakRss);
        System.out.printf("JVM heap peak used:          %10d MiB%n", heapPeak);
        System.out.printf("JVM non-heap used (end):     %10d MiB%n", nonHeap);
        System.out.printf("JVM direct peak used:        %10d MiB%n", sampler.jvmDirectPeakMiB());
        if (crtTracingEffective) {
            System.out.printf("CRT native peak (tracked):   %10d MiB%n", sampler.crtNativePeakMiB());
        } else {
            System.out.printf("CRT native peak (tracked):   %10s (aws.crt.memory.tracing disabled)%n", "n/a");
        }
        System.out.printf("Total heap allocated:        %10.1f GiB across all runs%n", allocGiB);
        System.out.printf("Young GCs:                   %10d (%d ms total)%n",
                delta.youngGCs, delta.youngGCTimeMs);
        System.out.printf("Old GCs:                     %10d (%d ms total)%n",
                delta.oldGCs, delta.oldGCTimeMs);
    }

    private static void printStats(long bytesPerRun, List<Double> durations,
                                    AllocSnapshot delta, MemorySampler sampler,
                                    double peakRss, long heapPeak, long nonHeap,
                                    boolean crtTracingEffective) {
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

        // Backward-compatible STATS: line at top level, extended with memory + allocation sub-objects.
        System.out.printf(
                "STATS:{\"runs\":%d,\"bytes_per_run\":%d,\"peak_rss_mib\":%.1f"
                        + ",\"duration\":{\"median\":%.6f,\"mean\":%.6f,\"min\":%.6f,\"max\":%.6f,\"stddev\":%.6f}"
                        + ",\"throughput_gbps\":{\"median\":%.6f,\"mean\":%.6f,\"min\":%.6f,\"max\":%.6f,\"stddev\":%.6f}"
                        + ",\"memory\":{\"peak_rss_mib\":%.1f,\"jvm_heap_used_peak_mib\":%d,\"jvm_non_heap_used_mib\":%d"
                        + ",\"jvm_direct_used_peak_mib\":%d,\"crt_native_peak_mib\":%d"
                        + ",\"crt_tracing_enabled\":%b}"
                        + ",\"allocation\":{\"total_heap_allocated_bytes\":%d,\"young_gcs\":%d,\"young_gc_ms\":%d"
                        + ",\"old_gcs\":%d,\"old_gc_ms\":%d}}%n",
                n, bytesPerRun, peakRss,
                dStats[0], dStats[1], dStats[2], dStats[3], dStats[4],
                tStats[0], tStats[1], tStats[2], tStats[3], tStats[4],
                peakRss, heapPeak, nonHeap,
                sampler.jvmDirectPeakMiB(), sampler.crtNativePeakMiB(),
                crtTracingEffective,
                delta.totalHeapAllocatedBytes, delta.youngGCs, delta.youngGCTimeMs,
                delta.oldGCs, delta.oldGCTimeMs);
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

        // Baseline allocation/GC snapshot BEFORE any runs (excludes JVM startup + client init).
        AllocSnapshot startSnap = AllocSnapshot.capture();

        // Start sampling volatile memory metrics (CRT native, JVM direct) during runs.
        MemorySampler sampler = new MemorySampler(100);
        sampler.start();

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

        // Stop sampling and gather all snapshots.
        sampler.stopSampling();
        AllocSnapshot endSnap = AllocSnapshot.capture();
        AllocSnapshot delta = endSnap.deltaFrom(startSnap);

        double peakRss = peakRssMiB();
        long heapPeak = jvmHeapUsedPeakMiB();
        long nonHeap = jvmNonHeapUsedMiB();
        // CRT tracing is effective if we've seen any tracked native memory.
        // (If tracing is disabled, CRT.nativeMemory() always returns 0.)
        boolean crtTracingEffective = sampler.crtNativePeakMiB() > 0
                || "1".equals(System.getProperty("aws.crt.memory.tracing"))
                || "2".equals(System.getProperty("aws.crt.memory.tracing"));

        printMemorySummary(delta, sampler, peakRss, heapPeak, nonHeap, crtTracingEffective);
        printStats(bytesPerRun, durations, delta, sampler, peakRss, heapPeak, nonHeap, crtTracingEffective);
    }
}

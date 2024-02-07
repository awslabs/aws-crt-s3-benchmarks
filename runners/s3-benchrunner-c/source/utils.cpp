#include "utils.h"

BenchmarkRunner::BenchmarkRunner(const BenchmarkConfig &config, string_view bucket, string_view region)
{
    this->config = config;
    this->bucket = bucket;
    this->region = region;

    // If we're uploading, and not using files on disk,
    // then generate an in-memory buffer of random data to upload.
    // All uploads will use this same buffer, so make it big enough for the largest file.
    if (!config.filesOnDisk)
    {
        for (auto &&task : config.tasks)
        {
            if (task.action == "upload")
            {
                if (task.size > randomDataForUpload.size())
                {
                    size_t prevSize = randomDataForUpload.size();
                    randomDataForUpload.resize(task.size);

                    independent_bits_engine<default_random_engine, CHAR_BIT, unsigned char> randEngine;
                    generate(randomDataForUpload.begin() + prevSize, randomDataForUpload.end(), randEngine);
                }
            }
        }
    }
}

// exit due to failure
[[noreturn]] void fail(string_view msg)
{
    cerr << "FAIL - " << msg << endl;
    abort();
}

// exit because we're skipping the benchmark (e.g. has version# this runner doesn't support yet)
[[noreturn]] void skip(string_view msg)
{
    cerr << "Skipping benchmark - " << msg << endl;
    exit(123);
}

uint64_t bytesFromKiB(uint64_t kibibytes)
{
    return kibibytes * 1024;
}

uint64_t bytesFromMiB(uint64_t mebibytes)
{
    return mebibytes * 1024 * 1024;
}

uint64_t bytesFromGiB(uint64_t gibibytes)
{
    return gibibytes * 1024 * 1024 * 1024;
}

double bytesToKiB(uint64_t bytes)
{
    return (double)bytes / 1024;
}

double bytesToMiB(uint64_t bytes)
{
    return (double)bytes / (1024 * 1024);
}

double bytesToGiB(uint64_t bytes)
{
    return (double)bytes / (1024 * 1024 * 1024);
}

double bytesToKilobit(uint64_t bytes)
{
    return ((double)bytes * 8) / 1'000;
}

double bytesToMegabit(uint64_t bytes)
{
    return ((double)bytes * 8) / 1'000'000;
}

double bytesToGigabit(uint64_t bytes)
{
    return ((double)bytes * 8) / 1'000'000'000;
}

aws_byte_cursor toCursor(string_view src)
{
    return aws_byte_cursor{.len = src.length(), .ptr = (uint8_t *)src.data()};
}

void printStats(uint64_t bytesPerRun, const vector<double> &durations)
{
    double n = durations.size();
    double durationMean = std::accumulate(durations.begin(), durations.end(), 0.0) / n;

    double durationVariance = std::accumulate(
        durations.begin(),
        durations.end(),
        0.0,
        [&durationMean, &n](double accumulator, const double &val)
        { return accumulator + ((val - durationMean) * (val - durationMean) / n); });

    double mbsMean = bytesToMegabit(bytesPerRun) / durationMean;
    double mbsVariance = bytesToMegabit(bytesPerRun) / durationVariance;

    struct aws_memory_usage_stats mu;
    aws_init_memory_usage_for_current_process(&mu);

    printf(
        "Overall stats; Throughput Mean:%.1f Mb/s Throughput Variance:%.1f Mb/s Duration Mean:%.3f s Duration "
        "Variance:%.3f s Peak RSS:%.3f Mb\n",
        mbsMean,
        mbsVariance,
        durationMean,
        durationVariance,
        (double)mu.maxrss / 1024.0);
}

void main_run(BenchmarkRunner &runner, BenchmarkConfig &config)
{
    uint64_t bytesPerRun = config.bytesPerRun();

    // Repeat benchmark until we exceed maxRepeatCount or maxRepeatSecs
    std::vector<double> durations;
    auto appStart = high_resolution_clock::now();
    for (int runI = 0; runI < config.maxRepeatCount; ++runI)
    {
        auto runStart = high_resolution_clock::now();

        runner.run();

        duration<double> runDurationSecs = high_resolution_clock::now() - runStart;
        double runSecs = runDurationSecs.count();
        durations.push_back(runSecs);
        fflush(stderr);
        printf(
            "Run:%d Secs:%.3f Gb/s:%.1f Mb/s:%.1f GiB/s:%.1f MiB/s:%.1f\n",
            runI + 1,
            runSecs,
            bytesToGigabit(bytesPerRun) / runSecs,
            bytesToMegabit(bytesPerRun) / runSecs,
            bytesToGiB(bytesPerRun) / runSecs,
            bytesToMiB(bytesPerRun) / runSecs);
        fflush(stdout);

        // break out if we've exceeded maxRepeatSecs
        duration<double> appDurationSecs = high_resolution_clock::now() - appStart;
        if (appDurationSecs >= 1s * config.maxRepeatSecs)
            break;
    }

    printStats(bytesPerRun, durations);
}

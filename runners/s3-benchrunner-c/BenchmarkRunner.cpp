#include "BenchmarkRunner.h"

#include <algorithm>
#include <fstream>
#include <iostream>
#include <random>

#include <aws/common/system_resource_util.h>

#include <nlohmann/json.hpp>

using namespace std;
using namespace std::chrono;
using json = nlohmann::json;

struct TaskConfig;
class Benchmark;

// exit due to failure
[[noreturn]] void fail(string_view msg)
{
    cerr << "FAIL - " << msg << endl;
    _Exit(255);
}

// exit because we're skipping the benchmark (e.g. has version# this runner doesn't support yet)
[[noreturn]] void skip(string_view msg)
{
    cerr << "Skipping benchmark - " << msg << endl;
    _Exit(123);
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

BenchmarkConfig::BenchmarkConfig(
    std::string_view jsonFilepath,
    std::string_view bucket,
    std::string_view region,
    double targetThroughputGbps)
    : bucket(bucket), region(region), targetThroughputGbps(targetThroughputGbps)
{
    auto f = ifstream(string(jsonFilepath));
    if (!f)
        fail(string("Couldn't open file: ") + string(jsonFilepath));

    auto json = json::parse(f, /*cb*/ nullptr, /*exceptions*/ false);
    if (json.is_discarded())
        fail(string("Couldn't parse JSON: ") + string(jsonFilepath));

    int version = json["version"];
    if (version != 2)
        skip("workload version not supported");

    this->maxRepeatCount = json["maxRepeatCount"];
    this->maxRepeatSecs = json["maxRepeatSecs"];

    if (!json["checksum"].is_null())
        this->checksum = json["checksum"];

    this->filesOnDisk = json["filesOnDisk"];

    for (auto &&taskJson : json["tasks"])
    {
        auto &task = this->tasks.emplace_back();
        task.action = taskJson["action"];
        task.key = taskJson["key"];
        task.size = taskJson["size"];
    }
}

uint64_t BenchmarkConfig::bytesPerRun() const
{
    uint64_t bytes = 0;
    for (auto &&task : tasks)
        bytes += task.size;
    return bytes;
}

// Instantiates S3 Client, does not run the benchmark yet
BenchmarkRunner::BenchmarkRunner(const BenchmarkConfig &config) : config(config)
{
    // If we're uploading, and not using files on disk,
    // then generate an in-memory buffer of random data to upload.
    // All uploads will use this same buffer, so make it big enough for the largest file.
    if (!config.filesOnDisk)
    {
        size_t maxUploadSize = 0;
        for (auto &&task : config.tasks)
            if (task.action == "upload")
                maxUploadSize = std::max(maxUploadSize, (size_t)task.size);

        randomDataForUpload.resize(maxUploadSize);
        independent_bits_engine<default_random_engine, CHAR_BIT, unsigned char> randEngine;
        generate(randomDataForUpload.begin(), randomDataForUpload.end(), randEngine);
    }
}

BenchmarkRunner::~BenchmarkRunner() = default;

// Print all kinds of stats about these values (median, mean, min, max, etc)
void printValueStats(const char *label, vector<double> values)
{
    std::sort(values.begin(), values.end());
    double n = values.size();
    double min = values.front();
    double max = values.back();
    double mean = std::accumulate(values.begin(), values.end(), 0.0) / n;

    double median = values.front();
    if (values.size() > 1)
    {
        size_t middle = values.size() / 2;
        if (values.size() % 2 == 1)
        {
            // odd number, use middle value
            median = values[middle];
        }
        else
        {
            // even number, use avg of two middle values
            double a = values[middle - 1];
            double b = values[middle];
            median = (a + b) / 2;
        }
    }

    double variance = std::accumulate(
        values.begin(),
        values.end(),
        0.0,
        [mean, n](double accumulator, const double &val) { return accumulator + ((val - mean) * (val - mean) / n); });

    double stdDev = std::sqrt(variance);

    printf(
        "Overall %s Median:%f Mean:%f Min:%f Max:%f Variance:%f StdDev:%f\n",
        label,
        median,
        mean,
        min,
        max,
        variance,
        stdDev);
}

void printAllStats(uint64_t bytesPerRun, const vector<double> &durations)
{
    vector<double> throughputsGbps;
    for (double duration : durations)
        throughputsGbps.push_back(bytesToGigabit(bytesPerRun) / duration);

    printValueStats("Throughput (Gb/s)", throughputsGbps);

    printValueStats("Duration (Secs)", durations);

    struct aws_memory_usage_stats mu;
    aws_init_memory_usage_for_current_process(&mu);

    printf("Peak RSS:%f MiB\n", (double)mu.maxrss / 1024.0);
}

int benchmarkRunnerMain(int argc, char *argv[], const CreateRunnerFromNameFn &createRunnerFromName)
{
    if (argc != 6)
        fail(string("usage: ") + argv[0] + " S3_CLIENT WORKLOAD BUCKET REGION TARGET_THROUGHPUT");

    string s3ClientId = argv[1];
    string workload = argv[2];
    string bucket = argv[3];
    string region = argv[4];
    double targetThroughputGbps = stod(argv[5]);

    auto config = BenchmarkConfig(workload, bucket, region, targetThroughputGbps);
    unique_ptr<BenchmarkRunner> benchmark = createRunnerFromName(s3ClientId, config);
    uint64_t bytesPerRun = config.bytesPerRun();

    // Repeat benchmark until we exceed maxRepeatCount or maxRepeatSecs
    std::vector<double> durations;
    auto appStart = high_resolution_clock::now();
    for (int runI = 0; runI < config.maxRepeatCount; ++runI)
    {
        auto runStart = high_resolution_clock::now();

        benchmark->run();

        duration<double> runDurationSecs = high_resolution_clock::now() - runStart;
        double runSecs = runDurationSecs.count();
        durations.push_back(runSecs);
        fflush(stderr);
        printf("Run:%d Secs:%f Gb/s:%f\n", runI + 1, runSecs, bytesToGigabit(bytesPerRun) / runSecs);
        fflush(stdout);

        // break out if we've exceeded maxRepeatSecs
        duration<double> appDurationSecs = high_resolution_clock::now() - appStart;
        if (appDurationSecs >= 1s * config.maxRepeatSecs)
            break;
    }

    printAllStats(bytesPerRun, durations);

    return 0;
}

#pragma once

#include <functional>
#include <memory>
#include <string>
#include <vector>

struct TaskConfig;

// exit due to failure
[[noreturn]] void fail(std::string_view msg);

// exit because we're skipping the benchmark (e.g. has version# this runner doesn't support yet)
[[noreturn]] void skip(std::string_view msg);

uint64_t bytesFromKiB(uint64_t kibibytes);
uint64_t bytesFromMiB(uint64_t mebibytes);
uint64_t bytesFromGiB(uint64_t gibibytes);
double bytesToKiB(uint64_t bytes);
double bytesToMiB(uint64_t bytes);
double bytesToGiB(uint64_t bytes);
double bytesToKilobit(uint64_t bytes);
double bytesToMegabit(uint64_t bytes);
double bytesToGigabit(uint64_t bytes);

// use standardized part-size across all benchmarks
#define PART_SIZE (8 * 1024 * 1024)

// struct for a benchmark config, loaded from JSON
struct BenchmarkConfig
{
    // loaded from workload json...
    int maxRepeatCount;
    int maxRepeatSecs;
    std::string checksum;
    bool filesOnDisk;
    std::vector<TaskConfig> tasks;

    // passed on cmdline...
    std::string bucket;
    std::string region;
    double targetThroughputGbps;
    std::vector<std::string> networkInterfaceNames;

    std::string telemetryFileBasePath = "";

    BenchmarkConfig(
        std::string_view jsonFilepath,
        std::string_view bucket,
        std::string_view region,
        double targetThroughputGbps,
        std::string_view network_interfaces,
        std::string_view telemetryFileBasePath);

    uint64_t bytesPerRun() const;
};

// struct for a task in the benchmark's JSON config
struct TaskConfig
{
    std::string action;
    std::string key;
    uint64_t size;
};

// Base class for runnable benchmark
class BenchmarkRunner
{
  protected:
    BenchmarkConfig config;

    // if uploading, and filesOnDisk is false, then upload this
    std::vector<uint8_t> randomDataForUpload;

  public:
    BenchmarkRunner(const BenchmarkConfig &config);
    virtual ~BenchmarkRunner();

    BenchmarkRunner(const BenchmarkRunner &) = delete;
    BenchmarkRunner &operator=(const BenchmarkRunner &) = delete;

    // A benchmark can be run repeatedly
    virtual void run(size_t runNumber) = 0;
};

using CreateRunnerFromNameFn =
    std::function<std::unique_ptr<BenchmarkRunner>(std::string_view id, const BenchmarkConfig &config)>;

// common main() function
int benchmarkRunnerMain(int argc, char *argv[], const CreateRunnerFromNameFn &createRunnerFromName);

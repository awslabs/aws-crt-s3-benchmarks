#include "BenchmarkRunner.h"

#include <algorithm>
#include <fstream>
#include <iostream>
#include <random>
#include <sstream>
#include <string>

#include <aws/common/system_resource_util.h>

#include <argh.h>
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
    double targetThroughputGbps,
    std::string_view networkInterfaceNames,
    std::string_view telemetryFileBasePath)
    : bucket(bucket), region(region), targetThroughputGbps(targetThroughputGbps),
      telemetryFileBasePath(telemetryFileBasePath)
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

    if (!networkInterfaceNames.empty())
    {
        std::istringstream ss((std::string(networkInterfaceNames)));
        std::string interface;
        while (std::getline(ss, interface, ','))
        {
            if (!interface.empty())
            {
                this->networkInterfaceNames.push_back(interface);
            }
        }
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

        // Generating randomness is slower then copying memory. Therefore, only fill SOME
        // of the buffer with randomness, and fill the rest with copies of that randomness.

        // We don't want any parts to be identical.
        // Use something that won't fall on a part boundary as we copy it.
        const size_t randomBlockSize = std::min((size_t)31415926, maxUploadSize); // approx 30MiB, digits of pi
        std::vector<uint8_t> randomBlock(randomBlockSize);
        independent_bits_engine<default_random_engine, CHAR_BIT, unsigned char> randEngine;
        generate(randomBlock.begin(), randomBlock.end(), randEngine);

        // Resize the buffer to the maximum upload size
        randomDataForUpload.resize(maxUploadSize);

        // Fill the buffer by repeating the random block
        size_t bytesWritten = 0;
        while (bytesWritten < maxUploadSize)
        {
            // Calculate how many bytes to copy in this iteration
            size_t bytesToCopy = std::min(randomBlockSize, maxUploadSize - bytesWritten);

            // Copy the bytes from the random block to the target buffer
            std::copy(
                randomBlock.begin(), randomBlock.begin() + bytesToCopy, randomDataForUpload.begin() + bytesWritten);

            bytesWritten += bytesToCopy;
        }
    }
}

BenchmarkRunner::~BenchmarkRunner() = default;

// If telemetry is enabled, output stats for each run to ./telemetry/<workload_name>/<current_date_time>/stats.txt
FILE *statsFile = NULL;

// Print to both stdout and statsFile
template <typename... Args> void StatsPrintf(const char *fmt, Args... args)
{
    // Print to stdout
    printf(fmt, args...);

    // Print to statsFile if it exists
    if (statsFile)
    {
        fprintf(statsFile, fmt, args...);
        fflush(statsFile);
    }
}

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

    StatsPrintf(
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

    StatsPrintf("Peak RSS:%f MiB\n", (double)mu.maxrss / 1024.0);
}

/**
 * Extracts the workload name from a path.
 * Given "path/to/my-workload.run.json" returns "my-workload".
 */
string workload_name(string_view path)
{
    // Get the filename without the path
    string filename = filesystem::path(path).filename().string();

    // Get everything before the first dot
    auto first_dot = filename.find('.');
    if (first_dot != string::npos)
    {
        return filename.substr(0, first_dot);
    }

    return filename;
}

struct Args
{
    string s3ClientId;
    string workload;
    string bucket;
    string region;
    double targetThroughputGbps;

    // Optional arguments
    string networkInterfaceNames = "";
    bool telemetry = false;
};

int benchmarkRunnerMain(int argc, char *argv[], const CreateRunnerFromNameFn &createRunnerFromName)
{
    // START Argument Parsing
    argh::parser cmdl;
    // pre-register optional named arguments to support --param_name param_value syntax
    cmdl.add_params({"--nic"});
    cmdl.parse(argc, argv);

    if (cmdl[{"-h", "--help"}] || cmdl.pos_args().size() < 6)
    {
        fail(
            std::string("usage: ") + argv[0] +
            " S3_CLIENT WORKLOAD BUCKET REGION TARGET_THROUGHPUT [--nic name1,name2] [--telemetry]");
    }

    struct Args parsedArgs;

    // Parse required positional parameters
    parsedArgs.s3ClientId = cmdl[1];
    parsedArgs.workload = cmdl[2];
    parsedArgs.bucket = cmdl[3];
    parsedArgs.region = cmdl[4];
    parsedArgs.targetThroughputGbps = stod(cmdl[5]);

    // Parse optional named arguments
    cmdl("nic") >> parsedArgs.networkInterfaceNames;

    if (cmdl["telemetry"])
    {
        parsedArgs.telemetry = true;
    }

    // END argument parsing

    string telemetryFileBasePath = "";
    if (parsedArgs.telemetry)
    {
        auto now = chrono::system_clock::to_time_t(chrono::system_clock::now());
        stringstream ss;
        ss << "telemetry/";
        ss << workload_name(parsedArgs.workload) << "/";
        ss << put_time(localtime(&now), "%Y-%m-%d_%H-%M-%S");

        telemetryFileBasePath = ss.str();
        // Create the directory
        error_code ec;
        filesystem::create_directories(telemetryFileBasePath, ec);
        if (ec)
        {
            fail(string("Unable to create directory for telemetry files: ") + ec.message());
        }
        statsFile = fopen((telemetryFileBasePath + "/stats.txt").c_str(), "w");
    }

    auto config = BenchmarkConfig(
        parsedArgs.workload,
        parsedArgs.bucket,
        parsedArgs.region,
        parsedArgs.targetThroughputGbps,
        parsedArgs.networkInterfaceNames,
        telemetryFileBasePath);
    unique_ptr<BenchmarkRunner> benchmark = createRunnerFromName(parsedArgs.s3ClientId, config);
    uint64_t bytesPerRun = config.bytesPerRun();

    // Repeat benchmark until we exceed maxRepeatCount or maxRepeatSecs
    std::vector<double> durations;
    auto appStart = high_resolution_clock::now();
    for (int runNumber = 1; runNumber <= config.maxRepeatCount; ++runNumber)
    {
        auto runStart = high_resolution_clock::now();

        benchmark->run(runNumber);

        duration<double> runDurationSecs = high_resolution_clock::now() - runStart;
        double runSecs = runDurationSecs.count();
        durations.push_back(runSecs);
        fflush(stderr);
        StatsPrintf("Run:%d Secs:%f Gb/s:%f\n", runNumber, runSecs, bytesToGigabit(bytesPerRun) / runSecs);
        fflush(stdout);

        // break out if we've exceeded maxRepeatSecs
        duration<double> appDurationSecs = high_resolution_clock::now() - appStart;
        if (appDurationSecs >= 1s * config.maxRepeatSecs)
            break;
    }

    printAllStats(bytesPerRun, durations);

    if (statsFile != NULL)
    {
        fclose(statsFile);
    }

    return 0;
}

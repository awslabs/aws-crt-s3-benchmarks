#pragma once

#include <aws/common/system_resource_util.h>
#include <aws/s3/s3_client.h>

#include <chrono>
#include <cstdio>
#include <fstream>
#include <functional>
#include <future>
#include <iostream>
#include <list>
#include <nlohmann/json.hpp>
#include <random>
#include <thread>
#include <vector>

using namespace std;
using namespace std::chrono;
using namespace std::chrono_literals;
using json = nlohmann::json;

/////////////// BEGIN ARBITRARY HARDCODED VALUES ///////////////

// 256MiB is Java Transfer Mgr V2's default
// TODO: Investigate. At time of writing, this noticeably impacts performance.
#define BACKPRESSURE_INITIAL_READ_WINDOW_MiB 256

/////////////// END ARBITRARY HARD-CODED VALUES ///////////////

// exit due to failure
[[noreturn]] void fail(string_view msg);
// exit because we're skipping the benchmark (e.g. has version# this runner doesn't support yet)
[[noreturn]] void skip(string_view msg);

// struct for a task in the benchmark's JSON config
struct TaskConfig
{
    string action;
    string key;
    uint64_t size;
};

// struct for a benchmark config, loaded from JSON
struct BenchmarkConfig
{
    int maxRepeatCount;
    int maxRepeatSecs;
    aws_s3_checksum_algorithm checksum;
    bool filesOnDisk;
    vector<TaskConfig> tasks;

    static BenchmarkConfig fromJson(const string &jsonFilepath)
    {
        BenchmarkConfig config;
        ifstream f(jsonFilepath);
        if (!f)
            fail(string("Couldn't open file: ") + string(jsonFilepath));

        auto json = json::parse(f, /*cb*/ nullptr, /*exceptions*/ false);
        if (json.is_discarded())
            fail(string("Couldn't parse JSON: ") + string(jsonFilepath));

        int version = json["version"];
        if (version != 2)
            skip("workload version not supported");

        config.maxRepeatCount = json["maxRepeatCount"];
        config.maxRepeatSecs = json["maxRepeatSecs"];

        config.checksum = AWS_SCA_NONE;
        if (!json["checksum"].is_null())
        {
            string checksumStr = json["checksum"];
            if (checksumStr == "CRC32")
                config.checksum = AWS_SCA_CRC32;
            else if (checksumStr == "CRC32C")
                config.checksum = AWS_SCA_CRC32C;
            else if (checksumStr == "SHA1")
                config.checksum = AWS_SCA_SHA1;
            else if (checksumStr == "SHA256")
                config.checksum = AWS_SCA_SHA256;
            else
                fail(string("Unknown checksum: ") + checksumStr);
        }

        config.filesOnDisk = json["filesOnDisk"];

        for (auto &&taskJson : json["tasks"])
        {
            auto &task = config.tasks.emplace_back();
            task.action = taskJson["action"];
            task.key = taskJson["key"];
            task.size = taskJson["size"];
        }

        return config;
    }

    uint64_t bytesPerRun() const
    {
        uint64_t bytes = 0;
        for (auto &&task : tasks)
            bytes += task.size;
        return bytes;
    }
};

class BenchmarkRunner
{
  protected:
    string bucket;
    string region;

    // if uploading, and filesOnDisk is false, then upload this
    vector<uint8_t> randomDataForUpload;

  public:
    BenchmarkConfig config;
    BenchmarkRunner(const BenchmarkConfig &config, string_view bucket, string_view region);

    ~BenchmarkRunner() = default;

    virtual void run() {}
};

class Task
{
  protected:
    promise<void> donePromise;
    future<void> doneFuture;

  public:
    size_t taskI;
    TaskConfig &config;

    Task(BenchmarkRunner &runner, size_t taskI)
        : taskI(taskI), config(runner.config.tasks[taskI]), donePromise(), doneFuture(donePromise.get_future()){};

    ~Task() = default;

    void waitUntilDone() { return doneFuture.wait(); }
};

uint64_t bytesFromKiB(uint64_t kibibytes);
uint64_t bytesFromMiB(uint64_t mebibytes);
uint64_t bytesFromGiB(uint64_t gibibytes);
double bytesToKiB(uint64_t bytes);
double bytesToMiB(uint64_t bytes);
double bytesToGiB(uint64_t bytes);
double bytesToKilobit(uint64_t bytes);
double bytesToMegabit(uint64_t bytes);
double bytesToGigabit(uint64_t bytes);

aws_byte_cursor toCursor(string_view src);

void printStats(uint64_t bytesPerRun, const vector<double> &durations);
void main_run(BenchmarkRunner &runner, BenchmarkConfig &config);

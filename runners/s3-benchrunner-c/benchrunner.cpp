#include <chrono>
#include <cstdio>
#include <iostream>
#include <fstream>
#include <functional>
#include <future>
#include <list>
#include <random>
#include <regex>
#include <thread>

#include <aws/auth/credentials.h>
#include <aws/http/connection.h>
#include <aws/http/request_response.h>
#include <aws/io/channel_bootstrap.h>
#include <aws/io/event_loop.h>
#include <aws/io/host_resolver.h>
#include <aws/s3/s3_client.h>
#include <aws/io/stream.h>
#include <aws/io/tls_channel_handler.h>
#include <nlohmann/json.hpp>

using namespace std;
using namespace std::chrono;
using namespace std::chrono_literals;
using json = nlohmann::json;

struct TaskConfig;
class Benchmark;

/////////////// BEGIN ARBITRARY HARDCODED VALUES ///////////////

// 256MiB is Java Transfer Mgr V2's default
// TODO: Investigate. At time of writing, this noticeably impacts performance.
#define BACKPRESSURE_INITIAL_READ_WINDOW_MiB 256

/////////////// END ARBITRARY HARD-CODED VALUES ///////////////

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

// struct for a benchmark config, loaded from JSON
struct BenchmarkConfig
{
    int maxRepeatCount;
    int maxRepeatSecs;
    aws_s3_checksum_algorithm checksum;
    bool filesOnDisk;
    vector<TaskConfig> tasks;

    static BenchmarkConfig fromJson(const string &jsonFilepath);
    uint64_t bytesPerRun() const;
};

// struct for a task in the benchmark's JSON config
struct TaskConfig
{
    string action;
    string key;
    uint64_t size;
};

// A runnable task
class Task
{
    Benchmark &benchmark;
    size_t taskI;
    TaskConfig &config;
    aws_s3_meta_request *metaRequest;
    promise<void> donePromise;
    future<void> doneFuture;

    FILE *downloadFile = NULL;

    static int onDownloadData(struct aws_s3_meta_request *meta_request,
                              const struct aws_byte_cursor *body,
                              uint64_t range_start,
                              void *user_data);

    static void onFinished(struct aws_s3_meta_request *meta_request,
                           const struct aws_s3_meta_request_result *meta_request_result,
                           void *user_data);

public:
    // Creates the task and begins its work
    Task(Benchmark &benchmark, size_t taskI);

    void waitUntilDone() { return doneFuture.wait(); }
};

// A runnable benchmark
class Benchmark
{
    BenchmarkConfig config;
    string bucket;
    string region;
    string targetGbps;

    // CRT boilerplate
    aws_allocator *alloc = NULL;
    aws_logger logger;
    aws_event_loop_group *eventLoopGroup = NULL;
    aws_host_resolver *hostResolver = NULL;
    aws_client_bootstrap *clientBootstrap = NULL;
    aws_tls_ctx *tlsCtx = NULL;
    aws_credentials_provider *credentialsProvider = NULL;
    aws_s3_client *s3Client = NULL;

    // if uploading, and filesOnDisk is false, then upload this
    vector<uint8_t> randomDataForUpload;

public:
    // Instantiates S3 Client, does not run the benchmark yet
    Benchmark(const BenchmarkConfig &config, string_view bucket, string_view region, double targetThroughputGbps);

    ~Benchmark();

    // A benchmark can be run repeatedly
    void run();

    friend class Task;
};

BenchmarkConfig BenchmarkConfig::fromJson(const string &jsonFilepath)
{
    BenchmarkConfig config;
    ifstream f(jsonFilepath);
    if (!f)
        fail(string("Couldn't open file: ") + string(jsonFilepath));

    auto json = json::parse(f, /*cb*/ nullptr, /*exceptions*/ false);
    if (json.is_discarded())
        fail(string("Couldn't parse JSON: ") + string(jsonFilepath));

    int version = json["version"];
    if (version > 1)
        skip("config version not supported");

    config.maxRepeatCount = json.value("maxRepeatCount", 10);
    config.maxRepeatSecs = json.value("maxRepeatSecs", 600);

    config.checksum = AWS_SCA_NONE;
    if (json.contains("checksum") && !json["checksum"].is_null())
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

    config.filesOnDisk = json.value("filesOnDisk", true);

    for (auto &&taskJson : json["tasks"])
    {
        auto &task = config.tasks.emplace_back();

        task.action = taskJson["action"];
        task.key = taskJson["key"];

        // size looks like "5GiB" or "10KiB" or "1" (bytes)
        string sizeStr = taskJson["size"];
        regex sizeRegex("^([0-9]+)(GiB|MiB|KiB|)$");
        cmatch sizeMatch;
        if (!regex_match(sizeStr.c_str(), sizeMatch, sizeRegex))
            fail(string("invalid size: ") + string(sizeStr));

        task.size = stoull(sizeMatch[1]);
        string sizeUnit = sizeMatch[2];
        if (sizeUnit == "KiB")
            task.size = bytesFromKiB(task.size);
        else if (sizeUnit == "MiB")
            task.size = bytesFromMiB(task.size);
        else if (sizeUnit == "GiB")
            task.size = bytesFromGiB(task.size);
    }

    return config;
}

uint64_t BenchmarkConfig::bytesPerRun() const
{
    uint64_t bytes = 0;
    for (auto &&task : tasks)
        bytes += task.size;
    return bytes;
}

// Instantiates S3 Client, does not run the benchmark yet
Benchmark::Benchmark(const BenchmarkConfig &config, string_view bucket, string_view region, double targetThroughputGbps)
{
    this->config = config;
    this->bucket = bucket;
    this->region = region;

    alloc = aws_default_allocator();

    aws_s3_library_init(alloc);

    struct aws_logger_standard_options logOpts;
    AWS_ZERO_STRUCT(logOpts);
    logOpts.level = AWS_LL_ERROR;
    logOpts.file = stderr;
    AWS_FATAL_ASSERT(aws_logger_init_standard(&logger, alloc, &logOpts) == 0);
    aws_logger_set(&logger);

    eventLoopGroup = aws_event_loop_group_new_default_pinned_to_cpu_group(
        alloc, 0 /*max-threads*/, 0 /*cpu-group*/, NULL /*shutdown-options*/);
    AWS_FATAL_ASSERT(eventLoopGroup != NULL);

    aws_host_resolver_default_options resolverOpts;
    AWS_ZERO_STRUCT(resolverOpts);
    resolverOpts.max_entries = 8;
    resolverOpts.el_group = eventLoopGroup;
    hostResolver = aws_host_resolver_new_default(alloc, &resolverOpts);
    AWS_FATAL_ASSERT(hostResolver != NULL);

    aws_client_bootstrap_options bootstrapOpts;
    AWS_ZERO_STRUCT(bootstrapOpts);
    bootstrapOpts.event_loop_group = eventLoopGroup;
    bootstrapOpts.host_resolver = hostResolver;
    clientBootstrap = aws_client_bootstrap_new(alloc, &bootstrapOpts);
    AWS_FATAL_ASSERT(clientBootstrap != NULL);

    aws_tls_ctx_options tlsCtxOpts;
    aws_tls_ctx_options_init_default_client(&tlsCtxOpts, alloc);
    tlsCtx = aws_tls_client_ctx_new(alloc, &tlsCtxOpts);
    AWS_FATAL_ASSERT(tlsCtx != NULL);

    aws_tls_connection_options tlsConnOpts;
    aws_tls_connection_options_init_from_ctx(&tlsConnOpts, tlsCtx);

    aws_credentials_provider_chain_default_options providerOpts;
    AWS_ZERO_STRUCT(providerOpts);
    providerOpts.bootstrap = clientBootstrap;
    providerOpts.tls_ctx = tlsCtx;
    credentialsProvider = aws_credentials_provider_new_chain_default(alloc, &providerOpts);
    AWS_FATAL_ASSERT(credentialsProvider != NULL);

    aws_signing_config_aws signingConfig;
    aws_s3_init_default_signing_config(&signingConfig, toCursor(region), credentialsProvider);

    aws_s3_client_config s3ClientConfig;
    AWS_ZERO_STRUCT(s3ClientConfig);
    s3ClientConfig.region = toCursor(region);
    s3ClientConfig.client_bootstrap = clientBootstrap;
    s3ClientConfig.tls_connection_options = &tlsConnOpts;
    s3ClientConfig.signing_config = &signingConfig;
    s3ClientConfig.part_size = bytesFromMiB(8);
    s3ClientConfig.throughput_target_gbps = targetThroughputGbps;

    // If writing data to disk, enable backpressure.
    // This prevents us from running out of memory due to downloading
    // data faster than we can write it to disk.
    if (config.filesOnDisk)
    {
        s3ClientConfig.enable_read_backpressure = true;
        s3ClientConfig.initial_read_window = bytesFromMiB(BACKPRESSURE_INITIAL_READ_WINDOW_MiB);
    }

    // struct aws_http_connection_monitoring_options httpMonitoringOpts;
    // AWS_ZERO_STRUCT(httpMonitoringOpts);
    // httpMonitoringOpts.minimum_throughput_bytes_per_second = 1;
    // httpMonitoringOpts.allowable_throughput_failure_interval_milliseconds = 750;
    // s3ClientConfig.monitoring_options = &httpMonitoringOpts;

    s3Client = aws_s3_client_new(alloc, &s3ClientConfig);
    AWS_FATAL_ASSERT(s3Client != NULL);

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

Benchmark::~Benchmark()
{
    s3Client = aws_s3_client_release(s3Client);
    credentialsProvider = aws_credentials_provider_release(credentialsProvider);
    aws_tls_ctx_release(tlsCtx);
    aws_tls_ctx_release(tlsCtx);
    tlsCtx = NULL;
    aws_client_bootstrap_release(clientBootstrap);
    clientBootstrap = NULL;
    aws_host_resolver_release(hostResolver);
    hostResolver = NULL;
    aws_event_loop_group_release(eventLoopGroup);
    eventLoopGroup = NULL;
    aws_s3_library_clean_up();
}

void Benchmark::run()
{
    // kick off all tasks
    list<Task> runningTasks;
    for (size_t i = 0; i < config.tasks.size(); ++i)
        runningTasks.emplace_back(*this, i);

    // wait until all tasks are done
    for (auto &&task : runningTasks)
        task.waitUntilDone();
}

void addHeader(aws_http_message *request, string_view name, string_view value)
{
    aws_http_header header = {toCursor(name), toCursor(value)};
    aws_http_message_add_header(request, header);
}

Task::Task(Benchmark &benchmark, size_t taskI)
    : benchmark(benchmark),
      taskI(taskI),
      config(benchmark.config.tasks[taskI]),
      donePromise(),
      doneFuture(donePromise.get_future())
{

    aws_s3_meta_request_options options;
    AWS_ZERO_STRUCT(options);
    options.user_data = this;
    options.finish_callback = Task::onFinished;

    auto request = aws_http_message_new_request(benchmark.alloc);
    options.message = request;
    addHeader(request, "Host", benchmark.bucket + ".s3." + benchmark.region + ".amazonaws.com");
    aws_http_message_set_request_path(request, toCursor(string("/") + config.key));

    aws_input_stream *inMemoryStreamForUpload = NULL;

    if (config.action == "upload")
    {
        options.type = AWS_S3_META_REQUEST_TYPE_PUT_OBJECT;

        aws_http_message_set_request_method(request, toCursor("PUT"));
        addHeader(request, "Content-Length", to_string(config.size));
        addHeader(request, "Content-Type", "application/octet-stream");

        if (benchmark.config.filesOnDisk)
            options.send_filepath = toCursor(config.key);
        else
        {
            // set up input-stream that uploads random data from a buffer
            auto randomDataCursor = aws_byte_cursor_from_array(
                benchmark.randomDataForUpload.data(), benchmark.randomDataForUpload.size());
            auto inMemoryStreamForUpload = aws_input_stream_new_from_cursor(benchmark.alloc, &randomDataCursor);
            aws_http_message_set_body_stream(request, inMemoryStreamForUpload);
            aws_input_stream_release(inMemoryStreamForUpload);
        }
    }
    else if (config.action == "download")
    {
        options.type = AWS_S3_META_REQUEST_TYPE_GET_OBJECT;

        aws_http_message_set_request_method(request, toCursor("GET"));
        addHeader(request, "Content-Length", "0");

        if (benchmark.config.filesOnDisk)
        {
            downloadFile = fopen(config.key.c_str(), "wb");
            AWS_FATAL_ASSERT(downloadFile != NULL);

            options.body_callback = Task::onDownloadData;
        }
    }
    else
        fail(string("Unknown task action: ") + config.action);

    aws_s3_checksum_config checksumConfig;
    AWS_ZERO_STRUCT(checksumConfig);
    if (benchmark.config.checksum != AWS_SCA_NONE)
    {
        checksumConfig.checksum_algorithm = benchmark.config.checksum;
        checksumConfig.location = AWS_SCL_HEADER;
        checksumConfig.validate_response_checksum = true;
        options.checksum_config = &checksumConfig;
    }

    metaRequest = aws_s3_client_make_meta_request(benchmark.s3Client, &options);
    AWS_FATAL_ASSERT(metaRequest != NULL);

    aws_http_message_release(request);
}

void Task::onFinished(struct aws_s3_meta_request *meta_request,
                      const struct aws_s3_meta_request_result *meta_request_result,
                      void *user_data)
{
    Task *task = static_cast<Task *>(user_data);
    // TODO: report failed meta-requests instead of killing benchmark?
    if (meta_request_result->error_code != 0)
    {
        printf("Task[%zu] failed. action:%s key:%s error_code:%s\n",
               task->taskI, task->config.action.c_str(), task->config.key.c_str(),
               aws_error_name(meta_request_result->error_code));
        if (meta_request_result->response_status != 0)
            printf("Status-Code: %d\n", meta_request_result->response_status);

        aws_http_headers *headers = meta_request_result->error_response_headers;
        if (headers != NULL)
        {
            for (size_t i = 0; i < aws_http_headers_count(headers); ++i)
            {
                aws_http_header headerI;
                aws_http_headers_get_index(headers, i, &headerI);
                printf(PRInSTR ": " PRInSTR "\n", AWS_BYTE_CURSOR_PRI(headerI.name), AWS_BYTE_CURSOR_PRI(headerI.value));
            }
        }

        aws_byte_buf *body = meta_request_result->error_response_body;
        if (body != NULL && body->len > 0)
            printf(PRInSTR "\n", AWS_BYTE_BUF_PRI(*body));

        fail("S3MetaRequest failed");
    }

    // clean up task
    if (task->downloadFile != NULL)
        fclose(task->downloadFile);
    aws_s3_meta_request_release(task->metaRequest);
    task->donePromise.set_value();
}

int Task::onDownloadData(struct aws_s3_meta_request *meta_request,
                         const struct aws_byte_cursor *body,
                         uint64_t range_start,
                         void *user_data)
{
    auto *task = static_cast<Task *>(user_data);

    size_t written = fwrite(body->ptr, 1, body->len, task->downloadFile);
    AWS_FATAL_ASSERT(written == body->len);

    // Increment read window so data will continue downloading
    aws_s3_meta_request_increment_read_window(meta_request, body->len);

    return AWS_OP_SUCCESS;
}

int main(int argc, char *argv[])
{
    if (argc != 5)
        fail("usage: s3benchrunner-c <config.json> <bucket> <region> <target-throughput-Gbps>");

    auto config = BenchmarkConfig::fromJson(argv[1]);
    string bucket = argv[2];
    string region = argv[3];
    double targetThroughputGbps = stod(argv[4]);

    auto benchmark = Benchmark(config, bucket, region, targetThroughputGbps);
    uint64_t bytesPerRun = config.bytesPerRun();

    // Repeat benchmark until we exceed maxRepeatCount or maxRepeatSecs
    auto appStart = high_resolution_clock::now();
    for (int runI = 0; runI < config.maxRepeatCount; ++runI)
    {
        auto runStart = high_resolution_clock::now();

        benchmark.run();

        duration<double> runDurationSecs = high_resolution_clock::now() - runStart;
        double runSecs = runDurationSecs.count();
        printf("Run:%d Secs:%.3f Gb/s:%.1f Mb/s:%.1f GiB/s:%.1f MiB/s:%.1f\n",
               runI + 1,
               runSecs,
               bytesToGigabit(bytesPerRun) / runSecs,
               bytesToMegabit(bytesPerRun) / runSecs,
               bytesToGiB(bytesPerRun) / runSecs,
               bytesToMiB(bytesPerRun) / runSecs);

        // break out if we've exceeded maxRepeatSecs
        duration<double> appDurationSecs = high_resolution_clock::now() - appStart;
        if (appDurationSecs >= 1s * config.maxRepeatSecs)
            break;
    }

    return 0;
}

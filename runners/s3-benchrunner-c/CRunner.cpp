#include "BenchmarkRunner.h"

#include <future>
#include <list>

#include <aws/auth/credentials.h>
#include <aws/common/string.h>
#include <aws/common/system_resource_util.h>
#include <aws/http/connection.h>
#include <aws/http/request_response.h>
#include <aws/io/channel_bootstrap.h>
#include <aws/io/event_loop.h>
#include <aws/io/host_resolver.h>
#include <aws/io/stream.h>
#include <aws/io/tls_channel_handler.h>
#include <aws/s3/s3_client.h>

using namespace std;

// Read-backpressure (feature added Sept 2022) can prevent running out of
// memory due to downloading data faster than we can write it to disk.
// 256MiB is Java Transfer Mgr V2's default initial window (as of Aug 2024).
// Unfortunately, this hurts the performance of single-file workloads
// due to limiting the number of parts in-flight for a given file.
// But the effect goes away if there are lots of files in a workload,
// because the total number of parts in-flight gets high enough.
//
// The memory-limiter (feature added 1 yr later in Nov 2023) is another way
// to prevent running out of memory.
//
// This benchmark can turn off backpressure and rely solely on the memory-limiter,
// since it always processes data synchronously within the body callback.
// #define BACKPRESSURE_INITIAL_READ_WINDOW_MiB 256 /* If commented out, backpressure is disabled */

aws_byte_cursor toCursor(string_view src)
{
    return aws_byte_cursor{.len = src.length(), .ptr = (uint8_t *)src.data()};
}

// Benchmark runner using aws-c-s3 directly
class CRunner : public BenchmarkRunner
{
  public:
    // CRT boilerplate
    aws_allocator *alloc = NULL;
    aws_logger logger;
    aws_event_loop_group *eventLoopGroup = NULL;
    aws_host_resolver *hostResolver = NULL;
    aws_client_bootstrap *clientBootstrap = NULL;
    aws_tls_ctx *tlsCtx = NULL;
    aws_credentials_provider *credentialsProvider = NULL;
    aws_s3_client *s3Client = NULL;

    // derived from bucket and region (e.g. mybucket.s3.us-west-2.amazonaws.com)
    string endpoint;

  public:
    // Instantiates S3 Client, does not run the benchmark yet
    CRunner(const BenchmarkConfig &config);

    ~CRunner() override;

    // A benchmark can be run repeatedly
    void run() override;

    friend class Task;
};

// A runnable task
class Task
{
    CRunner &runner;
    size_t taskI;
    TaskConfig &config;
    aws_s3_meta_request *metaRequest;
    promise<void> donePromise;
    future<void> doneFuture;

    static void onFinished(
        struct aws_s3_meta_request *meta_request,
        const struct aws_s3_meta_request_result *meta_request_result,
        void *user_data);

  public:
    // Creates the task and begins its work
    Task(CRunner &runner, size_t taskI);

    void waitUntilDone() { return doneFuture.wait(); }
};

std::unique_ptr<BenchmarkRunner> createCRunner(const BenchmarkConfig &config)
{
    return make_unique<CRunner>(config);
}

// Instantiates S3 Client, does not run the benchmark yet
CRunner::CRunner(const BenchmarkConfig &config) : BenchmarkRunner(config)
{
    bool isS3Express = config.bucket.ends_with("--x-s3");
    if (isS3Express)
    {
        // extract the "usw2-az3" from "mybucket--usw2-az3--x-s3"
        string substrNoSuffix = config.bucket.substr(0, config.bucket.rfind("--"));
        string azID = substrNoSuffix.substr(substrNoSuffix.rfind("--") + 2);

        // Endpoint looks like: mybucket--usw2-az3--x-s3.s3express-usw2-az3.us-west-2.amazonaws.com
        this->endpoint = config.bucket;
        this->endpoint += ".s3express-";
        this->endpoint += azID;
        this->endpoint += ".";
        this->endpoint += config.region;
        this->endpoint += ".amazonaws.com";
    }
    else
    {
        // Standard S3 endpoint looks like: mybucket.s3.us-west-2.amazonaws.com
        this->endpoint = config.bucket;
        this->endpoint += ".s3.";
        this->endpoint += config.region;
        this->endpoint += ".amazonaws.com";
    }

    alloc = aws_default_allocator();

    aws_s3_library_init(alloc);

    struct aws_logger_standard_options logOpts;
    AWS_ZERO_STRUCT(logOpts);
    logOpts.level = AWS_LL_ERROR;
    logOpts.file = stderr;
    AWS_FATAL_ASSERT(aws_logger_init_standard(&logger, alloc, &logOpts) == 0);
    aws_logger_set(&logger);

    eventLoopGroup = aws_event_loop_group_new_default(alloc, 0 /*max-threads*/, NULL /*shutdown-options*/);
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
    aws_s3_init_default_signing_config(&signingConfig, toCursor(config.region), credentialsProvider);

    aws_s3_client_config s3ClientConfig;
    AWS_ZERO_STRUCT(s3ClientConfig);
    s3ClientConfig.region = toCursor(config.region);
    s3ClientConfig.client_bootstrap = clientBootstrap;
    s3ClientConfig.tls_connection_options = &tlsConnOpts;
    s3ClientConfig.signing_config = &signingConfig;
    s3ClientConfig.part_size = PART_SIZE;
    s3ClientConfig.throughput_target_gbps = config.targetThroughputGbps;

    if (isS3Express)
    {
        signingConfig.algorithm = AWS_SIGNING_ALGORITHM_V4_S3EXPRESS;
        s3ClientConfig.enable_s3express = true;
    }

#if defined(BACKPRESSURE_INITIAL_READ_WINDOW_MiB)
    // If writing data to disk, enable backpressure.
    // This prevents us from running out of memory due to downloading
    // data faster than we can write it to disk.
    if (config.filesOnDisk)
    {
        s3ClientConfig.enable_read_backpressure = true;
        s3ClientConfig.initial_read_window = bytesFromMiB(BACKPRESSURE_INITIAL_READ_WINDOW_MiB);
    }
#endif

    // struct aws_http_connection_monitoring_options httpMonitoringOpts;
    // AWS_ZERO_STRUCT(httpMonitoringOpts);
    // httpMonitoringOpts.minimum_throughput_bytes_per_second = 1;
    // httpMonitoringOpts.allowable_throughput_failure_interval_milliseconds = 750;
    // s3ClientConfig.monitoring_options = &httpMonitoringOpts;

    s3Client = aws_s3_client_new(alloc, &s3ClientConfig);
    AWS_FATAL_ASSERT(s3Client != NULL);
}

CRunner::~CRunner()
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

void CRunner::run()
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

Task::Task(CRunner &runner, size_t taskI)
    : runner(runner), taskI(taskI), config(runner.config.tasks[taskI]), donePromise(),
      doneFuture(donePromise.get_future())
{

    aws_s3_meta_request_options options;
    AWS_ZERO_STRUCT(options);
    options.user_data = this;
    options.finish_callback = Task::onFinished;

    // TODO: add "sizeHint" to config, if true then set options.object_size_hint.
    // A transfer-manager downloading a directory would know the object size ahead of time.
    // Size hint could have a big performance impact when downloading lots of
    // small files and validating checksums.

    auto request = aws_http_message_new_request(runner.alloc);
    options.message = request;
    addHeader(request, "Host", runner.endpoint);
    aws_http_message_set_request_path(request, toCursor(string("/") + config.key));

    aws_input_stream *inMemoryStreamForUpload = NULL;

    if (config.action == "upload")
    {
        options.type = AWS_S3_META_REQUEST_TYPE_PUT_OBJECT;

        aws_http_message_set_request_method(request, toCursor("PUT"));
        addHeader(request, "Content-Length", to_string(config.size));
        addHeader(request, "Content-Type", "application/octet-stream");

        if (runner.config.filesOnDisk)
            options.send_filepath = toCursor(config.key);
        else
        {
            // set up input-stream that uploads random data from a buffer
            auto randomDataCursor =
                aws_byte_cursor_from_array(runner.randomDataForUpload.data(), runner.randomDataForUpload.size());
            auto inMemoryStreamForUpload = aws_input_stream_new_from_cursor(runner.alloc, &randomDataCursor);
            aws_http_message_set_body_stream(request, inMemoryStreamForUpload);
            aws_input_stream_release(inMemoryStreamForUpload);
        }
    }
    else if (config.action == "download")
    {
        options.type = AWS_S3_META_REQUEST_TYPE_GET_OBJECT;

        aws_http_message_set_request_method(request, toCursor("GET"));
        addHeader(request, "Content-Length", "0");

        if (runner.config.filesOnDisk)
        {
            options.recv_filepath = toCursor(config.key);
        }
    }
    else
        fail(string("Unknown task action: ") + config.action);

    aws_s3_checksum_config checksumConfig;
    AWS_ZERO_STRUCT(checksumConfig);
    if (!runner.config.checksum.empty())
    {
        if (runner.config.checksum == "CRC32")
            checksumConfig.checksum_algorithm = AWS_SCA_CRC32;
        else if (runner.config.checksum == "CRC32C")
            checksumConfig.checksum_algorithm = AWS_SCA_CRC32C;
        else if (runner.config.checksum == "SHA1")
            checksumConfig.checksum_algorithm = AWS_SCA_SHA1;
        else if (runner.config.checksum == "SHA256")
            checksumConfig.checksum_algorithm = AWS_SCA_SHA256;
        else
            fail(string("Unknown checksum: ") + runner.config.checksum);
        checksumConfig.location = AWS_SCL_HEADER;
        checksumConfig.validate_response_checksum = true;
        options.checksum_config = &checksumConfig;
    }

    metaRequest = aws_s3_client_make_meta_request(runner.s3Client, &options);
    AWS_FATAL_ASSERT(metaRequest != NULL);

    aws_http_message_release(request);
}

void Task::onFinished(
    struct aws_s3_meta_request *meta_request,
    const struct aws_s3_meta_request_result *meta_request_result,
    void *user_data)
{
    Task *task = static_cast<Task *>(user_data);
    // TODO: report failed meta-requests instead of killing benchmark?
    if (meta_request_result->error_code != 0)
    {
        printf(
            "Task[%zu] failed. action:%s key:%s error_code:%s\n",
            task->taskI,
            task->config.action.c_str(),
            task->config.key.c_str(),
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
                printf(
                    PRInSTR ": " PRInSTR "\n", AWS_BYTE_CURSOR_PRI(headerI.name), AWS_BYTE_CURSOR_PRI(headerI.value));
            }
        }

        aws_byte_buf *body = meta_request_result->error_response_body;
        if (body != NULL && body->len > 0)
            printf(PRInSTR "\n", AWS_BYTE_BUF_PRI(*body));

        fail("S3MetaRequest failed");
    }

    // clean up task
    aws_s3_meta_request_release(task->metaRequest);
    task->donePromise.set_value();
}

int main(int argc, char *argv[])
{
    return benchmarkRunnerMain(
        argc,
        argv,
        [](string_view id, const BenchmarkConfig &config)
        {
            if (id == "crt-c")
                return createCRunner(config);
            fail("Unsupported S3_CLIENT. Options are: crt-c");
        });
}

#include <aws/core/Aws.h>
#include <aws/core/http/HttpResponse.h>
#include <aws/s3-crt/S3CrtClient.h>
#include <aws/s3-crt/model/GetObjectRequest.h>
#include <aws/s3/S3Client.h>
#include <aws/s3/model/GetObjectRequest.h>

#include "utils.h"

class SdkCrtBenchmarkRunner : public BenchmarkRunner
{

    std::shared_ptr<Aws::S3Crt::S3CrtClient> client;
    Aws::SDKOptions options;

  public:
    friend class SdkCrtTask;
    SdkCrtBenchmarkRunner(
        const BenchmarkConfig &config,
        string_view bucket,
        string_view region,
        double targetThroughputGbps)
        : BenchmarkRunner(config, bucket, region)
    {

        Aws::InitAPI(options);
        Aws::S3Crt::ClientConfiguration client_config;
        client_config.region = region;
        client_config.throughputTargetGbps = targetThroughputGbps;

        this->client = Aws::MakeShared<Aws::S3Crt::S3CrtClient>("CrtClient", client_config);
    }

    ~SdkCrtBenchmarkRunner()
    {
        this->client.reset();
        Aws::ShutdownAPI(options);
    }

    void run();
};

// A runnable task
class SdkCrtTask : public Task
{
    SdkCrtBenchmarkRunner &runner;

  public:
    // Creates the task and begins its work
    SdkCrtTask(SdkCrtBenchmarkRunner &runner, size_t taskI) : runner(runner), Task(runner, taskI)
    {
        if (config.action == "upload")
        {
            fail(string("Unknown task action: ") + config.action);
        }
        else if (config.action == "download")
        {
            Aws::S3Crt::Model::GetObjectRequest request;
            request.SetBucket(runner.bucket);
            request.SetKey(config.key);
            auto getObjectCallback = [&](const Aws::S3Crt::S3CrtClient *client,
                                         const Aws::S3Crt::Model::GetObjectRequest &request,
                                         Aws::S3Crt::Model::GetObjectOutcome out_come,
                                         const std::shared_ptr<const Aws::Client::AsyncCallerContext> &context)
            {
                if (!out_come.IsSuccess())
                {
                    printf(
                        "Task[%zu] failed. action:%s key:%s error_message:%s\n",
                        this->taskI,
                        this->config.action.c_str(),
                        this->config.key.c_str(),
                        out_come.GetError().GetMessage());
                    if (out_come.GetError().GetResponseCode() != Aws::Http::HttpResponseCode::REQUEST_NOT_MADE)
                        printf("Status-Code: %d\n", out_come.GetError().GetResponseCode());
                    fail("GetObject failed");
                }
                else
                {
                    if (this->runner.config.filesOnDisk)
                    {
                        fail(string("Not implemented download to disk: ") + config.action);
                    }
                    this->donePromise.set_value();
                }
            };
            runner.client->GetObjectAsync(request, getObjectCallback, nullptr);
        }
        else
            fail(string("Unknown task action: ") + config.action);
    }
};

void SdkCrtBenchmarkRunner::run()
{
    // kick off all tasks
    list<SdkCrtTask> runningTasks;
    for (size_t i = 0; i < config.tasks.size(); ++i)
        runningTasks.emplace_back(*this, i);

    // wait until all tasks are done
    for (auto &&task : runningTasks)
        task.waitUntilDone();
}

class SdkBenchmarkRunner : public BenchmarkRunner
{

    std::shared_ptr<Aws::S3::S3Client> client;
    Aws::SDKOptions options;

  public:
    friend class SdkTask;
    SdkBenchmarkRunner(
        const BenchmarkConfig &config,
        string_view bucket,
        string_view region,
        double targetThroughputGbps)
        : BenchmarkRunner(config, bucket, region)
    {

        Aws::InitAPI(options);
        Aws::Client::ClientConfiguration client_config;
        client_config.region = region;

        this->client = Aws::MakeShared<Aws::S3::S3Client>("S3Client", client_config);
    }

    ~SdkBenchmarkRunner()
    {
        this->client.reset();
        Aws::ShutdownAPI(options);
    }

    void run();
};

// A runnable task
class SdkTask : public Task
{
    SdkBenchmarkRunner &runner;

  public:
    // Creates the task and begins its work
    SdkTask(SdkBenchmarkRunner &runner, size_t taskI) : runner(runner), Task(runner, taskI)
    {
        if (config.action == "upload")
        {
            fail(string("Unknown task action: ") + config.action);
        }
        else if (config.action == "download")
        {
            Aws::S3::Model::GetObjectRequest request;
            request.SetBucket(runner.bucket);
            request.SetKey(config.key);
            auto getObjectCallback = [&](const Aws::S3::S3Client *client,
                                         const Aws::S3::Model::GetObjectRequest &request,
                                         Aws::S3::Model::GetObjectOutcome out_come,
                                         const std::shared_ptr<const Aws::Client::AsyncCallerContext> &context)
            {
                if (!out_come.IsSuccess())
                {
                    printf(
                        "Task[%zu] failed. action:%s key:%s error_message:%s\n",
                        this->taskI,
                        this->config.action.c_str(),
                        this->config.key.c_str(),
                        out_come.GetError().GetMessage());
                    if (out_come.GetError().GetResponseCode() != Aws::Http::HttpResponseCode::REQUEST_NOT_MADE)
                        printf("Status-Code: %d\n", out_come.GetError().GetResponseCode());
                    fail("GetObject failed");
                }
                else
                {
                    if (this->runner.config.filesOnDisk)
                    {
                        fail(string("Not implemented download to disk: ") + config.action);
                    }
                    this->donePromise.set_value();
                }
            };
            runner.client->GetObjectAsync(request, getObjectCallback, nullptr);
        }
        else
            fail(string("Unknown task action: ") + config.action);
    }
};

void SdkBenchmarkRunner::run()
{
    // kick off all tasks
    list<SdkTask> runningTasks;
    for (size_t i = 0; i < config.tasks.size(); ++i)
        runningTasks.emplace_back(*this, i);

    // wait until all tasks are done
    for (auto &&task : runningTasks)
        task.waitUntilDone();
}

int main(int argc, char *argv[])
{
    if (argc != 6)
        fail("usage: s3-benchrunner-c S3_CLIENT WORKLOAD BUCKET REGION TARGET_THROUGHPUT");

    string s3ClientId = argv[1];

    if (s3ClientId != "sdk-cpp-crt" && s3ClientId != "sdk-cpp")
    {
        fail("Unsupported S3_CLIENT. Options are: sdk-cpp-crt, sdk-cpp");
    }

    auto config = BenchmarkConfig::fromJson(argv[2]);
    string bucket = argv[3];
    string region = argv[4];
    double targetThroughputGbps = stod(argv[5]);

    if (s3ClientId == "sdk-cpp-crt")
    {
        auto runner = SdkCrtBenchmarkRunner(config, bucket, region, targetThroughputGbps);
        main_run(runner, config);
    }
    else
    {
        auto runner = SdkBenchmarkRunner(config, bucket, region, targetThroughputGbps);
        main_run(runner, config);
    }
    return 0;
}

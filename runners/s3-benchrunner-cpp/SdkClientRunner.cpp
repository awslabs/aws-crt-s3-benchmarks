#include "SdkClientRunner.h"

#include <fstream>
#include <semaphore>
#include <thread>

#include <aws/core/Aws.h>
#include <aws/s3-crt/S3CrtClient.h>
#include <aws/s3-crt/model/GetObjectRequest.h>
#include <aws/s3-crt/model/PutObjectRequest.h>
#include <aws/s3/S3Client.h>
#include <aws/s3/model/GetObjectRequest.h>
#include <aws/s3/model/PutObjectRequest.h>

using namespace std;

const char *ALLOCATION_TAG = "BenchmarkRunner";

// lol, this does nothing with the data
// we'll override the bare minimum of basic_streambuf functions, to make it seem like it's "working"
class DownloadToRamNullBuf : public basic_streambuf<Aws::IOStream::char_type, Aws::IOStream::traits_type>
{
  protected:
    int_type overflow(int_type c) override { return traits_type::not_eof(c); }
};

// Benchmark runner for aws-sdk-cpp's S3 clients.
// Using templates with scary number of arguments because
// Aws::S3Crt::S3CrtClient and Aws::S3::S3Client are distinct classes,
// but their APIs are nearly identical. Using templates lets us avoid
// copy/pasting a ton of code.
template <
    class S3ClientT,
    class S3ErrorT,
    class GetObjectRequestT,
    class GetObjectResultT,
    class PutObjectRequestT,
    class PutObjectResultT>
class SdkClientRunner : public BenchmarkRunner
{
    using GetObjectOutcomeT = Aws::Utils::Outcome<GetObjectResultT, S3ErrorT>;
    using PutObjectOutcomeT = Aws::Utils::Outcome<PutObjectResultT, S3ErrorT>;
    using SdkClientRunnerT =
        SdkClientRunner<S3ClientT, S3ErrorT, GetObjectRequestT, GetObjectResultT, PutObjectRequestT, PutObjectResultT>;

    Aws::SDKOptions sdkOptions;
    unique_ptr<S3ClientT> client;

  public:
    SdkClientRunner(const BenchmarkConfig &config) : BenchmarkRunner(config)
    {
        Aws::InitAPI(sdkOptions);
        createS3Client();
    }

    ~SdkClientRunner() override { Aws::ShutdownAPI(sdkOptions); }

    void run() override
    {
        auto concurrencySemaphore = counting_semaphore(maxConcurrency());

        // kick off all tasks
        list<Task> runningTasks;
        for (size_t i = 0; i < config.tasks.size(); ++i)
            runningTasks.emplace_back(*this, concurrencySemaphore, i);

        // wait until all tasks are done
        for (auto &&task : runningTasks)
            task.waitUntilDone();
    };

  private:
    // Specialize these functions for each S3ClientT...
    void createS3Client();
    ptrdiff_t maxConcurrency();

    class Task
    {
        SdkClientRunnerT &runner;
        counting_semaphore<> &concurrencySemaphore;
        size_t taskI;
        TaskConfig &taskConfig;
        promise<void> donePromise;
        future<void> doneFuture;
        DownloadToRamNullBuf downloadToRamNullBuf;

      public:
        // The Task's constructor begins its work (once it acquires from the semaphore).
        Task(SdkClientRunnerT &runner, counting_semaphore<> &concurrencySemaphore, size_t taskI)
            : runner(runner), concurrencySemaphore(concurrencySemaphore), taskI(taskI),
              taskConfig(runner.config.tasks[taskI]), donePromise(), doneFuture(donePromise.get_future())
        {
            concurrencySemaphore.acquire();

            if (taskConfig.action == "upload")
            {
                fail("TODO: upload");
            }
            else if (taskConfig.action == "download")
            {
                GetObjectRequestT request;
                request.SetBucket(runner.config.bucket);
                request.SetKey(taskConfig.key);

                if (!runner.config.checksum.empty())
                {
                    skip("TODO: request.SetChecksumMode(ENABLED)");
                }

                if (runner.config.filesOnDisk)
                {
                    request.SetResponseStreamFactory(
                        [this]() { return new Aws::FStream(this->taskConfig.key, std::ios_base::out); });
                }
                else
                {
                    request.SetResponseStreamFactory([this] { return new Aws::IOStream(&this->downloadToRamNullBuf); });
                }

                auto onGetObjectFinished = [this](
                                               const S3ClientT *,
                                               const GetObjectRequestT &,
                                               GetObjectOutcomeT outcome,
                                               const std::shared_ptr<const Aws::Client::AsyncCallerContext> &) {
                    this->onFinished(outcome);
                };
                runner.client->GetObjectAsync(request, onGetObjectFinished, nullptr);
            }
            else
                fail(string("Unknown task action: ") + taskConfig.action);
        }

        void waitUntilDone() { return doneFuture.wait(); }

      private:
        template <class OutcomeT> void onFinished(OutcomeT &outcome)
        {
            if (!outcome.IsSuccess())
            {
                std::cout << "Task[" << this->taskI << "] failed. action:" << this->taskConfig.action
                          << " key:" << this->taskConfig.key << endl
                          << outcome.GetError() << endl;
                fail("GetObject failed");
            }

            this->concurrencySemaphore.release();
            this->donePromise.set_value();
        }
    };
};

////////////////////////////////////////////////////////////////////////////////
// Classic
////////////////////////////////////////////////////////////////////////////////

using SdkClassicClientRunner = SdkClientRunner<
    Aws::S3::S3Client,
    Aws::S3::S3Error,
    Aws::S3::Model::GetObjectRequest,
    Aws::S3::Model::GetObjectResult,
    Aws::S3::Model::PutObjectRequest,
    Aws::S3::Model::PutObjectResult>;

template <> void SdkClassicClientRunner::createS3Client()
{
    Aws::Client::ClientConfiguration clientConfig;
    clientConfig.region = this->config.region;

    this->client = make_unique<Aws::S3::S3Client>(clientConfig);
}

template <> ptrdiff_t SdkClassicClientRunner::maxConcurrency()
{
    // SDK creates 1 thread per async call, so limit concurrency.
    // Using more than just hardware_concurrency, since the threads are I/O bound, not CPU bound.
    return thread::hardware_concurrency() * 5;
}

unique_ptr<BenchmarkRunner> createSdkClassicClientRunner(const BenchmarkConfig &config)
{
    return make_unique<SdkClassicClientRunner>(config);
}

////////////////////////////////////////////////////////////////////////////////
// CRT
////////////////////////////////////////////////////////////////////////////////

using SdkCrtClientRunner = SdkClientRunner<
    Aws::S3Crt::S3CrtClient,
    Aws::S3Crt::S3CrtError,
    Aws::S3Crt::Model::GetObjectRequest,
    Aws::S3Crt::Model::GetObjectResult,
    Aws::S3Crt::Model::PutObjectRequest,
    Aws::S3Crt::Model::PutObjectResult>;

template <> void SdkCrtClientRunner::createS3Client()
{
    Aws::S3Crt::ClientConfiguration clientConfig;
    clientConfig.partSize = PART_SIZE;
    clientConfig.throughputTargetGbps = this->config.targetThroughputGbps;
    clientConfig.region = this->config.region;

    this->client = make_unique<Aws::S3Crt::S3CrtClient>(clientConfig);
}

template <> ptrdiff_t SdkCrtClientRunner::maxConcurrency()
{
    // CRT has its own thread pool, so it can handle more concurrency than classic SDK
    return 1000;
}

unique_ptr<BenchmarkRunner> createSdkCrtClientRunner(const BenchmarkConfig &config)
{
    return make_unique<SdkCrtClientRunner>(config);
}

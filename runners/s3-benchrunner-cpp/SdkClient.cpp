#include "BenchmarkRunner.h"

#include <fstream>
#include <semaphore>
#include <thread>

#include <aws/s3-crt/S3CrtClient.h>
#include <aws/s3-crt/model/GetObjectRequest.h>
#include <aws/s3-crt/model/PutObjectRequest.h>
#include <aws/s3/S3Client.h>
#include <aws/s3/model/GetObjectRequest.h>
#include <aws/s3/model/PutObjectRequest.h>

using namespace std;

// streambuf used in download-to-ram tests
// it simply discards the downloaded data
class DownloadToRamNullBuf : public streambuf
{
  protected:
    // discard single put characters
    int_type overflow(int_type c) override
    {
        // return any value except EOF
        return traits_type::not_eof(c);
    }

    // discard multiple put characters
    streamsize xsputn(const char *s, streamsize n) override
    {
        // return number of bytes "written"
        return n;
    }
};

// streambuf used in upload-from-ram tests
// it reads from a pre-existing vector of bytes
class UploadFromRamBuf : public streambuf
{
  public:
    UploadFromRamBuf(vector<uint8_t> &src) : streambuf()
    {
        char *begin = reinterpret_cast<char *>(src.data());
        char *end = begin + src.size();
        setg(begin, begin /*next*/, end);
    }

  protected:
    streampos seekoff(streamoff off, ios_base::seekdir way, ios_base::openmode which) override
    {
        // Only handle input mode
        if (which != ios_base::in)
            return pos_type(off_type(-1)); // Seeking not supported for output mode

        if (way == ios_base::beg)
            setg(eback(), eback() + off, egptr());
        else if (way == ios_base::cur)
            setg(eback(), gptr() + off, egptr());
        else if (way == ios_base::end)
            setg(eback(), egptr() + off, egptr());

        return gptr() - eback(); // Return the new position
    }

    streampos seekpos(streampos sp, ios_base::openmode which) override
    {
        return seekoff(sp - pos_type(off_type(0)), ios_base::beg, which);
    }
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
    class PutObjectResultT,
    class ChecksumAlgorithmT,
    class ChecksumModeT>
class SdkClientRunner : public BenchmarkRunner
{
    using GetObjectOutcomeT = Aws::Utils::Outcome<GetObjectResultT, S3ErrorT>;
    using PutObjectOutcomeT = Aws::Utils::Outcome<PutObjectResultT, S3ErrorT>;
    using SdkClientRunnerT = SdkClientRunner<
        S3ClientT,
        S3ErrorT,
        GetObjectRequestT,
        GetObjectResultT,
        PutObjectRequestT,
        PutObjectResultT,
        ChecksumAlgorithmT,
        ChecksumModeT>;

    unique_ptr<S3ClientT> client;

  public:
    SdkClientRunner(const BenchmarkConfig &config) : BenchmarkRunner(config) { createS3Client(); }

    void run(size_t runNumber) override
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
        unique_ptr<DownloadToRamNullBuf> downloadToRamNullBuf;
        unique_ptr<UploadFromRamBuf> uploadFromRamBuf;

      public:
        // The Task's constructor begins its work (once it acquires from the semaphore).
        Task(SdkClientRunnerT &runner, counting_semaphore<> &concurrencySemaphore, size_t taskI)
            : runner(runner), concurrencySemaphore(concurrencySemaphore), taskI(taskI),
              taskConfig(runner.config.tasks[taskI]), donePromise(), doneFuture(donePromise.get_future())
        {
            concurrencySemaphore.acquire();

            if (taskConfig.action == "upload")
            {
                PutObjectRequestT request{};
                request.SetBucket(runner.config.bucket);
                request.SetKey(taskConfig.key);

                if (!runner.config.checksum.empty())
                {
                    if (runner.config.checksum == "CRC32")
                        request.SetChecksumAlgorithm(ChecksumAlgorithmT::CRC32);
                    else if (runner.config.checksum == "CRC32C")
                        request.SetChecksumAlgorithm(ChecksumAlgorithmT::CRC32C);
                    else if (runner.config.checksum == "SHA1")
                        request.SetChecksumAlgorithm(ChecksumAlgorithmT::SHA1);
                    else if (runner.config.checksum == "SHA256")
                        request.SetChecksumAlgorithm(ChecksumAlgorithmT::SHA256);
                    else
                        fail(string("Unknown checksum: ") + runner.config.checksum);
                }
                else
                {
                    // NOTE: as of June 2024 this runner is SLOWER when no checksum is set.
                    // The SDK falls back to sending Content-MD5 in a header and there's no way to turn this off,
                    // see: https://github.com/aws/aws-sdk-cpp/issues/2933
                    // This is slow for several reasons:
                    // 1) MD5 is slower than algorithms like CRC32 (and obviously slower than no checksum).
                    // 2) The data must read twice (since we're using headers instead of trailers,
                    //    we can't get away with calculating the checksum and sending the data in a single read).
                    // 3) The SDK currently calculates MD5 synchronously even for PutObjectAsync() calls,
                    //    so the main thread becomes the bottleneck if there are many files in a workload.
                }

                if (runner.config.filesOnDisk)
                {
                    auto streamForUpload = make_shared<Aws::FStream>(taskConfig.key, ios_base::in | ios_base::binary);
                    if (!*streamForUpload)
                        fail(string("Failed to open file: ") + taskConfig.key);

                    request.SetBody(streamForUpload);
                }
                else
                {
                    this->uploadFromRamBuf = make_unique<UploadFromRamBuf>(runner.randomDataForUpload);
                    auto streamForUpload = make_shared<Aws::IOStream>(this->uploadFromRamBuf.get());
                    request.SetBody(streamForUpload);
                }

                auto onPutObjectFinished = [this](
                                               const S3ClientT *,
                                               const PutObjectRequestT &,
                                               PutObjectOutcomeT outcome,
                                               const shared_ptr<const Aws::Client::AsyncCallerContext> &)
                { this->onFinished(outcome); };

                runner.client->PutObjectAsync(request, onPutObjectFinished, nullptr);
            }
            else if (taskConfig.action == "download")
            {
                GetObjectRequestT request;
                request.SetBucket(runner.config.bucket);
                request.SetKey(taskConfig.key);

                if (!runner.config.checksum.empty())
                {
                    request.SetChecksumMode(ChecksumModeT::ENABLED);
                }

                if (runner.config.filesOnDisk)
                {
                    request.SetResponseStreamFactory([this]()
                                                     { return new Aws::FStream(this->taskConfig.key, ios_base::out); });
                }
                else
                {
                    this->downloadToRamNullBuf = make_unique<DownloadToRamNullBuf>();
                    request.SetResponseStreamFactory([this]
                                                     { return new Aws::IOStream(this->downloadToRamNullBuf.get()); });
                }

                auto onGetObjectFinished = [this](
                                               const S3ClientT *,
                                               const GetObjectRequestT &,
                                               GetObjectOutcomeT outcome,
                                               const shared_ptr<const Aws::Client::AsyncCallerContext> &)
                { this->onFinished(outcome); };

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
                cout << "Task[" << this->taskI << "] failed. action:" << this->taskConfig.action
                     << " key:" << this->taskConfig.key << endl
                     << outcome.GetError() << endl;
                fail("Request failed");
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
    Aws::S3::Model::PutObjectResult,
    Aws::S3::Model::ChecksumAlgorithm,
    Aws::S3::Model::ChecksumMode>;

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
    Aws::S3Crt::Model::PutObjectResult,
    Aws::S3Crt::Model::ChecksumAlgorithm,
    Aws::S3Crt::Model::ChecksumMode>;

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

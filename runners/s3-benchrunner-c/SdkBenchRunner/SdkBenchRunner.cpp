#include <aws/core/AmazonWebServiceRequest.h>
#include <aws/core/Aws.h>
#include <aws/core/http/HttpResponse.h>
#include <aws/core/utils/stream/PreallocatedStreamBuf.h>
#include <aws/core/utils/stream/ResponseStream.h>
#include <aws/s3-crt/S3CrtClient.h>
#include <aws/s3-crt/model/GetObjectRequest.h>
#include <aws/s3-crt/model/PutObjectRequest.h>
#include <aws/s3/S3Client.h>
#include <aws/s3/model/GetObjectRequest.h>
#include <aws/s3/model/PutObjectRequest.h>

#include "utils.h"

static const char *FILE_STREAM_FACTORY_TAG = "FILEStreamFactory";

class NullPreallocatedStreamBuf : public Aws::Utils::Stream::PreallocatedStreamBuf
{
  private:
    uint64_t m_buf_len;
    uint64_t m_dumped_put_cnt;
    bool seek_flaged;

  public:
    using Base = Aws::Utils::Stream::PreallocatedStreamBuf;
    explicit NullPreallocatedStreamBuf(unsigned char *buffer, uint64_t lengthToRead)
        : Base(buffer, lengthToRead), m_dumped_put_cnt(0), m_buf_len(lengthToRead), seek_flaged(false)
    {
    }

    ~NullPreallocatedStreamBuf() override = default;

    int overflow(int c) override
    {
        // flush buffer to dump
        (void)Base::seekpos(0, std::ios_base::out);
        m_dumped_put_cnt += m_buf_len;

        // dump the new char
        m_dumped_put_cnt++;

        return std::char_traits<char>::not_eof(c);
    };

    std::streamsize xsputn(const char *s, std::streamsize n) override
    {
        // dump them
        m_dumped_put_cnt += n;
        return n;
    }

    uint64_t get_total_dumped_puts(void) const { return m_dumped_put_cnt; }

    uint64_t get_total_puts(void) const { return m_dumped_put_cnt + (pptr() - pbase()); }
    pos_type seekoff(
        off_type off,
        std::ios_base::seekdir dir,
        std::ios_base::openmode which = std::ios_base::in | std::ios_base::out) override
    {
        seek_flaged = true;
        return Base::seekoff(off, dir, which);
    }
    pos_type seekpos(pos_type pos, std::ios_base::openmode which = std::ios_base::in | std::ios_base::out) override
    {
        seek_flaged = true;
        return Base::seekpos(pos, which);
    }
    bool get_seek_flaged(void) const { return seek_flaged; }

    void reset(void)
    {
        (void)Base::seekpos(0, std::ios_base::in);
        (void)Base::seekpos(0, std::ios_base::out);
        m_dumped_put_cnt = 0;
        seek_flaged = false;
    }
};

class MyIOStream : public Aws::IOStream
{
  public:
    using Base = Aws::IOStream;
    explicit MyIOStream(std::streambuf *buf) : Base(buf) {}

    ~MyIOStream() override = default;
};

class SdkCrtBenchmarkRunner : public BenchmarkRunner
{

    std::shared_ptr<Aws::S3Crt::S3CrtClient> client;

  public:
    friend class SdkCrtTask;
    NullPreallocatedStreamBuf &streambuf;

    SdkCrtBenchmarkRunner(
        const BenchmarkConfig &config,
        string_view bucket,
        string_view region,
        double targetThroughputGbps,
        NullPreallocatedStreamBuf &streambuf)
        : BenchmarkRunner(config, bucket, region), streambuf(streambuf)
    {
        Aws::S3Crt::ClientConfiguration client_config;
        client_config.region = region;
        client_config.throughputTargetGbps = targetThroughputGbps;

        this->client = Aws::MakeShared<Aws::S3Crt::S3CrtClient>("CrtClient", client_config);
    }

    ~SdkCrtBenchmarkRunner() = default;

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
            // if (runner.config.checksum == AWS_SCA_NONE)
            // {
            //     request.SetChecksumMode(Aws::S3Crt::Model::ChecksumMode::NOT_SET);
            // }
            // TODO: checksum
            /* Last 128 KB */
            // request.SetRange("bytes=-128000");
            if (runner.config.filesOnDisk)
            {
                request.SetResponseStreamFactory(
                    [&]()
                    {
                        /* TODO: the path doesn't seems to work?? */
                        return Aws::New<Aws::FStream>(
                            FILE_STREAM_FACTORY_TAG,
                            config.key,
                            std::ios_base::out | std::ios_base::in | std::ios_base::binary | std::ios_base::trunc);
                    });
            }
            else
            {
                request.SetResponseStreamFactory(
                    [&]() { return Aws::New<MyIOStream>(FILE_STREAM_FACTORY_TAG, &runner.streambuf); });
            }
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

  public:
    friend class SdkTask;
    NullPreallocatedStreamBuf &streambuf;

    SdkBenchmarkRunner(
        const BenchmarkConfig &config,
        string_view bucket,
        string_view region,
        double targetThroughputGbps,
        NullPreallocatedStreamBuf &streambuf)
        : BenchmarkRunner(config, bucket, region), streambuf(streambuf)
    {

        Aws::Client::ClientConfiguration client_config;
        client_config.region = region;

        this->client = Aws::MakeShared<Aws::S3::S3Client>("S3Client", client_config);
    }

    ~SdkBenchmarkRunner() = default;

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
            Aws::S3::Model::PutObjectRequest request;
            request.SetBucket(runner.bucket);
            request.SetKey(config.key);
            request.SetBody();
        }
        else if (config.action == "download")
        {
            Aws::S3::Model::GetObjectRequest request;
            request.SetBucket(runner.bucket);
            request.SetKey(config.key);
            // if (runner.config.checksum == AWS_SCA_NONE)
            // {
            //     request.SetChecksumMode(Aws::S3::Model::ChecksumMode::NOT_SET);
            // }
            // // /* Last 128 KB */
            // request.SetRange("bytes=-128000");
            if (runner.config.filesOnDisk)
            {
                request.SetResponseStreamFactory(
                    [&]()
                    {
                        /* TODO: the path doesn't seems to work?? */
                        return Aws::New<Aws::FStream>(
                            FILE_STREAM_FACTORY_TAG,
                            "test",
                            std::ios_base::out | std::ios_base::in | std::ios_base::binary | std::ios_base::trunc);
                    });
            }
            else
            {
                request.SetResponseStreamFactory(
                    [&]() { return Aws::New<MyIOStream>(FILE_STREAM_FACTORY_TAG, &runner.streambuf); });
            }
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
    Aws::SDKOptions options;
    Aws::InitAPI(options);
    {

        auto config = BenchmarkConfig::fromJson(argv[2]);
        string bucket = argv[3];
        string region = argv[4];
        double targetThroughputGbps = stod(argv[5]);
        unsigned char buffer[100];
        NullPreallocatedStreamBuf streamBuf(buffer, static_cast<size_t>(100));

        if (s3ClientId == "sdk-cpp-crt")
        {
            auto runner = SdkCrtBenchmarkRunner(config, bucket, region, targetThroughputGbps, streamBuf);
            main_run(runner, config);
        }
        else
        {
            auto runner = SdkBenchmarkRunner(config, bucket, region, targetThroughputGbps, streamBuf);
            main_run(runner, config);
        }
    }
    Aws::ShutdownAPI(options);
    return 0;
}

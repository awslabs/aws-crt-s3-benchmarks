#include "SdkClientRunner.h"

#include <semaphore>

#include <aws/s3/S3Client.h>
#include <aws/s3/model/GetObjectRequest.h>
#include <aws/s3/model/PutObjectRequest.h>
#include <aws/s3-crt/S3CrtClient.h>
#include <aws/s3-crt/model/GetObjectRequest.h>
#include <aws/s3-crt/model/PutObjectRequest.h>

using namespace std;

template <
    class S3ClientT,
    class GetObjectRequestT,
    class GetObjectResultT,
    class PutObjectRequestT,
    class PutObjectResultT>
class SdkClientRunner : public BenchmarkRunner
{
  public:
    SdkClientRunner(const BenchmarkConfig &config)
        : BenchmarkRunner(config), concurrencySemaphore(maxConcurrency())
    {
        createClient();
    }

    void run() override
    {

    };

  private:
    // Specialize this function for each S3ClientT
    void createClient();

    static ptrdiff_t maxConcurrency()
    {
        // TODO: different concurrency for classic? since it's creating 1 thread per call?
        return 10000;
    }

    unique_ptr<S3ClientT> client;

    // Use this to limit concurrent work.
    counting_semaphore<> concurrencySemaphore;

    class Task
    {

    };
};

////////////////////////////////////////////////////////////////////////////////
// Classic
////////////////////////////////////////////////////////////////////////////////

using SdkClassicClientRunner = SdkClientRunner<
    Aws::S3::S3Client,
    Aws::S3::Model::GetObjectRequest,
    Aws::S3::Model::GetObjectResult,
    Aws::S3::Model::PutObjectRequest,
    Aws::S3::Model::PutObjectResult>;

template <> void SdkClassicClientRunner::createClient()
{
    Aws::Client::ClientConfiguration clientConfig;
    clientConfig.region = this->config.region;

    this->client = make_unique<Aws::S3::S3Client>(clientConfig);
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
    Aws::S3Crt::Model::GetObjectRequest,
    Aws::S3Crt::Model::GetObjectResult,
    Aws::S3Crt::Model::PutObjectRequest,
    Aws::S3Crt::Model::PutObjectResult>;

template <> void SdkCrtClientRunner::createClient()
{
    Aws::S3Crt::ClientConfiguration clientConfig;
    clientConfig.partSize = PART_SIZE;
    clientConfig.throughputTargetGbps = this->config.targetThroughputGbps;
    clientConfig.region = this->config.region;

    this->client = make_unique<Aws::S3Crt::S3CrtClient>(clientConfig);
}

unique_ptr<BenchmarkRunner> createSdkCrtClientRunner(const BenchmarkConfig &config)
{
    return make_unique<SdkCrtClientRunner>(config);
}

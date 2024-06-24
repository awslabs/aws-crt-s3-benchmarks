#include "BenchmarkRunner.h"

#include <aws/core/Aws.h>

using namespace std;

// Create runner that uses C++ SDK's classic S3Client
std::unique_ptr<BenchmarkRunner> createSdkClassicClientRunner(const BenchmarkConfig &config);

// Create runner that uses C++ SDK's S3CrtClient
std::unique_ptr<BenchmarkRunner> createSdkCrtClientRunner(const BenchmarkConfig &config);

// Create runner that uses C++ SDK's transfer manager
std::unique_ptr<BenchmarkRunner> createSdkTransferManagerRunner(const BenchmarkConfig &config);

int main(int argc, char *argv[])
{
    Aws::SDKOptions sdkOptions;
    sdkOptions.loggingOptions.logLevel = Aws::Utils::Logging::LogLevel::Error;
    Aws::InitAPI(sdkOptions);

    int exitCode = benchmarkRunnerMain(
        argc,
        argv,
        [](string_view id, const BenchmarkConfig &config)
        {
            if (id == "sdk-cpp-tm-classic")
                return createSdkTransferManagerRunner(config);
            if (id == "sdk-cpp-client-classic")
                return createSdkClassicClientRunner(config);
            if (id == "sdk-cpp-client-crt")
                return createSdkCrtClientRunner(config);
            fail("Unsupported S3_CLIENT. Options are: sdk-cpp-tm-classic, sdk-cpp-client-classic, sdk-cpp-client-crt");
        });

    Aws::ShutdownAPI(sdkOptions);

    return exitCode;
}

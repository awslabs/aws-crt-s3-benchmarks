#include "SdkClientRunner.h"
#include "SdkTransferManagerRunner.h"

#include <aws/core/Aws.h>

using namespace std;

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

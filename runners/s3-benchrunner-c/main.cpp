#include "CS3Runner.h"

using namespace std;

int main(int argc, char *argv[])
{
    return benchmarkRunnerMain(argc, argv, [](string_view id, const BenchmarkConfig &config) {
        if (id == "crt-c")
            return createCS3BenchmarkRunner(config);
        fail("Unsupported S3_CLIENT. Options are: crt-c");
    });
}

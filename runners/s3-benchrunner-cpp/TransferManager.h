#pragma once

#include "BenchmarkRunner.h"

// Create runner that uses C++ SDK's transfer manager
std::unique_ptr<BenchmarkRunner> createSdkTransferManagerRunner(const BenchmarkConfig &config);

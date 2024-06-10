#pragma once

#include "BenchmarkRunner.h"

// Create runner that uses aws-c-s3 directly
std::unique_ptr<BenchmarkRunner> createCS3BenchmarkRunner(const BenchmarkConfig &config);

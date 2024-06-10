#pragma once

#include "BenchmarkRunner.h"

// Create runner that uses C++ SDK's classic S3Client
std::unique_ptr<BenchmarkRunner> createSdkClassicClientRunner(const BenchmarkConfig &config);

// Create runner that uses C++ SDK's S3CrtClient
std::unique_ptr<BenchmarkRunner> createSdkCrtClientRunner(const BenchmarkConfig &config);

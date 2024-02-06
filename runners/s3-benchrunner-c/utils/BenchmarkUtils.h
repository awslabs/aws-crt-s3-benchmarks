#pragma once

#include <chrono>
#include <cstdio>
#include <fstream>
#include <functional>
#include <future>
#include <iostream>
#include <list>
#include <nlohmann/json.hpp>
#include <random>
#include <thread>
#include <vector>

namespace BenchmarkUtils
{
    class BenchmarkUtils
    {
      public:
        BenchmarkUtils();

        uint64_t bytesFromKiB(uint64_t kibibytes) { return kibibytes * 1024; }

        uint64_t bytesFromMiB(uint64_t mebibytes) { return mebibytes * 1024 * 1024; }

        uint64_t bytesFromGiB(uint64_t gibibytes) { return gibibytes * 1024 * 1024 * 1024; }

        double bytesToKiB(uint64_t bytes) { return (double)bytes / 1024; }

        double bytesToMiB(uint64_t bytes) { return (double)bytes / (1024 * 1024); }

        double bytesToGiB(uint64_t bytes) { return (double)bytes / (1024 * 1024 * 1024); }

        double bytesToKilobit(uint64_t bytes) { return ((double)bytes * 8) / 1'000; }

        double bytesToMegabit(uint64_t bytes) { return ((double)bytes * 8) / 1'000'000; }

        double bytesToGigabit(uint64_t bytes) { return ((double)bytes * 8) / 1'000'000'000; }
    };
} // namespace BenchmarkUtils

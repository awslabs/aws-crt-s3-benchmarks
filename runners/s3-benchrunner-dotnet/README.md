# S3 Benchmark Runner for .NET SDK

This benchmark runner tests the AWS SDK for .NET's TransferUtility implementation.

## Requirements

- .NET 8.0 SDK
- AWS credentials configured with S3 access

## Building

From the root of this directory:

```bash
cd S3BenchRunner
dotnet build -c Release
```

## Running

The runner expects to be executed from the directory containing the files to upload/download. It follows the standard command line interface used by all benchmark runners:

```bash
dotnet run -c Release -- sdk-dotnet-tm WORKLOAD BUCKET REGION TARGET_THROUGHPUT
```

Arguments:
- `sdk-dotnet-tm`: The only supported S3 client ID (current TransferUtility implementation)
- `WORKLOAD`: Path to workload .run.json file
- `BUCKET`: S3 bucket name
- `REGION`: AWS region (e.g., us-west-2)
- `TARGET_THROUGHPUT`: Target throughput in Gbps (floating point)

Example:
```bash
dotnet run -c Release -- sdk-dotnet-tm workloads/download-1MB-1.run.json my-test-bucket us-west-2 100.0
```

## Output

Results are written to stdout in a user-friendly format:
```
Run:N Secs:X.XXXXXX Gb/s:X.XXXXXX
```

Where:
- N: Run number
- X.XXXXXX: Values with 6 decimal precision
- Secs: Duration of operation in seconds
- Gb/s: Throughput in gigabits per second

Example output:
```
Run:1 Secs:0.056775 Gb/s:0.009235
Run:2 Secs:0.027504 Gb/s:0.019063
Run:3 Secs:0.057251 Gb/s:0.009158
```

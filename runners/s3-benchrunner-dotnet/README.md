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

Results are written to stdout in CSV format with the following columns:
- Operation: "download" or "upload"
- S3Key: S3 object key
- LocalPath: Local file path
- SizeBytes: Size of file in bytes
- RunNumber: Current run number for this operation
- StartTime: Operation start time (ISO 8601)
- EndTime: Operation end time (ISO 8601)
- DurationSeconds: Operation duration in seconds
- ThroughputMbps: Throughput in megabits per second
- Success: true/false
- ErrorMessage: Error message if operation failed, empty if successful

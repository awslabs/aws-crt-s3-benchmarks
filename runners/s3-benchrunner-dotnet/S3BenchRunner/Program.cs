using System.CommandLine;
using Newtonsoft.Json;
using S3BenchRunner.Models;

namespace S3BenchRunner;


public class Program
{
    public static async Task<int> Main(string[] args)
    {
        // Define command line arguments as positional arguments
        var s3ClientArg = new Argument<string>(
            name: "s3-client",
            description: "S3 client to use (sdk-dotnet-tm)");

        var workloadArg = new Argument<FileInfo>(
            name: "workload",
            description: "Path to workload .run.json file");

        var bucketArg = new Argument<string>(
            name: "bucket",
            description: "S3 bucket name");

        var regionArg = new Argument<string>(
            name: "region",
            description: "AWS region (e.g. us-west-2)");

        var targetThroughputArg = new Argument<double>(
            name: "target-throughput",
            description: "Target throughput in Gbps");

        var rootCommand = new RootCommand("S3 benchmark runner for .NET SDK")
        {
            s3ClientArg,
            workloadArg,
            bucketArg,
            regionArg,
            targetThroughputArg
        };

        rootCommand.Description = @"S3 benchmark runner for .NET SDK

Usage:
  dotnet run -c Release -- sdk-dotnet-tm workload.json my-bucket us-west-2 100.0

Arguments:
  s3-client         S3 client to use (sdk-dotnet-tm)
  workload          Path to workload .run.json file
  bucket            S3 bucket name
  region            AWS region (e.g. us-west-2)
  target-throughput Target throughput in Gbps";

        rootCommand.SetHandler(async (s3Client, workload, bucket, region, targetThroughput) =>
        {
            try
            {
                // Validate S3 client type
                if (s3Client != "sdk-dotnet-tm")
                {
                    throw new ArgumentException($"Unsupported S3 client: {s3Client}. Only sdk-dotnet-tm is supported.");
                }

                // Load and validate workload config
                var workloadJson = await File.ReadAllTextAsync(workload.FullName);
                var workloadConfig = JsonConvert.DeserializeObject<WorkloadConfig>(workloadJson)
                    ?? throw new InvalidOperationException("Failed to parse workload config");

                // Log workload configuration
                Console.WriteLine("\nWorkload Configuration:");
                Console.WriteLine($"- MaxRepeatCount: {workloadConfig.MaxRepeatCount}");
                Console.WriteLine($"- MaxRepeatSecs: {workloadConfig.MaxRepeatSecs}");
                Console.WriteLine($"- FilesOnDisk: {workloadConfig.FilesOnDisk}");
                Console.WriteLine("\nTasks:");
                foreach (var task in workloadConfig.Tasks)
                {
                    Console.WriteLine($"- Task: action={task.Action}, size={task.Size:N0} bytes, key={task.S3Key}");
                }
                Console.WriteLine();

                // Calculate total bytes per run (sum of all task sizes)
                var bytesPerRun = workloadConfig.Tasks.Sum(t => t.Size);
                Console.WriteLine($"Total bytes per run: {bytesPerRun:N0}\n");

                // Create benchmark runner
                var benchmarkRunner = new TransferUtilityBenchmarkRunner(workloadConfig, bucket, region, targetThroughput);

                // Track overall start time for max duration check
                var appStartTime = DateTimeOffset.UtcNow;

                // Run benchmarks
                for (int run = 1; run <= workloadConfig.MaxRepeatCount; run++)
                {
                    // Check if we've exceeded the time limit
                    if ((DateTimeOffset.UtcNow - appStartTime).TotalSeconds > workloadConfig.MaxRepeatSecs)
                    {
                        break;
                    }

                    // Prepare for this run (clean up files)
                    benchmarkRunner.PrepareRun();

                    // Time each complete run of all tasks
                    var runStartTime = DateTimeOffset.UtcNow;
                    var success = true;

                    try
                    {
                        await benchmarkRunner.RunAsync();
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine($"\nError during run {run}:");
                        Console.WriteLine($"- Message: {ex.Message}");
                        Console.WriteLine($"- Stack trace: {ex.StackTrace}\n");
                        success = false;
                        Environment.ExitCode = 1;
                        break;
                    }

                    if (!success)
                    {
                        break;
                    }

                    var runEndTime = DateTimeOffset.UtcNow;
                    var runResult = new BenchmarkResult
                    {
                        Operation = workloadConfig.Tasks[0].Action, // Use first task's action type
                        S3Key = string.Join(",", workloadConfig.Tasks.Select(t => t.S3Key)),
                        LocalPath = string.Join(",", workloadConfig.Tasks.Select(t => t.LocalPath)),
                        SizeBytes = bytesPerRun,
                        RunNumber = run,
                        StartTime = runStartTime,
                        EndTime = runEndTime,
                        Success = true
                    };

                    // Write console format to stdout for user display
                    Console.WriteLine(runResult.ToConsoleString());
                }
            }
            catch (Exception ex)
            {
                Console.Error.WriteLine($"\nError:");
                Console.Error.WriteLine($"- Message: {ex.Message}");
                Console.Error.WriteLine($"- Stack trace: {ex.StackTrace}\n");
                Environment.ExitCode = 1;
            }
        },
        s3ClientArg, workloadArg, bucketArg, regionArg, targetThroughputArg);

        return await rootCommand.InvokeAsync(args);
    }
}

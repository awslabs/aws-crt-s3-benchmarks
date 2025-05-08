using System.CommandLine;
using Newtonsoft.Json;
using S3BenchRunner.Client;
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

                // Write CSV header to stderr for data collection
                Console.Error.WriteLine(BenchmarkResult.GetCsvHeader());

                // Run benchmarks
                using var client = new TransferUtilityClient(bucket, region);
                foreach (var task in workloadConfig.Tasks)
                {
                    var startTime = DateTimeOffset.UtcNow;
                    for (int run = 1; run <= workloadConfig.MaxRepeatCount; run++)
                    {
                        // Check if we've exceeded the time limit
                        if ((DateTimeOffset.UtcNow - startTime).TotalSeconds > workloadConfig.MaxRepeatSecs)
                        {
                            break;
                        }

                        BenchmarkResult result;
                        if (task.Action == "download")
                        {
                            result = await client.DownloadAsync(task.S3Key, task.LocalPath, run);
                        }
                        else if (task.Action == "upload")
                        {
                            result = await client.UploadAsync(task.LocalPath, task.S3Key, run);
                        }
                        else
                        {
                            throw new ArgumentException($"Unsupported action: {task.Action}");
                        }

                        // Write CSV to stderr for data collection
                        Console.Error.WriteLine(result.ToString());
                        // Write console format to stdout for user display
                        Console.WriteLine(result.ToConsoleString());

                        if (!result.Success)
                        {
                            Environment.ExitCode = 1;
                            break;
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                Console.Error.WriteLine($"Error: {ex.Message}");
                Environment.ExitCode = 1;
            }
        },
        s3ClientArg, workloadArg, bucketArg, regionArg, targetThroughputArg);

        return await rootCommand.InvokeAsync(args);
    }
}

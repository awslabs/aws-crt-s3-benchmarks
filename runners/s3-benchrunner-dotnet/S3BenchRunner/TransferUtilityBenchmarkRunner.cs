using S3BenchRunner.Client;
using S3BenchRunner.Models;

namespace S3BenchRunner;

public class TransferUtilityBenchmarkRunner : BenchmarkRunner
{
    private readonly TransferUtilityClient _client;

    public TransferUtilityBenchmarkRunner(WorkloadConfig config, string bucket, string region, double targetThroughputGbps, bool withResponseApis)
        : base(config, bucket, region)
    {
        _client = new TransferUtilityClient(bucket, region, config.FilesOnDisk, config.Tasks, withResponseApis);
    }

    public override async Task RunAsync()
        {
            // Execute ALL downloads in parallel (like Java CRT does)
            var downloadTasks = Config.Tasks
                .Where(t => t.Action == "download")
                .Select(async task => {
                    var success = await _client.DownloadAsync(task.S3Key, task.LocalPath);
                    if (!success)
                    {
                        throw new Exception($"Download failed for {task.S3Key}");
                    }
                });

            var uploadTasks = Config.Tasks
                .Where(t => t.Action == "upload")
                .Select(async task => {
                    var success = await _client.UploadAsync(task.LocalPath, task.S3Key);
                    if (!success)
                    {
                        throw new Exception($"Upload failed for {task.S3Key}");
                    }
                });

            // Wait for ALL tasks to complete in parallel
            await Task.WhenAll(downloadTasks.Concat(uploadTasks));
        }


    public void Dispose()
    {
        _client.Dispose();
    }
}

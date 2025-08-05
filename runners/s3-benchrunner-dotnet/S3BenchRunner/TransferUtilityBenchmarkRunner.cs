using S3BenchRunner.Client;
using S3BenchRunner.Models;

namespace S3BenchRunner;

public class TransferUtilityBenchmarkRunner : BenchmarkRunner
{
    private readonly TransferUtilityClient _client;

    public TransferUtilityBenchmarkRunner(WorkloadConfig config, string bucket, string region, double targetThroughputGbps)
        : base(config, bucket, region)
    {
        _client = new TransferUtilityClient(bucket, region, config.FilesOnDisk, config.Tasks);
    }

    public override async Task RunAsync()
    {
        // Process each download task individually
        foreach (var task in Config.Tasks.Where(t => t.Action == "download"))
        {
            var success = await _client.DownloadAsync(task.S3Key, task.LocalPath);
            if (!success)
            {
                throw new Exception("Download failed");
            }
        }

        // Process each upload task individually
        foreach (var task in Config.Tasks.Where(t => t.Action == "upload"))
        {
            var success = await _client.UploadAsync(task.LocalPath, task.S3Key);
            if (!success)
            {
                throw new Exception("Upload failed");
            }
        }
    }

    public void Dispose()
    {
        _client.Dispose();
    }
}

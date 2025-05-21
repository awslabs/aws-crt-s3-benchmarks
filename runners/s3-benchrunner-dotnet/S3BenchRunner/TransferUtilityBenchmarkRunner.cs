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
        // Group tasks by action type
        var downloadTasks = Config.Tasks.Where(t => t.Action == "download").ToList();
        var uploadTasks = Config.Tasks.Where(t => t.Action == "upload").ToList();

        // Handle downloads
        if (downloadTasks.Count() > 0)
        {
            var task = downloadTasks[0];
            var success = await _client.DownloadAsync(task.S3Key, task.LocalPath, downloadTasks);
            if (!success)
            {
                throw new Exception("Download failed");
            }
        }

        // Handle uploads
        if (uploadTasks.Count() > 0)
        {
            var task = uploadTasks[0];
            var success = await _client.UploadAsync(task.LocalPath, task.S3Key, uploadTasks);
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
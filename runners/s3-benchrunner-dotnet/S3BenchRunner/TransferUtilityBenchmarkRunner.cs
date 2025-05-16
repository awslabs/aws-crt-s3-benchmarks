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
        if (downloadTasks.Any())
        {
            // For multiple downloads, pass all tasks to enable directory-based download
            if (downloadTasks.Count > 1)
            {
                var firstTask = downloadTasks[0];
                var success = await _client.DownloadAsync(firstTask.S3Key, firstTask.LocalPath, downloadTasks);
                if (!success)
                {
                    throw new Exception("Download failed");
                }
            }
            else
            {
                var task = downloadTasks[0];
                var success = await _client.DownloadAsync(task.S3Key, task.LocalPath);
                if (!success)
                {
                    throw new Exception("Download failed");
                }
            }
        }

        // Handle uploads
        if (uploadTasks.Any())
        {
            if (uploadTasks.Count > 1)
            {
                var firstTask = uploadTasks[0];
                var success = await _client.UploadAsync(firstTask.LocalPath, firstTask.S3Key, uploadTasks);
                if (!success)
                {
                    throw new Exception("Upload failed");
                }
            }
            else
            {
                var task = uploadTasks[0];
                var success = await _client.UploadAsync(task.LocalPath, task.S3Key);
                if (!success)
                {
                    throw new Exception("Upload failed");
                }
            }
        }
    }

    public void Dispose()
    {
        _client.Dispose();
    }
}
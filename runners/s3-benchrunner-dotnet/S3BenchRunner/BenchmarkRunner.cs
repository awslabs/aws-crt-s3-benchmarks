using S3BenchRunner.Models;

namespace S3BenchRunner;

public abstract class BenchmarkRunner
{
    protected WorkloadConfig Config { get; }
    protected string Bucket { get; }
    protected string Region { get; }

    protected BenchmarkRunner(WorkloadConfig config, string bucket, string region)
    {
        Config = config;
        Bucket = bucket;
        Region = region;
    }

    public abstract Task RunAsync();

    public void PrepareRun()
    {
        // Preparation work between runs
        foreach (var task in Config.Tasks)
        {
            if (task.Action == "download")
            {
                var path = task.LocalPath;
                var directory = Path.GetDirectoryName(path);
                
                // Create directory if it doesn't exist
                if (!string.IsNullOrEmpty(directory) && !Directory.Exists(directory))
                {
                    Directory.CreateDirectory(directory);
                }
                
                // Delete file if it exists
                if (File.Exists(path))
                {
                    File.Delete(path);
                }
            }
        }
    }
}
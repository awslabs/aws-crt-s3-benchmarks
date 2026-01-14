namespace S3BenchRunner.Models;

public class BenchmarkResult
{
    public string Operation { get; set; } = string.Empty;
    public string S3Key { get; set; } = string.Empty;
    public string LocalPath { get; set; } = string.Empty;
    public long SizeBytes { get; set; }
    public int RunNumber { get; set; }
    public DateTimeOffset StartTime { get; set; }
    public DateTimeOffset EndTime { get; set; }
    public TimeSpan Duration => EndTime - StartTime;
    
    // Calculate throughput in Mbps based on total bytes transferred in this run
    // For workloads with multiple tasks, SizeBytes is the sum of all task sizes
    public double ThroughputMbps => (SizeBytes * 8.0) / (1_000_000 * Duration.TotalSeconds);
    public bool Success { get; set; }
    public string? ErrorMessage { get; set; }

    public override string ToString()
    {
        return $"{Operation},{S3Key},{LocalPath},{SizeBytes},{RunNumber}," +
               $"{StartTime:O},{EndTime:O},{Duration.TotalSeconds:F3}," +
               $"{ThroughputMbps:F2},{Success},{ErrorMessage ?? ""}";
    }

    public string ToConsoleString()
    {
        // Each run represents a complete execution of all tasks in the workload
        // Duration is the time taken to execute all tasks
        // ThroughputMbps is calculated using the sum of all task sizes
        return $"Run:{RunNumber} Secs:{Duration.TotalSeconds:F6} Gb/s:{ThroughputMbps/1000:F6}";
    }
}

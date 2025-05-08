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
        return $"Run:{RunNumber} Secs:{Duration.TotalSeconds:F6} Gb/s:{ThroughputMbps/1000:F6}";
    }
}

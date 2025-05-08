using Newtonsoft.Json;

namespace S3BenchRunner.Models;

public class WorkloadConfig
{
    [JsonProperty("version")]
    public int Version { get; set; }

    [JsonProperty("comment")]
    public string Comment { get; set; } = string.Empty;

    [JsonProperty("filesOnDisk")]
    public bool FilesOnDisk { get; set; }

    [JsonProperty("checksum")]
    public string? Checksum { get; set; }

    [JsonProperty("maxRepeatCount")]
    public int MaxRepeatCount { get; set; }

    [JsonProperty("maxRepeatSecs")]
    public int MaxRepeatSecs { get; set; }

    [JsonProperty("tasks")]
    public List<WorkloadTask> Tasks { get; set; } = new();
}

public class WorkloadTask
{
    [JsonProperty("action")]
    public string Action { get; set; } = string.Empty;

    [JsonProperty("key")]
    public string Key { get; set; } = string.Empty;

    [JsonProperty("size")]
    public long Size { get; set; }

    // Computed properties for compatibility with existing code
    public string Operation => Action;
    public string S3Key => Key;
    public string LocalPath => Path.Combine(Path.GetDirectoryName(Key) ?? string.Empty, Path.GetFileName(Key));
    public long SizeBytes => Size;
    public int Repeat => 1; // We'll use maxRepeatCount from the parent config
}

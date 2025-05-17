using Amazon.S3;
using Amazon.S3.Transfer;
using S3BenchRunner.Models;

namespace S3BenchRunner.Client;

public class TransferUtilityClient : IDisposable
{
    private readonly IAmazonS3 _s3Client;
    private readonly ITransferUtility _transferUtility;
    private readonly string _bucketName;
    private readonly bool _filesOnDisk;
    private readonly TransferUtilityConfig _transferConfig;

    public TransferUtilityClient(string bucketName, string region, bool filesOnDisk, IEnumerable<WorkloadTask> tasks)
    {
        _bucketName = bucketName;
        var config = new AmazonS3Config
        {
            RegionEndpoint = Amazon.RegionEndpoint.GetBySystemName(region),
            // Use path style addressing for compatibility with benchmark infrastructure
            ForcePathStyle = true
        };
        _s3Client = new AmazonS3Client(config);
        // Configure transfer utility with concurrent requests based on number of tasks
        _transferConfig = new TransferUtilityConfig
        {
            ConcurrentServiceRequests = tasks.Count()
        };
        _transferUtility = new TransferUtility(_s3Client, _transferConfig);
        _filesOnDisk = filesOnDisk;
    }

    private string GetCommonRootDirectory(IEnumerable<WorkloadTask> tasks)
    {
        if (!tasks.Any())
            throw new ArgumentException("No tasks provided");

        var firstPath = Path.GetDirectoryName(tasks.First().S3Key) 
            ?? throw new ArgumentException($"Invalid S3 key path: {tasks.First().S3Key}");
        if (string.IsNullOrEmpty(firstPath))
            throw new ArgumentException("Tasks must be in a directory");

        var commonRoot = firstPath;
        foreach (var task in tasks.Skip(1))
        {
            var taskPath = Path.GetDirectoryName(task.S3Key)
                ?? throw new ArgumentException($"Invalid S3 key path: {task.S3Key}");
            while (!string.IsNullOrEmpty(commonRoot) && !taskPath.StartsWith(commonRoot))
            {
                commonRoot = Path.GetDirectoryName(commonRoot);
            }

            if (string.IsNullOrEmpty(commonRoot))
                throw new ArgumentException("Tasks must share a common root directory");
        }

        return commonRoot;
    }

    public async Task<bool> DownloadAsync(string s3Key, string localPath, IEnumerable<WorkloadTask>? allTasks = null)
    {
        try
        {
            // If we have multiple tasks, use directory download
            if (_filesOnDisk && allTasks != null && allTasks.Count() > 1)
            {
                var commonRoot = GetCommonRootDirectory(allTasks);
                var localDir = Path.GetDirectoryName(localPath);

                // Download the directory
                var downloadRequest = new TransferUtilityDownloadDirectoryRequest
                {
                    BucketName = _bucketName,
                    LocalDirectory = localDir,
                    S3Directory = commonRoot,
                    DownloadFilesConcurrently = true
                };

                await _transferUtility.DownloadDirectoryAsync(downloadRequest);
            }
            else if (_filesOnDisk)
            {   
                // Download the file
                var downloadRequest = new TransferUtilityDownloadRequest
                {
                    BucketName = _bucketName,
                    Key = s3Key,
                    FilePath = localPath,
                };

                await _transferUtility.DownloadAsync(downloadRequest);
            }
            else
            {
                // Download to stream
                var streamRequest = new TransferUtilityOpenStreamRequest
                {
                    BucketName = _bucketName,
                    Key = s3Key
                };

                // Open stream from S3 and copy to null stream
                using var s3Stream = await _transferUtility.OpenStreamAsync(streamRequest);
                using var nullStream = Stream.Null;
                await s3Stream.CopyToAsync(nullStream);
            }

            return true;
        }
        catch (Exception)
        {
            return false;
        }
    }

    public async Task<bool> UploadAsync(string localPath, string s3Key, IEnumerable<WorkloadTask> allTasks)
    {
        try
        {
            if (_filesOnDisk && allTasks != null && allTasks.Count() > 1)
            {
                var commonRoot = GetCommonRootDirectory(allTasks);
                var localDir = Path.GetDirectoryName(localPath);

                if (!Directory.Exists(localDir))
                {
                    throw new DirectoryNotFoundException($"Source directory not found: {localDir}");
                }

                // Upload the directory
                var uploadRequest = new TransferUtilityUploadDirectoryRequest
                {
                    Directory = localDir,
                    BucketName = _bucketName,
                    KeyPrefix = commonRoot,
                    SearchPattern = "*",
                    SearchOption = SearchOption.AllDirectories
                };

                await _transferUtility.UploadDirectoryAsync(uploadRequest);
            }
            else if (_filesOnDisk)
            {
                if (!File.Exists(localPath))
                {
                    throw new FileNotFoundException($"Source file not found: {localPath}");
                }

                var fileInfo = new FileInfo(localPath);
                var uploadRequest = new TransferUtilityUploadRequest
                {
                    FilePath = localPath,
                    BucketName = _bucketName,
                    Key = s3Key
                };

                await _transferUtility.UploadAsync(uploadRequest);
            }
            else
            {
                // Upload from memory using random data
                // If we're not using files on disk, generate random data for uploads

                // Find largest upload size from tasks, matching Python's implementation
                var largestUpload = allTasks
                        .Where(t => t.Action == "upload")
                        .DefaultIfEmpty(new WorkloadTask { Size = 0 })
                        .Max(t => t.Size);
                
                var _randomData = new byte[largestUpload];
                Random.Shared.NextBytes(_randomData);
                long size = _randomData.Length;
                using var stream = new MemoryStream(_randomData, 0, (int)size);
                var uploadRequest = new TransferUtilityUploadRequest
                {
                    InputStream = stream,
                    BucketName = _bucketName,
                    Key = s3Key,
                    AutoCloseStream = true
                };

                await _transferUtility.UploadAsync(uploadRequest);
            }

            return true;
        }
        catch (Exception)
        {
            return false;
        }
    }

    public void Dispose()
    {
        _transferUtility.Dispose();
        _s3Client.Dispose();
    }
}
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
    private readonly byte[]? _randomData;


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
            ConcurrentServiceRequests = 500 // TODO possibly update
        };
        _transferUtility = new TransferUtility(_s3Client, _transferConfig);
        _filesOnDisk = filesOnDisk;

        if (!_filesOnDisk)
        {
            // Find largest upload size from tasks
            var largestUpload = tasks
                .Where(t => t.Action == "upload")
                .DefaultIfEmpty(new WorkloadTask { Size = 0 })
                .Max(t => t.Size);
            _randomData = new byte[largestUpload];
            Random.Shared.NextBytes(_randomData);
        }
    }


    public async Task<bool> DownloadAsync(string s3Key, string localPath)
    {
        try
        {
            Logger.LogVerbose($"Starting download: s3Key={s3Key}, localPath={localPath}");
            
            if (_filesOnDisk)
            {   
                Console.WriteLine($"Using single file download");
                // Download the file
                var downloadRequest = new TransferUtilityDownloadRequest
                {
                    BucketName = _bucketName,
                    Key = s3Key,
                    FilePath = localPath,
                };

                Console.WriteLine($"Download request: bucket={_bucketName}, key={s3Key}, file={localPath}");
                await _transferUtility.DownloadAsync(downloadRequest);
                
                // Add file size check
                var fileInfo = new FileInfo(localPath);
                Console.WriteLine($"Download complete: Size={fileInfo.Length:N0} bytes");
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
        catch (Exception ex)
        {
            Console.WriteLine($"Download failed: {ex.Message}");
            Console.WriteLine($"Stack trace: {ex.StackTrace}");
            return false;
        }
    }

    public async Task<bool> UploadAsync(string localPath, string s3Key)
    {
        try
        {
            if (_filesOnDisk)
            {
                if (!File.Exists(localPath))
                {
                    throw new FileNotFoundException($"Source file not found: {localPath}");
                }

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
                using var stream = new MemoryStream(_randomData, 0, _randomData.Length);
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

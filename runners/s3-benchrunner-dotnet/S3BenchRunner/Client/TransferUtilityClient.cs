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
    private long  largestUploadSize = 0;

    public TransferUtilityClient(string bucketName, string region, bool filesOnDisk, IEnumerable<WorkloadTask> tasks)
    {
        _bucketName = bucketName;

        // var crtOptions = new CrtHttpClientOptions
        // {
        //     MaxConnectionsPerServer = 200,      // Optimize for your concurrency
        //     InitialWindowSize = 16777216 * 4,      // 16MB for large S3 parts  
        //     ConnectTimeoutMs = 10000,          // 10 second timeout
        //     EnableHttp2 = true                 // HTTP/2 multiplexing
        // };

        var config = new AmazonS3Config
        {
            RegionEndpoint = Amazon.RegionEndpoint.GetBySystemName(region),
            // Use path style addressing for compatibility with benchmark infrastructure
            ForcePathStyle = true,
            // HttpClient()
            // HttpClientFactory = new CrtHttpClientFactory(crtOptions)

            // LogResponse = true,
            // LogMetrics = true
        };


        _s3Client = new AmazonS3Client(config);
        // Configure transfer utility with concurrent requests based on number of tasks
        _transferConfig = new TransferUtilityConfig
        {
            ConcurrentServiceRequests = 100,
            MaxInMemoryParts = 1024
        };
        _transferUtility = new TransferUtility(_s3Client, _transferConfig);
        _filesOnDisk = filesOnDisk;

        if (!_filesOnDisk)
        {
            // Find largest upload size from tasks
           largestUploadSize = tasks
                .Where(t => t.Action == "upload")
                .DefaultIfEmpty(new WorkloadTask { Size = 0 })
                .Max(t => t.Size);
        }
    }


    public async Task<bool> DownloadAsync(string s3Key, string localPath)
    {
        try
        {
            Logger.LogVerbose($"Starting download: s3Key={s3Key}, localPath={localPath}");
            
            if (_filesOnDisk)
            {   
                Logger.LogVerbose($"Using single file download");
                // Download the file
                var downloadRequest = new TransferUtilityDownloadRequest
                {
                    BucketName = _bucketName,
                    Key = s3Key,
                    FilePath = localPath,
                };

                Logger.LogVerbose($"Download request: bucket={_bucketName}, key={s3Key}, file={localPath}");
                await _transferUtility.DownloadAsync(downloadRequest);
                
                // Add file size check
                var fileInfo = new FileInfo(localPath);
                Logger.LogVerbose($"Download complete: Size={fileInfo.Length:N0} bytes");
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

                // Pre-allocate single buffer (reused across all reads)
                var buffer = new byte[32 * 1024 * 1024]; // 32MB buffer
                int bytesRead;

                while ((bytesRead = await s3Stream.ReadAsync(buffer, 0, buffer.Length)) > 0)
                {
                    // Data is read into buffer, then immediately "discarded"
                    // No WriteAsync calls, no copying, minimal overhead
                    // This measures pure BufferedMultipartStream.ReadAsync() performance
                }

            }

            return true;
        }
        catch (Exception ex)
        {
            Logger.LogAlways($"Download failed: {ex.Message}");
            Logger.LogVerbose($"Stack trace: {ex.StackTrace}");
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
                using var stream = new RandomDataStream(largestUploadSize);
                var uploadRequest = new TransferUtilityUploadRequest
                {
                    InputStream = stream,
                    BucketName = _bucketName,
                    Key = s3Key,
                    AutoCloseStream = true,
                };

                await _transferUtility.UploadAsync(uploadRequest);
            }

            return true;
        }
        catch (Exception ex)
        {
            Logger.LogAlways($"Upload failed: {ex.Message}");
            Logger.LogVerbose($"Stack trace: {ex.StackTrace}");
            return false;
        }
    }

    public void Dispose()
    {
        _transferUtility.Dispose();
        _s3Client.Dispose();
    }
}

public class RandomDataStream : Stream
{
    private readonly long _length;
    private long _position;
    private readonly Random _random = new Random();

    public RandomDataStream(long length) => _length = length;
    
    public override bool CanRead => true;
    public override bool CanSeek => false;
    public override bool CanWrite => false;
    public override long Length => _length;
    public override long Position { get => _position; set => throw new NotSupportedException(); }

    public override int Read(byte[] buffer, int offset, int count)
    {
        var remaining = (int)Math.Min(count, _length - _position);
        if (remaining <= 0) return 0;
        
        _random.NextBytes(buffer.AsSpan(offset, remaining));
        _position += remaining;
        return remaining;
    }

    // Required overrides
    public override void Flush() { }
    public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();
    public override void SetLength(long value) => throw new NotSupportedException();
    public override void Write(byte[] buffer, int offset, int count) => throw new NotSupportedException();
}

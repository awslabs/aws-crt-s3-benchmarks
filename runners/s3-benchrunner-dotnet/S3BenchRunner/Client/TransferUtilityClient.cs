using Amazon.S3;
using Amazon.S3.Transfer;
using S3BenchRunner.Models;
using System.IO;


namespace S3BenchRunner.Client;

public class TransferUtilityClient : IDisposable
{
    private readonly IAmazonS3 _s3Client;
    private readonly ITransferUtility _transferUtility;
    private readonly string _bucketName;
    private readonly bool _filesOnDisk;
    private readonly TransferUtilityConfig _transferConfig;
    private readonly bool _withResponseApis;
    private long  largestUploadSize = 0;

    public TransferUtilityClient(string bucketName, string region, bool filesOnDisk, IEnumerable<WorkloadTask> tasks, bool withResponseApis)
    {
        _bucketName = bucketName;
        _withResponseApis = withResponseApis;

        var config = new AmazonS3Config
        {
            RegionEndpoint = Amazon.RegionEndpoint.GetBySystemName(region)
        };


        _s3Client = new AmazonS3Client(config);
        // Configure transfer utility with concurrent requests based on number of tasks
        _transferConfig = new TransferUtilityConfig
        {
            ConcurrentServiceRequests = 100
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
            // Logger.LogVerbose($"Starting download: s3Key={s3Key}, localPath={localPath}");
            
            if (_filesOnDisk)
            {   
                // Logger.LogVerbose($"Using single file download");
                // Download the file
                var downloadRequest = new TransferUtilityDownloadRequest
                {
                    BucketName = _bucketName,
                    Key = s3Key,
                    FilePath = localPath,
                };

                // Logger.LogVerbose($"Download request: bucket={_bucketName}, key={s3Key}, file={localPath}");
                if (_withResponseApis)
                {
                    await _transferUtility.DownloadWithResponseAsync(downloadRequest);
                }
                else
                {
                    await _transferUtility.DownloadAsync(downloadRequest);
                }
                
                // Add file size check
                var fileInfo = new FileInfo(localPath);
                // Logger.LogVerbose($"Download complete: Size={fileInfo.Length:N0} bytes");
            }
            else
            {
                // Download to stream
                var streamRequest = new TransferUtilityOpenStreamRequest
                {
                    BucketName = _bucketName,
                    Key = s3Key
                };

                Stream stream;
                if (_withResponseApis)
                {
                    var response = await _transferUtility.OpenStreamWithResponseAsync(streamRequest);
                    stream = response.ResponseStream;
                }
                else
                {
                    stream = await _transferUtility.OpenStreamAsync(streamRequest);
                }

                // Pre-allocate single buffer (reused across all reads)
                var buffer = new byte[8 * 1024 * 1024]; 
                int bytesRead;

            using (stream)
            {
                using var cts = new CancellationTokenSource(TimeSpan.FromMinutes(30));
                            while ((bytesRead = await stream.ReadAsync(buffer, 0, buffer.Length, cts.Token)) > 0)
                                {
                                    // Console.WriteLine($"Read {bytesRead} bytes");
                                }
            }

            }

            return true;
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Download failed: {ex.Message}");
            // Logger.LogVerbose($"Stack trace: {ex.StackTrace}");
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

                if (_withResponseApis)
                {
                    await _transferUtility.UploadWithResponseAsync(uploadRequest);
                }
                else
                {
                    await _transferUtility.UploadAsync(uploadRequest);
                }
            }
            else
            {
                using var stream = new RandomDataStream(largestUploadSize, canSeek: false);
                var uploadRequest = new TransferUtilityUploadRequest
                {
                    InputStream = stream,
                    BucketName = _bucketName,
                    Key = s3Key,
                    AutoCloseStream = true,
                };

                if (_withResponseApis)
                {
                    await _transferUtility.UploadWithResponseAsync(uploadRequest);
                }
                else
                {
                    await _transferUtility.UploadAsync(uploadRequest);
                }
            }

            return true;
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Upload failed: {ex.Message}");
            // Logger.LogVerbose($"Stack trace: {ex.StackTrace}");
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
    private readonly bool _canSeek;

    public RandomDataStream(long length, bool canSeek = false)
    {
        _length = length;
        _canSeek = canSeek;
    }
    
    public override bool CanRead => true;
    public override bool CanSeek => _canSeek;
    public override bool CanWrite => false;
    public override long Length => _length;
    
    public override long Position 
    { 
        get => _position; 
        set 
        {
            if (!_canSeek)
                throw new NotSupportedException("Stream does not support seeking.");
            if (value < 0 || value > _length)
                throw new ArgumentOutOfRangeException(nameof(value));
            _position = value;
        }
    }

    public override int Read(byte[] buffer, int offset, int count)
    {
        var remaining = (int)Math.Min(count, _length - _position);
        if (remaining <= 0) return 0;
        
        _random.NextBytes(buffer.AsSpan(offset, remaining));
        _position += remaining;
        return remaining;
    }

    public override long Seek(long offset, SeekOrigin origin)
    {
        if (!_canSeek)
            throw new NotSupportedException("Stream does not support seeking.");
            
        long newPosition = origin switch
        {
            SeekOrigin.Begin => offset,
            SeekOrigin.Current => _position + offset,
            SeekOrigin.End => _length + offset,
            _ => throw new ArgumentException("Invalid seek origin.", nameof(origin))
        };
        
        if (newPosition < 0 || newPosition > _length)
            throw new ArgumentOutOfRangeException(nameof(offset), "Seek position is out of range.");
            
        _position = newPosition;
        return _position;
    }

    // Required overrides
    public override void Flush() { }
    public override void SetLength(long value) => throw new NotSupportedException();
    public override void Write(byte[] buffer, int offset, int count) => throw new NotSupportedException();
}

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
    private readonly byte[]? _randomData;

    private readonly string? _checksum;

    public TransferUtilityClient(string bucketName, string region, bool filesOnDisk, IEnumerable<WorkloadTask> tasks, string? checksum)
    {
        _bucketName = bucketName;
        var config = new AmazonS3Config
        {
            RegionEndpoint = Amazon.RegionEndpoint.GetBySystemName(region),
            // Use path style addressing for compatibility with benchmark infrastructure
            ForcePathStyle = true
        };
        _s3Client = new AmazonS3Client(config);
        _transferUtility = new TransferUtility(_s3Client);
        _filesOnDisk = filesOnDisk;
        _checksum = checksum;

        // If we're not using files on disk, generate random data for uploads
        if (!filesOnDisk)
        {
            // Find largest upload size from tasks, matching Python's implementation
            var largestUpload = tasks
                .Where(t => t.Action == "upload")
                .DefaultIfEmpty(new WorkloadTask { Size = 0 })
                .Max(t => t.Size);

            if (largestUpload > 0)
            {
                _randomData = new byte[largestUpload];
                Random.Shared.NextBytes(_randomData);
            }
        }
    }

    public async Task<BenchmarkResult> DownloadAsync(string s3Key, string localPath, int runNumber)
    {
        var result = new BenchmarkResult
        {
            Operation = "download",
            S3Key = s3Key,
            LocalPath = localPath,
            RunNumber = runNumber,
            StartTime = DateTimeOffset.UtcNow
        };

        try
        {
            // Get object metadata to set SizeBytes
            var metadata = await _s3Client.GetObjectMetadataAsync(_bucketName, s3Key);
            result.SizeBytes = metadata.ContentLength;

            if (_filesOnDisk)
            {
                // Clean up existing file first
                if (File.Exists(localPath))
                {
                    File.Delete(localPath);
                }

                // Ensure directory exists
                var directory = Path.GetDirectoryName(localPath);
                if (!string.IsNullOrEmpty(directory))
                {
                    Directory.CreateDirectory(directory);
                }

                // Download the file
                var downloadRequest = new TransferUtilityDownloadRequest
                {
                    BucketName = _bucketName,
                    Key = s3Key,
                    FilePath = localPath
                };

                await _transferUtility.DownloadAsync(downloadRequest);

                // Verify downloaded file
                if (File.Exists(localPath))
                {
                    var fileInfo = new FileInfo(localPath);
                    if (fileInfo.Length != metadata.ContentLength)
                    {
                        throw new Exception($"Downloaded file size ({fileInfo.Length}) does not match expected size ({metadata.ContentLength})");
                    }
                }
                else
                {
                    throw new Exception("File was not created after download");
                }
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

            result.Success = true;
        }
        catch (Exception ex)
        {
            result.Success = false;
            result.ErrorMessage = ex.Message;
        }
        finally
        {
            result.EndTime = DateTimeOffset.UtcNow;
        }

        return result;
    }

    public async Task<BenchmarkResult> UploadAsync(string localPath, string s3Key, int runNumber)
    {
        var result = new BenchmarkResult
        {
            Operation = "upload",
            S3Key = s3Key,
            LocalPath = localPath,
            RunNumber = runNumber,
            StartTime = DateTimeOffset.UtcNow
        };

        try
        {
            if (_filesOnDisk)
            {
                if (!File.Exists(localPath))
                {
                    throw new FileNotFoundException($"Source file not found: {localPath}");
                }

                var fileInfo = new FileInfo(localPath);
                result.SizeBytes = fileInfo.Length;
                var uploadRequest = new TransferUtilityUploadRequest
                {
                    FilePath = localPath,
                    BucketName = _bucketName,
                    Key = s3Key
                };

                await _transferUtility.UploadAsync(uploadRequest);

                // Verify upload by getting metadata
                var metadata = await _s3Client.GetObjectMetadataAsync(_bucketName, s3Key);
                if (metadata.ContentLength != fileInfo.Length)
                {
                    throw new Exception($"Uploaded file size ({metadata.ContentLength}) does not match source size ({fileInfo.Length})");
                }
            }
            else
            {
                // Upload from memory using random data
                if (_randomData == null)
                    throw new InvalidOperationException("Random data buffer not initialized");

                result.SizeBytes = result.SizeBytes > 0 ? result.SizeBytes : _randomData.Length;
                using var stream = new MemoryStream(_randomData, 0, (int)result.SizeBytes);
                var uploadRequest = new TransferUtilityUploadRequest
                {
                    InputStream = stream,
                    BucketName = _bucketName,
                    Key = s3Key,
                    AutoCloseStream = true
                };

                await _transferUtility.UploadAsync(uploadRequest);

                // Verify upload by getting metadata
                var metadata = await _s3Client.GetObjectMetadataAsync(_bucketName, s3Key);
                if (metadata.ContentLength != result.SizeBytes)
                {
                    throw new Exception($"Uploaded file size ({metadata.ContentLength}) does not match expected size ({result.SizeBytes})");
                }
            }

            result.Success = true;
        }
        catch (Exception ex)
        {
            result.Success = false;
            result.ErrorMessage = ex.Message;
        }
        finally
        {
            result.EndTime = DateTimeOffset.UtcNow;
        }

        return result;
    }

    public void Dispose()
    {
        _transferUtility.Dispose();
        _s3Client.Dispose();
    }
}

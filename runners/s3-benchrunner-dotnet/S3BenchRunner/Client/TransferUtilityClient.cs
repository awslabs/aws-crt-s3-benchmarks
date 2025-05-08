using Amazon.S3;
using Amazon.S3.Transfer;
using S3BenchRunner.Models;

namespace S3BenchRunner.Client;

public class TransferUtilityClient : IDisposable
{
    private readonly IAmazonS3 _s3Client;
    private readonly ITransferUtility _transferUtility;
    private readonly string _bucketName;

    public TransferUtilityClient(string bucketName, string region)
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
            // Ensure directory exists
            var directory = Path.GetDirectoryName(localPath);
            if (!string.IsNullOrEmpty(directory))
            {
                Directory.CreateDirectory(directory);
            }

            // Download the file
            await _transferUtility.DownloadAsync(new TransferUtilityDownloadRequest
            {
                BucketName = _bucketName,
                Key = s3Key,
                FilePath = localPath
            });

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
            if (!File.Exists(localPath))
            {
                throw new FileNotFoundException($"Source file not found: {localPath}");
            }

            var fileInfo = new FileInfo(localPath);
            result.SizeBytes = fileInfo.Length;
            await _transferUtility.UploadAsync(new TransferUtilityUploadRequest
            {
                FilePath = localPath,
                BucketName = _bucketName,
                Key = s3Key
            });

            // Verify upload by getting metadata
            var metadata = await _s3Client.GetObjectMetadataAsync(_bucketName, s3Key);
            if (metadata.ContentLength != fileInfo.Length)
            {
                throw new Exception($"Uploaded file size ({metadata.ContentLength}) does not match source size ({fileInfo.Length})");
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

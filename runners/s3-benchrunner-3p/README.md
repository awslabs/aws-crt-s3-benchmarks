# s3-benchrunner-3p

Third-party S3 client benchmark runner. This runner supports various third-party S3 clients for benchmarking.

```
usage: main.py [-h] [--verbose] EXECUTABLE_PATH {s5cmd,rclone} WORKLOAD BUCKET REGION TARGET_THROUGHPUT

Third-party S3 client benchmark runner. Supports various third-party S3 clients.

positional arguments:
  EXECUTABLE_PATH       Path to the S3 client executable
  {s5cmd,rclone}        S3 client to use
  WORKLOAD
  BUCKET
  REGION
  TARGET_THROUGHPUT

optional arguments:
  -h, --help            show this help message and exit
  --verbose
```

## Supported Clients

### s5cmd

[s5cmd](https://github.com/peak/s5cmd) is a fast S3 client written in Go. s5cmd is designed for high-performance S3 operations and supports:
* Parallel uploads/downloads
* Wildcard support
* Pipes for streaming data
* High concurrency operations

See [installation instructions](#installation) before running.

### How this works with s5cmd

s5cmd is a popular S3 client supports S3 operations through:
- Built-in parallelism and concurrency
- Efficient memory usage
- Native Go performance
- Support for large files and many small files

This runner skips workloads that cannot be efficiently executed with s5cmd's command structure, similar to how the CLI runner works.

Here are examples showing how workloads are executed:

1) Single file upload/download:
   * workload: `upload-5GiB-1x`

   * cmd: `s5cmd cp upload/5GiB/1 s3://my-bucket/upload/5GiB/1`

2) Multiple files in same directory:
   * workload: `upload-5GiB-20x`

   * cmd: `s5cmd cp upload/5GiB/* s3://my-bucket/upload/5GiB/`

3) Streaming from/to memory (single file only):
   * workload: `upload-5GiB-1x-ram`

   * cmd: `<5GiB_random_data> | s5cmd cp - s3://my-bucket/upload/5GiB/1`

### rclone

[rclone](https://rclone.org/) is a powerful command-line program to manage files on cloud storage. rclone supports:
* Multiple cloud storage providers (including AWS S3)
* Parallel transfers
* Streaming support
* Advanced features like bandwidth limiting, checksums, and encryption

See [installation instructions](#installation) before running.

### How this works with rclone

rclone is a versatile cloud storage tool that supports S3 operations through:
- Configurable parallelism with `--transfers` flag
- Native S3 API support
- Efficient streaming for large files
- Support for both single files and directory operations

This runner skips workloads that cannot be efficiently executed with rclone's command structure, similar to how the CLI runner works.

Here are examples showing how workloads are executed:

1) Single file upload/download:
   * workload: `upload-5GiB-1x`

   * cmd: `rclone copy upload/5GiB/1 :s3:my-bucket/upload/5GiB/1`

2) Multiple files in same directory:
   * workload: `upload-5GiB-20x`

   * cmd: `rclone copy upload/5GiB :s3:my-bucket/upload/5GiB`

3) Streaming from/to memory (single file only):
   * workload: `upload-5GiB-1x-ram`

   * cmd: `<5GiB_random_data> | rclone copy - :s3:my-bucket/upload/5GiB/1`

# Installation

## s5cmd Installation

### Install via Go

```sh
# Install a specific released version (recommended for reproducibility)
go install github.com/peak/s5cmd/v2@v2.3.0
```

**Note:** When using `go install` , the binary will be in `$HOME/go/bin`

```sh
# Verify installation
~/go/bin/s5cmd version
```

### Configuration

s5cmd uses standard AWS credentials and configuration. Make sure you have:
- AWS credentials configured (via AWS CLI, environment variables, or IAM roles)
- Appropriate S3 permissions for the bucket you're testing against

**Note:** This benchmark configures concurrency dynamically based on target throughput using the formula: `concurrency = target_throughput_Gbps / 0.4` as CRT does. For example, for 100 Gbps target throughput, the concurrency is set to 250. This ensures Apple to Apple comparison.

## rclone Installation

### Install from Official Source

```sh
# Install the latest version
curl https://rclone.org/install.sh | sudo bash

# Or download a specific version from https://rclone.org/downloads/
```

### Install via Package Manager

```sh
# macOS (via Homebrew)
brew install rclone

# Amazon Linux 2023
sudo dnf install rclone

# Ubuntu/Debian
sudo apt install rclone
```

**Note:** After installation, the binary is typically in `/usr/bin/rclone` or `/usr/local/bin/rclone`

```sh
# Verify installation
rclone version
```

### Configuration

rclone uses standard AWS credentials and configuration. Make sure you have:
- AWS credentials configured (via AWS CLI, environment variables, or IAM roles)
- Appropriate S3 permissions for the bucket you're testing against

**rclone Config File:** The runner automatically creates a temporary rclone configuration file internally. No manual configuration is needed.

#### Config File Options

The runner creates a config file with the following settings (documented at https://rclone.org/s3/):

```ini
[remote]
type = s3                    # S3 backend type
provider = AWS               # Use AWS S3
env_auth = true             # Get credentials from environment
region = us-west-2          # AWS region (from REGION command-line argument)
no_check_bucket = true      # Don't check if bucket exists or try to create it
directory_bucket = true     # Enable S3 Express (automatically added for S3 Express buckets)
```

The region is set in the config file from the REGION command-line argument, ensuring rclone operates in the correct AWS region.

#### Command-Line Options

The runner automatically configures these rclone flags based on the workload:

1. **Parallel File Transfers** ([docs](https://rclone.org/docs/#transfers-n)):
   - `--transfers <n>`

   - Number of file transfers to run in parallel (important for multiple small files)
   - Formula: `concurrency = target_throughput_Gbps / 0.4`

   - Example: 100 Gbps → 250 parallel transfers

2. **Upload Concurrency** ([docs](https://rclone.org/s3/#s3-upload-concurrency)):
   - `--s3-upload-concurrency <n>`

   - Controls concurrent chunks for multipart uploads (for large files)
   - Formula: `concurrency = target_throughput_Gbps / 0.4`

   - Example: 100 Gbps → 250 concurrent operations

3. **Download Parallelism** ([docs](https://rclone.org/docs/#multi-thread-streams-int)):
   - `--multi-thread-streams <n>`

   - Controls parallel streams for downloads (for large files)
   - Formula: `concurrency = target_throughput_Gbps / 0.4`

   - Example: 100 Gbps → 250 parallel streams

4. **Always Transfer Files** ([docs](https://rclone.org/docs/#ignore-times)):
   - `--ignore-times`

   - Forces rclone to always transfer files, don't skip based on timestamps
   - Essential for benchmarking to ensure consistent measurements across runs

5. **Checksum Control** ([docs](https://rclone.org/s3/#s3-disable-checksum)):
   - `--s3-disable-checksum`

   - Automatically used when no checksum is specified in workload
   - Workloads requiring specific checksums will skip (rclone only supports MD5)

6. **S3 Express Support**:
   - Automatically detects S3 Express buckets (ending with `--x-s3` )
   - Adds `directory_bucket = true` to config file
   - See [S3 Directory Bucket documentation](https://rclone.org/s3/#s3-directory-bucket)

**Note:** This benchmark configures concurrency dynamically to ensure Apple to Apple comparison with other clients.

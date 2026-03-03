# s3-benchrunner-s5cmd

```
usage: main.py [-h] [--verbose] WORKLOAD BUCKET REGION TARGET_THROUGHPUT

s5cmd benchmark runner. Uses s5cmd for S3 operations.

positional arguments:
  WORKLOAD
  BUCKET
  REGION
  TARGET_THROUGHPUT

optional arguments:
  -h, --help            show this help message and exit
  --verbose
```

This is the runner for [s5cmd](https://github.com/peak/s5cmd), a fast S3 client written in Go. s5cmd is designed for high-performance S3 operations and supports:
* Parallel uploads/downloads
* Wildcard support
* Pipes for streaming data
* High concurrency operations

See [installation instructions](#installation) before running.

### How this works with s5cmd

s5cmd provides excellent performance for S3 operations through:
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

# Installation

## Quick install

### Option 1: Download prebuilt binary (Recommended)

Download the latest stable release from GitHub:

```sh
# Download the latest release for Linux x86_64
S5CMD_VERSION="2.3.0"  # Check https://github.com/peak/s5cmd/releases for latest
curl -L "https://github.com/peak/s5cmd/releases/download/v${S5CMD_VERSION}/s5cmd_${S5CMD_VERSION}_Linux-64bit.tar.gz" -o s5cmd.tar.gz

# Extract and install
tar -xzf s5cmd.tar.gz
sudo mv s5cmd /usr/local/bin/
rm s5cmd.tar.gz

# Verify installation
s5cmd version
```

### Option 2: Install via Go (specific version)

```sh
# Install a specific released version (recommended for reproducibility)
go install github.com/peak/s5cmd/v2@v2.3.0

# Or install the latest release (not development version)
go install github.com/peak/s5cmd/v2@latest
```

**Note:** When using `go install`, the binary will be in `$HOME/go/bin`. See PATH setup below.

Make sure s5cmd is in your PATH:
```sh
# Verify installation
s5cmd version
```

## Configuration

s5cmd uses standard AWS credentials and configuration. Make sure you have:
- AWS credentials configured (via AWS CLI, environment variables, or IAM roles)
- Appropriate S3 permissions for the bucket you're testing against

s5cmd supports various configuration options:
- Concurrency level (--numworkers)
- Part size for multipart uploads (--part-size)
- Memory usage limits

This runner will configure s5cmd optimally based on the target throughput and workload characteristics.

## Performance Notes

s5cmd is designed for high performance and can often outperform other S3 clients due to:
- Native Go implementation
- Efficient memory management
- Built-in parallelism
- Optimized for both small and large files

The runner will automatically tune concurrency and other parameters based on the specified target throughput.

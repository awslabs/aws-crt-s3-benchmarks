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

# Installation

## Quick install

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

## Configuration

s5cmd uses standard AWS credentials and configuration. Make sure you have:
- AWS credentials configured (via AWS CLI, environment variables, or IAM roles)
- Appropriate S3 permissions for the bucket you're testing against

**Note:** This benchmark configures concurrency dynamically based on target throughput using the formula: `concurrency = target_throughput_Gbps / 0.4` as CRT does. For example, for 100 Gbps target throughput, the concurrency is set to 250. This ensures Apple to Apple comparison.

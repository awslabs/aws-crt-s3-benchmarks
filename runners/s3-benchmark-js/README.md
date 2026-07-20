# s3-benchmark-js

Benchmark runner for the AWS SDK for JavaScript S3 Transfer Manager (`@aws-sdk/lib-transfer-manager`).

## Prerequisites

- Node.js v22+
- AWS credentials configured (e.g. via instance profile, environment variables, or `~/.aws/credentials`)

## Setup

Clone the benchmark repo and set up the runner:

```sh
cd ~
git clone https://github.com/awslabs/aws-crt-s3-benchmarks.git
cd aws-crt-s3-benchmarks/runners/s3-benchmark-js
Before running npm install copy the lib-transfer-manager tgz file into s3-benchmark-js
yarn install
```

## Preparing files

For file-based workloads (`"filesOnDisk": true`), create the source files before running. This will create source files uploading from disk at cd ~/files. Also to run with checksum change  "checksum": "CRC32" in .run.json file.

```sh
cd ~/aws-crt-s3-benchmarks
python3 scripts/prep-s3-files.py \
  --bucket my-bucket \
  --region us-west-2 \
  --files-dir ~/files \
```

Or create a file manually:

```sh
mkdir -p ~/files/upload/5GiB-10x
dd if=/dev/urandom of=~/files/upload/5GiB-10x/01 bs=1M count=5120
```

## Usage

```
node main.mjs [--source-file PATH] S3_CLIENT WORKLOAD BUCKET REGION
```

| Argument | Description |
|----------|-------------|
| `S3_CLIENT` | Client identifier (e.g. `sdk-js-tm`) |
| `WORKLOAD` | Path to a `.run.json` workload file these are in workloads folder|
| `BUCKET` | S3 bucket name |
| `REGION` | AWS region (e.g. `us-west-2`) |
| `--source-file PATH` | (Optional) Use a single file for all upload tasks |

## Running a benchmark

For file-based workloads, run from the directory containing the prepped files so that relative task keys resolve correctly:

```sh
cd ~/files
node ~/aws-crt-s3-benchmarks/runners/s3-benchmark-js/main.mjs \
  sdk-js-tm \
  ~/aws-crt-s3-benchmarks/workloads/upload-5GiB-10x.run.json \
  my-bucket \
  us-west-2
```

For in-memory workloads (`"filesOnDisk": false`), run from any directory — random data is generated automatically:

```sh
node ~/aws-crt-s3-benchmarks/runners/s3-benchmark-js/main.mjs \
  sdk-js-tm \
  ~/aws-crt-s3-benchmarks/workloads/upload-5GiB-1x-ram.run.json \
  my-bucket \
  us-west-2
```

## Running with CPU profiling

```sh
cd ~/aws-crt-s3-benchmarks/runners/s3-benchmark-js
./profile.sh sdk-js-tm ~/aws-crt-s3-benchmarks/workloads/upload-5GiB-10x.run.json my-bucket us-west-2
```

Profiles are saved to `./profiles/<timestamp>/` as `.cpuprofile` files.

To view: open Chrome → `chrome://inspect` → "Open dedicated DevTools for Node" → Performance tab → Load profile.

## Configuration

The Transfer Manager is configured in `main.mjs`:

| Setting | Value | Description |
|---------|-------|-------------|
| `workerThreadCount` | 64 | Number of worker threads for parallel part uploads |
| `maxConcurrentUploads` | 64 | Max parts in-flight across all workers |
| `targetPartSizeBytes` | 16 MiB | Multipart upload part size |
| `requestChecksumCalculation` | `WHEN_SUPPORTED` | Enables CRC32 checksums |

## How it works

- **File-based workloads** (`filesOnDisk: true`): The file path is passed as `Body` to the Transfer Manager. Worker threads read the file at specific offsets in parallel for multipart uploads.
- **In-memory workloads** (`filesOnDisk: false`): Random data is pre-generated in a `SharedArrayBuffer`. Workers take zero-copy views (no allocation per part) for uploads.

## Output

Results are printed to stdout and appended to `./results/<workload-name>.txt`:

```
Run:1 Secs:45.123456 Gb/s:9.876543
Run:2 Secs:43.210987 Gb/s:10.234567
```

The benchmark repeats until `maxRepeatCount` or `maxRepeatSecs` (from the workload file) is reached.

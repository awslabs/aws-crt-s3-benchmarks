# s3-benchrunner-js

```
usage: index.js S3_CLIENT WORKLOAD BUCKET REGION TARGET_THROUGHPUT [--verbose]
```

Benchmark runner for the [AWS SDK for JavaScript v3](https://github.com/aws/aws-sdk-js-v3).

Pass which client you want to benchmark as `S3_CLIENT`:

- `sdk-js`: Uses the [`@aws-sdk/lib-transfer-manager`](https://github.com/aws/aws-sdk-js-v3/tree/main/lib/lib-transfer-manager)
  `S3TransferManager` for both uploads and downloads, to measure transfer-manager
  performance.

## How this works

The AWS SDK for JavaScript is promise-based. Tasks in a workload are dispatched
concurrently from a fixed pool of workers — the JS analog of the Python boto3
runner's `ThreadPoolExecutor`. The pool size is bounded by `OBJECT_CONCURRENCY`
(see below) so large fan-out workloads don't exhaust memory.

- **Uploads** go through `S3TransferManager.upload`, which performs a multipart
  upload when the body exceeds the multipart threshold. This runner sets a low
  threshold (5 MiB, S3's minimum part size) so uploads exercise the transfer
  manager's multipart path. Bodies are read from disk (`filesOnDisk: true`) or
  from a pre-allocated in-memory buffer (`filesOnDisk: false`).
- **Downloads to disk** (`filesOnDisk: true`) go through
  `S3TransferManager.downloadToFile` (the file-based transfer-manager download
  path).
- **RAM-only downloads** (`filesOnDisk: false`) go through
  `S3TransferManager.downloadToNull`. Each response body is consumed, counted,
  and checksum-validated in a worker thread, then discarded without transferring
  body buffers to the main thread or writing a file. The runner verifies that
  `bytesRead` matches the workload's declared object size.

Note on `TARGET_THROUGHPUT`: unlike the CRT-based clients, the JS SDK does not
expose a "target throughput" knob that maps to internal connection count. The
argument is accepted for CLI compatibility with the harness but is not applied.

### Environment variables

- `MAX_CONCURRENCY`: max concurrent part requests per transfer. Default: number
  of vCPUs x 4.
- `OBJECT_CONCURRENCY`: max number of objects (tasks) transferred at once.
  Unset means unbounded (all tasks run concurrently). Set this to bound buffered
  memory on large fan-out workloads (e.g. `download-max-throughput` = 100 x 5 GiB).
- `useODirect=true`: on Linux, enables best-effort O_DIRECT writes for
  disk-backed downloads. The runner switches to RANGE downloads so subsequent
  8 MiB ranges have 4 KiB-aligned offsets. The first range and any final partial
  4 KiB block remain buffered. Unsupported filesystems log a warning and fall
  back to buffered writes.

## Installation

`@aws-sdk/lib-transfer-manager` is currently an alpha package that is **not
published to the public npm registry**, so this runner consumes it from a local
tarball (`aws-sdk-lib-transfer-manager-*.tgz`) referenced by `package.json` via a
`file:` dependency.

1. Clone and build `aws-sdk-js-v3`, then pack `lib-transfer-manager` into this
   directory:

   ```sh
   # from your aws-sdk-js-v3 checkout
   cd lib/lib-transfer-manager
   yarn && yarn build
   yarn pack --out /path/to/runners/s3-benchrunner-js/aws-sdk-lib-transfer-manager-<version>.tgz
   ```

2. Make sure the tarball filename in `package.json`'s
   `@aws-sdk/lib-transfer-manager` dependency matches the file you just packed.

3. Install:

   ```sh
   cd runners/s3-benchrunner-js
   yarn install
   ```

Once `@aws-sdk/lib-transfer-manager` is published, the `file:` dependency can be
replaced with a normal version range (`"0.x"`).

## Running

Run from the `FILES_DIR` you passed to `prep-s3-files.py` (task keys double as
on-disk relative paths):

```sh
cd FILES_DIR
node path/to/runners/s3-benchrunner-js/index.js sdk-js WORKLOAD.run.json BUCKET REGION TARGET_THROUGHPUT
```

Requires AWS credentials with read/write access to the bucket (resolved via the
default provider chain).

## Output

One line per run, matching the other runners so shared tooling can parse it:

```
Run:1 Secs:1.234567 Gb/s:12.345678
```

This is the format consumed by `scripts/utils/metrics.py`.

Peak RSS and average CPU are measured per run but are **not** printed to stdout;
they are written to the JSON results file. After all runs complete, a JSON
results file is written to this runner's own directory at
`runners/s3-benchrunner-js/results/<workload-name>/<timestamp>.json` (regardless
of the directory you launch from; override the base dir with the `RESULTS_DIR`
env var) containing every run's
`secs`/`gbps`/`peakRssBytes`/`cpuAvgCores`/`cpuAvgPercent`
plus a `summary` with:

- throughput: `minGbps`, `maxGbps`, `meanGbps`, `medianGbps`, and `peakGbps`
  (peak throughput = the best run's average Gb/s)
- `peakRssBytes` / `peakRssMiB`: peak RSS across all runs
- `avgCpuPercent` / `avgCpuCores`: CPU averaged across runs

A failure to write the results file is logged as a warning but does not fail the
benchmark.

Exit codes: `123` = workload skipped (e.g. unsupported version), `255` = error.

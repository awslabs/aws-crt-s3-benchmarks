## aws-crt-s3-benchmarks

This project is for benchmarking different S3 workloads using various languages and libraries.

## Running Benchmarks

### Requirements
*   To start:
    *   Python 3.9+ with pip
*   On Amazon Linux 2023, a script is provided to install further tools.
    Otherwise, depending on the language you want to benchmark, you'll need:
    *   CMake 3.22+
    *   C99 / C++20 compiler (e.g. gcc, clang)
    *   JDK17+ (e.g. corretto, openjdk)
    *   Maven
    *   Python C extension headers and libraries (e.g. python3-devel)

To benchmark **ALL** the workloads, your machine needs 300+ GiB of disk space available,
and fast enough internet to upload a terabyte to S3 within your lifetime.
But if you're only running 1 workload, you'll upload fewer files and use less disk space.

Your machine must have AWS credentials, with permission to read and write to an S3 bucket.

### Get Started

First, clone this repo.

Then install the [requirements](#requirements) listed above.
On Amazon Linux 2023, you can simply run this script:
```sh
./aws-crt-s3-benchmarks/scripts/install-tools-AL2023.py
```

Then, install packages needed by the python scripts:
```sh
python3 -m pip install -r aws-crt-s3-benchmarks/scripts/requirements.txt
```

### Prepare S3 Files

Next, run `prep-s3-files.py`. This script creates and configures
an S3 bucket, put files in S3 for benchmarks to download,
and create files on disk for benchmarks to upload:

```sh
./aws-crt-s3-benchmarks/scripts/prep-s3-files.py --bucket BUCKET --region REGION --files-dir FILES_DIR [--workload WORKLOAD]
```
*   `--bucket BUCKET`: S3 Bucket (created if necessary)
*   `--region REGION`: AWS region (e.g. us-west-2)
*   `--files-dir FILES_DIR`: Root directory for files to upload and download (e.g. ~/files) (created if necessary)
*   `--workload WORKLOAD`: Path to specific workload.run.json file.
        May be specified multiple times.
        If not specified, everything in [workloads/](workloads/) is prepared
        (uploading 100+ GiB to S3 and creating 100+ GiB on disk).

This script can be run repeatedly. It skips unnecessary work
(e.g. won't upload a file that already exists).

### Build a Runner

You must build a "runner" for the library you'll be benchmarking.
For example, [runners/s3-benchrunner-c](runners/s3-benchrunner-c/) tests the
[aws-c-s3](https://github.com/awslabs/aws-c-s3/) library.
See [runners/](runners/#readme) for more info.

Every runner comes a `build.py` script:
```sh
./aws-crt-s3-benchmarks/runners/RUNNER_X/scripts/build.py --build-dir BUILD_DIR
```
*   `--build-dir BUILD_DIR`: Root directory for build artifacts and git clones of dependencies
        (e.g. ~/build/RUNNER_X)

The last line of output from `build.py` displays the `RUNNER_CMD`
you'll need in the next step.

NOTE: Each runner has a `README.md` with more advanced instructions.
`build.py` isn't meant to handle advanced use cases like tweaking dependencies,
iterating locally, DEBUG builds, etc.

### Run a Benchmark

All runners have the same command line interface, and expect to be run from the
`FILES_DIR` you passed to the [prep-s3-files.py](#prepare-s3-files) script.

```sh
cd FILES_DIR

RUNNER_CMD WORKLOAD BUCKET REGION TARGET_THROUGHPUT
```

*   `RUNNER_CMD`: Command to launch runner (e.g. java -jar path/to/runner.jar)
        This is the last line printed by `build.py` in the [previous step](#build-a-runner).
*   `WORKLOAD`: Path to workload `.run.json` file (see: [workloads/](../workloads))
*   `BUCKET`: S3 bucket name (e.g. my-test-bucket)
*   `REGION`: AWS Region (e.g. us-west-2)
*   `TARGET_THROUGHPUT`: Target throughput, in gigabits per second.
        Floating point allowed. Enter the EC2 type's "Network Bandwidth (Gbps)"
        (e.g. "100.0" for [c5n.18xlarge](https://aws.amazon.com/ec2/instance-types/c5/))

Most runners should search for AWS credentials
[something like this](https://docs.aws.amazon.com/cli/latest/userguide/cli-chap-configure.html#configure-precedence).

If you want to run multiple workloads (or ALL workloads) in one go,
use this helper script: [run-benchmarks.py](scripts/run-benchmarks.py).

## Authoring New Workloads

See [workloads/](workloads/#readme)

## Security

See [CONTRIBUTING](CONTRIBUTING.md#security-issue-notifications) for more information.

## License

This project is licensed under the Apache-2.0 License.

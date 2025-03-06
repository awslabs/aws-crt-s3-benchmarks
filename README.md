## aws-crt-s3-benchmarks

This project is for benchmarking different S3 workloads using various languages and S3 clients.

This project is under active development and subject to change.

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

Next, run `scripts/prep-s3-files.py`. This script creates and configures
an S3 bucket, put files in S3 for benchmarks to download,
and create files on disk for benchmarks to upload:

```sh
usage: prep-s3-files.py [-h] --bucket BUCKET --region REGION --files-dir FILES_DIR
                        [--workloads WORKLOADS [WORKLOADS ...]]

Create files (on disk, and in S3 bucket) needed to run the benchmarks

optional arguments:
  -h, --help            show this help message and exit
  --bucket BUCKET       S3 bucket (will be created if necessary)
  --region REGION       AWS region (e.g. us-west-2)
  --files-dir FILES_DIR
                        Root directory for files to upload and download (e.g. ~/files)
  --workloads WORKLOADS [WORKLOADS ...]
                        Path to specific workload.run.json file. If not specified,
                        everything in workloads/ is prepared (uploading
                        100+ GiB to S3 and creating 100+ GiB on disk).
```

This script can be run repeatedly. It skips unnecessary work
(e.g. won't upload a file that already exists).

### S3 Clients

Here are the IDs used for various S3 Clients, and the runner you must build to benchmark them:

| S3_CLIENT | Actual S3 Client Used | Language | Benchmark Runner |
|-----------|-----------------------|------|------------------|
| `crt-c` | [aws-c-s3](https://github.com/awslabs/aws-c-s3) | `c` | [runners/s3-benchrunner-c](runners/s3-benchrunner-c/) |
| `crt-python` | [aws-crt-python](https://github.com/awslabs/aws-crt-python/) | `python` | [runners/s3-benchrunner-python](runners/s3-benchrunner-python/) |
| `boto3-crt` | [boto3](https://github.com/boto/boto3/) using CRT | `python` | [runners/s3-benchrunner-python](runners/s3-benchrunner-python/) |
| `boto3-classic` | [boto3](https://github.com/boto/boto3/) with pure-python transfer manager | `python` | [runners/s3-benchrunner-python](runners/s3-benchrunner-python/) |
| `cli-crt` | [AWS CLI v2](https://github.com/aws/aws-cli/tree/v2) using CRT | `python` | [runners/s3-benchrunner-python](runners/s3-benchrunner-python/) |
| `cli-classic` | [AWS CLI v2](https://github.com/aws/aws-cli/tree/v2) with pure-python transfer manager | `python` | [runners/s3-benchrunner-python](runners/s3-benchrunner-python/) |
| `crt-java` | [aws-crt-java](https://github.com/awslabs/aws-crt-java/) | `java` | [runners/s3-benchrunner-java](runners/s3-benchrunner-java/) |
| `sdk-java-client-crt` | [aws-sdk-java-v2](https://github.com/aws/aws-sdk-java-v2/) with CRT based S3AsyncClient | `java` | [runners/s3-benchrunner-java](runners/s3-benchrunner-java/) |
| `sdk-java-client-classic` | [aws-sdk-java-v2](https://github.com/aws/aws-sdk-java-v2/) with pure-java S3AsyncClient | `java` | [runners/s3-benchrunner-java](runners/s3-benchrunner-java/) |
| `sdk-java-tm-crt` | [aws-sdk-java-v2](https://github.com/aws/aws-sdk-java-v2/) with CRT based S3TransferManager | `java` | [runners/s3-benchrunner-java](runners/s3-benchrunner-java/) |
| `sdk-java-tm-classic` | [aws-sdk-java-v2](https://github.com/aws/aws-sdk-java-v2/) with pure-java S3TransferManager | `java` | [runners/s3-benchrunner-java](runners/s3-benchrunner-java/) |
| `sdk-cpp-client-crt` | [aws-sdk-cpp](https://github.com/aws/aws-sdk-cpp) with S3CrtClient | `cpp` | [runners/s3-benchrunner-cpp](runners/s3-benchrunner-cpp/) |
| `sdk-cpp-client-classic` | [aws-sdk-cpp](https://github.com/aws/aws-sdk-cpp) with (non-CRT) S3Client | `cpp` | [runners/s3-benchrunner-cpp](runners/s3-benchrunner-cpp/) |
| `sdk-cpp-tm-classic` | [aws-sdk-cpp](https://github.com/aws/aws-sdk-cpp) with (non-CRT) TransferManager | `cpp` | [runners/s3-benchrunner-cpp](runners/s3-benchrunner-cpp/) |
| `sdk-rust-tm` | [aws-s3-transfer-manager-rs](https://github.com/awslabs/aws-s3-transfer-manager-rs/) | `rust` | [runners/s3-benchrunner-rust](runners/s3-benchrunner-rust/) |

### Build a Runner

You must build a "runner" for the S3 client you'll be benchmarking. For example, build [runners/s3-benchrunner-python](runners/s3-benchrunner-python/) to benchmark aws-crt-python, boto3, or AWS CLI.

Run `scripts/build-runner.py`:
```sh
usage: build-runner.py [-h] --lang {c,python,java} --build-dir BUILD_DIR [--branch BRANCH]

Build a runner and its dependencies

optional arguments:
  -h, --help            show this help message and exit
  --lang {c,python,java}
                        Build s3-benchrunner-<lang>
  --build-dir BUILD_DIR
                        Root dir for build artifacts
  --branch BRANCH       Git branch/commit/tag to use when pulling dependencies
```

The last line of output from `build-runner.py` displays the `RUNNER_CMD`
you'll need in the next step.

NOTE: Each runner has a `README.md` with more advanced instructions.
`build-runner.py` isn't meant to handle advanced use cases like tweaking dependencies,
iterating locally, DEBUG builds, etc.

### Run a Benchmark

All runners have the same command line interface, and expect to be run from the
`FILES_DIR` you passed to the [prep-s3-files.py](#prepare-s3-files) script.

```sh
cd FILES_DIR

RUNNER_CMD S3_CLIENT WORKLOAD BUCKET REGION TARGET_THROUGHPUT [NETWORK_INTERFACE_NAMES]
```

*   `S3_CLIENT`: ID of S3 client to use (See [table](#s3-clients) above)
*   `RUNNER_CMD`: Command to launch runner (e.g. java -jar path/to/runner.jar)
        This is the last line printed by `build-runner.py` in the [previous step](#build-a-runner).
*   `WORKLOAD`: Path to workload `.run.json` file (see: [workloads/](../workloads))
*   `BUCKET`: S3 bucket name (e.g. my-test-bucket)
*   `REGION`: AWS Region (e.g. us-west-2)
*   `TARGET_THROUGHPUT`: Target throughput, in gigabits per second.
        Floating point allowed. Enter the EC2 type's "Network Bandwidth (Gbps)"
        (e.g. "100.0" for [c5n.18xlarge](https://aws.amazon.com/ec2/instance-types/c5/))
*   `NETWORK_INTERFACE_NAMES`: **This is optionally supported for crt-c Runner**
        A comma separated list of network interface names without any spaces like "ens5,ens6"

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

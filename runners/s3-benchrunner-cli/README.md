# s3-benchrunner-cli

```
usage: benchrunner.py [-h] [--verbose] [--use-existing-aws-config] BENCHMARK BUCKET REGION TARGET_THROUGHPUT

Benchmark runner for AWS CLI

positional arguments:
  BENCHMARK
  BUCKET
  REGION
  TARGET_THROUGHPUT

optional arguments:
  -h, --help            show this help message and exit
  --verbose             Show CLI commands and their output
  --use-existing-aws-config
                        If set, your existing AWS_CONFIG_FILE is used. (instead of one that customizes
                        'preferred_transfer_client')
```

This runner uses your existing `aws` CLI installation.
If you want to build/install the CLI from Github, see [instructions below](#building-locally).

This runner skips benchmarks unless it can do them in a single AWS CLI command.
We do this because it wouldn't be fair comparing CLI commands issued one
after the other, against other runners that do multiple commands in parallel.

Here are examples, showing how a given benchmark is run in a single CLI command:

1) Uploading or downloading a single file is simple:
    * benchmark: `upload-5GiB`
    * cmd: `aws s3 cp upload/5GiB/1 s3://my-s3-benchmarks/upload/5GiB/1`

2) A benchmark with multiple files only works if they're in the same directory:
    * benchmark: `upload-5GiB-20x`
    * cmd: `aws s3 cp upload/5GiB s3://my-s3-benchmarks/upload/5GiB --recursive`

3) If the benchmark doesn't use every file in the directory, then we `--include` the ones we want:
    * benchmark: `upload-5GiB-10x`
    * cmd: `aws s3 cp upload/5GiB s3://my-s3-benchmarks/upload/5GiB --recursive --exclude "*" --include 1 --include 2 --include 3 --include 4 --include 5 --include 6 --include 7 --include 8 --include 9 --include 10`

4) If the benchmark has `"filesOnDisk": false` then we upload from stdin, or download to stdout. This only works if the benchmark has 1 file.
    * benchmark: `upload-5GiB-ram`
    * cmd: `<5GiB_random_data> | aws s3 cp - s3://my-s3-benchmarks/upload/5GiB/1`

## Building locally

Here are instructions to use a locally built AWS CLI.

First, create a virtual environment, to isolate your dev versions from system defaults:
```sh
python3 -m venv .venv
```

Now we'll use python in the virtual environment...
Install some dependencies...
```
.venv/bin/python3 -m pip install --upgrade pip boto3
```

Next, pull the AWS CLI source code and install it in your virtual environment
(`--editable` so we can modify its source without reinstalling):
```sh
git clone --branch v2 https://github.com/aws/aws-cli.git
.venv/bin/python3 -m pip install --editable aws-cli
```

And if you want the latest aws-crt-python, pull it and install that too:
```sh
git clone --recurse-submodules https://github.com/awslabs/aws-crt-python.git
.venv/bin/python3 -m pip install --editable aws-crt-python
```
pip complains that the newly installed 1.0.0.dev0 clashes
with the version requirements from awscli, but we ignore this.

Now, you can execute the runner using your virtual environment with the latest CLI and CRT:
```sh
.venv/bin/python3 path/to/aws-crt-s3-benchmarks/runners/s3-benchrunner-cli/benchrunner.py --help
```

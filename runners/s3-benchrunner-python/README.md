# s3-benchrunner-python

```
usage: main.py [-h] [--verbose] {crt-python,boto3-classic,boto3-crt,cli-classic,cli-crt} WORKLOAD BUCKET REGION TARGET_THROUGHPUT

Python benchmark runner. Pick which S3 library to use.

positional arguments:
  {crt-python,boto3-classic,boto3-crt,cli-classic,cli-crt}
  WORKLOAD
  BUCKET
  REGION
  TARGET_THROUGHPUT

optional arguments:
  -h, --help            show this help message and exit
  --verbose
```

This is the runner for python libraries. Pass which library you want to benchmark:
* `crt-python`: Uses the [aws-crt-python](https://github.com/awslabs/aws-crt-python/) (the CRT bindings for python) directly.
* `boto3-classic`: Uses [boto3](https://github.com/boto/boto3), with pure-python transfer manager.
* `boto3-crt`: Uses boto3, with CRT transfer manager.
* `cli-classic`: Uses [AWS CLI](https://github.com/aws/aws-cli/), with pure-python transfer manager.
* `cli-crt`: Uses AWS CLI, with CRT transfer manager.

See [installation instructions](#installation) before running.

### How this works with aws-crt-python

When using aws-crt-python (async API), all tasks are kicked off from the main thread,
then it waits for all tasks to finish. The CRT maintains its own thread pool
(EventLoopGroup) where the actual work is done.

### How this works with boto3

When using boto3 (synchronous API), a [ThreadPoolExecutor](https://docs.python.org/3/library/concurrent.futures.html#threadpoolexecutor)
is used to run all tasks. This is significantly faster than running tasks one
after another on the main thread. I'm not sure if this is how Python programmers
would naturally do things, but it's simple enough to recommend given the huge payoff.

### How this works with AWS CLI

When using AWS CLI, this runner skips workloads unless it can do them in a single command.
If we used multiple commands, one after another, performance would look bad
compared to other libraries that run multiple commands in parallel.
That's not a fair comparison (no one runs CLI commands in parallel) so we skip those workloads.

Here are examples, showing how a given workload is run in a single CLI command:

1) Uploading or downloading a single file is simple:
    * workload: `upload-5GiB`
    * cmd: `aws s3 cp upload/5GiB/1 s3://my-s3-benchmarks/upload/5GiB/1`

2) A workload with multiple files only works if they're in the same directory
   (and no other files exist in that directory):
    * workload: `upload-5GiB-20x`
    * cmd: `aws s3 cp upload/5GiB s3://my-s3-benchmarks/upload/5GiB --recursive`

3) If the workload has `"filesOnDisk": false` then we upload from stdin, or download to stdout. This only works if the workload has 1 file.
    * workload: `upload-5GiB-ram`
    * cmd: `<5GiB_random_data> | aws s3 cp - s3://my-s3-benchmarks/upload/5GiB/1`

# Installation

## Quick install

To test against the most recent public releases of these libraries:

First, install the CLI if you don't already have it:
https://docs.aws.amazon.com/cli/latest/userguide/getting-started-install.html

Then install boto3 with CRT:
```sh
python3 -m pip install --upgrade "boto3[crt]"
```

## Building locally

To test against the bleeding edge, install these libraries from source.

First, create a virtual environment, to isolate your dev versions from system defaults:
```sh
python3 -m venv .venv
source .venv/bin/activate
(.venv) pip install --upgrade pip wheel
```

Now make a build dir somewhere.
You're going to pull the source code for dependencies and install them...
```
(.venv) cd path/to/my/build/dir
```

First, AWS CLI (`--editable` so you can modify its source without reinstalling):
```sh
(.venv) git clone --branch v2 https://github.com/aws/aws-cli.git
(.venv) python3 -m pip install --editable aws-cli
```

Next boto3:
```sh
(.venv) git clone https://github.com/boto/boto3.git
(.venv) python3 -m pip install --editable boto3
```

Next s3transfer:
```sh
(.venv) git clone https://github.com/boto/s3transfer.git
(.venv) python3 -m pip install --editable s3transfer
```

Next botocore:
```sh
(.venv) git clone https://github.com/boto/botocore.git
(.venv) python3 -m pip install --editable botocore
```

Finally aws-crt-python:
```sh
(.venv) git clone --recurse-submodules https://github.com/awslabs/aws-crt-python.git
(.venv) python3 -m pip install --editable aws-crt-python
```
pip complains that the newly installed 1.0.0.dev0 clashes
with the version requirements from other packages, but you can ignore this.

Now, you can execute the runner using your virtual environment with all the latest code:
```sh
(.venv) python3 path/to/aws-crt-s3-benchmarks/runners/s3-benchrunner-python/main.py --help
```

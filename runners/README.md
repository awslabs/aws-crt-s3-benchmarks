# Benchmark Runners

A runner must be built for each library we want to test.
For example, [s3-benchrunner-c](s3-benchrunner-c/) tests the
[aws-c-s3](https://github.com/awslabs/aws-c-s3/) library.

# Building

Each runner has scripts to get you up and running on Amazon Linux 2023.

First install necessary tools:

`./<runner-dir>/scripts/install.py`

Then build:

`./<runner-dir>/scripts/build.py --build-dir <build-dir>`

Where `<build-dir>`is a temp directory for script to use.
The script may `git clone` dependencies into this location.

The last line of output from build.py displays the `runner-cmd` to use [below](#running).

NOTE: Each runner has a `README.md` with more advanced instructions.
`build.py` isn't meant to handle advanced use cases like tweaking dependencies,
iterating locally, DEBUG builds, etc.

# Running

All runners have the same command line interface:

`runner-cmd <benchmark.json> <bucket> <region> <target-throughput-Gbps>`

*   `runner-cmd`: Command to launch runner (e.g. `java -jar target/s3-benchrunner.jar`)
*   `<benchmark.json>`: Path to benchmark JSON file (see: [benchmarks/](../benchmarks))
*   `<bucket>`: S3 bucket name (e.g. "my-test-bucket")
*   `<region>`: AWS Region (e.g. "us-west-2")
*   `<target-throughput-Gbps>`: Target throughput, in gigabits per second.
        Floating point allowed. Enter the EC2 type's "Network Bandwidth (Gbps)"
        (e.g. "100.0" for [c5n.18xlarge](https://aws.amazon.com/ec2/instance-types/c5/))

Your machine must have AWS credentials for accessing the bucket.
Most runners should search for AWS credentials
[something like this](https://docs.aws.amazon.com/cli/latest/userguide/cli-chap-configure.html#configure-precedence).


# S3 Benchmarks CDK

CDK Python project that sets up infrastructure to automatically
run the S3 benchmarks on a variety of EC2 instance types.

## Deploying to your AWS Account

First, install CDK and bootstrap your account if you haven't already done so. See
[AWS CDK Prerequisites](https://docs.aws.amazon.com/cdk/v2/guide/work-with.html#work-with-prerequisites)

Then create and activate a Python virtual environment, if you haven't already done so.

Then cd into this directory.

Then install the CDK's python requirements:
```sh
python3 -m pip install -r requirements.txt
```

Then deploy this CDK app:
```sh
cdk deploy
```

## Architecture

The requirements driving this architecture are:
*   Run the benchmarks on many different EC2 instance types.
*   Only 1 benchmark at a time should be running, across your whole AWS account.
    *   S3 will start raising 503 Slow Down errors when there are too many
        requests per second for a given account / bucket.
        So running benchmarks on multiple machines simultaneously
        would lead to more 503 errors and give worse results
        than benchmarking 1 machine at a time.
*   The benchmarks take a long time.
*   EC2 instances should not be running 24/7.
    *   Some types are very expensive, so they should spin down when they're not in use.

### AWS Batch

We chose [AWS Batch](https://aws.amazon.com/batch/) to run the benchmarking jobs,
since it's a service that's all about processing queues of long-running jobs,
using specific instance types, and it can spin them down when not in use.
Batch seems like a pretty thin wrapper around ECS.

Downsides are:
*   It doesn't offer EVERY instance type.
    *   Specifically, some older and weaker types are not available,
        but we're mostly interested in bleeding edge and powerful,
        so hopefully it works out.
*   You must use containers.
    *   Not sure if this will be an issue?

Possible Alternatives:
*   A script that manually starts and stops EC2 instances.
*   Use ECS directly, instead of via Batch.

### Per-Instance Job

This is what we call a Batch job that uses a specific EC2 instance type
to build and run all desired [Runners](../runners/#readme),
on all desired [workloads](../workloads/#readme).

### Orchestrator

The Orchestrator is a Batch job that kicks off each Per-Instance Job,
waits for it to complete, then kicks off the next.

We tried to use event-driven stuff like Lambda and Event Bridge and
Step Functions to orchestrate running 1 thing at a time,
but it was a huge pain. Since we're already using AWS Batch
to run the benchmarks, we may as well use it to orchestrate,
using good old fashioned code.

The cheapest possible machine should be used to run this job.
It's mostly just sitting around waiting.

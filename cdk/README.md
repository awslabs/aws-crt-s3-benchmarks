
# S3 Benchmarks CDK

CDK Python project that sets up infrastructure to automatically run the S3 benchmarks on a variety of EC2 instance types.

## Deploying to your AWS Account

1) Install CDK and bootstrap your account if you haven't already done so. See [AWS CDK Prerequisites](https://docs.aws.amazon.com/cdk/v2/guide/work-with.html#work-with-prerequisites)

1) Ensure the Docker daemon is running.

1) Create and activate a Python virtual environment, if you haven't already done so.

1) Install python requirements:
    ```sh
    python3 -m pip install -r aws-crt-s3-benchmarks/scripts/requirements.txt
    ```

1) `cd` into this `cdk/` directory.

1) Check that your service quotas are high enough by running:
    ```sh
    ./check-service-quotas.py --region REGION
    ```
    These service quotas refer to the max running vCPU count, for categories of EC2 instance types, allowed for your account. If the script tells you to increase a quota, you probably should, unless you're certain you don't want to run benchmarks on instance types like that.

1) Create a settings file for the account you'll be deploying to. Name it something like "myname.settings.json". It should look like:
    ```json
    {
        "account": "012345678901",
        "region": "us-west-2",
        "bucket": "my-benchmarking-bucket"
    }
    ```
    Fields are:
    * `account`: AWS account ID
    * `region`: AWS region
    * `bucket`: (Optional) If you want to use a pre-existing bucket, or you want the bucket to persist when stack is destroyed, pass its name here. If you omit this field, or set the value `""` or `null`, a bucket will be created that gets destroyed when the stack is destroyed.

1) Deploy this CDK app, passing in your settings file:
    ```sh
    cdk deploy -c settings=<myname.settings.json>
    ```

## Troubleshooting

If your Batch job is stuck in the RUNNABLE state forever, use the
[AWSSupport-TroubleshootAWSBatchJob](https://console.aws.amazon.com/systems-manager/documents/AWSSupport-TroubleshootAWSBatchJob/description) to find out why. Some reasons we've encountered...
* The Batch Compute Environment was created with 1 vCPU (it must be a multiple of the vCPU an Instance Type natively has)
* The Batch Job was created with memory equal to what the Instance Type natively has (not all that memory is available to container jobs).
* Insufficient service quotas for EC2 types (see `check-service-quotas.py`)
* EC2 instance type unavailable (for rare EC2 types)

## Architecture

The requirements driving this architecture are:

* Run the benchmarks on many different EC2 instance types.
* Only one benchmark at a time should be running.
    * Preparing the S3 bucket is slow, so we want it to be persistent and re-use it.
    * S3 can raise 503 Slow Down errors when a bucket is being hammered (it's nuanced, [see here](https://docs.aws.amazon.com/AmazonS3/latest/userguide/optimizing-performance.html)). So multiple machines running benchmarks simultaneously could give worse results than benchmarking one machine at a time.
* The benchmarks take a long time.
* Expensive EC2 instance types should not be running 24/7.
    * They should spin down when they're not in use.

### AWS Batch

We chose [AWS Batch](https://aws.amazon.com/batch/) to run the benchmarking jobs, since it's a service that's all about processing queues of long-running jobs, using specific instance types, and it can spin them down when not in use. Batch seems like a pretty thin wrapper around ECS.

Downsides are:
* It doesn't offer EVERY instance type.
    * Specifically, some older and weaker types are not available, but we're mostly interested in bleeding edge and powerful, so hopefully it works out.
* You must use containers.
    * Not sure if this will be an issue?
* We end up with multiple queues.
    * If you want a specific EC2 type to run your job, you end up with a separate job queue for each "Batch Compute Environment".
    * Now we need an "orchestrator" to ensure only one queue at a time has a job in it.

Possible Alternatives:
* A script that manually starts and stops EC2 instances.
* Use ECS directly, instead of via Batch.

### Per-Instance Job

This is what we call a Batch job that uses a specific EC2 instance type, builds [runners](../runners/#readme), and benchmarks all desired [workloads](../workloads/#readme), using all desired S3 clients.

This job runs [per-instance-job.py](per-instance-job.py), which git clones the `aws-crt-s3-benchmarks` repo, then runs the [prep-build-run-benchmarks.py](../scripts/prep-build-run-benchmarks.py) script within that repo, which handles the rest. The reason that the per-instance-job clones the repo at runtime, instead of embedding at CDK deploy time, is to let us test different branches of `aws-crt-s3-benchmarks` without redeploying the CDK stack.

### Orchestrator

The Orchestrator is a Batch job that kicks off each Per-Instance Job, waits for it to complete, then kicks off the next.

Since we're already using AWS Batch to run the benchmarks, we may as well use it to orchestrate, using good old fashioned code. The cheapest possible machine should be used to run this job. It's mostly just sitting around waiting.

Possible Alternatives:
* [Step Functions](https://aws.amazon.com/step-functions/): @graebm explored using event-driven state machines, but he's not a pro at this, and felt like the design he came up with felt overly complex for an internal tool:
    * An SQS FIFO queue of benchmarking requests.
    * Step Functions state machine processes one request at a time from the queue, running the appropriate Batch Job.
    * But SQS doesn't easily integrate with long-running state machines, and state machines aren't supposed to run in a loop forever, so...
    * We'd need a Lambda to launch the state machine, triggered whenever something was put in the queue.
    * But a queue that triggers a Lambda automatically pops the request from the queue, so we'd need to move the request to a second queue that the state machine processes.
    * But now we're running the state machine immediately when something is added to the queue. To prevent overlapping work, each state machine instance needs to wait until all other instances are finished. Or, each state machine instance runs a loop: exiting if it can't immediately process a request. So if multiple requests result in multiple state machines, the 1st one ends up processing all the requests. Subsequent state machines exited early when they saw that someone else was already working on it.

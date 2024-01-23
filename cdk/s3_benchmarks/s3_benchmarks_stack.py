import aws_cdk as cdk
from aws_cdk import (
    CfnOutput,
    Stack,
    aws_batch as batch,
    aws_ec2 as ec2,
    aws_ecr_assets as ecr_assets,
    aws_ecs as ecs,
    aws_events as events,
    aws_events_targets as events_targets,
    aws_iam as iam,
    aws_s3 as s3,
)
from constructs import Construct
from math import floor
import subprocess
from typing import Optional

import s3_benchmarks

# The "default" set of instance types to benchmark.
# This, and the other defaults below, serve several purposes:
# - When submitting a job via the console, these are the default values.
# - These defaults are what the Canary runs.
# - (TODO) A dashboard is set up to view these instance-type/s3-client/workload combinations.
DEFAULT_INSTANCE_TYPES = [
    'c5n.18xlarge',
]

# The "default" set of S3 clients to benchmark.
# For now, only have the Canary test the CRT-based clients.
DEFAULT_S3_CLIENTS = [
    'crt-c',
    'crt-java',
    'crt-python',
    'cli-crt',
    'boto3-crt',
]

# The "default" set of workloads to benchmark.
# This isn't everything in workloads/, it's a reasonable subset
# of use cases that won't take TOO long to run.
DEFAULT_WORKLOADS = [
    'download-max-throughput',  # how fast can we theoretically go?
    'upload-max-throughput',
    'download-30GiB-1x',  # very big file
    'upload-30GiB-1x',
    'download-5GiB-1x',  # moderately big file
    'upload-5GiB-1x',
    'download-5GiB-1x-ram',  # no disk access to slow us down
    'upload-5GiB-1x-ram',
    'download-256KiB-10_000x',  # lots of small files
    'upload-256KiB-10_000x',
]

PER_INSTANCE_STORAGE_GiB = 500


class S3BenchmarksStack(Stack):

    def __init__(self, scope: Construct, construct_id: str,
                 existing_bucket_name: Optional[str],
                 add_canary: bool,
                 **kwargs):
        super().__init__(scope, construct_id, **kwargs)

        # If existing bucket specified, use it.
        # Otherwise, create one that will be destroyed when stack is destroyed.
        if existing_bucket_name:
            self.bucket = s3.Bucket.from_bucket_name(
                self, "Bucket", existing_bucket_name)
        else:
            self.bucket = s3.Bucket(
                self, "Bucket",
                auto_delete_objects=True,
                removal_policy=cdk.RemovalPolicy.DESTROY,
            )
            # note: lifecycle rules for this bucket will be set later,
            # by prep-s3-files.py, which runs as part of the per-instance job

        self.vpc = ec2.Vpc(
            self, "Vpc",
            # Add gateway endpoint for S3.
            # Otherwise, it costs thousands of dollars to naively send terabytes
            # of S3 traffic through the default NAT gateway (ask me how I know).
            gateway_endpoints={"S3": ec2.GatewayVpcEndpointOptions(
                service=ec2.GatewayVpcEndpointAwsService.S3)},
        )

        self._define_all_per_instance_batch_jobs()

        self._define_orchestrator_batch_job()

        self._add_git_commit_cfn_output()

        if add_canary:
            self._add_canary()

    def _define_all_per_instance_batch_jobs(self):
        # First, create resources shared by all per-instance jobs...

        # Create role for the per-instance job scripts that actually run the benchmarks.
        # Every AWS call you add to these scripts will fail until you add a policy that allows it.
        self.per_instance_job_role = iam.Role(
            self, "PerInstanceJobRole",
            assumed_by=iam.ServicePrincipal("ecs-tasks.amazonaws.com"),
            max_session_duration=cdk.Duration.hours(
                s3_benchmarks.PER_INSTANCE_JOB_TIMEOUT_HOURS),
        )
        # per-instance-job can do whatever it wants to the bucket
        self.per_instance_job_role.add_to_policy(iam.PolicyStatement(
            actions=["s3:*"],
            resources=[self.bucket.bucket_arn,
                       f"{self.bucket.bucket_arn}/*"],
            effect=iam.Effect.ALLOW,
        ))
        # job reports metrics to CloudWatch
        self.per_instance_job_role.add_to_policy(iam.PolicyStatement(
            actions=["cloudwatch:PutMetricData"],
            # CloudWatch requires "*" for resources, but you can add conditions
            # https://docs.aws.amazon.com/service-authorization/latest/reference/list_amazoncloudwatch.html
            resources=["*"],
            conditions={
                "StringEquals": {"cloudwatch:namespace": "S3Benchmarks"},
            },
            effect=iam.Effect.ALLOW,
        ))

        # Per-instance jobs needs more than the default 30GiB storage.
        # Use a "launch template" to customize this, see:
        # https://docs.aws.amazon.com/batch/latest/userguide/launch-templates.html
        self.per_instance_launch_template = ec2.LaunchTemplate(
            self, f"PerInstanceLaunchTemplate",
            block_devices=[ec2.BlockDevice(
                device_name='/dev/xvda',
                volume=ec2.BlockDeviceVolume.ebs(
                    volume_size=PER_INSTANCE_STORAGE_GiB,
                    volume_type=ec2.EbsDeviceVolumeType.GP3,
                ),
            )],
        )

        # Now create the actual jobs...
        for instance_type in s3_benchmarks.ALL_INSTANCE_TYPES:
            self._define_per_instance_batch_job(instance_type)

    def _define_per_instance_batch_job(self, instance_type: s3_benchmarks.InstanceType):
        # "c5n.18xlarge" -> "c5n-18xlarge"
        id_with_hyphens = instance_type.id.replace('.', '-')

        ec2_instance_type = ec2.InstanceType(instance_type.id)

        compute_env = batch.ManagedEc2EcsComputeEnvironment(
            self, f"PerInstanceComputeEnv-{id_with_hyphens}",
            # scale down to 0 when there's no work
            minv_cpus=0,
            # run 1 job at a time by limiting to num vcpus available on instance type
            maxv_cpus=instance_type.vcpu,
            instance_types=[ec2_instance_type],
            # prevent CDK from adding 'optimal' instance type, we only want to one type specified above
            use_optimal_instance_classes=False,
            launch_template=self.per_instance_launch_template,
            vpc=self.vpc,
            vpc_subnets=ec2.SubnetSelection(
                subnet_type=ec2.SubnetType.PRIVATE_WITH_EGRESS),
        )

        job_queue = batch.JobQueue(
            self, f"PerInstanceJobQueue-{id_with_hyphens}",
            # specify name so orchestrator script can easily reference it
            job_queue_name=instance_type.resource_name(),
            compute_environments=[batch.OrderedComputeEnvironment(
                compute_environment=compute_env, order=0)],
        )

        container_defn = batch.EcsEc2ContainerDefinition(
            self, f"PerInstanceContainerDefn-{id_with_hyphens}",
            image=ecs.ContainerImage.from_asset(
                directory='.',
                file='per-instance-job.Dockerfile',
                platform=_ec2_instance_type_to_ecr_platform(ec2_instance_type)),
            cpu=instance_type.vcpu,
            memory=_max_container_memory(
                cdk.Size.gibibytes(instance_type.mem_GiB)),
            command=[
                "python3", "/per-instance-job.py",
                "--bucket", self.bucket.bucket_name,
                "--region", self.region,
                "--branch", "Ref::branch",
                "--instance-type", instance_type.id,
                "--s3-clients", "Ref::s3Clients",
                "--workloads", "Ref::workloads",
            ],
            job_role=self.per_instance_job_role,
        )

        job_defn = batch.EcsJobDefinition(
            self, f"PerInstanceJobDefn-{id_with_hyphens}",
            # specify name so orchestrator script can easily reference it
            job_definition_name=instance_type.resource_name(),
            container=container_defn,
            timeout=cdk.Duration.hours(
                s3_benchmarks.PER_INSTANCE_JOB_TIMEOUT_HOURS),
            parameters={
                "branch": "main",
                "s3Clients": ','.join(DEFAULT_S3_CLIENTS),
                "workloads": ','.join(DEFAULT_WORKLOADS),
            },
        )

    def _define_orchestrator_batch_job(self):
        """
        Set up AWS Batch job that orchestrates running benchmarks
        on 1 or more EC2 instance types.
        """

        # - WARNING: instance type's vCPUs number..
        #       - MUST match compute environment's `maxv_cpus` (or jobs get stuck in RUNNABLE state).
        #       - MUST match job definition's `cpu` (to ensure 1 job runs at a time).
        instance_type = s3_benchmarks.ORCHESTRATOR_INSTANCE_TYPE
        ec2_instance_type = ec2.InstanceType(instance_type.id)

        self.orchestrator_compute_env = batch.ManagedEc2EcsComputeEnvironment(
            self, "OrchestratorComputeEnv",
            # scale down to 0 when there's no work
            minv_cpus=0,
            # run 1 job at a time by limiting to num vcpus available on instance type
            maxv_cpus=instance_type.vcpu,
            instance_types=[ec2_instance_type],
            # don't add 'optimal' instance type
            use_optimal_instance_classes=False,
            vpc=self.vpc,
            vpc_subnets=ec2.SubnetSelection(
                subnet_type=ec2.SubnetType.PRIVATE_WITH_EGRESS),
        )

        self.orchestrator_job_queue = batch.JobQueue(
            self, "OrchestratorJobQueue",
            compute_environments=[batch.OrderedComputeEnvironment(
                compute_environment=self.orchestrator_compute_env, order=0)],
        )

        # Set up role for the orchestrator-job.py script.
        # Every AWS call you add to this script will fail until you add a policy that allows it.
        self.orchestrator_job_role = iam.Role(
            self, "OrchestratorJobRole",
            assumed_by=iam.ServicePrincipal("ecs-tasks.amazonaws.com"),
            max_session_duration=cdk.Duration.hours(
                s3_benchmarks.ORCHESTRATOR_JOB_TIMEOUT_HOURS),
        )
        self.orchestrator_job_role.add_to_policy(iam.PolicyStatement(
            actions=["batch:SubmitJob"],
            # "*" at the end necessary so orchestrator-job.py can submit job
            # by its hard-coded name, like "S3Benchmarks-PerInstance-c5n.18xlarge".
            # The resolved names have an incrementing version like ":16" at the end.
            # So we can't remove the "*" unless we add complexity to pass all
            # fully resolved names over to the job script.
            resources=[f"arn:{self.partition}:batch:{self.region}:{self.account}:job-queue/S3Benchmarks-PerInstance-*",
                       f"arn:{self.partition}:batch:{self.region}:{self.account}:job-definition/S3Benchmarks-PerInstance-*"],
            effect=iam.Effect.ALLOW,
        ))
        # policy for actions that don't support resource-level permissions
        self.orchestrator_job_role.add_to_policy(iam.PolicyStatement(
            actions=["batch:DescribeJobs"],
            resources=["*"],
            effect=iam.Effect.ALLOW,
        ))

        self.orchestrator_container_defn = batch.EcsEc2ContainerDefinition(
            self, "OrchestratorContainerDefn",
            image=ecs.ContainerImage.from_asset(
                directory='.',
                file='orchestrator-job.Dockerfile',
                platform=_ec2_instance_type_to_ecr_platform(ec2_instance_type)),
            cpu=instance_type.vcpu,
            memory=cdk.Size.mebibytes(256),  # cheap and puny
            command=[
                "python3", "/orchestrator-job.py",
                "--region", self.region,
                "--branch", "Ref::branch",
                "--instance-types", "Ref::instanceTypes",
                "--s3-clients", "Ref::s3Clients",
                "--workloads", "Ref::workloads",
            ],
            job_role=self.orchestrator_job_role,
        )

        self.orchestrator_job_defn = batch.EcsJobDefinition(
            self, "OrchestratorJobDefn",
            container=self.orchestrator_container_defn,
            timeout=cdk.Duration.hours(
                s3_benchmarks.ORCHESTRATOR_JOB_TIMEOUT_HOURS),
            parameters={
                "branch": "main",
                "instanceTypes": ','.join(DEFAULT_INSTANCE_TYPES),
                "s3Clients": ','.join(DEFAULT_S3_CLIENTS),
                "workloads": ','.join(DEFAULT_WORKLOADS),
            },
        )

    def _add_git_commit_cfn_output(self):
        """
        Output the git commit this stack was generated from.
        """
        run_result = subprocess.run(
            ['git', 'rev-parse', 'HEAD'],
            capture_output=True,
            check=True,
            text=True)
        git_commit = run_result.stdout.strip()

        CfnOutput(
            self, "GitCommit",
            value=git_commit,
            description="Git commit this stack was generated from")

    def _add_canary(self):
        """
        Add canary that regularly runs the benchmarks
        via an AWS Event Bridge cron rule.
        """
        events.Rule(
            self, "CanaryCronRule",
            # run the night before each workday
            # Note this is UTC so hour=8 means midnight PST
            schedule=events.Schedule.cron(
                minute='0', hour='8', week_day='MON-FRI'),
            targets=[events_targets.BatchJob(
                job_queue_arn=self.orchestrator_job_queue.job_queue_arn,
                job_queue_scope=self.orchestrator_job_queue,
                job_definition_arn=self.orchestrator_job_defn.job_definition_arn,
                job_definition_scope=self.orchestrator_job_defn)],
        )


def _max_container_memory(instance_type_memory: cdk.Size) -> cdk.Size:
    """
    Given an instance type's total memory, return the max amount a container can use.
    We want the benchmarks to get as much memory as possible,
    but the system needs a certain amount of memory to itself.
    If the container says it needs too much, the job will get
    stuck in the RUNNABLE state.
    """
    # Once your ECS cluster has instances running, you can look up Memory Available:
    # -> https://us-west-2.console.aws.amazon.com/ecs/v2/clusters
    # -> Infrastructure
    # -> Container instances
    # -> Resources and networking
    # -> Memory Available
    # But I don't know how to get numbers before the instance is running.
    # So this "ratio" and "min" are guesses, based on observing a few instance types:
    # - p4d.24xlarge with 96 vCPU & 1152GiB memory, needs 30931MiB (2.6%) memory reserved
    # - c5n.18xlarge with 72 vCPU & 192GiB memory, needs 7502MiB (3.8%) memory reserved
    # - c5.large with 2 vCPU & 4GiB memory, needs 418MiB (10.2%) memory reserved
    # - c6g.medium with 1 vCPU & 2GiB memory, needs 158MiB (7.7%) memory reserved
    reserved_ratio = 0.15
    reserved_min_MiB = 512

    instance_MiB = instance_type_memory.to_mebibytes()
    reserved_MiB = max(reserved_min_MiB, instance_MiB * reserved_ratio)
    # final value must be in whole MiB
    container_MiB = floor(instance_MiB - reserved_MiB)
    return cdk.Size.mebibytes(container_MiB)


def _ec2_instance_type_to_ecr_platform(ec2_instance_type: ec2.InstanceType) -> ecr_assets.Platform:
    if ec2_instance_type.architecture == ec2.InstanceArchitecture.ARM_64:
        return ecr_assets.Platform.LINUX_ARM64
    else:
        return ecr_assets.Platform.LINUX_AMD64

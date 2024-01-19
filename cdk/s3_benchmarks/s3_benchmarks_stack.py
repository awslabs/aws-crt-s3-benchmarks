import aws_cdk as cdk
from aws_cdk import (
    CfnOutput,
    Stack,
    aws_batch as batch,
    aws_ec2 as ec2,
    aws_ecr_assets as ecr_assets,
    aws_ecs as ecs,
    aws_iam as iam,
    aws_s3 as s3,
)
from constructs import Construct
from math import floor
import subprocess
from typing import Optional

import s3_benchmarks

# The "default" set of instance types to benchmark
# TODO: put more thought into this
DEFAULT_INSTANCE_TYPES = [
    'c5n.18xlarge',
]

# The "default" set of S3 clients to benchmark
# TODO: put more thought into this
DEFAULT_S3_CLIENTS = [
    'crt-c'
]

# The "default" set of workloads to benchmark
# TODO: put more thought into this
DEFAULT_WORKLOADS = [
    'download-Caltech256Sharded',
    'upload-Caltech256Sharded',
]

PER_INSTANCE_STORAGE_GiB = 500


class S3BenchmarksStack(Stack):

    def __init__(self, scope: Construct, construct_id: str, existing_bucket_name: Optional[str], **kwargs):
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

        self.vpc = ec2.Vpc(self, "Vpc")

        for instance_type in s3_benchmarks.ALL_INSTANCE_TYPES:
            self._define_per_instance_batch_job(instance_type)

        self._define_orchestrator_batch_job()

        self._add_git_commit_cfn_output()

    def _define_per_instance_batch_job(self, instance_type: s3_benchmarks.InstanceType):
        # "c5n.18xlarge" -> "c5n-18xlarge"
        id_with_hyphens = instance_type.id.replace('.', '-')

        ec2_instance_type = ec2.InstanceType(instance_type.id)

        # The per-instance job needs more than the default 30GiB storage.
        # Use launch template to customize this, see:
        # https://docs.aws.amazon.com/batch/latest/userguide/launch-templates.html
        # Create template once, it can be used by all per-instance jobs.
        if not hasattr(self, 'per_instance_launch_template'):
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

        # Set up role for the per-instance job scripts that actually run the benchmarks.
        # Every AWS call you add to these scripts will fail until you add a policy that allows it.
        # Create role once, it can be used by all per-instance jobs.
        if not hasattr(self, 'per_instance_job_role'):
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

        # How we chose c6g.medium:
        # - 2nd cheapest type supported by AWS Batch ($0.034/hr as of Dec 2023 in us-west-2)
        # - a1.medium is cheaper ($0.0255/hr), but Amazon Linux 2023 doesn't support 1st gen Gravitons
        # - just FYI, EC2 has cheaper types (t4g.nano for $0.0042/hr) that Batch doesn't support
        # - WARNING: instance type's vCPUs number..
        #       - MUST match compute environment's `maxv_cpus` (or jobs get stuck in RUNNABLE state).
        #       - MUST match job definition's `cpu` (to ensure 1 job runs at a time).
        instance_type = ec2.InstanceType('c6g.medium')

        compute_env = batch.ManagedEc2EcsComputeEnvironment(
            self, "OrchestratorComputeEnv",
            # scale down to 0 when there's no work
            minv_cpus=0,
            # run 1 job at a time by limiting to num vcpus available on instance type
            maxv_cpus=1,
            instance_types=[instance_type],
            # don't add 'optimal' instance type
            use_optimal_instance_classes=False,
            vpc=self.vpc,
            vpc_subnets=ec2.SubnetSelection(
                subnet_type=ec2.SubnetType.PRIVATE_WITH_EGRESS),
        )

        job_queue = batch.JobQueue(
            self, "OrchestratorJobQueue",
            compute_environments=[batch.OrderedComputeEnvironment(
                compute_environment=compute_env, order=0)],
        )

        # Set up role for the orchestrator-job.py script.
        # Every AWS call you add to this script will fail until you add a policy that allows it.
        job_role = iam.Role(
            self, "OrchestratorJobRole",
            assumed_by=iam.ServicePrincipal("ecs-tasks.amazonaws.com"),
            max_session_duration=cdk.Duration.hours(
                s3_benchmarks.ORCHESTRATOR_JOB_TIMEOUT_HOURS),
        )
        job_role.add_to_policy(iam.PolicyStatement(
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
        job_role.add_to_policy(iam.PolicyStatement(
            actions=["batch:DescribeJobs"],
            resources=["*"],
            effect=iam.Effect.ALLOW,
        ))

        container_defn = batch.EcsEc2ContainerDefinition(
            self, "OrchestratorContainerDefn",
            image=ecs.ContainerImage.from_asset(
                directory='.',
                file='orchestrator-job.Dockerfile',
                platform=_ec2_instance_type_to_ecr_platform(instance_type)),
            cpu=1,  # cheap and puny
            memory=cdk.Size.mebibytes(256),  # cheap and puny
            command=[
                "python3", "/orchestrator-job.py",
                "--region", self.region,
                "--branch", "Ref::branch",
                "--instance-types", "Ref::instanceTypes",
                "--s3-clients", "Ref::s3Clients",
                "--workloads", "Ref::workloads",
            ],
            job_role=job_role,
        )

        job_defn = batch.EcsJobDefinition(
            self, "OrchestratorJobDefn",
            container=container_defn,
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

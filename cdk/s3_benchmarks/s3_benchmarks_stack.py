import aws_cdk as cdk
from aws_cdk import (
    CfnOutput,
    Stack,
    aws_batch as batch,
    aws_cloudwatch as cloudwatch,
    aws_ec2 as ec2,
    aws_ecr_assets as ecr_assets,
    aws_ecs as ecs,
    aws_events as events,
    aws_events_targets as events_targets,
    aws_iam as iam,
    aws_s3 as s3,
)
from constructs import Construct
from dataclasses import dataclass
from math import floor
import subprocess
from typing import Optional

import s3_benchmarks


@dataclass
class S3ClientProps:
    # hex color code, prefixed with ‘#’ (e.g. ‘#00ff00’) used in dashboards
    color: str


# The "default" set of instance types to benchmark.
# This, and the other defaults below, serve several purposes:
# - When submitting a job via the console, these are the default values.
# - These defaults are what the Canary runs.
# - (TODO) A dashboard is set up to view these instance-type/s3-client/workload combinations.
DEFAULT_INSTANCE_TYPES = [
    'c7gn.16xlarge',
]

# The "default" set of S3 clients to benchmark.
# Only add clients under active development (e.g. CRT-based),
# since each one adds significantly to execution time.
DEFAULT_S3_CLIENTS = {
    'crt-c': S3ClientProps(color=cloudwatch.Color.RED),
    'sdk-rust-tm': S3ClientProps(color=cloudwatch.Color.ORANGE),
    'crt-java': S3ClientProps(color=cloudwatch.Color.GREEN),
    'sdk-java-client-crt': S3ClientProps(color=cloudwatch.Color.BROWN),
    'sdk-java-tm-classic': S3ClientProps(color='#ffd43b'),  # yellow
    'sdk-java-tm-crt': S3ClientProps(color=cloudwatch.Color.GREY),
    'crt-python': S3ClientProps(color=cloudwatch.Color.BLUE),
    'cli-crt': S3ClientProps(color=cloudwatch.Color.PURPLE),
    'boto3-crt': S3ClientProps(color=cloudwatch.Color.PINK),
}

# The "default" set of workloads to benchmark.
# This isn't everything in workloads/, it's a reasonable spread
# of use cases that won't take TOO long to run.
DEFAULT_WORKLOADS = [
    'download-max-throughput',  # how fast can we theoretically go?
    'upload-max-throughput',
    'download-256KiB-10_000x',  # lots of small files
    'upload-256KiB-10_000x',
    'download-30GiB-1x',  # very big file
    'upload-30GiB-1x',
    'download-30GiB-1x-ram',  # no disk access to slow us down
    'upload-30GiB-1x-ram',
    'download-5GiB-1x',  # moderately big file
    'upload-5GiB-1x',
    'download-5GiB-1x-ram',  # no disk access to slow us down
    'upload-5GiB-1x-ram',
]

PER_INSTANCE_STORAGE_GiB = 500


class S3BenchmarksStack(Stack):

    def __init__(self, scope: Construct, construct_id: str,
                 existing_bucket_names: Optional[list[str]],
                 availability_zone: Optional[str],
                 add_canary: bool,
                 **kwargs):
        super().__init__(scope, construct_id, **kwargs)

        # If no availability zone specified, pick one.
        if not availability_zone:
            availability_zone = self.availability_zones[0]

        # If buckets provided, use them.
        # Otherwise, create one that will be destroyed when stack is destroyed.
        if existing_bucket_names:
            # note: not using s3.Bucket.from_bucket_name() because (as of March 2024)
            # CDK doesn't work with S3 Express (gives wrong ARN for bucket)
            self.bucket_names = existing_bucket_names
        else:
            bucket = s3.Bucket(
                self, "Bucket",
                auto_delete_objects=True,
                removal_policy=cdk.RemovalPolicy.DESTROY
            )
            # note: lifecycle rules for this bucket will be set later,
            # by prep-s3-files.py, which runs as part of the per-instance job

            self.bucket_names = [bucket.bucket_name]

        self.vpc = ec2.Vpc(
            self, "Vpc",
            # Add gateway endpoint for S3.
            # Otherwise, it costs thousands of dollars to naively send terabytes
            # of S3 traffic through the default NAT gateway (ask me how I know).
            #
            # Also add one for S3 Express.
            # If you naively assumed the S3 one would cover this,
            # you'd be out thousands of dollars more (ask me how I know).
            gateway_endpoints={
                "S3": ec2.GatewayVpcEndpointOptions(
                    service=ec2.GatewayVpcEndpointAwsService("s3")),
                "S3Express": ec2.GatewayVpcEndpointOptions(
                    service=ec2.GatewayVpcEndpointAwsService("s3express"))
            },
            availability_zones=[availability_zone],
        )

        self._define_all_per_instance_batch_jobs()

        self._define_orchestrator_batch_job()

        self._add_git_commit_cfn_output()

        self._define_all_dashboards()

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
        # per-instance-job can do whatever it wants to the buckets
        for bucket in self.bucket_names:
            if s3_benchmarks.is_s3express_bucket(bucket):
                bucket_arn = f"arn:{self.partition}:s3express:{self.region}:{self.account}:bucket/{bucket}"
                service = "s3express"
            else:
                bucket_arn = f"arn:{self.partition}:s3:::{bucket}"
                service = "s3"
            self.per_instance_job_role.add_to_policy(iam.PolicyStatement(
                actions=[f"{service}:*"],
                resources=[bucket_arn,
                           f"{bucket_arn}/*"],
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

        # Use "launch templates" to customize the machines running per-instance jobs, see:
        # https://docs.aws.amazon.com/batch/latest/userguide/launch-templates.html
        self.per_instance_launch_templates = {}
        # Per-instance jobs using EBS need more than the default 30GiB storage.
        self.per_instance_launch_templates[s3_benchmarks.StorageConfiguration.EBS] = ec2.LaunchTemplate(
            self, f"PerInstanceLaunchTemplate",
            block_devices=[ec2.BlockDevice(
                device_name='/dev/xvda',
                volume=ec2.BlockDeviceVolume.ebs(
                    volume_size=PER_INSTANCE_STORAGE_GiB,
                    volume_type=ec2.EbsDeviceVolumeType.GP3,
                ),
            )],
        )

        # Per-instance jobs using Instance Storage need their ephemeral volumes formatted and bound.
        # The device path format is /dev/nvme[0-26]n1.
        # /dev/nvme0n1 will be the EBS volume and the first instance storage device path will be /dev/nvme1n1
        # See https://docs.aws.amazon.com/ebs/latest/userguide/nvme-ebs-volumes.html
        self.per_instance_launch_templates[s3_benchmarks.StorageConfiguration.INSTANCE_STORAGE] = ec2.LaunchTemplate(
            self, f"PerInstanceLaunchTemplateWithNVMeStorage",
            user_data=ec2.MultipartUserData(),
        )
        instance_storage_startup_shell_script = ec2.UserData.for_linux()
        instance_storage_startup_shell_script.add_commands(
            'mkfs -t xfs /dev/nvme1n1',
            f"mkdir {s3_benchmarks.PER_INSTANCE_WORK_DIR}",
            f"mount /dev/nvme1n1 {s3_benchmarks.PER_INSTANCE_WORK_DIR}"
        )
        self.per_instance_launch_templates[s3_benchmarks.StorageConfiguration.INSTANCE_STORAGE].user_data.add_part(
            ec2.MultipartBody.from_user_data(instance_storage_startup_shell_script))

        # Now create the actual jobs...
        for instance_type in s3_benchmarks.INSTANCE_TYPES.values():
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
            launch_template=self.per_instance_launch_templates[instance_type.storage_configuration],
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
                "--buckets", "Ref::buckets",
                "--region", self.region,
                "--branch", "Ref::branch",
                "--instance-type", instance_type.id,
                "--s3-clients", "Ref::s3Clients",
                "--workloads", "Ref::workloads",
            ],
            job_role=self.per_instance_job_role,
            volumes=[batch.EcsVolume.host(container_path=s3_benchmarks.PER_INSTANCE_WORK_DIR,
                                          host_path=s3_benchmarks.PER_INSTANCE_WORK_DIR, name="workdir")],
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
                "buckets": ','.join(self.bucket_names),
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
                '--buckets', "Ref::buckets",
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
                "buckets": ','.join(self.bucket_names),
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

    def _define_all_dashboards(self):
        """
        Add CloudWatch Dashboards to show the results of the "default" benchmarks.
        Each instance-type gets its own dashboard, then have a graph per workload,
        and in that graph plot the results of each s3-client.
        """
        for bucket in self.bucket_names:
            for instance_type_id in DEFAULT_INSTANCE_TYPES:
                instance_type = s3_benchmarks.INSTANCE_TYPES[instance_type_id]
                self._define_per_instance_dashboard(instance_type, bucket)

    def _define_per_instance_dashboard(self, instance_type: s3_benchmarks.InstanceType, bucket: str):
        storage_class = s3_benchmarks.get_bucket_storage_class(bucket)
        id_with_hyphens = instance_type.id.replace('.', '-')

        dashboard = cloudwatch.Dashboard(
            self, f"PerInstanceDashboard-{storage_class}-{id_with_hyphens}",
            dashboard_name=f"S3Benchmarks-{storage_class}-{id_with_hyphens}",
        )
        dashboard.apply_removal_policy(cdk.RemovalPolicy.DESTROY)

        graph_per_workload = []
        for workload in DEFAULT_WORKLOADS:
            # Give each workload its own graph,
            # with 1 metric for each s3-client.
            # These metrics are created by <aws-crt-s3-benchmarks>/scripts/metrics.py
            metric_per_s3_client = []
            for s3_client_id, s3_client_props in DEFAULT_S3_CLIENTS.items():
                metric_per_s3_client.append(cloudwatch.Metric(
                    namespace="S3Benchmarks",
                    metric_name=f"Throughput",
                    dimensions_map={
                        "S3Client": s3_client_id,
                        "InstanceType": instance_type.id,
                        "Branch": "main",
                        "Workload": workload,
                        "StorageClass": storage_class,
                    },
                    label=s3_client_id,
                    color=s3_client_props.color,
                    # The Canary runs daily. Set period to match
                    # so we get a line connecting the sparse data points.
                    period=cdk.Duration.days(1),
                ))

            graph_per_workload.append(cloudwatch.GraphWidget(
                title=workload,
                left=metric_per_s3_client,
                left_y_axis=cloudwatch.YAxisProps(
                    # Have y-axis go from 0 to max-theoretical-throughput.
                    # pro: easy to compare different graphs, since they all have same range.
                    # pro: 0-max is intuitive.
                    # con: for some graphs, the results are all clustered near 0.
                    min=0,
                    max=instance_type.bandwidth_Gbps,
                    # Turn off automatic units and manually label them.
                    # I don't know why automatic doesn't work, when metrics.py
                    # is calling PutMetricData() with Unit="Gigabits/Second"
                    show_units=False,
                    label="Gigabits/s",
                ),
                # Double the default height (6), to help see results in graphs that all cluster near 0.
                height=12,
            ))

        # let CDK format the graphs, with N per row
        GRAPHS_PER_ROW = 4
        for i in range(0, len(graph_per_workload), GRAPHS_PER_ROW):
            row_of_graphs = graph_per_workload[i:i+GRAPHS_PER_ROW]
            dashboard.add_widgets(*row_of_graphs)

    def _add_canary(self):
        """
        Add canary that regularly runs the benchmarks
        via an AWS Event Bridge cron rule.
        """
        events.Rule(
            self, "CanaryCronRule",
            # run nightly
            # Note this is UTC so hour=7 means 11pm PST
            schedule=events.Schedule.cron(
                minute='0', hour='7'),
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

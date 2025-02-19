"""
This file is used by the CDK stack, and also the Batch jobs.
Do not import ANY libraries that aren't part of the std library.
"""

from dataclasses import dataclass
from enum import Enum


class StorageConfiguration(Enum):
    """Storage configuration to use in the EC2 instance """
    EBS = 'EBS'
    INSTANCE_STORAGE = 'INSTANCE_STORAGE'


@dataclass
class InstanceType:
    """EC2 instance type we'll be running benchmarks on"""
    id: str
    vcpu: int
    mem_GiB: float
    bandwidth_Gbps: float
    quota_code: str
    storage_configuration: StorageConfiguration

    def resource_name(self):
        return f"S3Benchmarks-PerInstance-{self.id.replace('.', '-')}"


# EC2 Quota: Running On-Demand Standard (A, C, D, H, I, M, R, T, Z) instances
QUOTA_CODE_STANDARD_INSTANCES = "L-1216C47A"

# Instance types to run benchmarks on
INSTANCE_TYPES: dict[str, InstanceType] = {}


def _add(instance_type: InstanceType):
    INSTANCE_TYPES[instance_type.id] = instance_type


_add(InstanceType("c5n.18xlarge", vcpu=72, mem_GiB=192,
                  bandwidth_Gbps=100, quota_code=QUOTA_CODE_STANDARD_INSTANCES, storage_configuration=StorageConfiguration.EBS))

_add(InstanceType("c7gn.16xlarge", vcpu=64, mem_GiB=128,
                  bandwidth_Gbps=200, quota_code=QUOTA_CODE_STANDARD_INSTANCES, storage_configuration=StorageConfiguration.EBS))

_add(InstanceType("m6idn.24xlarge", vcpu=96, mem_GiB=384,
                  bandwidth_Gbps=150, quota_code=QUOTA_CODE_STANDARD_INSTANCES, storage_configuration=StorageConfiguration.INSTANCE_STORAGE))

# Orchestrator instance type
# How we chose c6g.medium (in Dec 2023, in us-west-2) (All of this likely different in the future):
# - 2nd cheapest type ($0.034/hr) supported by AWS Batch
# - a1.medium is cheaper ($0.0255/hr) but Amazon Linux 2023 doesn't support 1st gen Gravitons
# - just FYI, EC2 has cheaper types (t4g.nano for $0.0042/hr) that Batch doesn't support
ORCHESTRATOR_INSTANCE_TYPE = InstanceType(
    "c6g.medium", vcpu=1, mem_GiB=2,
    bandwidth_Gbps=10, quota_code=QUOTA_CODE_STANDARD_INSTANCES, storage_configuration=StorageConfiguration.EBS)

# Timeout for job running on our slowest EC2 instance type,
# running ALL benchmarking workloads, using ALL S3 clients, and ALL buckets.
PER_INSTANCE_JOB_TIMEOUT_HOURS = 12.0

# Timeout for orchestrator to run each per-instance benchmarking job,
# one after the other.
ORCHESTRATOR_JOB_TIMEOUT_HOURS = 12.0


PER_INSTANCE_WORK_DIR = "/workdir"


def is_s3express_bucket(bucket: str) -> bool:
    return bucket.endswith('--x-s3')


def get_bucket_storage_class(bucket: str) -> str:
    return 'S3Express' if is_s3express_bucket(bucket) else 'S3Standard'

"""
This file is used by the CDK stack, and also the Batch jobs.
Do not import ANY libraries that aren't part of the std library.
"""

from dataclasses import dataclass


@dataclass
class InstanceType:
    """EC2 instance type we'll be running benchmarks on"""
    id: str
    vcpu: int
    mem_GiB: float
    bandwidth_Gbps: float

    def resource_name(self):
        return f"S3Benchmarks-PerInstance-{self.id.replace('.', '-')}"


ALL_INSTANCE_TYPES = [
    InstanceType("c5n.18xlarge", vcpu=72, mem_GiB=192, bandwidth_Gbps=100),
    InstanceType("c7gn.16xlarge", vcpu=64, mem_GiB=128, bandwidth_Gbps=200),
]

# Timeout for job running on our slowest EC2 instance type,
# running ALL benchmarking workloads, using ALL S3 clients.
PER_INSTANCE_JOB_TIMEOUT_HOURS = 6.0

# Timeout for orchestrator to run each per-instance benchmarking job,
# one after the other.
ORCHESTRATOR_JOB_TIMEOUT_HOURS = 12.0

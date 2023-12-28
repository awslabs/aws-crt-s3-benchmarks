print("HELLO FROM PER_INSTANCE_JOB")

import os
for name, val in os.environ.items():
    print(f"{name}: {val}")

import s3_benchmarks
print(f"s3_benchmarks.ALL_INSTANCE_TYPES: {s3_benchmarks.ALL_INSTANCE_TYPES}")

import boto3
print("cool I imported boto")

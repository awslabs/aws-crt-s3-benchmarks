from amazonlinux:2023

RUN dnf install -y git

# s3_benchmarks/__init__.py is shared by CDK Stack and Batch jobs
COPY s3_benchmarks/__init__.py /s3_benchmarks/

COPY per-instance-job.py /

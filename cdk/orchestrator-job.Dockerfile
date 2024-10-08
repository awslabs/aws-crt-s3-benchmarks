FROM amazonlinux:2023

RUN dnf install -y python3-pip \
    && python3 -m pip install boto3

# s3_benchmarks/__init__.py is shared by CDK Stack and Batch jobs
COPY s3_benchmarks/__init__.py /s3_benchmarks/

COPY orchestrator-job.py /

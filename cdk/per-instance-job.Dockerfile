FROM amazonlinux:2023

RUN dnf install -y git

# Installing rustup is a pain, because you need to modify the shell afterwards.
# Easier to just do it here, vs later via install-tools-AL2023.py
RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y
ENV PATH="/root/.cargo/bin:${PATH}"

# s3_benchmarks/__init__.py is shared by CDK Stack and Batch jobs
COPY s3_benchmarks/__init__.py /s3_benchmarks/

COPY per-instance-job.py /

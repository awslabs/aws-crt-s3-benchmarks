FROM amazonlinux:2023

RUN dnf install -y git tar findutils libicu

RUN dnf install -y python3-pip \
    && python3 -m pip install boto3

# Installing rustup is a pain, because you need to modify the shell afterwards.
# Easier to just do it here, vs later via install-tools-AL2023.py
RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y
ENV PATH="/root/.cargo/bin:${PATH}"

# Install .NET SDK using the official install script
RUN curl -L https://dot.net/v1/dotnet-install.sh -o dotnet-install.sh && \
    chmod +x ./dotnet-install.sh && \
    ./dotnet-install.sh --version latest && \
    rm dotnet-install.sh

# Add .NET tools to PATH
ENV PATH="/root/.dotnet/tools:${PATH}"
ENV DOTNET_ROOT="/root/.dotnet"
ENV PATH="${DOTNET_ROOT}:${PATH}"

# s3_benchmarks/__init__.py is shared by CDK Stack and Batch jobs
COPY s3_benchmarks/__init__.py /s3_benchmarks/

COPY per-instance-job.py /

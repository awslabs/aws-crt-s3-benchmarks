#!/usr/bin/env bash
#
# setup-ec2.sh
#
# Run this after SSH'ing into a fresh Amazon Linux 2023 EC2 instance.
# It installs dependencies, clones the benchmark repo, preps S3 files,
# and leaves the instance ready to run benchmarks.
#
# Usage:
#   curl -sL <raw-github-url>/setup-ec2.sh | bash -s -- <BUCKET> <REGION>
#   -- OR --
#   ./setup-ec2.sh <BUCKET> <REGION>
#
# Prerequisites:
#   - EC2 instance with IAM role attached (s3-benchmark-role)
#   - 500 GiB gp3 storage
#   - Amazon Linux 2023
#

set -euo pipefail

BUCKET="${1:?Usage: $0 <BUCKET> <REGION>}"
REGION="${2:?Usage: $0 <BUCKET> <REGION>}"

BENCH_BRANCH="${3:-cli-benchmarking}"
WORK_DIR="${HOME}/benchmark"
FILES_DIR="${WORK_DIR}/files"
REPO_URL="https://github.com/awslabs/aws-crt-s3-benchmarks.git"

echo "============================================="
echo " EC2 Benchmark Setup"
echo "============================================="
echo " Bucket: ${BUCKET}"
echo " Region: ${REGION}"
echo " Branch: ${BENCH_BRANCH}"
echo " Work dir: ${WORK_DIR}"
echo "============================================="

##############################################################################
# 1. Install system packages
##############################################################################
echo ""
echo ">>> Installing system packages..."

sudo dnf install -y \
    git \
    python3-pip \
    python3.11 \
    python3.11-pip \
    python3.11-devel \
    bc \
    jq \
    time \
    tmux

##############################################################################
# 2. Set up Python 3.11 virtual environment
##############################################################################
echo ""
echo ">>> Setting up Python 3.11 virtual environment..."

mkdir -p "${WORK_DIR}"
cd "${WORK_DIR}"

python3.11 -m venv .venv
source .venv/bin/activate

pip install --upgrade pip wheel

##############################################################################
# 3. Install boto3 with CRT support
##############################################################################
echo ""
echo ">>> Installing boto3 with CRT..."

pip install "boto3[crt]"

# Verify installation
python3 -c "import boto3; print(f'boto3: {boto3.__version__}')"
python3 -c "import awscrt; print(f'awscrt: {awscrt.__version__}')"

##############################################################################
# 4. Clone benchmark repo
##############################################################################
echo ""
echo ">>> Cloning aws-crt-s3-benchmarks (branch: ${BENCH_BRANCH})..."

if [[ -d "${WORK_DIR}/aws-crt-s3-benchmarks" ]]; then
    cd "${WORK_DIR}/aws-crt-s3-benchmarks"
    git fetch
    git checkout "${BENCH_BRANCH}" || git checkout main
    git pull
else
    git clone --branch "${BENCH_BRANCH}" "${REPO_URL}" "${WORK_DIR}/aws-crt-s3-benchmarks" \
        || git clone "${REPO_URL}" "${WORK_DIR}/aws-crt-s3-benchmarks"
    cd "${WORK_DIR}/aws-crt-s3-benchmarks"
    git checkout "${BENCH_BRANCH}" 2>/dev/null || true
fi

##############################################################################
# 5. Prep S3 files and local files for upload workloads
##############################################################################
echo ""
echo ">>> Preparing S3 files and local upload files..."

mkdir -p "${FILES_DIR}"

# Only prep the workloads we care about
WORKLOADS=(
    "${WORK_DIR}/aws-crt-s3-benchmarks/workloads/download-256KiB-10_000x.run.json"
    "${WORK_DIR}/aws-crt-s3-benchmarks/workloads/upload-256KiB-10_000x.run.json"
    "${WORK_DIR}/aws-crt-s3-benchmarks/workloads/download-5GiB-1x.run.json"
    "${WORK_DIR}/aws-crt-s3-benchmarks/workloads/upload-5GiB-1x.run.json"
    "${WORK_DIR}/aws-crt-s3-benchmarks/workloads/download-1GiB-1x.run.json"
    "${WORK_DIR}/aws-crt-s3-benchmarks/workloads/upload-1GiB-1x.run.json"
)

cd "${WORK_DIR}/aws-crt-s3-benchmarks/scripts"

python3 prep-s3-files.py \
    --bucket "${BUCKET}" \
    --region "${REGION}" \
    --files-dir "${FILES_DIR}" \
    --workloads "${WORKLOADS[@]}"

##############################################################################
# 6. Create start-bench.sh convenience wrapper (runs in tmux, detachable)
##############################################################################
echo ""
echo ">>> Creating start-bench.sh wrapper..."

cat > "${WORK_DIR}/start-bench.sh" << 'WRAPPER_EOF'
#!/usr/bin/env bash
#
# start-bench.sh — launches benchmarks in a tmux session you can detach from.
#
# Usage:
#   ./start-bench.sh <BUCKET> <REGION> <THROUGHPUT_GBPS>
#
# After starting, you can safely disconnect SSH.
# Reconnect and reattach with:  tmux attach -t bench
#
set -euo pipefail

BUCKET="${1:?Usage: $0 <BUCKET> <REGION> <THROUGHPUT_GBPS>}"
REGION="${2:?Usage: $0 <BUCKET> <REGION> <THROUGHPUT_GBPS>}"
THROUGHPUT="${3:?Usage: $0 <BUCKET> <REGION> <THROUGHPUT_GBPS>}"

WORK_DIR="${HOME}/benchmark"
BENCH_SCRIPT="${WORK_DIR}/aws-crt-s3-benchmarks/cli-bench/run-benchmarks.sh"
LOG_FILE="${WORK_DIR}/bench.log"

# Kill existing bench session if any
tmux kill-session -t bench 2>/dev/null || true

echo "Starting benchmarks in tmux session 'bench'..."
echo "  Bucket:     ${BUCKET}"
echo "  Region:     ${REGION}"
echo "  Throughput: ${THROUGHPUT} Gbps"
echo "  Log:        ${LOG_FILE}"
echo ""

# Launch in a new tmux session
# The session activates the venv, runs the benchmark, and logs output
tmux new-session -d -s bench \
    "cd ${WORK_DIR} && source .venv/bin/activate && ${BENCH_SCRIPT} ${BUCKET} ${REGION} ${THROUGHPUT} 2>&1 | tee ${LOG_FILE}; echo ''; echo 'BENCHMARKS COMPLETE. Results in ~/benchmark/results/'; echo 'Press any key to close...'; read -n1"

echo "============================================="
echo " Benchmarks running in background!"
echo "============================================="
echo ""
echo " You can now safely disconnect SSH."
echo ""
echo " To watch progress:     tmux attach -t bench"
echo " To detach again:       Ctrl+B then D"
echo " To check if running:   tmux ls"
echo " To view log:           tail -f ${LOG_FILE}"
echo ""
echo " When complete, results are in: ~/benchmark/results/"
echo "============================================="
WRAPPER_EOF

chmod +x "${WORK_DIR}/start-bench.sh"

##############################################################################
# Done
##############################################################################
echo ""
echo ">>> Setup complete!"
echo ""
echo "============================================="
echo " Ready to run benchmarks"
echo "============================================="
echo ""
echo " RECOMMENDED (detachable, survives SSH disconnect):"
echo "   cd ${WORK_DIR}"
echo "   ./start-bench.sh ${BUCKET} ${REGION} <THROUGHPUT_GBPS>"
echo ""
echo " Then you can safely close SSH. Reconnect later and:"
echo "   tmux attach -t bench     # watch live"
echo "   tail -f ~/benchmark/bench.log  # check progress"
echo ""
echo " THROUGHPUT_GBPS by instance type:"
echo "   t2.micro     -> 0.5"
echo "   t3.micro     -> 5.0"
echo "   t3.small     -> 5.0"
echo "   t3.medium    -> 5.0"
echo "   m5.large     -> 10.0"
echo "   m5.xlarge    -> 10.0"
echo "   c5.xlarge    -> 10.0"
echo "   c5n.large    -> 25.0"
echo ""
echo " Results will be in: ${WORK_DIR}/results/<instance-id>/"
echo "============================================="

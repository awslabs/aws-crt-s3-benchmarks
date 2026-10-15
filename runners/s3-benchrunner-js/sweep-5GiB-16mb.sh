#!/usr/bin/env bash
# Sweep upload-5GiB-1x (files on disk) at 16 MiB part size across concurrencies.
# Usage: BUCKET REGION  (defaults: s3-benchmarkrunner-bucket us-west-2)
set -euo pipefail

BUCKET="${1:-s3-benchmarkrunner-bucket}"
REGION="${2:-us-west-2}"
FILES_DIR="${FILES_DIR:-$HOME/files}"
TARGET=200.0

RUNNER_DIR="/home/ec2-user/repo/aws-crt-s3-benchmarks/runners/s3-benchrunner-js"
WORKLOAD="/home/ec2-user/repo/aws-crt-s3-benchmarks/workloads/upload-5GiB-1x.run.json"

cd "$FILES_DIR"
for CONC in 256 512 640; do
  echo "=================================================="
  echo "== MAX_CONCURRENCY=$CONC  PART_SIZE_MIB=16"
  echo "=================================================="
  PART_SIZE_MIB=16 MAX_CONCURRENCY="$CONC" \
    node "$RUNNER_DIR/index.js" sdk-js "$WORKLOAD" "$BUCKET" "$REGION" "$TARGET"
done

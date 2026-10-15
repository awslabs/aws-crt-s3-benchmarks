#!/usr/bin/env bash
#
# Sweep a download workload across several MAX_CONCURRENCY values with the
# s3-benchrunner-js (sdk-js / S3TransferManager) runner.
#
# Downloads are disk-backed (filesOnDisk: true -> downloadToFile). O_DIRECT is
# enabled by default (the runner switches to RANGE downloads for 4 KiB-aligned
# offsets); set USE_O_DIRECT=false to use buffered writes instead.
#
# Usage:
#   ./sweep-download.sh [WORKLOAD_NAME]
#
#   WORKLOAD_NAME  base name of a workload in workloads/ WITHOUT the .run.json
#                  suffix. Default: download-5GiB-1x
#                  e.g.  ./sweep-download.sh download-30GiB-1x
#                        ./sweep-download.sh download-5GiB-20x
#
# Environment overrides:
#   BUCKET              S3 bucket            (default: smilkuri-new-tm-benchmarks)
#   REGION              AWS region           (default: us-west-2)
#   FILES_DIR           dir to run from      (default: /data/files)
#   PART_SIZE_MIB       multipart part size  (default: 16)
#   UV_THREADPOOL_SIZE  libuv threadpool size (default: unset -> Node default 4)
#   CONCURRENCIES       space-separated list (default: "512 640 1024 1536")
#   OBJECT_CONCURRENCY  max objects transferred at once (default: unset = unbounded;
#                       set for fan-out workloads like 5GiB-20x to bound memory)
#   USE_O_DIRECT        true|false           (default: true)
#   TARGET              TARGET_THROUGHPUT arg, ignored by JS runner (default: 200.0)
#   RESULTS_DIR         base dir for JSON results
#                       (default: <runner-dir>/results_m8ib.48xlarge)
#
set -euo pipefail

WORKLOAD_NAME="${1:-download-5GiB-20x}"

BUCKET="${BUCKET:-smilkuri-new-tm-benchmarks}"
REGION="${REGION:-us-west-2}"
FILES_DIR="${FILES_DIR:-/data/files}"
PART_SIZE_MIB="${PART_SIZE_MIB:-16}"
UV_THREADPOOL_SIZE="${UV_THREADPOOL_SIZE:-}"
TARGET="${TARGET:-200.0}"
CONCURRENCIES="${CONCURRENCIES:-256}"
USE_O_DIRECT="${USE_O_DIRECT:-true}"
# OBJECT_CONCURRENCY intentionally left unset by default (= unbounded). Only
# forwarded to the runner when the caller sets it.
OBJECT_CONCURRENCY="${OBJECT_CONCURRENCY:-}"

# Resolve paths relative to this script so it works regardless of CWD.
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
RUNNER="$SCRIPT_DIR/index.js"
WORKLOAD="$REPO_ROOT/workloads/${WORKLOAD_NAME}.run.json"

# Base dir for JSON results. The runner reads RESULTS_DIR and writes
# <RESULTS_DIR>/<workload>/<timestamp>.json.
RESULTS_DIR="${RESULTS_DIR:-$SCRIPT_DIR/results_m8ib.48xlarge}"
export RESULTS_DIR

if [[ ! -f "$WORKLOAD" ]]; then
  echo "ERROR: workload not found: $WORKLOAD" >&2
  echo "       pass a base name present in $REPO_ROOT/workloads (without .run.json)" >&2
  exit 2
fi
if [[ ! -d "$FILES_DIR" ]]; then
  echo "ERROR: FILES_DIR does not exist: $FILES_DIR" >&2
  echo "       run prep-s3-files.py first, or set FILES_DIR=..." >&2
  exit 2
fi

echo "Sweep (download): $WORKLOAD_NAME"
echo "  workload   : $WORKLOAD"
echo "  bucket     : $BUCKET   region: $REGION"
echo "  files_dir  : $FILES_DIR"
echo "  part size  : ${PART_SIZE_MIB} MiB"
echo "  uv pool    : ${UV_THREADPOOL_SIZE:-node default (4)}"
echo "  o_direct   : ${USE_O_DIRECT}"
echo "  obj conc   : ${OBJECT_CONCURRENCY:-unbounded}"
echo "  concurrency: $CONCURRENCIES"
echo "  results    : $RESULTS_DIR"
echo

cd "$FILES_DIR"
for CONC in $CONCURRENCIES; do
  echo "=================================================="
  echo "== $WORKLOAD_NAME  MAX_CONCURRENCY=$CONC  PART_SIZE_MIB=$PART_SIZE_MIB  O_DIRECT=$USE_O_DIRECT"
  echo "=================================================="
  # Build the env assignment list, only including optional vars if set.
  ENV_ARGS=(
    "PART_SIZE_MIB=$PART_SIZE_MIB"
    "MAX_CONCURRENCY=$CONC"
    "USE_O_DIRECT=$USE_O_DIRECT"
  )
  if [[ -n "$UV_THREADPOOL_SIZE" ]]; then
    ENV_ARGS+=("UV_THREADPOOL_SIZE=$UV_THREADPOOL_SIZE")
  fi
  if [[ -n "$OBJECT_CONCURRENCY" ]]; then
    ENV_ARGS+=("OBJECT_CONCURRENCY=$OBJECT_CONCURRENCY")
  fi
  env "${ENV_ARGS[@]}" \
    node "$RUNNER" sdk-js "$WORKLOAD" "$BUCKET" "$REGION" "$TARGET"
  echo
done

echo "Done. JSON results written under: $RESULTS_DIR/$WORKLOAD_NAME/"

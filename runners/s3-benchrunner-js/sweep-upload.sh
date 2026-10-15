#!/usr/bin/env bash
#
# Sweep an upload workload across several MAX_CONCURRENCY values with the
# s3-benchrunner-js (sdk-js / S3TransferManager) runner.
#
# Usage:
#   ./sweep-upload.sh [WORKLOAD_NAME]
#
#   WORKLOAD_NAME  base name of a workload in workloads/ WITHOUT the .run.json
#                  suffix. Default: upload-5GiB-1x
#                  e.g.  ./sweep-upload.sh upload-30GiB-1x
#                        ./sweep-upload.sh upload-50GiB-1x
#
# Environment overrides:
#   S3_CLIENT      sdk-js | sdk-js-lib-storage  (default: sdk-js)
#                  sdk-js            -> S3TransferManager.upload
#                  sdk-js-lib-storage-> lib-storage Upload (comparison baseline)
#   BUCKET         S3 bucket            (default: smilkuri-new-tm-benchmarks)
#   REGION         AWS region           (default: us-west-2)
#   FILES_DIR      dir to run from      (default: /data/files)
#   PART_SIZE_MIB  multipart part size  (default: 16)
#   UV_THREADPOOL_SIZE  libuv threadpool size (default: unset -> Node default 4)
#   CONCURRENCIES  space-separated list (default: "512 640 1024 1536")
#   OBJECT_CONCURRENCY  max objects transferred at once (default: unset = unbounded;
#                       set for fan-out workloads like 5GiB-20x to bound memory)
#   TARGET         TARGET_THROUGHPUT arg, ignored by JS runner (default: 200.0)
#   RESULTS_DIR    base dir for JSON results
#                  (default: <runner-dir>/results_m8ib.48xlarge)
#
set -euo pipefail

WORKLOAD_NAME="${1:-upload-5GiB-20x}"

S3_CLIENT="${S3_CLIENT:-sdk-js}"   # sdk-js (S3TransferManager) | sdk-js-lib-storage (lib-storage Upload)
BUCKET="${BUCKET:-smilkuri-new-tm-benchmarks}"
REGION="${REGION:-us-west-2}"
FILES_DIR="${FILES_DIR:-/data/files}"
PART_SIZE_MIB="${PART_SIZE_MIB:-16}"
TARGET="${TARGET:-200.0}"
CONCURRENCIES="${CONCURRENCIES:-512}"
# libuv threadpool size. Only forwarded to node when the caller sets it;
# otherwise Node uses its own default (4). Set e.g. UV_THREADPOOL_SIZE=128.
UV_THREADPOOL_SIZE="${UV_THREADPOOL_SIZE:-}"
# OBJECT_CONCURRENCY: max objects transferred at once. Unset = unbounded.
# Set it for fan-out workloads (e.g. 5GiB-20x) to bound memory/connections.
OBJECT_CONCURRENCY="${OBJECT_CONCURRENCY:-}"

case "$S3_CLIENT" in
  sdk-js|sdk-js-lib-storage) ;;
  *) echo "ERROR: S3_CLIENT must be 'sdk-js' or 'sdk-js-lib-storage' (got '$S3_CLIENT')" >&2; exit 2 ;;
esac

# Resolve paths relative to this script so it works regardless of CWD.
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
RUNNER="$SCRIPT_DIR/index.js"
WORKLOAD="$REPO_ROOT/workloads/${WORKLOAD_NAME}.run.json"

# Base dir for JSON results. Defaults to an instance-tagged dir inside the
# runner; the runner reads RESULTS_DIR and writes <RESULTS_DIR>/<workload>/<ts>.json.
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

echo "Sweep: $WORKLOAD_NAME"
echo "  client     : $S3_CLIENT"
echo "  workload   : $WORKLOAD"
echo "  bucket     : $BUCKET   region: $REGION"
echo "  files_dir  : $FILES_DIR"
echo "  part size  : ${PART_SIZE_MIB} MiB"
echo "  uv pool    : ${UV_THREADPOOL_SIZE:-node default (4)}"
echo "  obj conc   : ${OBJECT_CONCURRENCY:-unbounded}"
echo "  concurrency: $CONCURRENCIES"
echo

cd "$FILES_DIR"
for CONC in $CONCURRENCIES; do
  echo "=================================================="
  echo "== $S3_CLIENT  $WORKLOAD_NAME  MAX_CONCURRENCY=$CONC  PART_SIZE_MIB=$PART_SIZE_MIB"
  echo "=================================================="
  ENV_ARGS=(
    "PART_SIZE_MIB=$PART_SIZE_MIB"
    "MAX_CONCURRENCY=$CONC"
  )
  if [[ -n "$UV_THREADPOOL_SIZE" ]]; then
    ENV_ARGS+=("UV_THREADPOOL_SIZE=$UV_THREADPOOL_SIZE")
  fi
  if [[ -n "$OBJECT_CONCURRENCY" ]]; then
    ENV_ARGS+=("OBJECT_CONCURRENCY=$OBJECT_CONCURRENCY")
  fi
  env "${ENV_ARGS[@]}" \
    node "$RUNNER" "$S3_CLIENT" "$WORKLOAD" "$BUCKET" "$REGION" "$TARGET"
  echo
done

echo "Done. JSON results written under: $RESULTS_DIR/$WORKLOAD_NAME/"

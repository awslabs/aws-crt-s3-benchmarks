#!/usr/bin/env bash
#
# run-benchmarks.sh
#
# Runs boto3-classic and boto3-crt benchmarks across selected workloads,
# collecting throughput, peak RSS, CPU usage, and Python heap stats.
# Outputs structured JSON results for later aggregation.
#
# Usage:
#   ./run-benchmarks.sh <BUCKET> <REGION> <TARGET_THROUGHPUT_GBPS>
#
# Example:
#   ./run-benchmarks.sh my-s3-bench-bucket us-west-2 10.0
#
# Throughput guide by instance type:
#   t2.micro     -> 0.5
#   t3.micro     -> 5.0
#   t3.small     -> 5.0
#   t3.medium    -> 5.0
#   m5.large     -> 10.0
#   m5.xlarge    -> 10.0
#   c5.xlarge    -> 10.0
#   c5n.large    -> 25.0
#

set -euo pipefail

##############################################################################
# Args
##############################################################################

BUCKET="${1:?Usage: $0 <BUCKET> <REGION> <TARGET_THROUGHPUT_GBPS>}"
REGION="${2:?Usage: $0 <BUCKET> <REGION> <TARGET_THROUGHPUT_GBPS>}"
TARGET_THROUGHPUT="${3:?Usage: $0 <BUCKET> <REGION> <TARGET_THROUGHPUT_GBPS>}"

##############################################################################
# Config
##############################################################################

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BENCH_REPO="$(cd "${SCRIPT_DIR}/.." && pwd)"
RUNNER_DIR="${BENCH_REPO}/runners/s3-benchrunner-python"
WORKLOADS_DIR="${BENCH_REPO}/workloads"
WORK_DIR="${HOME}/benchmark"
FILES_DIR="${WORK_DIR}/files"
OUTPUT_DIR="${WORK_DIR}/results"

# Override maxRepeatSecs in workload files. Set to 86400 (24h) for overnight runs.
# Set to 600 (default) for quick validation runs.
MAX_REPEAT_SECS="${MAX_REPEAT_SECS:-86400}"

# Network credit drain warmup before each workload (seconds).
# Saturates NIC to deplete burst credits so benchmarks run at baseline bandwidth.
# Set to 0 to skip warmup. Default: 60 seconds.
WARMUP_SECS="${WARMUP_SECS:-60}"

# Runners to benchmark
RUNNERS=("boto3-classic" "boto3-crt")

# Workloads: 10k small files + single large files (upload & download, on disk)
WORKLOADS=(
    "download-256KiB-10_000x.run.json"
    "upload-256KiB-10_000x.run.json"
    "download-5GiB-1x.run.json"
    "upload-5GiB-1x.run.json"
    "download-30GiB-1x.run.json"
    "upload-30GiB-1x.run.json"
)

##############################################################################
# Instance metadata (EC2 IMDSv2)
##############################################################################

get_instance_metadata() {
    local token
    token=$(curl -sf -X PUT "http://169.254.169.254/latest/api/token" \
        -H "X-aws-ec2-metadata-token-ttl-seconds: 21600" 2>/dev/null || echo "")

    if [[ -n "$token" ]]; then
        INSTANCE_ID=$(curl -sf -H "X-aws-ec2-metadata-token: $token" \
            http://169.254.169.254/latest/meta-data/instance-id 2>/dev/null || echo "local")
        INSTANCE_TYPE=$(curl -sf -H "X-aws-ec2-metadata-token: $token" \
            http://169.254.169.254/latest/meta-data/instance-type 2>/dev/null || echo "unknown")
    else
        INSTANCE_ID="local-$(hostname -s)"
        INSTANCE_TYPE="unknown"
    fi
}

get_instance_metadata

##############################################################################
# Setup
##############################################################################

mkdir -p "${OUTPUT_DIR}/${INSTANCE_ID}"

echo "============================================="
echo "Boto3 S3 Benchmark Suite"
echo "============================================="
echo "Instance:    ${INSTANCE_ID} (${INSTANCE_TYPE})"
echo "Bucket:      ${BUCKET}"
echo "Region:      ${REGION}"
echo "Throughput:  ${TARGET_THROUGHPUT} Gbps"
echo "Repeat cap:  ${MAX_REPEAT_SECS}s (override with MAX_REPEAT_SECS env var)"
echo "Warmup:      ${WARMUP_SECS}s credit drain per workload (override with WARMUP_SECS env var)"
if (( $(echo "${TARGET_THROUGHPUT} <= 5.0" | bc -l) )); then
    echo "30GiB skip:  YES (throughput <= 5.0 Gbps, 5 GiB provides equivalent baseline data)"
fi
echo "Output:      ${OUTPUT_DIR}/${INSTANCE_ID}/"
echo "Runners:     ${RUNNERS[*]}"
echo "Workloads:   ${#WORKLOADS[@]} workloads"
echo "============================================="

##############################################################################
# Run benchmarks
##############################################################################

for runner in "${RUNNERS[@]}"; do
    for workload in "${WORKLOADS[@]}"; do
        workload_path="${WORKLOADS_DIR}/${workload}"
        if [[ ! -f "${workload_path}" ]]; then
            echo "WARN: workload not found: ${workload_path}, skipping"
            continue
        fi

        # Auto-skip 30 GiB workloads on low-bandwidth instances.
        # When target throughput <= 5 Gbps, the 5 GiB workload already runs for
        # 80+ seconds at baseline, providing the same sustained-transfer data.
        # The 30 GiB workload would just repeat that measurement for 6x longer.
        # Instances skipped: t2.micro (0.5), t3.micro/small/medium (5.0)
        # Instances kept: m5.large, m5.xlarge, c5.xlarge (10.0), c5n.large (25.0)
        if [[ "${workload}" == *"30GiB"* ]]; then
            skip_30g=$(echo "${TARGET_THROUGHPUT} <= 5.0" | bc -l)
            if [[ "${skip_30g}" == "1" ]]; then
                echo ""
                echo "  SKIP: ${workload%.run.json} (target throughput ${TARGET_THROUGHPUT} Gbps <= 5.0, 5 GiB workload provides equivalent baseline data)"
                continue
            fi
        fi

        # Output file name
        workload_name="${workload%.run.json}"
        output_file="${OUTPUT_DIR}/${INSTANCE_ID}/${runner}_${workload_name}.json"
        time_file="/tmp/bench_time_$$_${runner}_${workload_name}.txt"
        stdout_file="/tmp/bench_stdout_$$_${runner}_${workload_name}.txt"
        heap_file="/tmp/bench_heap_$$_${runner}_${workload_name}.txt"

        echo ""
        echo "---------------------------------------------"
        echo "  Runner:   ${runner}"
        echo "  Workload: ${workload_name}"
        echo "---------------------------------------------"

        # Drain network burst credits before the benchmark starts.
        # Downloads a large S3 object to /dev/null for WARMUP_SECS seconds
        # so the actual benchmark runs at baseline bandwidth, not burst.
        if [[ "${WARMUP_SECS}" -gt 0 ]]; then
            echo "  Warming up (draining network credits for ${WARMUP_SECS}s)..."
            timeout "${WARMUP_SECS}" aws s3 cp "s3://${BUCKET}/download/30GiB-1x/1" /dev/null \
                --region "${REGION}" > /dev/null 2>&1 || true
        fi

        # Connection sampling temp file
        conn_file="/tmp/bench_conn_$$_${runner}_${workload_name}.txt"

        # Create a patched workload with our MAX_REPEAT_SECS override
        patched_workload="/tmp/bench_workload_$$_${runner}_${workload_name}.json"
        sed "s/\"maxRepeatSecs\": *[0-9]*/\"maxRepeatSecs\": ${MAX_REPEAT_SECS}/" \
            "${workload_path}" > "${patched_workload}"

        # Change to files dir so uploads/downloads happen there
        cd "${FILES_DIR}"

        # Run with /usr/bin/time for RSS + CPU measurement
        # GNU time format: wall_secs user_secs sys_secs max_rss_kib
        set +e
        /usr/bin/time -f "%e %U %S %M" -o "${time_file}" \
            python3 "${SCRIPT_DIR}/bench-wrapper.py" "${heap_file}" \
            "${RUNNER_DIR}/main.py" "${runner}" "${patched_workload}" \
            "${BUCKET}" "${REGION}" "${TARGET_THROUGHPUT}" \
            > "${stdout_file}" 2>&1 &
        BENCH_PID=$!

        # Start connection sampler in background
        # Polls ss every 1s, records ESTABLISHED TCP connections for the bench process.
        # BENCH_PID is /usr/bin/time; python3 is its child. We match connections
        # owned by any process in the subtree.
        (
            while kill -0 ${BENCH_PID} 2>/dev/null; do
                # Get child PIDs (ps --ppid gives direct children of time = python3)
                child_pids=$(ps --ppid ${BENCH_PID} -o pid= 2>/dev/null | tr -s ' \n' ' ')
                # Build a grep -E pattern: "pid=123,|pid=456,|pid=789,"
                grep_pat="pid=${BENCH_PID},"
                for p in ${child_pids}; do
                    grep_pat="${grep_pat}|pid=${p},"
                done
                count=$(ss -tnp state established 2>/dev/null \
                    | grep -cE "${grep_pat}" 2>/dev/null || echo "0")
                echo "${count}" >> "${conn_file}"
                sleep 1
            done
        ) &
        SAMPLER_PID=$!

        # Wait for benchmark to finish
        wait ${BENCH_PID}
        exit_code=$?

        # Stop the sampler
        kill ${SAMPLER_PID} 2>/dev/null || true
        wait ${SAMPLER_PID} 2>/dev/null || true
        set -e

        # Handle skip code (runner doesn't support this workload)
        if [[ ${exit_code} -eq 123 ]]; then
            echo "  SKIPPED"
            rm -f "${heap_file}" "${time_file}" "${stdout_file}" "${conn_file}" "${patched_workload}"
            continue
        fi

        if [[ ${exit_code} -ne 0 ]]; then
            echo "  FAILED (exit code: ${exit_code})"
            tail -20 "${stdout_file}" 2>/dev/null || true
            rm -f "${heap_file}" "${time_file}" "${stdout_file}" "${conn_file}" "${patched_workload}"
            continue
        fi

        # Parse /usr/bin/time output
        if [[ -f "${time_file}" ]]; then
            read -r wall_secs user_secs sys_secs max_rss_kib < "${time_file}"
        else
            wall_secs="0" user_secs="0" sys_secs="0" max_rss_kib="0"
        fi

        # Calculate CPU%
        cpu_total=$(echo "${user_secs} + ${sys_secs}" | bc)
        if (( $(echo "${wall_secs} > 0" | bc -l) )); then
            cpu_percent=$(echo "scale=1; (${cpu_total} / ${wall_secs}) * 100" | bc)
        else
            cpu_percent="0"
        fi

        # Parse heap stats
        heap_peak_bytes="0"
        if [[ -f "${heap_file}" ]]; then
            heap_peak_bytes=$(cat "${heap_file}" | tr -d '[:space:]')
        fi
        # Default if empty
        heap_peak_bytes="${heap_peak_bytes:-0}"

        # Parse connection samples (peak, average, sample count)
        conn_peak="0"
        conn_avg="0"
        conn_samples="0"
        if [[ -f "${conn_file}" && -s "${conn_file}" ]]; then
            conn_samples=$(wc -l < "${conn_file}" | tr -d '[:space:]')
            conn_peak=$(sort -n "${conn_file}" | tail -1 | tr -d '[:space:]')
            conn_sum=$(awk '{s+=$1} END {print s}' "${conn_file}")
            if [[ "${conn_samples}" -gt 0 ]]; then
                conn_avg=$(echo "scale=1; ${conn_sum} / ${conn_samples}" | bc)
            fi
        fi
        conn_peak="${conn_peak:-0}"
        conn_avg="${conn_avg:-0}"

        # Parse throughput from runner output (all Run: lines)
        # Build per-run JSON array
        runs_json="["
        first=true
        while IFS= read -r line; do
            [[ -z "$line" ]] && continue
            run_num=$(echo "$line" | sed -n 's/.*Run:\([0-9]*\).*/\1/p')
            run_secs=$(echo "$line" | sed -n 's/.*Secs:\([0-9.]*\).*/\1/p')
            run_gbps=$(echo "$line" | sed -n 's/.*Gb\/s:\([0-9.]*\).*/\1/p')
            [[ -z "$run_num" ]] && continue
            if [[ "$first" == "true" ]]; then
                first=false
            else
                runs_json+=","
            fi
            runs_json+="{\"run\":${run_num},\"secs\":${run_secs},\"gbps\":${run_gbps}}"
        done < <(grep '^Run:' "${stdout_file}" 2>/dev/null || true)
        runs_json+="]"

        # Get last run's throughput as the summary value
        throughput_gbps=$(grep '^Run:' "${stdout_file}" | tail -1 | sed -n 's/.*Gb\/s:\([0-9.]*\).*/\1/p' || echo "0")
        throughput_gbps="${throughput_gbps:-0}"

        # Write output JSON
        cat > "${output_file}" << EOF
{
    "metadata": {
        "timestamp": "$(date -u +%Y-%m-%dT%H:%M:%SZ)",
        "instance_id": "${INSTANCE_ID}",
        "instance_type": "${INSTANCE_TYPE}",
        "runner": "${runner}",
        "workload": "${workload_name}",
        "bucket": "${BUCKET}",
        "region": "${REGION}",
        "target_throughput_gbps": ${TARGET_THROUGHPUT}
    },
    "summary": {
        "throughput_gbps": ${throughput_gbps},
        "wall_time_secs": ${wall_secs},
        "user_time_secs": ${user_secs},
        "sys_time_secs": ${sys_secs},
        "cpu_percent": ${cpu_percent},
        "peak_rss_kib": ${max_rss_kib},
        "python_heap_peak_bytes": ${heap_peak_bytes},
        "connections_peak": ${conn_peak},
        "connections_avg": ${conn_avg},
        "connections_samples": ${conn_samples}
    },
    "runs": ${runs_json}
}
EOF

        echo "  Throughput: ${throughput_gbps} Gb/s"
        echo "  Peak RSS:   ${max_rss_kib} KiB ($(echo "scale=1; ${max_rss_kib}/1024" | bc) MiB)"
        echo "  CPU:        ${cpu_percent}%"
        echo "  Heap peak:  ${heap_peak_bytes} bytes ($(echo "scale=1; ${heap_peak_bytes}/1048576" | bc) MiB)"
        echo "  Connections: peak=${conn_peak} avg=${conn_avg} (${conn_samples} samples)"
        echo "  Wall time:  ${wall_secs}s"
        echo "  -> ${output_file}"

        # Cleanup temp files
        rm -f "${heap_file}" "${time_file}" "${stdout_file}" "${conn_file}" "${patched_workload}"
    done
done

echo ""
echo "============================================="
echo "All benchmarks complete."
echo "Results: ${OUTPUT_DIR}/${INSTANCE_ID}/"
echo "============================================="
ls -la "${OUTPUT_DIR}/${INSTANCE_ID}/"
echo ""
echo "To collect results, from your laptop run:"
echo "  scp -i <key> -r ec2-user@<ip>:${OUTPUT_DIR}/${INSTANCE_ID} ./results/"

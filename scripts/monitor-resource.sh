#!/bin/bash
# Monitor system resources (CPU/memory/network/disk) while running a benchmark.
# Accepts the same arguments as run-benchmarks.py, wrapping the runner command
# with resource monitoring that outputs a CSV for analysis with plot.py.
#
# Usage:
#   ./monitor-resource.sh \
#       --runner-cmd "java -jar path/to/runner.jar" \
#       --s3-client crt-java \
#       --bucket my-bucket \
#       --region us-west-2 \
#       --throughput 100.0 \
#       --workloads workloads/download-max-throughput.run.json \
#       [--interval-ms 1000] \
#       [--output metrics.csv]
#
# Arguments (same as run-benchmarks.py):
#   --runner-cmd    Command to launch runner (required)
#   --s3-client     S3 client to benchmark (required)
#   --bucket        S3 bucket name (required)
#   --region        AWS region (required)
#   --throughput    Target throughput in Gbps (required)
#   --workloads     Path to workload .run.json file (required)
#
# Additional arguments:
#   --interval-ms   Sampling interval in milliseconds (default: 1000)
#                   For short workloads (<10s), use 200-500ms
#   --output        Output CSV file path (default: metrics_YYYYMMDD_HHMMSS.csv)

set -euo pipefail

# --- Argument parsing ---
RUNNER_CMD=""
S3_CLIENT=""
BUCKET=""
REGION=""
THROUGHPUT=""
WORKLOADS=""
INTERVAL_MS=1000
OUTPUT=""

while [[ $# -gt 0 ]]; do
    case "$1" in
        --runner-cmd)   RUNNER_CMD="$2";  shift 2 ;;
        --s3-client)    S3_CLIENT="$2";   shift 2 ;;
        --bucket)       BUCKET="$2";      shift 2 ;;
        --region)       REGION="$2";      shift 2 ;;
        --throughput)   THROUGHPUT="$2";  shift 2 ;;
        --workloads)    WORKLOADS="$2";   shift 2 ;;
        --interval-ms)  INTERVAL_MS="$2"; shift 2 ;;
        --output)       OUTPUT="$2";      shift 2 ;;
        -h|--help)
            echo "Usage: ./monitor-resource.sh --runner-cmd CMD --s3-client ID --bucket BUCKET --region REGION --throughput GBPS --workloads WORKLOAD [--interval-ms 1000] [--output file.csv]"
            exit 0
            ;;
        *)
            echo "Unknown argument: $1" >&2
            echo "Run with --help for usage" >&2
            exit 1
            ;;
    esac
done

# Validate required arguments
for arg_name in runner-cmd s3-client bucket region throughput workloads; do
    var_name=$(echo "$arg_name" | tr '-' '_' | tr '[:lower:]' '[:upper:]')
    eval "val=\$$var_name"
    if [ -z "$val" ]; then
        echo "Error: --$arg_name is required" >&2
        exit 1
    fi
done

# --- Configuration ---
INTERVAL_S=$(echo "$INTERVAL_MS" | awk '{printf "%.3f", $1/1000}')
CSV="${OUTPUT:-metrics_$(date +%Y%m%d_%H%M%S).csv}"
IFACE=$(ip route show default | awk '{print $5; exit}')

# Auto-detect disk device: prefer md0 (RAID), then first nvme*n1, then root device
if grep -q ' md0 ' /proc/diskstats 2>/dev/null; then
    DISK_DEV="md0"
elif lsblk -dno NAME 2>/dev/null | grep -q '^nvme'; then
    DISK_DEV=$(lsblk -dno NAME 2>/dev/null | grep '^nvme' | head -1)
else
    DISK_DEV=$(lsblk -dno NAME 2>/dev/null | head -1)
fi

# --- Initialize CSV ---
echo "timestamp,cpu_percent,mem_used_gib,mem_total_gib,mem_cached_gib,mem_dirty_mib,net_rx_gbps,net_tx_gbps,disk_write_mbs,disk_read_mbs" > "$CSV"

# Get initial network counters
prev_rx=$(cat /sys/class/net/$IFACE/statistics/rx_bytes)
prev_tx=$(cat /sys/class/net/$IFACE/statistics/tx_bytes)
prev_time=$(date +%s%N)

# Get initial disk counters (sectors_read=$6, sectors_written=$10 in /proc/diskstats)
if [ -n "$DISK_DEV" ] && grep -q "[[:space:]]${DISK_DEV}[[:space:]]" /proc/diskstats 2>/dev/null; then
    prev_disk_write=$(awk "/[[:space:]]${DISK_DEV}[[:space:]]/{print \$10}" /proc/diskstats)
    prev_disk_read=$(awk "/[[:space:]]${DISK_DEV}[[:space:]]/{print \$6}" /proc/diskstats)
    DISK_MONITORING=true
else
    prev_disk_write=0
    prev_disk_read=0
    DISK_MONITORING=false
fi

# --- Start the benchmark in background ---
$RUNNER_CMD $S3_CLIENT "$WORKLOADS" $BUCKET $REGION $THROUGHPUT &
PID=$!

echo "Monitoring PID $PID, writing to $CSV (interface: $IFACE, disk: ${DISK_DEV:-none}, interval: ${INTERVAL_MS}ms)"

# --- Sampling loop ---
while kill -0 $PID 2>/dev/null; do
    sleep "$INTERVAL_S"

    ts=$(date +%s.%N)
    now_ns=$(date +%s%N)

    # CPU (system-wide from /proc/stat delta)
    CPU_SAMPLE_S=$(echo "$INTERVAL_S" | awk '{v=$1*0.1; if(v<0.05) v=0.05; if(v>0.1) v=0.1; printf "%.3f", v}')
    cpu=$(awk '/^cpu /{u=$2+$4; t=$2+$3+$4+$5+$6+$7+$8; if(NR==1){pu=u;pt=t}else{printf "%.1f", (u-pu)/(t-pt)*100}}' \
        <(cat /proc/stat) <(sleep "$CPU_SAMPLE_S" && cat /proc/stat) 2>/dev/null)
    # Fallback: use top
    if [ -z "$cpu" ]; then
        cpu=$(top -bn1 | awk '/^%Cpu/{printf "%.1f", 100-$8}')
    fi

    # Memory
    read mem_used mem_total mem_cached mem_dirty <<< $(awk '/^MemTotal:/{t=$2} /^MemAvailable:/{a=$2} /^Cached:/{c=$2} /^Dirty:/{d=$2} END{printf "%d %d %.1f %.1f", (t-a)/1048576, t/1048576, c/1048576, d/1024}' /proc/meminfo)

    # Network
    cur_rx=$(cat /sys/class/net/$IFACE/statistics/rx_bytes)
    cur_tx=$(cat /sys/class/net/$IFACE/statistics/tx_bytes)
    elapsed_ns=$((now_ns - prev_time))
    elapsed_s=$(echo "$elapsed_ns" | awk '{printf "%.9f", $1/1000000000}')

    rx_gbps=$(echo "$cur_rx $prev_rx $elapsed_s" | awk '{printf "%.2f", ($1-$2)*8/1000000000/$3}')
    tx_gbps=$(echo "$cur_tx $prev_tx $elapsed_s" | awk '{printf "%.2f", ($1-$2)*8/1000000000/$3}')

    prev_rx=$cur_rx
    prev_tx=$cur_tx
    prev_time=$now_ns

    # Disk (sectors * 512 bytes)
    if [ "$DISK_MONITORING" = true ]; then
        cur_disk_write=$(awk "/[[:space:]]${DISK_DEV}[[:space:]]/{print \$10}" /proc/diskstats)
        cur_disk_read=$(awk "/[[:space:]]${DISK_DEV}[[:space:]]/{print \$6}" /proc/diskstats)
        disk_write_mbs=$(echo "$cur_disk_write $prev_disk_write $elapsed_s" | awk '{printf "%.1f", ($1-$2)*512/1048576/$3}')
        disk_read_mbs=$(echo "$cur_disk_read $prev_disk_read $elapsed_s" | awk '{printf "%.1f", ($1-$2)*512/1048576/$3}')
        prev_disk_write=$cur_disk_write
        prev_disk_read=$cur_disk_read
    else
        disk_write_mbs="0.0"
        disk_read_mbs="0.0"
    fi

    echo "$ts,$cpu,$mem_used,$mem_total,$mem_cached,$mem_dirty,$rx_gbps,$tx_gbps,$disk_write_mbs,$disk_read_mbs" >> "$CSV"
done

wait $PID
EXIT_CODE=$?
echo "Process exited with code $EXIT_CODE. Metrics saved to $CSV"
exit $EXIT_CODE

#!/bin/bash
# Usage: ./capture.sh <command to run>
# Example: ./capture.sh /mnt/raid/aws-crt-s3-benchmarks/runners/s3-benchrunner-c/build/s3-benchrunner-c crt-c ...
# Output: metrics.csv in current directory

CSV="metrics_$(date +%Y%m%d_%H%M%S).csv"
IFACE=$(ip route show default | awk '{print $5; exit}')

echo "timestamp,cpu_percent,mem_used_gib,mem_total_gib,mem_cached_gib,mem_dirty_mib,net_rx_gbps,net_tx_gbps,disk_write_mbs,disk_read_mbs" > "$CSV"

# Get initial network counters
prev_rx=$(cat /sys/class/net/$IFACE/statistics/rx_bytes)
prev_tx=$(cat /sys/class/net/$IFACE/statistics/tx_bytes)
prev_time=$(date +%s%N)

# Get initial disk counters from md0 (RAID0 device)
# /proc/diskstats fields: $4=reads_completed $5=reads_merged $6=sectors_read $7=ms_reading
#                         $8=writes_completed $9=writes_merged $10=sectors_written
prev_disk_write=$(awk '/[[:space:]]md0[[:space:]]/{print $10}' /proc/diskstats)
prev_disk_read=$(awk '/[[:space:]]md0[[:space:]]/{print $6}' /proc/diskstats)

# Start the benchmark in background
"$@" &
PID=$!

echo "Monitoring PID $PID, writing to $CSV (interface: $IFACE)"

while kill -0 $PID 2>/dev/null; do
    sleep 1

    ts=$(date +%s.%N)
    now_ns=$(date +%s%N)

    # CPU (system-wide idle from /proc/stat)
    read pu pt <<< $(awk '/^cpu /{print $2+$4, $2+$3+$4+$5+$6+$7+$8; exit}' /proc/stat)
    sleep 0.2
    read cu ct <<< $(awk '/^cpu /{print $2+$4, $2+$3+$4+$5+$6+$7+$8; exit}' /proc/stat)
    dt=$((ct - pt))
    if [ "$dt" -gt 0 ]; then
        cpu=$(awk "BEGIN{printf \"%.1f\", ($cu-$pu)/$dt*100}")
    else
        cpu="0.0"
    fi
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

    # Disk (sectors * 512 bytes, /proc/diskstats: $6=sectors_read, $10=sectors_written)
    cur_disk_write=$(awk '/[[:space:]]md0[[:space:]]/{print $10}' /proc/diskstats)
    cur_disk_read=$(awk '/[[:space:]]md0[[:space:]]/{print $6}' /proc/diskstats)
    disk_write_mbs=$(echo "$cur_disk_write $prev_disk_write $elapsed_s" | awk '{printf "%.1f", ($1-$2)*512/1048576/$3}')
    disk_read_mbs=$(echo "$cur_disk_read $prev_disk_read $elapsed_s" | awk '{printf "%.1f", ($1-$2)*512/1048576/$3}')
    prev_disk_write=$cur_disk_write
    prev_disk_read=$cur_disk_read

    echo "$ts,$cpu,$mem_used,$mem_total,$mem_cached,$mem_dirty,$rx_gbps,$tx_gbps,$disk_write_mbs,$disk_read_mbs" >> "$CSV"
done

wait $PID
EXIT_CODE=$?
echo "Process exited with code $EXIT_CODE. Metrics saved to $CSV"
exit $EXIT_CODE

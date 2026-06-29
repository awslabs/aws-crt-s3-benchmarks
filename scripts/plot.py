#!/usr/bin/env python3
"""Plot benchmark metrics from capture.sh CSV output.
Usage: python3 plot.py metrics_*.csv [--output chart.png]
"""
import sys
import pandas as pd  # type: ignore[import-untyped]
import matplotlib.pyplot as plt  # type: ignore[import-not-found]

csv_path = sys.argv[1]
output = sys.argv[3] if len(
    sys.argv) > 3 and sys.argv[2] == '--output' else csv_path.replace('.csv', '.png')

df = pd.read_csv(csv_path)
df['elapsed'] = df['timestamp'] - df['timestamp'].iloc[0]

fig, axes = plt.subplots(5, 1, figsize=(14, 12), sharex=True)
fig.suptitle(f'Benchmark Metrics: {csv_path}', fontsize=12)

# Network
axes[0].plot(df['elapsed'], df['net_rx_gbps'], label='RX', color='blue')
axes[0].plot(df['elapsed'], df['net_tx_gbps'], label='TX', color='orange')
axes[0].set_ylabel('Gbps')
axes[0].set_title('Network Throughput')
axes[0].legend()
axes[0].grid(True, alpha=0.3)

# Disk
axes[1].plot(df['elapsed'], df['disk_write_mbs'] /
             1024, label='Write', color='red')
axes[1].plot(df['elapsed'], df['disk_read_mbs'] /
             1024, label='Read', color='green')
axes[1].set_ylabel('GiB/s')
axes[1].set_title('Disk I/O (actual device writes)')
axes[1].legend()
axes[1].grid(True, alpha=0.3)

# CPU
axes[2].plot(df['elapsed'], df['cpu_percent'], color='purple')
axes[2].set_ylabel('%')
axes[2].set_title('CPU Usage')
axes[2].grid(True, alpha=0.3)

# Memory
axes[3].plot(df['elapsed'], df['mem_used_gib'], label='Used', color='brown')
axes[3].plot(df['elapsed'], df['mem_cached_gib'], label='Cached', color='cyan')
axes[3].axhline(y=df['mem_total_gib'].iloc[0],
                color='gray', linestyle='--', label='Total')
axes[3].set_ylabel('GiB')
axes[3].set_title('Memory Usage')
axes[3].legend()
axes[3].grid(True, alpha=0.3)

# Dirty pages
axes[4].plot(df['elapsed'], df['mem_dirty_mib'], color='darkred')
axes[4].set_ylabel('MiB')
axes[4].set_title('Page Cache Dirty (pending writeback)')
axes[4].set_xlabel('Time (seconds)')
axes[4].grid(True, alpha=0.3)

plt.tight_layout()
plt.savefig(output, dpi=150, bbox_inches='tight')
print(f"Saved: {output}")

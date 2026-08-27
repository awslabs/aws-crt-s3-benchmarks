# cli-bench: Boto3 S3 Benchmarking on Low-Power EC2

Benchmark boto3-classic and boto3-crt S3 transfer performance across a range of EC2 instance types to establish baselines on weaker systems.

## Quick Start

### On each EC2 instance (after SSH):

```bash
# One-command setup (installs deps, clones repo, preps S3 files)
cd ~
git clone --branch cli-benchmarking https://github.com/awslabs/aws-crt-s3-benchmarks.git ~/benchmark/aws-crt-s3-benchmarks
cd ~/benchmark/aws-crt-s3-benchmarks/cli-bench
chmod +x setup-ec2.sh run-benchmarks.sh
./setup-ec2.sh <BUCKET> <REGION>

# Run benchmarks (activate venv first)
source ~/benchmark/.venv/bin/activate
./run-benchmarks.sh <BUCKET> <REGION> <THROUGHPUT_GBPS>
```

### Throughput by instance type:

| Instance | Throughput arg |
|----------|---------------|
| t2.micro | 0.5 |
| t3.micro | 5.0 |
| t3.small | 5.0 |
| t3.medium | 5.0 |
| m5.large | 10.0 |
| m5.xlarge | 10.0 |
| c5.xlarge | 10.0 |
| c5n.large | 25.0 |

### Collect results (from your laptop):

```bash
# For each instance:
scp -i <key> -r ec2-user@<ip>:~/benchmark/results/<instance-id> ./results/

# Aggregate all:
python3 aggregate-results.py ./results --csv summary.csv --json summary.json
```

## Files

| File | Purpose |
|------|---------|
| `setup-ec2.sh` | One-time EC2 instance setup (packages, venv, S3 prep) |
| `run-benchmarks.sh` | Run all benchmarks, collect metrics, output JSON |
| `bench-wrapper.py` | Python wrapper adding tracemalloc heap measurement |
| `aggregate-results.py` | Combine results from all instances into CSV/JSON |

## What's Measured

| Metric | Method |
|--------|--------|
| Throughput (Gb/s) | Runner's per-run output |
| Peak RSS (KiB) | GNU `/usr/bin/time` |
| CPU % | (user + sys) / wall × 100 |
| Python heap peak | `tracemalloc` |
| Per-run timing | Individual run seconds and throughput |

## Output Format

Each benchmark produces a JSON file:
```
~/benchmark/results/<instance-id>/<runner>_<workload>.json
```

Example:
```json
{
    "metadata": {
        "instance_id": "i-0abc123",
        "instance_type": "t3.micro",
        "runner": "boto3-classic",
        "workload": "download-5GiB-1x",
        ...
    },
    "summary": {
        "throughput_gbps": 0.842,
        "peak_rss_kib": 245760,
        "cpu_percent": 34.2,
        "python_heap_peak_bytes": 52428800,
        ...
    },
    "runs": [
        {"run": 1, "secs": 47.5, "gbps": 0.842},
        ...
    ]
}
```

## Workloads

- `download-256KiB-10_000x` / `upload-256KiB-10_000x` — 10k small files (AI/ML pattern)
- `download-5GiB-1x` / `upload-5GiB-1x` — single large file
- `download-30GiB-1x` / `upload-30GiB-1x` — single very large file

All use `filesOnDisk: true`.

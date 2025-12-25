#!/bin/zsh

# S3 Benchmark Runner Script
# Runs multiple workloads with both regular and WithResponse APIs
# Saves each run's output to a unique file

# Configurable variables
BUCKET="${BUCKET:-multibucketgarrett}"
REGION="${REGION:-us-west-2}"
TARGET_THROUGHPUT="${TARGET_THROUGHPUT:-100}"
PROJECT_PATH="/home/ec2-user/aws-crt-s3-benchmarks/runners/s3-benchrunner-dotnet/S3BenchRunner"
WORKLOADS_PATH="/home/ec2-user/aws-crt-s3-benchmarks/workloads"

# Create results directory with timestamp
TIMESTAMP=$(date +"%Y%m%d-%H%M%S")
RESULTS_DIR="results-${TIMESTAMP}"
mkdir -p "$RESULTS_DIR"

echo "=========================================="
echo "S3 .NET SDK Benchmark Runner"
echo "=========================================="
echo "Bucket: $BUCKET"
echo "Region: $REGION"
echo "Target Throughput: $TARGET_THROUGHPUT Gbps"
echo "Results Directory: $RESULTS_DIR"
echo "=========================================="
echo ""

# Array of workloads to test
WORKLOADS=(
    "download-5GiB-1x.run.json"
    "download-5GiB-1x-ram.run.json"
    "download-30GiB-1x.run.json"
    "download-30GiB-1x-ram.run.json"
)

# Track progress
TOTAL_RUNS=$((${#WORKLOADS[@]} * 2))
CURRENT_RUN=0

# Run each workload with both API variants
for WORKLOAD in "${WORKLOADS[@]}"; do
    # Extract base name without extension
    BASENAME="${WORKLOAD%.run.json}"
    
    # Run with regular APIs
    CURRENT_RUN=$((CURRENT_RUN + 1))
    OUTPUT_FILE="${RESULTS_DIR}/${BASENAME}-regular.log"
    echo "[$CURRENT_RUN/$TOTAL_RUNS] Running $WORKLOAD with regular APIs..."
    echo "Output: $OUTPUT_FILE"
    echo "=== Running $WORKLOAD with regular APIs ===" > "$OUTPUT_FILE"
    echo "Start time: $(date)" >> "$OUTPUT_FILE"
    echo "" >> "$OUTPUT_FILE"
    
    dotnet run -c Release --project "$PROJECT_PATH" -- sdk-dotnet-tm "$WORKLOADS_PATH/$WORKLOAD" "$BUCKET" "$REGION" "$TARGET_THROUGHPUT" false 2>&1 | tee -a "$OUTPUT_FILE"
    
    echo "" >> "$OUTPUT_FILE"
    echo "End time: $(date)" >> "$OUTPUT_FILE"
    echo ""
    
    # Run with WithResponse APIs
    CURRENT_RUN=$((CURRENT_RUN + 1))
    OUTPUT_FILE="${RESULTS_DIR}/${BASENAME}-withresponse.log"
    echo "[$CURRENT_RUN/$TOTAL_RUNS] Running $WORKLOAD with WithResponse APIs..."
    echo "Output: $OUTPUT_FILE"
    echo "=== Running $WORKLOAD with WithResponse APIs ===" > "$OUTPUT_FILE"
    echo "Start time: $(date)" >> "$OUTPUT_FILE"
    echo "" >> "$OUTPUT_FILE"
    
    dotnet run -c Release --project "$PROJECT_PATH" -- sdk-dotnet-tm "$WORKLOADS_PATH/$WORKLOAD" "$BUCKET" "$REGION" "$TARGET_THROUGHPUT" true 2>&1 | tee -a "$OUTPUT_FILE"
    
    echo "" >> "$OUTPUT_FILE"
    echo "End time: $(date)" >> "$OUTPUT_FILE"
    echo ""
done

echo "=========================================="
echo "All benchmarks completed!"
echo "Results saved to: $RESULTS_DIR"
echo "=========================================="
echo ""
echo "Output files:"
ls -lh "$RESULTS_DIR"

# S3 Benchmark Runner Script (PowerShell version)
# Runs multiple workloads with both regular and WithResponse APIs
# Saves each run's output to a unique file

param(
    [string]$Bucket = "multibucketgarrett",
    [string]$Region = "us-west-2",
    [double]$TargetThroughput = 100,
    [string]$ProjectPath = "C:\dev\repos\aws-crt-s3-benchmarks\runners\s3-benchrunner-dotnet\S3BenchRunner",
    [string]$WorkloadsPath = "C:\dev\repos\aws-crt-s3-benchmarks\workloads"
)

# Create results directory with timestamp
$Timestamp = Get-Date -Format "yyyyMMdd-HHmmss"
$ResultsDir = "results-$Timestamp"
New-Item -ItemType Directory -Path $ResultsDir -Force | Out-Null

Write-Host "==========================================" -ForegroundColor Cyan
Write-Host "S3 .NET SDK Benchmark Runner" -ForegroundColor Cyan
Write-Host "==========================================" -ForegroundColor Cyan
Write-Host "Bucket: $Bucket"
Write-Host "Region: $Region"
Write-Host "Target Throughput: $TargetThroughput Gbps"
Write-Host "Results Directory: $ResultsDir"
Write-Host "==========================================" -ForegroundColor Cyan
Write-Host ""

# Array of workloads to test
$Workloads = @(
    "download-5GiB-1x.run.json",
    "download-5GiB-1x-ram.run.json",
    "download-30GiB-1x.run.json",
    "download-30GiB-1x-ram.run.json"
)

# Track progress
$TotalRuns = $Workloads.Count * 2
$CurrentRun = 0

# Run each workload with both API variants
foreach ($Workload in $Workloads) {
    # Extract base name without extension
    $BaseName = $Workload -replace '\.run\.json$', ''
    
    # Run with regular APIs
    $CurrentRun++
    $OutputFile = Join-Path $ResultsDir "$BaseName-regular.log"
    Write-Host "[$CurrentRun/$TotalRuns] Running $Workload with regular APIs..." -ForegroundColor Yellow
    Write-Host "Output: $OutputFile" -ForegroundColor Gray
    
    "=== Running $Workload with regular APIs ===" | Out-File -FilePath $OutputFile -Encoding UTF8
    "Start time: $(Get-Date)" | Out-File -FilePath $OutputFile -Append -Encoding UTF8
    "" | Out-File -FilePath $OutputFile -Append -Encoding UTF8
    
    dotnet run -c Release --project $ProjectPath -- sdk-dotnet-tm "$WorkloadsPath\$Workload" $Bucket $Region $TargetThroughput $false 2>&1 | Tee-Object -FilePath $OutputFile -Append
    
    "" | Out-File -FilePath $OutputFile -Append -Encoding UTF8
    "End time: $(Get-Date)" | Out-File -FilePath $OutputFile -Append -Encoding UTF8
    Write-Host ""
    
    # Run with WithResponse APIs
    $CurrentRun++
    $OutputFile = Join-Path $ResultsDir "$BaseName-withresponse.log"
    Write-Host "[$CurrentRun/$TotalRuns] Running $Workload with WithResponse APIs..." -ForegroundColor Yellow
    Write-Host "Output: $OutputFile" -ForegroundColor Gray
    
    "=== Running $Workload with WithResponse APIs ===" | Out-File -FilePath $OutputFile -Encoding UTF8
    "Start time: $(Get-Date)" | Out-File -FilePath $OutputFile -Append -Encoding UTF8
    "" | Out-File -FilePath $OutputFile -Append -Encoding UTF8
    
    dotnet run -c Release --project $ProjectPath -- sdk-dotnet-tm "$WorkloadsPath\$Workload" $Bucket $Region $TargetThroughput $true 2>&1 | Tee-Object -FilePath $OutputFile -Append
    
    "" | Out-File -FilePath $OutputFile -Append -Encoding UTF8
    "End time: $(Get-Date)" | Out-File -FilePath $OutputFile -Append -Encoding UTF8
    Write-Host ""
}

Write-Host "==========================================" -ForegroundColor Green
Write-Host "All benchmarks completed!" -ForegroundColor Green
Write-Host "Results saved to: $ResultsDir" -ForegroundColor Green
Write-Host "==========================================" -ForegroundColor Green
Write-Host ""
Write-Host "Output files:" -ForegroundColor Cyan
Get-ChildItem $ResultsDir | Format-Table Name, Length, LastWriteTime -AutoSize

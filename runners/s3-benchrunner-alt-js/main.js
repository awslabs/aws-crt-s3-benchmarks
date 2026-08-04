#!/usr/bin/env node

import { readFileSync, writeFileSync, mkdirSync } from "node:fs";
import { basename, dirname, join } from "node:path";
import { performance } from "node:perf_hooks";
import { S3UploadManager } from "@aws-sdk/lib-tm-alt";

const args = process.argv.slice(2);
if (args.length < 5) {
  console.error("Usage: main.js S3_CLIENT WORKLOAD BUCKET REGION FILES_DIR [MAX_CONCURRENCY] [TARGET_PART_SIZE_MB]");
  process.exit(1);
}

const [s3ClientId, workloadPath, bucket, region, filesDir, maxConcurrencyStr, targetPartSizeMBStr] = args;

process.chdir(filesDir);
const maxConcurrency = maxConcurrencyStr ? parseInt(maxConcurrencyStr, 10) : 256;
const targetPartSizeBytes = targetPartSizeMBStr ? parseInt(targetPartSizeMBStr, 10) * 1024 * 1024 : 64 * 1024 * 1024;
const workers = 64;
const concurrencyPerWorker = Math.max(1, Math.floor(maxConcurrency / workers));

if (s3ClientId !== "sdk-js-tm-alt") {
  console.error(`FAIL - Unknown S3_CLIENT: ${s3ClientId}. Options: sdk-js-tm-alt`);
  process.exit(255);
}

const config = JSON.parse(readFileSync(workloadPath, "utf-8"));
if (config.version !== 2) {
  console.error(`Skipping benchmark - workload version not supported: ${config.version}`);
  process.exit(123);
}

const totalBytes = config.tasks.reduce((sum, t) => sum + t.size, 0);
const results = [];
const appStart = performance.now();

const WARMUP_RUNS = 1;
const DELAY_BETWEEN_RUNS_MS = 500;

for (let runI = 0; runI < config.maxRepeatCount + WARMUP_RUNS; runI++) {
  const uploadTasks = config.tasks.filter((t) => t.action === "upload");

  const runStart = performance.now();

  for (const task of uploadTasks) {
    const um = new S3UploadManager({
      bucket,
      region,
      uploadSource: config.filesOnDisk ? "file" : "memory",
      sourceFilePath: config.filesOnDisk ? task.key : null,
      workers,
      concurrency: concurrencyPerWorker,
      partSize: targetPartSizeBytes,
    });
    await um.upload({ keys: [task.key], sizes: { [task.key]: task.size } });
    um.close();
  }

  const runSecs = (performance.now() - runStart) / 1000;
  const gbps = (totalBytes * 8) / (runSecs * 1_000_000_000);

  if (runI < WARMUP_RUNS) {
    console.log(`Warmup:${runI + 1} Secs:${runSecs.toFixed(6)} Gb/s:${gbps.toFixed(6)} (discarded)`);
  } else {
    const recordedRun = runI - WARMUP_RUNS + 1;
    results.push({ run: recordedRun, secs: runSecs, gbps });
    console.log(`Run:${recordedRun} Secs:${runSecs.toFixed(6)} Gb/s:${gbps.toFixed(6)}`);
  }

  if ((performance.now() - appStart) / 1000 >= config.maxRepeatSecs) {
    break;
  }

  // Small delay between runs to let GC and connections settle
  await new Promise((r) => setTimeout(r, DELAY_BETWEEN_RUNS_MS));
}

// Write results to disk
const scriptDir = dirname(new URL(import.meta.url).pathname);
const resultsDir = join(scriptDir, "results");
mkdirSync(resultsDir, { recursive: true });

function formatBytes(bytes) {
  if (bytes >= 1024 * 1024 * 1024) return `${(bytes / (1024 * 1024 * 1024)).toFixed(1)}GiB`;
  if (bytes >= 1024 * 1024) return `${(bytes / (1024 * 1024)).toFixed(1)}MiB`;
  if (bytes >= 1024) return `${(bytes / 1024).toFixed(1)}KiB`;
  return `${bytes}B`;
}

const workloadName = basename(workloadPath, ".run.json");
const lines = [
  `Workload: ${workloadName}`,
  `Workers: ${workers}`,
  `MaxConcurrency: ${maxConcurrency} (${concurrencyPerWorker} per worker)`,
  `TargetPartSize: ${formatBytes(targetPartSizeBytes)}`,
  `TotalBytes: ${formatBytes(totalBytes)}`,
  "",
  ...results.map((r) => `Run:${r.run} Secs:${r.secs.toFixed(6)} Gb/s:${r.gbps.toFixed(6)}`),
];

const timestamp = new Date().toISOString().replace(/[:.]/g, "-");
const outputPath = join(resultsDir, `result-${timestamp}.txt`);
writeFileSync(outputPath, lines.join("\n") + "\n");
console.log(`Results written to ${outputPath}`);

#!/usr/bin/env node

import { readFileSync, writeFileSync, createReadStream, statSync, mkdirSync } from "node:fs";
import { basename } from "node:path";
import { dirname, join } from "node:path";
import { performance } from "node:perf_hooks";
import crypto from "node:crypto";
import { S3Client } from "@aws-sdk/client-s3";
import { S3TransferManager } from "@aws-sdk/lib-transfer-manager";

const args = process.argv.slice(2);
if (args.length < 5) {
  console.error("Usage: main.js S3_CLIENT WORKLOAD BUCKET REGION FILES_DIR [MAX_CONCURRENCY] [TARGET_PART_SIZE_MB]");
  process.exit(1);
}

const [s3ClientId, workloadPath, bucket, region, filesDir, maxConcurrencyStr, targetPartSizeMBStr] = args;

// Change to files directory so relative task keys resolve correctly
process.chdir(filesDir);
const maxConcurrency = maxConcurrencyStr ? parseInt(maxConcurrencyStr, 10) : undefined;
const targetPartSizeBytes = targetPartSizeMBStr ? parseInt(targetPartSizeMBStr, 10) * 1024 * 1024 : undefined;

if (s3ClientId !== "sdk-js-tm") {
  console.error(`FAIL - Unknown S3_CLIENT: ${s3ClientId}. Options: sdk-js-tm`);
  process.exit(255);
}

const config = JSON.parse(readFileSync(workloadPath, "utf-8"));
if (config.version !== 2) {
  console.error(`Skipping benchmark - workload version not supported: ${config.version}`);
  process.exit(123);
}

const totalBytes = config.tasks.reduce((sum, t) => sum + t.size, 0);
const s3Client = new S3Client({ region });
const tm = new S3TransferManager({ s3: s3Client, maxConcurrency, targetPartSizeBytes });

const results = [];
const appStart = performance.now();
const WARMUP_RUNS = 1;
const DELAY_BETWEEN_RUNS_MS = 500;

for (let i = 0; i < config.maxRepeatCount + WARMUP_RUNS; i++) {
  const runStart = performance.now();

  // Run uploads
  const uploads = config.tasks
    .filter((t) => t.action === "upload")
    .map((task) => {
      const params = { Bucket: bucket, Key: task.key };
      if (config.filesOnDisk) {
        params.Body = createReadStream(task.key);
        params.ContentLength = statSync(task.key).size;
      } else {
        params.Body = crypto.randomBytes(task.size);
        params.ContentLength = task.size;
      }
      return tm.upload(params);
    });

  await Promise.all(uploads);

  const runSecs = (performance.now() - runStart) / 1000;
  const gbps = (totalBytes * 8) / (runSecs * 1_000_000_000);

  if (i < WARMUP_RUNS) {
    console.log(`Warmup:${i + 1} Secs:${runSecs.toFixed(6)} Gb/s:${gbps.toFixed(6)} (discarded)`);
  } else {
    const recordedRun = i - WARMUP_RUNS + 1;
    results.push({ run: recordedRun, secs: runSecs, gbps });
    console.log(`Run:${recordedRun} Secs:${runSecs.toFixed(6)} Gb/s:${gbps.toFixed(6)}`);
  }

  if ((performance.now() - appStart) / 1000 >= config.maxRepeatSecs) {
    break;
  }

  // Small delay between runs to let GC and connections settle
  await new Promise((r) => setTimeout(r, DELAY_BETWEEN_RUNS_MS));
}

s3Client.destroy();

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
  `MaxConcurrency: ${maxConcurrency ?? "default"}`,
  `TargetPartSize: ${targetPartSizeBytes ? formatBytes(targetPartSizeBytes) : "default"}`,
  `TotalBytes: ${formatBytes(totalBytes)}`,
  "",
  ...results.map((r) => `Run:${r.run} Secs:${r.secs.toFixed(6)} Gb/s:${r.gbps.toFixed(6)}`),
];

const timestamp = new Date().toISOString().replace(/[:.]/g, "-");
const outputPath = join(resultsDir, `result-${timestamp}.txt`);
writeFileSync(outputPath, lines.join("\n") + "\n");
console.log(`Results written to ${outputPath}`);

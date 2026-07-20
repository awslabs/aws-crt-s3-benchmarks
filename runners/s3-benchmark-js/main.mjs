#!/usr/bin/env node

import { readFileSync, createReadStream, mkdirSync, appendFileSync } from "fs";
import { basename } from "path";
import { randomBytes } from "crypto";
import { S3TransferManager } from "@aws-sdk/lib-transfer-manager";

function bytesToGigabits(bytes) {
  return (bytes * 8) / 1_000_000_000;
}

function parseArgs() {
  const argv = process.argv.slice(2);
  const positional = [];
  let sourceFile = null;
  for (let i = 0; i < argv.length; i++) {
    if (argv[i] === "--source-file") {
      sourceFile = argv[++i];
    } else {
      positional.push(argv[i]);
    }
  }
  if (positional.length < 4) {
    console.error(
      "Usage: node main.mjs [--source-file PATH] S3_CLIENT WORKLOAD BUCKET REGION"
    );
    process.exit(1);
  }
  return {
    s3Client: positional[0],
    workloadPath: positional[1],
    bucket: positional[2],
    region: positional[3],
    sourceFile,
  };
}

function loadWorkload(workloadPath) {
  return JSON.parse(readFileSync(workloadPath, "utf-8"));
}

async function runTask(tm, task, bucket, filesOnDisk, randomData, sourceFile) {
  const body = filesOnDisk
    ? createReadStream(sourceFile ?? task.key)
    : randomData.subarray(0, task.size);

  await tm.upload({
    Bucket: bucket,
    Key: task.key,
    Body: body,
    ContentLength: task.size,
  });
}

async function runAllTasks(tm, tasks, bucket, filesOnDisk, randomData, sourceFile) {
  await Promise.all(
    tasks.map((task) => runTask(tm, task, bucket, filesOnDisk, randomData, sourceFile))
  );
}

async function main() {
  const { s3Client, workloadPath, bucket, region, sourceFile } = parseArgs();
  const workload = loadWorkload(workloadPath);

  // Pre-generate random data in a SharedArrayBuffer so the TM can pass it
  // directly to worker threads without an extra copy.
  let randomData = new Uint8Array(0);
  if (!workload.filesOnDisk) {
    const maxUploadSize = workload.tasks
      .filter((t) => t.action === "upload")
      .reduce((max, t) => Math.max(max, t.size), 0);
    if (maxUploadSize > 0) {
      const sab = new SharedArrayBuffer(maxUploadSize);
      randomData = new Uint8Array(sab);
      for (let offset = 0; offset < maxUploadSize; offset += 1024 * 1024 * 1024) {
        const chunkSize = Math.min(1024 * 1024 * 1024, maxUploadSize - offset);
        randomData.set(randomBytes(chunkSize), offset);
      }
    }
  }

  let bytesPerRun = 0;
  for (const task of workload.tasks) {
    bytesPerRun += task.size;
  }

  process.env.AWS_REGION = region;

  const tm = new S3TransferManager({
    requestChecksumCalculation: "WHEN_SUPPORTED",
    workerThreadCount: 64,
    maxConcurrentUploads: 256,
    targetPartSizeBytes: 16 * 1024 * 1024,
  });

  const workerThreadCount = 64;
  const partSizeMiB = 16;
  const checksum = workload.checksum === true ? "CRC32" : (workload.checksum || "none");

  const header = `\n=== Workload: ${workloadPath} ===\nWorkers: ${workerThreadCount} | Concurrency per worker: ${Math.ceil(256 / workerThreadCount)} | Part size: ${partSizeMiB} MiB | Checksum: ${checksum}\nDate: ${new Date().toISOString()}\n`;
  console.log(header);

  const resultsDir = new URL("./results", import.meta.url).pathname;
  mkdirSync(resultsDir, { recursive: true });
  const workloadName = basename(workloadPath, ".run.json");
  const resultsFile = `${resultsDir}/${workloadName}.txt`;
  appendFileSync(resultsFile, header);

  const appStart = performance.now();
  for (let run = 1; run <= workload.maxRepeatCount; run++) {
    const runStart = performance.now();
    await runAllTasks(tm, workload.tasks, bucket, workload.filesOnDisk, randomData, sourceFile);
    const runSecs = (performance.now() - runStart) / 1000;
    const gbps = bytesToGigabits(bytesPerRun) / runSecs;

    const line = `Run:${run} Secs:${runSecs.toFixed(6)} Gb/s:${gbps.toFixed(6)}`;
    console.log(line);
    appendFileSync(resultsFile, line + "\n");

    if ((performance.now() - appStart) / 1000 >= workload.maxRepeatSecs) break;
  }

  // Destroy the TM's internal S3 client to terminate worker threads gracefully,
  // allowing them to flush CPU profiles before exit.
  tm.s3?.config?.requestHandler?.destroy?.();
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});

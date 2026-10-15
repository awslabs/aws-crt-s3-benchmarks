/**
 * Exercises the AWS SDK for JavaScript v3 S3 Transfer Manager
 * (`@aws-sdk/lib-transfer-manager`) across the harness's JSON workloads,
 * covering both uploads and downloads, each with a disk-based and a RAM-only
 * path selected by the workload's `filesOnDisk` flag:
 *   - upload,   filesOnDisk: true  -> S3TransferManager.upload         (multipart, body streamed from file)
 *   - upload,   filesOnDisk: false -> S3TransferManager.upload         (multipart, body from in-memory buffer)
 *   - download, filesOnDisk: true  -> S3TransferManager.downloadToFile (file-based path)
 *   - download, filesOnDisk: false -> S3TransferManager.downloadToNull (worker-side discard path)
 *
 * Usage:
 *   node index.js S3_CLIENT WORKLOAD BUCKET REGION TARGET_THROUGHPUT [--verbose]
 *
 *   S3_CLIENT: "sdk-js"
 *   WORKLOAD:  path to a workload .run.json file
 *
 * Environment variables:
 *   MAX_CONCURRENCY:    max concurrent part requests per transfer.
 *                       Default: number of vCPUs x 4.
 *   OBJECT_CONCURRENCY: max number of objects (tasks) transferred at once.
 *                       Bounds total buffered memory to prevent OOM on large
 *                       fan-out workloads (e.g. download-max-throughput =
 *                       100 x 5 GiB). Default: unbounded.
 *   USE_O_DIRECT:       set to true for Linux disk-download benchmarks. Uses
 *                       RANGE requests and attempts O_DIRECT worker writes;
 *                       unsupported filesystems fall back to buffered I/O.
 *
 * Must be run from the FILES_DIR passed to prep-s3-files.py, because task keys
 * double as relative file paths on disk.
 *
 * Output (one line per run, matching the other runners):
 *   Run:1 Secs:1.234567 Gb/s:12.345678
 *
 * Per-run peak RSS and average CPU are recorded in the JSON results file (not
 * printed to stdout).
 *
 * Exit codes: 123 = workload skipped (e.g. unsupported version), 255 = error.
 */

import { S3Client } from "@aws-sdk/client-s3";
import { Upload } from "@aws-sdk/lib-storage";
import { S3TransferManager } from "@aws-sdk/lib-transfer-manager";
import { createReadStream, mkdirSync, readFileSync, writeFileSync } from "node:fs";
import { mkdir, rm } from "node:fs/promises";
import os from "node:os";
import { basename, dirname, join } from "node:path";
import { performance } from "node:perf_hooks";
import { fileURLToPath } from "node:url";


const S3_CLIENT_IDS = ["sdk-js", "sdk-js-lib-storage"];
const SKIP_EXIT_CODE = 123;
const ERROR_EXIT_CODE = 255;

// Directory containing this runner. Results are written here (under results/)
// regardless of the current working directory the benchmark is launched from.
const RUNNER_DIR = dirname(fileURLToPath(import.meta.url));


const MULTIPART_THRESHOLD_BYTES = 5 * 1024 * 1024;
// Target multipart part size. Override with PART_SIZE_MIB (in MiB).
const TARGET_PART_SIZE_BYTES = (process.env.PART_SIZE_MIB
  ? Math.max(5, Number.parseInt(process.env.PART_SIZE_MIB, 10) || 16)
  : 16) * 1024 * 1024;
// Max concurrent part requests per transfer (transfer-manager default is 32).
// Defaults to (vCPUs x 4).
const VCPUS =
  typeof os.availableParallelism === "function" ? os.availableParallelism() : os.cpus().length;
const MAX_CONCURRENCY = process.env.MAX_CONCURRENCY
  ? Math.max(1, Number.parseInt(process.env.MAX_CONCURRENCY, 10) || 1)
  : VCPUS * 4;
// Max number of OBJECTS (tasks) transferred at once. Unset => no cap (all tasks
// run concurrently).
const OBJECT_CONCURRENCY = process.env.OBJECT_CONCURRENCY
  ? Math.max(1, Number.parseInt(process.env.OBJECT_CONCURRENCY, 10) || 1)
  : null;
// O_DIRECT is only used for disk-backed downloads. RANGE mode gives worker
// writes predictable 8 MiB, 4 KiB-aligned offsets; PART mode follows the
// source object's multipart layout and cannot guarantee that alignment.
const USE_O_DIRECT = process.env.USE_O_DIRECT === "true";
const MULTIPART_DOWNLOAD_TYPE = USE_O_DIRECT ? "RANGE" : "PART";

const exitWithSkip = (msg) => {
  process.stderr.write(`Skipping benchmark - ${msg}\n`);
  process.exit(SKIP_EXIT_CODE);
};
const exitWithError = (msg) => {
  process.stderr.write(`FAIL - ${msg}\n`);
  process.exit(ERROR_EXIT_CODE);
};

const msToSecs = (ms) => ms / 1000.0;
const bytesToGigabit = (bytes) => (bytes * 8) / 1_000_000_000.0;
const bytesToMiB = (bytes) => bytes / (1024 * 1024);

// Cheap RSS read (fast path added in Node 15.6) with a fallback.
const readRssBytes =
  typeof process.memoryUsage.rss === "function"
    ? () => process.memoryUsage.rss()
    : () => process.memoryUsage().rss;

/**
 * Samples process resource usage across a single timed run:
 *   - peak RSS: max resident set size seen (polled on an interval).
 *   - average CPU: total CPU time (user+system, including worker threads, since
 *     process.cpuUsage covers the whole OS process) divided by wall time.
 *   - event-loop utilization (ELU): fraction of wall time the MAIN thread's
 *     event loop was busy (0..1). Near 1.0 means the main thread is the
 *     bottleneck (orchestration/stream plumbing), even if total CPU looks low.
 * Reported both as cores-busy (e.g. 8.3 = ~8.3 fully-used cores on average) and
 * as a percentage of all vCPUs on the machine.
 */
class RunSampler {
  constructor(intervalMs = 100) {
    this.intervalMs = intervalMs;
    this.peakRssBytes = 0;
    this.cpuUserMicros = 0;
    this.cpuSystemMicros = 0;
    this.wallMs = 0;
    this.eluBusy = 0;
    this._timer = null;
    this._cpuStart = null;
    this._wallStartMs = 0;
    this._eluStart = null;
  }

  start() {
    this._cpuStart = process.cpuUsage();
    this._wallStartMs = performance.now();
    this._eluStart = performance.eventLoopUtilization();
    this.peakRssBytes = readRssBytes();
    this._timer = setInterval(() => {
      const rss = readRssBytes();
      if (rss > this.peakRssBytes) this.peakRssBytes = rss;
    }, this.intervalMs);
    // Don't let the sampler keep the event loop alive on its own.
    if (typeof this._timer.unref === "function") this._timer.unref();
  }

  stop() {
    if (this._timer) {
      clearInterval(this._timer);
      this._timer = null;
    }
    const cpu = process.cpuUsage(this._cpuStart);
    this.cpuUserMicros = cpu.user;
    this.cpuSystemMicros = cpu.system;
    this.wallMs = performance.now() - this._wallStartMs;
    // ELU over the window: utilization field is already the 0..1 busy fraction.
    this.eluBusy = performance.eventLoopUtilization(this._eluStart).utilization;
    const rss = readRssBytes();
    if (rss > this.peakRssBytes) this.peakRssBytes = rss;
  }

  get avgCpuCores() {
    const cpuSecs = (this.cpuUserMicros + this.cpuSystemMicros) / 1_000_000;
    const wallSecs = this.wallMs / 1000;
    return wallSecs > 0 ? cpuSecs / wallSecs : 0;
  }

  get avgCpuPercent() {
    return VCPUS > 0 ? (this.avgCpuCores / VCPUS) * 100 : 0;
  }

  /** Main-thread event-loop utilization over the run, 0..1. */
  get eventLoopUtilization() {
    return this.eluBusy;
  }
}


function deriveAction(tasks) {
  return tasks[0]?.action ?? "unknown";
}

function parseArgs(argv) {
  const verbose = argv.includes("--verbose");
  const args = argv.slice(2).filter((a) => a !== "--verbose");
  if (args.length < 5) {
    exitWithError(
      "usage: index.js S3_CLIENT WORKLOAD BUCKET REGION TARGET_THROUGHPUT [--verbose]\n" +
        `  S3_CLIENT must be one of: ${S3_CLIENT_IDS.join(", ")}`
    );
  }
  const [s3Client, workload, bucket, region, targetThroughput] = args;
  if (!S3_CLIENT_IDS.includes(s3Client)) {
    exitWithError(`Unknown S3 client: ${s3Client}. Expected one of: ${S3_CLIENT_IDS.join(", ")}`);
  }
  const targetThroughputGbps = Number.parseFloat(targetThroughput);
  if (!Number.isFinite(targetThroughputGbps)) {
    exitWithError(`TARGET_THROUGHPUT must be a number, got: ${targetThroughput}`);
  }
  return { s3Client, workload, bucket, region, targetThroughputGbps, verbose };
}

function loadWorkload(path) {
  const workload = JSON.parse(readFileSync(path, "utf-8"));
  // Runners MUST skip workloads whose version they don't understand.
  if (workload.version !== 2) {
    exitWithSkip(`workload version not supported: ${workload.version}`);
  }
  return {
    filesOnDisk: workload.filesOnDisk,
    checksum: workload.checksum ?? null, // null | CRC32 | CRC32C | SHA1 | SHA256
    // MAX_REPEAT_COUNT env var overrides the workload's maxRepeatCount when set
    // (useful to limit runs, e.g. MAX_REPEAT_COUNT=3). Falls back to the workload value.
    maxRepeatCount: process.env.MAX_REPEAT_COUNT
      ? Math.max(1, Number.parseInt(process.env.MAX_REPEAT_COUNT, 10) || workload.maxRepeatCount)
      : workload.maxRepeatCount,
    maxRepeatSecs: workload.maxRepeatSecs,
    tasks: workload.tasks.map((t) => ({ action: t.action, key: t.key, size: t.size })),
  };
}

// Runner

class TransferManagerRunner {
  constructor({ s3ClientId, workload, bucket, region, verbose }) {
    this.s3ClientId = s3ClientId;
    this.workload = workload;
    this.bucket = bucket;
    this.verbose = verbose;

    this.s3 = new S3Client({
      region,
      requestChecksumCalculation: "WHEN_REQUIRED",
      responseChecksumValidation: workload.checksum ? "WHEN_SUPPORTED" : "WHEN_REQUIRED",
    });

    this.tm = new S3TransferManager({
      s3: this.s3,
      multipartUploadThresholdBytes: MULTIPART_THRESHOLD_BYTES,
      targetPartSizeBytes: TARGET_PART_SIZE_BYTES,
      multipartDownloadType: MULTIPART_DOWNLOAD_TYPE,
      useODirect: USE_O_DIRECT,
      maxConcurrentDownloads: MAX_CONCURRENCY,
      maxConcurrentUploads: MAX_CONCURRENCY,
    });

    // For RAM uploads (filesOnDisk === false), pre-allocate one SharedArrayBuffer
    // large enough for the biggest upload before timing begins. Each upload passes
    // a zero-copy slice of this SAB-backed Buffer to the transfer manager, allowing
    // its threaded multipart path to share the source directly with workers.
    this.randomUploadData = null;
    if (!workload.filesOnDisk) {
      let largest = 0;
      for (const t of workload.tasks) {
        if (t.action === "upload" && t.size > largest) largest = t.size;
      }
      if (largest > 0) {
        this.randomUploadData = Buffer.from(new SharedArrayBuffer(largest));
      }
    }
  }

  log(msg) {
    if (this.verbose) process.stderr.write(`${msg}\n`);
  }

  bytesPerRun() {
    return this.workload.tasks.reduce((sum, t) => sum + t.size, 0);
  }

  /** Untimed prep between runs: clear pre-existing download targets, make dirs. */
  async prepareRun() {
    this.log("preparing run...");
    for (const t of this.workload.tasks) {
      if (t.action === "download" && this.workload.filesOnDisk) {
        await rm(t.key, { force: true });
        await mkdir(dirname(t.key), { recursive: true });
      }
    }
  }

  async doTask(task) {
    const checksum = this.workload.checksum ?? undefined;

    if (task.action === "upload") {
      const body = this.workload.filesOnDisk
        ? createReadStream(task.key)
        : this.randomUploadData.subarray(0, task.size);
      const uploadParams = {
        Bucket: this.bucket,
        Key: task.key,
        Body: body,
        ContentLength: task.size,
        ...(checksum && { ChecksumAlgorithm: checksum }),
      };
      this.log(`${this.s3ClientId} upload ${task.key} (${task.size} bytes)`);
      if (this.s3ClientId === "sdk-js-lib-storage") {
        await new Upload({
          client: this.s3,
          params: uploadParams,
          queueSize: MAX_CONCURRENCY,
          partSize: TARGET_PART_SIZE_BYTES,
          leavePartsOnError: false,
        }).done();
      } else {
        await this.tm.upload(uploadParams);
      }
      return;
    }

    if (task.action === "download") {
      const request = {
        Bucket: this.bucket,
        Key: task.key,
        ...(checksum && { ChecksumMode: "ENABLED" }),
      };
      if (this.workload.filesOnDisk) {
        this.log(`downloadToFile ${task.key} (${task.size} bytes)`);
        await this.tm.downloadToFile({ ...request, destination: task.key });
      } else {
        this.log(`downloadToNull ${task.key} (${task.size} bytes)`);
        const response = await this.tm.downloadToNull(request);
        if (response.bytesRead !== task.size) {
          throw new Error(
            `Downloaded ${response.bytesRead} bytes for ${task.key}, expected workload size ${task.size}`
          );
        }
      }
      return;
    }

    throw new Error(`Unknown action: ${task.action}`);
  }

  /**
   * Perform all tasks with a bounded number in flight at once
   * (OBJECT_CONCURRENCY). Uses a fixed pool of workers pulling from a shared
   * index so at most N transfers run concurrently — this caps total buffered
   * memory and prevents OOM on large fan-out workloads. Surfaces the first
   * failure and stops dispatching further tasks.
   */
  async run() {
    const tasks = this.workload.tasks;
    const limit = Math.min(OBJECT_CONCURRENCY ?? tasks.length, tasks.length);
    let nextIndex = 0;
    let firstError = null;

    const worker = async () => {
      while (true) {
        if (firstError) return; // stop pulling new work once something failed
        const i = nextIndex++;
        if (i >= tasks.length) return;
        try {
          await this.doTask(tasks[i]);
        } catch (err) {
          if (!firstError) {
            firstError = err;
            process.stderr.write(
              `Failed on task ${i + 1}/${tasks.length}: ${JSON.stringify(tasks[i])}\n`
            );
          }
          return;
        }
      }
    };

    await Promise.all(Array.from({ length: limit }, () => worker()));

    if (firstError) {
      throw firstError;
    }
  }
}

// Main: repeat loop + Gb/s reporting

async function main() {
  const { s3Client: s3ClientId, workload: workloadPath, bucket, region, verbose } = parseArgs(process.argv);
  const workload = loadWorkload(workloadPath);
  const runner = new TransferManagerRunner({ s3ClientId, workload, bucket, region, verbose });
  const bytesPerRun = runner.bytesPerRun();

  /** @type {{run: number, secs: number, gbps: number, peakRssBytes: number, cpuAvgCores: number, cpuAvgPercent: number, elu: number}[]} */
  const runs = [];

  const appStartMs = performance.now();
  for (let runI = 0; runI < workload.maxRepeatCount; runI++) {
    await runner.prepareRun(); // NOT timed

    const sampler = new RunSampler();
    sampler.start();
    const runStartMs = performance.now();
    await runner.run();
    const runSecs = msToSecs(performance.now() - runStartMs);
    sampler.stop();

    const gbps = bytesToGigabit(bytesPerRun) / runSecs;
    const peakRssBytes = sampler.peakRssBytes;
    const cpuAvgCores = sampler.avgCpuCores;
    const cpuAvgPercent = sampler.avgCpuPercent;
    const elu = sampler.eventLoopUtilization;
    runs.push({ run: runI + 1, secs: runSecs, gbps, peakRssBytes, cpuAvgCores, cpuAvgPercent, elu });

    // Keep stdout to one line per run, exactly "Run:N Secs:X Gb/s:Y" (the format
    // scripts/utils/metrics.py parses). Resource metrics go to stderr so they are
    // visible live without disturbing the parsed stdout line.
    process.stdout.write(`Run:${runI + 1} Secs:${runSecs.toFixed(6)} Gb/s:${gbps.toFixed(6)}\n`);
    process.stderr.write(
      `  [run ${runI + 1}] peakRSS=${bytesToMiB(peakRssBytes).toFixed(0)}MiB ` +
        `cpu=${cpuAvgPercent.toFixed(1)}%(${cpuAvgCores.toFixed(1)} cores) ` +
        `elu=${elu.toFixed(3)}\n`
    );

    if (msToSecs(performance.now() - appStartMs) >= workload.maxRepeatSecs) break;
  }

  writeResults(workloadPath, workload, bytesPerRun, runs, runner.s3ClientId);
  process.exit(0);
}

/**
 * Write per-run results plus summary stats to a JSON file under
 * <runner-dir>/results/<workload-name>/<timestamp>.json (override the base dir
 * with the RESULTS_DIR env var). Summary includes throughput
 * (min/max/mean/median/peak Gb/s), peak RSS, and average CPU. Failures to write
 * are logged but do not fail the benchmark.
 */
function writeResults(workloadPath, workload, bytesPerRun, runs, s3ClientId) {
  if (runs.length === 0) return;

  const gbpsValues = runs.map((r) => r.gbps).sort((a, b) => a - b);
  const sum = gbpsValues.reduce((a, b) => a + b, 0);
  const mid = Math.floor(gbpsValues.length / 2);
  const median =
    gbpsValues.length % 2 === 0 ? (gbpsValues[mid - 1] + gbpsValues[mid]) / 2 : gbpsValues[mid];

  const mean = (xs) => xs.reduce((a, b) => a + b, 0) / xs.length;
  const peakRssBytes = Math.max(...runs.map((r) => r.peakRssBytes));
  const avgCpuPercent = mean(runs.map((r) => r.cpuAvgPercent));
  const avgCpuCores = mean(runs.map((r) => r.cpuAvgCores));
  const maxElu = Math.max(...runs.map((r) => r.elu));
  const meanElu = mean(runs.map((r) => r.elu));

  const report = {
    workload: basename(workloadPath),
    s3Client: s3ClientId,
    action: deriveAction(workload.tasks),
    maxConcurrency: MAX_CONCURRENCY,
    objectConcurrency: OBJECT_CONCURRENCY ?? "unbounded",
    multipartDownloadType: MULTIPART_DOWNLOAD_TYPE,
    useODirect: USE_O_DIRECT,
    partSizeBytes: TARGET_PART_SIZE_BYTES,
    numTasks: workload.tasks.length,
    bytesPerRun,
    filesOnDisk: workload.filesOnDisk,
    checksum: workload.checksum,
    vcpus: VCPUS,
    runs,
    summary: {
      numRuns: runs.length,
      minGbps: gbpsValues[0],
      maxGbps: gbpsValues[gbpsValues.length - 1],
      meanGbps: sum / gbpsValues.length,
      medianGbps: median,
      // Peak throughput = best (fastest) run's average Gb/s.
      peakGbps: gbpsValues[gbpsValues.length - 1],
      // Peak RSS across all runs, and CPU averaged across runs.
      peakRssBytes,
      peakRssMiB: bytesToMiB(peakRssBytes),
      avgCpuPercent,
      avgCpuCores,
      maxEventLoopUtilization: maxElu,
      meanEventLoopUtilization: meanElu,
    },
  };

  const workloadName = basename(workloadPath).replace(/\.run\.json$/, "");
  const stamp = new Date().toISOString().replace(/[:.]/g, "-");
  const outDir = join(process.env.RESULTS_DIR ?? join(RUNNER_DIR, "results"), workloadName);
  const outPath = join(outDir, `${stamp}.json`);

  try {
    mkdirSync(outDir, { recursive: true });
    writeFileSync(outPath, JSON.stringify(report, null, 2) + "\n");
    process.stderr.write(`Wrote results to ${outPath}\n`);
  } catch (err) {
    process.stderr.write(`WARNING: failed to write results file: ${err?.message ?? err}\n`);
  }
}

main().catch((err) => exitWithError(err?.stack ?? String(err)));

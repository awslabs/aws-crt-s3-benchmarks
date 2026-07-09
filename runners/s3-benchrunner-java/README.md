# s3-benchrunner-java

s3-benchrunner for [aws-crt-java](https://github.com/awslabs/aws-crt-java).

## Building

```sh
cd aws-crt-s3-benchmarks/runners/s3-benchrunner-java
mvn package
```

This produces the uber-jar: `target/s3-benchrunner-java-1.0-SNAPSHOT.jar` .

### Using a local build of aws-crt-java and aws-sdk-java-v2

By default, the latest release of aws-crt-java and aws-sdk-java-v2 are pulled from Maven Central. If you want to build these locally...

First, install aws-crt-java (this installs version 1.0.0-SNAPSHOT):
```sh
cd my/dev/dir
git clone https://github.com/awslabs/aws-crt-java.git
cd aws-crt-java
git submodule update --init
mvn install -Dmaven.test.skip
```

Next, install the SDK:
```
cd my/dev/dir
git clone https://github.com/aws/aws-sdk-java-v2.git
cd aws-sdk-java-v2
mvn clean install -pl :s3-transfer-manager,:s3,:bom-internal,:bom -P quick --am -Dawscrt.version=1.0.0-SNAPSHOT
```

Finally, build the runner:
```sh
cd /path/to/s3-benchrunner-java
mvn clean package -Dawscrt.version=1.0.0-SNAPSHOT
```

### Working in IntelliJ

Submissions welcome, I'm bad at Java.

### Working in VSCode

Submissions welcome, I'm bad at Java.

## Running

If you built the uber-jar via `mvn package` , the `RUNNER_CMD` is:

 `java -jar path/to/s3-benchrunner-java-1.0-SNAPSHOT.jar [args...]`

and the args you pass are described [here](../README.md#running).

## Optional JVM system properties

The runner honors several JVM system properties for benchmark tuning:

| Property | Default | Effect |
|----------|--------:|--------|
| `-Daws.crt.memory.tracing=1` | auto-enabled | Enables CRT native memory tracking. Required for the `crt_native_peak_mib` field in the memory summary. The runner auto-enables level 1 (totals only, minimal overhead) if not otherwise set. Set to `2` for stack-trace tracking (higher overhead), or `0` to disable. |
| `-Daws.crt.backpressure.window_mib=<N>` | `0` (off) | Enables read backpressure in `crt-java` runner with the given window size in MiB. Set to `80` to match the SDK's default. Set to `0` (default) to disable backpressure entirely (unbounded in-flight bytes). |
| `-Daws.sdk.s3.initial_read_buffer_mib=<N>` | SDK default (partSize × 10 = 80 MiB) | Overrides the SDK CRT client's `initialReadBufferSizeInBytes`. Larger values let more bytes buffer before backpressure kicks in. |

Example: run crt-java with SDK-matching backpressure config:
```sh
java -Daws.crt.backpressure.window_mib=80 -jar s3-benchrunner-java-1.0-SNAPSHOT.jar crt-java ...
```

## Memory + allocation output

Before the machine-readable `STATS:{...}` line, the runner emits a human-readable summary of memory and GC activity:

```
=== JVM Memory & Allocation ===
Peak RSS (total process):        18249.0 MiB
JVM heap peak used:                250 MiB
JVM non-heap used (end):           180 MiB
JVM direct peak used:               82 MiB
CRT native peak (tracked):       17600 MiB
Total heap allocated:              187.3 GiB across all runs
Young GCs:                          342 (425 ms total)
Old GCs:                              2 (18 ms total)
```

Categories:

- **Peak RSS**: total process resident-set high-water mark from `/proc/self/status VmHWM`
- **JVM heap peak used**: sum across all heap pools' peak-usage counters
- **JVM non-heap used**: end-of-run non-heap memory (metaspace, code cache)
- **JVM direct peak used**: peak DirectByteBuffer memory, sampled every 100 ms
- **CRT native peak (tracked)**: peak `CRT.nativeMemory()` reading (only non-zero when `aws.crt.memory.tracing` is enabled — the runner auto-enables it)
- **Total heap allocated**: cumulative bytes allocated across all threads during the runs (from `ThreadMXBean.getTotalThreadAllocatedBytes()`). This is the primary signal for allocation-churn improvements.
- **Young GCs / Old GCs**: collection counts + total pause time across the runs

The same fields also appear in `STATS:{"memory":{...},"allocation":{...}}` for programmatic consumers.

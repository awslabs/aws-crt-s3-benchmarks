# s3-benchrunner-java

s3-benchrunner for [aws-crt-java](https://github.com/awslabs/aws-crt-java).

Supported `S3_CLIENT` values:
- `crt-java` — uses aws-crt-java directly via JNI
- `ffm-java` — uses aws-crt-java via FFM (Foreign Function & Memory API, requires Java 22+)
- `sdk-java-client-crt` — AWS SDK v2 async S3 client backed by CRT
- `sdk-java-tm-crt` — AWS SDK v2 Transfer Manager backed by CRT
- `sdk-java-client-classic` — AWS SDK v2 async S3 client (classic/Netty)
- `sdk-java-tm-classic` — AWS SDK v2 Transfer Manager (classic/Netty)

## Building

```sh
cd aws-crt-s3-benchmarks/runners/s3-benchrunner-java
mvn package
```

This produces the uber-jar: `target/s3-benchrunner-java-1.0-SNAPSHOT.jar` .

> **Note:** Java 22 or newer is required to compile and run this runner.

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

### Using the FFM branch of aws-crt-java (for `ffm-java` benchmarks)

The `ffm-java` s3-client uses an aws-crt-java branch that replaces JNI with the
Java 22 Foreign Function & Memory (FFM) API. Use the `--branch` flag when
building via the scripts to check out the FFM branch automatically:

```sh
# Build and run ffm-java benchmarks using the FFM branch of aws-crt-java
python3 scripts/prep-build-run-benchmarks.py \
  --buckets <YOUR_BUCKET> \
  --region <YOUR_REGION> \
  --throughput <YOUR_THROUGHPUT_GBPS> \
  --build-dir /tmp/build \
  --files-dir /tmp/files \
  --s3-clients ffm-java \
  --branch <FFM_BRANCH_NAME>
```

Or build only:
```sh
python3 scripts/build-runner.py \
  --lang java \
  --build-dir /tmp/build \
  --branch <FFM_BRANCH_NAME>
```

To benchmark JNI vs FFM side-by-side, run both clients in the same invocation:
```sh
python3 scripts/prep-build-run-benchmarks.py \
  --buckets <YOUR_BUCKET> \
  --region <YOUR_REGION> \
  --throughput <YOUR_THROUGHPUT_GBPS> \
  --build-dir /tmp/build \
  --files-dir /tmp/files \
  --s3-clients crt-java ffm-java \
  --branch <FFM_BRANCH_NAME>
```

> **Note:** When both `crt-java` and `ffm-java` are requested in the same run,
> both use the same built jar (the FFM branch). The `crt-java` client will
> therefore also use the FFM-backed library. To compare a JNI build against an
> FFM build, run them in separate invocations with different `--branch` values
> and different `--build-dir` paths.

### Working in IntelliJ

Submissions welcome, I'm bad at Java.

### Working in VSCode

Submissions welcome, I'm bad at Java.

## Running

If you built the uber-jar via `mvn package` , the `RUNNER_CMD` is:

 `java -jar path/to/s3-benchrunner-java-1.0-SNAPSHOT.jar [args...]`

and the args you pass are described [here](../README.md#running).

# s3-benchrunner-java

```
usage: java -jar /path/to/s3-benchrunner-java-1.0-SNAPSHOT.jar {crt-java,sdk-java-client-crt,sdk-java-tm-crt} WORKLOAD BUCKET REGION TARGET_THROUGHPUT

Java benchmark runner. Pick which S3 library to use.

positional arguments:
  {crt-java,sdk-java-client-crt,sdk-java-tm-crt}
  WORKLOAD
  BUCKET
  REGION
  TARGET_THROUGHPUT
```

This is the runner for java libraries. Pass which library you want to benchmark:
* `crt-java`: Uses the [aws-crt-java](https://github.com/awslabs/aws-crt-java/) (the CRT bindings for java) directly.
* `sdk-java-client-crt`: Uses CRT based [S3AsyncClient](https://sdk.amazonaws.com/java/api/latest/software/amazon/awssdk/services/s3/S3AsyncClient.html) from [aws-sdk-java-v2](https://github.com/aws/aws-sdk-java-v2/).
* `sdk-java-tm-crt`: Uses CRT based [S3TransferManager](https://sdk.amazonaws.com/java/api/latest/software/amazon/awssdk/transfer/s3/S3TransferManager.html) from [aws-sdk-java-v2](https://github.com/aws/aws-sdk-java-v2/).

## Building

```sh
cd aws-crt-s3-benchmarks/runners/s3-benchrunner-java
mvn package
```

This produces the uber-jar: `target/s3-benchrunner-java-1.0-SNAPSHOT.jar` .

### Using a local build of aws-crt-java

By default, the latest release of aws-crt-java is pulled from Maven Central.

If you want to build aws-crt-java locally:

```sh
cd my/dev/dir
git clone https://github.com/awslabs/aws-crt-java.git
cd aws-crt-java
git submodule update --init
mvn install -Dmaven.test.skip
```

This installs version 1.0.0-SNAPSHOT.

Now get the runner to use it by building with the "snapshot" profile active:

```sh
cd /path/to/s3-benchrunner-java
mvn -P snapshot package
```

### Working in IntelliJ

Submissions welcome, I'm bad at Java.

### Working in VSCode

Submissions welcome, I'm bad at Java.

## Running

If you built the uber-jar via `mvn package` , the `RUNNER_CMD` is:

 `java -jar path/to/s3-benchrunner-java-1.0-SNAPSHOT.jar [args...]`

and the args you pass are described [here](../README.md#running).

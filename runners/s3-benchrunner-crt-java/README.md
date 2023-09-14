# s3-benchrunner-crt-java

s3-benchrunner for [aws-crt-java](https://github.com/awslabs/aws-crt-java).

## Building

```sh
cd aws-crt-s3-benchmarks/runners/s3-benchrunner-crt-java
mvn package
```

This produces the uber-jar: `target/s3-benchrunner-crt-java-1.0-SNAPSHOT.jar`.

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
cd /path/to/s3-benchrunner-crt-java
mvn -P snapshot package
```

### Working in IntelliJ

Submissions welcome, I'm bad at Java.

### Working in VSCode

Submissions welcome, I'm bad at Java.

## Running

If you built the uber-jar via `mvn package`, the `runner-cmd` is:

`java -jar path/to/s3-benchrunner-crt-java-1.0-SNAPSHOT.jar [args...]`

and the args you pass are described [here](../README.md#running).

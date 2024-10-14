# s3-benchrunner-rust

s3-benchrunner for [aws-s3-transfer-manager-rs](https://github.com/awslabs/aws-s3-transfer-manager-rs/).

## Building

```sh
cd aws-crt-s3-benchmarks/runners/s3-benchrunner-rust
cargo build --release
```

This produces: `target/release/s3-benchrunner-rust`

## Running

```
Usage: s3-benchrunner-rust [OPTIONS] <S3_CLIENT> <WORKLOAD> <BUCKET> <REGION> <TARGET_THROUGHPUT>

Arguments:
  <S3_CLIENT>
          ID of S3 library to use

          Possible values:
          - sdk-rust-tm: use aws-s3-transfer-manager crate

  <WORKLOAD>
          Path to workload file (e.g. download-1GiB.run.json)

  <BUCKET>
          S3 bucket name (e.g. my-test-bucket)

  <REGION>
          AWS Region (e.g. us-west-2)

  <TARGET_THROUGHPUT>
          Target throughput, in gigabits per second (e.g. "100.0" for c5n.18xlarge)

Options:
      --telemetry
          Emit telemetry via OTLP/gRPC to http://localhost:4317

  -h, --help
          Print help (see a summary with '-h')
```

See further instructions [here](../../README.md#run-a-benchmark).

### Viewing Telemetry

Use the `--telemetry` flag to export OpenTelemetry Protocol data as a `trace_*.json` files in the working directory.

Run `graph.py`:
```sh
usage: graph.py [-h] TRACE_JSON

Graph a benchmark run

positional arguments:
  TRACE_JSON  trace_*.json file to graph.

options:
  -h, --help  show this help message and exit
```

View new `trace_*.html` file, in same directory as `trace_*.json` file.
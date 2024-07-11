use std::{env, process};

use s3_benchrunner_rust::BenchmarkConfig;

fn main() {
    let args: Vec<String> = env::args().collect();

    let [_, s3_client_id, workload, bucket, region, target_throughput] = &args[..] else {
        eprintln!("usage: s3-benchrunner-rust S3_CLIENT WORKLOAD BUCKET REGION TARGET_THROUGHPUT");
        process::exit(1);
    };

    let target_throughput: f64 = target_throughput
        .parse()
        .expect("TARGET_THROUGHPUT should be a Gb/s float");

    let config = BenchmarkConfig::new(workload, bucket, region, target_throughput);
}

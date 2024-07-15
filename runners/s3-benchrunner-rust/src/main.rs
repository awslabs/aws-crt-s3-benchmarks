use clap::{Parser, ValueEnum};
use std::time::Instant;

use s3_benchrunner_rust::{
    bytes_to_gigabits, BenchmarkConfig, BenchmarkRunner, TransferManagerRunner,
};

#[derive(Parser)]
#[command()]
struct Args {
    #[arg(value_enum, help = "ID of S3 library to use")]
    s3_client: S3ClientId,
    #[arg(help = "Path to workload file (e.g. download-1GiB.run.json)")]
    workload: String,
    #[arg(help = "S3 bucket name (e.g. my-test-bucket)")]
    bucket: String,
    #[arg(help = "AWS Region (e.g. us-west-2)")]
    region: String,
    #[arg(help = "Target throughput, in gigabits per second (e.g. \"100.0\" for c5n.18xlarge)")]
    target_throughput: f64,
}

#[derive(ValueEnum, Clone)]
enum S3ClientId {
    #[clap(name = "sdk-rust-tm", help = "use aws-s3-transfer-manager crate")]
    TransferManager,
    // TODO:
    // #[clap(name="sdk-rust-client", help="use aws-sdk-s3 crate")]
    // SdkClient,
}

fn main() {
    let args = Args::parse();

    let config = BenchmarkConfig::new(
        &args.workload,
        &args.bucket,
        &args.region,
        args.target_throughput,
    );

    // create appropriate benchmark runner
    let runner: Box<dyn BenchmarkRunner> = match args.s3_client {
        S3ClientId::TransferManager => Box::new(TransferManagerRunner::new(config)),
    };
    let workload = &runner.config().workload;
    let bytes_per_run: u64 = workload.tasks.iter().map(|x| x.size).sum();
    let gigabits_per_run = bytes_to_gigabits(bytes_per_run);

    // repeat benchmark until we exceed max_repeat_count or max_repeat_secs
    let app_start = Instant::now();
    for run_i in 0..workload.max_repeat_count {
        let run_start = Instant::now();

        runner.run();

        let run_secs = run_start.elapsed().as_secs_f64();
        println!(
            "Run:{} Secs:{:.6} Gb/s:{:.6}",
            run_i + 1,
            run_secs,
            gigabits_per_run / run_secs
        );

        // break out if we've exceeded max_repeat_secs
        if app_start.elapsed().as_secs_f64() >= workload.max_repeat_secs {
            break;
        }
    }
}

use clap::{Parser, ValueEnum};
use std::process::exit;
use std::time::Instant;
use tracing::{info_span, Instrument};

use s3_benchrunner_rust::{
    bytes_to_gigabits, prepare_run, telemetry, BenchmarkConfig, Result, RunBenchmark,
    SkipBenchmarkError, TransferManagerRunner,
};
#[derive(Parser, Debug)]
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
    #[arg(long, help = "Emit telemetry via OTLP/gRPC to http://localhost:4317")]
    telemetry: bool,
    #[arg(
        long,
        help = "Instead of using 1 upload_objects()/download_objects() call for multiple files on disk, use N upload()/download() calls."
    )]
    disable_directory: bool,
}

#[derive(ValueEnum, Clone, Debug)]
enum S3ClientId {
    #[clap(name = "sdk-rust-tm", help = "use aws-s3-transfer-manager crate")]
    TransferManager,
    // TODO:
    // #[clap(name="sdk-rust-client", help="use aws-sdk-s3 crate")]
    // SdkClient,
}

#[tokio::main]
async fn main() {
    let args = Args::parse();

    let result = execute(&args).await;
    if let Err(e) = result {
        match e.downcast_ref::<SkipBenchmarkError>() {
            None => {
                panic!("{e:?}");
            }
            Some(msg) => {
                eprintln!("Skipping benchmark - {msg}");
                exit(123);
            }
        }
    }
}

async fn execute(args: &Args) -> Result<()> {
    let mut telemetry = if args.telemetry {
        // If emitting telemetry, set that up as tracing_subscriber.
        Some(telemetry::init_tracing_subscriber().unwrap())
    } else {
        // Otherwise, set the default subscriber,
        // which prints to stdout if env-var set like RUST_LOG=trace
        tracing_subscriber::fmt()
            .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
            .with_ansi(false) // Disable ANSI colors
            .init();
        None
    };

    // create appropriate benchmark runner
    let runner = new_runner(args).await?;

    let workload = &runner.config().workload;
    let workload_name = workload_name(&args.workload);
    let bytes_per_run: u64 = workload.tasks.iter().map(|x| x.size).sum();
    let gigabits_per_run = bytes_to_gigabits(bytes_per_run);

    // repeat benchmark until we exceed max_repeat_count or max_repeat_secs
    let app_start = Instant::now();
    for run_num in 1..=workload.max_repeat_count {
        prepare_run(workload)?;

        let run_start_datetime = chrono::Utc::now();
        let run_start = Instant::now(); // high resolution

        runner
            .run()
            .instrument(info_span!(
                "run-benchmark",
                num = run_num,
                workload = workload_name
            ))
            .await?;

        let run_secs = run_start.elapsed().as_secs_f64();

        // flush any telemetry
        if let Some(telemetry) = &mut telemetry {
            telemetry.flush_to_file(&trace_file_name(
                workload_name,
                &run_start_datetime,
                run_num,
            ));
        }

        eprintln!(
            "Run:{} Secs:{:.6} Gb/s:{:.6}",
            run_num,
            run_secs,
            gigabits_per_run / run_secs
        );

        // break out if we've exceeded max_repeat_secs
        if app_start.elapsed().as_secs_f64() >= workload.max_repeat_secs {
            break;
        }
    }

    Ok(())
}

async fn new_runner(args: &Args) -> Result<Box<dyn RunBenchmark>> {
    let config = BenchmarkConfig::new(
        &args.workload,
        &args.bucket,
        &args.region,
        args.target_throughput,
        args.disable_directory,
    )?;
    match args.s3_client {
        S3ClientId::TransferManager => {
            let transfer_manager = TransferManagerRunner::new(config).await;
            Ok(Box::new(transfer_manager))
        }
    }
}

// Given "path/to/my-workload.run.json" return "my-workload"
fn workload_name(path: &str) -> &str {
    let filename = path.rsplit('/').next().unwrap_or(path);
    let without_extension = filename.split('.').next().unwrap_or(filename);
    without_extension
}

fn trace_file_name(
    workload: &str,
    run_start: &chrono::DateTime<chrono::Utc>,
    run_num: u32,
) -> String {
    let run_start = run_start.format("%Y%m%dT%H%M%SZ").to_string();
    format!("trace_{run_start}_{workload}_run{run_num:02}.json")
}

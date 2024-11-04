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
    #[arg(long, help = "Emit telemetry to trace_*.json")]
    telemetry: bool,
    #[arg(long, help = "Emit flamegraph_*.svg")]
    flamegraph: bool,
}

#[derive(ValueEnum, Clone, Debug)]
enum S3ClientId {
    #[clap(name = "sdk-rust-tm", help = "use aws-s3-transfer-manager crate")]
    TransferManager,
    // TODO:
    // #[clap(name="sdk-rust-client", help="use aws-sdk-s3 crate")]
    // SdkClient,
}

fn main() {
    let args = Args::parse();

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap();

    match runtime.block_on(execute(&args)) {
        Err(e) => match e.downcast_ref::<SkipBenchmarkError>() {
            None => {
                panic!("{e:?}");
            }
            Some(msg) => {
                eprintln!("Skipping benchmark - {msg}");
                exit(123);
            }
        },
        Ok(()) => (),
    }
}

async fn execute(args: &Args) -> Result<()> {
    let mut telemetry = if args.telemetry {
        // If emitting telemetry, set that up as tracing_subscriber.
        Some(telemetry::init_tracing_subscriber().unwrap())
    } else {
        // Otherwise, set the default subscriber,
        // which prints to stdout if env-var set like RUST_LOG=trace
        tracing_subscriber::fmt::init();
        None
    };

    // create appropriate benchmark runner
    let runner = new_runner(args).await?;

    let workload = &runner.config().workload;
    let workload_name = workload_name(&args.workload);
    let bytes_per_run: u64 = workload.tasks.iter().map(|x| x.size).sum();
    let gigabits_per_run = bytes_to_gigabits(bytes_per_run);

    // repeat benchmark until we exceed max_repeat_count or max_repeat_secs
    let app_start_datetime = chrono::Utc::now();
    let app_start = Instant::now();
    for run_num in 1..=workload.max_repeat_count {
        prepare_run(workload)?;

        let profiler = if args.flamegraph {
            Some(
                pprof::ProfilerGuardBuilder::default()
                    .frequency(10)
                    .blocklist(&["libc", "libgcc", "pthread", "vdso"])
                    .build()?,
            )
        } else {
            None
        };

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

        if let Some(profiler) = &profiler {
            let report = profiler.report().build()?;
            let file = std::fs::File::create(artifact_file_name(
                "flamegraph",
                workload_name,
                &app_start_datetime,
                run_num,
                gigabits_per_run / run_secs,
                ".svg",
            ))?;
            report.flamegraph(file)?;
        }

        // flush any telemetry
        if let Some(telemetry) = &mut telemetry {
            telemetry.flush_to_file(&artifact_file_name(
                "trace",
                workload_name,
                &app_start_datetime,
                run_num,
                gigabits_per_run / run_secs,
                ".json",
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

// Get file name to use when emitting things like telemetry and flamegraph files per benchmark run
fn artifact_file_name(
    prefix: &str,
    workload: &str,
    timestamp: &chrono::DateTime<chrono::Utc>,
    run_num: u32,
    run_gigabits_per_sec: f64,
    suffix: &str,
) -> String {
    let timestamp = timestamp.format("%Y%m%dT%H%M%SZ").to_string();
    let run_gigabits_per_sec = run_gigabits_per_sec.round() as u64;
    format!("{prefix}_{timestamp}_{workload}_run{run_num:02}_{run_gigabits_per_sec:03}Gbps{suffix}")
}

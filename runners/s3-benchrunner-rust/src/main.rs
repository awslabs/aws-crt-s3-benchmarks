use clap::{Parser, Subcommand, ValueEnum};
use std::fs::File;
use std::time::Instant;
use std::{path::Path, process::exit};
use tracing::{info_span, Instrument};

use opentelemetry_proto::tonic::{
    collector::trace::v1::{trace_service_client::TraceServiceClient, ExportTraceServiceRequest},
    trace::v1::TracesData,
};
use tonic::transport::Channel;

use s3_benchrunner_rust::{
    bytes_to_gigabits, prepare_run, telemetry, BenchmarkConfig, Result, RunBenchmark,
    SkipBenchmarkError, TransferManagerRunner,
};

#[derive(Parser, Debug)]
struct SimpleCli {
    #[command(flatten)]
    run_args: RunArgs,
}

#[derive(Parser, Debug)]
struct ExtendedCli {
    #[command(subcommand)]
    command: Command,
    #[command(flatten)]
    run_args: Option<RunArgs>,
}

#[derive(Subcommand, Debug)]
enum Command {
    RunBenchmark(RunArgs),
    UploadOtlp(UploadOtlpArgs),
}

#[derive(Debug, clap::Args)]
#[command(args_conflicts_with_subcommands = true)]
#[command(flatten_help = true)]
struct RunArgs {
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

#[derive(Debug, clap::Args)]
#[command(flatten_help = true)]
struct UploadOtlpArgs {
    /// OLTP endpoint to export data to
    #[arg(long, default_value = "http://localhost:4317")]
    oltp_endpoint: String,

    /// Path to the trace file (in opentelemetry-proto JSON format) to upload
    json_file: String,
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
async fn main() -> Result<()> {
    let command = SimpleCli::try_parse()
        .map(|cli| Command::RunBenchmark(cli.run_args))
        .unwrap_or_else(|_| ExtendedCli::parse().command);

    match command {
        Command::RunBenchmark(args) => {
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
        Command::UploadOtlp(args) => upload_otlp(args).await?,
    }

    Ok(())
}

async fn execute(args: &RunArgs) -> Result<()> {
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

async fn new_runner(args: &RunArgs) -> Result<Box<dyn RunBenchmark>> {
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

async fn upload_otlp(args: UploadOtlpArgs) -> Result<()> {
    let path = Path::new(&args.json_file);
    let f = File::open(path)?;
    let trace_data = read_spans_from_json(f)?;
    println!("loaded {} spans", trace_data.resource_spans.len());

    let endpoint = Channel::from_shared(args.oltp_endpoint)?;
    let channel = endpoint.connect_lazy();
    let mut client = TraceServiceClient::new(channel);

    let requests: Vec<_> = trace_data
        .resource_spans
        .chunks(4_096)
        .map(|batch| ExportTraceServiceRequest {
            resource_spans: batch.to_vec(),
        })
        .collect();

    for request in requests {
        let resp = client.export(request).await?;
        println!("export response: {:?}", resp);
    }

    Ok(())
}

// read a file contains ResourceSpans in json format
pub fn read_spans_from_json(file: File) -> Result<TracesData> {
    let reader = std::io::BufReader::new(file);
    let trace_data: TracesData = serde_json::from_reader(reader)?;
    Ok(trace_data)
}

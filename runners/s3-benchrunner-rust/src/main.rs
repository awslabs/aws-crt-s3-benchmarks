use std::{fs::File, io, path::PathBuf};
use std::io::{BufReader, Write};
use std::time::{Duration, Instant};

use aws_config::meta::region::RegionProviderChain;
use aws_sdk_s3::Client;
use aws_sdk_s3::operation::get_object::GetObjectError;
use clap::Parser;
use futures::future::join_all;
use serde::{Deserialize, Serialize};

/// Defines the command line arguments structure
#[derive(Parser, Debug)]
#[clap(author, version, about, long_about = None)]
struct Args {
    /// Path to the workload JSON file
    #[clap(value_parser)]
    workload: PathBuf,

    /// S3 bucket name
    #[clap(value_parser)]
    bucket: String,

    /// AWS region
    #[clap(value_parser)]
    region: String,

    /// Target throughput
    #[clap(value_parser)]
    target_throughput: f64,

    /// S3 client type or configuration
    #[clap(value_parser)]
    s3_client: String,
}


#[derive(Debug, Serialize, Deserialize, Clone)]
struct BenchmarkConfig {
    version: u8,
    filesOnDisk: bool,
    checksum: Option<String>,
    maxRepeatCount: u32,
    maxRepeatSecs: u32,
    tasks: Vec<Task>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
struct Task {
    action: String,
    key: String,
    size: u64,
}

async fn get_object(client: &Client, bucket: &str, object: &str) -> Result<usize, anyhow::Error> {
    println!("waahm7: downloading object");
    let mut object = client
        .get_object()
        .bucket(bucket)
        .key(object)
        .send()
        .await?; // ? is used to return error from this function if there is an error

    let mut byte_count = 0_usize;
    while let Some(bytes) = object.body.try_next().await? {
        let bytes_len = bytes.len();
        //file.write_all(&bytes)?;
        byte_count += bytes_len;
    }
    println!("waahm7: downloaded");

    Ok(byte_count)
}

struct Benchmark {
    config: BenchmarkConfig,
    bucket: String,
    client: Client
}

impl Benchmark {
    async fn new(config: BenchmarkConfig, bucket: String, region: String) -> Benchmark {
        //let region = Region::new(region);
        let region_provider = RegionProviderChain::default_provider().or_else("us-west-2");
        let sdk_config = aws_config::from_env().region(region_provider).load().await;
        let client = Client::new(&sdk_config);

        return Benchmark {
            config,
            bucket,
            client
        }
    }

    async fn run(&self) -> Result<(), GetObjectError> {
        let futures: Vec<_> = self.config.tasks.iter().map(|task| {
            get_object(&self.client, &self.bucket, &task.key)
        }).collect();
        let results = join_all(futures).await;
        // Check if any of the futures resulted in an error
        for result in results {
            match result {
                Ok(_) => continue, // If the future succeeded, continue checking the rest
                Err(err) => {
                    println!("Error is {err}");
                    continue;
                }, // Return an error if any future fails
            }
        }

        // If all futures succeeded, return Ok(())
        Ok(())
    }
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();
    let args = Args::parse();
    println!("Args: {:?}", args);

    let file = File::open(args.workload).expect("Failed to open workload file");
    let reader = BufReader::new(file);


    let config: BenchmarkConfig = serde_json::from_reader(reader)
        .unwrap();
    let benchmark = Benchmark::new(config.clone(), args.bucket, args.region).await;

    let mut durations = Vec::new();
    let app_start = Instant::now();
    for run_i in 0..1 {
        let run_start = Instant::now();
        let result = benchmark.run().await;
        match result {
            Ok(_) => println!("waahm7: downloaded all objects"),
            Err(err) => println!("waahm7: download failed with {err}")
        }
        let run_duration_secs = run_start.elapsed().as_secs();
        durations.push(run_duration_secs);
        io::stderr().flush().unwrap();
        // Print the run
        io::stdout().flush().unwrap();

        if app_start.elapsed() >= Duration::from_secs(config.maxRepeatSecs as u64) {
            break;
        }
    }
    // print final stats

    println!("{:#?}", config)
}

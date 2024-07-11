use serde::Deserialize;
use std::{fs::File, io::BufReader, process};

fn exit_with_skip_code(msg: &str) -> ! {
    eprintln!("Skipping benchmark - {msg}");
    process::exit(123)
}

/// All configuration for a benchmark runner.
/// Includes values from workload json file, and from the command line
#[derive(Debug)]
pub struct BenchmarkConfig {
    pub workload: WorkloadConfig,
    pub bucket: String,
    pub region: String,
    pub target_throughput_gigabits: f64,
}

/// From the workload's JSON file
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct WorkloadConfig {
    pub version: i32,
    pub files_on_disk: bool,
    pub checksum: Option<String>,
    pub max_repeat_count: i32,
    pub max_repeat_secs: i32,
    pub tasks: Vec<TaskConfig>,
}

/// A task in the workload's JSON file
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TaskConfig {
    pub action: String,
    pub key: String,
    pub size: u64,
}

/// All benchmark configuration (combination of json workload and command line args)
impl BenchmarkConfig {
    pub fn new(
        workload_path: &str,
        bucket: &str,
        region: &str,
        target_throughput_gigabits: f64,
    ) -> Self {
        let json_file = File::open(workload_path).unwrap_or_else(|err| {
            panic!("Failed opening '{workload_path}' - {err}");
        });
        let json_reader = BufReader::new(json_file);

        // exit with skip code if workload has different version
        // which may materialize as a "data" error, because it no longer matches our structs
        let workload: WorkloadConfig = serde_json::from_reader(json_reader).unwrap_or_else(|err| {
            if err.is_data() {
                exit_with_skip_code(&format!(
                    "Can't parse '{workload_path}'. Different version maybe? - {err}"
                ));
            } else {
                panic!("Failed parsing json from '{workload_path}' - {err}")
            }
        });

        if workload.version != 2 {
            exit_with_skip_code(&format!(
                "Workload version not supported: {}",
                workload.version
            ));
        };

        BenchmarkConfig {
            workload,
            bucket: bucket.to_string(),
            region: region.to_string(),
            target_throughput_gigabits,
        }
    }
}

pub trait BenchmarkRunner {
    fn run(&self);
}

/// Benchmark runner using aws-s3-transfer-manager
pub struct TransferManagerRunner<'a> {
    config: &'a BenchmarkConfig,
}

impl<'a> TransferManagerRunner<'a> {
    pub fn new(config: &BenchmarkConfig) -> TransferManagerRunner {
        TransferManagerRunner { config }
    }
}

impl<'a> BenchmarkRunner for TransferManagerRunner<'a> {
    fn run(&self) {
        // TODO: actually run the workload
    }
}

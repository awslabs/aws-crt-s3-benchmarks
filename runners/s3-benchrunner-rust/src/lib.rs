use serde::Deserialize;
use std::{fs::File, io::BufReader, process};

mod transfer_manager;
pub use transfer_manager::TransferManagerRunner;

fn exit_with_skip_code(msg: &str) -> ! {
    eprintln!("Skipping benchmark - {msg}");
    process::exit(123)
}

pub fn bytes_to_gigabits(bytes: u64) -> f64 {
    let bits = bytes * 8;
    (bits as f64) / 1_000_000_000.0
}

/// All configuration for a benchmark runner.
/// Includes values from workload json file, and from the command line
#[derive(Debug)]
pub struct BenchmarkConfig {
    pub workload: WorkloadConfig,
    pub bucket: String,
    pub region: String,
    pub target_throughput_gigabits_per_sec: f64,
}

/// From the workload's JSON file
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct WorkloadConfig {
    pub version: u32,
    pub files_on_disk: bool,
    pub checksum: Option<ChecksumAlgorithm>,
    pub max_repeat_count: u32,
    pub max_repeat_secs: f64,
    pub tasks: Vec<TaskConfig>,
}

/// A task in the workload's JSON file
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TaskConfig {
    pub action: TaskAction,
    pub key: String,
    pub size: u64,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum TaskAction {
    Download,
    Upload,
}

#[derive(Debug, Deserialize)]
pub enum ChecksumAlgorithm {
    CRC32,
    CRC32C,
    SHA1,
    SHA256,
}

/// All benchmark configuration (combination of json workload and command line args)
impl BenchmarkConfig {
    pub fn new(
        workload_path: &str,
        bucket: &str,
        region: &str,
        target_throughput_gigabits_per_sec: f64,
    ) -> Self {
        let json_file =
            File::open(workload_path).expect(&format!("Failed opening '{workload_path}'"));

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
            target_throughput_gigabits_per_sec,
        }
    }
}

pub trait BenchmarkRunner {
    fn run(&self);
    fn config(&self) -> &BenchmarkConfig;
}
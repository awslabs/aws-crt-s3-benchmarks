use anyhow::{anyhow, Context};
use async_trait::async_trait;
use serde::Deserialize;
use std::{fs::File, io::BufReader, path::Path};

pub mod telemetry;

mod transfer_manager;
pub use transfer_manager::TransferManagerRunner;

pub type Result<T> = anyhow::Result<T>;

pub const MEBIBYTE: u64 = 1024 * 1024;
pub const PART_SIZE: u64 = 8 * MEBIBYTE;

/// Used when the runner knows it can't run a workload.
/// It's not the user's fault, it's not a bug.
#[derive(thiserror::Error, Debug)]
#[error("skipping benchmark - {0}")]
pub struct SkipBenchmarkError(String);

/// Create Result<T>::Err containing a SkipBenchmarkError,
/// with a message created from calling format!() on the args passed into this macro.
#[macro_export]
macro_rules! skip_benchmark {
    ($($args:tt)*) => {
        Err($crate::SkipBenchmarkError(format!($($args)*)).into())
    };
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
    pub disable_directory: bool,
}

/// From the workload's JSON file
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct WorkloadConfig {
    pub version: u32,
    pub files_on_disk: bool,
    pub checksum: Option<String>,
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

/// Possible values for the "action" field of the workload's JSON file
#[derive(Debug, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub enum TaskAction {
    Download,
    Upload,
}

/// All benchmark configuration (combination of json workload and command line args)
impl BenchmarkConfig {
    pub fn new(
        workload_path: &str,
        bucket: &str,
        region: &str,
        target_throughput_gigabits_per_sec: f64,
        disable_directory: bool,
    ) -> Result<Self> {
        let json_file = File::open(workload_path)
            .with_context(|| format!("Failed opening '{workload_path}'"))?;

        let json_reader = BufReader::new(json_file);

        // exit with skip code if workload has different version
        // which may materialize as a "data" error, because it no longer matches our structs
        let workload: WorkloadConfig = match serde_json::from_reader(json_reader) {
            Ok(workload) => workload,
            Err(e) => {
                return skip_benchmark!(
                    "Can't parse '{workload_path}'. Different version maybe? - {e}"
                )
            }
        };

        if workload.version != 2 {
            return skip_benchmark!("Workload version not supported: {}", workload.version);
        };

        Ok(BenchmarkConfig {
            workload,
            bucket: bucket.to_string(),
            region: region.to_string(),
            target_throughput_gigabits_per_sec,
            disable_directory,
        })
    }
}

#[async_trait]
pub trait RunBenchmark {
    async fn run(&self) -> Result<()>;
    fn config(&self) -> &BenchmarkConfig;
}

// Do prep work between runs, before timers starts (e.g. create intermediate directories)
pub fn prepare_run(workload: &WorkloadConfig) -> Result<()> {
    if workload.files_on_disk {
        for task_config in &workload.tasks {
            let filepath = Path::new(&task_config.key);
            match task_config.action {
                TaskAction::Download => {
                    if filepath.exists() {
                        // delete pre-existing file, in case overwrite is slower than original write.
                        std::fs::remove_file(filepath).with_context(|| {
                            format!("failed removing file from previous run: {filepath:?}")
                        })?;
                    } else if let Some(dir) = filepath.parent() {
                        // create directory if necessary
                        if !dir.exists() {
                            std::fs::create_dir_all(dir)
                                .with_context(|| format!("failed creating directory: {dir:?}"))?;
                        }
                    }
                }

                TaskAction::Upload => {
                    if !filepath.is_file() {
                        return Err(anyhow!("file not found: {filepath:?}"));
                    }
                }
            }
        }
    }

    Ok(())
}

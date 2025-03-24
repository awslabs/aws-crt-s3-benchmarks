use std::{cmp::min, path::PathBuf, sync::Arc};

use anyhow::Context;
use async_trait::async_trait;
use aws_sdk_s3::types::ChecksumAlgorithm;
use aws_sdk_s3_transfer_manager::{
    io::InputStream,
    operation::upload::ChecksumStrategy,
    types::{ConcurrencyMode, PartSize, TargetThroughput},
};
use bytes::{Buf, Bytes};
use tokio::fs::File;
use tokio::io::AsyncWriteExt;
use tokio::task::JoinSet;
use tracing::{info_span, Instrument};

use crate::{
    skip_benchmark, BenchmarkConfig, Result, RunBenchmark, TaskAction, TaskConfig, PART_SIZE,
};

/// Benchmark runner using aws-s3-transfer-manager
#[derive(Clone)]
pub struct TransferManagerRunner {
    handle: Arc<Handle>,
}

struct Handle {
    config: BenchmarkConfig,
    transfer_manager: aws_sdk_s3_transfer_manager::Client,
    random_data_for_upload: Bytes,
    transfer_path: Option<String>,
}

impl TransferManagerRunner {
    pub async fn new(config: BenchmarkConfig) -> TransferManagerRunner {
        // Create random buffer to upload
        let upload_data_size: usize = if config.workload.files_on_disk {
            0
        } else {
            config
                .workload
                .tasks
                .iter()
                .filter(|task| task.action == TaskAction::Upload)
                .map(|task| task.size)
                .max()
                .unwrap_or(0)
                .try_into()
                .unwrap()
        };
        let random_data_for_upload = new_random_bytes(upload_data_size);

        let tm_config = aws_sdk_s3_transfer_manager::from_env()
            .concurrency(ConcurrencyMode::TargetThroughput(
                TargetThroughput::new_gigabits_per_sec(
                    config.target_throughput_gigabits_per_sec as u64,
                ),
            ))
            .part_size(PartSize::Target(PART_SIZE))
            .load()
            .await;

        let transfer_manager = aws_sdk_s3_transfer_manager::Client::new(tm_config);
        let transfer_path = find_common_parent_dir(&config);
        TransferManagerRunner {
            handle: Arc::new(Handle {
                config,
                transfer_manager,
                random_data_for_upload,
                transfer_path,
            }),
        }
    }

    async fn run_task(self, task_i: usize) -> Result<()> {
        let task_config = &self.config().workload.tasks[task_i];

        match task_config.action {
            TaskAction::Download => {
                self.download(task_config)
                    .instrument(info_span!("download", key = task_config.key))
                    .await
            }
            TaskAction::Upload => {
                self.upload(task_config)
                    .instrument(info_span!("upload", key = task_config.key))
                    .await
            }
        }
    }
    async fn download_objects(&self) -> Result<()> {
        let path = self.handle.transfer_path.as_ref().unwrap();
        let dest = PathBuf::from(path);
        let download_objects_handle = self
            .handle
            .transfer_manager
            .download_objects()
            .bucket(&self.config().bucket)
            .key_prefix(path)
            .destination(&dest)
            .send()
            .await?;
        download_objects_handle.join().await?;
        Ok(())
    }

    async fn upload_objects(&self) -> Result<()> {
        let path = self.handle.transfer_path.as_ref().unwrap();
        let upload_objects_handle = self
            .handle
            .transfer_manager
            .upload_objects()
            .bucket(&self.config().bucket)
            .key_prefix(path)
            .source(path)
            .send()
            .await?;
        upload_objects_handle.join().await?;
        Ok(())
    }

    async fn download(&self, task_config: &TaskConfig) -> Result<()> {
        let key = &task_config.key;

        let mut download_handle = self
            .handle
            .transfer_manager
            .download()
            .bucket(&self.config().bucket)
            .key(key)
            .initiate()
            .with_context(|| format!("failed starting download: {key}"))?;

        // if files_on_disk: open file for writing
        let mut dest_file = if self.config().workload.files_on_disk {
            let file = File::create(key)
                .instrument(info_span!("open-file"))
                .await
                .with_context(|| format!("failed creating file: {key}"))?;
            Some(file)
        } else {
            None
        };

        let mut total_size = 0u64;
        let mut seq: u64 = 0;
        while let Some(chunk_result) = download_handle
            .body_mut()
            .next()
            .instrument(info_span!("next-chunk", seq, offset = total_size))
            .await
        {
            let output =
                chunk_result.with_context(|| format!("failed downloading next chunk of: {key}"))?;
            let mut chunk = output.data;

            let chunk_size = chunk.remaining();
            total_size += chunk_size as u64;
            seq += 1;

            if let Some(dest_file) = &mut dest_file {
                dest_file
                    .write_all_buf(&mut chunk)
                    .instrument(info_span!("write-file", bytes = chunk_size))
                    .await?;
            }
        }

        assert_eq!(total_size, task_config.size);

        Ok(())
    }

    async fn upload(&self, task_config: &TaskConfig) -> Result<()> {
        let key = &task_config.key;

        let stream = if self.config().workload.files_on_disk {
            InputStream::from_path(key).with_context(|| "Failed to create stream")?
        } else {
            self.handle
                .random_data_for_upload
                .slice(0..(task_config.size as usize))
                .into()
        };

        let checksum_strategy = match &self.config().workload.checksum {
            Some(algorithm_name) => Some(
                ChecksumStrategy::builder()
                    .algorithm(ChecksumAlgorithm::from(algorithm_name.as_str()))
                    .build()?,
            ),
            _ => None,
        };

        let upload_handle = self
            .handle
            .transfer_manager
            .upload()
            .bucket(&self.config().bucket)
            .key(key)
            .body(stream)
            .set_checksum_strategy(checksum_strategy)
            .initiate()?;

        upload_handle
            .join()
            .await
            .with_context(|| format!("failed uploading: {key}"))?;

        Ok(())
    }
}

#[async_trait]
impl RunBenchmark for TransferManagerRunner {
    async fn run(&self) -> Result<()> {
        let workload_config = &self.config().workload;

        match &self.handle.transfer_path {
            Some(transfer_path) => {
                // Use the objects API to download/upload directory directly
                match workload_config.tasks[0].action {
                    TaskAction::Download => {
                        self.download_objects()
                            .instrument(info_span!("download-directory", directory = transfer_path))
                            .await?
                    }
                    TaskAction::Upload => {
                        if workload_config.checksum.is_some() {
                            return skip_benchmark!(
                                "upload_objects() doesn't let you specify checksum algorithm at this time",
                            );
                        }

                        self.upload_objects()
                            .instrument(info_span!("upload-directory", directory = transfer_path))
                            .await?
                    }
                }
            }
            None => {
                // Spawn concurrent tasks for all uploads/downloads.
                // We want the benchmark to fail fast if anything goes wrong,
                // so we're using a JoinSet.
                let mut task_set: JoinSet<Result<()>> = JoinSet::new();
                // Iterate through all the tasks to download/upload each object.
                for i in 0..workload_config.tasks.len() {
                    let task = self.clone().run_task(i);
                    task_set.spawn(task.instrument(tracing::Span::current()));
                }

                while let Some(join_result) = task_set.join_next().await {
                    let task_result = join_result.unwrap();
                    task_result?;
                }
            }
        }
        Ok(())
    }

    fn config(&self) -> &BenchmarkConfig {
        &self.handle.config
    }
}

/// Find the common parent directory for all tasks.
/// Returns None if we shouldn't be doing upload/download on a whole directory.
fn find_common_parent_dir(config: &BenchmarkConfig) -> Option<String> {
    if config.workload.files_on_disk && !config.disable_directory && config.workload.tasks.len() > 1
    {
        let first_task = &config.workload.tasks[0];

        // Find the common parents directory for all the tasks.
        // If there is no common parent, we can't use the same directory for downloads.
        let mut common_root = std::path::Path::new(&first_task.key).parent()?;
        for task in &config.workload.tasks {
            let task_path = std::path::Path::new(&task.key);
            common_root = common_root.ancestors().find(|ancestor| {
                task_path
                    .ancestors()
                    .any(|task_ancestor| task_ancestor == *ancestor)
            })?;
            if task.action != first_task.action {
                panic!("Can't use directory for both download and upload");
            }
        }

        // S3Express requires that the prefix must end with delimiter
        Some(format!(
            "{}{}",
            common_root.to_str()?,
            std::path::MAIN_SEPARATOR
        ))
    } else {
        None
    }
}

// Quickly generate a buffer of random data.
// This is fancy because a naive approach can add MINUTES to each debug run,
// and we want devs to iterate quickly.
fn new_random_bytes(size: usize) -> Bytes {
    // fastrand's fill() is the fastest we found.
    // we also tested rand's fill_bytes(), and aws_lc_rs's SystemRandom.
    let mut rng = fastrand::Rng::new();

    // Generating randomness is slower then copying memory. Therefore, only fill SOME
    // of the buffer with randomness, and fill the rest with copies of that randomness.
    // In debug, with 30GiB, this trick reduces time from 172 sec -> 7 sec.

    // We don't want any parts to be identical.
    // Use something that won't fall on a part boundary as we copy it.
    let rand_len = min(31415926, size); // approx 30MiB, digits of pi

    // Avoid re-allocations by reserving exact amount
    let mut data = Vec::<u8>::new();
    data.reserve_exact(size);

    // Fill the beginning with randomness.
    unsafe {
        // Unsafe set_len saves a half-second in debug, compared to resize().
        data.set_len(rand_len);
    }
    rng.fill(&mut data);

    // Copy randomness until it's the size we want
    while data.len() < size {
        let extend_len = min(data.len(), size - data.len());
        data.extend_from_within(0..extend_len);
    }
    data.into()
}

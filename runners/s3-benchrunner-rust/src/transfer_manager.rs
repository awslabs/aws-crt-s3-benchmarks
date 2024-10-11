use std::{cmp::min, sync::Arc};

use anyhow::Context;
use async_trait::async_trait;
use aws_s3_transfer_manager::{
    io::InputStream,
    types::{ConcurrencySetting, PartSize},
};
use bytes::{Buf, Bytes};
use tokio::fs::File;
use tokio::io::AsyncWriteExt;
use tokio::task::JoinSet;
use tracing::{info_span, Instrument};

use crate::{
    BenchmarkConfig, Result, RunBenchmark, SkipBenchmarkError, TaskAction, TaskConfig, PART_SIZE,
};

/// Benchmark runner using aws-s3-transfer-manager
#[derive(Clone)]
pub struct TransferManagerRunner {
    handle: Arc<Handle>,
}

struct Handle {
    config: BenchmarkConfig,
    transfer_manager: aws_s3_transfer_manager::Client,
    random_data_for_upload: Bytes,
}

impl TransferManagerRunner {
    pub async fn new(config: BenchmarkConfig) -> TransferManagerRunner {
        // Blugh, the user shouldn't need to manually configure concurrency like this.
        let total_concurrency = calculate_concurrency(config.target_throughput_gigabits_per_sec);

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

        let tm_config = aws_s3_transfer_manager::from_env()
            .concurrency(ConcurrencySetting::Explicit(total_concurrency))
            .part_size(PartSize::Target(PART_SIZE))
            .load()
            .await;

        let transfer_manager = aws_s3_transfer_manager::Client::new(tm_config);

        TransferManagerRunner {
            handle: Arc::new(Handle {
                config,
                transfer_manager,
                random_data_for_upload,
            }),
        }
    }

    async fn run_task(self, task_i: usize, parent_span: tracing::Span) -> Result<()> {
        let task_config = &self.config().workload.tasks[task_i];

        if self.config().workload.checksum.is_some() {
            return Err(SkipBenchmarkError("checksums not yet implemented".to_string()).into());
        }

        match task_config.action {
            TaskAction::Download => {
                self.download(task_config)
                    .instrument(info_span!(parent: parent_span, "download", key=task_config.key))
                    .await
            }
            TaskAction::Upload => {
                self.upload(task_config)
                    .instrument(info_span!(parent: parent_span, "upload", key=task_config.key))
                    .await
            }
        }
    }

    async fn download(&self, task_config: &TaskConfig) -> Result<()> {
        let key = &task_config.key;

        let mut download_handle = self
            .handle
            .transfer_manager
            .download()
            .bucket(&self.config().bucket)
            .key(key)
            .send()
            .instrument(info_span!("initial-send"))
            .await
            .with_context(|| format!("failed starting download: {key}"))?;

        // if files_on_disk: open file for writing
        let mut dest_file = if self.config().workload.files_on_disk {
            let file = File::create(key)
                .instrument(info_span!("file-open"))
                .await
                .with_context(|| format!("failed creating file: {key}"))?;
            Some(file)
        } else {
            None
        };

        let mut total_size = 0u64;
        while let Some(chunk_result) = download_handle
            .body_mut()
            .next()
            .instrument(info_span!("body-next"))
            .await
        {
            let mut chunk =
                chunk_result.with_context(|| format!("failed downloading next chunk of: {key}"))?;

            let chunk_size = chunk.remaining();
            total_size += chunk_size as u64;

            if let Some(dest_file) = &mut dest_file {
                dest_file
                    .write_all_buf(&mut chunk)
                    .instrument(info_span!("file-write", bytes = chunk_size))
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

        let upload_handle = self
            .handle
            .transfer_manager
            .upload()
            .bucket(&self.config().bucket)
            .key(key)
            .body(stream)
            .send()
            .instrument(info_span!("initial-send"))
            .await
            .with_context(|| format!("failed starting upload: {key}"))?;

        upload_handle
            .join()
            .instrument(info_span!("join"))
            .await
            .with_context(|| format!("failed uploading: {key}"))?;

        Ok(())
    }
}

#[async_trait]
impl RunBenchmark for TransferManagerRunner {
    async fn run(&self) -> Result<()> {
        // Spawn concurrent tasks for all uploads/downloads.
        // We want the benchmark to fail fast if anything goes wrong,
        // so we're using a JoinSet.
        let mut task_set: JoinSet<Result<()>> = JoinSet::new();
        for i in 0..self.config().workload.tasks.len() {
            let parent_span_of_task = tracing::Span::current();
            let task = self.clone().run_task(i, parent_span_of_task);
            task_set.spawn(task);
        }

        while let Some(join_result) = task_set.join_next().await {
            let task_result = join_result.unwrap();
            task_result?;
        }

        Ok(())
    }

    fn config(&self) -> &BenchmarkConfig {
        &self.handle.config
    }
}

/// Calculate the best concurrency, given target throughput.
/// This is based on aws-c-s3's math for determining max-http-connections, circa July 2024:
/// https://github.com/awslabs/aws-c-s3/blob/94e3342c12833c519900516edd2e85c78dc1639d/source/s3_client.c#L57-L69
/// These are magic numbers work well for large-object workloads.
fn calculate_concurrency(target_throughput_gigabits_per_sec: f64) -> usize {
    let concurrency = target_throughput_gigabits_per_sec * 2.5;
    (concurrency as usize).max(10)
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

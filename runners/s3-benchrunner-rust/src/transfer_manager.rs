use std::{iter::repeat_with, sync::Arc};

use anyhow::Context;
use async_trait::async_trait;
use aws_config::{self, BehaviorVersion, Region};
use aws_s3_transfer_manager::{
    io::InputStream,
    types::{ConcurrencySetting, PartSize},
};
use bytes::Bytes;
use tokio::fs::File;
use tokio::io::AsyncWriteExt;
use tokio::task::JoinSet;

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
        let sdk_config = aws_config::defaults(BehaviorVersion::latest())
            .region(Region::new(config.region.clone()))
            .load()
            .await;

        // Blugh, the user shouldn't need to manually configure concurrency like this.
        let total_concurrency = calculate_concurrency(config.target_throughput_gigabits_per_sec);
        let num_objects = config.workload.tasks.len();
        let concurrency_per_object = (total_concurrency / num_objects).max(1);

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
        let random_data_for_upload: Bytes = {
            // TODO: Can we optimize this further? Some ideas are trying a different library, using
            // 64-bit numbers, or generating a smaller buffer and then concatenating it a bunch of
            // times?
            let mut rng = fastrand::Rng::new();
            let data: Vec<u8> = repeat_with(|| rng.u8(..)).take(upload_data_size).collect();
            data.into()
        };

        let s3_client = aws_sdk_s3::Client::new(&sdk_config);
        let tm_config = aws_s3_transfer_manager::Config::builder()
            .concurrency(ConcurrencySetting::Explicit(concurrency_per_object))
            .part_size(PartSize::Target(PART_SIZE))
            .client(s3_client)
            .build();

        let transfer_manager = aws_s3_transfer_manager::Client::new(tm_config);

        TransferManagerRunner {
            handle: Arc::new(Handle {
                config,
                transfer_manager,
                random_data_for_upload,
            }),
        }
    }

    async fn run_task(self, task_i: usize) -> Result<()> {
        let task_config = &self.config().workload.tasks[task_i];

        if self.config().workload.checksum.is_some() {
            return Err(SkipBenchmarkError("checksums not yet implemented".to_string()).into());
        }

        match task_config.action {
            TaskAction::Download => self.download(task_config).await,
            TaskAction::Upload => self.upload(task_config).await,
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
            .await
            .with_context(|| format!("failed starting download: {key}"))?;

        // if files_on_disk: open file for writing
        let mut dest_file = if self.config().workload.files_on_disk {
            let file = File::create(key)
                .await
                .with_context(|| format!("failed creating file: {key}"))?;
            Some(file)
        } else {
            None
        };

        let mut total_size = 0u64;
        while let Some(chunk_result) = download_handle.body_mut().next().await {
            let chunk =
                chunk_result.with_context(|| format!("failed downloading next chunk of: {key}"))?;

            for segment in chunk.into_segments() {
                // if files_on_disk: write to file
                if let Some(dest_file) = &mut dest_file {
                    dest_file
                        .write_all(&segment)
                        .await
                        .with_context(|| format!("failed writing file: {key}"))?;
                }

                total_size += segment.len() as u64;
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

        self.handle
            .transfer_manager
            .upload()
            .bucket(&self.config().bucket)
            .key(key)
            .body(stream)
            .send()
            .await
            .with_context(|| format!("failed starting upload: {key}"))?
            .join()
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
            task_set.spawn(self.clone().run_task(i));
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

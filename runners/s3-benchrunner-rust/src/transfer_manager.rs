use anyhow::Context;
use async_trait::async_trait;
use aws_config::{self, BehaviorVersion, Region};
use aws_s3_transfer_manager::download::Downloader;
use aws_sdk_s3::{operation::get_object::builders::GetObjectInputBuilder, types::ChecksumMode};

use crate::{
    BenchmarkConfig, Result, RunBenchmark, RunnerError, TaskAction, TaskConfig, PART_SIZE,
};

/// Benchmark runner using aws-s3-transfer-manager
pub struct TransferManagerRunner {
    config: BenchmarkConfig,
    downloader: Downloader,
}

impl TransferManagerRunner {
    pub async fn new(config: BenchmarkConfig) -> TransferManagerRunner {
        let sdk_config = aws_config::defaults(BehaviorVersion::v2024_03_28())
            .region(Region::new(config.region.clone()))
            .load()
            .await;

        let downloader = Downloader::builder()
            .sdk_config(sdk_config)
            .target_part_size(PART_SIZE)
            .build();

        TransferManagerRunner { config, downloader }
    }

    async fn download(&self, task_config: &TaskConfig) -> Result<()> {
        let checksum_mode = if self.config.workload.checksum.is_some() {
            Some(ChecksumMode::Enabled)
        } else {
            None
        };

        let input = GetObjectInputBuilder::default()
            .bucket(&self.config.bucket)
            .key(&task_config.key)
            .set_checksum_mode(checksum_mode);

        let mut handle = self
            .downloader
            .download(input.into())
            .await
            .with_context(|| "download failed")?;

        let mut total_size: u64 = 0;
        while let Some(chunk_result) = handle.body.next().await {
            let chunk = chunk_result.with_context(|| "next body failed")?;
            for segment in chunk.into_segments() {
                total_size += segment.len() as u64;
            }
            // TODO: write to disk
        }

        assert_eq!(total_size, task_config.size);

        Ok(())
    }
}

#[async_trait]
impl RunBenchmark for TransferManagerRunner {
    async fn run(&self) -> Result<()> {
        // TODO: run tasks concurrently
        for task_config in self.config.workload.tasks.iter() {
            match task_config.action {
                TaskAction::Download => self.download(task_config).await?,
                TaskAction::Upload => {
                    return Err(RunnerError::SkipBenchmark(
                        "Upload not yet supported".to_string(),
                    ));
                }
            }
        }

        Ok(())
    }

    fn config(&self) -> &BenchmarkConfig {
        &self.config
    }
}

use crate::{BenchmarkConfig, BenchmarkRunner};

/// Benchmark runner using aws-s3-transfer-manager
pub struct TransferManagerRunner {
    config: BenchmarkConfig,
}

impl TransferManagerRunner {
    pub fn new(config: BenchmarkConfig) -> TransferManagerRunner {
        TransferManagerRunner { config }
    }
}

impl BenchmarkRunner for TransferManagerRunner {
    fn run(&self) {
        // TODO: actually run the workload
    }

    fn config(&self) -> &BenchmarkConfig {
        &self.config
    }
}

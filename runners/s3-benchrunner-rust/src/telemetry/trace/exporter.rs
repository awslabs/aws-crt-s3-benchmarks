//! code adapted from: https://github.com/open-telemetry/opentelemetry-rust/blob/3193320fa6dc17e89a7bed6090000aef781ac29c/opentelemetry-stdout/src/trace/exporter.rs

use anyhow::Context;
use core::fmt;
use futures_util::future::BoxFuture;
use opentelemetry_sdk::export::{self, trace::ExportResult};
use std::{
    fs::File,
    io::BufWriter,
    sync::{Arc, Mutex},
};

use crate::telemetry::trace::transform::SpanData;
use opentelemetry_sdk::resource::Resource;

/// Magic number based on: In Oct 2024, downloading 1 30GiB file generated 11,000+ batches per run.
/// This should give plenty of headroom for more tracing data and larger workloads.
const QUEUED_BATCHES_INITIAL_CAPACITY: usize = 2_097_152;

/// An OpenTelemetry exporter that queues up spans, and flushes them to a file when it's told
#[derive(Clone)]
pub struct SpanExporter {
    queued_batches: Arc<Mutex<Vec<SdkSpanDataBatch>>>,
    resource: Resource,
}

/// A batch of SDK SpanData, and the associated resource
pub struct SdkSpanDataBatch {
    pub resource: Resource,
    pub batch: Vec<export::trace::SpanData>,
}

impl fmt::Debug for SpanExporter {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("SpanExporter")
    }
}

impl opentelemetry_sdk::export::trace::SpanExporter for SpanExporter {
    fn export(&mut self, batch: Vec<export::trace::SpanData>) -> BoxFuture<'static, ExportResult> {
        // Queue batch, along with the current resource
        let batch = SdkSpanDataBatch {
            resource: self.resource.clone(),
            batch: batch,
        };
        self.queued_batches.lock().unwrap().push(batch);

        Box::pin(std::future::ready(ExportResult::Ok(())))
    }

    fn shutdown(&mut self) {}

    fn set_resource(&mut self, res: &opentelemetry_sdk::Resource) {
        self.resource = res.clone();
    }
}

impl SpanExporter {
    /// Create a span exporter with the current configuration
    pub fn new() -> SpanExporter {
        SpanExporter {
            queued_batches: Arc::new(Mutex::new(Vec::with_capacity(
                QUEUED_BATCHES_INITIAL_CAPACITY,
            ))),
            resource: Resource::empty(),
        }
    }

    pub fn flush_to_file(&mut self, path: &str) -> crate::Result<()> {
        // Take contents of self.queued_batches
        let queued_batches = {
            let mut _mutex_guard = self.queued_batches.lock().unwrap();
            let self_queue = &mut *_mutex_guard;
            let prev_capacity = self_queue.capacity();
            let new_queue = std::mem::take(self_queue);
            // take() resets capacity. Put it back where it was
            *self_queue = Vec::with_capacity(prev_capacity);
            new_queue
        };

        // Transform sdk spans into serde spans
        let span_data = SpanData::new(queued_batches);

        // Write to file
        let file =
            File::create_new(path).with_context(|| format!("Failed opening trace file: {path}"))?;
        let writer = BufWriter::new(file);
        serde_json::to_writer(writer, &span_data)
            .with_context(|| format!("Failed writing json to: {path}"))
    }
}

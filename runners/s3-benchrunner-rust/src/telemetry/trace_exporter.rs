// code adapted from https://github.com/open-telemetry/opentelemetry-rust/blob/v0.24.1/opentelemetry-stdout/src/trace/exporter.rs

use futures_util::future::BoxFuture;
use serde::Serialize;

pub struct JsonSpanExporter {
    writer: Option<Box<dyn std::io::Write + Send + Sync>>,
    otel_resource: opentelemetry_sdk::Resource,
}

impl JsonSpanExporter {
    pub fn new() -> Self {
        JsonSpanExporter {
            writer: Some(Box::new(std::io::stderr())),
            otel_resource: opentelemetry_sdk::Resource::empty(),
        }
    }
}

fn write_spans_as_json(
    writer: &mut dyn std::io::Write,
    serde_spans: SerdeSpanData,
) -> opentelemetry::trace::TraceResult<()> {
    serde_json::to_writer(writer, &serde_spans)
        .map_err(|e| opentelemetry::trace::TraceError::Other(Box::new(e)))
}

impl opentelemetry_sdk::export::trace::SpanExporter for JsonSpanExporter {
    fn export(
        &mut self,
        otel_spans: Vec<opentelemetry_sdk::export::trace::SpanData>,
    ) -> BoxFuture<'static, opentelemetry_sdk::export::trace::ExportResult> {
        let res: opentelemetry_sdk::export::trace::ExportResult = {
            if let Some(writer) = &mut self.writer {
                // convert to serde-compatible spans
                let serde_spans = SerdeSpanData::new(otel_spans, &self.otel_resource);

                write_spans_as_json(writer, serde_spans).and_then(|_| {
                    writer
                        .write_all(b"\n")
                        .map_err(|e| opentelemetry::trace::TraceError::Other(Box::new(e)))
                })
            } else {
                Err("exporter is shut down".into())
            }
        };

        Box::pin(std::future::ready(res))
    }

    fn shutdown(&mut self) {
        self.writer.take();
    }

    fn set_resource(&mut self, res: &opentelemetry_sdk::Resource) {
        self.otel_resource = res.clone();
    }
}

impl core::fmt::Debug for JsonSpanExporter {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.write_str("JsonSpanExporter")
    }
}

/// Transformed trace data that can be serialized
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct SerdeSpanData {
    // resource_spans: Vec<SerdeResourceSpans>,
}

impl SerdeSpanData {
    fn new(
        _otel_spans: Vec<opentelemetry_sdk::export::trace::SpanData>,
        _otel_resource: &opentelemetry_sdk::Resource,
    ) -> Self {
        SerdeSpanData {}
    }
}

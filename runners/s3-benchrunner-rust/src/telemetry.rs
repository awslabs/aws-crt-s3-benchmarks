//! code adapted from: https://github.com/tokio-rs/tracing-opentelemetry/blob/v0.24.0/examples/opentelemetry-otlp.rs

// Avoid adding `use` declarations to the top of this file.
// If you MUST shorten a path, add the `use` within a function.
// The examples this code is adapted from had `use` declarations, and
// I (graebm) found it hard to understand what all the boilerplate was doing.
// With full paths, it's clear that the boilerplate is about tying together
// different ecosystems (`opentelemetry` vs `tracing`). These ecosystems
// split their features among many crates, and full paths make it more clear.

use std::env;

use crate::Result;

pub mod common;
pub mod trace;

// Create OTEL Resource (the entity that produces telemetry)
fn otel_resource() -> opentelemetry_sdk::Resource {
    use opentelemetry::KeyValue;
    use opentelemetry_sdk::Resource;
    use opentelemetry_semantic_conventions::resource::SERVICE_NAME;

    Resource::default().merge(&Resource::new(vec![KeyValue::new(
        SERVICE_NAME,
        env!("CARGO_PKG_NAME"),
    )]))
}

// Construct OpenTelemetry TracerProvider
fn new_otel_tracer_provider() -> opentelemetry_sdk::trace::TracerProvider {
    opentelemetry_sdk::trace::TracerProvider::builder()
        .with_config(
            opentelemetry_sdk::trace::Config::default()
                // If export trace to AWS X-Ray, you can use XrayIdGenerator
                .with_id_generator(opentelemetry_sdk::trace::RandomIdGenerator::default())
                .with_resource(otel_resource()),
        )
        // .with_simple_exporter(opentelemetry_stdout::SpanExporter::default())
        .with_batch_exporter(
            crate::telemetry::trace::SpanExporter::default(),
            opentelemetry_sdk::runtime::Tokio,
        )
        .build()
}

/// TelemetryGuard ensures data gets flushed when the guard goes out of scope.
pub struct TelemetryGuard {
    otel_tracer_provider: opentelemetry_sdk::trace::TracerProvider,
}

impl Drop for TelemetryGuard {
    fn drop(&mut self) {
        opentelemetry::global::shutdown_tracer_provider();
    }
}

pub fn init_tracing_subscriber() -> Result<TelemetryGuard> {
    let otel_tracer_provider = new_otel_tracer_provider();

    use opentelemetry::trace::TracerProvider as _;
    let otel_tracer = otel_tracer_provider.tracer(env!("CARGO_PKG_NAME"));

    // We want data emitted from the `tracing` crate to be exported as OpenTelemetry data.
    // To do this, register an `OpenTelemetryLayer` as a `tracing_subscriber`.
    //
    // TODO: stop using `tracing_opentelemetry::OpenTelemetryLayer` (from makers of Tokio)
    // when OpenTelemetry adds `tracing` integration in the OpenTelemetry SDK itself.
    // - We've had issues where these crates don't all work together:
    //   https://github.com/tokio-rs/tracing-opentelemetry/issues/159
    // - OpenTelemetry says they're working on adding on their own integration:
    //   https://github.com/open-telemetry/opentelemetry-rust/issues/1571#issuecomment-2258910019)

    use tracing_subscriber::prelude::*;

    let filter = tracing_subscriber::EnvFilter::new("info")
        .add_directive("s3_benchrunner_rust=info".parse().unwrap())
        .add_directive("aws_s3_transfer_manager=debug".parse().unwrap());

    tracing_subscriber::registry()
        .with(filter)
        .with(tracing_opentelemetry::OpenTelemetryLayer::new(otel_tracer))
        .init();

    Ok(TelemetryGuard {
        otel_tracer_provider,
    })
}

impl TelemetryGuard {
    pub fn flush(&self) {
        for flush_result in self.otel_tracer_provider.force_flush() {
            if let Err(e) = flush_result {
                // don't treat as fatal error
                eprintln!("Failed to flush telemetry traces: {e:?}");
                return;
            }
        }
    }
}

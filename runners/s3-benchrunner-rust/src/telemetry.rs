//! code adapted from: https://github.com/tokio-rs/tracing-opentelemetry/blob/v0.24.0/examples/opentelemetry-otlp.rs

// Avoid adding `use` declarations to the top of this file.
// If you MUST shorten a path, add the `use` within a function.
// The examples this code is adapted from had `use` declarations, and
// I (graebm) found it hard to understand what all the boilerplate was doing.
// With full paths, it's clear that the boilerplate is about tying together
// different ecosystems (`opentelemetry` vs `tracing`). These ecosystems
// split their features among many crates, and full paths make it more clear.

use anyhow::Context;

use crate::Result;

// Create OTEL Resource (the entity that produces telemetry)
fn otel_resource() -> opentelemetry_sdk::Resource {
    use opentelemetry::KeyValue;
    use opentelemetry_semantic_conventions::{
        resource::{DEPLOYMENT_ENVIRONMENT, SERVICE_NAME, SERVICE_VERSION},
        SCHEMA_URL,
    };

    opentelemetry_sdk::Resource::from_schema_url(
        [
            KeyValue::new(SERVICE_NAME, env!("CARGO_PKG_NAME")),
            KeyValue::new(SERVICE_VERSION, env!("CARGO_PKG_VERSION")),
            KeyValue::new(DEPLOYMENT_ENVIRONMENT, "develop"),
        ],
        SCHEMA_URL,
    )
}

// Construct OpenTelemetry Tracer
fn new_otel_tracer() -> Result<opentelemetry_sdk::trace::Tracer> {
    use opentelemetry_sdk::trace::Sampler;

    opentelemetry_otlp::new_pipeline()
        .tracing()
        .with_trace_config(
            opentelemetry_sdk::trace::Config::default()
                // Customize sampling strategy
                .with_sampler(Sampler::ParentBased(Box::new(Sampler::TraceIdRatioBased(
                    1.0,
                ))))
                // If export trace to AWS X-Ray, you can use XrayIdGenerator
                .with_id_generator(opentelemetry_sdk::trace::RandomIdGenerator::default())
                .with_resource(otel_resource()),
        )
        .with_batch_config(opentelemetry_sdk::trace::BatchConfig::default())
        .with_exporter(opentelemetry_otlp::new_exporter().tonic())
        .install_batch(opentelemetry_sdk::runtime::Tokio)
        .with_context(|| format!(""))
}

/// TelemetryGuard ensures data gets flushed when the guard goes out of scope.
pub struct TelemetryGuard {
    otel_tracer: opentelemetry_sdk::trace::Tracer,
}

impl Drop for TelemetryGuard {
    fn drop(&mut self) {
        opentelemetry::global::shutdown_tracer_provider();
    }
}

pub fn init_tracing_subscriber() -> Result<TelemetryGuard> {
    let otel_tracer = new_otel_tracer()?;

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
        // .add_directive("s3_benchrunner_rust=info".parse().unwrap())
        // .add_directive("aws_s3_transfer_manager=debug".parse().unwrap())
        ;

    tracing_subscriber::registry()
        .with(filter)
        .with(tracing_opentelemetry::OpenTelemetryLayer::new(
            otel_tracer.clone(),
        ))
        .init();

    Ok(TelemetryGuard { otel_tracer })
}

impl TelemetryGuard {
    pub fn try_flush(&self) {
        let otel_sdk_tracer_provider = self.otel_tracer.provider().unwrap();
        for flush_result in otel_sdk_tracer_provider.force_flush() {
            if let Err(e) = flush_result {
                eprintln!("Failed to flush telemetry traces: {e:?}");
                return
            }
        }
    }
}

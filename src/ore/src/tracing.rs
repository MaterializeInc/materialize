// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Utilities for configuring [`tracing`]

use std::collections::HashMap;
use std::io;
use std::time::Duration;

use http::HeaderMap;
use hyper::client::HttpConnector;
use hyper_tls::HttpsConnector;
use opentelemetry::global;
use opentelemetry::propagation::{Extractor, Injector};
use opentelemetry::sdk::propagation::TraceContextPropagator;
use opentelemetry::sdk::{trace, Resource};
use opentelemetry::KeyValue;
use tonic::metadata::MetadataMap;
use tonic::transport::Endpoint;
use tracing::{Event, Level, Subscriber};
use tracing_opentelemetry::OpenTelemetrySpanExt;
use tracing_subscriber::filter::{LevelFilter, Targets};
use tracing_subscriber::fmt::format::{format, Writer};
use tracing_subscriber::fmt::{self, FmtContext, FormatEvent, FormatFields};
use tracing_subscriber::layer::{Layer, SubscriberExt};
use tracing_subscriber::registry::LookupSpan;
use tracing_subscriber::util::SubscriberInitExt;

/// Configuration for setting up [`tracing`]
#[derive(Debug)]
pub struct TracingConfig {
    /// The name of the service being traced.
    pub service_name: String,
    /// Whether to prefix each log line with the service name.
    pub log_service_name: bool,
    /// `Targets` filter string.
    pub log_filter: Targets,
    /// When `Some(_)`, the https endpoint to send
    /// opentelemetry traces.
    pub opentelemetry_config: Option<OpenTelemetryConfig>,
    /// When enabled, optionally turn on the
    /// tokio console.
    #[cfg(feature = "tokio-console")]
    pub tokio_console: bool,
}

/// Configuration to setup Opentelemetry [`tracing`] subscribers
#[derive(Debug)]
pub struct OpenTelemetryConfig {
    /// When `Some(_)`, the https endpoint to send
    /// opentelemetry traces.
    pub endpoint: String,
    /// When `opentelemetry_endpoint` is `Some(_)`,
    /// configure additional comma separated
    /// headers.
    pub headers: HeaderMap,
    /// When `opentelemetry_endpoint` is `Some(_)`,
    /// configure the opentelemetry layer with this log filter.
    // TODO(guswynn): switch to just `Targets` when all processes
    // have this supported.
    pub log_filter: Option<Targets>,
}

/// Configures [`tracing`] and OpenTelemetry.
// Setting up OpenTelemetry in the background requires we are in a Tokio runtime
// context, hence the `async`.
#[allow(clippy::unused_async)]
pub async fn configure(config: TracingConfig) -> Result<(), anyhow::Error> {
    let fmt_layer = fmt::layer()
        .event_format(PrefixFormat {
            inner: format(),
            prefix: config.log_service_name.then(|| config.service_name.clone()),
        })
        .with_writer(io::stderr)
        .with_ansi(atty::is(atty::Stream::Stderr))
        // Ensure panics are logged, even if the user has specified
        // otherwise.
        .with_filter(config.log_filter.with_target("panic", LevelFilter::ERROR));

    let otel_layer = if let Some(otel_config) = config.opentelemetry_config {
        // TODO(guswynn): figure out where/how to call
        // opentelemetry::global::shutdown_tracer_provider();
        opentelemetry::global::set_text_map_propagator(TraceContextPropagator::new());

        // Manually set up an OpenSSL-backed, h2, proxied `Channel`,
        // with the timeout configured according to:
        // https://docs.rs/opentelemetry-otlp/latest/opentelemetry_otlp/struct.TonicExporterBuilder.html#method.with_channel
        let channel = Endpoint::from_shared(otel_config.endpoint)?
            .timeout(Duration::from_secs(
                opentelemetry_otlp::OTEL_EXPORTER_OTLP_TIMEOUT_DEFAULT,
            ))
            // TODO(guswynn): investigate if this should be non-lazy.
            .connect_with_connector_lazy({
                let mut http = HttpConnector::new();
                http.enforce_http(false);
                HttpsConnector::from((
                    http,
                    // This is the same as the default, plus an h2 ALPN request.
                    tokio_native_tls::TlsConnector::from(
                        native_tls::TlsConnector::builder()
                            .request_alpns(&["h2"])
                            .build()
                            .unwrap(),
                    ),
                ))
            });
        let exporter = opentelemetry_otlp::new_exporter()
            .tonic()
            .with_channel(channel)
            .with_metadata(MetadataMap::from_headers(otel_config.headers));
        let tracer = opentelemetry_otlp::new_pipeline()
            .tracing()
            .with_trace_config(trace::config().with_resource(Resource::new([KeyValue::new(
                "service.name",
                config.service_name,
            )])))
            .with_exporter(exporter)
            .install_batch(opentelemetry::runtime::Tokio)
            .unwrap();
        let layer = tracing_opentelemetry::layer()
            .with_tracer(tracer)
            .with_filter(
                otel_config
                    .log_filter
                    .unwrap_or_else(|| Targets::new().with_default(LevelFilter::DEBUG)),
            );
        Some(layer)
    } else {
        None
    };

    let stack = tracing_subscriber::registry();
    let stack = stack.with(fmt_layer);
    let stack = stack.with(otel_layer);
    #[cfg(feature = "tokio-console")]
    let stack = stack.with(config.tokio_console.then(|| console_subscriber::spawn()));
    stack.init();

    Ok(())
}

/// Returns the level of a specific target from a [`Targets`].
pub fn target_level(targets: &Targets, target: &str) -> Level {
    if targets.would_enable(target, &Level::TRACE) {
        Level::TRACE
    } else if targets.would_enable(target, &Level::DEBUG) {
        Level::DEBUG
    } else if targets.would_enable(target, &Level::INFO) {
        Level::INFO
    } else if targets.would_enable(target, &Level::WARN) {
        Level::WARN
    } else {
        Level::ERROR
    }
}

/// A wrapper around a [`tracing_subscriber::Format`] that adds an optional
/// prefix to each event.
#[derive(Debug)]
pub struct PrefixFormat<F> {
    inner: F,
    prefix: Option<String>,
}

impl<F, C, N> FormatEvent<C, N> for PrefixFormat<F>
where
    C: Subscriber + for<'a> LookupSpan<'a>,
    N: for<'a> FormatFields<'a> + 'static,
    F: FormatEvent<C, N>,
{
    fn format_event(
        &self,
        ctx: &FmtContext<'_, C, N>,
        mut writer: Writer<'_>,
        event: &Event<'_>,
    ) -> std::fmt::Result {
        match &self.prefix {
            None => self.inner.format_event(ctx, writer, event)?,
            Some(prefix) => {
                let style = ansi_term::Style::new();
                let target_style = if writer.has_ansi_escapes() {
                    style.bold()
                } else {
                    style
                };
                write!(
                    writer,
                    "{}{}:{} ",
                    target_style.prefix(),
                    prefix,
                    target_style.infix(style)
                )?;
                self.inner.format_event(ctx, writer, event)?;
            }
        }
        Ok(())
    }
}

/// An OpenTelemetry context.
///
/// Allows associating [`tracing`] spans across task or thread boundaries.
#[derive(Debug, Clone)]
pub struct OpenTelemetryContext {
    inner: HashMap<String, String>,
}

impl OpenTelemetryContext {
    /// Attaches this `Context` into the current `tracing` span.
    pub fn attach_as_parent(&self) {
        let parent_cx = global::get_text_map_propagator(|prop| prop.extract(&self.inner));
        tracing::Span::current().set_parent(parent_cx);
    }

    /// Obtain a `Context` from the current `tracing` span.
    pub fn obtain() -> Self {
        let mut map = std::collections::HashMap::new();
        global::get_text_map_propagator(|propagator| {
            propagator.inject_context(&tracing::Span::current().context(), &mut map)
        });

        Self { inner: map }
    }

    /// Obtain an empty `Context`.
    pub fn empty() -> Self {
        Self {
            inner: HashMap::new(),
        }
    }
}

impl Extractor for OpenTelemetryContext {
    fn get(&self, key: &str) -> Option<&str> {
        Extractor::get(&self.inner, key)
    }
    fn keys(&self) -> Vec<&str> {
        Extractor::keys(&self.inner)
    }
}

impl Injector for OpenTelemetryContext {
    fn set(&mut self, key: &str, value: String) {
        Injector::set(&mut self.inner, key, value)
    }
}

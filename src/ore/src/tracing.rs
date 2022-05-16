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
use opentelemetry::sdk::{trace, Resource};
use opentelemetry::KeyValue;
use tonic::metadata::MetadataMap;
use tonic::transport::Endpoint;
use tracing::{Event, Level, Subscriber};
use tracing_opentelemetry::OpenTelemetrySpanExt;
use tracing_subscriber::filter::{LevelFilter, Targets};
use tracing_subscriber::fmt::format::{format, Format, Writer};
use tracing_subscriber::fmt::{self, FmtContext, FormatEvent, FormatFields};
use tracing_subscriber::layer::{Layer, Layered, SubscriberExt};
use tracing_subscriber::registry::LookupSpan;
use tracing_subscriber::util::SubscriberInitExt;

fn create_h2_alpn_https_connector() -> HttpsConnector<HttpConnector> {
    // This accomplishes the same thing as the default
    // + adding a `request_alpn`
    let mut http = HttpConnector::new();
    http.enforce_http(false);

    HttpsConnector::from((
        http,
        tokio_native_tls::TlsConnector::from(
            native_tls::TlsConnector::builder()
                .request_alpns(&["h2"])
                .build()
                .unwrap(),
        ),
    ))
}

/// Setting up opentel in the background requires we are in a tokio-runtime
/// context, hence the `async`
// TODO(guswynn): figure out where/how to call opentelemetry::global::shutdown_tracer_provider();
#[allow(clippy::unused_async)]
async fn configure_opentelemetry_and_init<
    L: Layer<S> + Send + Sync + 'static,
    S: Subscriber + Send + Sync + 'static,
>(
    stack: Layered<L, S>,
    opentelemetry_config: Option<OpenTelemetryConfig>,
) -> Result<(), anyhow::Error>
where
    Layered<L, S>: tracing_subscriber::util::SubscriberInitExt,
    for<'ls> S: LookupSpan<'ls>,
{
    if let Some(otel_config) = opentelemetry_config {
        use opentelemetry::sdk::propagation::TraceContextPropagator;
        opentelemetry::global::set_text_map_propagator(TraceContextPropagator::new());

        // Manually setup an openssl-backed, h2, proxied `Channel`,
        // and setup the timeout according to
        // https://docs.rs/opentelemetry-otlp/latest/opentelemetry_otlp/struct.TonicExporterBuilder.html#method.with_channel
        let endpoint = Endpoint::from_shared(otel_config.endpoint)?.timeout(Duration::from_secs(
            opentelemetry_otlp::OTEL_EXPORTER_OTLP_TIMEOUT_DEFAULT,
        ));

        // TODO(guswynn): investigate if this should be non-lazy
        let channel = endpoint.connect_with_connector_lazy(create_h2_alpn_https_connector());
        let otlp_exporter = opentelemetry_otlp::new_exporter()
            .tonic()
            .with_channel(channel)
            .with_metadata(MetadataMap::from_headers(otel_config.headers));

        let tracer =
            opentelemetry_otlp::new_pipeline()
                .tracing()
                .with_trace_config(trace::config().with_resource(Resource::new(vec![
                    KeyValue::new("service.name", "materialized"),
                ])))
                .with_exporter(otlp_exporter)
                .install_batch(opentelemetry::runtime::Tokio)
                .unwrap();
        let otel_layer = tracing_opentelemetry::layer().with_tracer(tracer);

        let stack = stack.with(
            otel_layer.with_filter(
                otel_config
                    .log_filter
                    .unwrap_or_else(|| Targets::new().with_default(LevelFilter::DEBUG)),
            ),
        );

        stack.init();
        Ok(())
    } else {
        stack.init();
        Ok(())
    }
}

/// Configuration for setting up [`tracing`]
#[derive(Debug)]
pub struct TracingConfig {
    /// `Targets` filter string.
    pub log_filter: Targets,
    /// When `Some(_)`, the https endpoint to send
    /// opentelemetry traces.
    pub opentelemetry_config: Option<OpenTelemetryConfig>,
    /// Optional prefix for log lines
    pub prefix: Option<String>,
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
pub async fn configure(config: TracingConfig) -> Result<(), anyhow::Error> {
    // NOTE: Try harder than usual to avoid panicking in this function. It runs
    // before our custom panic hook is installed (because the panic hook needs
    // tracing configured to execute), so a panic here will not direct the
    // user to file a bug report.

    // Ensure panics are logged, even if the user has specified
    // otherwise.
    let filter = config.log_filter.with_target("panic", LevelFilter::ERROR);

    let stack = tracing_subscriber::registry().with(
        fmt::layer()
            .with_writer(io::stderr)
            .with_ansi(atty::is(atty::Stream::Stderr))
            .event_format(SubprocessFormat::new_default(config.prefix))
            .with_filter(filter),
    );

    #[cfg(feature = "tokio-console")]
    let stack = stack.with(config.tokio_console.then(|| console_subscriber::spawn()));

    configure_opentelemetry_and_init(stack, config.opentelemetry_config).await?;

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

/// A wrapper around a `tracing_subscriber` `Format` that
/// prepends a subprocess name to the event logs
#[derive(Debug)]
pub struct SubprocessFormat<F> {
    inner: F,
    process_name: Option<String>,
}

impl SubprocessFormat<Format> {
    /// Make a new `SubprocessFormat` that wraps
    /// the `tracing_subscriber` default [`FormatEvent`].
    pub fn new_default(process_name: Option<String>) -> Self {
        SubprocessFormat {
            inner: format(),
            process_name,
        }
    }
}

impl<F, C, N> FormatEvent<C, N> for SubprocessFormat<F>
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
        match &self.process_name {
            None => self.inner.format_event(ctx, writer, event)?,
            Some(process_name) => {
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
                    process_name,
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

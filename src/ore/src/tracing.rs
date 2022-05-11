// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Utilities for configuring [`tracing`]

use std::io::{self, Write};
use std::time::Duration;

use http::HeaderMap;
use hyper::client::HttpConnector;
use hyper_tls::HttpsConnector;
use opentelemetry::sdk::{trace, Resource};
use opentelemetry::KeyValue;
use tonic::metadata::MetadataMap;
use tonic::transport::Endpoint;
use tracing::{Event, Subscriber};
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
#[allow(clippy::unused_async)]
async fn configure_opentelemetry_and_init<
    L: Layer<S> + Send + Sync + 'static,
    S: Subscriber + Send + Sync + 'static,
>(
    stack: Layered<L, S>,
    opentelemetry_endpoint: Option<String>,
    opentelemetry_headers: HeaderMap,
) -> Result<(), anyhow::Error>
where
    Layered<L, S>: tracing_subscriber::util::SubscriberInitExt,
    for<'ls> S: LookupSpan<'ls>,
{
    if let Some(endpoint) = opentelemetry_endpoint {
        // Manually setup an openssl-backed, h2, proxied `Channel`,
        // and setup the timeout according to
        // https://docs.rs/opentelemetry-otlp/latest/opentelemetry_otlp/struct.TonicExporterBuilder.html#method.with_channel
        let endpoint = Endpoint::from_shared(endpoint)?.timeout(Duration::from_secs(
            opentelemetry_otlp::OTEL_EXPORTER_OTLP_TIMEOUT_DEFAULT,
        ));

        // TODO(guswynn): investigate if this should be non-lazy
        let channel = endpoint.connect_with_connector_lazy(create_h2_alpn_https_connector());
        let otlp_exporter = opentelemetry_otlp::new_exporter()
            .tonic()
            .with_channel(channel)
            .with_metadata(MetadataMap::from_headers(opentelemetry_headers));

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

        let stack = stack.with(otel_layer);

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
    pub opentelemetry_endpoint: Option<String>,
    /// When `opentelemetry_endpoint` is `Some(_)`,
    /// configure additional comma separated
    /// headers.
    pub opentelemetry_headers: HeaderMap,
    /// Optional prefix for log lines
    pub prefix: Option<String>,
    /// When enabled, optionally turn on the
    /// tokio console.
    #[cfg(feature = "tokio-console")]
    pub tokio_console: bool,
}

/// Configures tracing according to the provided command-line arguments.
/// Returns a `Write` stream that represents the main place `tracing` will
/// log to.
pub async fn configure(config: TracingConfig) -> Result<Box<dyn Write>, anyhow::Error> {
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

    configure_opentelemetry_and_init(
        stack,
        config.opentelemetry_endpoint,
        config.opentelemetry_headers,
    )
    .await?;

    Ok(Box::new(io::stderr()))
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

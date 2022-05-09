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
use std::marker::PhantomData;
use std::str::FromStr;
use std::time::Duration;

use anyhow::Context as _;
use hyper::client::HttpConnector;
use hyper_tls::HttpsConnector;
use opentelemetry::sdk::{trace, Resource};
use opentelemetry::KeyValue;
use prometheus::IntCounterVec;
use tonic::metadata::{MetadataKey, MetadataMap};
use tonic::transport::Endpoint;
use tracing::{Event, Subscriber};
use tracing_subscriber::filter::{LevelFilter, Targets};
use tracing_subscriber::fmt::format::{format, Format, Writer};
use tracing_subscriber::fmt::{self, FmtContext, FormatEvent, FormatFields};
use tracing_subscriber::layer::{Context, Layer, Layered, SubscriberExt};
use tracing_subscriber::registry::LookupSpan;
use tracing_subscriber::util::SubscriberInitExt;

use crate::metric;
use crate::metrics::MetricsRegistry;

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
    opentelemetry_endpoint: Option<&str>,
    opentelemetry_headers: Option<&str>,
) -> Result<(), anyhow::Error>
where
    Layered<L, S>: tracing_subscriber::util::SubscriberInitExt,
    for<'ls> S: LookupSpan<'ls>,
{
    if let Some(endpoint) = opentelemetry_endpoint {
        // Manually setup an openssl-backed, h2, proxied `Channel`,
        // and setup the timeout according to
        // https://docs.rs/opentelemetry-otlp/latest/opentelemetry_otlp/struct.TonicExporterBuilder.html#method.with_channel
        let endpoint = Endpoint::from_shared(endpoint.to_owned())?.timeout(Duration::from_secs(
            opentelemetry_otlp::OTEL_EXPORTER_OTLP_TIMEOUT_DEFAULT,
        ));

        // TODO(guswynn): investigate if this should be non-lazy
        let channel = endpoint.connect_with_connector_lazy(create_h2_alpn_https_connector());
        let otlp_exporter = opentelemetry_otlp::new_exporter()
            .tonic()
            .with_channel(channel);

        let otlp_exporter = if let Some(headers) = opentelemetry_headers {
            let mut mmap = MetadataMap::new();
            for header in headers.split(',') {
                let mut splits = header.splitn(2, '=');
                let k = splits
                    .next()
                    .context("opentelemetry-headers must be of the form key=value")?;
                let v = splits
                    .next()
                    .context("opentelemetry-headers must be of the form key=value")?;

                mmap.insert(MetadataKey::from_str(k)?, v.parse()?);
            }
            otlp_exporter.with_metadata(mmap)
        } else {
            otlp_exporter
        };

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
pub struct TracingConfig<'a> {
    /// `Targets` filter string.
    pub log_filter: &'a str,
    /// When `Some(_)`, the https endpoint to send
    /// opentelemetry traces.
    pub opentelemetry_endpoint: Option<&'a str>,
    /// When `opentelemetry_endpoint` is `Some(_)`,
    /// configure additional comma separated
    /// headers.
    pub opentelemetry_headers: Option<&'a str>,
    /// Optional prefix for log lines
    pub prefix: Option<&'a str>,
    /// When enabled, optionally turn on the
    /// tokio console.
    #[cfg(feature = "tokio-console")]
    pub tokio_console: bool,
}

/// Configures tracing according to the provided command-line arguments.
/// Returns a `Write` stream that represents the main place `tracing` will
/// log to.
pub async fn configure(
    config: TracingConfig<'_>,
    metrics_registry: &MetricsRegistry,
) -> Result<Box<dyn Write>, anyhow::Error> {
    // NOTE: Try harder than usual to avoid panicking in this function. It runs
    // before our custom panic hook is installed (because the panic hook needs
    // tracing configured to execute), so a panic here will not direct the
    // user to file a bug report.

    let filter = Targets::from_str(config.log_filter)
        .context("parsing --log-filter option")?
        // Ensure panics are logged, even if the user has specified
        // otherwise.
        .with_target("panic", LevelFilter::ERROR);

    let log_message_counter: IntCounterVec = metrics_registry.register(metric!(
        name: "mz_log_message_total",
        help: "The number of log messages produced by this materialized instance",
        var_labels: ["severity"],
    ));

    let stack = tracing_subscriber::registry()
        .with(MetricsRecorderLayer::new(log_message_counter).with_filter(filter.clone()))
        .with(
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

/// A tracing [`Layer`] that allows hooking into the reporting/filtering chain
/// for log messages, incrementing a counter for the severity of messages
/// reported.
#[derive(Debug)]
pub struct MetricsRecorderLayer<S> {
    counter: IntCounterVec,
    _inner: PhantomData<S>,
}

impl<S> MetricsRecorderLayer<S> {
    /// Construct a metrics-recording layer.
    pub fn new(counter: IntCounterVec) -> Self {
        Self {
            counter,
            _inner: PhantomData,
        }
    }
}

impl<S> Layer<S> for MetricsRecorderLayer<S>
where
    S: Subscriber + for<'a> LookupSpan<'a>,
{
    fn on_event(&self, ev: &Event<'_>, _ctx: Context<'_, S>) {
        let metadata = ev.metadata();
        self.counter
            .with_label_values(&[&metadata.level().to_string()])
            .inc();
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
    pub fn new_default(process_name: Option<impl Into<String>>) -> Self {
        SubprocessFormat {
            inner: format(),
            process_name: process_name.map(|pn| pn.into()),
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

#[cfg(test)]
mod test {
    use std::collections::HashMap;

    use super::MetricsRecorderLayer;
    use crate::metric;
    use crate::metrics::raw::IntCounterVec;
    use crate::metrics::MetricsRegistry;
    use tracing::{error, info, warn};
    use tracing_subscriber::layer::SubscriberExt;
    use tracing_subscriber::util::SubscriberInitExt;

    #[test]
    fn increments_per_sev_counter() {
        let r = MetricsRegistry::new();
        let counter: IntCounterVec = r.register(metric!(
            name: "test_counter",
            help: "a test counter",
            var_labels: ["severity"],
        ));
        tracing_subscriber::registry()
            .with(MetricsRecorderLayer::new(counter))
            .init();

        info!("test message");
        (0..5).for_each(|_| warn!("a warning"));
        error!("test error");
        error!("test error");

        println!("gathered: {:?}", r.gather());

        let metric = r
            .gather()
            .into_iter()
            .find(|fam| fam.get_name() == "test_counter")
            .expect("Didn't find the counter we set up");
        let mut sevs: HashMap<&str, u32> = HashMap::new();
        for counter in metric.get_metric() {
            let sev = counter.get_label()[0].get_value();
            sevs.insert(sev, counter.get_counter().get_value() as u32);
        }
        let mut sevs: Vec<(&str, u32)> = sevs.into_iter().collect();
        sevs.sort_by_key(|(name, _)| name.to_string());
        assert_eq!(&[("ERROR", 2), ("INFO", 1), ("WARN", 5)][..], &sevs[..]);
    }
}

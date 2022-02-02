// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::fs;
use std::io;
use std::marker::PhantomData;
use std::path::PathBuf;
use std::str::FromStr;

use anyhow::Context as _;
use hyper::client::HttpConnector;
use hyper_proxy::ProxyConnector;
use hyper_tls::HttpsConnector;
use prometheus::IntCounterVec;
use tokio::runtime::Runtime;
use tracing::{Event, Subscriber};
use tracing_subscriber::filter::{LevelFilter, Targets};
use tracing_subscriber::fmt;
use tracing_subscriber::layer::{Context, Layer, SubscriberExt};
use tracing_subscriber::registry::LookupSpan;
use tracing_subscriber::util::SubscriberInitExt;

use mz_ore::metric;
use mz_ore::metrics::{MetricsRegistry, ThirdPartyMetric};

use crate::Args;

fn create_h2_alpn_https_connector() -> ProxyConnector<HttpsConnector<HttpConnector>> {
    // This accomplishes the same thing as the default
    // + adding a `request_alpn`
    let mut http = HttpConnector::new();
    http.enforce_http(false);

    mz_http_proxy::hyper::connector(HttpsConnector::from((
        http,
        tokio_native_tls::TlsConnector::from(
            native_tls::TlsConnector::builder()
                .request_alpns(&["h2"])
                .build()
                .unwrap(),
        ),
    )))
}

fn configure_opentelemetry_and_init<
    L: tracing_subscriber::layer::Layer<S> + Send + Sync + 'static,
    S: ::tracing::Subscriber + Send + Sync + 'static,
>(
    stack: tracing_subscriber::layer::Layered<L, S>,
    runtime: &tokio::runtime::Runtime,
    opentelemetry_endpoint: Option<String>,
    opentelemetry_headers: Option<String>,
) -> Result<(), anyhow::Error>
where
    tracing_subscriber::layer::Layered<L, S>: tracing_subscriber::util::SubscriberInitExt,
    for<'ls> S: tracing_subscriber::registry::LookupSpan<'ls>,
{
    if let Some(endpoint) = opentelemetry_endpoint {
        // Setting up opentel in the background requires we are in a tokio-runtime
        // context
        let _guard = runtime.enter();

        // Manually setup an openssl-backed, h2, proxied `Channel`
        let endpoint = tonic::transport::Endpoint::from_shared(endpoint)?;
        let channel = endpoint.connect_with_connector_lazy(create_h2_alpn_https_connector())?;
        let mut mmap = tonic::metadata::MetadataMap::new();
        // clap guarantees this is set if `opentelemetry_endpoint` is set
        for header in opentelemetry_headers.unwrap().split(',') {
            let mut splits = header.splitn(2, '=');
            let k = splits
                .next()
                .context("opentelemetry-headers must be of the form key=value")?;
            let v = splits
                .next()
                .context("opentelemetry-headers must be of the form key=value")?;

            mmap.insert(tonic::metadata::MetadataKey::from_str(k)?, v.parse()?);
        }

        let otlp_exporter = opentelemetry_otlp::new_exporter()
            .tonic()
            .with_channel(channel)
            .with_metadata(mmap);

        let tracer = opentelemetry_otlp::new_pipeline()
            .tracing()
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
/// Configures tracing according to the provided command-line arguments.
pub fn configure(
    args: &Args,
    metrics_registry: &MetricsRegistry,
    runtime: &Runtime,
) -> Result<(), anyhow::Error> {
    // NOTE: Try harder than usual to avoid panicking in this function. It runs
    // before our custom panic hook is installed (because the panic hook needs
    // tracing configured to execute), so a panic here will not direct the
    // user to file a bug report.

    let filter = Targets::from_str(&args.log_filter)
        .context("parsing --log-filter option")?
        // Ensure panics are logged, even if the user has specified
        // otherwise.
        .with_target("panic", LevelFilter::ERROR);

    let log_message_counter: ThirdPartyMetric<IntCounterVec> = metrics_registry
        .register_third_party_visible(metric!(
            name: "mz_log_message_total",
            help: "The number of log messages produced by this materialized instance",
            var_labels: ["severity"],
        ));

    match args.log_file.as_deref() {
        Some("stderr") => {
            // The user explicitly directed logs to stderr. Log only to
            // stderr with the user-specified `filter`.
            let stack = tracing_subscriber::registry()
                .with(MetricsRecorderLayer::new(log_message_counter).with_filter(filter.clone()))
                .with(
                    fmt::layer()
                        .with_writer(io::stderr)
                        .with_ansi(atty::is(atty::Stream::Stderr))
                        .with_filter(filter),
                );

            #[cfg(feature = "tokio-console")]
            let stack = stack.with(args.tokio_console.then(|| console_subscriber::spawn()));

            configure_opentelemetry_and_init(
                stack,
                runtime,
                args.opentelemetry_endpoint.clone(),
                args.opentelemetry_headers.clone(),
            )?
        }
        log_file => {
            // Logging to a file. If the user did not explicitly specify
            // a file, bubble up warnings and errors to stderr.
            let stderr_level = match log_file {
                Some(_) => LevelFilter::OFF,
                None => LevelFilter::WARN,
            };
            let stack = tracing_subscriber::registry()
                .with(MetricsRecorderLayer::new(log_message_counter).with_filter(filter.clone()))
                .with({
                    let path = match log_file {
                        Some(log_file) => PathBuf::from(log_file),
                        None => args.data_directory.join("materialized.log"),
                    };
                    if let Some(parent) = path.parent() {
                        fs::create_dir_all(parent).with_context(|| {
                            format!("creating log file directory: {}", parent.display())
                        })?;
                    }
                    let file = fs::OpenOptions::new()
                        .append(true)
                        .create(true)
                        .open(&path)
                        .with_context(|| format!("creating log file: {}", path.display()))?;
                    fmt::layer()
                        .with_ansi(false)
                        .with_writer(move || file.try_clone().expect("failed to clone log file"))
                        .with_filter(filter.clone())
                })
                .with(
                    fmt::layer()
                        .with_writer(io::stderr)
                        .with_ansi(atty::is(atty::Stream::Stderr))
                        .with_filter(stderr_level)
                        .with_filter(filter),
                );

            #[cfg(feature = "tokio-console")]
            let stack = stack.with(args.tokio_console.then(|| console_subscriber::spawn()));

            configure_opentelemetry_and_init(
                stack,
                runtime,
                args.opentelemetry_endpoint.clone(),
                args.opentelemetry_headers.clone(),
            )?
        }
    }

    Ok(())
}

/// A tracing [`Layer`] that allows hooking into the reporting/filtering chain
/// for log messages, incrementing a counter for the severity of messages
/// reported.
pub struct MetricsRecorderLayer<S> {
    counter: ThirdPartyMetric<IntCounterVec>,
    _inner: PhantomData<S>,
}

impl<S> MetricsRecorderLayer<S> {
    /// Construct a metrics-recording layer.
    pub fn new(counter: ThirdPartyMetric<IntCounterVec>) -> Self {
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
            .third_party_metric_with_label_values(&[&metadata.level().to_string()])
            .inc();
    }
}

#[cfg(test)]
mod test {
    use std::collections::HashMap;

    use super::MetricsRecorderLayer;
    use mz_ore::metric;
    use mz_ore::metrics::raw::IntCounterVec;
    use mz_ore::metrics::{MetricsRegistry, ThirdPartyMetric};
    use tracing::{error, info, warn};
    use tracing_subscriber::layer::SubscriberExt;
    use tracing_subscriber::util::SubscriberInitExt;

    #[test]
    fn increments_per_sev_counter() {
        let r = MetricsRegistry::new();
        let counter: ThirdPartyMetric<IntCounterVec> = r.register_third_party_visible(metric!(
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

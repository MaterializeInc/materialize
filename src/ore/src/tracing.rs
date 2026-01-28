// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License in the LICENSE file at the
// root of this repository, or online at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Tracing utilities.
//!
//! This module contains application tracing utilities built on top of the
//! [`tracing`] and [`opentelemetry`] libraries. The key exports are:
//!
//!  * The **[`configure`]** function, which configures the `tracing` and
//!    `opentelemetry` crates with sensible defaults and should be called during
//!    initialization of every Materialize binary.
//!
//!  * The **[`OpenTelemetryContext`]** type, which carries a tracing span
//!    across thread or task boundaries within a process.

use std::collections::BTreeMap;
use std::hash::{Hash, Hasher};
use std::io;
use std::io::IsTerminal;
use std::str::FromStr;
use std::sync::LazyLock;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;

#[cfg(feature = "tokio-console")]
use console_subscriber::ConsoleLayer;
use derivative::Derivative;
use http::HeaderMap;
use hyper_tls::HttpsConnector;
use hyper_util::client::legacy::connect::HttpConnector;
use opentelemetry::propagation::{Extractor, Injector};
use opentelemetry::trace::TracerProvider;
use opentelemetry::{KeyValue, global};
use opentelemetry_otlp::WithTonicConfig;
use opentelemetry_sdk::propagation::TraceContextPropagator;
use opentelemetry_sdk::runtime::Tokio;
use opentelemetry_sdk::trace::span_processor_with_async_runtime::BatchSpanProcessor;
use opentelemetry_sdk::{Resource, trace};
use prometheus::IntCounter;
use tonic::metadata::MetadataMap;
use tonic::transport::Endpoint;
use tracing::{Event, Level, Span, Subscriber, warn};
#[cfg(feature = "capture")]
use tracing_capture::{CaptureLayer, SharedStorage};
use tracing_opentelemetry::OpenTelemetrySpanExt;
use tracing_subscriber::filter::Directive;
use tracing_subscriber::fmt::format::{Writer, format};
use tracing_subscriber::fmt::{self, FmtContext, FormatEvent, FormatFields};
use tracing_subscriber::layer::{Layer, SubscriberExt};
use tracing_subscriber::registry::LookupSpan;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::{EnvFilter, Registry, reload};

use crate::metric;
use crate::metrics::MetricsRegistry;
#[cfg(feature = "tokio-console")]
use crate::netio::SocketAddr;
use crate::now::{EpochMillis, NowFn, SYSTEM_TIME};

/// Application tracing configuration.
///
/// See the [`configure`] function for details.
#[derive(Derivative)]
#[derivative(Debug)]
pub struct TracingConfig<F> {
    /// The name of the service.
    pub service_name: &'static str,
    /// Configuration of the stderr log.
    pub stderr_log: StderrLogConfig,
    /// Optional configuration of the [`opentelemetry`] library.
    pub opentelemetry: Option<OpenTelemetryConfig>,
    /// Optional configuration for the [Tokio console] integration.
    ///
    /// [Tokio console]: https://github.com/tokio-rs/console
    #[cfg_attr(nightly_doc_features, doc(cfg(feature = "tokio-console")))]
    #[cfg(feature = "tokio-console")]
    pub tokio_console: Option<TokioConsoleConfig>,
    /// Optional configuration for capturing spans during tests.
    #[cfg(feature = "capture")]
    #[derivative(Debug = "ignore")]
    pub capture: Option<SharedStorage>,
    /// Optional Sentry configuration.
    pub sentry: Option<SentryConfig<F>>,
    /// The version of this build of the service.
    pub build_version: &'static str,
    /// The commit SHA of this build of the service.
    pub build_sha: &'static str,
    /// Registry for prometheus metrics.
    pub registry: MetricsRegistry,
}

/// Configures Sentry reporting.
#[derive(Debug, Clone)]
pub struct SentryConfig<F> {
    /// Sentry data source name to submit events to.
    pub dsn: String,
    /// The environment name to report to Sentry.
    ///
    /// If unset, the Sentry SDK will attempt to read the value from the
    /// `SENTRY_ENVIRONMENT` environment variable.
    pub environment: Option<String>,
    /// Additional tags to include on each Sentry event/exception.
    pub tags: BTreeMap<String, String>,
    /// A filter that classifies events before sending them to Sentry.
    pub event_filter: F,
}

/// Configures the stderr log.
#[derive(Debug)]
pub struct StderrLogConfig {
    /// The format in which to emit messages.
    pub format: StderrLogFormat,
    /// A filter which determines which events are emitted to the log.
    pub filter: EnvFilter,
}

/// Specifies the format of a stderr log message.
#[derive(Debug, Clone)]
pub enum StderrLogFormat {
    /// Format as human readable, optionally colored text.
    ///
    /// Best suited for direct human consumption in a terminal.
    Text {
        /// An optional prefix for each log message.
        prefix: Option<String>,
    },
    /// Format as JSON (in reality, JSONL).
    ///
    /// Best suited for ingestion in structured logging aggregators.
    Json,
}

/// Configuration for the [`opentelemetry`] library.
#[derive(Debug)]
pub struct OpenTelemetryConfig {
    /// The [OTLP/HTTP] endpoint to export OpenTelemetry data to.
    ///
    /// [OTLP/HTTP]: https://github.com/open-telemetry/opentelemetry-specification/blob/b13c1648bae16323868a5caf614bc10c917cc6ca/specification/protocol/otlp.md#otlphttp
    pub endpoint: String,
    /// Additional headers to send with every request to the endpoint.
    pub headers: HeaderMap,
    /// A filter which determines which events are exported.
    pub filter: EnvFilter,
    /// How many spans can be queued before dropping.
    pub max_batch_queue_size: usize,
    /// How many spans to process in a single batch
    pub max_export_batch_size: usize,
    /// How many concurrent export tasks to allow.
    /// More tasks can lead to more memory consumed by the exporter.
    pub max_concurrent_exports: usize,
    /// Delay between consecutive batch exports.
    pub batch_scheduled_delay: Duration,
    /// How long to wait for a batch to be sent before dropping it.
    pub max_export_timeout: Duration,
    /// `opentelemetry::sdk::resource::Resource` to include with all
    /// traces.
    pub resource: Resource,
}

/// Configuration of the [Tokio console] integration.
///
/// [Tokio console]: https://github.com/tokio-rs/console
#[cfg_attr(nightly_doc_features, doc(cfg(feature = "tokio-console")))]
#[cfg(feature = "tokio-console")]
#[derive(Debug, Clone)]
pub struct TokioConsoleConfig {
    /// The address on which to listen for Tokio console connections.
    ///
    /// See [`console_subscriber::Builder::server_addr`].
    pub listen_addr: SocketAddr,
    /// How frequently to publish updates to clients.
    ///
    /// See [`console_subscriber::Builder::publish_interval`].
    pub publish_interval: Duration,
    /// How long data is retained for completed tasks.
    ///
    /// See [`console_subscriber::Builder::retention`].
    pub retention: Duration,
}

type Reloader = Arc<dyn Fn(EnvFilter, Vec<Directive>) -> Result<(), anyhow::Error> + Send + Sync>;
type DirectiveReloader = Arc<dyn Fn(Vec<Directive>) -> Result<(), anyhow::Error> + Send + Sync>;

/// A handle to the tracing infrastructure configured with [`configure`].
#[derive(Clone)]
pub struct TracingHandle {
    stderr_log: Reloader,
    opentelemetry: Reloader,
    sentry: DirectiveReloader,
}

impl TracingHandle {
    /// Creates a inoperative tracing handle.
    ///
    /// Primarily useful in tests.
    pub fn disabled() -> TracingHandle {
        TracingHandle {
            stderr_log: Arc::new(|_, _| Ok(())),
            opentelemetry: Arc::new(|_, _| Ok(())),
            sentry: Arc::new(|_| Ok(())),
        }
    }

    /// Dynamically reloads the stderr log filter.
    pub fn reload_stderr_log_filter(
        &self,
        filter: EnvFilter,
        defaults: Vec<Directive>,
    ) -> Result<(), anyhow::Error> {
        (self.stderr_log)(filter, defaults)
    }

    /// Dynamically reloads the OpenTelemetry log filter.
    pub fn reload_opentelemetry_filter(
        &self,
        filter: EnvFilter,
        defaults: Vec<Directive>,
    ) -> Result<(), anyhow::Error> {
        (self.opentelemetry)(filter, defaults)
    }

    /// Dynamically reloads the additional sentry directives.
    pub fn reload_sentry_directives(&self, defaults: Vec<Directive>) -> Result<(), anyhow::Error> {
        (self.sentry)(defaults)
    }
}

impl std::fmt::Debug for TracingHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        f.debug_struct("TracingHandle").finish_non_exhaustive()
    }
}

// Note that the following defaults are used on startup, regardless of the
// parameters in LaunchDarkly. If we need to, we can add cli flags to control
// then going forward.

/// By default we turn off tracing from the following crates, because they
/// have error spans which are noisy.
///
/// Note: folks should feel free to add more crates here if we find more
/// with long lived Spans.
pub const LOGGING_DEFAULTS_STR: [&str; 2] = [
    "kube_client::client::builder=off",
    // aws_config is very noisy at the INFO level by default.
    // It logs every time it successfully loads credentials.
    "aws_config::profile::credentials=off",
];
/// Same as [`LOGGING_DEFAULTS_STR`], but structured as [`Directive`]s.
pub static LOGGING_DEFAULTS: LazyLock<Vec<Directive>> = LazyLock::new(|| {
    LOGGING_DEFAULTS_STR
        .into_iter()
        .map(|directive| Directive::from_str(directive).expect("valid directive"))
        .collect()
});
/// By default we turn off tracing from the following crates, because they
/// have long-lived Spans, which OpenTelemetry does not handle well.
///
/// Note: folks should feel free to add more crates here if we find more
/// with long lived Spans.
pub const OPENTELEMETRY_DEFAULTS_STR: [&str; 2] = ["h2=off", "hyper=off"];
/// Same as [`OPENTELEMETRY_DEFAULTS_STR`], but structured as [`Directive`]s.
pub static OPENTELEMETRY_DEFAULTS: LazyLock<Vec<Directive>> = LazyLock::new(|| {
    OPENTELEMETRY_DEFAULTS_STR
        .into_iter()
        .map(|directive| Directive::from_str(directive).expect("valid directive"))
        .collect()
});

/// By default we turn off tracing from the following crates, because they
/// have error spans which are noisy.
pub const SENTRY_DEFAULTS_STR: [&str; 2] =
    ["kube_client::client::builder=off", "mysql_async::conn=off"];
/// Same as [`SENTRY_DEFAULTS_STR`], but structured as [`Directive`]s.
pub static SENTRY_DEFAULTS: LazyLock<Vec<Directive>> = LazyLock::new(|| {
    SENTRY_DEFAULTS_STR
        .into_iter()
        .map(|directive| Directive::from_str(directive).expect("valid directive"))
        .collect()
});

/// The [`GLOBAL_SUBSCRIBER`] type.
type GlobalSubscriber = Arc<dyn Subscriber + Send + Sync + 'static>;

/// An [`Arc`] of the tracing [`Subscriber`] constructed and initialized in
/// [`configure`]. The value is written when [`configure`] runs.
pub static GLOBAL_SUBSCRIBER: OnceLock<GlobalSubscriber> = OnceLock::new();

/// Enables application tracing via the [`tracing`] and [`opentelemetry`]
/// libraries.
///
/// The `tracing` library is configured to emit events as textual log lines to
/// stderr. [`StderrLogConfig`] offer a small degree of control over this
/// behavior.
///
/// If the `opentelemetry` parameter is `Some`, the `tracing` library is
/// additionally configured to export events to an observability backend, like
/// [Jaeger] or [Honeycomb].
///
/// The `tokio_console` parameter enables integration with the [Tokio console].
/// When enabled, `tracing` events are collected and made available to the Tokio
/// console via a server running on port
///
/// [Jaeger]: https://jaegertracing.io
/// [Honeycomb]: https://www.honeycomb.io
/// [Tokio console]: https://github.com/tokio-rs/console
// Setting up OpenTelemetry in the background requires we are in a Tokio runtime
// context, hence the `async`.
#[allow(clippy::unused_async)]
pub async fn configure<F>(config: TracingConfig<F>) -> Result<TracingHandle, anyhow::Error>
where
    F: Fn(&tracing::Metadata<'_>) -> sentry_tracing::EventFilter + Send + Sync + 'static,
{
    let stderr_log_layer: Box<dyn Layer<Registry> + Send + Sync> = match config.stderr_log.format {
        StderrLogFormat::Text { prefix } => {
            // See: https://no-color.org/
            let no_color = std::env::var_os("NO_COLOR").unwrap_or_else(|| "".into()) != "";
            Box::new(
                fmt::layer()
                    .with_writer(io::stderr)
                    .event_format(PrefixFormat {
                        inner: format(),
                        prefix,
                    })
                    .with_ansi(!no_color && io::stderr().is_terminal()),
            )
        }
        StderrLogFormat::Json => Box::new(
            fmt::layer()
                .with_writer(io::stderr)
                .json()
                .with_current_span(true),
        ),
    };
    let (stderr_log_filter, stderr_log_filter_reloader) = reload::Layer::new({
        let mut filter = config.stderr_log.filter;
        for directive in LOGGING_DEFAULTS.iter() {
            filter = filter.add_directive(directive.clone());
        }
        filter
    });
    // Add rate limiting for OpenTelemetry internal logs to prevent log spam
    // when there are issues with the OpenTelemetry pipeline (e.g., channel full,
    // connection errors). This only affects logs from "opentelemetry*" targets.
    let otel_rate_limit_filter = OpenTelemetryRateLimitingFilter::new(Duration::from_secs(
        OPENTELEMETRY_RATE_LIMIT_BACKOFF_SECS,
    ));
    let stderr_log_layer = stderr_log_layer
        .with_filter(stderr_log_filter)
        .with_filter(otel_rate_limit_filter);
    let stderr_log_reloader = Arc::new(move |mut filter: EnvFilter, defaults: Vec<Directive>| {
        for directive in &defaults {
            filter = filter.add_directive(directive.clone());
        }
        Ok(stderr_log_filter_reloader.reload(filter)?)
    });

    let (otel_layer, otel_reloader): (_, Reloader) = if let Some(otel_config) = config.opentelemetry
    {
        opentelemetry::global::set_text_map_propagator(TraceContextPropagator::new());

        // Manually set up an OpenSSL-backed, h2, proxied `Channel`,
        // with the timeout configured according to:
        // https://docs.rs/opentelemetry-otlp/latest/opentelemetry_otlp/struct.TonicExporterBuilder.html#method.with_channel
        let channel = Endpoint::from_shared(otel_config.endpoint)?
            .timeout(opentelemetry_otlp::OTEL_EXPORTER_OTLP_TIMEOUT_DEFAULT)
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
        let exporter = opentelemetry_otlp::SpanExporter::builder()
            .with_tonic()
            .with_channel(channel)
            .with_metadata(MetadataMap::from_headers(otel_config.headers))
            .build()?;
        let batch_config = opentelemetry_sdk::trace::BatchConfigBuilder::default()
            .with_max_queue_size(otel_config.max_batch_queue_size)
            .with_max_export_batch_size(otel_config.max_export_batch_size)
            .with_max_concurrent_exports(otel_config.max_concurrent_exports)
            .with_scheduled_delay(otel_config.batch_scheduled_delay)
            .with_max_export_timeout(otel_config.max_export_timeout)
            .build();
        let batch_span_processor = BatchSpanProcessor::builder(exporter, Tokio)
            .with_batch_config(batch_config)
            .build();
        let tracer = trace::SdkTracerProvider::builder()
            .with_resource(
                Resource::builder()
                    .with_service_name(config.service_name.to_string())
                    .with_attributes(
                        otel_config
                            .resource
                            .iter()
                            .map(|(k, v)| KeyValue::new(k.clone(), v.clone())),
                        // TODO handle schema url?
                    )
                    .build(),
            )
            .with_span_processor(batch_span_processor)
            .with_max_events_per_span(2048)
            .build()
            .tracer(config.service_name);

        let (filter, filter_handle) = reload::Layer::new({
            let mut filter = otel_config.filter;
            for directive in OPENTELEMETRY_DEFAULTS.iter() {
                filter = filter.add_directive(directive.clone());
            }
            filter
        });
        let metrics_layer = MetricsLayer::new(&config.registry);
        let layer = tracing_opentelemetry::layer()
            // OpenTelemetry does not handle long-lived Spans well, and they end up continuously
            // eating memory until OOM. So we set a max number of events that are allowed to be
            // logged to a Span, once this max is passed, old events will get dropped
            //
            // TODO(parker-timmerman|guswynn): make this configurable with LaunchDarkly
            .with_tracer(tracer)
            .and_then(metrics_layer)
            // WARNING, ENTERING SPOOKY ZONE 2.0
            //
            // Notice we use `with_filter` here. `and_then` will apply the filter globally.
            .with_filter(filter);
        let reloader = Arc::new(move |mut filter: EnvFilter, defaults: Vec<Directive>| {
            // Re-apply our defaults on reload.
            for directive in &defaults {
                filter = filter.add_directive(directive.clone());
            }
            Ok(filter_handle.reload(filter)?)
        });
        (Some(layer), reloader)
    } else {
        let reloader = Arc::new(|_, _| Ok(()));
        (None, reloader)
    };

    #[cfg(feature = "tokio-console")]
    let tokio_console_layer = if let Some(console_config) = config.tokio_console.clone() {
        let builder = ConsoleLayer::builder()
            .publish_interval(console_config.publish_interval)
            .retention(console_config.retention);
        let builder = match console_config.listen_addr {
            SocketAddr::Inet(addr) => builder.server_addr(addr),
            SocketAddr::Unix(addr) => {
                let path = addr.as_pathname().unwrap().as_ref();
                builder.server_addr(path)
            }
            SocketAddr::Turmoil(_) => unimplemented!(),
        };
        Some(builder.spawn())
    } else {
        None
    };

    let (sentry_layer, sentry_reloader): (_, DirectiveReloader) =
        if let Some(sentry_config) = config.sentry {
            let guard = sentry::init((
                sentry_config.dsn,
                sentry::ClientOptions {
                    attach_stacktrace: true,
                    release: Some(format!("materialize@{0}", config.build_version).into()),
                    environment: sentry_config.environment.map(Into::into),
                    ..Default::default()
                },
            ));

            // Forgetting the guard ensures that the Sentry transport won't shut down for the
            // lifetime of the process.
            std::mem::forget(guard);

            sentry::configure_scope(|scope| {
                scope.set_tag("service_name", config.service_name);
                scope.set_tag("build_sha", config.build_sha.to_string());
                for (k, v) in sentry_config.tags {
                    scope.set_tag(&k, v);
                }
            });

            let (filter, filter_handle) = reload::Layer::new({
                // Please see the comment on `with_filter` below.
                let mut filter = EnvFilter::new("info");
                for directive in SENTRY_DEFAULTS.iter() {
                    filter = filter.add_directive(directive.clone());
                }
                filter
            });
            let layer = sentry_tracing::layer()
                .event_filter(sentry_config.event_filter)
                // WARNING, ENTERING THE SPOOKY ZONE
                //
                // While sentry provides an event filter above that maps events to types of sentry events, its `Layer`
                // implementation does not participate in `tracing`'s level-fast-path implementation, which depends on
                // a hidden api (<https://github.com/tokio-rs/tracing/blob/b28c9351dd4f34ed3c7d5df88bb5c2e694d9c951/tracing-subscriber/src/layer/mod.rs#L861-L867>)
                // which is primarily manged by filters (like below). The fast path skips verbose log
                // (and span) levels that no layer is interested by reading a single atomic. Usually, not implementing this
                // api means "give me everything, including `trace`, unless you attach a filter to me.
                //
                // The curious thing here (and a bug in tracing) is that _some configurations of our layer stack above_,
                // if you don't have this filter can cause the fast-path to trigger, despite the fact
                // that the sentry layer would specifically communicating that it wants to see
                // everything. This bug appears to be related to the presence of a `reload::Layer`
                // _around a filter, not a layer_, and guswynn is tracking investigating it here:
                // <https://github.com/MaterializeInc/database-issues/issues/4794>. Because we don't
                // enable a reload-able filter in CI/locally, but DO in production (the otel layer), it
                // was once possible to trigger and rely on the fast path in CI, but not notice that it
                // was disabled in production.
                //
                // The behavior of this optimization is now tested in various scenarios (in
                // `test/tracing`). Regardless, when the upstream bug is fixed/resolved,
                // we will continue to place this here, as the sentry layer only cares about
                // events <= INFO, so we want to use the fast-path if no other layer
                // is interested in high-fidelity events.
                .with_filter(filter);
            let reloader = Arc::new(move |defaults: Vec<Directive>| {
                // Please see the comment on `with_filter` above.
                let mut filter = EnvFilter::new("info");
                // Re-apply our defaults on reload.
                for directive in &defaults {
                    filter = filter.add_directive(directive.clone());
                }
                Ok(filter_handle.reload(filter)?)
            });
            (Some(layer), reloader)
        } else {
            let reloader = Arc::new(|_| Ok(()));
            (None, reloader)
        };

    #[cfg(feature = "capture")]
    let capture = config.capture.map(|storage| CaptureLayer::new(&storage));

    let stack = tracing_subscriber::registry();
    let stack = stack.with(stderr_log_layer);
    #[cfg(feature = "capture")]
    let stack = stack.with(capture);
    let stack = stack.with(otel_layer);
    #[cfg(feature = "tokio-console")]
    let stack = stack.with(tokio_console_layer);
    let stack = stack.with(sentry_layer);

    // Set the stack as a global subscriber.
    assert!(GLOBAL_SUBSCRIBER.set(Arc::new(stack)).is_ok());
    // Initialize the subscriber.
    Arc::clone(GLOBAL_SUBSCRIBER.get().unwrap()).init();

    #[cfg(feature = "tokio-console")]
    if let Some(console_config) = config.tokio_console {
        let endpoint = match console_config.listen_addr {
            SocketAddr::Inet(addr) => format!("http://{addr}"),
            SocketAddr::Unix(addr) => format!("file://localhost{addr}"),
            SocketAddr::Turmoil(_) => unimplemented!(),
        };
        tracing::info!("starting tokio console on {endpoint}");
    }

    let handle = TracingHandle {
        stderr_log: stderr_log_reloader,
        opentelemetry: otel_reloader,
        sentry: sentry_reloader,
    };

    Ok(handle)
}

/// Returns the [`Level`] of a crate from an [`EnvFilter`] by performing an
/// exact match between `crate` and the original `EnvFilter` directive.
pub fn crate_level(filter: &EnvFilter, crate_name: &'static str) -> Level {
    // TODO: implement `would_enable` on `EnvFilter` or equivalent
    // to avoid having to manually parse out the directives. This
    // would also significantly broaden the lookups the fn is able
    // to do (modules, spans, fields, etc).

    let mut default_level = Level::ERROR;
    // EnvFilter roundtrips through its Display fmt, so it
    // is safe to split out its individual directives here
    for directive in format!("{}", filter).split(',') {
        match directive.split('=').collect::<Vec<_>>().as_slice() {
            [target, level] => {
                if *target == crate_name {
                    match Level::from_str(*level) {
                        Ok(level) => return level,
                        Err(err) => warn!("invalid level for {}: {}", target, err),
                    }
                }
            }
            [token] => match Level::from_str(*token) {
                Ok(level) => default_level = default_level.max(level),
                Err(_) => {
                    // a target without a level is interpreted as trace
                    if *token == crate_name {
                        default_level = default_level.max(Level::TRACE);
                    }
                }
            },
            _ => {}
        }
    }

    default_level
}

/// A wrapper around a [`FormatEvent`] that adds an optional prefix to each
/// event.
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
                let mut prefix = yansi::Paint::new(prefix);
                if writer.has_ansi_escapes() {
                    prefix = prefix.bold();
                }
                write!(writer, "{}: ", prefix)?;
                self.inner.format_event(ctx, writer, event)?;
            }
        }
        Ok(())
    }
}

/// An OpenTelemetry context.
///
/// Allows associating [`tracing`] spans across task or thread boundaries.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct OpenTelemetryContext {
    inner: BTreeMap<String, String>,
}

impl OpenTelemetryContext {
    /// Attaches this `Context` to the current [`tracing`] span,
    /// as its parent.
    ///
    /// If there is not enough information in this `OpenTelemetryContext`
    /// to create a context, then the current thread's `Context` is used
    /// defaulting to the default `Context`.
    pub fn attach_as_parent(&self) {
        self.attach_as_parent_to(&tracing::Span::current())
    }

    /// Attaches this `Context` to the given [`tracing`] Span, as its parent.
    ///
    /// If there is not enough information in this `OpenTelemetryContext`
    /// to create a context, then the current thread's `Context` is used
    /// defaulting to the default `Context`.
    pub fn attach_as_parent_to(&self, span: &Span) {
        let parent_cx = global::get_text_map_propagator(|prop| prop.extract(self));
        let _ = span.set_parent(parent_cx);
    }

    /// Obtains a `Context` from the current [`tracing`] span.
    pub fn obtain() -> Self {
        let mut context = Self::empty();
        global::get_text_map_propagator(|propagator| {
            propagator.inject_context(&tracing::Span::current().context(), &mut context)
        });

        context
    }

    /// Obtains an empty `Context`.
    pub fn empty() -> Self {
        Self {
            inner: BTreeMap::new(),
        }
    }
}

impl Extractor for OpenTelemetryContext {
    fn get(&self, key: &str) -> Option<&str> {
        self.inner.get(&key.to_lowercase()).map(|v| v.as_str())
    }

    fn keys(&self) -> Vec<&str> {
        self.inner.keys().map(|k| k.as_str()).collect::<Vec<_>>()
    }
}

impl Injector for OpenTelemetryContext {
    fn set(&mut self, key: &str, value: String) {
        self.inner.insert(key.to_lowercase(), value);
    }
}

impl From<OpenTelemetryContext> for BTreeMap<String, String> {
    fn from(ctx: OpenTelemetryContext) -> Self {
        ctx.inner
    }
}

impl From<BTreeMap<String, String>> for OpenTelemetryContext {
    fn from(map: BTreeMap<String, String>) -> Self {
        Self { inner: map }
    }
}

struct MetricsLayer {
    on_close: IntCounter,
}

impl MetricsLayer {
    fn new(registry: &MetricsRegistry) -> Self {
        MetricsLayer {
            on_close: registry.register(metric!(
                name: "mz_otel_on_close",
                help: "count of on_close events sent to otel",
            )),
        }
    }
}

impl<S: tracing::Subscriber> Layer<S> for MetricsLayer {
    fn on_close(&self, _id: tracing::span::Id, _ctx: tracing_subscriber::layer::Context<'_, S>) {
        self.on_close.inc()
    }
}

/// The target prefix for OpenTelemetry internal logs.
/// OpenTelemetry crates emit logs with targets like "opentelemetry", "opentelemetry_sdk",
/// "opentelemetry_otlp", etc.
const OPENTELEMETRY_TARGET_PREFIX: &str = "opentelemetry";

/// Default backoff duration for rate-limiting OpenTelemetry internal logs.
const OPENTELEMETRY_RATE_LIMIT_BACKOFF_SECS: u64 = 30;

/// A rate-limiting filter that throttles OpenTelemetry internal log messages.
///
/// OpenTelemetry can emit a large number of duplicate error messages (e.g., when
/// the batch processor channel is full or when there are connection issues).
/// This filter rate-limits these messages to avoid log spam while still ensuring
/// the errors are visible.
///
/// Only logs from OpenTelemetry targets (those starting with "opentelemetry") are
/// rate-limited. All other logs pass through unchanged.
#[derive(Debug)]
pub struct OpenTelemetryRateLimitingFilter {
    /// How long to suppress duplicate messages from OpenTelemetry.
    backoff_duration: Duration,
    /// Tracks the last time each unique message key was logged.
    /// The key is a hash of (target, message format string).
    last_logged: Mutex<BTreeMap<u64, EpochMillis>>,
    /// Counter for the number of suppressed messages.
    suppressed_count: AtomicU64,
    /// Function to get the current time (helps with testing).
    now_fn: NowFn,
}

impl OpenTelemetryRateLimitingFilter {
    /// Creates a new rate-limiting filter with the specified backoff duration.
    ///
    /// Messages from OpenTelemetry targets will be suppressed if they occur
    /// more frequently than once per `backoff_duration`.
    pub fn new(backoff_duration: Duration) -> Self {
        Self {
            backoff_duration,
            last_logged: Mutex::new(BTreeMap::new()),
            suppressed_count: AtomicU64::new(0),
            now_fn: SYSTEM_TIME.clone(),
        }
    }

    /// Sets a custom time function for testing purposes.
    #[cfg(test)]
    fn with_now_fn(mut self, now_fn: NowFn) -> Self {
        self.now_fn = now_fn;
        self
    }

    /// Computes a hash key for the given event metadata.
    fn compute_key(metadata: &tracing::Metadata<'_>) -> u64 {
        let mut hasher = std::hash::DefaultHasher::new();
        metadata.target().hash(&mut hasher);
        metadata.name().hash(&mut hasher);
        // Include the file and line if available for more precise deduplication
        if let Some(file) = metadata.file() {
            file.hash(&mut hasher);
        }
        if let Some(line) = metadata.line() {
            line.hash(&mut hasher);
        }
        hasher.finish()
    }

    /// Returns true if the event should be logged (not rate-limited).
    fn should_log(&self, metadata: &tracing::Metadata<'_>) -> bool {
        // Only rate-limit OpenTelemetry internal logs
        if !metadata.target().starts_with(OPENTELEMETRY_TARGET_PREFIX) {
            return true;
        }

        let key = Self::compute_key(metadata);
        let now = (self.now_fn)();

        let mut last_logged = self.last_logged.lock().unwrap();

        if let Some(last_time) = last_logged.get(&key) {
            if Duration::from_millis(now - last_time) < self.backoff_duration {
                self.suppressed_count.fetch_add(1, Ordering::Relaxed);
                return false;
            }
        }

        last_logged.insert(key, now);

        // Periodically clean up old entries to prevent unbounded growth
        // Keep entries that are still within the backoff window
        if last_logged.len() > 1000 {
            last_logged
                .retain(|_, time| Duration::from_millis(now - *time) < self.backoff_duration);
        }

        true
    }

    /// Returns the number of messages that have been suppressed.
    pub fn suppressed_count(&self) -> u64 {
        self.suppressed_count.load(Ordering::Relaxed)
    }
}

impl<S> tracing_subscriber::layer::Filter<S> for OpenTelemetryRateLimitingFilter
where
    S: tracing::Subscriber + for<'lookup> LookupSpan<'lookup>,
{
    fn enabled(
        &self,
        metadata: &tracing::Metadata<'_>,
        _ctx: &tracing_subscriber::layer::Context<'_, S>,
    ) -> bool {
        self.should_log(metadata)
    }

    fn event_enabled(
        &self,
        event: &Event<'_>,
        _ctx: &tracing_subscriber::layer::Context<'_, S>,
    ) -> bool {
        self.should_log(event.metadata())
    }
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;
    use tracing::Level;
    use tracing_subscriber::filter::{EnvFilter, LevelFilter, Targets};

    #[crate::test]
    fn overriding_targets() {
        let user_defined = Targets::new().with_target("my_crate", Level::INFO);

        let default = Targets::new().with_target("my_crate", LevelFilter::OFF);
        assert!(!default.would_enable("my_crate", &Level::INFO));

        // The user_defined filters should override the default
        let filters = Targets::new()
            .with_targets(default)
            .with_targets(user_defined);
        assert!(filters.would_enable("my_crate", &Level::INFO));
    }

    #[crate::test]
    fn crate_level() {
        // target=level directives only. should default to ERROR if unspecified
        let filter = EnvFilter::from_str("abc=trace,def=debug").expect("valid");
        assert_eq!(super::crate_level(&filter, "abc"), Level::TRACE);
        assert_eq!(super::crate_level(&filter, "def"), Level::DEBUG);
        assert_eq!(super::crate_level(&filter, "def"), Level::DEBUG);
        assert_eq!(
            super::crate_level(&filter, "abc::doesnt::exist"),
            Level::ERROR
        );
        assert_eq!(super::crate_level(&filter, "doesnt::exist"), Level::ERROR);

        // add in a global default
        let filter = EnvFilter::from_str("abc=trace,def=debug,info").expect("valid");
        assert_eq!(super::crate_level(&filter, "abc"), Level::TRACE);
        assert_eq!(
            super::crate_level(&filter, "abc::doesnt:exist"),
            Level::INFO
        );
        assert_eq!(super::crate_level(&filter, "def"), Level::DEBUG);
        assert_eq!(super::crate_level(&filter, "nan"), Level::INFO);

        // a directive with mod path doesn't match the top-level crate
        let filter = EnvFilter::from_str("abc::def::ghi=trace,debug").expect("valid");
        assert_eq!(super::crate_level(&filter, "abc"), Level::DEBUG);
        assert_eq!(super::crate_level(&filter, "def"), Level::DEBUG);
        assert_eq!(
            super::crate_level(&filter, "gets_the_default"),
            Level::DEBUG
        );

        // directives with spans and fields don't match the top-level crate
        let filter =
            EnvFilter::from_str("abc[s]=trace,def[s{g=h}]=debug,[{s2}]=debug,info").expect("valid");
        assert_eq!(super::crate_level(&filter, "abc"), Level::INFO);
        assert_eq!(super::crate_level(&filter, "def"), Level::INFO);
        assert_eq!(super::crate_level(&filter, "gets_the_default"), Level::INFO);

        // a bare target without a level is taken as trace
        let filter = EnvFilter::from_str("abc,info").expect("valid");
        assert_eq!(super::crate_level(&filter, "abc"), Level::TRACE);
        assert_eq!(super::crate_level(&filter, "gets_the_default"), Level::INFO);
        // the contract of `crate_level` is that it only matches top-level crates.
        // if we had a proper EnvFilter::would_match impl, this assertion should
        // be Level::TRACE
        assert_eq!(super::crate_level(&filter, "abc::def"), Level::INFO);
    }

    #[crate::test]
    fn otel_rate_limiting_filter_backoff() {
        use std::sync::Arc;
        use std::sync::atomic::{AtomicU64, Ordering};
        use std::time::Duration;
        use tracing::Callsite;

        use crate::now::NowFn;

        // Create a controllable time source
        let current_time = Arc::new(AtomicU64::new(0));
        let time_for_closure = Arc::clone(&current_time);
        let now_fn: NowFn = NowFn::from(move || time_for_closure.load(Ordering::SeqCst));

        let filter = super::OpenTelemetryRateLimitingFilter::new(Duration::from_millis(100))
            .with_now_fn(now_fn);

        // Test that key computation is stable
        static OTEL_CALLSITE: tracing::callsite::DefaultCallsite =
            tracing::callsite::DefaultCallsite::new(&tracing::Metadata::new(
                "test_event",
                "opentelemetry_sdk::trace",
                Level::WARN,
                Some(file!()),
                Some(line!()),
                Some(module_path!()),
                tracing::field::FieldSet::new(&[], tracing::callsite::Identifier(&OTEL_CALLSITE)),
                tracing::metadata::Kind::EVENT,
            ));
        let otel_meta = OTEL_CALLSITE.metadata();

        static OTHER_CALLSITE: tracing::callsite::DefaultCallsite =
            tracing::callsite::DefaultCallsite::new(&tracing::Metadata::new(
                "test_event",
                "my_app::module",
                Level::WARN,
                Some(file!()),
                Some(line!()),
                Some(module_path!()),
                tracing::field::FieldSet::new(&[], tracing::callsite::Identifier(&OTHER_CALLSITE)),
                tracing::metadata::Kind::EVENT,
            ));
        let other_meta = OTHER_CALLSITE.metadata();

        // Non-OpenTelemetry events should always pass through
        assert!(filter.should_log(other_meta));
        assert!(filter.should_log(other_meta));
        assert!(filter.should_log(other_meta));
        assert_eq!(filter.suppressed_count(), 0);

        // First OpenTelemetry event should pass through
        assert!(filter.should_log(otel_meta));
        assert_eq!(filter.suppressed_count(), 0);

        // Subsequent OpenTelemetry events within backoff should be suppressed
        current_time.store(50, Ordering::SeqCst); // 50ms later, still within 100ms backoff
        assert!(!filter.should_log(otel_meta));
        assert!(!filter.should_log(otel_meta));
        assert_eq!(filter.suppressed_count(), 2);

        // After backoff, event should pass through again
        current_time.store(150, Ordering::SeqCst); // 150ms later, past 100ms backoff
        assert!(filter.should_log(otel_meta));
        assert_eq!(filter.suppressed_count(), 2);
    }
}

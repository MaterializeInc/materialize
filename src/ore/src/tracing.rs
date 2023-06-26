// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

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
use std::io;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

use anyhow::bail;
#[cfg(feature = "tokio-console")]
use console_subscriber::ConsoleLayer;
use http::HeaderMap;
use hyper::client::HttpConnector;
use hyper_tls::HttpsConnector;
use opentelemetry::propagation::{Extractor, Injector};
use opentelemetry::sdk::propagation::TraceContextPropagator;
use opentelemetry::sdk::{trace, Resource};
use opentelemetry::{global, KeyValue};
use sentry::integrations::debug_images::DebugImagesIntegration;
use tonic::metadata::MetadataMap;
use tonic::transport::Endpoint;
use tracing::{warn, Event, Level, Subscriber};
use tracing_opentelemetry::OpenTelemetrySpanExt;
use tracing_subscriber::filter::Directive;
use tracing_subscriber::fmt::format::{format, Writer};
use tracing_subscriber::fmt::{self, FmtContext, FormatEvent, FormatFields};
use tracing_subscriber::layer::{Layer, SubscriberExt};
use tracing_subscriber::registry::LookupSpan;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::{reload, EnvFilter, Registry};

#[cfg(feature = "tokio-console")]
use crate::netio::SocketAddr;

/// Application tracing configuration.
///
/// See the [`configure`] function for details.
#[derive(Debug)]
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
    /// Optional Sentry configuration.
    pub sentry: Option<SentryConfig<F>>,
    /// The version of this build of the service.
    pub build_version: &'static str,
    /// The commit SHA of this build of the service.
    pub build_sha: &'static str,
    /// The time of this build of the service.
    pub build_time: &'static str,
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
    /// `opentelemetry::sdk::resource::Resource` to include with all
    /// traces.
    pub resource: Resource,
    /// Whether to startup with the dynamic OpenTelemetry layer enabled
    pub start_enabled: bool,
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

type Reloader = Arc<dyn Fn(EnvFilter) -> Result<(), anyhow::Error> + Send + Sync>;

/// A handle to the tracing infrastructure configured with [`configure`].
#[derive(Clone)]
pub struct TracingHandle {
    stderr_log: Reloader,
    opentelemetry: Reloader,
}

impl TracingHandle {
    /// Creates a inoperative tracing handle.
    ///
    /// Primarily useful in tests.
    pub fn disabled() -> TracingHandle {
        TracingHandle {
            stderr_log: Arc::new(|_| Ok(())),
            opentelemetry: Arc::new(|_| Ok(())),
        }
    }

    /// Dynamically reloads the stderr log filter.
    pub fn reload_stderr_log_filter(&self, filter: EnvFilter) -> Result<(), anyhow::Error> {
        (self.stderr_log)(filter)
    }

    /// Dynamically reloads the OpenTelemetry log filter.
    pub fn reload_opentelemetry_filter(&self, filter: EnvFilter) -> Result<(), anyhow::Error> {
        (self.opentelemetry)(filter)
    }
}

impl std::fmt::Debug for TracingHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        f.debug_struct("TracingHandle").finish_non_exhaustive()
    }
}

/// A guard for the tracing infrastructure configured with [`configure`].
///
/// This guard should be kept alive for the lifetime of the program.
#[must_use = "Must hold for the lifetime of the program, otherwise tracing will be shutdown"]
pub struct TracingGuard {
    _sentry_guard: Option<sentry::ClientInitGuard>,
}

impl Drop for TracingGuard {
    fn drop(&mut self) {
        opentelemetry::global::shutdown_tracer_provider();
    }
}

impl std::fmt::Debug for TracingGuard {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        f.debug_struct("TracingGuard").finish_non_exhaustive()
    }
}

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
pub async fn configure<F>(
    config: TracingConfig<F>,
) -> Result<(TracingHandle, TracingGuard), anyhow::Error>
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
                    .with_ansi(!no_color && atty::is(atty::Stream::Stderr)),
            )
        }
        StderrLogFormat::Json => Box::new(
            fmt::layer()
                .with_writer(io::stderr)
                .json()
                .with_current_span(true),
        ),
    };
    let (stderr_log_filter, stderr_log_filter_reloader) =
        reload::Layer::new(config.stderr_log.filter);
    let stderr_log_layer = stderr_log_layer.with_filter(stderr_log_filter);
    let stderr_log_reloader =
        Arc::new(move |filter| Ok(stderr_log_filter_reloader.reload(filter)?));

    let (otel_layer, otel_reloader): (_, Reloader) = if let Some(otel_config) = config.opentelemetry
    {
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
            .with_trace_config(
                trace::config().with_resource(
                    // The latter resources win, so if the user specifies
                    // `service.name` in the configuration, it will override the
                    // `service.name` value we configure here.
                    Resource::new([KeyValue::new(
                        "service.name",
                        config.service_name.to_string(),
                    )])
                    .merge(&otel_config.resource),
                ),
            )
            .with_exporter(exporter)
            .install_batch(opentelemetry::runtime::Tokio)
            .unwrap();

        // By default we turn off tracing from the following crates, because they
        // have long-lived Spans, which OpenTelemetry does not handle well.
        //
        // Note: folks should feel free to add more crates here if we find more
        // with long lived Spans.
        let default_directives: Vec<Directive> = vec![
            Directive::from_str("h2=off").expect("valid directive"),
            Directive::from_str("hyper=off").expect("valid directive"),
        ];

        let (filter, filter_handle) = reload::Layer::new(if otel_config.start_enabled {
            let mut filter = otel_config.filter;
            for directive in &default_directives {
                filter = filter.add_directive(directive.clone());
            }
            filter
        } else {
            // The default `EnvFilter` has everything disabled.
            EnvFilter::default()
        });
        let layer = tracing_opentelemetry::layer()
            // OpenTelemetry does not handle long-lived Spans well, and they end up continuously
            // eating memory until OOM. So we set a max number of events that are allowed to be
            // logged to a Span, once this max is passed, old events will get dropped
            //
            // TODO(parker-timmerman|guswynn): make this configurable with LaunchDarkly
            .max_events_per_span(2048)
            .with_tracer(tracer)
            .and_then(filter);
        let reloader = Arc::new(move |mut filter: EnvFilter| {
            // Re-apply our defaults on reload.
            for directive in &default_directives {
                filter = filter.add_directive(directive.clone());
            }
            Ok(filter_handle.reload(filter)?)
        });
        (Some(layer), reloader)
    } else {
        let reloader = Arc::new(move |_| bail!("OpenTelemetry is disabled"));
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
        };
        Some(builder.spawn())
    } else {
        None
    };

    let (sentry_guard, sentry_layer) = if let Some(sentry_config) = config.sentry {
        let guard = sentry::init((
            sentry_config.dsn,
            sentry::ClientOptions {
                attach_stacktrace: true,
                release: Some(config.build_version.into()),
                environment: sentry_config.environment.map(Into::into),
                integrations: vec![Arc::new(DebugImagesIntegration::new())],
                ..Default::default()
            },
        ));

        sentry::configure_scope(|scope| {
            scope.set_tag("service_name", config.service_name);
            scope.set_tag("build_sha", config.build_sha.to_string());
            scope.set_tag("build_time", config.build_time.to_string());
            for (k, v) in sentry_config.tags {
                scope.set_tag(&k, v);
            }
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
            // <https://github.com/MaterializeInc/materialize/issues/16556>. Because we don't
            // enable a reload-able filter in CI/locally, but DO in production (the otel layer), it
            // was once possible to trigger and rely on the fast path in CI, but not notice that it
            // was disabled in production.
            //
            // The behavior of this optimization is now tested in various scenarios (in
            // `test/tracing`). Regardless, when the upstream bug is fixed/resolved,
            // we will continue to place this here, as the sentry layer only cares about
            // events <= INFO, so we want to use the fast-path if no other layer
            // is interested in high-fidelity events.
            .with_filter(tracing::level_filters::LevelFilter::INFO);

        (Some(guard), Some(layer))
    } else {
        (None, None)
    };

    let stack = tracing_subscriber::registry();
    let stack = stack.with(stderr_log_layer);
    let stack = stack.with(otel_layer);
    #[cfg(feature = "tokio-console")]
    let stack = stack.with(tokio_console_layer);
    let stack = stack.with(sentry_layer);
    stack.init();

    #[cfg(feature = "tokio-console")]
    if let Some(console_config) = config.tokio_console {
        let endpoint = match console_config.listen_addr {
            SocketAddr::Inet(addr) => format!("http://{addr}"),
            SocketAddr::Unix(addr) => format!("file://localhost{addr}"),
        };
        tracing::info!("starting tokio console on {endpoint}");
    }

    let handle = TracingHandle {
        stderr_log: stderr_log_reloader,
        opentelemetry: otel_reloader,
    };
    let guard = TracingGuard {
        _sentry_guard: sentry_guard,
    };

    Ok((handle, guard))
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
        let parent_cx = global::get_text_map_propagator(|prop| prop.extract(self));
        tracing::Span::current().set_parent(parent_cx);
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

#[cfg(test)]
mod tests {
    use std::str::FromStr;
    use tracing::Level;
    use tracing_subscriber::filter::{EnvFilter, LevelFilter, Targets};

    #[mz_test_macro::test]
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

    #[mz_test_macro::test]
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
}

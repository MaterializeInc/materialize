// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Dyncfgs used by the balancer.

use std::str::FromStr;
use std::time::Duration;

use anyhow::anyhow;
use mz_dyncfg::{Config, ConfigSet, ConfigUpdates};
use mz_tracing::params::TracingParameters;
use mz_tracing::{CloneableEnvFilter, SerializableDirective};
use tracing_subscriber::filter::Directive;

// The defaults here must be set to an appropriate value in case LaunchDarkly is down because we
// continue startup even in that case.
//
// All configuration names should be prefixed with "balancerd_" to avoid name collisions.
/// Duration to wait after listeners closed via SIGTERM for outstanding connections to complete.
pub const SIGTERM_CONNECTION_WAIT: Config<Duration> = Config::new(
    "balancerd_sigterm_connection_wait",
    Duration::from_secs(60 * 9),
    "Duration to wait after listeners closed via SIGTERM for outstanding connections to complete.",
);

/// Duration to wait after SIGTERM to begin shutdown of servers.
pub const SIGTERM_LISTEN_WAIT: Config<Duration> = Config::new(
    "balancerd_sigterm_listen_wait",
    Duration::from_secs(60),
    "Duration to wait after SIGTERM to begin shutdown of servers.",
);

/// Whether to inject tcp proxy protocol headers to downstream http servers.
pub const INJECT_PROXY_PROTOCOL_HEADER_HTTP: Config<bool> = Config::new(
    "balancerd_inject_proxy_protocol_header_http",
    false,
    "Whether to inject tcp proxy protocol headers to downstream http servers.",
);

/// Whether to enable SNI-based tenant resolution for PgWire connections.
pub const PGWIRE_SNI_TENANT_RESOLUTION: Config<bool> = Config::new(
    "balancerd_pgwire_sni_tenant_resolution",
    true,
    "Whether to enable SNI-based tenant resolution for PgWire connections.",
);

/// Whether to enable frontegg-based tenant resolution for PgWire connections.
pub const PGWIRE_FRONTEGG_TENANT_RESOLUTION: Config<bool> = Config::new(
    "balancerd_pgwire_frontegg_tenant_resolution",
    true,
    "Whether to enable frontegg-based tenant resolution for PgWire connections.",
);

/// TTL for DNS tenant resolution cache entries.
pub const TENANT_CACHE_TTL: Config<Duration> = Config::new(
    "balancerd_tenant_cache_ttl",
    Duration::from_secs(60), // 60 seconds - DOS prevention
    "TTL for DNS tenant resolution cache entries.",
);

/// Enable negative caching for tenant resolution.
pub const TENANT_RESOLUTION_NEGATIVE_CACHING: Config<bool> = Config::new(
    "balancerd_tenant_resolution_negative_caching",
    false, // 10 seconds - DOS prevention
    "Whether to cache negative DNS results for tennant/addr lookups.",
);

/// TTL for DNS address resolution cache entries.
pub const ADDR_CACHE_TTL: Config<Duration> = Config::new(
    "balancerd_addr_cache_ttl",
    Duration::from_secs(1), // 1 second for fast rollover - DOS prevention
    "TTL for DNS address resolution cache entries.",
);

/// Sets the filter to apply to stderr logging.
pub const LOGGING_FILTER: Config<&str> = Config::new(
    "balancerd_log_filter",
    "info",
    "Sets the filter to apply to stderr logging.",
);

/// Sets the filter to apply to OpenTelemetry-backed distributed tracing.
pub const OPENTELEMETRY_FILTER: Config<&str> = Config::new(
    "balancerd_opentelemetry_filter",
    "info",
    "Sets the filter to apply to OpenTelemetry-backed distributed tracing.",
);

/// Sets additional default directives to apply to stderr logging.
/// These apply to all variations of `log_filter`. Directives other than
/// `module=off` are likely incorrect. Comma separated list.
pub const LOGGING_FILTER_DEFAULTS: Config<fn() -> String> = Config::new(
    "balancerd_log_filter_defaults",
    || mz_ore::tracing::LOGGING_DEFAULTS_STR.join(","),
    "Sets additional default directives to apply to stderr logging. \
    These apply to all variations of `log_filter`. Directives other than \
    `module=off` are likely incorrect. Comma separated list.",
);

/// Sets additional default directives to apply to OpenTelemetry-backed
/// distributed tracing.
/// These apply to all variations of `opentelemetry_filter`. Directives other than
/// `module=off` are likely incorrect. Comma separated list.
pub const OPENTELEMETRY_FILTER_DEFAULTS: Config<fn() -> String> = Config::new(
    "balancerd_opentelemetry_filter_defaults",
    || mz_ore::tracing::OPENTELEMETRY_DEFAULTS_STR.join(","),
    "Sets additional default directives to apply to OpenTelemetry-backed \
    distributed tracing. \
    These apply to all variations of `opentelemetry_filter`. Directives other than \
    `module=off` are likely incorrect. Comma separated list.",
);

/// Sets additional default directives to apply to sentry logging. \
/// These apply on top of a default `info` directive. Directives other than \
/// `module=off` are likely incorrect. Comma separated list.
pub const SENTRY_FILTERS: Config<fn() -> String> = Config::new(
    "balancerd_sentry_filters",
    || mz_ore::tracing::SENTRY_DEFAULTS_STR.join(","),
    "Sets additional default directives to apply to sentry logging. \
    These apply on top of a default `info` directive. Directives other than \
    `module=off` are likely incorrect. Comma separated list.",
);

/// Adds the full set of all balancer `Config`s.
pub fn all_dyncfgs(configs: ConfigSet) -> ConfigSet {
    configs
        .add(&SIGTERM_CONNECTION_WAIT)
        .add(&SIGTERM_LISTEN_WAIT)
        .add(&INJECT_PROXY_PROTOCOL_HEADER_HTTP)
        .add(&PGWIRE_SNI_TENANT_RESOLUTION)
        .add(&PGWIRE_FRONTEGG_TENANT_RESOLUTION)
        .add(&TENANT_CACHE_TTL)
        .add(&ADDR_CACHE_TTL)
        .add(&TENANT_RESOLUTION_NEGATIVE_CACHING)
        .add(&LOGGING_FILTER)
        .add(&OPENTELEMETRY_FILTER)
        .add(&LOGGING_FILTER_DEFAULTS)
        .add(&OPENTELEMETRY_FILTER_DEFAULTS)
        .add(&SENTRY_FILTERS)
}

/// Overrides default values for the Balancerd ConfigSet.
///
/// This is meant to be used in combination with clap cli flag
/// `--default-config key=value`
/// Not all ConfigSet values can be defaulted with this
/// function. An error will be returned if a key does
/// not accept default overrides, or if there is a value
/// parsing error..
pub(crate) fn set_defaults(
    config_set: &ConfigSet,
    default_config: Vec<(String, String)>,
) -> Result<(), anyhow::Error> {
    let mut config_updates = ConfigUpdates::default();
    for (k, v) in default_config.iter() {
        if k.as_str() == INJECT_PROXY_PROTOCOL_HEADER_HTTP.name() {
            config_updates.add_dynamic(
                INJECT_PROXY_PROTOCOL_HEADER_HTTP.name(),
                mz_dyncfg::ConfigVal::Bool(bool::from_str(v)?),
            )
        } else if k.as_str() == PGWIRE_SNI_TENANT_RESOLUTION.name() {
            config_updates.add_dynamic(
                PGWIRE_SNI_TENANT_RESOLUTION.name(),
                mz_dyncfg::ConfigVal::Bool(bool::from_str(v)?),
            )
        } else if k.as_str() == PGWIRE_FRONTEGG_TENANT_RESOLUTION.name() {
            config_updates.add_dynamic(
                PGWIRE_FRONTEGG_TENANT_RESOLUTION.name(),
                mz_dyncfg::ConfigVal::Bool(bool::from_str(v)?),
            )
        } else {
            return Err(anyhow!("Invalid default config value {k}"));
        }
    }
    config_updates.apply(config_set);
    Ok(())
}

/// Get all dynamic tracing config parameters from this [`ConfigSet`].
pub fn tracing_config(configs: &ConfigSet) -> Result<TracingParameters, String> {
    fn to_serializable_directives(
        config: &Config<fn() -> String>,
        configs: &ConfigSet,
    ) -> Result<Vec<SerializableDirective>, String> {
        let directives = config.get(configs);
        let directives: Vec<_> = directives
            .split(',')
            .map(Directive::from_str)
            .collect::<Result<_, _>>()
            .map_err(|e| e.to_string())?;
        Ok(directives.into_iter().map(|d| d.into()).collect())
    }

    let log_filter = LOGGING_FILTER.get(configs);
    let log_filter = CloneableEnvFilter::from_str(&log_filter).map_err(|e| e.to_string())?;

    let opentelemetry_filter = OPENTELEMETRY_FILTER.get(configs);
    let opentelemetry_filter =
        CloneableEnvFilter::from_str(&opentelemetry_filter).map_err(|e| e.to_string())?;

    let log_filter_defaults = to_serializable_directives(&LOGGING_FILTER_DEFAULTS, configs)?;

    let opentelemetry_filter_defaults =
        to_serializable_directives(&OPENTELEMETRY_FILTER_DEFAULTS, configs)?;

    let sentry_filters = to_serializable_directives(&SENTRY_FILTERS, configs)?;

    Ok(TracingParameters {
        log_filter: Some(log_filter),
        opentelemetry_filter: Some(opentelemetry_filter),
        log_filter_defaults,
        opentelemetry_filter_defaults,
        sentry_filters,
    })
}

/// Returns true if `updates` contains an update to a tracing config, false otherwise.
pub fn has_tracing_config_update(updates: &ConfigUpdates) -> bool {
    [
        LOGGING_FILTER.name(),
        OPENTELEMETRY_FILTER.name(),
        LOGGING_FILTER_DEFAULTS.name(),
        OPENTELEMETRY_FILTER_DEFAULTS.name(),
        SENTRY_FILTERS.name(),
    ]
    .into_iter()
    .any(|name| updates.updates.contains_key(name))
}

// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use mz_ore::tracing::TracingHandle;
use serde::{Deserialize, Serialize};
use tracing::warn;

use crate::{CloneableEnvFilter, SerializableDirective};

/// Parameters related to `tracing`.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct TracingParameters {
    /// Filter to apply to stderr logging.
    pub log_filter: Option<CloneableEnvFilter>,
    /// Filter to apply to OpenTelemetry/distributed tracing.
    pub opentelemetry_filter: Option<CloneableEnvFilter>,
    /// Additional directives for `log_filter`.
    pub log_filter_defaults: Vec<SerializableDirective>,
    /// Additional directives for `opentelemetry_filter`.
    pub opentelemetry_filter_defaults: Vec<SerializableDirective>,
    /// Additional directives on top of the `info` filter for `sentry`.
    pub sentry_filters: Vec<SerializableDirective>,
}

impl TracingParameters {
    pub fn apply(&self, tracing_handle: &TracingHandle) {
        if let Some(filter) = &self.log_filter {
            if let Err(e) = tracing_handle.reload_stderr_log_filter(
                filter.clone().into(),
                self.log_filter_defaults
                    .iter()
                    .map(|d| d.clone().into())
                    .collect(),
            ) {
                warn!(
                    "unable to apply stderr log filter: {:?}. filter={}, defaults={:?}",
                    e, filter, self.log_filter_defaults
                );
            }
        }
        if let Some(filter) = &self.opentelemetry_filter {
            if let Err(e) = tracing_handle.reload_opentelemetry_filter(
                filter.clone().into(),
                self.opentelemetry_filter_defaults
                    .iter()
                    .map(|d| d.clone().into())
                    .collect(),
            ) {
                warn!(
                    "unable to apply OpenTelemetry filter: {:?}. filter={}, defaults={:?}",
                    e, filter, self.opentelemetry_filter_defaults
                );
            }
        }
        if let Err(e) = tracing_handle.reload_sentry_directives(
            self.sentry_filters
                .iter()
                .map(|d| d.clone().into())
                .collect(),
        ) {
            warn!(
                "unable to apply sentry directives: {:?}. directives={:?}",
                e, self.sentry_filters
            );
        }
    }

    pub fn update(&mut self, other: Self) {
        let Self {
            log_filter,
            opentelemetry_filter,
            log_filter_defaults,
            opentelemetry_filter_defaults,
            sentry_filters,
        } = self;

        let Self {
            log_filter: other_log_filter,
            opentelemetry_filter: other_opentelemetry_filter,
            log_filter_defaults: other_log_filter_defaults,
            opentelemetry_filter_defaults: other_opentelemetry_filter_defaults,
            sentry_filters: other_sentry_filters,
        } = other;

        if let Some(v) = other_log_filter {
            *log_filter = Some(v);
        }
        if let Some(v) = other_opentelemetry_filter {
            *opentelemetry_filter = Some(v);
        }

        *log_filter_defaults = other_log_filter_defaults;
        *opentelemetry_filter_defaults = other_opentelemetry_filter_defaults;
        *sentry_filters = other_sentry_filters;
    }
}

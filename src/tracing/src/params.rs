// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::str::FromStr;

use mz_ore::tracing::TracingHandle;
use mz_proto::{ProtoType, RustType, TryFromProtoError};
use proptest::prelude::{any, Arbitrary, BoxedStrategy, Strategy};
use serde::{Deserialize, Serialize};
use tracing::warn;

use crate::{CloneableEnvFilter, SerializableDirective};

include!(concat!(env!("OUT_DIR"), "/mz_tracing.params.rs"));

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

impl Arbitrary for TracingParameters {
    type Strategy = BoxedStrategy<Self>;
    type Parameters = ();

    fn arbitrary_with(_: Self::Parameters) -> Self::Strategy {
        (
            any::<Option<CloneableEnvFilter>>(),
            any::<Option<CloneableEnvFilter>>(),
            any::<Vec<SerializableDirective>>(),
            any::<Vec<SerializableDirective>>(),
            any::<Vec<SerializableDirective>>(),
        )
            .prop_map(
                |(
                    log_filter,
                    opentelemetry_filter,
                    log_filter_defaults,
                    opentelemetry_filter_defaults,
                    sentry_filters,
                )| Self {
                    log_filter,
                    opentelemetry_filter,
                    log_filter_defaults,
                    opentelemetry_filter_defaults,
                    sentry_filters,
                },
            )
            .boxed()
    }
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

impl RustType<String> for CloneableEnvFilter {
    fn into_proto(&self) -> String {
        format!("{}", self)
    }

    fn from_proto(proto: String) -> Result<Self, TryFromProtoError> {
        CloneableEnvFilter::from_str(&proto)
            // this isn't an accurate enum for this error, but it seems preferable
            // to adding in a dependency on mz_tracing / tracing to mz_proto just
            // to improve the error message here
            .map_err(|x| TryFromProtoError::UnknownEnumVariant(x.to_string()))
    }
}

impl RustType<String> for SerializableDirective {
    fn into_proto(&self) -> String {
        format!("{}", self)
    }

    fn from_proto(proto: String) -> Result<Self, TryFromProtoError> {
        SerializableDirective::from_str(&proto)
            // this isn't an accurate enum for this error, but it seems preferable
            // to adding in a dependency on mz_tracing / tracing to mz_proto just
            // to improve the error message here
            .map_err(|x| TryFromProtoError::UnknownEnumVariant(x.to_string()))
    }
}

impl RustType<ProtoTracingParameters> for TracingParameters {
    fn into_proto(&self) -> ProtoTracingParameters {
        ProtoTracingParameters {
            log_filter: self.log_filter.into_proto(),
            opentelemetry_filter: self.opentelemetry_filter.into_proto(),
            log_filter_defaults: self.log_filter_defaults.into_proto(),
            opentelemetry_filter_defaults: self.opentelemetry_filter_defaults.into_proto(),
            sentry_filters: self.sentry_filters.into_proto(),
        }
    }

    fn from_proto(proto: ProtoTracingParameters) -> Result<Self, TryFromProtoError> {
        Ok(Self {
            log_filter: proto.log_filter.into_rust()?,
            opentelemetry_filter: proto.opentelemetry_filter.into_rust()?,
            log_filter_defaults: proto.log_filter_defaults.into_rust()?,
            opentelemetry_filter_defaults: proto.opentelemetry_filter_defaults.into_rust()?,
            sentry_filters: proto.sentry_filters.into_rust()?,
        })
    }
}

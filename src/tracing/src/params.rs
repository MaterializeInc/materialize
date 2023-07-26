// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::str::FromStr;

use proptest_derive::Arbitrary;
use serde::{Deserialize, Serialize};
use tracing::warn;

use mz_ore::tracing::TracingHandle;
use mz_proto::{ProtoType, RustType, TryFromProtoError};

use crate::CloneableEnvFilter;

include!(concat!(env!("OUT_DIR"), "/mz_tracing.params.rs"));

/// Parameters related to `tracing`.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize, Arbitrary)]
pub struct TracingParameters {
    /// Filter to apply to stderr logging.
    pub log_filter: Option<CloneableEnvFilter>,
    /// Filter to apply to OpenTelemetry/distributed tracing.
    pub opentelemetry_filter: Option<CloneableEnvFilter>,
}

impl TracingParameters {
    pub fn apply(&self, tracing_handle: &TracingHandle) {
        if let Some(filter) = &self.log_filter {
            if let Err(e) = tracing_handle.reload_stderr_log_filter(filter.clone().into()) {
                warn!(
                    "unable to apply stderr log filter: {:?}. filter={}",
                    e, filter
                );
            }
        }
        if let Some(filter) = &self.opentelemetry_filter {
            if let Err(e) = tracing_handle.reload_opentelemetry_filter(filter.clone().into()) {
                warn!(
                    "unable to apply OpenTelemetry filter: {:?}. filter={}",
                    e, filter
                );
            }
        }
    }

    pub fn update(&mut self, other: Self) {
        let Self {
            log_filter,
            opentelemetry_filter,
        } = self;

        let Self {
            log_filter: other_log_filter,
            opentelemetry_filter: other_opentelemetry_filter,
        } = other;

        if let Some(v) = other_log_filter {
            *log_filter = Some(v);
        }
        if let Some(v) = other_opentelemetry_filter {
            *opentelemetry_filter = Some(v);
        }
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

impl RustType<ProtoTracingParameters> for TracingParameters {
    fn into_proto(&self) -> ProtoTracingParameters {
        ProtoTracingParameters {
            log_filter: self.log_filter.into_proto(),
            opentelemetry_filter: self.opentelemetry_filter.into_proto(),
        }
    }

    fn from_proto(proto: ProtoTracingParameters) -> Result<Self, TryFromProtoError> {
        Ok(Self {
            log_filter: proto.log_filter.into_rust()?,
            opentelemetry_filter: proto.opentelemetry_filter.into_rust()?,
        })
    }
}

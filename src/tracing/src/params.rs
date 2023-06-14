// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//!

use mz_ore::tracing::TracingHandle;
use proptest::arbitrary::any;
use proptest::prelude::{Arbitrary, BoxedStrategy};
use proptest::strategy::Strategy;
use proptest_derive::Arbitrary;
use serde::{de, Deserialize, Serialize, Serializer};
use std::str::FromStr;


use mz_proto::{ProtoType, RustType, TryFromProtoError};

use crate::CloneableEnvFilter;

include!(concat!(env!("OUT_DIR"), "/mz_tracing.params.rs"));

/// WIP
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize, Arbitrary)]
pub struct TracingParameters {
    /// WIP
    pub log_filter: Option<CloneableEnvFilter>,
    /// WIP
    pub opentelemetry_filter: Option<CloneableEnvFilter>,
}

impl Arbitrary for CloneableEnvFilter {
    type Strategy = BoxedStrategy<Self>;
    type Parameters = ();

    fn arbitrary_with(_: Self::Parameters) -> Self::Strategy {
        // WIP: write a real implementation
        any::<String>()
            .prop_filter_map("valid", |x| match CloneableEnvFilter::from_str(&x) {
                Ok(ok) => Some(ok),
                Err(_) => None,
            })
            .boxed()
    }
}

impl serde::Serialize for CloneableEnvFilter {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&format!("{}", self))
    }
}

impl<'de> Deserialize<'de> for CloneableEnvFilter {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        Ok(Self::from_str(s.as_str()).map_err(|x| de::Error::custom(x.to_string()))?)
    }
}

impl TracingParameters {
    pub fn apply(&self, tracing_handle: &TracingHandle) {
        if let Some(filter) = &self.log_filter {
            // WIP
            let _ = tracing_handle.reload_stderr_log_filter(filter.clone().inner());
        }
        if let Some(filter) = &self.opentelemetry_filter {
            let _ = tracing_handle.reload_opentelemetry_filter(filter.clone().inner());
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
        Ok(CloneableEnvFilter::from_str(&proto)
            // WIP: TryFromProtoError doesn't have a generic variant, should we add one?
            .map_err(|x| TryFromProtoError::UnknownEnumVariant(x.to_string()))?)
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

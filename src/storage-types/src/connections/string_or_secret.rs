// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use mz_ore::future::InTask;
use mz_proto::{RustType, TryFromProtoError};
use mz_repr::CatalogItemId;
use mz_secrets::SecretsReader;
use proptest_derive::Arbitrary;
use serde::{Deserialize, Serialize};

use crate::connections::SecretsReaderExt;

include!(concat!(
    env!("OUT_DIR"),
    "/mz_storage_types.connections.string_or_secret.rs"
));

#[derive(Arbitrary, Clone, Debug, Eq, PartialEq, Hash, Serialize, Deserialize)]
pub enum StringOrSecret {
    String(String),
    Secret(CatalogItemId),
}

impl StringOrSecret {
    /// Gets the value as a string, reading the secret if necessary.
    pub async fn get_string(
        &self,
        in_task: InTask,
        secrets_reader: &Arc<dyn SecretsReader>,
    ) -> anyhow::Result<String> {
        match self {
            StringOrSecret::String(s) => Ok(s.clone()),
            StringOrSecret::Secret(id) => secrets_reader.read_string_in_task_if(in_task, *id).await,
        }
    }

    /// Asserts that this string or secret is a string and returns its contents.
    pub fn unwrap_string(&self) -> &str {
        match self {
            StringOrSecret::String(s) => s,
            StringOrSecret::Secret(_) => panic!("StringOrSecret::unwrap_string called on a secret"),
        }
    }

    /// Asserts that this string or secret is a secret and returns its global
    /// ID.
    pub fn unwrap_secret(&self) -> CatalogItemId {
        match self {
            StringOrSecret::String(_) => panic!("StringOrSecret::unwrap_secret called on a string"),
            StringOrSecret::Secret(id) => *id,
        }
    }
}

impl RustType<ProtoStringOrSecret> for StringOrSecret {
    fn into_proto(&self) -> ProtoStringOrSecret {
        use proto_string_or_secret::Kind;
        ProtoStringOrSecret {
            kind: Some(match self {
                StringOrSecret::String(s) => Kind::String(s.clone()),
                StringOrSecret::Secret(id) => Kind::Secret(id.into_proto()),
            }),
        }
    }

    fn from_proto(proto: ProtoStringOrSecret) -> Result<Self, TryFromProtoError> {
        use proto_string_or_secret::Kind;
        let kind = proto
            .kind
            .ok_or_else(|| TryFromProtoError::missing_field("ProtoStringOrSecret::kind"))?;
        Ok(match kind {
            Kind::String(s) => StringOrSecret::String(s),
            Kind::Secret(id) => StringOrSecret::Secret(CatalogItemId::from_proto(id)?),
        })
    }
}

impl<V: std::fmt::Display> From<V> for StringOrSecret {
    fn from(v: V) -> StringOrSecret {
        StringOrSecret::String(format!("{}", v))
    }
}

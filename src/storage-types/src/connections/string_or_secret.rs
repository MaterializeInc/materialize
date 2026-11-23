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
use mz_repr::CatalogItemId;
use mz_secrets::SecretsReader;
#[cfg(any(test, feature = "proptest"))]
use proptest_derive::Arbitrary;
use serde::{Deserialize, Serialize};

use crate::connections::SecretsReaderExt;

/// A value given either inline or by reference to a secret.
///
/// `Debug` never prints an inline value. The type carries usernames and
/// certificates as well as OAuth credentials and session tokens, and connection
/// structs are dumped wholesale into logs, so the inline string is hidden for
/// every field.
#[derive(Clone, Eq, PartialEq, Hash, Serialize, Deserialize)]
#[cfg_attr(any(test, feature = "proptest"), derive(Arbitrary))]
pub enum StringOrSecret {
    String(String),
    Secret(CatalogItemId),
}

impl std::fmt::Debug for StringOrSecret {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            StringOrSecret::String(_) => f.write_str("String(<redacted>)"),
            StringOrSecret::Secret(id) => f.debug_tuple("Secret").field(id).finish(),
        }
    }
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

impl<V: std::fmt::Display> From<V> for StringOrSecret {
    fn from(v: V) -> StringOrSecret {
        StringOrSecret::String(format!("{}", v))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[mz_ore::test]
    fn debug_redacts_inline_string() {
        let inline = StringOrSecret::String("hunter2".into());
        assert_eq!(format!("{inline:?}"), "String(<redacted>)");
        assert_eq!(format!("{inline:#?}"), "String(<redacted>)");

        let secret = StringOrSecret::Secret(CatalogItemId::User(7));
        assert_eq!(format!("{secret:?}"), "Secret(User(7))");
    }
}

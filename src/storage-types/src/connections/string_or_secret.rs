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
use mz_secrets::SecretsReader;
pub use mz_storage_types_base::connections::string_or_secret::StringOrSecret;

use crate::connections::SecretsReaderExt;

/// Behavior of [`StringOrSecret`] that reads from the secrets store.
#[async_trait::async_trait]
pub trait StringOrSecretExt {
    /// Gets the value as a string, reading the secret if necessary.
    async fn get_string(
        &self,
        in_task: InTask,
        secrets_reader: &Arc<dyn SecretsReader>,
    ) -> anyhow::Result<String>;
}

#[async_trait::async_trait]
impl StringOrSecretExt for StringOrSecret {
    async fn get_string(
        &self,
        in_task: InTask,
        secrets_reader: &Arc<dyn SecretsReader>,
    ) -> anyhow::Result<String> {
        match self {
            StringOrSecret::String(s) => Ok(s.clone()),
            StringOrSecret::Secret(id) => secrets_reader.read_string_in_task_if(in_task, *id).await,
        }
    }
}

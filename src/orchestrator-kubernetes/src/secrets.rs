// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Management of user secrets via Kubernetes.

use std::iter;
use std::sync::Arc;

use anyhow::anyhow;
use async_trait::async_trait;
use k8s_openapi::api::core::v1::Secret;
use k8s_openapi::ByteString;
use kube::api::{DeleteParams, ObjectMeta, Patch, PatchParams};
use kube::Api;

use mz_repr::GlobalId;
use mz_secrets::{SecretsController, SecretsReader};

use crate::{util, KubernetesOrchestrator, FIELD_MANAGER};

#[async_trait]
impl SecretsController for KubernetesOrchestrator {
    async fn ensure(&self, id: GlobalId, contents: &[u8]) -> Result<(), anyhow::Error> {
        let name = secret_name(id);
        let data = iter::once(("contents".into(), ByteString(contents.into())));
        let secret = Secret {
            metadata: ObjectMeta {
                name: Some(name.clone()),
                ..Default::default()
            },
            data: Some(data.collect()),
            ..Default::default()
        };
        self.secret_api
            .patch(
                &name,
                &PatchParams::apply(FIELD_MANAGER).force(),
                &Patch::Apply(secret),
            )
            .await?;
        Ok(())
    }

    async fn delete(&self, id: GlobalId) -> Result<(), anyhow::Error> {
        // We intentionally don't wait for the secret to be deleted; our
        // obligation is only to initiate the deletion. Garbage collecting
        // secrets that fail to delete will be the responsibility of a future
        // garbage collection task.
        match self
            .secret_api
            .delete(&secret_name(id), &DeleteParams::default())
            .await
        {
            Ok(_) => Ok(()),
            // Secret is already deleted.
            Err(kube::Error::Api(e)) if e.code == 404 => Ok(()),
            Err(e) => return Err(e.into()),
        }
    }

    fn reader(&self) -> Arc<dyn SecretsReader> {
        Arc::new(KubernetesSecretsReader {
            secret_api: self.secret_api.clone(),
        })
    }
}

/// Reads secrets managed by a [`KubernetesOrchestrator`].
#[derive(Debug)]
pub struct KubernetesSecretsReader {
    secret_api: Api<Secret>,
}

impl KubernetesSecretsReader {
    /// Constructs a new Kubernetes secrets reader.
    ///
    /// The `context` parameter works like
    /// [`KubernetesOrchestratorConfig::context`](crate::KubernetesOrchestratorConfig::context).
    pub async fn new(context: String) -> Result<KubernetesSecretsReader, anyhow::Error> {
        let (client, _) = util::create_client(context).await?;
        let secret_api: Api<Secret> = Api::default_namespaced(client);
        Ok(KubernetesSecretsReader { secret_api })
    }
}

#[async_trait]
impl SecretsReader for KubernetesSecretsReader {
    async fn read(&self, id: GlobalId) -> Result<Vec<u8>, anyhow::Error> {
        let secret = self.secret_api.get(&secret_name(id)).await?;
        let mut data = secret
            .data
            .ok_or_else(|| anyhow!("internal error: secret missing data field"))?;
        let contents = data
            .remove("contents")
            .ok_or_else(|| anyhow!("internal error: secret missing contents field"))?;
        Ok(contents.0)
    }
}

fn secret_name(id: GlobalId) -> String {
    format!("user-managed-{id}")
}

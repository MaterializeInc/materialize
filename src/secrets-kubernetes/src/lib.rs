// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use anyhow::{bail, Error};
use async_trait::async_trait;
use k8s_openapi::api::core::v1::Secret;
use k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta;
use k8s_openapi::ByteString;
use kube::api::{Patch, PatchParams};
use kube::config::KubeConfigOptions;
use kube::{Api, Client, Config};
use mz_secrets::{SecretOp, SecretsController};
use std::collections::BTreeMap;
use std::fs::OpenOptions;
use std::path::{Path, PathBuf};
use tokio::time::{self, Duration};
use tracing::info;

const FIELD_MANAGER: &str = "materialized";

// This name has to match the secret name defined in the cluster Environment Controller
// It also has to match the name of the secret defined in the developer tooling
pub const SECRET_NAME: &str = "user-managed-secrets";

pub struct KubernetesSecretsController {
    secret_api: Api<Secret>,
}

impl KubernetesSecretsController {
    pub async fn new(context: String) -> Result<KubernetesSecretsController, anyhow::Error> {
        let kubeconfig_options = KubeConfigOptions {
            context: Some(context),
            ..Default::default()
        };

        let kubeconfig = match Config::from_kubeconfig(&kubeconfig_options).await {
            Ok(config) => config,
            Err(kubeconfig_err) => match Config::from_cluster_env() {
                Ok(config) => config,
                Err(in_cluster_err) => {
                    bail!("failed to infer config: in-cluster: ({in_cluster_err}), kubeconfig: ({kubeconfig_err})");
                }
            },
        };
        let client = Client::try_from(kubeconfig)?;
        let secret_api: Api<Secret> = Api::default_namespaced(client);

        // ensure that the secret has been created in this environment
        secret_api.get(&*SECRET_NAME.to_string()).await?;

        Ok(KubernetesSecretsController { secret_api })
    }
}

#[async_trait]
impl SecretsController for KubernetesSecretsController {
    async fn apply(&mut self, ops: Vec<SecretOp>) -> Result<(), Error> {
        let mut secret: Secret = self.secret_api.get(&*SECRET_NAME.to_string()).await?;

        let mut data = secret.data.map_or_else(BTreeMap::new, |m| m);

        for op in ops.iter() {
            match op {
                SecretOp::Ensure { id, contents } => {
                    info!("inserting/updating secret with ID: {}", id);
                    data.insert(id.to_string(), ByteString(contents.clone()));
                }
                SecretOp::Delete { id } => {
                    info!("deleting secret with ID: {}", id);
                    data.remove(&*id.to_string());
                }
            }
        }

        secret.data = Some(data);

        // Managed_fields can not be present in the object that is being patched.
        // if present, they lead to an 'metadata.managedFields must be nil' error
        secret.metadata.managed_fields = None;

        self.secret_api
            .patch(
                &SECRET_NAME,
                &PatchParams::apply(FIELD_MANAGER).force(),
                &Patch::Apply(secret),
            )
            .await?;

        // guarantee that all new secrets are reflected on our local filesystem
        let secrets_storage_path = PathBuf::from("/secrets");
        for op in ops.iter() {
            let mut interval = time::interval(Duration::from_millis(100));
            for _i in 0..90 {
                match op {
                    SecretOp::Ensure { id, contents } => {
                        let file_path = secrets_storage_path.join(format!("{}", id));
                        if Path::exists(&*file_path) {
                            break;
                        }
                    }
                    SecretOp::Delete { id } => {
                        let file_path = secrets_storage_path.join(format!("{}", id));
                        if !Path::exists(&*file_path) {
                            break;
                        }
                    }
                }
                interval.tick().await;
            }
        }

        return Ok(());
    }
}

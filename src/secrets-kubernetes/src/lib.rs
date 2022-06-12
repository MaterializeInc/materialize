// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use anyhow::{anyhow, bail, Error};
use async_trait::async_trait;
use k8s_openapi::api::core::v1::{Pod, Secret};
use k8s_openapi::ByteString;
use kube::api::{Patch, PatchParams};
use kube::config::KubeConfigOptions;
use kube::{Api, Client, Config, ResourceExt};
use mz_ore::retry::Retry;
use mz_secrets::{SecretOp, SecretsController};
use rand::random;
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::time::Duration;
use tokio::io;
use tracing::info;

const FIELD_MANAGER: &str = "environmentd";
const POD_ANNOTATION: &str = "environmentd.materialize.cloud/secret-refresh";
const POLL_TIMEOUT: u64 = 120;

pub struct KubernetesSecretsControllerConfig {
    pub user_defined_secret: String,
    pub user_defined_secret_mount_path: String,
    pub refresh_pod_name: String,
}

pub struct KubernetesSecretsController {
    secret_api: Api<Secret>,
    pod_api: Api<Pod>,
    config: KubernetesSecretsControllerConfig,
}

impl KubernetesSecretsController {
    pub async fn new(
        context: String,
        config: KubernetesSecretsControllerConfig,
    ) -> Result<KubernetesSecretsController, anyhow::Error> {
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
        let secret_api: Api<Secret> = Api::default_namespaced(client.clone());
        let pod_api: Api<Pod> = Api::default_namespaced(client);

        // ensure that the secret has been created in this environment
        secret_api.get(&*config.user_defined_secret).await?;

        if !Path::new(&config.user_defined_secret_mount_path).is_dir() {
            bail!(
                "Configured secrets location could not be found on filesystem: ({})",
                config.user_defined_secret_mount_path
            );
        }

        Ok(KubernetesSecretsController {
            secret_api,
            pod_api,
            config,
        })
    }

    async fn trigger_resync(&mut self) -> Result<(), Error> {
        let mut pod = Pod::default();
        pod.annotations_mut().insert(
            String::from(POD_ANNOTATION),
            format!("{:x}", random::<u64>()),
        );

        self.pod_api
            .patch(
                &self.config.refresh_pod_name,
                &PatchParams::apply(FIELD_MANAGER).force(),
                &Patch::Apply(pod),
            )
            .await?;

        return Ok(());
    }

    async fn try_exists(path: PathBuf) -> Result<bool, Error> {
        match tokio::fs::metadata(path).await {
            Ok(_) => Ok(true),
            Err(x) if x.kind() == io::ErrorKind::NotFound => Ok(false),
            Err(x) => Err(Error::from(x)),
        }
    }
}

#[async_trait]
impl SecretsController for KubernetesSecretsController {
    async fn apply(&mut self, ops: Vec<SecretOp>) -> Result<(), Error> {
        let mut secret: Secret = self
            .secret_api
            .get(&*self.config.user_defined_secret)
            .await?;

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
                &self.config.user_defined_secret,
                &PatchParams::apply(FIELD_MANAGER).force(),
                &Patch::Apply(secret),
            )
            .await?;

        self.trigger_resync().await?;

        // guarantee that all new secrets are reflected on our local filesystem
        let secrets_storage_path =
            PathBuf::from(self.config.user_defined_secret_mount_path.clone());
        for op in ops.iter() {
            match op {
                SecretOp::Ensure { id, .. } => {
                    Retry::default()
                        .max_duration(Duration::from_secs(POLL_TIMEOUT))
                        .retry_async(|_| async {
                            let file_path = secrets_storage_path.join(format!("{}", id));
                            match KubernetesSecretsController::try_exists(file_path).await {
                                Ok(result) => {
                                    if result {
                                        Ok(())
                                    } else {
                                        Err(anyhow!("Secret write operation has timed out. Secret with id {} could not be written", id))
                                    }
                                }
                                Err(e) => Err(e)
                            }
                        }).await?
                }
                _ => {}
            }
        }

        return Ok(());
    }

    fn supports_multi_statement_txn(&self) -> bool {
        true
    }
}

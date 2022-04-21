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
use kube::api::{Patch, PatchParams, PostParams};
use kube::config::KubeConfigOptions;
use kube::{Api, Client, Config};
use mz_secrets::{SecretOp, SecretsController};
use std::collections::BTreeMap;
use tracing::{error, info};

const FIELD_MANAGER: &str = "materialized";
const HARDCODED_NAME: &str = "dataflowd-secret";

pub struct KubernetesSecretsController {
    secret_api: Api<Secret>,
}

impl KubernetesSecretsController {
    fn make_secret_with_name(name: String) -> Secret {
        Secret {
            data: None,
            immutable: None,
            metadata: ObjectMeta {
                name: Some(name),
                ..Default::default()
            },
            string_data: None,
            type_: None,
        }
    }

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

        let name: String = format!("{}", HARDCODED_NAME);

        let secret = KubernetesSecretsController::make_secret_with_name(name.clone());
        match secret_api.create(&PostParams::default(), &secret).await {
            Ok(_) => Ok(()),
            Err(kube::Error::Api(e)) if e.code == 409 => {
                info!("Secret {} already exists", name);
                Ok(())
            }
            Err(e) => {
                error!("creating secret failed: {}", e);
                Err(e)
            }
        }?;

        Ok(KubernetesSecretsController { secret_api })
    }
}

#[async_trait]
impl SecretsController for KubernetesSecretsController {
    async fn apply(&mut self, ops: Vec<SecretOp>) -> Result<(), Error> {
        let name: String = format!("{}", HARDCODED_NAME);

        let mut secret: Secret = self.secret_api.get(&*name).await?;

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
                &name,
                &PatchParams::apply(FIELD_MANAGER).force(),
                &Patch::Apply(secret),
            )
            .await?;

        return Ok(());
    }
}

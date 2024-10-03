// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Instant;

use anyhow::anyhow;
use async_trait::async_trait;
use aws_config::SdkConfig;
use aws_sdk_secretsmanager::config::Builder as SecretsManagerConfigBuilder;
use aws_sdk_secretsmanager::error::SdkError;
use aws_sdk_secretsmanager::primitives::Blob;
use aws_sdk_secretsmanager::types::{Filter, FilterNameStringType, Tag};
use aws_sdk_secretsmanager::Client;
use mz_repr::CatalogItemId;
use mz_secrets::{SecretsController, SecretsReader};
use tracing::info;
use uuid::Uuid;

#[derive(Clone, Debug)]
pub struct AwsSecretsController {
    pub client: AwsSecretsClient,
    pub kms_key_alias: String,
    pub default_tags: BTreeMap<String, String>,
}

pub async fn load_sdk_config() -> SdkConfig {
    let mut config_loader = mz_aws_util::defaults();
    if let Ok(endpoint) = std::env::var("AWS_ENDPOINT_URL") {
        config_loader = config_loader.endpoint_url(endpoint);
    }
    config_loader.load().await
}

async fn load_secrets_manager_client() -> Client {
    let sdk_config = load_sdk_config().await;
    let sm_config = SecretsManagerConfigBuilder::from(&sdk_config).build();
    Client::from_conf(sm_config)
}

impl AwsSecretsController {
    pub async fn new(
        prefix: &str,
        key_alias: &str,
        default_tags: BTreeMap<String, String>,
    ) -> Self {
        AwsSecretsController {
            client: AwsSecretsClient::new(prefix).await,
            kms_key_alias: key_alias.to_string(),
            default_tags,
        }
    }

    fn tags(&self) -> Vec<Tag> {
        self.default_tags
            .iter()
            .map(|(key, value)| Tag::builder().key(key).value(value).build())
            .collect()
    }
}

#[async_trait]
impl SecretsController for AwsSecretsController {
    async fn ensure(&self, id: CatalogItemId, contents: &[u8]) -> Result<(), anyhow::Error> {
        match self
            .client
            .client
            .create_secret()
            .name(self.client.secret_name(id))
            .kms_key_id(self.kms_key_alias.clone())
            .secret_binary(Blob::new(contents))
            .set_tags(Some(self.tags()))
            .send()
            .await
        {
            Ok(_) => {}
            Err(SdkError::ServiceError(e)) if e.err().is_resource_exists_exception() => {
                self.client
                    .client
                    .put_secret_value()
                    .secret_id(self.client.secret_name(id))
                    .secret_binary(Blob::new(contents))
                    .send()
                    .await?;
            }
            Err(e) => Err(e)?,
        }
        Ok(())
    }

    async fn delete(&self, id: CatalogItemId) -> Result<(), anyhow::Error> {
        match self
            .client
            .client
            .delete_secret()
            .secret_id(self.client.secret_name(id))
            .force_delete_without_recovery(true)
            .send()
            .await
        {
            Ok(_) => Ok(()),
            // Secret is already deleted.
            Err(SdkError::ServiceError(e)) if e.err().is_resource_not_found_exception() => Ok(()),
            Err(e) => {
                return Err(e.into());
            }
        }
    }

    async fn list(&self) -> Result<Vec<CatalogItemId>, anyhow::Error> {
        let mut ids = Vec::new();
        let mut filters = self.default_tags.iter().fold(
            Vec::with_capacity(self.default_tags.len() * 2 + 1),
            |mut filters, (key, value)| {
                filters.push(
                    Filter::builder()
                        .key(FilterNameStringType::TagKey)
                        .values(key)
                        .build(),
                );
                filters.push(
                    Filter::builder()
                        .key(FilterNameStringType::TagValue)
                        .values(value)
                        .build(),
                );
                filters
            },
        );
        filters.push(
            Filter::builder()
                .key(FilterNameStringType::Name)
                .values(&self.client.secret_name_prefix)
                .build(),
        );
        let mut secrets_paginator = self
            .client
            .client
            .list_secrets()
            .set_filters(Some(filters))
            .into_paginator()
            .send();
        while let Some(page) = secrets_paginator.next().await {
            for secret in page?.secret_list() {
                let required_tags_count: usize = secret
                    .tags()
                    .into_iter()
                    .filter_map(|tag| {
                        tag.key().and_then(|key| {
                            if self.default_tags.contains_key(key)
                                && tag.value() == self.default_tags.get(key).map(|x| x.as_str())
                            {
                                Some(1)
                            } else {
                                None
                            }
                        })
                    })
                    .sum();
                // Ignore improperly tagged objects.
                if required_tags_count != self.default_tags.len() {
                    continue;
                }
                // Ignore invalidly named objects.
                let Some(id) = self.client.id_from_secret_name(secret.name().unwrap()) else {
                    continue;
                };
                ids.push(id);
            }
        }
        Ok(ids)
    }

    fn reader(&self) -> Arc<dyn SecretsReader> {
        Arc::new(self.client.clone())
    }
}

#[derive(Clone, Debug)]
pub struct AwsSecretsClient {
    pub(crate) client: Client,
    pub(crate) secret_name_prefix: String,
}

impl AwsSecretsClient {
    pub async fn new(prefix: &str) -> Self {
        Self {
            client: load_secrets_manager_client().await,
            // TODO [Alex Hunt] move this to a shared function that can be imported by the
            // region-controller.
            secret_name_prefix: prefix.to_owned(),
        }
    }

    fn secret_name(&self, id: CatalogItemId) -> String {
        format!("{}{}", self.secret_name_prefix, id)
    }

    fn id_from_secret_name(&self, name: &str) -> Option<CatalogItemId> {
        name.strip_prefix(&self.secret_name_prefix)
            .and_then(|id| id.parse().ok())
    }
}

#[async_trait]
impl SecretsReader for AwsSecretsClient {
    async fn read(&self, id: CatalogItemId) -> Result<Vec<u8>, anyhow::Error> {
        let op_id = Uuid::new_v4();
        info!(secret_id = %id, %op_id, "reading secret from AWS");
        let start = Instant::now();
        let secret = async {
            Ok(self
                .client
                .get_secret_value()
                .secret_id(self.secret_name(id))
                .send()
                .await?
                .secret_binary()
                .ok_or_else(|| anyhow!("internal error: secret missing secret_binary field"))?
                .to_owned()
                .into_inner())
        }
        .await;
        info!(%op_id, success = %secret.is_ok(), "secret read in {:?}", start.elapsed());
        secret
    }
}

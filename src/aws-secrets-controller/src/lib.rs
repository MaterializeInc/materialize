// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

// BEGIN LINT CONFIG
// DO NOT EDIT. Automatically generated by bin/gen-lints.
// Have complaints about the noise? See the note in misc/python/materialize/cli/gen-lints.py first.
#![allow(unknown_lints)]
#![allow(clippy::style)]
#![allow(clippy::complexity)]
#![allow(clippy::large_enum_variant)]
#![allow(clippy::mutable_key_type)]
#![allow(clippy::stable_sort_primitive)]
#![allow(clippy::map_entry)]
#![allow(clippy::box_default)]
#![allow(clippy::drain_collect)]
#![warn(clippy::bool_comparison)]
#![warn(clippy::clone_on_ref_ptr)]
#![warn(clippy::no_effect)]
#![warn(clippy::unnecessary_unwrap)]
#![warn(clippy::dbg_macro)]
#![warn(clippy::todo)]
#![warn(clippy::wildcard_dependencies)]
#![warn(clippy::zero_prefixed_literal)]
#![warn(clippy::borrowed_box)]
#![warn(clippy::deref_addrof)]
#![warn(clippy::double_must_use)]
#![warn(clippy::double_parens)]
#![warn(clippy::extra_unused_lifetimes)]
#![warn(clippy::needless_borrow)]
#![warn(clippy::needless_question_mark)]
#![warn(clippy::needless_return)]
#![warn(clippy::redundant_pattern)]
#![warn(clippy::redundant_slicing)]
#![warn(clippy::redundant_static_lifetimes)]
#![warn(clippy::single_component_path_imports)]
#![warn(clippy::unnecessary_cast)]
#![warn(clippy::useless_asref)]
#![warn(clippy::useless_conversion)]
#![warn(clippy::builtin_type_shadow)]
#![warn(clippy::duplicate_underscore_argument)]
#![warn(clippy::double_neg)]
#![warn(clippy::unnecessary_mut_passed)]
#![warn(clippy::wildcard_in_or_patterns)]
#![warn(clippy::crosspointer_transmute)]
#![warn(clippy::excessive_precision)]
#![warn(clippy::overflow_check_conditional)]
#![warn(clippy::as_conversions)]
#![warn(clippy::match_overlapping_arm)]
#![warn(clippy::zero_divided_by_zero)]
#![warn(clippy::must_use_unit)]
#![warn(clippy::suspicious_assignment_formatting)]
#![warn(clippy::suspicious_else_formatting)]
#![warn(clippy::suspicious_unary_op_formatting)]
#![warn(clippy::mut_mutex_lock)]
#![warn(clippy::print_literal)]
#![warn(clippy::same_item_push)]
#![warn(clippy::useless_format)]
#![warn(clippy::write_literal)]
#![warn(clippy::redundant_closure)]
#![warn(clippy::redundant_closure_call)]
#![warn(clippy::unnecessary_lazy_evaluations)]
#![warn(clippy::partialeq_ne_impl)]
#![warn(clippy::redundant_field_names)]
#![warn(clippy::transmutes_expressible_as_ptr_casts)]
#![warn(clippy::unused_async)]
#![warn(clippy::disallowed_methods)]
#![warn(clippy::disallowed_macros)]
#![warn(clippy::disallowed_types)]
#![warn(clippy::from_over_into)]
#![warn(clippy::large_futures)]
// END LINT CONFIG

use std::collections::BTreeMap;
use std::sync::Arc;

use anyhow::anyhow;
use async_trait::async_trait;
use aws_config::meta::region::RegionProviderChain;
use aws_config::SdkConfig;
use aws_sdk_secretsmanager::config::{Builder as SecretsManagerConfigBuilder, Region};
use aws_sdk_secretsmanager::error::SdkError;
use aws_sdk_secretsmanager::primitives::Blob;
use aws_sdk_secretsmanager::types::{Filter, FilterNameStringType, Tag};
use aws_sdk_secretsmanager::Client;
use futures::stream::StreamExt;
use mz_repr::GlobalId;
use mz_secrets::{SecretsController, SecretsReader};

#[derive(Clone, Debug)]
pub struct AwsSecretsController {
    pub client: AwsSecretsClient,
    pub kms_key_alias: String,
    pub default_tags: BTreeMap<String, String>,
}

pub async fn load_sdk_config(region: String) -> SdkConfig {
    let region_provider = RegionProviderChain::first_try(Region::new(region));
    let mut config_loader = aws_config::from_env().region(region_provider);
    if let Ok(endpoint) = std::env::var("AWS_ENDPOINT_URL") {
        config_loader = config_loader.endpoint_url(endpoint);
    }
    config_loader.load().await
}

async fn load_secrets_manager_client(region: String) -> Client {
    let sdk_config = load_sdk_config(region).await;
    let sm_config = SecretsManagerConfigBuilder::from(&sdk_config).build();
    Client::from_conf(sm_config)
}

impl AwsSecretsController {
    pub async fn new(
        region: &str,
        prefix: &str,
        key_alias: &str,
        default_tags: BTreeMap<String, String>,
    ) -> Self {
        AwsSecretsController {
            client: AwsSecretsClient::new(region, prefix).await,
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
    async fn ensure(&self, id: GlobalId, contents: &[u8]) -> Result<(), anyhow::Error> {
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

    async fn delete(&self, id: GlobalId) -> Result<(), anyhow::Error> {
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

    async fn list(&self) -> Result<Vec<GlobalId>, anyhow::Error> {
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
            for secret in page?.secret_list().unwrap_or_default() {
                let required_tags_count: usize = secret
                    .tags()
                    .expect("we just filtered to things with the expected tags")
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
    pub async fn new(region: &str, prefix: &str) -> Self {
        Self {
            client: load_secrets_manager_client(region.to_owned()).await,
            // TODO [Alex Hunt] move this to a shared function that can be imported by the
            // region-controller.
            secret_name_prefix: prefix.to_owned(),
        }
    }

    fn secret_name(&self, id: GlobalId) -> String {
        format!("{}{}", self.secret_name_prefix, id)
    }

    fn id_from_secret_name(&self, name: &str) -> Option<GlobalId> {
        name.strip_prefix(&self.secret_name_prefix)
            .and_then(|id| id.parse().ok())
    }
}

#[async_trait]
impl SecretsReader for AwsSecretsClient {
    async fn read(&self, id: GlobalId) -> Result<Vec<u8>, anyhow::Error> {
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
}

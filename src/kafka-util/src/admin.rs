// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Helpers for working with Kafka's admin API.

use std::collections::BTreeMap;
use std::iter;
use std::time::Duration;

use anyhow::{anyhow, bail};
use itertools::Itertools;
use mz_ore::collections::CollectionExt;
use mz_ore::retry::Retry;
use mz_ore::str::separated;
use rdkafka::admin::{
    AdminClient, AdminOptions, AlterConfig, ConfigEntry, ConfigResource, ConfigSource, NewTopic,
    OwnedResourceSpecifier, ResourceSpecifier,
};
use rdkafka::client::ClientContext;
use rdkafka::error::{KafkaError, RDKafkaErrorCode};
use tracing::{info, warn};

/// Get the current configuration for a particular topic.
///
/// Materialize may not have permission to list configs for the topic, so callers of this method
/// should "fail open" if the configs are not available.
pub async fn get_topic_config<'a, C>(
    client: &'a AdminClient<C>,
    admin_opts: &AdminOptions,
    topic_name: &str,
) -> anyhow::Result<Vec<ConfigEntry>>
where
    C: ClientContext,
{
    let ConfigResource { specifier, entries } = client
        .describe_configs([&ResourceSpecifier::Topic(topic_name)], admin_opts)
        .await?
        .into_iter()
        .exactly_one()??;

    match specifier {
        OwnedResourceSpecifier::Topic(name) if name.as_str() == topic_name => {}
        unexpected => {
            bail!("describe configs returned unexpected resource specifier: {unexpected:?}")
        }
    };

    Ok(entries)
}

/// Alter the current configuration for a particular topic.
///
/// Materialize may not have permission to alter configs for the topic, so callers of this method
/// should "fail open" if the configs are not available.
pub async fn alter_topic_config<'a, C>(
    client: &'a AdminClient<C>,
    admin_opts: &AdminOptions,
    topic_name: &str,
    new_config: impl IntoIterator<Item = (&str, &str)>,
) -> anyhow::Result<()>
where
    C: ClientContext,
{
    let mut alter_config = AlterConfig::new(ResourceSpecifier::Topic(topic_name));
    for (key, val) in new_config {
        alter_config = alter_config.set(key, val);
    }
    let result = client
        .alter_configs([&alter_config], admin_opts)
        .await?
        .into_iter()
        .exactly_one()?;

    let (specifier, result) = match result {
        Ok(specifier) => (specifier, Ok(())),
        Err((specifier, err)) => (specifier, Err(KafkaError::AdminOp(err))),
    };

    match specifier {
        OwnedResourceSpecifier::Topic(name) if name.as_str() == topic_name => {}
        unexpected => {
            bail!("alter configs returned unexpected resource specifier: {unexpected:?}")
        }
    };

    Ok(result?)
}

/// When the topic we want to ensure already exists, what happens?
#[derive(Debug, Copy, Clone)]
pub enum EnsureTopicConfig {
    /// Do nothing: assume whatever config is there is fine.
    Skip,
    /// Check and log the existing config, but don't try and change it.
    Check,
    /// If the remote config doesn't match, try and change it to match.
    Alter,
}

/// Expect the current configuration for a particular topic to match a provided set of values.
/// If there are other configs set, we will ignore them.
///
/// Materialize may not have permission to alter configs for the topic, so callers of this method
/// should "fail open" if the configs are not available.
pub async fn ensure_topic_config<'a, C>(
    client: &'a AdminClient<C>,
    admin_opts: &AdminOptions,
    new_topic: &'a NewTopic<'a>,
    expect: EnsureTopicConfig,
) -> anyhow::Result<bool>
where
    C: ClientContext,
{
    let try_alter = match expect {
        EnsureTopicConfig::Skip => return Ok(true),
        EnsureTopicConfig::Check => false,
        EnsureTopicConfig::Alter => true,
    };

    let mut expected_configs: BTreeMap<_, _> = new_topic.config.iter().copied().collect();

    let actual_configs = get_topic_config(client, admin_opts, new_topic.name).await?;
    info!(
        topic = new_topic.name,
        "got configuration for existing topic: [{}]",
        separated(
            ", ",
            actual_configs.iter().map(|e| {
                let kv = [&*e.name, e.value.as_ref().map_or("<none>", |v| &*v)];
                separated(": ", kv)
            })
        )
    );

    let actual_config_values: BTreeMap<_, _> = actual_configs
        .iter()
        .filter_map(|e| e.value.as_ref().map(|v| (e.name.as_str(), v.as_str())))
        .collect();
    for (config, expected) in &expected_configs {
        match actual_config_values.get(config) {
            Some(actual) => {
                if actual != expected {
                    warn!(
                        topic = new_topic.name,
                        config, expected, actual, "unexpected value for config entry"
                    )
                }
            }
            None => {
                warn!(
                    topic = new_topic.name,
                    config, expected, "missing expected value for config entry"
                )
            }
        }
    }

    if try_alter {
        // rdkafka does not support the newer incremental-alter APIs. Instead, we copy over values
        // from the fetched dynamic config that we don't explicitly want to overwrite.
        for entry in &actual_configs {
            if entry.source != ConfigSource::DynamicTopic {
                continue;
            }
            let Some(value) = entry.value.as_ref() else {
                continue;
            };
            expected_configs.entry(&entry.name).or_insert(value);
        }
        alter_topic_config(client, admin_opts, new_topic.name, expected_configs).await?;
        Ok(true)
    } else {
        Ok(false)
    }
}

/// Creates a Kafka topic if it does not exist, and waits for it to be reported in the broker
/// metadata.
///
/// This function is a wrapper around [`AdminClient::create_topics`] that
/// attempts to ensure the topic creation has propagated throughout the Kafka
/// cluster before returning. Kafka topic creation is asynchronous, so
/// attempting to consume from or produce to a topic immediately after its
/// creation can result in "unknown topic" errors.
///
/// This function does not return successfully unless it can find the metadata
/// for the newly-created topic in a call to [`rdkafka::client::Client::fetch_metadata`] and
/// verify that the metadata reports the topic has the number of partitions
/// requested in `new_topic`. Empirically, this seems to be the condition that
/// guarantees that future attempts to consume from or produce to the topic will
/// succeed.
///
/// Returns a boolean indicating whether the topic already existed.
pub async fn ensure_topic<'a, C>(
    client: &'a AdminClient<C>,
    admin_opts: &AdminOptions,
    new_topic: &'a NewTopic<'a>,
    on_existing: EnsureTopicConfig,
) -> anyhow::Result<bool>
where
    C: ClientContext,
{
    let res = client
        .create_topics(iter::once(new_topic), admin_opts)
        .await?;

    let already_exists = match res.as_slice() {
        &[Ok(_)] => false,
        &[Err((_, RDKafkaErrorCode::TopicAlreadyExists))] => true,
        &[Err((_, e))] => bail!(KafkaError::AdminOp(e)),
        other => bail!(
            "kafka topic creation returned {} results, but exactly one result was expected",
            other.len()
        ),
    };

    // We don't need to read in metadata / do any validation if the topic already exists.
    if already_exists {
        match ensure_topic_config(client, admin_opts, new_topic, on_existing).await {
            Ok(true) => {}
            Ok(false) => {
                info!(
                    topic = new_topic.name,
                    "did not sync topic config; configs may not match expected values"
                );
            }
            Err(error) => {
                warn!(
                    topic = new_topic.name,
                    "unable to enforce topic config; configs may not match expected values: {error:#}"
                )
            }
        }

        return Ok(true);
    }

    // Topic creation is asynchronous, and if we don't wait for it to complete,
    // we might produce a message (below) that causes it to get automatically
    // created with the default number partitions, and not the number of
    // partitions requested in `new_topic`.
    Retry::default()
        .max_duration(Duration::from_secs(30))
        .retry_async(|_| async {
            let metadata = client
                .inner()
                // N.B. It is extremely important not to ask specifically
                // about the topic here, even though the API supports it!
                // Asking about the topic will create it automatically...
                // with the wrong number of partitions. Yes, this is
                // unbelievably horrible.
                .fetch_metadata(None, Some(Duration::from_secs(10)))?;
            let topic = metadata
                .topics()
                .iter()
                .find(|t| t.name() == new_topic.name)
                .ok_or(anyhow!("unable to fetch topic metadata after creation"))?;
            // If the desired number of partitions is not "use the broker
            // default", wait for the topic to have the correct number of
            // partitions.
            if new_topic.num_partitions != -1 {
                let actual = i32::try_from(topic.partitions().len())?;
                if actual != new_topic.num_partitions {
                    bail!(
                        "topic reports {actual} partitions, but expected {} partitions",
                        new_topic.num_partitions
                    );
                }
            }
            Ok(false)
        })
        .await
}

/// Deletes a Kafka topic and waits for it to be reported absent in the broker metadata.
///
/// This function is a wrapper around [`AdminClient::delete_topics`] that attempts to ensure the
/// topic deletion has propagated throughout the Kafka cluster before returning.
///
/// This function does not return successfully unless it can observe the metadata not containing
/// the newly-created topic in a call to [`rdkafka::client::Client::fetch_metadata`]
pub async fn delete_existing_topic<'a, C>(
    client: &'a AdminClient<C>,
    admin_opts: &AdminOptions,
    topic: &'a str,
) -> Result<(), DeleteTopicError>
where
    C: ClientContext,
{
    delete_topic_helper(client, admin_opts, topic, false).await
}

/// Like `delete_existing_topic` but allow topic to be already deleted
pub async fn delete_topic<'a, C>(
    client: &'a AdminClient<C>,
    admin_opts: &AdminOptions,
    topic: &'a str,
) -> Result<(), DeleteTopicError>
where
    C: ClientContext,
{
    delete_topic_helper(client, admin_opts, topic, true).await
}

async fn delete_topic_helper<'a, C>(
    client: &'a AdminClient<C>,
    admin_opts: &AdminOptions,
    topic: &'a str,
    allow_missing: bool,
) -> Result<(), DeleteTopicError>
where
    C: ClientContext,
{
    let res = client.delete_topics(&[topic], admin_opts).await?;
    if res.len() != 1 {
        return Err(DeleteTopicError::TopicCountMismatch(res.len()));
    }
    let already_missing = match res.into_element() {
        Ok(_) => Ok(false),
        Err((_, RDKafkaErrorCode::UnknownTopic)) if allow_missing => Ok(true),
        Err((_, e)) => Err(DeleteTopicError::Kafka(KafkaError::AdminOp(e))),
    }?;

    // We don't need to read in metadata / do any validation if the topic already exists.
    if already_missing {
        return Ok(());
    }

    // Topic deletion is asynchronous, and if we don't wait for it to complete,
    // we might produce a message (below) that causes it to get automatically
    // created with the default number partitions, and not the number of
    // partitions requested in `new_topic`.
    Retry::default()
        .max_duration(Duration::from_secs(30))
        .retry_async(|_| async {
            let metadata = client
                .inner()
                // N.B. It is extremely important not to ask specifically
                // about the topic here, even though the API supports it!
                // Asking about the topic will create it automatically...
                // with the wrong number of partitions. Yes, this is
                // unbelievably horrible.
                .fetch_metadata(None, Some(Duration::from_secs(10)))?;
            let topic_exists = metadata.topics().iter().any(|t| t.name() == topic);
            if topic_exists {
                Err(DeleteTopicError::TopicRessurected)
            } else {
                Ok(())
            }
        })
        .await
}

/// An error while creating a Kafka topic.
#[derive(Debug, thiserror::Error)]
pub enum DeleteTopicError {
    /// An error from the underlying Kafka library.
    #[error(transparent)]
    Kafka(#[from] KafkaError),
    /// Topic creation returned the wrong number of results.
    #[error("kafka topic creation returned {0} results, but exactly one result was expected")]
    TopicCountMismatch(usize),
    /// The topic remained in metadata after being deleted.
    #[error("topic was recreated after being deleted")]
    TopicRessurected,
}

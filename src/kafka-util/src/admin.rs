// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Helpers for working with Kafka's admin API.

use std::iter;
use std::time::Duration;

use mz_ore::collections::CollectionExt;
use mz_ore::retry::Retry;
use rdkafka::admin::{AdminClient, AdminOptions, NewTopic};
use rdkafka::client::ClientContext;
use rdkafka::error::{KafkaError, RDKafkaErrorCode};

/// Creates a Kafka topic and waits for it to be reported in the broker
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
pub async fn create_new_topic<'a, C>(
    client: &'a AdminClient<C>,
    admin_opts: &AdminOptions,
    new_topic: &'a NewTopic<'a>,
) -> Result<(), CreateTopicError>
where
    C: ClientContext,
{
    create_topic_helper(client, admin_opts, new_topic, false).await
}

/// Like `create_new_topic` but allow topic to already exist
pub async fn ensure_topic<'a, C>(
    client: &'a AdminClient<C>,
    admin_opts: &AdminOptions,
    new_topic: &'a NewTopic<'a>,
) -> Result<(), CreateTopicError>
where
    C: ClientContext,
{
    create_topic_helper(client, admin_opts, new_topic, true).await
}

async fn create_topic_helper<'a, C>(
    client: &'a AdminClient<C>,
    admin_opts: &AdminOptions,
    new_topic: &'a NewTopic<'a>,
    allow_existing: bool,
) -> Result<(), CreateTopicError>
where
    C: ClientContext,
{
    let res = client
        .create_topics(iter::once(new_topic), admin_opts)
        .await?;
    if res.len() != 1 {
        return Err(CreateTopicError::TopicCountMismatch(res.len()));
    }
    let already_exists = match res.into_element() {
        Ok(_) => Ok(false),
        Err((_, RDKafkaErrorCode::TopicAlreadyExists)) if allow_existing => Ok(true),
        Err((_, e)) => Err(CreateTopicError::Kafka(KafkaError::AdminOp(e))),
    }?;

    // We don't need to read in metadata / do any validation if the topic already exists.
    if already_exists {
        return Ok(());
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
                .ok_or(CreateTopicError::MissingMetadata)?;
            let num_partitions = i32::try_from(topic.partitions().len())
                .map_err(|_| CreateTopicError::TooManyPartitions)?;
            if num_partitions != new_topic.num_partitions {
                return Err(CreateTopicError::PartitionCountMismatch {
                    expected: new_topic.num_partitions,
                    actual: num_partitions,
                });
            }
            Ok(())
        })
        .await
}

/// An error while creating a Kafka topic.
#[derive(Debug, thiserror::Error)]
pub enum CreateTopicError {
    /// An error from the underlying Kafka library.
    #[error(transparent)]
    Kafka(#[from] KafkaError),
    /// Topic creation returned the wrong number of results.
    #[error("kafka topic creation returned {0} results, but exactly one result was expected")]
    TopicCountMismatch(usize),
    /// The topic metadata could not be fetched after the topic was created.
    #[error("unable to fetch topic metadata after creation")]
    MissingMetadata,
    /// The topic reported more than the maximum allowable number of partitions.
    #[error("the topic reported more than {} partitions", i32::MAX)]
    TooManyPartitions,
    /// The topic metadata reported a number of partitions that did not match
    /// the number of partitions in the topic creation request.
    #[error("topic reports {actual} partitions, but expected {expected} partitions")]
    PartitionCountMismatch {
        /// The requested number of partitions.
        expected: i32,
        /// The reported number of partitions.
        actual: i32,
    },
}

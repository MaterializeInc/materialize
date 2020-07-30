// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Helpers for working with Kafka's admin API.

use std::error::Error;
use std::fmt;
use std::iter;
use std::time::Duration;

use rdkafka::admin::{AdminClient, AdminOptions, NewTopic};
use rdkafka::client::ClientContext;
use rdkafka::error::{KafkaError, RDKafkaError};

use ore::collections::CollectionExt;
use ore::retry;

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
/// for the newly-created topic in a call to [`Client::fetch_metadata`] and
/// verify that the metadata reports the topic has the number of partitions
/// requested in `new_topic`. Empirically, this seems to be the condition that
/// guarantees that future attempts to consume from or produce to the topic will
/// succeed.
pub async fn create_topic<'a, C>(
    client: &'a AdminClient<C>,
    admin_opts: &AdminOptions,
    new_topic: &'a NewTopic<'a>,
) -> Result<(), CreateTopicError>
where
    C: ClientContext,
{
    let res = client
        .create_topics(iter::once(new_topic), &admin_opts)
        .await?;
    if res.len() != 1 {
        return Err(CreateTopicError::InvariantViolation(format!(
            "kafka topic creation returned {} results, but exactly one result was expected",
            res.len()
        )));
    }
    match res.into_element() {
        Ok(_) | Err((_, RDKafkaError::TopicAlreadyExists)) => Ok(()),
        Err((_, e)) => Err(CreateTopicError::Kafka(KafkaError::AdminOp(e))),
    }?;

    // Topic creation is asynchronous, and if we don't wait for it to
    // complete, we might produce a message (below) that causes it to
    // get automatically created with multiple partitions. (Since
    // multiple partitions have no ordering guarantees, this violates
    // many assumptions that our tests make.)
    retry::retry_for(Duration::from_secs(8), |_| async {
        let metadata = client
            .inner()
            // N.B. It is extremely important not to ask specifically
            // about the topic here, even though the API supports it!
            // Asking about the topic will create it automatically...
            // with the wrong number of partitions. Yes, this is
            // unbelievably horrible.
            .fetch_metadata(None, Some(Duration::from_secs(1)))?;
        let topic = metadata
            .topics()
            .iter()
            .find(|t| t.name() == new_topic.name)
            .ok_or(CreateTopicError::MissingMetadata)?;
        if topic.partitions().len() as i32 != new_topic.num_partitions {
            return Err(CreateTopicError::PartitionCountMismatch {
                expected: new_topic.num_partitions,
                actual: topic.partitions().len() as i32,
            });
        }
        Ok(())
    })
    .await
}

/// An error while creating a Kafka topic.
#[derive(Debug)]
pub enum CreateTopicError {
    /// An error from the underlying Kafka library.
    Kafka(KafkaError),
    /// An invariant was violated in the protocol.
    InvariantViolation(String),
    /// The topic metadata could not be fetched after the topic was created.
    MissingMetadata,
    /// The topic metadata reported a number of partitions that did not match
    /// the number of partitions in the topic creation request.
    PartitionCountMismatch {
        /// The requested number of partitions.
        expected: i32,
        /// The reported number of partitions.
        actual: i32,
    },
}

impl fmt::Display for CreateTopicError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            CreateTopicError::Kafka(e) => write!(f, "{}", e),
            CreateTopicError::InvariantViolation(s) => f.write_str(s),
            CreateTopicError::MissingMetadata => {
                f.write_str("unable to fetch topic metadata after creation")
            }
            CreateTopicError::PartitionCountMismatch { expected, actual } => write!(
                f,
                "topic reports {} partitions, but expected {} partitions",
                actual, expected
            ),
        }
    }
}

impl Error for CreateTopicError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            CreateTopicError::Kafka(e) => Some(e),
            CreateTopicError::InvariantViolation(_)
            | CreateTopicError::MissingMetadata
            | CreateTopicError::PartitionCountMismatch { .. } => None,
        }
    }
}

impl From<KafkaError> for CreateTopicError {
    fn from(e: KafkaError) -> CreateTopicError {
        CreateTopicError::Kafka(e)
    }
}

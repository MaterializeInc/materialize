// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Helpers for working with Kafka's client API.

use std::time::Duration;

use anyhow::bail;
use mz_ore::collections::CollectionExt;
use rdkafka::client::Client;
use rdkafka::consumer::ConsumerContext;
use rdkafka::producer::{DefaultProducerContext, DeliveryResult, ProducerContext};
use rdkafka::ClientContext;
use tracing::{debug, error, info, warn};

/// A `ClientContext` implementation that uses `tracing` instead of `log` macros.
///
/// All code in Materialize that constructs Kafka clients should use this context or
/// a custom context that delegates the `log` and `error` methods to this implementation.
pub struct MzClientContext;

impl ClientContext for MzClientContext {
    fn log(&self, level: rdkafka::config::RDKafkaLogLevel, fac: &str, log_message: &str) {
        use rdkafka::config::RDKafkaLogLevel::*;
        // Copied from https://docs.rs/rdkafka/0.28.0/src/rdkafka/client.rs.html#58-79
        // but using `tracing`
        match level {
            Emerg | Alert | Critical | Error => {
                error!(target: "librdkafka", "{} {}", fac, log_message);
            }
            Warning => warn!(target: "librdkafka", "{} {}", fac, log_message),
            Notice => info!(target: "librdkafka", "{} {}", fac, log_message),
            Info => info!(target: "librdkafka", "{} {}", fac, log_message),
            Debug => debug!(target: "librdkafka", "{} {}", fac, log_message),
        }
    }
    // Refer to the comment on the `log` callback.
    fn error(&self, error: rdkafka::error::KafkaError, reason: &str) {
        error!("librdkafka: {}: {}", error, reason);
    }
}

// Implement `ConsumerContext` and `ProducerContext` for `MzClientContext`, so that it can be used
// in place of `DefaultProducerContext` and `DefaultConsumerContext`, but use tracing for logging.
// (These trait have a `: ClientContext` super-trait bound)
impl ConsumerContext for MzClientContext {}
impl ProducerContext for MzClientContext {
    type DeliveryOpaque = <DefaultProducerContext as ProducerContext>::DeliveryOpaque;
    fn delivery(
        &self,
        delivery_result: &DeliveryResult<'_>,
        delivery_opaque: Self::DeliveryOpaque,
    ) {
        DefaultProducerContext.delivery(delivery_result, delivery_opaque);
    }
}

/// Retrieve number of partitions for a given `topic` using the given `client`
pub fn get_partitions<C: ClientContext>(
    client: &Client<C>,
    topic: &str,
    timeout: Duration,
) -> Result<Vec<i32>, anyhow::Error> {
    let meta = client.fetch_metadata(Some(&topic), timeout)?;
    if meta.topics().len() != 1 {
        bail!(
            "topic {} has {} metadata entries; expected 1",
            topic,
            meta.topics().len()
        );
    }
    let meta_topic = meta.topics().into_element();
    if meta_topic.name() != topic {
        bail!(
            "got results for wrong topic {} (expected {})",
            meta_topic.name(),
            topic
        );
    }

    if meta_topic.partitions().len() == 0 {
        bail!("topic {} does not exist", topic);
    }

    Ok(meta_topic.partitions().iter().map(|x| x.id()).collect())
}

/// Creates a new `rdkafka` `ClientConfig`, configured correctly for
/// materialized and related services.
#[allow(clippy::disallowed_methods)]
pub fn create_new_client_config() -> rdkafka::config::ClientConfig {
    rdkafka::config::ClientConfig::new()
}

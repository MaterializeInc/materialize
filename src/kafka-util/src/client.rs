// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Helpers for working with Kafka's API

use rdkafka::{
    consumer::ConsumerContext,
    producer::{DefaultProducerContext, DeliveryResult, ProducerContext},
    ClientContext,
};
use tracing::{debug, error, info, warn};

/// A `ClientContext` implementation that correctly uses `tracing` instead of `log` macros
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

// Default implementation's of ConsumerContext and ProducerContext that uses the above log impl for
// the super traits
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

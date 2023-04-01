// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Helpers for working with Kafka's client API.

use std::collections::BTreeMap;
use std::error::Error;
use std::sync::Arc;
use std::time::Duration;

use anyhow::bail;
use mz_ore::collections::CollectionExt;
use rdkafka::client::{BrokerAddr, Client, NativeClient, OAuthToken};
use rdkafka::config::{ClientConfig, RDKafkaLogLevel};
use rdkafka::consumer::{ConsumerContext, Rebalance};
use rdkafka::error::{KafkaError, KafkaResult};
use rdkafka::producer::{DefaultProducerContext, DeliveryResult, ProducerContext};
use rdkafka::types::RDKafkaRespErr;
use rdkafka::util::Timeout;
use rdkafka::{ClientContext, Statistics, TopicPartitionList};
use tracing::{debug, error, info, warn, Level};

/// A `ClientContext` implementation that uses `tracing` instead of `log`
/// macros.
///
/// All code in Materialize that constructs Kafka clients should use this
/// context or a custom context that delegates the `log` and `error` methods to
/// this implementation.
#[derive(Clone)]
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

    fn error(&self, error: KafkaError, reason: &str) {
        // Refer to the comment in the `log` callback.
        error!(target: "librdkafka", "{}: {}", error, reason);
    }
}

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

/// Rewrites a broker address.
///
/// For use with [`BrokerRewritingClientContext`].
#[derive(Debug, Clone)]
pub struct BrokerRewrite {
    /// The rewritten hostname.
    pub host: String,
    /// The rewritten port.
    ///
    /// If unspecified, the broker's original port is left unchanged.
    pub port: Option<u16>,
}

/// A client context that supports rewriting broker addresses.
#[derive(Clone)]
pub struct BrokerRewritingClientContext<C> {
    inner: C,
    rewrites: BTreeMap<BrokerAddr, Arc<dyn Fn() -> BrokerRewrite + Send + Sync>>,
}

impl<C> BrokerRewritingClientContext<C> {
    /// Constructs a new context that wraps `inner`.
    pub fn new(inner: C) -> BrokerRewritingClientContext<C> {
        BrokerRewritingClientContext {
            inner,
            rewrites: BTreeMap::new(),
        }
    }

    /// Adds a broker rewrite rule.
    ///
    /// `rewrite` is a function that returns a `BrokerRewrite` that specifies
    /// how to rewrite the address for `broker`.
    ///
    /// The function is invoked by librdkafka on every connection attempt to the
    /// broker. This permits the rewrite to evolve over time, for example, if
    /// the rewrite is for a tunnel whose address changes if the tunnel fails
    /// and restarts.
    pub fn add_broker_rewrite<F>(&mut self, broker: BrokerAddr, rewrite: F)
    where
        F: Fn() -> BrokerRewrite + Send + Sync + 'static,
    {
        self.rewrites.insert(broker, Arc::new(rewrite));
    }

    /// Returns a reference to the wrapped context.
    pub fn inner(&self) -> &C {
        &self.inner
    }
}

impl<C> ClientContext for BrokerRewritingClientContext<C>
where
    C: ClientContext,
{
    const ENABLE_REFRESH_OAUTH_TOKEN: bool = C::ENABLE_REFRESH_OAUTH_TOKEN;

    fn rewrite_broker_addr(&self, addr: BrokerAddr) -> BrokerAddr {
        match self.rewrites.get(&addr) {
            None => addr,
            Some(rewrite) => {
                let rewrite = rewrite();
                let new_addr = BrokerAddr {
                    host: rewrite.host,
                    port: match rewrite.port {
                        None => addr.port.clone(),
                        Some(port) => port.to_string(),
                    },
                };
                info!(
                    "rewriting broker {}:{} to {}:{}",
                    addr.host, addr.port, new_addr.host, new_addr.port
                );
                new_addr
            }
        }
    }

    fn log(&self, level: RDKafkaLogLevel, fac: &str, log_message: &str) {
        self.inner.log(level, fac, log_message)
    }

    fn error(&self, error: KafkaError, reason: &str) {
        self.inner.error(error, reason)
    }

    fn stats(&self, statistics: Statistics) {
        self.inner.stats(statistics)
    }

    fn stats_raw(&self, statistics: &[u8]) {
        self.inner.stats_raw(statistics)
    }

    fn generate_oauth_token(
        &self,
        oauthbearer_config: Option<&str>,
    ) -> Result<OAuthToken, Box<dyn Error>> {
        self.inner.generate_oauth_token(oauthbearer_config)
    }
}

impl<C> ConsumerContext for BrokerRewritingClientContext<C>
where
    C: ConsumerContext,
{
    fn rebalance(
        &self,
        native_client: &NativeClient,
        err: RDKafkaRespErr,
        tpl: &mut TopicPartitionList,
    ) {
        self.inner.rebalance(native_client, err, tpl)
    }

    fn pre_rebalance<'a>(&self, rebalance: &Rebalance<'a>) {
        self.inner.pre_rebalance(rebalance)
    }

    fn post_rebalance<'a>(&self, rebalance: &Rebalance<'a>) {
        self.inner.post_rebalance(rebalance)
    }

    fn commit_callback(&self, result: KafkaResult<()>, offsets: &TopicPartitionList) {
        self.inner.commit_callback(result, offsets)
    }

    fn main_queue_min_poll_interval(&self) -> Timeout {
        self.inner.main_queue_min_poll_interval()
    }
}

impl<C> ProducerContext for BrokerRewritingClientContext<C>
where
    C: ProducerContext,
{
    type DeliveryOpaque = C::DeliveryOpaque;

    fn delivery(
        &self,
        delivery_result: &DeliveryResult<'_>,
        delivery_opaque: Self::DeliveryOpaque,
    ) {
        self.inner.delivery(delivery_result, delivery_opaque)
    }
}

/// Retrieve number of partitions for a given `topic` using the given `client`
pub fn get_partitions<C: ClientContext>(
    client: &Client<C>,
    topic: &str,
    timeout: Duration,
) -> Result<Vec<i32>, anyhow::Error> {
    let meta = client.fetch_metadata(Some(topic), timeout)?;
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

/// A simpler version of [`create_new_client_config`] that defaults
/// the `log_level` to `INFO` and should only be used in tests.
pub fn create_new_client_config_simple() -> ClientConfig {
    create_new_client_config(tracing::Level::INFO)
}

/// Build a new [`rdkafka`] [`ClientConfig`] with its `log_level` set correctly
/// based on the passed through [`tracing::Level`]. This level should be
/// determined for `target: "librdkafka"`.
pub fn create_new_client_config(tracing_level: Level) -> ClientConfig {
    #[allow(clippy::disallowed_methods)]
    let mut config = ClientConfig::new();

    let level = if tracing_level >= Level::DEBUG {
        RDKafkaLogLevel::Debug
    } else if tracing_level >= Level::INFO {
        RDKafkaLogLevel::Info
    } else if tracing_level >= Level::WARN {
        RDKafkaLogLevel::Warning
    } else {
        RDKafkaLogLevel::Error
    };
    // WARNING WARNING WARNING
    //
    // For whatever reason, if you change this `target` to something else, this
    // log line might break. I (guswynn) did some extensive investigation with
    // the tracing folks, and we learned that this edge case only happens with
    // 1. a different target
    // 2. only this file (so far as I can tell)
    // 3. only in certain subscriber combinations
    // 4. only if the `tracing-log` feature is on.
    //
    // Our conclusion was that one of our dependencies is doing something
    // problematic with `log`.
    //
    // For now, this works, and prints a nice log line exactly when we want it.
    //
    // TODO(guswynn): when we can remove `tracing-log`, remove this warning
    tracing::debug!(target: "librdkafka", level = ?level, "Determined log level for librdkafka");
    config.set_log_level(level);

    // Patch the librdkafka debug log system into the Rust `log` ecosystem. This
    // is a very simple integration at the moment; enabling `debug`-level logs
    // for the `librdkafka` target enables the full firehouse of librdkafka
    // debug logs. We may want to investigate finer-grained control.
    if tracing_level >= Level::DEBUG {
        tracing::debug!(target: "librdkafka", "Enabling debug logs for rdkafka");
        config.set("debug", "all");
    }

    config
}

// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Common client functionality.
//!
//! [`Client`] is the part shared by consumers, producers and admin clients: the
//! connections to the cluster, metadata and watermark lookups, and the user's
//! [`ClientContext`].

use std::error::Error;
use std::io;
use std::net::{SocketAddr, ToSocketAddrs};
use std::sync::Arc;

use kafka_protocol::messages::list_offsets_request::{ListOffsetsPartition, ListOffsetsTopic};
use kafka_protocol::messages::{BrokerId, ListOffsetsRequest, TopicName};
use kafka_protocol::protocol::StrBytes;
use tracing::{debug, error, info, warn};

use crate::config::settings::{IsolationLevel, SaslMechanism};
use crate::config::{ClientSettings, RDKafkaLogLevel};
use crate::error::{KafkaError, KafkaResult, RDKafkaError};
use crate::internal::cluster::{self, Cluster, MetadataTopics};
use crate::internal::hooks::{Hooks, Logger};
use crate::internal::net::RequestOptions;
use crate::internal::net::tls::TlsConfig;
use crate::internal::{protocol, runtime};
use crate::metadata::Metadata;
use crate::statistics::Statistics;
use crate::types::RDKafkaErrorCode;
use crate::util::{Deadline, Timeout};

/// Client-level context.
///
/// Each client (consumers and producers included) has a context object that
/// can be used to customize its behavior. Implementing `ClientContext` enables
/// the customization of methods common to all clients, while
/// [`ProducerContext`](crate::producer::ProducerContext) and
/// [`ConsumerContext`](crate::consumer::ConsumerContext) are specific to
/// producers and consumers.
///
/// # Threading
///
/// * [`log`](Self::log) (and `ProducerContext::delivery`, `ConsumerContext`
///   callbacks) are called inline from the client's I/O tasks. They must
///   return quickly and must not block, or they stall I/O for every client in
///   the process. They may be called concurrently from several threads.
/// * [`error`](Self::error) and [`stats`](Self::stats) are delivered like
///   rust-rdkafka delivers them from librdkafka's main queue: a consumer
///   delivers them on the thread that calls `BaseConsumer::poll` or polls a
///   `StreamConsumer` stream (not `PartitionQueue::poll`), so a consumer that
///   is never polled never receives them; producers deliver them promptly
///   from I/O tasks, as rust-rdkafka's polling thread would; admin clients
///   never deliver them. Errors of a consumer that is not polled are kept up
///   to a bound, and only its latest statistics. A producer's callbacks may
///   still run shortly after its last handle is dropped, since dropping does
///   not wait for the I/O tasks to stop.
/// * [`resolve_broker_addr`](Self::resolve_broker_addr) and
///   [`generate_oauth_token`](Self::generate_oauth_token) are called on a
///   blocking thread, where they may block, including with
///   `tokio::runtime::Handle::block_on` on another runtime.
pub trait ClientContext: Send + Sync + 'static {
    /// Whether to obtain SASL `OAUTHBEARER` tokens by calling
    /// [`ClientContext::generate_oauth_token`].
    ///
    /// If disabled, the client has no token source and `OAUTHBEARER`
    /// authentication fails.
    const ENABLE_REFRESH_OAUTH_TOKEN: bool = false;

    /// Receives log lines from the client.
    ///
    /// The default implementation forwards the log lines to `tracing` under
    /// the target `librdkafka`, mapping the error levels to `error!`.
    fn log(&self, level: RDKafkaLogLevel, fac: &str, log_message: &str) {
        match level {
            RDKafkaLogLevel::Emerg
            | RDKafkaLogLevel::Alert
            | RDKafkaLogLevel::Critical
            | RDKafkaLogLevel::Error => {
                error!(target: "librdkafka", "librdkafka: {} {}", fac, log_message)
            }
            RDKafkaLogLevel::Warning => {
                warn!(target: "librdkafka", "librdkafka: {} {}", fac, log_message)
            }
            RDKafkaLogLevel::Notice | RDKafkaLogLevel::Info => {
                info!(target: "librdkafka", "librdkafka: {} {}", fac, log_message)
            }
            RDKafkaLogLevel::Debug => {
                debug!(target: "librdkafka", "librdkafka: {} {}", fac, log_message)
            }
        }
    }

    /// Receives the statistics of the client, every `statistics.interval.ms`
    /// when that is non-zero.
    ///
    /// The default implementation logs the statistics at `info` level.
    fn stats(&self, statistics: Statistics) {
        info!("Client stats: {:?}", statistics);
    }

    /// Receives global errors from the client.
    ///
    /// The default implementation logs the error at `error` level.
    fn error(&self, error: KafkaError, reason: &str) {
        error!("librdkafka: {}: {}", error, reason);
    }

    /// Performs DNS resolution on a broker address.
    ///
    /// Called for every connection attempt to a broker, bootstrap and
    /// advertised ones alike, with the host and port as configured or
    /// advertised. TLS verification and SNI use the original host name, not
    /// the resolved addresses.
    ///
    /// The default implementation uses the system resolver.
    fn resolve_broker_addr(&self, host: &str, port: u16) -> Result<Vec<SocketAddr>, io::Error> {
        (host, port).to_socket_addrs().map(|addrs| addrs.collect())
    }

    /// Generates an OAuth token from the provided configuration.
    ///
    /// Only called when [`ClientContext::ENABLE_REFRESH_OAUTH_TOKEN`] is true
    /// and the client uses SASL `OAUTHBEARER`: when the client is created and
    /// then when 80% of the previous token's lifetime has passed. Errors are
    /// retried after 10 seconds.
    ///
    /// The default implementation always fails.
    fn generate_oauth_token(
        &self,
        _oauthbearer_config: Option<&str>,
    ) -> Result<OAuthToken, Box<dyn Error>> {
        Err("Default implementation of generate_oauth_token must be overridden".into())
    }
}

/// An empty [`ClientContext`] that can be used when no customizations are
/// needed.
#[derive(Clone, Debug, Default)]
pub struct DefaultClientContext;

impl ClientContext for DefaultClientContext {}

/// A low-level client shared by consumers, producers and admin clients.
///
/// Dropping the last handle to a client stops its background work.
pub struct Client<C: ClientContext + 'static = DefaultClientContext> {
    context: Arc<C>,
    cluster: Arc<Cluster>,
}

impl<C: ClientContext + 'static> Client<C> {
    /// Creates a client and starts connecting to the bootstrap servers.
    ///
    /// Fails with [`KafkaError::ClientCreation`] if the settings are unusable,
    /// for example if TLS material cannot be parsed, with librdkafka's texts.
    pub(crate) fn new(settings: ClientSettings, context: C) -> KafkaResult<Client<C>> {
        let context = Arc::new(context);
        let hooks: Arc<dyn Hooks> = Arc::<C>::clone(&context);
        let creation_logger = Logger::for_creation(Arc::clone(&hooks), &settings);
        creation_logger.log_conf_warnings(&settings);
        // Like librdkafka, build the TLS context once, so that malformed
        // certificates and keys fail client creation.
        let tls = if settings.security_protocol.uses_tls() {
            Some(
                TlsConfig::new(&settings.ssl, &creation_logger)
                    .map_err(KafkaError::ClientCreation)?,
            )
        } else {
            None
        };
        // rust-rdkafka enables librdkafka's SASL callback queue only with
        // `ENABLE_REFRESH_OAUTH_TOKEN`, and fails creation of an OAUTHBEARER
        // client without it.
        if settings.sasl.mechanism == Some(SaslMechanism::OAuthBearer) && !hooks.oauth_enabled() {
            return Err(KafkaError::OAuthConfig(RDKafkaError::new(
                RDKafkaErrorCode::NotConfigured,
                "No SASL mechanism using callbacks is configured",
            )));
        }
        let logger = Logger::new(Arc::clone(&hooks), &settings);
        let cluster = Cluster::new(Arc::new(settings), hooks, logger, tls);
        Ok(Client { context, cluster })
    }

    /// The shared cluster state used by the client engines.
    pub(crate) fn cluster(&self) -> &Arc<Cluster> {
        &self.cluster
    }

    /// Returns a reference to the context.
    pub fn context(&self) -> &Arc<C> {
        &self.context
    }

    /// Requests the metadata of one topic, or of all topics, from the cluster.
    ///
    /// Blocks until a broker answers or the timeout expires. A missing topic
    /// is reported in [`MetadataTopic::error`](crate::metadata::MetadataTopic::error),
    /// not as `Err`. Fails with [`KafkaError::MetadataFetch`] carrying
    /// `BrokerTransportFailure` if no broker was reachable within the timeout,
    /// or `OperationTimedOut` if the request timed out.
    pub fn fetch_metadata<T: Into<Timeout>>(
        &self,
        topic: Option<&str>,
        timeout: T,
    ) -> KafkaResult<Metadata> {
        let deadline = Deadline::new(timeout);
        let cluster = Arc::clone(&self.cluster);
        let topic = topic.map(str::to_string);
        runtime::block_on(async move {
            let names: Vec<String> = topic.into_iter().collect();
            let topics = if names.is_empty() {
                MetadataTopics::All
            } else {
                // Like `rd_kafka_topic_new`, a requested topic becomes known
                // and is refreshed periodically.
                cluster.add_known_topics(names.iter().map(String::as_str));
                MetadataTopics::Some(&names)
            };
            let (broker, response) = cluster
                .fetch_metadata(topics, deadline)
                .await
                .map_err(|e| KafkaError::MetadataFetch(e.code()))?;
            Ok(cluster::metadata_to_public(
                &response,
                broker.id(),
                broker.name(),
            ))
        })
    }

    /// Returns the low and high watermarks for a specific topic and partition.
    ///
    /// Blocks until the partition leader answers or the timeout expires. Under
    /// `isolation.level=read_committed` (the default) the high watermark is
    /// the last stable offset. Fails with [`KafkaError::MetadataFetch`].
    pub fn fetch_watermarks<T: Into<Timeout>>(
        &self,
        topic: &str,
        partition: i32,
        timeout: T,
    ) -> KafkaResult<(i64, i64)> {
        let deadline = Deadline::new(timeout);
        let cluster = Arc::clone(&self.cluster);
        let topic = topic.to_string();
        runtime::block_on(async move {
            watermarks(&cluster, &topic, partition, deadline)
                .await
                .map_err(KafkaError::MetadataFetch)
        })
    }
}

impl<C: ClientContext + 'static> Drop for Client<C> {
    fn drop(&mut self) {
        // The client is the last user handle: stop all background work.
        self.cluster.shutdown();
    }
}

/// Lists the earliest and latest offsets of a partition at its leader, with
/// the client's isolation level, retrying leader changes until the deadline.
async fn watermarks(
    cluster: &Cluster,
    topic: &str,
    partition: i32,
    deadline: Deadline,
) -> Result<(i64, i64), RDKafkaErrorCode> {
    let isolation_level: i8 = match cluster.settings().isolation_level {
        IsolationLevel::ReadUncommitted => 0,
        IsolationLevel::ReadCommitted => 1,
    };
    loop {
        let leader = cluster
            .leader(topic, partition, deadline)
            .await
            .map_err(|e| e.code())?;
        let list = |timestamp: i64| {
            let leader = Arc::clone(&leader);
            async move {
                let response = leader
                    .request::<ListOffsetsRequest, _>(
                        |version| {
                            let mut req = ListOffsetsRequest::default()
                                .with_replica_id(BrokerId(-1))
                                .with_topics(vec![
                                    ListOffsetsTopic::default()
                                        .with_name(TopicName(StrBytes::from_string(
                                            topic.to_string(),
                                        )))
                                        .with_partitions(vec![
                                            ListOffsetsPartition::default()
                                                .with_partition_index(partition)
                                                .with_timestamp(timestamp),
                                        ]),
                                ]);
                            if version >= 2 {
                                req.isolation_level = isolation_level;
                            }
                            req
                        },
                        RequestOptions::default(),
                    )
                    .await
                    .map_err(|e| e.code())?;
                let p = response
                    .topics
                    .iter()
                    .flat_map(|t| t.partitions.iter())
                    .find(|p| p.partition_index == partition)
                    .ok_or(RDKafkaErrorCode::BadMessage)?;
                match protocol::broker_error(p.error_code) {
                    Some(code) => Err(code),
                    None => Ok(p.offset),
                }
            }
        };
        let both = futures::future::try_join(list(-2), list(-1));
        let result = match deadline.instant() {
            Some(at) => tokio::time::timeout_at(at.into(), both)
                .await
                .unwrap_or(Err(RDKafkaErrorCode::OperationTimedOut)),
            None => both.await,
        };
        match result {
            Ok(offsets) => return Ok(offsets),
            Err(
                code @ (RDKafkaErrorCode::NotLeaderForPartition
                | RDKafkaErrorCode::LeaderNotAvailable
                | RDKafkaErrorCode::FencedLeaderEpoch
                | RDKafkaErrorCode::UnknownLeaderEpoch
                | RDKafkaErrorCode::BrokerTransportFailure
                | RDKafkaErrorCode::DestroyBroker),
            ) if !deadline.is_elapsed() => {
                // The cached leader is stale: refresh and retry.
                let topics = [topic.to_string()];
                if let Err(e) = cluster.refresh_metadata(&topics, deadline).await {
                    return Err(if deadline.is_elapsed() {
                        code
                    } else {
                        e.code()
                    });
                }
                tokio::time::sleep(cluster.settings().retry_backoff).await;
            }
            Err(code) => return Err(code),
        }
    }
}

/// A generated OAuth token and its associated metadata.
///
/// When using the `OAUTHBEARER` SASL authentication method, this type is
/// returned from [`ClientContext::generate_oauth_token`]. The token and
/// principal name must not contain embedded null characters.
///
/// Specifying SASL extensions is not currently supported.
pub struct OAuthToken {
    /// The token value to set.
    pub token: String,
    /// The Kafka principal name associated with the token.
    pub principal_name: String,
    /// When the token expires, in number of milliseconds since the Unix epoch.
    pub lifetime_ms: i64,
}

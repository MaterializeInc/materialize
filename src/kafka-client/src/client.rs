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

use tracing::{debug, error, info, warn};

use crate::config::{ClientSettings, RDKafkaLogLevel};
use crate::error::{KafkaError, KafkaResult};
use crate::internal::cluster::Cluster;
use crate::metadata::Metadata;
use crate::statistics::Statistics;
use crate::util::Timeout;

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
/// Callbacks run on the client's I/O threads, not on the threads that call the
/// client:
///
/// * [`log`](Self::log), [`error`](Self::error) and [`stats`](Self::stats)
///   (and `ProducerContext::delivery`, `ConsumerContext` callbacks) are called
///   inline from I/O tasks. They must return quickly and must not block, or
///   they stall I/O for every client in the process. They may be called
///   concurrently from several threads.
/// * [`resolve_broker_addr`](Self::resolve_broker_addr) and
///   [`generate_oauth_token`](Self::generate_oauth_token) are called on a
///   blocking thread, where they may block, including with
///   `tokio::runtime::Handle::block_on` on another runtime.
///
/// Unlike rust-rdkafka, `error` and `stats` do not depend on the application
/// polling the client.
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
        let _ = (settings, context);
        todo!("foundation: build the cluster (connections, metadata) from settings and context")
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
        let _ = (topic, timeout.into());
        todo!("foundation: Metadata request with the client's allow.auto.create.topics")
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
        let _ = (topic, partition, timeout.into());
        todo!("foundation: ListOffsets(EARLIEST, LATEST) to the leader with isolation.level")
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

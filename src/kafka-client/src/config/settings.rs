// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Typed client settings parsed from a [`ClientConfig`].
//!
//! Parsing follows librdkafka 2.12.1 (`rdkafka_conf.c`) in two phases:
//!
//! * Per property, like `rd_kafka_conf_set`: unknown names, malformed values
//!   and out-of-range values fail with [`KafkaError::ClientConfig`] carrying
//!   librdkafka's error text, the key and the value. Properties are applied in
//!   key order, so when two aliases of one property are set, the alias that
//!   sorts last wins.
//! * Across properties, like `rd_kafka_new`: dependent defaults are filled in
//!   and conflicts fail with [`KafkaError::ClientCreation`] carrying
//!   librdkafka's text, e.g. ``"`acks` must be set to `all` when
//!   `enable.idempotence` is true"``, or `Unsupported SASL mechanism: GARBAGE`.
//!
//! Every property Materialize, testdrive and CI set is typed here, along with
//! the common aliases. The properties in [`IGNORED_PROPERTIES`] are accepted
//! and have no effect. Everything else is rejected.
//!
//! Validation that needs the network layer (PEM and PKCS#12 parsing, key and
//! certificate matching) is not done here; SSL material is kept as raw strings
//! and paths.

use std::collections::BTreeMap;
use std::fmt;
use std::time::Duration;

use crate::config::{ClientConfig, RDKafkaLogLevel};
use crate::error::{KafkaError, KafkaResult};
use crate::types::RDKafkaConfRes;
use mz_ore::cast::CastLossy;

use crate::util::{duration_from_millis, duration_from_millis_f64};

/// The kind of client the settings are for.
///
/// Admin clients are producer-type clients in librdkafka; they share producer
/// defaults (for example `allow.auto.create.topics=true`) and warnings.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ClientKind {
    Consumer,
    Producer,
    Admin,
}

impl ClientKind {
    fn is_consumer(self) -> bool {
        matches!(self, ClientKind::Consumer)
    }
}

/// `security.protocol`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SecurityProtocol {
    Plaintext,
    Ssl,
    SaslPlaintext,
    SaslSsl,
}

impl SecurityProtocol {
    pub(crate) fn uses_tls(self) -> bool {
        matches!(self, SecurityProtocol::Ssl | SecurityProtocol::SaslSsl)
    }

    pub(crate) fn uses_sasl(self) -> bool {
        matches!(
            self,
            SecurityProtocol::SaslPlaintext | SecurityProtocol::SaslSsl
        )
    }

    /// The lowercase name librdkafka uses, e.g. in broker names
    /// (`sasl_ssl://host:port/id`).
    pub(crate) fn name(self) -> &'static str {
        match self {
            SecurityProtocol::Plaintext => "plaintext",
            SecurityProtocol::Ssl => "ssl",
            SecurityProtocol::SaslPlaintext => "sasl_plaintext",
            SecurityProtocol::SaslSsl => "sasl_ssl",
        }
    }
}

/// A supported SASL mechanism.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SaslMechanism {
    Plain,
    ScramSha256,
    ScramSha512,
    OAuthBearer,
}

impl SaslMechanism {
    /// The mechanism name sent in SaslHandshake.
    pub(crate) fn name(self) -> &'static str {
        match self {
            SaslMechanism::Plain => "PLAIN",
            SaslMechanism::ScramSha256 => "SCRAM-SHA-256",
            SaslMechanism::ScramSha512 => "SCRAM-SHA-512",
            SaslMechanism::OAuthBearer => "OAUTHBEARER",
        }
    }
}

/// `isolation.level`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum IsolationLevel {
    ReadUncommitted,
    ReadCommitted,
}

/// `auto.offset.reset`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum AutoOffsetReset {
    /// `smallest`, `earliest`, `beginning`.
    Earliest,
    /// `largest`, `latest`, `end`.
    Latest,
    /// `error`: raise a consumer error instead of resetting.
    Error,
}

/// One entry of `partition.assignment.strategy`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum AssignmentStrategy {
    Range,
    RoundRobin,
    CooperativeSticky,
}

impl AssignmentStrategy {
    /// The protocol name used in JoinGroup.
    pub(crate) fn name(self) -> &'static str {
        match self {
            AssignmentStrategy::Range => "range",
            AssignmentStrategy::RoundRobin => "roundrobin",
            AssignmentStrategy::CooperativeSticky => "cooperative-sticky",
        }
    }
}

/// `compression.codec` (`compression.type`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CompressionCodec {
    None,
    Gzip,
    Snappy,
    Lz4,
    Zstd,
}

/// `partitioner`: librdkafka's builtin partitioners.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum PartitionerKind {
    Random,
    Consistent,
    ConsistentRandom,
    Murmur2,
    Murmur2Random,
    Fnv1a,
    Fnv1aRandom,
}

/// A librdkafka `debug` context.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum DebugContext {
    Generic,
    Broker,
    Topic,
    Metadata,
    Feature,
    Queue,
    Msg,
    Protocol,
    Cgrp,
    Security,
    Fetch,
    Interceptor,
    Plugin,
    Consumer,
    Admin,
    Eos,
    Mock,
    Assignor,
    Conf,
    Telemetry,
}

const DEBUG_CONTEXT_NAMES: &[(&str, DebugContext)] = &[
    ("generic", DebugContext::Generic),
    ("broker", DebugContext::Broker),
    ("topic", DebugContext::Topic),
    ("metadata", DebugContext::Metadata),
    ("feature", DebugContext::Feature),
    ("queue", DebugContext::Queue),
    ("msg", DebugContext::Msg),
    ("protocol", DebugContext::Protocol),
    ("cgrp", DebugContext::Cgrp),
    ("security", DebugContext::Security),
    ("fetch", DebugContext::Fetch),
    ("interceptor", DebugContext::Interceptor),
    ("plugin", DebugContext::Plugin),
    ("consumer", DebugContext::Consumer),
    ("admin", DebugContext::Admin),
    ("eos", DebugContext::Eos),
    ("mock", DebugContext::Mock),
    ("assignor", DebugContext::Assignor),
    ("conf", DebugContext::Conf),
    ("telemetry", DebugContext::Telemetry),
];

/// The set of enabled `debug` contexts.
#[derive(Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct DebugContexts(u32);

impl DebugContexts {
    fn bit(ctx: DebugContext) -> u32 {
        let index = DEBUG_CONTEXT_NAMES
            .iter()
            .position(|(_, c)| *c == ctx)
            .expect("every context is named");
        1 << index
    }

    pub(crate) fn all() -> DebugContexts {
        DebugContexts(
            DEBUG_CONTEXT_NAMES
                .iter()
                .fold(0, |acc, (_, c)| acc | Self::bit(*c)),
        )
    }

    pub(crate) fn contains(self, ctx: DebugContext) -> bool {
        self.0 & Self::bit(ctx) != 0
    }

    pub(crate) fn is_empty(self) -> bool {
        self.0 == 0
    }

    fn insert(&mut self, ctx: DebugContext) {
        self.0 |= Self::bit(ctx);
    }

    fn remove(&mut self, ctx: DebugContext) {
        self.0 &= !Self::bit(ctx);
    }
}

impl fmt::Debug for DebugContexts {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_set()
            .entries(
                DEBUG_CONTEXT_NAMES
                    .iter()
                    .filter(|(_, c)| self.contains(*c))
                    .map(|(n, _)| n),
            )
            .finish()
    }
}

/// TLS settings. PEM strings and paths are unparsed.
#[derive(Clone, Default)]
pub(crate) struct SslSettings {
    pub ca_pem: Option<String>,
    pub ca_location: Option<String>,
    pub certificate_pem: Option<String>,
    pub certificate_location: Option<String>,
    pub key_pem: Option<String>,
    pub key_location: Option<String>,
    pub key_password: Option<String>,
    /// PKCS#12 file with key and certificate; requires `keystore_password`.
    pub keystore_location: Option<String>,
    pub keystore_password: Option<String>,
    pub crl_location: Option<String>,
    pub cipher_suites: Option<String>,
    pub curves_list: Option<String>,
    pub sigalgs_list: Option<String>,
    /// `ssl.endpoint.identification.algorithm=https`: verify that the broker
    /// certificate matches the broker host name (SAN, else CN).
    pub endpoint_identification: bool,
    /// `enable.ssl.certificate.verification`.
    pub enable_certificate_verification: bool,
}

impl fmt::Debug for SslSettings {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let redact = |v: &Option<String>| v.as_ref().map(|_| "[redacted]");
        f.debug_struct("SslSettings")
            .field("ca_pem", &self.ca_pem.as_ref().map(|_| "[set]"))
            .field("ca_location", &self.ca_location)
            .field(
                "certificate_pem",
                &self.certificate_pem.as_ref().map(|_| "[set]"),
            )
            .field("certificate_location", &self.certificate_location)
            .field("key_pem", &redact(&self.key_pem))
            .field("key_location", &self.key_location)
            .field("key_password", &redact(&self.key_password))
            .field("keystore_location", &self.keystore_location)
            .field("keystore_password", &redact(&self.keystore_password))
            .field("crl_location", &self.crl_location)
            .field("cipher_suites", &self.cipher_suites)
            .field("curves_list", &self.curves_list)
            .field("sigalgs_list", &self.sigalgs_list)
            .field("endpoint_identification", &self.endpoint_identification)
            .field(
                "enable_certificate_verification",
                &self.enable_certificate_verification,
            )
            .finish()
    }
}

/// SASL settings.
#[derive(Clone)]
pub(crate) struct SaslSettings {
    /// The selected mechanism. `Some` iff `security.protocol` uses SASL.
    pub mechanism: Option<SaslMechanism>,
    /// `sasl.username`. Required (possibly empty) for PLAIN and SCRAM.
    pub username: Option<String>,
    /// `sasl.password`. Required (possibly empty) for PLAIN and SCRAM.
    pub password: Option<String>,
    /// `sasl.oauthbearer.config`, passed to
    /// [`ClientContext::generate_oauth_token`](crate::ClientContext::generate_oauth_token).
    pub oauthbearer_config: Option<String>,
}

impl fmt::Debug for SaslSettings {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SaslSettings")
            .field("mechanism", &self.mechanism)
            .field("username", &self.username)
            .field("password", &self.password.as_ref().map(|_| "[redacted]"))
            .field(
                "oauthbearer_config",
                &self.oauthbearer_config.as_ref().map(|_| "[redacted]"),
            )
            .finish()
    }
}

/// Consumer settings. Parsed for every client kind, because some apply to
/// non-consumers too (`isolation.level` governs ListOffsets everywhere).
#[derive(Debug, Clone)]
pub(crate) struct ConsumerSettings {
    /// `group.id`. An empty value means no group.
    pub group_id: Option<String>,
    pub group_instance_id: Option<String>,
    /// In priority order. Validated: all eager or all cooperative.
    pub partition_assignment_strategy: Vec<AssignmentStrategy>,
    pub session_timeout: Duration,
    pub heartbeat_interval: Duration,
    pub max_poll_interval: Duration,
    pub enable_auto_commit: bool,
    pub auto_commit_interval: Duration,
    pub enable_auto_offset_store: bool,
    pub auto_offset_reset: AutoOffsetReset,
    pub enable_partition_eof: bool,
    pub check_crcs: bool,
    pub fetch_wait_max: Duration,
    pub fetch_min_bytes: i32,
    /// `fetch.max.bytes` after librdkafka's adjustment for consumers: unless
    /// set explicitly, `max(min(fetch.max.bytes, queued.max.messages.kbytes *
    /// 1024), message.max.bytes)`.
    pub fetch_max_bytes: i32,
    /// `fetch.message.max.bytes` (`max.partition.fetch.bytes`): the initial
    /// per-partition fetch size.
    pub fetch_message_max_bytes: i32,
    pub fetch_error_backoff: Duration,
    pub fetch_queue_backoff: Duration,
    /// Per-queue prefetch limit in messages.
    pub queued_min_messages: i32,
    /// Per-queue prefetch limit in kilobytes.
    ///
    /// NOTE: librdkafka's fetcher compares queue sizes against this value
    /// times 1000, not 1024 (`rdkafka.c`), while the `fetch.max.bytes`
    /// adjustment multiplies by 1024. Use [`Self::queued_max_bytes`] for the
    /// fetcher limit.
    pub queued_max_messages_kbytes: i32,
}

impl ConsumerSettings {
    /// The per-queue prefetch byte limit used by librdkafka's fetcher:
    /// `queued.max.messages.kbytes * 1000`.
    pub(crate) fn queued_max_bytes(&self) -> u64 {
        u64::try_from(self.queued_max_messages_kbytes).expect("validated positive") * 1000
    }
}

/// Producer settings. Parsed for every client kind.
#[derive(Debug, Clone)]
pub(crate) struct ProducerSettings {
    pub transactional_id: Option<String>,
    pub transaction_timeout: Duration,
    /// Effective idempotence: forced on by `transactional.id`. Always false
    /// for consumers.
    pub enable_idempotence: bool,
    /// `acks`: -1 (all), 0, or the number of acks. Forced to -1 by
    /// idempotence.
    pub acks: i16,
    pub request_timeout: Duration,
    /// `message.timeout.ms`; `None` for 0 (infinite). Defaults to
    /// `transaction.timeout.ms` for transactional producers.
    pub message_timeout: Option<Duration>,
    /// `linger.ms` (`queue.buffering.max.ms`), fractional milliseconds allowed.
    /// Lowered below `message.timeout.ms` unless set explicitly.
    pub linger: Duration,
    pub batch_size: i32,
    pub batch_num_messages: i32,
    /// `queue.buffering.max.messages`; `None` for 0 (unlimited).
    pub queue_buffering_max_messages: Option<u32>,
    /// `queue.buffering.max.kbytes * 1024`.
    pub queue_buffering_max_bytes: u64,
    pub compression: CompressionCodec,
    /// `compression.level`; `None` for -1 (codec default).
    pub compression_level: Option<i32>,
    pub partitioner: PartitionerKind,
    /// `retries` (`message.send.max.retries`). `i32::MAX` with idempotence.
    pub retries: i32,
    /// Defaults to `2 * linger`, capped at 900 s.
    pub sticky_partitioning_linger: Duration,
    pub delivery_report_only_error: bool,
}

/// Typed settings for one client, parsed from a [`ClientConfig`].
#[derive(Debug, Clone)]
pub(crate) struct ClientSettings {
    pub kind: ClientKind,
    /// The level below which (more verbose) log lines are dropped, from
    /// [`ClientConfig::log_level`].
    pub log_level: RDKafkaLogLevel,
    /// The level that gates log lines emitted during client creation, such as
    /// [`Self::warnings`]: the `log_level` property (default 6, `Info`), or
    /// `Debug` when `debug` is set. librdkafka applies
    /// `ClientConfig::log_level` only after creation.
    pub creation_log_level: RDKafkaLogLevel,
    pub debug: DebugContexts,
    pub client_id: String,
    /// `bootstrap.servers` (`metadata.broker.list`) entries, split on commas
    /// and whitespace, unparsed (`[proto://]host[:port]`).
    pub bootstrap_servers: Vec<String>,
    pub message_max_bytes: i32,
    /// For consumers, raised to at least `fetch.max.bytes + 512` unless set.
    pub receive_message_max_bytes: i32,
    /// Capped to 5 by idempotence unless set.
    pub max_in_flight: i32,
    /// `topic.metadata.refresh.interval.ms`; `None` for -1 (no periodic
    /// refresh).
    pub metadata_refresh_interval: Option<Duration>,
    /// Defaults to `retry.backoff.ms`.
    pub metadata_refresh_fast_interval: Duration,
    pub metadata_propagation_max: Duration,
    /// Defaults to false for consumers and true for producers and admin
    /// clients, as in librdkafka.
    pub allow_auto_create_topics: bool,
    /// Per-request timeout. For transactional producers defaults to
    /// `max(transaction.timeout.ms - 100, 900)` ms.
    pub socket_timeout: Duration,
    pub socket_connection_setup_timeout: Duration,
    pub socket_keepalive: bool,
    pub socket_nagle_disable: bool,
    /// `None` for 0 (system default).
    pub socket_send_buffer_bytes: Option<u32>,
    /// `None` for 0 (system default).
    pub socket_receive_buffer_bytes: Option<u32>,
    /// `None` for 0 (never close idle connections).
    pub connections_max_idle: Option<Duration>,
    pub reconnect_backoff: Duration,
    pub reconnect_backoff_max: Duration,
    pub retry_backoff: Duration,
    pub retry_backoff_max: Duration,
    /// `None` for 0 (statistics disabled).
    pub statistics_interval: Option<Duration>,
    pub log_connection_close: bool,
    pub api_version_request_timeout: Duration,
    pub security_protocol: SecurityProtocol,
    pub ssl: SslSettings,
    pub sasl: SaslSettings,
    /// Applies to Fetch and to every ListOffsets (watermarks,
    /// `offsets_for_times`) for all client kinds.
    pub isolation_level: IsolationLevel,
    pub consumer: ConsumerSettings,
    pub producer: ProducerSettings,
    /// librdkafka's `CONFWARN` lines, to be logged with facility `CONFWARN`
    /// when the client is created, gated by [`Self::creation_log_level`].
    pub warnings: Vec<(RDKafkaLogLevel, String)>,
}

/// librdkafka properties that are accepted and have no effect.
///
/// They tune librdkafka internals with no counterpart in this client (logging
/// queues, buffer copy thresholds, telemetry, metadata recovery), or select
/// features that are unavailable regardless (Kerberos). Accepting them keeps
/// configurations written for librdkafka working; their values are not
/// validated.
pub(crate) const IGNORED_PROPERTIES: &[&str] = &[
    "builtin.features",
    "client.dns.lookup",
    "client.software.name",
    "client.software.version",
    "consume.callback.max.messages",
    "enable.metrics.push",
    "enable.random.seed",
    "internal.termination.signal",
    "log.queue",
    "log.thread.name",
    "message.copy.max.bytes",
    "metadata.recovery.rebootstrap.trigger.ms",
    "metadata.recovery.strategy",
    "offset.store.method",
    "queue.buffering.backpressure.threshold",
    "queuing.strategy",
    "sasl.kerberos.keytab",
    "sasl.kerberos.kinit.cmd",
    "sasl.kerberos.min.time.before.relogin",
    "sasl.kerberos.principal",
    "sasl.kerberos.service.name",
    "socket.max.fails",
    "ssl.ca.certificate.stores",
    "topic.blacklist",
    "topic.metadata.refresh.fast.cnt",
    "topic.metadata.refresh.sparse",
];

/// Properties whose values must not appear in logs or `Debug` output.
const SENSITIVE_PROPERTIES: &[&str] = &[
    "sasl.password",
    "sasl.oauthbearer.config",
    "ssl.key.password",
    "ssl.key.pem",
    "ssl.keystore.password",
];

/// Reports whether `key` names a property whose value is secret.
pub(crate) fn is_sensitive(key: &str) -> bool {
    let key = key.strip_prefix("topic.").unwrap_or(key);
    SENSITIVE_PROPERTIES.contains(&key)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Scope {
    Global,
    Consumer,
    Producer,
}

enum PropType {
    /// A string, left-trimmed of whitespace.
    Str,
    /// A string without `,` or ` ` (librdkafka's `validate_single`).
    SingleStr,
    /// One of librdkafka's builtin partitioner names.
    Partitioner,
    Bool,
    /// An integer with an inclusive range and optional symbolic names.
    Int {
        min: i32,
        max: i32,
        names: &'static [(&'static str, i32)],
    },
    Float {
        min: f64,
        max: f64,
    },
    /// One of the names, case-insensitive.
    Enum(&'static [&'static str]),
    /// `debug` contexts: comma-separated, case-insensitive, with optional
    /// `+`/`-` prefixes.
    DebugFlags,
}

struct Property {
    name: &'static str,
    scope: Scope,
    /// Whether librdkafka defines this on the topic configuration, which can
    /// also be addressed with a `topic.` prefix.
    topic_level: bool,
    ty: PropType,
}

const fn prop(name: &'static str, scope: Scope, ty: PropType) -> Property {
    Property {
        name,
        scope,
        topic_level: false,
        ty,
    }
}

const fn topic_prop(name: &'static str, scope: Scope, ty: PropType) -> Property {
    Property {
        name,
        scope,
        topic_level: true,
        ty,
    }
}

const fn int(min: i32, max: i32) -> PropType {
    PropType::Int {
        min,
        max,
        names: &[],
    }
}

use PropType::{Bool, Str};
use Scope::{Consumer as C, Global as G, Producer as P};

/// The typed properties, in librdkafka's table order (which determines the
/// order of `CONFWARN` lines).
///
/// NOTE: Not all of them take effect. The crate docs list those that are
/// only validated.
const PROPERTIES: &[Property] = &[
    prop("client.id", G, Str),
    prop("metadata.broker.list", G, Str),
    prop("message.max.bytes", G, int(1000, 1_000_000_000)),
    prop("receive.message.max.bytes", G, int(1000, i32::MAX)),
    prop(
        "max.in.flight.requests.per.connection",
        G,
        int(1, 1_000_000),
    ),
    prop("topic.metadata.refresh.interval.ms", G, int(-1, 3_600_000)),
    prop("metadata.max.age.ms", G, int(1, 86_400_000)),
    prop("topic.metadata.refresh.fast.interval.ms", G, int(1, 60_000)),
    prop("topic.metadata.propagation.max.ms", G, int(0, 3_600_000)),
    prop("debug", G, PropType::DebugFlags),
    prop("socket.timeout.ms", G, int(10, 300_000)),
    prop("socket.send.buffer.bytes", G, int(0, 100_000_000)),
    prop("socket.receive.buffer.bytes", G, int(0, 100_000_000)),
    prop("socket.keepalive.enable", G, Bool),
    prop("socket.nagle.disable", G, Bool),
    prop("broker.address.ttl", G, int(0, 86_400_000)),
    prop(
        "broker.address.family",
        G,
        PropType::Enum(&["any", "v4", "v6"]),
    ),
    prop("socket.connection.setup.timeout.ms", G, int(1000, i32::MAX)),
    prop("connections.max.idle.ms", G, int(0, i32::MAX)),
    prop("reconnect.backoff.ms", G, int(0, 3_600_000)),
    prop("reconnect.backoff.max.ms", G, int(0, 3_600_000)),
    prop("statistics.interval.ms", G, int(0, 86_400_000)),
    prop("log_level", G, int(0, 7)),
    prop("log.connection.close", G, Bool),
    prop("api.version.request", G, Bool),
    prop("api.version.request.timeout.ms", G, int(1, 300_000)),
    prop("allow.auto.create.topics", G, Bool),
    prop(
        "security.protocol",
        G,
        PropType::Enum(&["plaintext", "ssl", "sasl_plaintext", "sasl_ssl"]),
    ),
    prop("ssl.cipher.suites", G, Str),
    prop("ssl.curves.list", G, Str),
    prop("ssl.sigalgs.list", G, Str),
    prop("ssl.key.location", G, Str),
    prop("ssl.key.password", G, Str),
    prop("ssl.key.pem", G, Str),
    prop("ssl.certificate.location", G, Str),
    prop("ssl.certificate.pem", G, Str),
    prop("ssl.ca.location", G, Str),
    prop("ssl.ca.pem", G, Str),
    prop("ssl.crl.location", G, Str),
    prop("ssl.keystore.location", G, Str),
    prop("ssl.keystore.password", G, Str),
    prop("enable.ssl.certificate.verification", G, Bool),
    prop(
        "ssl.endpoint.identification.algorithm",
        G,
        PropType::Enum(&["none", "https"]),
    ),
    prop("sasl.mechanisms", G, PropType::SingleStr),
    prop("sasl.username", G, Str),
    prop("sasl.password", G, Str),
    prop("sasl.oauthbearer.config", G, Str),
    prop("group.id", C, Str),
    prop("group.instance.id", C, Str),
    prop("partition.assignment.strategy", C, Str),
    prop("session.timeout.ms", C, int(1, 3_600_000)),
    prop("heartbeat.interval.ms", C, int(1, 3_600_000)),
    prop("max.poll.interval.ms", C, int(1, 86_400_000)),
    prop("enable.auto.commit", C, Bool),
    prop("auto.commit.interval.ms", C, int(0, 86_400_000)),
    prop("enable.auto.offset.store", C, Bool),
    prop("queued.min.messages", C, int(1, 10_000_000)),
    prop("queued.max.messages.kbytes", C, int(1, 2_097_151)),
    prop("fetch.wait.max.ms", C, int(0, 300_000)),
    prop("fetch.queue.backoff.ms", C, int(0, 300_000)),
    prop("fetch.message.max.bytes", C, int(1, 1_000_000_000)),
    prop("fetch.max.bytes", C, int(0, 2_147_483_135)),
    prop("fetch.min.bytes", C, int(1, 100_000_000)),
    prop("fetch.error.backoff.ms", C, int(0, 300_000)),
    prop(
        "isolation.level",
        C,
        PropType::Enum(&["read_uncommitted", "read_committed"]),
    ),
    prop("enable.partition.eof", C, Bool),
    prop("check.crcs", C, Bool),
    prop("client.rack", G, Str),
    prop("transactional.id", P, Str),
    prop("transaction.timeout.ms", P, int(1000, i32::MAX)),
    prop("enable.idempotence", P, Bool),
    prop("queue.buffering.max.messages", P, int(0, i32::MAX)),
    prop("queue.buffering.max.kbytes", P, int(1, i32::MAX)),
    prop(
        "queue.buffering.max.ms",
        P,
        PropType::Float {
            min: 0.0,
            max: 900_000.0,
        },
    ),
    prop("message.send.max.retries", P, int(0, i32::MAX)),
    prop("retry.backoff.ms", G, int(1, 300_000)),
    prop("retry.backoff.max.ms", G, int(1, 300_000)),
    prop(
        "compression.codec",
        P,
        PropType::Enum(&["none", "gzip", "snappy", "lz4", "zstd"]),
    ),
    prop("batch.num.messages", P, int(1, 1_000_000)),
    prop("batch.size", P, int(1, i32::MAX)),
    prop("delivery.report.only.error", P, Bool),
    prop("sticky.partitioning.linger.ms", P, int(0, 900_000)),
    topic_prop(
        "request.required.acks",
        P,
        PropType::Int {
            min: -1,
            max: 1000,
            names: &[("all", -1)],
        },
    ),
    topic_prop("request.timeout.ms", P, int(1, 900_000)),
    topic_prop("message.timeout.ms", P, int(0, i32::MAX)),
    topic_prop("partitioner", P, PropType::Partitioner),
    topic_prop("compression.level", P, int(-1, 12)),
    topic_prop(
        "auto.offset.reset",
        C,
        PropType::Enum(&[
            "smallest",
            "earliest",
            "beginning",
            "largest",
            "latest",
            "end",
            "error",
        ]),
    ),
];

/// librdkafka's property aliases: alias name, canonical name. Errors name the
/// canonical property, as in librdkafka.
const ALIASES: &[(&str, &str)] = &[
    ("bootstrap.servers", "metadata.broker.list"),
    ("max.in.flight", "max.in.flight.requests.per.connection"),
    ("sasl.mechanism", "sasl.mechanisms"),
    ("max.partition.fetch.bytes", "fetch.message.max.bytes"),
    ("linger.ms", "queue.buffering.max.ms"),
    ("retries", "message.send.max.retries"),
    ("compression.type", "compression.codec"),
    ("acks", "request.required.acks"),
    ("delivery.timeout.ms", "message.timeout.ms"),
];

const PARTITIONERS: &[(&str, PartitionerKind)] = &[
    ("random", PartitionerKind::Random),
    ("consistent", PartitionerKind::Consistent),
    ("consistent_random", PartitionerKind::ConsistentRandom),
    ("murmur2", PartitionerKind::Murmur2),
    ("murmur2_random", PartitionerKind::Murmur2Random),
    ("fnv1a", PartitionerKind::Fnv1a),
    ("fnv1a_random", PartitionerKind::Fnv1aRandom),
];

enum Lookup {
    Property(&'static Property),
    Ignored,
    /// Unknown; carries the name librdkafka reports.
    Unknown(String),
}

fn find_property(name: &str) -> Option<&'static Property> {
    let name = ALIASES
        .iter()
        .find(|(alias, _)| *alias == name)
        .map_or(name, |(_, canonical)| *canonical);
    PROPERTIES.iter().find(|p| p.name == name)
}

/// Resolves a property name like `rd_kafka_conf_set`: global properties
/// first, then topic properties, which may carry a `topic.` prefix.
fn lookup(key: &str) -> Lookup {
    if let Some(p) = find_property(key) {
        return Lookup::Property(p);
    }
    if IGNORED_PROPERTIES.contains(&key) {
        return Lookup::Ignored;
    }
    let topic_key = key.strip_prefix("topic.").unwrap_or(key);
    match find_property(topic_key) {
        Some(p) if p.topic_level => Lookup::Property(p),
        _ if IGNORED_PROPERTIES.contains(&topic_key) => Lookup::Ignored,
        _ => Lookup::Unknown(topic_key.to_string()),
    }
}

#[derive(Debug, Clone, PartialEq)]
enum Value {
    Str(String),
    Bool(bool),
    Int(i32),
    Float(f64),
    Enum(&'static str),
    Flags(DebugContexts),
}

fn is_c_space(c: char) -> bool {
    matches!(c, ' ' | '\t' | '\n' | '\x0b' | '\x0c' | '\r')
}

fn left_trim(s: &str) -> &str {
    s.trim_start_matches(is_c_space)
}

/// Parses `value` for `prop` like `rd_kafka_anyconf_set_prop`. `Ok(None)`
/// means the value is a no-op (an empty enum or flag value).
fn parse_value(prop: &Property, value: &str) -> Result<Option<Value>, String> {
    let name = prop.name;
    match &prop.ty {
        PropType::Str => Ok(Some(Value::Str(left_trim(value).to_string()))),
        PropType::SingleStr => {
            let v = left_trim(value);
            if v.contains(',') || v.contains(' ') {
                return Err(format!(
                    "Invalid value for configuration property \"{name}\": {v}"
                ));
            }
            Ok(Some(Value::Str(v.to_string())))
        }
        PropType::Partitioner => {
            let v = left_trim(value);
            if !PARTITIONERS.iter().any(|(p, _)| *p == v) {
                return Err(format!(
                    "Invalid value for configuration property \"{name}\": {v}"
                ));
            }
            Ok(Some(Value::Str(v.to_string())))
        }
        PropType::Bool => {
            if value.eq_ignore_ascii_case("true") || value.eq_ignore_ascii_case("t") || value == "1"
            {
                Ok(Some(Value::Bool(true)))
            } else if value.eq_ignore_ascii_case("false")
                || value.eq_ignore_ascii_case("f")
                || value == "0"
            {
                Ok(Some(Value::Bool(false)))
            } else {
                Err(format!("Expected bool value for \"{name}\": true or false"))
            }
        }
        PropType::Int { min, max, names } => {
            let ival = match c_strtol_int(value) {
                Some(v) => v,
                None => names
                    .iter()
                    .find(|(n, _)| n.eq_ignore_ascii_case(value))
                    .map(|(_, v)| *v)
                    .ok_or_else(|| {
                        format!("Invalid value for configuration property \"{name}\"")
                    })?,
            };
            if ival < *min || ival > *max {
                return Err(format!(
                    "Configuration property \"{name}\" value {ival} is outside allowed range \
                     {min}..{max}\n"
                ));
            }
            Ok(Some(Value::Int(ival)))
        }
        PropType::Float { min, max } => {
            let dval = c_strtod(value)
                .ok_or_else(|| format!("Invalid value for configuration property \"{name}\""))?;
            if dval < *min || dval > *max {
                return Err(format!(
                    "Configuration property \"{name}\" value {} is outside allowed range {}..{}\n",
                    fmt_g(dval),
                    fmt_g(*min),
                    fmt_g(*max)
                ));
            }
            Ok(Some(Value::Float(dval)))
        }
        PropType::Enum(names) => {
            let v = left_trim(value);
            if v.is_empty() {
                return Ok(None);
            }
            match names.iter().find(|n| n.eq_ignore_ascii_case(v)) {
                Some(n) => Ok(Some(Value::Enum(n))),
                None => Err(format!(
                    "Invalid value \"{v}\" for configuration property \"{name}\""
                )),
            }
        }
        PropType::DebugFlags => {
            let mut flags = DebugContexts::default();
            let mut any = false;
            for token in value.split(',') {
                let token = left_trim(token);
                let (add, token) = match token.strip_prefix('+') {
                    Some(t) => (true, t),
                    None => match token.strip_prefix('-') {
                        Some(t) => (false, t),
                        None => (true, token),
                    },
                };
                if token.is_empty() {
                    continue;
                }
                let contexts = if token.eq_ignore_ascii_case("all") {
                    DebugContexts::all()
                } else {
                    match DEBUG_CONTEXT_NAMES
                        .iter()
                        .find(|(n, _)| n.eq_ignore_ascii_case(token))
                    {
                        Some((_, ctx)) => {
                            let mut c = DebugContexts::default();
                            c.insert(*ctx);
                            c
                        }
                        None => {
                            return Err(format!(
                                "Invalid value \"{token}\" for configuration property \"{name}\""
                            ));
                        }
                    }
                };
                for (_, ctx) in DEBUG_CONTEXT_NAMES {
                    if contexts.contains(*ctx) {
                        if add {
                            flags.insert(*ctx);
                        } else {
                            flags.remove(*ctx);
                        }
                    }
                }
                any = true;
            }
            Ok(any.then_some(Value::Flags(flags)))
        }
    }
}

/// C `strtol(s, &end, 0)` followed by a cast to `int`: leading whitespace,
/// optional sign, `0x` hex or `0` octal prefixes, trailing garbage ignored,
/// overflow clamped to `i64` then truncated to 32 bits. `None` if no digits
/// were consumed.
fn c_strtol_int(s: &str) -> Option<i32> {
    let b = s.as_bytes();
    let mut i = b.iter().take_while(|c| is_c_space(char::from(**c))).count();
    let mut negative = false;
    if let Some(sign @ (b'+' | b'-')) = b.get(i) {
        negative = *sign == b'-';
        i += 1;
    }
    let radix = if b.get(i) == Some(&b'0')
        && matches!(b.get(i + 1), Some(b'x' | b'X'))
        && b.get(i + 2).is_some_and(|c| c.is_ascii_hexdigit())
    {
        i += 2;
        16
    } else if b.get(i) == Some(&b'0') {
        8
    } else {
        10
    };
    let mut value: i128 = 0;
    let mut digits = 0;
    while let Some(d) = b.get(i).and_then(|c| char::from(*c).to_digit(radix)) {
        value = (value * i128::from(radix) + i128::from(d)).min(i128::from(i64::MAX) + 1);
        digits += 1;
        i += 1;
    }
    if digits == 0 {
        return None;
    }
    let value = if negative { -value } else { value };
    let clamped = i64::try_from(value.clamp(i128::from(i64::MIN), i128::from(i64::MAX)))
        .expect("clamped to i64");
    let low = u32::try_from(clamped & 0xffff_ffff).expect("masked to 32 bits");
    Some(i32::from_ne_bytes(low.to_ne_bytes()))
}

/// C `strtod`: the longest prefix of the form
/// `[space][sign]digits[.digits][(e|E)[sign]digits]`. `None` if it has no
/// digits.
fn c_strtod(s: &str) -> Option<f64> {
    let t = left_trim(s);
    let b = t.as_bytes();
    let mut i = 0;
    if matches!(b.first(), Some(b'+' | b'-')) {
        i += 1;
    }
    let int_digits = b[i..].iter().take_while(|c| c.is_ascii_digit()).count();
    i += int_digits;
    let mut frac_digits = 0;
    if b.get(i) == Some(&b'.') {
        frac_digits = b[i + 1..].iter().take_while(|c| c.is_ascii_digit()).count();
        if int_digits + frac_digits > 0 {
            i += 1 + frac_digits;
        }
    }
    if int_digits + frac_digits == 0 {
        return None;
    }
    if matches!(b.get(i), Some(b'e' | b'E')) {
        let mut j = i + 1;
        if matches!(b.get(j), Some(b'+' | b'-')) {
            j += 1;
        }
        let exp_digits = b[j..].iter().take_while(|c| c.is_ascii_digit()).count();
        if exp_digits > 0 {
            i = j + exp_digits;
        }
    }
    t[..i].parse().ok()
}

/// C `printf("%g")`: six significant digits, scientific notation for
/// exponents below -4 or above 5, trailing zeros removed.
fn fmt_g(v: f64) -> String {
    if v == 0.0 {
        return "0".to_string();
    }
    // `strtod` overflows to infinity, e.g. for "1e999".
    if v.is_nan() {
        return "nan".to_string();
    }
    if v.is_infinite() {
        return if v > 0.0 { "inf" } else { "-inf" }.to_string();
    }
    let sci = format!("{v:.5e}");
    let (mantissa, exp) = sci.split_once('e').expect("formatted with exponent");
    let exp: i32 = exp.parse().expect("valid exponent");
    let strip = |s: &str| -> String {
        if s.contains('.') {
            s.trim_end_matches('0').trim_end_matches('.').to_string()
        } else {
            s.to_string()
        }
    };
    if exp < -4 || exp >= 6 {
        let sign = if exp < 0 { '-' } else { '+' };
        format!("{}e{}{:02}", strip(mantissa), sign, exp.abs())
    } else {
        let decimals = usize::try_from(5 - exp).expect("exp <= 5");
        strip(&format!("{v:.decimals$}"))
    }
}

/// The values set in a configuration, by canonical property name.
struct SetValues(BTreeMap<&'static str, Value>);

impl SetValues {
    fn is_set(&self, name: &str) -> bool {
        self.0.contains_key(name)
    }

    fn str(&self, name: &str) -> Option<&str> {
        match self.0.get(name) {
            Some(Value::Str(s)) => Some(s.as_str()),
            None => None,
            Some(v) => panic!("property {name} is not a string: {v:?}"),
        }
    }

    fn string(&self, name: &str) -> Option<String> {
        self.str(name).map(str::to_string)
    }

    fn bool(&self, name: &str, default: bool) -> bool {
        match self.0.get(name) {
            Some(Value::Bool(b)) => *b,
            None => default,
            Some(v) => panic!("property {name} is not a bool: {v:?}"),
        }
    }

    fn int(&self, name: &str, default: i32) -> i32 {
        match self.0.get(name) {
            Some(Value::Int(i)) => *i,
            None => default,
            Some(v) => panic!("property {name} is not an integer: {v:?}"),
        }
    }

    fn float(&self, name: &str, default: f64) -> f64 {
        match self.0.get(name) {
            Some(Value::Float(f)) => *f,
            None => default,
            Some(v) => panic!("property {name} is not a float: {v:?}"),
        }
    }

    fn enumeration(&self, name: &str, default: &'static str) -> &'static str {
        match self.0.get(name) {
            Some(Value::Enum(e)) => *e,
            None => default,
            Some(v) => panic!("property {name} is not an enum: {v:?}"),
        }
    }

    fn flags(&self, name: &str) -> DebugContexts {
        match self.0.get(name) {
            Some(Value::Flags(f)) => *f,
            None => DebugContexts::default(),
            Some(v) => panic!("property {name} is not a flag set: {v:?}"),
        }
    }
}

fn millis(ms: i32) -> Duration {
    duration_from_millis(i64::from(ms))
}

fn log_level_from_int(level: i32) -> RDKafkaLogLevel {
    match level {
        0 => RDKafkaLogLevel::Emerg,
        1 => RDKafkaLogLevel::Alert,
        2 => RDKafkaLogLevel::Critical,
        3 => RDKafkaLogLevel::Error,
        4 => RDKafkaLogLevel::Warning,
        5 => RDKafkaLogLevel::Notice,
        6 => RDKafkaLogLevel::Info,
        _ => RDKafkaLogLevel::Debug,
    }
}

fn creation_error(msg: impl Into<String>) -> KafkaError {
    KafkaError::ClientCreation(msg.into())
}

/// librdkafka's limit on in-flight requests per connection with idempotence.
const IDEMPOTENCE_MAX_IN_FLIGHT: i32 = 5;

impl ClientSettings {
    /// Parses and validates `config` for a client of the given kind.
    ///
    /// Fails with [`KafkaError::ClientConfig`] for an unknown property or an
    /// invalid value, and with [`KafkaError::ClientCreation`] for conflicting
    /// properties or an unusable SASL setup, with librdkafka's texts.
    pub(crate) fn from_config(config: &ClientConfig, kind: ClientKind) -> KafkaResult<Self> {
        let mut values = BTreeMap::new();
        for (key, value) in config.iter() {
            let prop = match lookup(key) {
                Lookup::Property(p) => p,
                Lookup::Ignored => continue,
                Lookup::Unknown(name) => {
                    return Err(KafkaError::ClientConfig(
                        RDKafkaConfRes::RD_KAFKA_CONF_UNKNOWN,
                        format!("No such configuration property: \"{name}\""),
                        key.to_string(),
                        value.to_string(),
                    ));
                }
            };
            match parse_value(prop, value) {
                Ok(Some(v)) => {
                    values.insert(prop.name, v);
                }
                Ok(None) => {}
                Err(errstr) => {
                    return Err(KafkaError::ClientConfig(
                        RDKafkaConfRes::RD_KAFKA_CONF_INVALID,
                        errstr,
                        key.to_string(),
                        value.to_string(),
                    ));
                }
            }
        }
        Self::finalize(&SetValues(values), config.log_level, kind)
    }

    /// Applies defaults and cross-property rules, like librdkafka's
    /// `rd_kafka_conf_finalize`, `rd_kafka_topic_conf_finalize`, assignor
    /// setup and SASL provider selection in `rd_kafka_new`, in that order.
    fn finalize(v: &SetValues, log_level: RDKafkaLogLevel, kind: ClientKind) -> KafkaResult<Self> {
        let is_consumer = kind.is_consumer();

        let ssl = SslSettings {
            ca_pem: v.string("ssl.ca.pem"),
            ca_location: v.string("ssl.ca.location"),
            certificate_pem: v.string("ssl.certificate.pem"),
            certificate_location: v.string("ssl.certificate.location"),
            key_pem: v.string("ssl.key.pem"),
            key_location: v.string("ssl.key.location"),
            key_password: v.string("ssl.key.password"),
            keystore_location: v.string("ssl.keystore.location"),
            keystore_password: v.string("ssl.keystore.password"),
            crl_location: v.string("ssl.crl.location"),
            cipher_suites: v.string("ssl.cipher.suites"),
            curves_list: v.string("ssl.curves.list"),
            sigalgs_list: v.string("ssl.sigalgs.list"),
            endpoint_identification: v
                .enumeration("ssl.endpoint.identification.algorithm", "https")
                == "https",
            enable_certificate_verification: v.bool("enable.ssl.certificate.verification", true),
        };
        if ssl.keystore_location.is_some() && ssl.keystore_password.is_none() {
            return Err(creation_error(
                "`ssl.keystore.password` is mandatory when `ssl.keystore.location` is set",
            ));
        }

        let message_max_bytes = v.int("message.max.bytes", 1_000_000);
        let mut receive_message_max_bytes = v.int("receive.message.max.bytes", 100_000_000);
        let mut socket_timeout_ms = v.int("socket.timeout.ms", 60_000);
        let mut max_in_flight = v.int("max.in.flight.requests.per.connection", 1_000_000);

        // Consumer settings.
        let session_timeout_ms = v.int("session.timeout.ms", 45_000);
        let max_poll_interval_ms = v.int("max.poll.interval.ms", 300_000);
        let queued_max_messages_kbytes = v.int("queued.max.messages.kbytes", 65_536);
        let mut fetch_max_bytes = v.int("fetch.max.bytes", 52_428_800);
        if is_consumer {
            if max_poll_interval_ms < session_timeout_ms {
                return Err(creation_error(
                    "`max.poll.interval.ms`must be >= `session.timeout.ms`",
                ));
            }
            if v.is_set("fetch.max.bytes") {
                if fetch_max_bytes < message_max_bytes {
                    return Err(creation_error(
                        "`fetch.max.bytes` must be >= `message.max.bytes`",
                    ));
                }
            } else {
                let queued_bytes = i64::from(queued_max_messages_kbytes) * 1024;
                let adjusted = i64::from(fetch_max_bytes)
                    .min(queued_bytes)
                    .max(i64::from(message_max_bytes));
                fetch_max_bytes = i32::try_from(adjusted).expect("bounded by i32 inputs");
            }
            let needed = i64::from(fetch_max_bytes) + 512;
            if v.is_set("receive.message.max.bytes") {
                if needed > i64::from(receive_message_max_bytes) {
                    return Err(creation_error(
                        "`receive.message.max.bytes` must be >= `fetch.max.bytes` + 512",
                    ));
                }
            } else {
                receive_message_max_bytes =
                    i32::try_from(needed.max(i64::from(receive_message_max_bytes)))
                        .unwrap_or(i32::MAX);
            }
        }

        // Producer settings.
        let transactional_id = v.string("transactional.id");
        let transaction_timeout_ms = v.int("transaction.timeout.ms", 60_000);
        let mut enable_idempotence = v.bool("enable.idempotence", false);
        let mut retries = v.int("message.send.max.retries", i32::MAX);
        let mut linger_ms = v.float("queue.buffering.max.ms", 5.0);
        let mut sticky_linger_ms = v.int("sticky.partitioning.linger.ms", 10);
        if is_consumer {
            enable_idempotence = false;
        } else {
            if transactional_id.is_some() && !enable_idempotence {
                if v.is_set("enable.idempotence") {
                    return Err(creation_error(
                        "`transactional.id` requires `enable.idempotence=true`",
                    ));
                }
                enable_idempotence = true;
            }
            if transactional_id.is_some() {
                if !v.is_set("socket.timeout.ms") {
                    socket_timeout_ms = (transaction_timeout_ms - 100).max(900);
                } else if i64::from(transaction_timeout_ms) + 100 < i64::from(socket_timeout_ms) {
                    return Err(creation_error(
                        "`socket.timeout.ms` must be set <= `transaction.timeout.ms` + 100",
                    ));
                }
            }
            if enable_idempotence {
                if v.is_set("max.in.flight.requests.per.connection") {
                    if max_in_flight > IDEMPOTENCE_MAX_IN_FLIGHT {
                        return Err(creation_error(
                            "`max.in.flight` must be set <= 5 when `enable.idempotence` is true",
                        ));
                    }
                } else {
                    max_in_flight = max_in_flight.min(IDEMPOTENCE_MAX_IN_FLIGHT);
                }
                if v.is_set("message.send.max.retries") {
                    if retries < 1 {
                        return Err(creation_error(
                            "`retries` must be set >= 1 when `enable.idempotence` is true",
                        ));
                    }
                } else {
                    retries = i32::MAX;
                }
            }
            if !v.is_set("sticky.partitioning.linger.ms") {
                let doubled = i64::cast_lossy(2.0 * linger_ms).min(900_000);
                sticky_linger_ms = i32::try_from(doubled).expect("bounded by 900000");
            }
        }

        let metadata_refresh_interval_ms = v.int("topic.metadata.refresh.interval.ms", 300_000);

        let reconnect_backoff_ms = v.int("reconnect.backoff.ms", 100);
        let reconnect_backoff_max_ms = v.int("reconnect.backoff.max.ms", 10_000);
        if reconnect_backoff_max_ms < reconnect_backoff_ms {
            return Err(creation_error(
                "`reconnect.backoff.max.ms` must be >= `reconnect.backoff.ms`",
            ));
        }

        let retry_backoff_ms = v.int("retry.backoff.ms", 100);
        let retry_backoff_max_ms = v.int("retry.backoff.max.ms", 1000);
        let metadata_refresh_fast_interval_ms =
            if v.is_set("topic.metadata.refresh.fast.interval.ms") {
                v.int("topic.metadata.refresh.fast.interval.ms", 100)
            } else {
                retry_backoff_ms
            };

        let bootstrap_servers: Vec<String> = v
            .str("metadata.broker.list")
            .unwrap_or("")
            .split(|c: char| c == ',' || is_c_space(c))
            .filter(|s| !s.is_empty())
            .map(str::to_string)
            .collect();

        let mut connections_max_idle_ms = v.int("connections.max.idle.ms", 0);
        if !v.is_set("connections.max.idle.ms")
            && v.str("metadata.broker.list")
                .is_some_and(|b| b.to_ascii_lowercase().contains("azure"))
        {
            connections_max_idle_ms = (4 * 60 - 10) * 1000;
        }

        let allow_auto_create_topics = v.bool("allow.auto.create.topics", !is_consumer);

        // Topic-level producer settings.
        let mut message_timeout_ms = v.int("message.timeout.ms", 300_000);
        let mut acks = v.int("request.required.acks", -1);
        if !is_consumer {
            let topic_conf_set = PROPERTIES.iter().any(|p| p.topic_level && v.is_set(p.name));
            if topic_conf_set
                && message_timeout_ms != 0
                && f64::from(message_timeout_ms) <= linger_ms
            {
                if v.is_set("queue.buffering.max.ms") {
                    return Err(creation_error(
                        "`message.timeout.ms` must be greater than `linger.ms`",
                    ));
                }
                linger_ms = f64::from(message_timeout_ms) - 0.1;
            }
            if enable_idempotence {
                if v.is_set("request.required.acks") {
                    if acks != -1 {
                        return Err(creation_error(
                            "`acks` must be set to `all` when `enable.idempotence` is true",
                        ));
                    }
                } else {
                    acks = -1;
                }
                if transactional_id.is_some() {
                    if !v.is_set("message.timeout.ms") {
                        message_timeout_ms = transaction_timeout_ms;
                    } else if message_timeout_ms > transaction_timeout_ms {
                        return Err(creation_error(
                            "`message.timeout.ms` must be set <= `transaction.timeout.ms`",
                        ));
                    }
                }
            }
            if message_timeout_ms != 0
                && f64::from(message_timeout_ms) <= linger_ms
                && v.is_set("queue.buffering.max.ms")
            {
                return Err(creation_error(
                    "`message.timeout.ms` must be greater than `linger.ms`",
                ));
            }
        }

        // Assignors are initialized for every client kind.
        let partition_assignment_strategy = parse_assignment_strategy(
            v.str("partition.assignment.strategy")
                .unwrap_or("range,roundrobin"),
        )?;

        // SASL provider selection.
        let security_protocol = match v.enumeration("security.protocol", "plaintext") {
            "plaintext" => SecurityProtocol::Plaintext,
            "ssl" => SecurityProtocol::Ssl,
            "sasl_plaintext" => SecurityProtocol::SaslPlaintext,
            "sasl_ssl" => SecurityProtocol::SaslSsl,
            other => unreachable!("validated security.protocol {other}"),
        };
        let sasl_mechanism_name = v.str("sasl.mechanisms").unwrap_or("GSSAPI");
        let sasl_username = v.string("sasl.username");
        let sasl_password = v.string("sasl.password");
        let mechanism = if security_protocol.uses_sasl() {
            Some(select_sasl_mechanism(
                sasl_mechanism_name,
                sasl_username.is_some() && sasl_password.is_some(),
            )?)
        } else {
            None
        };

        let debug = v.flags("debug");
        let creation_log_level = if debug.is_empty() {
            log_level_from_int(v.int("log_level", 6))
        } else {
            RDKafkaLogLevel::Debug
        };

        let consumer = ConsumerSettings {
            group_id: v.string("group.id").filter(|g| !g.is_empty()),
            group_instance_id: v.string("group.instance.id").filter(|g| !g.is_empty()),
            partition_assignment_strategy,
            session_timeout: millis(session_timeout_ms),
            heartbeat_interval: millis(v.int("heartbeat.interval.ms", 3000)),
            max_poll_interval: millis(max_poll_interval_ms),
            enable_auto_commit: v.bool("enable.auto.commit", true),
            auto_commit_interval: millis(v.int("auto.commit.interval.ms", 5000)),
            enable_auto_offset_store: v.bool("enable.auto.offset.store", true),
            auto_offset_reset: match v.enumeration("auto.offset.reset", "largest") {
                "smallest" | "earliest" | "beginning" => AutoOffsetReset::Earliest,
                "largest" | "latest" | "end" => AutoOffsetReset::Latest,
                "error" => AutoOffsetReset::Error,
                other => unreachable!("validated auto.offset.reset {other}"),
            },
            enable_partition_eof: v.bool("enable.partition.eof", false),
            check_crcs: v.bool("check.crcs", false),
            fetch_wait_max: millis(v.int("fetch.wait.max.ms", 500)),
            fetch_min_bytes: v.int("fetch.min.bytes", 1),
            fetch_max_bytes,
            fetch_message_max_bytes: v.int("fetch.message.max.bytes", 1_048_576),
            fetch_error_backoff: millis(v.int("fetch.error.backoff.ms", 500)),
            fetch_queue_backoff: millis(v.int("fetch.queue.backoff.ms", 1000)),
            queued_min_messages: v.int("queued.min.messages", 100_000),
            queued_max_messages_kbytes,
        };

        let queue_buffering_max_messages = v.int("queue.buffering.max.messages", 100_000);
        let queue_buffering_max_kbytes = v.int("queue.buffering.max.kbytes", 1_048_576);
        let compression_level = v.int("compression.level", -1);
        let producer = ProducerSettings {
            transactional_id,
            transaction_timeout: millis(transaction_timeout_ms),
            enable_idempotence,
            acks: i16::try_from(acks).expect("validated range -1..=1000"),
            request_timeout: millis(v.int("request.timeout.ms", 30_000)),
            message_timeout: (message_timeout_ms != 0).then(|| millis(message_timeout_ms)),
            linger: duration_from_millis_f64(linger_ms),
            batch_size: v.int("batch.size", 1_000_000),
            batch_num_messages: v.int("batch.num.messages", 10_000),
            queue_buffering_max_messages: u32::try_from(queue_buffering_max_messages)
                .ok()
                .filter(|n| *n != 0),
            queue_buffering_max_bytes: u64::try_from(queue_buffering_max_kbytes)
                .expect("validated positive")
                * 1024,
            compression: match v.enumeration("compression.codec", "none") {
                "none" => CompressionCodec::None,
                "gzip" => CompressionCodec::Gzip,
                "snappy" => CompressionCodec::Snappy,
                "lz4" => CompressionCodec::Lz4,
                "zstd" => CompressionCodec::Zstd,
                other => unreachable!("validated compression.codec {other}"),
            },
            compression_level: (compression_level != -1).then_some(compression_level),
            partitioner: {
                let name = v.str("partitioner").unwrap_or("consistent_random");
                PARTITIONERS
                    .iter()
                    .find(|(n, _)| *n == name)
                    .map(|(_, p)| *p)
                    .expect("validated partitioner")
            },
            retries,
            sticky_partitioning_linger: millis(sticky_linger_ms),
            delivery_report_only_error: v.bool("delivery.report.only.error", false),
        };

        let mut settings = ClientSettings {
            kind,
            log_level,
            creation_log_level,
            debug,
            client_id: v
                .string("client.id")
                .unwrap_or_else(|| "rdkafka".to_string()),
            bootstrap_servers,
            message_max_bytes,
            receive_message_max_bytes,
            max_in_flight,
            metadata_refresh_interval: (metadata_refresh_interval_ms >= 0)
                .then(|| millis(metadata_refresh_interval_ms)),
            metadata_refresh_fast_interval: millis(metadata_refresh_fast_interval_ms),
            metadata_propagation_max: millis(v.int("topic.metadata.propagation.max.ms", 30_000)),
            allow_auto_create_topics,
            socket_timeout: millis(socket_timeout_ms),
            socket_connection_setup_timeout: millis(
                v.int("socket.connection.setup.timeout.ms", 30_000),
            ),
            socket_keepalive: v.bool("socket.keepalive.enable", false),
            socket_nagle_disable: v.bool("socket.nagle.disable", true),
            socket_send_buffer_bytes: u32::try_from(v.int("socket.send.buffer.bytes", 0))
                .ok()
                .filter(|n| *n != 0),
            socket_receive_buffer_bytes: u32::try_from(v.int("socket.receive.buffer.bytes", 0))
                .ok()
                .filter(|n| *n != 0),
            connections_max_idle: (connections_max_idle_ms != 0)
                .then(|| millis(connections_max_idle_ms)),
            reconnect_backoff: millis(reconnect_backoff_ms),
            reconnect_backoff_max: millis(reconnect_backoff_max_ms),
            retry_backoff: millis(retry_backoff_ms),
            retry_backoff_max: millis(retry_backoff_max_ms),
            statistics_interval: match v.int("statistics.interval.ms", 0) {
                0 => None,
                ms => Some(millis(ms)),
            },
            log_connection_close: v.bool("log.connection.close", true),
            api_version_request_timeout: millis(v.int("api.version.request.timeout.ms", 10_000)),
            security_protocol,
            ssl,
            sasl: SaslSettings {
                mechanism,
                username: sasl_username,
                password: sasl_password,
                oauthbearer_config: v.string("sasl.oauthbearer.config"),
            },
            isolation_level: match v.enumeration("isolation.level", "read_committed") {
                "read_committed" => IsolationLevel::ReadCommitted,
                "read_uncommitted" => IsolationLevel::ReadUncommitted,
                other => unreachable!("validated isolation.level {other}"),
            },
            consumer,
            producer,
            warnings: Vec::new(),
        };
        settings.warnings = conf_warnings(v, &settings, sasl_mechanism_name);
        Ok(settings)
    }
}

/// Parses `partition.assignment.strategy` like `rd_kafka_assignors_init`.
fn parse_assignment_strategy(value: &str) -> KafkaResult<Vec<AssignmentStrategy>> {
    let mut strategies = Vec::new();
    for name in value.split(',') {
        let name = name.trim_matches(|c: char| c == ' ' || is_c_space(c));
        if name.is_empty() {
            continue;
        }
        let strategy = match name {
            "range" => AssignmentStrategy::Range,
            "roundrobin" => AssignmentStrategy::RoundRobin,
            "cooperative-sticky" => AssignmentStrategy::CooperativeSticky,
            _ => {
                return Err(creation_error(format!(
                    "Unsupported partition.assignment.strategy: {name}"
                )));
            }
        };
        if !strategies.contains(&strategy) {
            strategies.push(strategy);
        }
    }
    let cooperative = strategies
        .iter()
        .filter(|s| **s == AssignmentStrategy::CooperativeSticky)
        .count();
    if cooperative > 0 && cooperative < strategies.len() {
        return Err(creation_error(format!(
            "All partition.assignment.strategy ({value}) assignors must have the same protocol \
             type, online migration between assignors with different protocol types is not \
             supported"
        )));
    }
    Ok(strategies)
}

/// Selects the SASL mechanism like `rd_kafka_sasl_select_provider`, including
/// the providers' configuration checks. Mechanism names are case-sensitive.
fn select_sasl_mechanism(name: &str, credentials_set: bool) -> KafkaResult<SaslMechanism> {
    let mechanism = if name == "GSSAPI" {
        // librdkafka in Materialize's build has no GSSAPI provider; keep its
        // exact failure text, which tests assert on.
        return Err(creation_error(
            "No provider for SASL mechanism GSSAPI: recompile librdkafka with libsasl2 or \
             openssl support. Current build options: PLAIN SASL_SCRAM OAUTHBEARER",
        ));
    } else if name == "PLAIN" {
        SaslMechanism::Plain
    } else if name.starts_with("SCRAM-SHA-") {
        if !credentials_set {
            return Err(creation_error(
                "sasl.username and sasl.password must be set",
            ));
        }
        match name {
            "SCRAM-SHA-256" => SaslMechanism::ScramSha256,
            "SCRAM-SHA-512" => SaslMechanism::ScramSha512,
            // NOTE: librdkafka also accepts SCRAM-SHA-1, which Kafka brokers
            // do not implement.
            _ => {
                return Err(creation_error(format!(
                    "Unsupported hash function: {name} (try SCRAM-SHA-512)"
                )));
            }
        }
    } else if name == "OAUTHBEARER" {
        SaslMechanism::OAuthBearer
    } else {
        return Err(creation_error(format!(
            "Unsupported SASL mechanism: {name}"
        )));
    };
    if mechanism == SaslMechanism::Plain && !credentials_set {
        return Err(creation_error(
            "sasl.username and sasl.password must be set",
        ));
    }
    Ok(mechanism)
}

/// librdkafka's `rd_kafka_conf_warn` lines for the set properties.
fn conf_warnings(
    v: &SetValues,
    settings: &ClientSettings,
    sasl_mechanism_name: &str,
) -> Vec<(RDKafkaLogLevel, String)> {
    let mut warnings = Vec::new();
    let is_consumer = settings.kind.is_consumer();
    for p in PROPERTIES.iter().filter(|p| v.is_set(p.name)) {
        let ignored_by = match (p.scope, is_consumer) {
            (Scope::Consumer, false) => Some(("consumer", "producer")),
            (Scope::Producer, true) => Some(("producer", "consumer")),
            _ => None,
        };
        if let Some((prop_kind, client_kind)) = ignored_by {
            warnings.push((
                RDKafkaLogLevel::Warning,
                format!(
                    "Configuration property {} is a {prop_kind} property and will be ignored by \
                     this {client_kind} instance",
                    p.name
                ),
            ));
        }
    }
    let retry_backoff_ms = settings.retry_backoff.as_millis();
    let retry_backoff_max_ms = settings.retry_backoff_max.as_millis();
    if retry_backoff_ms > retry_backoff_max_ms {
        warnings.push((
            RDKafkaLogLevel::Warning,
            format!(
                "Configuration `retry.backoff.ms` with value {retry_backoff_ms} is greater than \
                 configuration `retry.backoff.max.ms` with value {retry_backoff_max_ms}. A static \
                 backoff with value `retry.backoff.max.ms` will be applied."
            ),
        ));
    }
    let fast_ms = settings.metadata_refresh_fast_interval.as_millis();
    if v.is_set("topic.metadata.refresh.fast.interval.ms") && fast_ms > retry_backoff_max_ms {
        warnings.push((
            RDKafkaLogLevel::Warning,
            format!(
                "Configuration `topic.metadata.refresh.fast.interval.ms` with value {fast_ms} is \
                 greater than configuration `retry.backoff.max.ms` with value \
                 {retry_backoff_max_ms}. A static backoff with value `retry.backoff.max.ms` will \
                 be applied."
            ),
        ));
    }
    if is_consumer {
        let fetch_wait_ms = settings.consumer.fetch_wait_max.as_millis();
        let socket_timeout_ms = settings.socket_timeout.as_millis();
        if fetch_wait_ms + 1000 > socket_timeout_ms {
            warnings.push((
                RDKafkaLogLevel::Warning,
                format!(
                    "Configuration property `fetch.wait.max.ms` ({fetch_wait_ms}) should be set \
                     lower than `socket.timeout.ms` ({socket_timeout_ms}) by at least 1000ms to \
                     avoid blocking and timing out sub-sequent requests"
                ),
            ));
        }
    }
    if v.is_set("sasl.mechanisms") && !settings.security_protocol.uses_sasl() {
        warnings.push((
            RDKafkaLogLevel::Warning,
            format!(
                "Configuration property `sasl.mechanism` set to `{sasl_mechanism_name}` but \
                 `security.protocol` is not configured for SASL: recommend setting \
                 `security.protocol` to SASL_SSL or SASL_PLAINTEXT"
            ),
        ));
    }
    if v.is_set("sasl.username")
        && !(sasl_mechanism_name.starts_with("SCRAM") || sasl_mechanism_name == "PLAIN")
    {
        warnings.push((
            RDKafkaLogLevel::Warning,
            "Configuration property `sasl.username` only applies when `sasl.mechanism` is set to \
             PLAIN or SCRAM-SHA-.."
                .to_string(),
        ));
    }
    if settings.bootstrap_servers.is_empty() {
        warnings.push((
            RDKafkaLogLevel::Notice,
            "No `bootstrap.servers` configured: client will not be able to connect to Kafka \
             cluster"
                .to_string(),
        ));
    }
    warnings
}

#[cfg(test)]
#[allow(clippy::disallowed_methods)]
mod tests {
    use super::*;

    fn config(pairs: &[(&str, &str)]) -> ClientConfig {
        let mut c = ClientConfig::new();
        for (k, v) in pairs {
            c.set(*k, *v);
        }
        c
    }

    fn parse(kind: ClientKind, pairs: &[(&str, &str)]) -> KafkaResult<ClientSettings> {
        ClientSettings::from_config(&config(pairs), kind)
    }

    fn err(kind: ClientKind, pairs: &[(&str, &str)]) -> String {
        parse(kind, pairs).unwrap_err().to_string()
    }

    #[mz_ore::test]
    fn defaults() {
        let s = parse(ClientKind::Consumer, &[]).unwrap();
        assert_eq!(s.client_id, "rdkafka");
        assert!(s.bootstrap_servers.is_empty());
        assert_eq!(s.security_protocol, SecurityProtocol::Plaintext);
        assert_eq!(s.sasl.mechanism, None);
        assert!(s.ssl.endpoint_identification);
        assert!(s.ssl.enable_certificate_verification);
        assert_eq!(s.isolation_level, IsolationLevel::ReadCommitted);
        assert!(!s.allow_auto_create_topics);
        assert_eq!(s.socket_timeout, Duration::from_secs(60));
        assert_eq!(s.socket_connection_setup_timeout, Duration::from_secs(30));
        assert!(!s.socket_keepalive);
        assert!(s.socket_nagle_disable);
        assert_eq!(s.reconnect_backoff, Duration::from_millis(100));
        assert_eq!(s.reconnect_backoff_max, Duration::from_secs(10));
        assert_eq!(s.retry_backoff, Duration::from_millis(100));
        assert_eq!(s.retry_backoff_max, Duration::from_secs(1));
        assert_eq!(s.metadata_refresh_interval, Some(Duration::from_secs(300)));
        assert_eq!(s.metadata_refresh_fast_interval, Duration::from_millis(100));
        assert_eq!(s.message_max_bytes, 1_000_000);
        assert_eq!(s.statistics_interval, None);
        assert_eq!(s.max_in_flight, 1_000_000);
        assert_eq!(s.creation_log_level, RDKafkaLogLevel::Info);
        assert!(s.debug.is_empty());

        let c = &s.consumer;
        assert_eq!(c.group_id, None);
        assert_eq!(
            c.partition_assignment_strategy,
            [AssignmentStrategy::Range, AssignmentStrategy::RoundRobin]
        );
        assert_eq!(c.auto_offset_reset, AutoOffsetReset::Latest);
        assert!(!c.enable_partition_eof);
        assert!(c.enable_auto_commit);
        assert_eq!(c.auto_commit_interval, Duration::from_secs(5));
        assert!(c.enable_auto_offset_store);
        assert_eq!(c.queued_max_messages_kbytes, 65_536);
        assert_eq!(c.queued_max_bytes(), 65_536_000);
        assert_eq!(c.queued_min_messages, 100_000);
        assert_eq!(c.fetch_wait_max, Duration::from_millis(500));
        assert_eq!(c.fetch_min_bytes, 1);
        assert_eq!(c.fetch_message_max_bytes, 1_048_576);
        assert_eq!(c.session_timeout, Duration::from_secs(45));
        assert_eq!(c.heartbeat_interval, Duration::from_secs(3));
        assert_eq!(c.max_poll_interval, Duration::from_secs(300));
        assert!(!c.check_crcs);
        // fetch.max.bytes is capped to queued.max.messages.kbytes * 1024 and
        // receive.message.max.bytes raised to fit it.
        assert_eq!(c.fetch_max_bytes, 52_428_800);
        assert_eq!(s.receive_message_max_bytes, 100_000_000);

        let p = parse(ClientKind::Producer, &[]).unwrap();
        assert!(p.allow_auto_create_topics);
        assert!(
            parse(ClientKind::Admin, &[])
                .unwrap()
                .allow_auto_create_topics
        );
        let pp = &p.producer;
        assert!(!pp.enable_idempotence);
        assert_eq!(pp.acks, -1);
        assert_eq!(pp.linger, Duration::from_millis(5));
        assert_eq!(pp.batch_size, 1_000_000);
        assert_eq!(pp.batch_num_messages, 10_000);
        assert_eq!(pp.queue_buffering_max_messages, Some(100_000));
        assert_eq!(pp.queue_buffering_max_bytes, 1_048_576 * 1024);
        assert_eq!(pp.message_timeout, Some(Duration::from_secs(300)));
        assert_eq!(pp.transaction_timeout, Duration::from_secs(60));
        assert_eq!(pp.compression, CompressionCodec::None);
        assert_eq!(pp.partitioner, PartitionerKind::ConsistentRandom);
        assert_eq!(pp.retries, i32::MAX);
        assert_eq!(pp.request_timeout, Duration::from_secs(30));
        assert_eq!(pp.sticky_partitioning_linger, Duration::from_millis(10));
        assert_eq!(p.receive_message_max_bytes, 100_000_000);
    }

    /// The configuration of Materialize's transactional sink producer.
    #[mz_ore::test]
    fn sink_producer() {
        let s = parse(
            ClientKind::Producer,
            &[
                ("bootstrap.servers", "b1:9092,b2:9093"),
                ("socket.keepalive.enable", "true"),
                ("socket.timeout.ms", "60000"),
                ("socket.connection.setup.timeout.ms", "30000"),
                ("allow.auto.create.topics", "false"),
                ("security.protocol", "SASL_SSL"),
                ("sasl.mechanisms", "SCRAM-SHA-512"),
                ("sasl.username", "u"),
                ("sasl.password", "p"),
                ("ssl.ca.pem", "\n-----BEGIN CERTIFICATE-----"),
                ("retry.backoff.ms", "100"),
                ("retry.backoff.max.ms", "1000"),
                ("reconnect.backoff.ms", "100"),
                ("reconnect.backoff.max.ms", "30000"),
                ("enable.idempotence", "true"),
                ("compression.type", "lz4"),
                ("queue.buffering.max.kbytes", "2147483647"),
                ("queue.buffering.max.messages", "0"),
                ("queue.buffering.max.ms", "10"),
                ("transaction.timeout.ms", "600000"),
                ("transactional.id", "mz-sink"),
                ("client.id", "materialize-sink"),
                ("statistics.interval.ms", "1000"),
                ("message.max.bytes", "1000000"),
                ("batch.size", "1000000"),
                ("batch.num.messages", "10000"),
            ],
        )
        .unwrap();
        assert_eq!(s.bootstrap_servers, ["b1:9092", "b2:9093"]);
        assert_eq!(s.security_protocol, SecurityProtocol::SaslSsl);
        assert_eq!(s.sasl.mechanism, Some(SaslMechanism::ScramSha512));
        assert_eq!(s.ssl.ca_pem.as_deref(), Some("-----BEGIN CERTIFICATE-----"));
        assert!(!s.allow_auto_create_topics);
        assert!(s.socket_keepalive);
        assert_eq!(s.max_in_flight, 5);
        assert_eq!(s.statistics_interval, Some(Duration::from_secs(1)));
        let p = &s.producer;
        assert!(p.enable_idempotence);
        assert_eq!(p.acks, -1);
        assert_eq!(p.compression, CompressionCodec::Lz4);
        assert_eq!(p.queue_buffering_max_messages, None);
        assert_eq!(p.queue_buffering_max_bytes, 2_147_483_647 * 1024);
        assert_eq!(p.linger, Duration::from_millis(10));
        assert_eq!(p.sticky_partitioning_linger, Duration::from_millis(20));
        // message.timeout.ms follows transaction.timeout.ms.
        assert_eq!(p.message_timeout, Some(Duration::from_secs(600)));
        assert_eq!(p.retries, i32::MAX);
        assert_eq!(s.client_id, "materialize-sink");
    }

    /// The configuration of Materialize's source reader consumer and testdrive.
    #[mz_ore::test]
    fn source_consumer_and_testdrive() {
        let s = parse(
            ClientKind::Consumer,
            &[
                ("bootstrap.servers", "kafka:9092"),
                ("enable.auto.commit", "false"),
                ("auto.offset.reset", "earliest"),
                ("topic.metadata.refresh.interval.ms", "30000"),
                ("fetch.message.max.bytes", "134217728"),
                ("group.id", "materialize-1"),
                ("client.id", "materialize-1"),
                ("isolation.level", "read_uncommitted"),
                ("enable.partition.eof", "true"),
                ("debug", "all"),
            ],
        )
        .unwrap();
        let c = &s.consumer;
        assert_eq!(c.group_id.as_deref(), Some("materialize-1"));
        assert!(!c.enable_auto_commit);
        assert_eq!(c.auto_offset_reset, AutoOffsetReset::Earliest);
        assert_eq!(c.fetch_message_max_bytes, 134_217_728);
        assert!(c.enable_partition_eof);
        assert_eq!(s.isolation_level, IsolationLevel::ReadUncommitted);
        assert_eq!(s.metadata_refresh_interval, Some(Duration::from_secs(30)));
        assert!(s.debug.contains(DebugContext::Broker));
        assert!(s.debug.contains(DebugContext::Telemetry));
        assert_eq!(s.creation_log_level, RDKafkaLogLevel::Debug);

        // testdrive's base config, applied to a producer.
        let s = parse(
            ClientKind::Producer,
            &[
                ("bootstrap.servers", "kafka:9092"),
                ("group.id", "materialize-testdrive"),
                ("auto.offset.reset", "earliest"),
                ("isolation.level", "read_committed"),
                ("security.protocol", "ssl"),
                ("ssl.keystore.location", "/x.p12"),
                ("ssl.keystore.password", "pw"),
                ("message.max.bytes", "15728640"),
                ("acks", "all"),
            ],
        )
        .unwrap();
        assert_eq!(s.ssl.keystore_location.as_deref(), Some("/x.p12"));
        assert_eq!(s.message_max_bytes, 15_728_640);
        assert_eq!(s.producer.acks, -1);
        let warnings: Vec<_> = s.warnings.iter().map(|(_, w)| w.as_str()).collect();
        assert!(
            warnings.contains(
                &"Configuration property group.id is a consumer property and will be ignored \
                  by this producer instance"
            ),
            "{warnings:?}"
        );
    }

    #[mz_ore::test]
    fn aliases() {
        let s = parse(
            ClientKind::Producer,
            &[
                ("metadata.broker.list", "a:1"),
                ("linger.ms", "2.5"),
                ("delivery.timeout.ms", "1000"),
                ("request.required.acks", "1"),
                ("compression.codec", "zstd"),
                ("max.in.flight", "7"),
                ("retries", "3"),
                ("sasl.mechanism", "PLAIN"),
            ],
        )
        .unwrap();
        assert_eq!(s.bootstrap_servers, ["a:1"]);
        assert_eq!(s.producer.linger, Duration::from_micros(2500));
        assert_eq!(s.producer.message_timeout, Some(Duration::from_secs(1)));
        assert_eq!(s.producer.acks, 1);
        assert_eq!(s.producer.compression, CompressionCodec::Zstd);
        assert_eq!(s.max_in_flight, 7);
        assert_eq!(s.producer.retries, 3);

        let s = parse(
            ClientKind::Consumer,
            &[
                ("max.partition.fetch.bytes", "100"),
                ("topic.auto.offset.reset", "smallest"),
            ],
        )
        .unwrap();
        assert_eq!(s.consumer.fetch_message_max_bytes, 100);
        assert_eq!(s.consumer.auto_offset_reset, AutoOffsetReset::Earliest);
    }

    #[mz_ore::test]
    fn property_errors() {
        let c = ClientKind::Consumer;
        assert_eq!(
            err(c, &[("foo", "bar")]),
            "Client config error: No such configuration property: \"foo\" foo bar"
        );
        assert_eq!(
            err(c, &[("topic.foo", "bar")]),
            "Client config error: No such configuration property: \"foo\" topic.foo bar"
        );
        assert_eq!(
            err(c, &[("isolation.level", "x")]),
            "Client config error: Invalid value \"x\" for configuration property \
             \"isolation.level\" isolation.level x"
        );
        assert_eq!(
            err(c, &[("enable.auto.commit", "yes")]),
            "Client config error: Expected bool value for \"enable.auto.commit\": true or \
             false enable.auto.commit yes"
        );
        assert_eq!(
            err(c, &[("socket.timeout.ms", "5")]),
            "Client config error: Configuration property \"socket.timeout.ms\" value 5 is \
             outside allowed range 10..300000\n socket.timeout.ms 5"
        );
        assert_eq!(
            err(c, &[("socket.timeout.ms", "abc")]),
            "Client config error: Invalid value for configuration property \
             \"socket.timeout.ms\" socket.timeout.ms abc"
        );
        assert_eq!(
            err(c, &[("acks", "most")]),
            "Client config error: Invalid value for configuration property \
             \"request.required.acks\" acks most"
        );
        assert_eq!(
            err(c, &[("linger.ms", "1000000")]),
            "Client config error: Configuration property \"queue.buffering.max.ms\" value \
             1e+06 is outside allowed range 0..900000\n linger.ms 1000000"
        );
        // strtod overflows to infinity; formatting it must not panic.
        assert_eq!(
            err(c, &[("queue.buffering.max.ms", "1e999")]),
            "Client config error: Configuration property \"queue.buffering.max.ms\" value \
             inf is outside allowed range 0..900000\n queue.buffering.max.ms 1e999"
        );
        assert_eq!(
            err(c, &[("debug", "all,bogus")]),
            "Client config error: Invalid value \"bogus\" for configuration property \"debug\" \
             debug all,bogus"
        );
        assert_eq!(
            err(c, &[("sasl.mechanisms", "PLAIN,SCRAM-SHA-256")]),
            "Client config error: Invalid value for configuration property \
             \"sasl.mechanisms\": PLAIN,SCRAM-SHA-256 sasl.mechanisms PLAIN,SCRAM-SHA-256"
        );
        assert_eq!(
            err(c, &[("partitioner", "murmur3")]),
            "Client config error: Invalid value for configuration property \"partitioner\": \
             murmur3 partitioner murmur3"
        );
        assert!(matches!(
            parse(c, &[("foo", "bar")]),
            Err(KafkaError::ClientConfig(
                RDKafkaConfRes::RD_KAFKA_CONF_UNKNOWN,
                ..
            ))
        ));
        assert!(matches!(
            parse(c, &[("check.crcs", "x")]),
            Err(KafkaError::ClientConfig(
                RDKafkaConfRes::RD_KAFKA_CONF_INVALID,
                ..
            ))
        ));
        // Values are matched case-insensitively and empty enum values are
        // no-ops.
        let s = parse(
            c,
            &[
                ("security.protocol", ""),
                ("isolation.level", "READ_UNCOMMITTED"),
            ],
        )
        .unwrap();
        assert_eq!(s.security_protocol, SecurityProtocol::Plaintext);
        assert_eq!(s.isolation_level, IsolationLevel::ReadUncommitted);
        // Ignored properties are accepted.
        parse(
            c,
            &[
                ("log.queue", "whatever"),
                ("topic.queuing.strategy", "fifo"),
            ],
        )
        .unwrap();
    }

    #[mz_ore::test]
    fn creation_errors() {
        let p = ClientKind::Producer;
        assert_eq!(
            err(
                ClientKind::Admin,
                &[
                    ("security.protocol", "sasl_plaintext"),
                    ("sasl.mechanisms", "GSSAPI")
                ]
            ),
            "Client creation error: No provider for SASL mechanism GSSAPI: recompile librdkafka \
             with libsasl2 or openssl support. Current build options: PLAIN SASL_SCRAM \
             OAUTHBEARER"
        );
        // GSSAPI is the default mechanism.
        assert!(err(p, &[("security.protocol", "SASL_SSL")]).contains("GSSAPI"));
        assert_eq!(
            err(
                p,
                &[
                    ("security.protocol", "SASL_SSL"),
                    ("sasl.mechanisms", "GARBAGE")
                ]
            ),
            "Client creation error: Unsupported SASL mechanism: GARBAGE"
        );
        assert_eq!(
            err(
                p,
                &[
                    ("security.protocol", "SASL_SSL"),
                    ("sasl.mechanisms", "PLAIN")
                ]
            ),
            "Client creation error: sasl.username and sasl.password must be set"
        );
        assert_eq!(
            err(
                p,
                &[
                    ("security.protocol", "SASL_SSL"),
                    ("sasl.mechanisms", "SCRAM-SHA-1"),
                    ("sasl.username", "u"),
                    ("sasl.password", "p"),
                ]
            ),
            "Client creation error: Unsupported hash function: SCRAM-SHA-1 (try SCRAM-SHA-512)"
        );
        // OAUTHBEARER with an empty username, as for AWS IAM.
        let s = parse(
            p,
            &[
                ("security.protocol", "SASL_SSL"),
                ("sasl.mechanisms", "OAUTHBEARER"),
                ("sasl.username", ""),
            ],
        )
        .unwrap();
        assert_eq!(s.sasl.mechanism, Some(SaslMechanism::OAuthBearer));
        assert_eq!(
            err(p, &[("enable.idempotence", "true"), ("acks", "1")]),
            "Client creation error: `acks` must be set to `all` when `enable.idempotence` is true"
        );
        assert_eq!(
            err(
                p,
                &[("transactional.id", "t"), ("enable.idempotence", "false")]
            ),
            "Client creation error: `transactional.id` requires `enable.idempotence=true`"
        );
        assert_eq!(
            err(p, &[("enable.idempotence", "true"), ("max.in.flight", "6")]),
            "Client creation error: `max.in.flight` must be set <= 5 when `enable.idempotence` \
             is true"
        );
        assert_eq!(
            err(
                p,
                &[
                    ("transactional.id", "t"),
                    ("transaction.timeout.ms", "1000"),
                    ("socket.timeout.ms", "2000"),
                ]
            ),
            "Client creation error: `socket.timeout.ms` must be set <= `transaction.timeout.ms` \
             + 100"
        );
        assert_eq!(
            err(
                p,
                &[
                    ("transactional.id", "t"),
                    ("transaction.timeout.ms", "1000"),
                    ("message.timeout.ms", "2000"),
                ]
            ),
            "Client creation error: `message.timeout.ms` must be set <= `transaction.timeout.ms`"
        );
        assert_eq!(
            err(p, &[("linger.ms", "100"), ("message.timeout.ms", "50")]),
            "Client creation error: `message.timeout.ms` must be greater than `linger.ms`"
        );
        assert_eq!(
            err(
                p,
                &[
                    ("reconnect.backoff.ms", "200"),
                    ("reconnect.backoff.max.ms", "100")
                ]
            ),
            "Client creation error: `reconnect.backoff.max.ms` must be >= `reconnect.backoff.ms`"
        );
        assert_eq!(
            err(p, &[("ssl.keystore.location", "/x")]),
            "Client creation error: `ssl.keystore.password` is mandatory when \
             `ssl.keystore.location` is set"
        );
        assert_eq!(
            err(ClientKind::Consumer, &[("session.timeout.ms", "400000")]),
            "Client creation error: `max.poll.interval.ms`must be >= `session.timeout.ms`"
        );
        assert_eq!(
            err(
                ClientKind::Consumer,
                &[("fetch.max.bytes", "1000"), ("message.max.bytes", "2000")]
            ),
            "Client creation error: `fetch.max.bytes` must be >= `message.max.bytes`"
        );
        assert_eq!(
            err(
                ClientKind::Consumer,
                &[("partition.assignment.strategy", "range,sticky")]
            ),
            "Client creation error: Unsupported partition.assignment.strategy: sticky"
        );
        assert!(
            err(
                ClientKind::Consumer,
                &[("partition.assignment.strategy", "range,cooperative-sticky")]
            )
            .contains("must have the same protocol type")
        );
        // SASL settings are not checked for non-SASL protocols.
        parse(p, &[("sasl.mechanisms", "GARBAGE")]).unwrap();
    }

    #[mz_ore::test]
    fn derived_defaults() {
        // Transactional producers derive socket.timeout.ms from the
        // transaction timeout.
        let s = parse(
            ClientKind::Producer,
            &[
                ("transactional.id", "t"),
                ("transaction.timeout.ms", "5000"),
            ],
        )
        .unwrap();
        assert_eq!(s.socket_timeout, Duration::from_millis(4900));
        assert!(s.producer.enable_idempotence);
        let s = parse(
            ClientKind::Producer,
            &[
                ("transactional.id", "t"),
                ("transaction.timeout.ms", "1000"),
            ],
        )
        .unwrap();
        assert_eq!(s.socket_timeout, Duration::from_millis(900));

        // linger.ms is lowered below message.timeout.ms when only the latter
        // (a topic-level property) is set.
        let s = parse(ClientKind::Producer, &[("message.timeout.ms", "3")]).unwrap();
        assert_eq!(s.producer.linger, Duration::from_micros(2900));

        // A consumer's fetch.max.bytes follows queued.max.messages.kbytes, and
        // receive.message.max.bytes follows fetch.max.bytes.
        let s = parse(
            ClientKind::Consumer,
            &[("queued.max.messages.kbytes", "1024")],
        )
        .unwrap();
        assert_eq!(s.consumer.fetch_max_bytes, 1024 * 1024);
        let s = parse(ClientKind::Consumer, &[("fetch.max.bytes", "200000000")]).unwrap();
        assert_eq!(s.receive_message_max_bytes, 200_000_512);

        let s = parse(
            ClientKind::Consumer,
            &[("topic.metadata.refresh.interval.ms", "-1")],
        )
        .unwrap();
        assert_eq!(s.metadata_refresh_interval, None);

        let s = parse(
            ClientKind::Consumer,
            &[("bootstrap.servers", "x.servicebus.windows.net:9093,AZURE")],
        )
        .unwrap();
        assert_eq!(s.connections_max_idle, Some(Duration::from_secs(230)));

        let s = parse(ClientKind::Consumer, &[("retry.backoff.ms", "2000")]).unwrap();
        assert_eq!(s.metadata_refresh_fast_interval, Duration::from_secs(2));
        assert!(
            s.warnings
                .iter()
                .any(|(_, w)| w.starts_with("Configuration `retry.backoff.ms` with value 2000")),
            "{:?}",
            s.warnings
        );
    }

    #[mz_ore::test]
    fn warnings() {
        let s = parse(
            ClientKind::Consumer,
            &[
                ("bootstrap.servers", "a:1"),
                ("transactional.id", "x"),
                ("sasl.mechanisms", "PLAIN"),
                ("sasl.username", "u"),
            ],
        )
        .unwrap();
        let w: Vec<_> = s.warnings.iter().map(|(_, w)| w.as_str()).collect();
        assert_eq!(
            w,
            [
                "Configuration property transactional.id is a producer property and will be \
                 ignored by this consumer instance",
                "Configuration property `sasl.mechanism` set to `PLAIN` but `security.protocol` \
                 is not configured for SASL: recommend setting `security.protocol` to SASL_SSL \
                 or SASL_PLAINTEXT",
            ]
        );
        let s = parse(ClientKind::Admin, &[("sasl.username", "")]).unwrap();
        assert_eq!(
            s.warnings,
            [
                (
                    RDKafkaLogLevel::Warning,
                    "Configuration property `sasl.username` only applies when `sasl.mechanism` \
                     is set to PLAIN or SCRAM-SHA-.."
                        .to_string()
                ),
                (
                    RDKafkaLogLevel::Notice,
                    "No `bootstrap.servers` configured: client will not be able to connect to \
                     Kafka cluster"
                        .to_string()
                ),
            ]
        );
        let s = parse(
            ClientKind::Consumer,
            &[("bootstrap.servers", "a:1"), ("socket.timeout.ms", "1000")],
        )
        .unwrap();
        assert_eq!(
            s.warnings,
            [(
                RDKafkaLogLevel::Warning,
                "Configuration property `fetch.wait.max.ms` (500) should be set lower than \
                 `socket.timeout.ms` (1000) by at least 1000ms to avoid blocking and timing out \
                 sub-sequent requests"
                    .to_string()
            )]
        );
    }

    #[mz_ore::test]
    fn c_number_parsing() {
        assert_eq!(c_strtol_int("42"), Some(42));
        assert_eq!(c_strtol_int("  -7"), Some(-7));
        assert_eq!(c_strtol_int("+7abc"), Some(7));
        assert_eq!(c_strtol_int("0x10"), Some(16));
        assert_eq!(c_strtol_int("010"), Some(8));
        assert_eq!(c_strtol_int("08"), Some(0));
        assert_eq!(c_strtol_int("0x"), Some(0));
        assert_eq!(c_strtol_int("abc"), None);
        assert_eq!(c_strtol_int(""), None);
        assert_eq!(c_strtol_int("-"), None);
        // `(int)` truncation of the `long` result.
        assert_eq!(c_strtol_int("2147483648"), Some(i32::MIN));
        assert_eq!(c_strtol_int("4294967297"), Some(1));
        assert_eq!(c_strtol_int("99999999999999999999999"), Some(-1));

        assert_eq!(c_strtod("2.5"), Some(2.5));
        assert_eq!(c_strtod(" 10ms"), Some(10.0));
        assert_eq!(c_strtod(".5"), Some(0.5));
        assert_eq!(c_strtod("5."), Some(5.0));
        assert_eq!(c_strtod("1e3"), Some(1000.0));
        assert_eq!(c_strtod("1e"), Some(1.0));
        assert_eq!(c_strtod("-0.25"), Some(-0.25));
        assert_eq!(c_strtod("."), None);
        assert_eq!(c_strtod("x"), None);

        assert_eq!(fmt_g(0.0), "0");
        assert_eq!(fmt_g(900_000.0), "900000");
        assert_eq!(fmt_g(1_000_000.0), "1e+06");
        assert_eq!(fmt_g(2.5), "2.5");
        assert_eq!(fmt_g(-1.0), "-1");
        assert_eq!(fmt_g(0.0001), "0.0001");
        assert_eq!(fmt_g(0.00001), "1e-05");
        assert_eq!(fmt_g(123_456_789.0), "1.23457e+08");
        assert_eq!(fmt_g(f64::INFINITY), "inf");
        assert_eq!(fmt_g(f64::NEG_INFINITY), "-inf");
        assert_eq!(fmt_g(f64::NAN), "nan");
    }

    #[mz_ore::test]
    fn sensitive() {
        assert!(is_sensitive("sasl.password"));
        assert!(is_sensitive("ssl.key.pem"));
        assert!(!is_sensitive("sasl.username"));
        let s = parse(
            ClientKind::Producer,
            &[
                ("sasl.password", "hunter2"),
                ("ssl.key.password", "hunter3"),
            ],
        )
        .unwrap();
        let debug = format!("{s:?}");
        assert!(
            !debug.contains("hunter2") && !debug.contains("hunter3"),
            "{debug}"
        );
    }
}

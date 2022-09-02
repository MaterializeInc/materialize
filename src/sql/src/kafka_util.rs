// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Provides parsing and convenience functions for working with Kafka from the `sql` package.

use std::collections::{BTreeMap, HashSet};
use std::sync::{Arc, Mutex};

use anyhow::bail;
use rdkafka::client::ClientContext;
use rdkafka::consumer::{BaseConsumer, Consumer, ConsumerContext};
use rdkafka::{Offset, TopicPartitionList};
use tokio::time::Duration;

use mz_kafka_util::client::{create_new_client_config, MzClientContext};
use mz_ore::task;
use mz_secrets::SecretsReader;
use mz_sql_parser::ast::display::AstDisplay;
use mz_sql_parser::ast::{AstInfo, KafkaConfigOption, KafkaConfigOptionName};
use mz_storage::types::connections::{KafkaConnection, StringOrSecret};

use crate::names::Aug;
use crate::normalize::generate_extracted_config;
use crate::plan::with_options::TryFromValue;
use crate::plan::PlanError;

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum KafkaOptionCheckContext {
    Source,
    Sink,
}

/// Verifies that [`KafkaConfigOption`]s are only used in the appropriate contexts
pub fn validate_options_for_context<T: AstInfo>(
    options: &[KafkaConfigOption<T>],
    context: KafkaOptionCheckContext,
) -> Result<(), anyhow::Error> {
    use KafkaConfigOptionName::*;
    use KafkaOptionCheckContext::*;

    for KafkaConfigOption { name, .. } in options {
        let limited_to_context = match name {
            Acks => None,
            ClientId => None,
            EnableAutoCommit => None,
            EnableIdempotence => None,
            FetchMessageMaxBytes => None,
            GroupIdPrefix => None,
            IsolationLevel => None,
            StatisticsIntervalMs => None,
            TopicMetadataRefreshIntervalMs => None,
            TransactionTimeoutMs => None,
            StartTimestamp => Some(Source),
            StartOffset => Some(Source),
            PartitionCount => Some(Sink),
            ReplicationFactor => Some(Sink),
            RetentionBytes => Some(Sink),
            RetentionMs => Some(Sink),
        };
        if limited_to_context.is_some() && limited_to_context != Some(context) {
            bail!(
                "cannot set {} for {}",
                name.to_ast_string(),
                match context {
                    Source => "SOURCE",
                    Sink => "SINK",
                }
            );
        }
    }

    Ok(())
}

generate_extracted_config!(
    KafkaConfigOption,
    (Acks, String),
    (ClientId, String),
    (EnableAutoCommit, bool),
    (EnableIdempotence, bool),
    (FetchMessageMaxBytes, i32),
    (GroupIdPrefix, String),
    (
        IsolationLevel,
        String,
        Default(String::from("read_committed"))
    ),
    (StatisticsIntervalMs, i32, Default(1_000)),
    (TopicMetadataRefreshIntervalMs, i32),
    (TransactionTimeoutMs, i32),
    (StartTimestamp, i64),
    (StartOffset, Vec<i64>),
    (PartitionCount, i32, Default(-1)),
    (ReplicationFactor, i32, Default(-1)),
    (RetentionBytes, i64),
    (RetentionMs, i64)
);

/// The config options we expect to pass along when connecting to librdkafka
#[derive(Debug)]
pub struct LibRdKafkaConfig(pub BTreeMap<String, StringOrSecret>);

impl TryFrom<&KafkaConfigOptionExtracted> for LibRdKafkaConfig {
    type Error = PlanError;
    fn try_from(
        KafkaConfigOptionExtracted {
            acks,
            client_id,
            enable_auto_commit,
            enable_idempotence,
            fetch_message_max_bytes,
            isolation_level,
            statistics_interval_ms,
            topic_metadata_refresh_interval_ms,
            transaction_timeout_ms,
            ..
        }: &KafkaConfigOptionExtracted,
    ) -> Result<LibRdKafkaConfig, Self::Error> {
        let mut o = BTreeMap::new();

        macro_rules! fill_options {
            // Values that are not option can just be wrapped in some before being passed to the macro
            ($v:expr, $s:expr) => {
                if let Some(v) = $v {
                    o.insert($s.to_string(), StringOrSecret::String(v.to_string()));
                }
            };
            ($v:expr, $s:expr, $check:expr, $err:expr) => {
                if let Some(v) = $v {
                    if !$check(v) {
                        sql_bail!($err);
                    }
                    o.insert($s.to_string(), StringOrSecret::String(v.to_string()));
                }
            };
        }

        fill_options!(acks, "acks");
        fill_options!(client_id, "client.id");
        fill_options!(
            Some(statistics_interval_ms),
            "statistics.interval.ms",
            |i: &i32| { 0 <= *i && *i <= 86_400_000 },
            "STATISTICS INTERVAL MS must be within [0, 86,400,000]"
        );
        fill_options!(
            topic_metadata_refresh_interval_ms,
            "topic.metadata.refresh.interval.ms",
            |i: &i32| { 0 <= *i && *i <= 3_600_000 },
            "TOPIC METADATA REFRESH INTERVAL MS must be within [0, 3,600,000]"
        );
        fill_options!(enable_auto_commit, "enable.auto.commit");
        fill_options!(Some(isolation_level), "isolation.level");
        fill_options!(
            transaction_timeout_ms,
            "transaction.timeout.ms",
            |i: &i32| 0 <= *i,
            "TRANSACTION TIMEOUT MS must be greater than or equval to 0"
        );
        fill_options!(enable_idempotence, "enable.idempotence");
        fill_options!(
            fetch_message_max_bytes,
            "fetch.message.max_bytes",
            // The range of values comes from `fetch.message.max.bytes` in
            // https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md
            |i: &i32| { 0 <= *i && *i <= 1_000_000_000 },
            "FETCH MESSAGE MAX BYTES must be within [0, 1,000,000,000]"
        );

        Ok(LibRdKafkaConfig(o))
    }
}

/// An enum that represents start offsets for a kafka consumer.
#[derive(Debug)]
pub enum KafkaStartOffsetType {
    /// Fully specified, either by the user or generated.
    StartOffset(Vec<i64>),
    /// Specified by the user.
    StartTimestamp(i64),
}

impl TryFrom<&KafkaConfigOptionExtracted> for Option<KafkaStartOffsetType> {
    type Error = PlanError;
    fn try_from(
        KafkaConfigOptionExtracted {
            start_offset,
            start_timestamp,
            ..
        }: &KafkaConfigOptionExtracted,
    ) -> Result<Option<KafkaStartOffsetType>, Self::Error> {
        Ok(match (start_offset, start_timestamp) {
            (Some(_), Some(_)) => {
                sql_bail!("cannot specify START TIMESTAMP and START OFFSET at same time")
            }
            (Some(so), _) => Some(KafkaStartOffsetType::StartOffset(so.clone())),
            (_, Some(sto)) => Some(KafkaStartOffsetType::StartTimestamp(*sto)),
            _ => None,
        })
    }
}

/// Create a new `rdkafka::ClientConfig` with the provided
/// [`options`](https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md),
/// and test its ability to create an `rdkafka::consumer::BaseConsumer`.
///
/// Expected to test the output of `extract_security_config`.
///
/// # Panics
///
/// - `options` does not contain `bootstrap.servers` as a key
///
/// # Errors
///
/// - `librdkafka` cannot create a BaseConsumer using the provided `options`.
pub async fn create_consumer(
    topic: &str,
    kafka_connection: &KafkaConnection,
    options: &BTreeMap<String, StringOrSecret>,
    librdkafka_log_level: tracing::Level,
    secrets_reader: &dyn SecretsReader,
) -> Result<Arc<BaseConsumer<KafkaErrCheckContext>>, PlanError> {
    let mut config = create_new_client_config(librdkafka_log_level);
    mz_storage::types::connections::populate_client_config(
        kafka_connection.clone(),
        options,
        std::collections::HashSet::new(),
        &mut config,
        secrets_reader,
    )
    .await;

    // We need this only for logging which broker we're connecting to; the
    // setting itself makes its way into `config`.
    let broker = config
        .get("bootstrap.servers")
        .expect("callers must have already set bootstrap.servers");

    let consumer: Arc<BaseConsumer<KafkaErrCheckContext>> = Arc::new(
        config
            .create_with_context(KafkaErrCheckContext::default())
            .map_err(|e| sql_err!("{}", e))?,
    );
    let context = Arc::clone(&consumer.context());
    let owned_topic = String::from(topic);
    // Wait for a metadata request for up to one second. This greatly
    // increases the probability that we'll see a connection error if
    // e.g. the hostname was mistyped. librdkafka doesn't expose a
    // better API for asking whether a connection succeeded or failed,
    // unfortunately.
    task::spawn_blocking(move || format!("kafka_set_metadata:{broker}:{topic}"), {
        let consumer = Arc::clone(&consumer);
        move || {
            let _ = consumer.fetch_metadata(Some(&owned_topic), Duration::from_secs(1));
        }
    })
    .await
    .map_err(|e| sql_err!("{}", e))?;
    let error = context.error.lock().expect("lock poisoned");
    if let Some(error) = &*error {
        sql_bail!("librdkafka: {}", error)
    }
    Ok(consumer)
}

/// Returns start offsets for the partitions of `topic` and the provided
/// `START TIMESTAMP` option.
///
/// For each partition, the returned offset is the earliest offset whose
/// timestamp is greater than or equal to the given timestamp for the
/// partition. If no such message exists (or the Kafka broker is before
/// 0.10.0), the current end offset is returned for the partition.
///
/// The provided `START TIMESTAMP` option must be a non-zero number:
/// * Non-Negative numbers will used as is (e.g. `1622659034343`)
/// * Negative numbers will be translated to a timestamp in millis
///   before now (e.g. `-10` means 10 millis ago)
///
/// If `START TIMESTAMP` has not been configured, an empty Option is
/// returned.
pub async fn lookup_start_offsets(
    consumer: Arc<BaseConsumer<KafkaErrCheckContext>>,
    topic: &str,
    offsets: KafkaStartOffsetType,
    now: u64,
) -> Result<Option<Vec<i64>>, PlanError> {
    let time_offset = match offsets {
        KafkaStartOffsetType::StartTimestamp(time) => time,
        _ => return Ok(None),
    };

    let time_offset = if time_offset < 0 {
        let now: i64 = now.try_into()?;
        let ts = now - time_offset.abs();

        if ts <= 0 {
            sql_bail!("Relative START TIMESTAMP must be smaller than current system timestamp")
        }
        ts
    } else {
        time_offset
    };

    // Lookup offsets
    // TODO(guswynn): see if we can add broker to this name
    task::spawn_blocking(|| format!("kafka_lookup_start_offsets:{topic}"), {
        let topic = topic.to_string();
        move || {
            // There cannot be more than i32 partitions
            let num_partitions = mz_kafka_util::client::get_partitions(
                consumer.as_ref().client(),
                &topic,
                Duration::from_secs(10),
            )
            .map_err(|e| sql_err!("{}", e))?
            .len();

            let mut tpl = TopicPartitionList::with_capacity(1);
            tpl.add_partition_range(&topic, 0, num_partitions as i32 - 1);
            tpl.set_all_offsets(Offset::Offset(time_offset))
                .map_err(|e| sql_err!("{}", e))?;

            let offsets_for_times = consumer
                .offsets_for_times(tpl, Duration::from_secs(10))
                .map_err(|e| sql_err!("{}", e))?;

            // Translate to `start_offsets`
            let start_offsets = offsets_for_times
                .elements()
                .iter()
                .map(|elem| match elem.offset() {
                    Offset::Offset(offset) => Ok(offset),
                    Offset::End => fetch_end_offset(&consumer, &topic, elem.partition()),
                    _ => sql_bail!(
                        "Unexpected offset {:?} for partition {}",
                        elem.offset(),
                        elem.partition()
                    ),
                })
                .collect::<Result<Vec<_>, _>>()?;

            if start_offsets.len() != num_partitions {
                sql_bail!(
                    "Expected offsets for {} partitions, but received {}",
                    num_partitions,
                    start_offsets.len(),
                );
            }

            Ok(Some(start_offsets))
        }
    })
    .await
    .map_err(|e| sql_err!("{}", e))?
}

// Kafka supports bulk lookup of watermarks, but it is not exposed in rdkafka.
// If that ever changes, we will want to first collect all pids that have no
// offset for a given timestamp and then do a single request (instead of doing
// a request for each partition individually).
fn fetch_end_offset(
    consumer: &BaseConsumer<KafkaErrCheckContext>,
    topic: &str,
    pid: i32,
) -> Result<i64, PlanError> {
    let (_low, high) = consumer
        .fetch_watermarks(topic, pid, Duration::from_secs(10))
        .map_err(|e| sql_err!("{}", e))?;
    Ok(high)
}

/// Gets error strings from `rdkafka` when creating test consumers.
#[derive(Default, Debug)]
pub struct KafkaErrCheckContext {
    pub error: Mutex<Option<String>>,
}

impl ConsumerContext for KafkaErrCheckContext {}

impl ClientContext for KafkaErrCheckContext {
    // `librdkafka` doesn't seem to propagate all errors up the stack, but does
    // log them, so we are currently relying on the `log` callback for error
    // handling in some situations.
    fn log(&self, level: rdkafka::config::RDKafkaLogLevel, fac: &str, log_message: &str) {
        use rdkafka::config::RDKafkaLogLevel::*;
        // `INFO` messages with a `fac` of `FAIL` occur when e.g. connecting to
        // an SSL-authed broker without credentials.
        if fac == "FAIL" || matches!(level, Emerg | Alert | Critical | Error) {
            let mut error = self.error.lock().expect("lock poisoned");
            // Do not allow logging to overwrite other values if
            // present.
            if error.is_none() {
                *error = Some(log_message.to_string());
            }
        }
        MzClientContext.log(level, fac, log_message)
    }
    // Refer to the comment on the `log` callback.
    fn error(&self, error: rdkafka::error::KafkaError, reason: &str) {
        // Allow error to overwrite value irrespective of other conditions
        // (i.e. logging).
        *self.error.lock().expect("lock poisoned") = Some(reason.to_string());
        MzClientContext.error(error, reason)
    }
}

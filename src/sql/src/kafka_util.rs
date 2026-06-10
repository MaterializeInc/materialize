// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Provides parsing and convenience functions for working with Kafka from the `sql` package.

use std::collections::BTreeMap;
use std::sync::Arc;

use mz_kafka_util::client::DEFAULT_TOPIC_METADATA_REFRESH_INTERVAL;
use mz_ore::task;
use mz_sql_parser::ast::display::AstDisplay;
use mz_sql_parser::ast::{
    Expr, KafkaSinkConfigOption, KafkaSinkConfigOptionName, KafkaSourceConfigOption,
    KafkaSourceConfigOptionName,
};
use mz_storage_types::sinks::KafkaSinkCompressionType;
use rdkafka::consumer::{BaseConsumer, Consumer, ConsumerContext};
use rdkafka::{Offset, TopicPartitionList};
use tokio::time::Duration;

use crate::ast::Value;
use crate::catalog::SessionCatalog;
use crate::names::Aug;
use crate::normalize::generate_extracted_config;
use crate::plan::PlanError;
use crate::plan::with_options::{ImpliedValue, TryFromValue};

generate_extracted_config!(
    KafkaSourceConfigOption,
    (GroupIdPrefix, String),
    (Topic, String),
    (
        TopicMetadataRefreshInterval,
        Duration,
        Default(DEFAULT_TOPIC_METADATA_REFRESH_INTERVAL)
    ),
    (StartTimestamp, i64),
    (StartOffset, Vec<i64>)
);

generate_extracted_config!(
    KafkaSinkConfigOption,
    (
        CompressionType,
        KafkaSinkCompressionType,
        Default(KafkaSinkCompressionType::Lz4)
    ),
    (PartitionBy, Expr<Aug>),
    (ProgressGroupIdPrefix, String),
    (TransactionalIdPrefix, String),
    (LegacyIds, bool),
    (Topic, String),
    (TopicConfig, BTreeMap<String, String>),
    (
        TopicMetadataRefreshInterval,
        Duration,
        Default(DEFAULT_TOPIC_METADATA_REFRESH_INTERVAL)
    ),
    (TopicPartitionCount, i32),
    (TopicReplicationFactor, i32)
);

impl TryFromValue<Value> for KafkaSinkCompressionType {
    fn try_from_value(v: Value) -> Result<Self, PlanError> {
        match v {
            Value::String(v) => match v.to_lowercase().as_str() {
                "none" => Ok(KafkaSinkCompressionType::None),
                "gzip" => Ok(KafkaSinkCompressionType::Gzip),
                "snappy" => Ok(KafkaSinkCompressionType::Snappy),
                "lz4" => Ok(KafkaSinkCompressionType::Lz4),
                "zstd" => Ok(KafkaSinkCompressionType::Zstd),
                // The caller will add context, resulting in an error like
                // "invalid COMPRESSION TYPE: <bad-compression-type>".
                _ => sql_bail!("{}", v),
            },
            _ => sql_bail!("compression type must be a string"),
        }
    }

    fn try_into_value(self, _catalog: &dyn SessionCatalog) -> Option<Value> {
        Some(Value::String(match self {
            KafkaSinkCompressionType::None => "none".to_string(),
            KafkaSinkCompressionType::Gzip => "gzip".to_string(),
            KafkaSinkCompressionType::Snappy => "snappy".to_string(),
            KafkaSinkCompressionType::Lz4 => "lz4".to_string(),
            KafkaSinkCompressionType::Zstd => "zstd".to_string(),
        }))
    }

    fn name() -> String {
        "Kafka sink compression type".to_string()
    }
}

impl ImpliedValue for KafkaSinkCompressionType {
    fn implied_value() -> Result<Self, PlanError> {
        sql_bail!("must provide a compression type value")
    }
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
/// * Non-negative numbers will used as is (e.g. `1622659034343`)
/// * Negative numbers will be translated to a timestamp in millis
///   before now (e.g. `-10` means 10 millis ago)
pub async fn lookup_start_offsets<C>(
    consumer: Arc<BaseConsumer<C>>,
    topic: &str,
    time_offset: i64,
    now: u64,
    fetch_metadata_timeout: Duration,
) -> Result<Vec<i64>, PlanError>
where
    C: ConsumerContext + 'static,
{
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
                fetch_metadata_timeout,
            )
            .map_err(|e| sql_err!("{}", e))?
            .len();

            let num_partitions_i32 = i32::try_from(num_partitions)
                .map_err(|_| sql_err!("kafka topic had more than {} partitions", i32::MAX))?;

            let mut tpl = TopicPartitionList::with_capacity(1);
            tpl.add_partition_range(&topic, 0, num_partitions_i32 - 1);
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

            Ok(start_offsets)
        }
    })
    .await
}

// Kafka supports bulk lookup of watermarks, but it is not exposed in rdkafka.
// If that ever changes, we will want to first collect all pids that have no
// offset for a given timestamp and then do a single request (instead of doing
// a request for each partition individually).
fn fetch_end_offset<C>(consumer: &BaseConsumer<C>, topic: &str, pid: i32) -> Result<i64, PlanError>
where
    C: ConsumerContext,
{
    let (_low, high) = consumer
        .fetch_watermarks(topic, pid, Duration::from_secs(10))
        .map_err(|e| sql_err!("{}", e))?;
    Ok(high)
}

/// Validates that the provided start offsets are valid for the specified topic.
/// At present, the validation is merely that there are not more start offsets
/// than parts in the topic.
pub async fn validate_start_offsets<C>(
    consumer: Arc<BaseConsumer<C>>,
    topic: &str,
    start_offsets: Vec<i64>,
    fetch_metadata_timeout: Duration,
) -> Result<(), PlanError>
where
    C: ConsumerContext + 'static,
{
    // TODO(guswynn): see if we can add broker to this name
    task::spawn_blocking(|| format!("kafka_validate_start_offsets:{topic}"), {
        let topic = topic.to_string();
        move || {
            let num_partitions = mz_kafka_util::client::get_partitions(
                consumer.as_ref().client(),
                &topic,
                fetch_metadata_timeout,
            )
            .map_err(|e| sql_err!("{}", e))?
            .len();
            if start_offsets.len() > num_partitions {
                sql_bail!(
                    "START OFFSET specified more partitions ({}) than topic ({}) contains ({})",
                    start_offsets.len(),
                    topic,
                    num_partitions
                )
            }
            Ok(())
        }
    })
    .await
}

/// Validates that we can connect to the broker and obtain metadata about the topic.
pub async fn ensure_topic_exists<C>(
    consumer: Arc<BaseConsumer<C>>,
    topic: &str,
    fetch_metadata_timeout: Duration,
) -> Result<(), PlanError>
where
    C: ConsumerContext + 'static,
{
    task::spawn_blocking(|| format!("kafka_ensure_topic_exists:{topic}"), {
        let topic = topic.to_string();
        move || {
            mz_kafka_util::client::get_partitions(
                consumer.as_ref().client(),
                &topic,
                fetch_metadata_timeout,
            )
            .map_err(|e| sql_err!("{}", e))?;
            Ok(())
        }
    })
    .await
}

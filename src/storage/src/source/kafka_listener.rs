// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! A source that ingests records that Kafka producers write to this replica.
//!
//! The replica's [`KafkaServer`] accepts produce requests and hands the records for the source's
//! topics to this source. Every topic has a single partition, and the source acts as the log of
//! all of them: it assigns each record the next offset of one sequence shared by its topics, so
//! the source timestamp ([`MzOffset`]) is the partition offset, and a topic's offsets have gaps.
//!
//! The source's [`log`] is the durable copy of every record it accepts. A batch is acknowledged
//! once it is appended to the log, and its records are then emitted to every export of its
//! topic. Several exports can hold one topic, and each commits at its own pace. When the source
//! restarts, each export is sent the log's records at and beyond its upper again, so an export
//! created later is sent its topic's records from the start. Batches from idempotent producers
//! are deduplicated against the log, so a resent batch is acknowledged with its original offsets
//! and appended once. Batches from other producers are appended at least once.
//!
//! The unacknowledged bytes that make producers wait are bounded by the records emitted to a
//! topic's exports that some export has not committed yet, so a lagging export delays its topic's
//! producers.
//!
//! A committed Kafka transaction arrives as one batch, possibly with compacted records, such as
//! a Materialize Kafka sink's progress records. The source stores those in its log, and hands
//! the latest of each key back to the server when it registers the topic whose batch carried it.

use std::collections::{BTreeMap, VecDeque};
use std::convert::Infallible;
use std::sync::Arc;
use std::task::Poll;
use std::time::Duration;

use chrono::DateTime;
use differential_dataflow::AsCollection;
use futures::StreamExt;
use itertools::Itertools;
use mz_kafka_server::{
    Admission, CompactedRecord, KafkaServer, ProduceAck, ProduceBatch, ProducedRecord,
    ProducerStates, RecordValidator, SaslPlainCredentials, TopicOptions, TopicRegistration,
    TopicSchemas,
};
use mz_ore::cast::CastFrom;
use mz_ore::error::ErrorExt;
use mz_repr::adt::timestamp::CheckedTimestamp;
use mz_repr::{Datum, Diff, GlobalId, Row};
use mz_storage_types::errors::{DataflowError, SourceError, SourceErrorDetails};
use mz_storage_types::sources::encoding::{AvroEncoding, DataEncoding};
use mz_storage_types::sources::kafka::KafkaMetadataKind;
use mz_storage_types::sources::kafka_listener::kafka_listener_log_shard;
use mz_storage_types::sources::{
    KafkaListenerSourceConnection, MzOffset, SourceExportDataConfig, SourceExportDetails,
    SourceTimestamp,
};
use mz_storage_types::wire_format::WireFormat;
use mz_timely_util::builder_async::{OperatorBuilder as AsyncOperatorBuilder, PressOnDropButton};
use timely::container::CapacityContainerBuilder;
use timely::dataflow::channels::pact::Exchange;
use timely::dataflow::operators::Operator;
use timely::dataflow::operators::core::Partition;
use timely::dataflow::{Scope, StreamVec};
use timely::progress::Antichain;

use crate::healthcheck::{HealthStatusMessage, HealthStatusUpdate, StatusNamespace};
use crate::source::RawSourceCreationConfig;
use crate::source::generator::synthesize_probes;
use crate::source::types::{
    Probe, ResumeUppers, SignaledFuture, SourceMessage, SourceRender, StackedCollection,
};

mod log;
mod validate;

use log::{BatchEntry, CompactedEntry, Entry, Log, RecordEntry, Recovered};
use validate::{AllOf, ExportValidator};

/// How many bytes of produced records the source takes at once, at most. The source's frontier
/// advances past a whole group at a time.
const MAX_GROUP_BYTES: usize = 8 * 1024 * 1024;

/// The share of the replica's memory, as one in this many bytes, that a source's records may
/// take from the moment the source takes them until every export of their topic commits them.
/// Beyond it, producers wait. This bounds throughput as well as memory: a topic carries at most
/// its share divided by the time its records take to commit, which is about a second and a half
/// with a 1 s tick. A 32 GiB replica with a fixed 256 MiB per topic topped out at 1.4M records
/// of 100 bytes a second, and at 1.8M with 1 GiB.
///
/// NOTE: every Kafka listener source of a replica takes this share.
const UNCOMMITTED_MEMORY_SHARE: usize = 8;

/// The bytes a source's records may take until committed, when the replica's memory limit is
/// unknown.
const DEFAULT_UNCOMMITTED_BYTES: usize = 1024 * 1024 * 1024;

/// How a source bounds the memory of the records it has not committed, as
/// [`ProduceBatch::bytes`] counts them.
#[derive(Debug, Clone, Copy)]
struct MemoryBounds {
    /// Across all topics, which bounds the memory that lagging exports hold.
    total: usize,
    /// Per topic. Beyond this, the source stops taking the topic's batches until its exports
    /// catch up, so that a lagging topic does not stop the others.
    per_topic: usize,
    /// Per topic, of batches that the Kafka server holds until the source takes them.
    queued: usize,
}

impl MemoryBounds {
    fn new(cluster_memory_limit: Option<usize>) -> Self {
        let total = cluster_memory_limit
            .map_or(DEFAULT_UNCOMMITTED_BYTES, |m| m / UNCOMMITTED_MEMORY_SHARE);
        MemoryBounds {
            total,
            per_topic: total / 2,
            queued: total / 8,
        }
    }
}

/// How long the log remembers an idle idempotent producer. This is Kafka's default
/// `producer.id.expiration.ms`.
const PRODUCER_RETENTION_MS: i64 = 24 * 60 * 60 * 1000;

/// How often idle producers are forgotten.
const PRODUCER_EXPIRY_INTERVAL: Duration = Duration::from_secs(60);

/// How long to wait before retrying to set up the topics.
const SETUP_RETRY_INTERVAL: Duration = Duration::from_secs(1);

/// A record on its way to the export at the given output index, at the given offset.
type OutputRecord = (u64, usize, ProducedRecord);

impl SourceRender for KafkaListenerSourceConnection {
    type Time = MzOffset;
    const STATUS_NAMESPACE: StatusNamespace = StatusNamespace::KafkaListener;

    fn render<'scope>(
        self,
        scope: Scope<'scope, MzOffset>,
        config: &RawSourceCreationConfig,
        resume_uppers: impl futures::Stream<Item = ResumeUppers<MzOffset>> + 'static,
        start_signal: impl std::future::Future<Output = ()> + 'static,
    ) -> (
        BTreeMap<
            GlobalId,
            StackedCollection<'scope, MzOffset, Result<SourceMessage, DataflowError>>,
        >,
        StreamVec<'scope, MzOffset, HealthStatusMessage>,
        StreamVec<'scope, MzOffset, Probe<MzOffset>>,
        Vec<PressOnDropButton>,
    ) {
        let mut builder = AsyncOperatorBuilder::new(config.name.clone(), scope.clone());

        let (records_output, records) =
            builder.new_output::<CapacityContainerBuilder<Vec<OutputRecord>>>();
        let (_progress_output, progress_stream) =
            builder.new_output::<CapacityContainerBuilder<Vec<Infallible>>>();
        let (health_output, health_stream) = builder.new_output::<CapacityContainerBuilder<_>>();

        // The exports, in output order, with their topics and resume offsets.
        let exports: Vec<ExportInfo> = config
            .source_exports
            .iter()
            .map(|(id, export)| {
                let SourceExportDetails::KafkaListener(details) = &export.details else {
                    panic!("unexpected source export details: {:?}", export.details)
                };
                let resume_upper = Antichain::from_iter(
                    config
                        .source_resume_uppers
                        .get(id)
                        .expect("all source exports must be present in source resume uppers")
                        .iter()
                        .map(MzOffset::decode_row),
                );
                let validator = if self.validate_on_produce {
                    ExportValidator::new(
                        export.data_config.clone(),
                        format!("{}-validate-{id}", config.name),
                        config.metrics.decode_defs.clone(),
                        config.config.clone(),
                    )
                } else {
                    None
                };
                ExportInfo {
                    id: *id,
                    topic: details.topic.as_str().into(),
                    validator,
                    schemas: served_schemas(&export.data_config),
                    metadata_columns: details
                        .metadata_columns
                        .iter()
                        .map(|(_name, kind)| kind.clone())
                        .collect(),
                    resume_offset: resume_upper.into_option().map(|o| o.offset),
                }
            })
            .collect();
        let export_ids: Vec<_> = exports.iter().map(|e| e.id).collect();
        let metadata_columns: Vec<_> = exports.iter().map(|e| e.metadata_columns.clone()).collect();

        // Worker 0 receives every record. Turning records into messages, and everything
        // downstream of that until decoding exchanges them by value, is per-record work that
        // would otherwise also run on worker 0. Spreading records over all workers here, in runs
        // of 1000 offsets, moves that work off it.
        let peers = u64::cast_from(scope.peers());
        let messages = records.unary::<CapacityContainerBuilder<Vec<_>>, _, _, _>(
            Exchange::new(move |(offset, _, _): &OutputRecord| (offset / 1000) % peers),
            &format!("{}-messages", config.name),
            |_, _| {
                move |input, output| {
                    input.for_each_time(|time, data| {
                        let mut session = output.session(&time);
                        for (offset, export, record) in data.flat_map(|data| data.drain(..)) {
                            let offset = MzOffset::from(offset);
                            let message =
                                source_message(&record, offset, &metadata_columns[export]);
                            session.give(((export, message), offset, Diff::ONE));
                        }
                    });
                }
            },
        );

        let data_streams: Vec<_> = messages.partition::<CapacityContainerBuilder<_>, _, _>(
            u64::cast_from(export_ids.len()),
            |((output, data), time, diff): (
                (usize, Result<SourceMessage, DataflowError>),
                MzOffset,
                Diff,
            )| (u64::cast_from(output), (data, time, diff)),
        );
        let data_collections = export_ids
            .iter()
            .copied()
            .zip_eq(data_streams)
            .map(|(id, stream)| (id, stream.as_collection()))
            .collect();

        // A topic is served from one place, so a single worker runs the source. Clients reach a
        // replica's process 0 only, both locally and through the cluster's Kubernetes service,
        // and worker 0 is always in process 0.
        // NOTE: every Kafka listener source of a cluster therefore runs on the same worker.
        let is_active_worker = config.worker_id == 0;
        let source_id = config.id;
        let statistics = config.statistics.clone();
        let server = config.kafka_server.clone();
        let bounds = MemoryBounds::new(config.cluster_memory_limit);
        let secrets_reader = Arc::clone(&config.config.connection_context.secrets_reader);
        let busy_signal = Arc::clone(&config.busy_signal);
        let persist_clients = Arc::clone(&config.persist_clients);
        let log_location = config.remap_metadata.persist_location.clone();
        let log_shard = kafka_listener_log_shard(config.remap_metadata.data_shard);
        let now_fn = config.now_fn.clone();

        let button = builder.build(move |caps| {
            SignaledFuture::new(busy_signal, async move {
                let [mut data_cap, mut progress_cap, health_cap] = caps.try_into().unwrap();

                if !is_active_worker {
                    // Statistics are summed across workers. Report 0 to mark this worker as
                    // having started up.
                    for stats in statistics.values() {
                        stats.set_offset_known(0);
                        stats.set_offset_committed(0);
                    }
                    return;
                }

                // An export whose upper is empty takes no more records.
                let live: Vec<usize> = (0..exports.len())
                    .filter(|i| exports[*i].resume_offset.is_some())
                    .collect();
                // The source emits nothing below the lowest upper of its live exports. Records
                // between that and the log's end are replayed from the log.
                let Some(min_resume_offset) =
                    live.iter().filter_map(|i| exports[*i].resume_offset).min()
                else {
                    return;
                };
                data_cap.downgrade(&MzOffset::from(min_resume_offset));
                progress_cap.downgrade(&MzOffset::from(min_resume_offset));

                start_signal.await;

                let report = |update: HealthStatusUpdate| {
                    for id in export_ids.iter().copied().map(Some).chain([None]) {
                        health_output.give(
                            &health_cap,
                            HealthStatusMessage {
                                id,
                                namespace: StatusNamespace::KafkaListener,
                                update: update.clone(),
                            },
                        );
                    }
                };

                let Some(server) = server else {
                    report(HealthStatusUpdate::stalled(
                        "this cluster replica does not accept Kafka connections".into(),
                        Some(
                            "Replicas accept Kafka connections when provisioned while \
                             enable_replica_kafka_listener is on."
                                .into(),
                        ),
                    ));
                    std::future::pending::<()>().await;
                    unreachable!()
                };

                // The live exports of each topic, and the lowest offset that any of them resumes
                // at, from which the log replays the topic.
                let mut topic_outputs: BTreeMap<Arc<str>, Vec<usize>> = BTreeMap::new();
                let mut resume_offsets: BTreeMap<Arc<str>, u64> = BTreeMap::new();
                for i in &live {
                    let export = &exports[*i];
                    let offset = export.resume_offset.expect("live");
                    topic_outputs
                        .entry(Arc::clone(&export.topic))
                        .or_default()
                        .push(*i);
                    resume_offsets
                        .entry(Arc::clone(&export.topic))
                        .and_modify(|o| *o = (*o).min(offset))
                        .or_insert(offset);
                }

                // The log must be read before the topics accept records, so that producers that
                // resend batches find them.
                let (log, recovered) = loop {
                    let result = async {
                        let mut log =
                            Log::open(&persist_clients, log_location.clone(), log_shard, source_id)
                                .await?;
                        let recovered = log.recover(&resume_offsets).await?;
                        anyhow::Ok((log, recovered))
                    }
                    .await;
                    match result {
                        Ok(opened) => break opened,
                        Err(e) => {
                            report(HealthStatusUpdate::stalled(
                                e.display_with_causes().to_string(),
                                None,
                            ));
                            tokio::time::sleep(SETUP_RETRY_INTERVAL).await;
                        }
                    }
                };
                let Recovered {
                    mut producers,
                    compacted,
                    mut next_version,
                    retractions,
                    records: replay,
                    next_offset: log_next_offset,
                } = recovered;
                // An export can be ahead of the log only if it was written before the log held
                // records. Offsets below its upper must not be assigned again.
                let max_resume_offset = live
                    .iter()
                    .filter_map(|i| exports[*i].resume_offset)
                    .max()
                    .expect("live is not empty");
                let mut next_offset = log_next_offset.max(max_resume_offset);
                // The latest compacted record of each key.
                let mut compacted: BTreeMap<(String, bytes::Bytes), CompactedEntry> = compacted
                    .into_iter()
                    .map(|e| ((e.record.topic.clone(), e.record.key.clone()), e))
                    .collect();
                let (mut log, mut commits) = log.into_writer();
                // Entries to retract with the next append.
                let mut pending_retractions = retractions;
                // Groups whose log append has not committed yet, in order.
                let mut uncommitted: VecDeque<Group> = VecDeque::new();
                // The latest log append that committed, and the latest one started.
                let mut committed_token = None;
                let mut last_token = None;

                let registrations = loop {
                    match register(
                        &server,
                        &self,
                        &*secrets_reader,
                        &exports,
                        &topic_outputs,
                        &compacted,
                        bounds.queued,
                    )
                    .await
                    {
                        Ok(registrations) => break registrations,
                        Err(e) => {
                            let error = e.display_with_causes().to_string();
                            report(HealthStatusUpdate::stalled(error, None));
                            tokio::time::sleep(SETUP_RETRY_INTERVAL).await;
                        }
                    }
                };
                let mut topics: Vec<TopicState> = topic_outputs
                    .into_iter()
                    .zip_eq(registrations)
                    .map(|((topic, outputs), registration)| {
                        let committed_offset = resume_offsets[&topic];
                        TopicState {
                            producers: producers.remove(&topic).unwrap_or_default(),
                            topic,
                            outputs,
                            registration,
                            committed_offset,
                            pending: VecDeque::new(),
                            uncommitted_bytes: 0,
                        }
                    })
                    .collect();
                let mut total_uncommitted_bytes = 0;

                // Exports that restart, or start, behind the log are sent its records again.
                let topic_index: BTreeMap<Arc<str>, usize> = topics
                    .iter()
                    .enumerate()
                    .map(|(i, t)| (Arc::clone(&t.topic), i))
                    .collect();
                let replayed = replay.len();
                for entry in replay {
                    let topic = &topics[topic_index[&entry.topic]];
                    for output in &topic.outputs {
                        let resume = exports[*output].resume_offset.expect("live");
                        if entry.offset >= resume {
                            records_output
                                .give(&data_cap, (entry.offset, *output, entry.record.clone()));
                        }
                    }
                }
                data_cap.downgrade(&MzOffset::from(next_offset));
                progress_cap.downgrade(&MzOffset::from(next_offset));

                tracing::info!(
                    %source_id,
                    topics = ?topics.iter().map(|t| &t.topic).collect::<Vec<_>>(),
                    replayed,
                    "Kafka listener source accepting records at offset {next_offset}",
                );
                report(HealthStatusUpdate::running());
                for i in &live {
                    if let Some(stats) = statistics.get(&exports[*i].id) {
                        stats.set_offset_known(next_offset);
                        stats.set_offset_committed(exports[*i].resume_offset.expect("live"));
                    }
                }

                let mut resume_uppers = std::pin::pin!(resume_uppers);
                let mut expire_producers = tokio::time::interval(PRODUCER_EXPIRY_INTERVAL);
                // The topic polled first, which rotates so that a busy topic cannot starve
                // the others.
                let mut first_polled = 0;

                loop {
                    tokio::select! {
                        // Stop taking a topic's batches while its exports lag too far behind,
                        // which makes its producers wait.
                        (first, batch) = std::future::poll_fn(|cx| {
                            poll_topics(&mut topics, first_polled, bounds.per_topic, cx)
                        }), if total_uncommitted_bytes < bounds.total => {
                            first_polled = (first + 1) % topics.len();
                            let mut batches = vec![(first, batch)];
                            let mut group_bytes = batches[0].1.bytes;
                            topics[first].uncommitted_bytes += group_bytes;
                            let mut took = true;
                            while took && group_bytes < MAX_GROUP_BYTES {
                                took = false;
                                for (i, topic) in topics.iter_mut().enumerate() {
                                    if topic.uncommitted_bytes >= bounds.per_topic {
                                        continue;
                                    }
                                    let Some(batch) = topic.registration.try_recv() else {
                                        continue;
                                    };
                                    topic.uncommitted_bytes += batch.bytes;
                                    group_bytes += batch.bytes;
                                    batches.push((i, batch));
                                    took = true;
                                }
                            }
                            total_uncommitted_bytes += group_bytes;

                            let mut group = Group {
                                log_token: None,
                                upper: next_offset,
                                records: Vec::new(),
                                acks: Vec::new(),
                            };
                            let mut log_updates = Vec::new();
                            let now = i64::try_from(now_fn()).expect("now fits in i64");
                            for (i, batch) in batches {
                                let ProduceBatch {
                                    records,
                                    producer,
                                    compacted: compacted_records,
                                    ack,
                                    bytes,
                                } = batch;
                                let topic = &mut topics[i];
                                let base_offset =
                                    i64::try_from(group.upper).expect("offsets fit in i64");
                                let mut pending = PendingAck {
                                    ack,
                                    base_offset,
                                    bytes,
                                    compacted: Vec::new(),
                                };
                                if let Some(id) = &producer {
                                    match topic.producers.admit(id, records.len()) {
                                        Admission::Append => {}
                                        Admission::Duplicate { base_offset } => {
                                            // The original may still be committing. This group
                                            // completes after every append started before it.
                                            pending.base_offset = base_offset;
                                            group.acks.push((i, pending));
                                            continue;
                                        }
                                        Admission::Reject(error) => {
                                            topic.uncommitted_bytes -= bytes;
                                            total_uncommitted_bytes -= bytes;
                                            pending.ack.fail(error);
                                            continue;
                                        }
                                    }
                                    let (batch, evicted) =
                                        topic.producers.record(id, records.len(), base_offset, now);
                                    log_updates.push((
                                        Entry::Batch(BatchEntry {
                                            topic: Arc::clone(&topic.topic),
                                            producer_id: id.producer_id,
                                            epoch: id.epoch,
                                            batch,
                                        }),
                                        1,
                                    ));
                                    pending_retractions.extend(evicted.into_iter().map(
                                        |(epoch, batch)| {
                                            Entry::Batch(BatchEntry {
                                                topic: Arc::clone(&topic.topic),
                                                producer_id: id.producer_id,
                                                epoch,
                                                batch,
                                            })
                                        },
                                    ));
                                }
                                for record in records {
                                    log_updates.push((
                                        Entry::Record(RecordEntry {
                                            topic: Arc::clone(&topic.topic),
                                            offset: group.upper,
                                            record: record.clone(),
                                        }),
                                        1,
                                    ));
                                    group.records.push((group.upper, i, record));
                                    group.upper += 1;
                                }
                                for record in compacted_records {
                                    let entry = CompactedEntry {
                                        home: Arc::clone(&topic.topic),
                                        record,
                                        upper: group.upper,
                                        version: next_version,
                                    };
                                    next_version += 1;
                                    log_updates.push((Entry::Compacted(entry.clone()), 1));
                                    pending.compacted.push(entry);
                                }
                                group.acks.push((i, pending));
                            }
                            next_offset = group.upper;

                            if log_updates.is_empty() {
                                group.log_token = last_token;
                            } else {
                                log_updates.extend(
                                    pending_retractions.drain(..).map(|entry| (entry, -1)),
                                );
                                let token = log.append(log_updates);
                                group.log_token = Some(token);
                                last_token = Some(token);
                            }
                            uncommitted.push_back(group);
                        }
                        Some(commit) = commits.recv() => {
                            match commit {
                                Ok(token) => committed_token = Some(token),
                                Err(e) => {
                                    // Dropping the acks fails the batches with a retriable
                                    // error. Their producers retry against the restarted
                                    // source.
                                    drop(uncommitted);
                                    drop(topics);
                                    report(HealthStatusUpdate::halting(
                                        e.display_with_causes().to_string(),
                                        None,
                                    ));
                                    std::future::pending::<()>().await;
                                    unreachable!()
                                }
                            }
                        }
                        Some(uppers) = resume_uppers.next() => {
                            for topic in topics.iter_mut() {
                                let mut committed = None::<u64>;
                                for output in &topic.outputs {
                                    let export = &exports[*output];
                                    let Some(upper) = uppers
                                        .exports
                                        .get(&export.id)
                                        .and_then(|u| u.as_option())
                                    else {
                                        continue;
                                    };
                                    if let Some(stats) = statistics.get(&export.id) {
                                        stats.set_offset_committed(upper.offset);
                                    }
                                    let offset = upper.offset;
                                    committed = Some(committed.map_or(offset, |c| c.min(offset)));
                                }
                                if let Some(committed) = committed {
                                    topic.committed_offset = committed;
                                }
                                while topic
                                    .pending
                                    .front()
                                    .is_some_and(|(upper, _)| *upper <= topic.committed_offset)
                                {
                                    let (_, bytes) =
                                        topic.pending.pop_front().expect("checked above");
                                    topic.uncommitted_bytes -= bytes;
                                    total_uncommitted_bytes -= bytes;
                                }
                            }
                        }
                        _ = expire_producers.tick() => {
                            let now = i64::try_from(now_fn()).expect("now fits in i64");
                            let cutoff = now.saturating_sub(PRODUCER_RETENTION_MS);
                            for topic in topics.iter_mut() {
                                pending_retractions.extend(
                                    topic.producers.expire(cutoff).into_iter().map(
                                        |(producer_id, epoch, batch)| {
                                            Entry::Batch(BatchEntry {
                                                topic: Arc::clone(&topic.topic),
                                                producer_id,
                                                epoch,
                                                batch,
                                            })
                                        },
                                    ),
                                );
                            }
                        }
                    }

                    // Once a group's append commits, after the groups before it, its batches are
                    // durable: they are acknowledged and their records emitted to every export of
                    // their topic.
                    let mut emitted = false;
                    while uncommitted.front().is_some_and(|group| {
                        group
                            .log_token
                            .is_none_or(|t| committed_token.is_some_and(|c| t <= c))
                    }) {
                        let group = uncommitted.pop_front().expect("checked above");
                        for (offset, i, record) in group.records {
                            let outputs = &topics[i].outputs;
                            let (last, rest) = outputs.split_last().expect("a topic has exports");
                            for output in rest {
                                records_output.give(&data_cap, (offset, *output, record.clone()));
                            }
                            records_output.give(&data_cap, (offset, *last, record));
                        }
                        for (i, pending) in group.acks {
                            topics[i].pending.push_back((group.upper, pending.bytes));
                            for entry in pending.compacted {
                                let key = (entry.record.topic.clone(), entry.record.key.clone());
                                if let Some(old) = compacted.insert(key, entry) {
                                    pending_retractions.push(Entry::Compacted(old));
                                }
                            }
                            pending.ack.commit(pending.base_offset);
                        }
                        let upper = MzOffset::from(group.upper);
                        data_cap.downgrade(&upper);
                        progress_cap.downgrade(&upper);
                        emitted = true;
                    }
                    if emitted {
                        for i in &live {
                            if let Some(stats) = statistics.get(&exports[*i].id) {
                                stats.set_offset_known(data_cap.time().offset);
                            }
                        }
                    }
                }
            })
        });

        // A listener's snapshot is empty: no records exist until a producer sends some.
        let probe_stream = synthesize_probes(
            config.id,
            progress_stream,
            config.timestamp_interval,
            config.now_fn.clone(),
            true,
        );

        (
            data_collections,
            health_stream,
            probe_stream,
            vec![button.press_on_drop()],
        )
    }
}

/// An export of the source.
struct ExportInfo {
    id: GlobalId,
    topic: Arc<str>,
    validator: Option<Arc<dyn RecordValidator>>,
    schemas: TopicSchemas,
    metadata_columns: Vec<KafkaMetadataKind>,
    /// The offset to resume the export at, or `None` if it takes no more records.
    resume_offset: Option<u64>,
}

/// A topic the source serves, and the state of its exports.
struct TopicState {
    topic: Arc<str>,
    /// The output indexes of the topic's live exports.
    outputs: Vec<usize>,
    registration: TopicRegistration,
    producers: ProducerStates,
    /// The offset below which every export of the topic committed every record.
    committed_offset: u64,
    /// The upper of each acknowledged batch's group, and the batch's bytes, until every export
    /// of the topic commits past it, in offset order.
    pending: VecDeque<(u64, usize)>,
    /// The bytes of the topic's batches that some export has not committed.
    uncommitted_bytes: usize,
}

/// A batch whose producer waits for its acknowledgement.
struct PendingAck {
    ack: ProduceAck,
    base_offset: i64,
    bytes: usize,
    /// The compacted records that the batch's transaction committed.
    compacted: Vec<CompactedEntry>,
}

/// Produced batches that the source took at once.
struct Group {
    /// The log append the group waits for: its own, or for a group of duplicates only, the
    /// latest append started before it, which may hold their originals.
    log_token: Option<u64>,
    /// The offset after the group's last record.
    upper: u64,
    /// The records, at their offsets, with the index of their topic.
    records: Vec<(u64, usize, ProducedRecord)>,
    /// The group's batches, with the index of their topic.
    acks: Vec<(usize, PendingAck)>,
}

/// Polls the topics whose exports have room for more uncommitted bytes, starting at `first`,
/// and returns the first batch that is waiting, with its topic's index.
fn poll_topics(
    topics: &mut [TopicState],
    first: usize,
    max_uncommitted_bytes: usize,
    cx: &mut std::task::Context<'_>,
) -> Poll<(usize, ProduceBatch)> {
    let n = topics.len();
    for i in (0..n).map(|k| (first + k) % n) {
        let topic = &mut topics[i];
        if topic.uncommitted_bytes >= max_uncommitted_bytes {
            continue;
        }
        if let Poll::Ready(batch) = topic.registration.poll_recv(cx) {
            return Poll::Ready((i, batch));
        }
    }
    Poll::Pending
}

/// Claims `topics` on `server`, in order. A topic's exports are given as output indexes.
///
/// NOTE: the registry serves the schemas of a topic's first export with any.
async fn register(
    server: &KafkaServer,
    connection: &KafkaListenerSourceConnection,
    secrets_reader: &dyn mz_secrets::SecretsReader,
    exports: &[ExportInfo],
    topics: &BTreeMap<Arc<str>, Vec<usize>>,
    compacted: &BTreeMap<(String, bytes::Bytes), CompactedEntry>,
    queue_bytes: usize,
) -> Result<Vec<TopicRegistration>, anyhow::Error> {
    // NOTE: the password is read once, so changing the secret takes effect only when the source
    // restarts.
    let credentials = match &connection.sasl {
        Some(sasl) => Some(SaslPlainCredentials {
            username: sasl.username.clone(),
            password: secrets_reader.read_string(sasl.password).await?,
        }),
        None => None,
    };
    // Registrations made before a failure are released when dropped.
    let mut registrations = Vec::with_capacity(topics.len());
    for (topic, outputs) in topics {
        let validators: Vec<_> = outputs
            .iter()
            .filter_map(|i| exports[*i].validator.clone())
            .collect();
        let validator: Option<Arc<dyn RecordValidator>> = match validators.len() {
            0 => None,
            1 => validators.into_iter().next(),
            _ => Some(Arc::new(AllOf(validators))),
        };
        let schemas = outputs
            .iter()
            .map(|i| exports[*i].schemas.clone())
            .find(|s| s.key.is_some() || s.value.is_some())
            .unwrap_or_default();
        let restored: Vec<CompactedRecord> = compacted
            .values()
            .filter(|e| e.home == *topic)
            .map(|e| e.record.clone())
            .collect();
        registrations.push(server.register_topic(
            topic.to_string(),
            TopicOptions {
                credentials: credentials.clone(),
                validator,
                schemas,
                queue_bytes,
                compacted: restored,
            },
        )?);
    }
    Ok(registrations)
}

/// The schemas that the replica's schema registry serves for an export with `data_config`.
///
/// Only Avro schemas given inline with the Confluent wire format are served. Their decoder
/// ignores the schema ID in each record, so a record encoded with an ID that this registry
/// assigned decodes like any other. An export whose schemas come from a schema registry
/// connection resolves IDs in that registry, where an ID from this one names a different schema.
fn served_schemas(data_config: &SourceExportDataConfig) -> TopicSchemas {
    let inline_avro = |encoding: &DataEncoding| match encoding {
        DataEncoding::Avro(AvroEncoding {
            schema,
            reference_schemas,
            wire_format: WireFormat::Confluent { registry: None },
        }) if reference_schemas.is_empty() => Some(schema.clone()),
        _ => None,
    };
    match &data_config.encoding {
        Some(encoding) => TopicSchemas {
            key: encoding.key.as_ref().and_then(inline_avro),
            value: inline_avro(&encoding.value),
        },
        None => TopicSchemas::default(),
    }
}

/// Converts a produced record into the same message a Kafka source emits for it.
fn source_message(
    record: &ProducedRecord,
    offset: MzOffset,
    metadata_columns: &[KafkaMetadataKind],
) -> Result<SourceMessage, DataflowError> {
    let mut metadata = Row::default();
    let mut packer = metadata.packer();
    for kind in metadata_columns {
        match kind {
            KafkaMetadataKind::Partition => packer.push(Datum::Int32(0)),
            KafkaMetadataKind::Offset => packer.push(Datum::UInt64(offset.offset)),
            KafkaMetadataKind::Timestamp => {
                let datum: Datum = DateTime::from_timestamp_millis(record.timestamp)
                    .and_then(|dt| CheckedTimestamp::try_from(dt.naive_utc()).ok())
                    .into();
                packer.push(datum)
            }
            KafkaMetadataKind::Header { key, use_bytes } => {
                let value = record
                    .headers
                    .iter()
                    .filter(|(k, _)| k == key)
                    .last()
                    .ok_or_else(|| {
                        header_error(format!(
                            "A header with key '{key}' was not found in the message headers"
                        ))
                    })?;
                match &value.1 {
                    None => packer.push(Datum::Null),
                    Some(v) if *use_bytes => packer.push(Datum::Bytes(v)),
                    Some(v) => match std::str::from_utf8(v) {
                        Ok(s) => packer.push(Datum::String(s)),
                        Err(_) => {
                            return Err(header_error(format!(
                                "Found ill-formed byte sequence in header '{key}' that cannot be \
                                 decoded as valid utf-8 (original bytes: {v:x?})"
                            )));
                        }
                    },
                }
            }
            KafkaMetadataKind::Headers => packer.push_list_with(|list| {
                for (key, value) in &record.headers {
                    list.push_list_with(|record_row| {
                        record_row.push(Datum::String(key));
                        record_row.push(match value {
                            Some(v) => Datum::Bytes(v),
                            None => Datum::Null,
                        });
                    });
                }
            }),
        }
    }

    let bytes_row = |bytes: &Option<bytes::Bytes>| match bytes {
        Some(bytes) => Row::pack([Datum::Bytes(bytes)]),
        None => Row::pack([Datum::Null]),
    };
    Ok(SourceMessage {
        key: bytes_row(&record.key),
        value: bytes_row(&record.value),
        metadata,
    })
}

fn header_error(message: String) -> DataflowError {
    DataflowError::SourceError(Box::new(SourceError {
        error: SourceErrorDetails::Other(message.into()),
        hint: None,
    }))
}

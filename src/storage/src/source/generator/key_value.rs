// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::BTreeMap;
use std::convert::Infallible;
use std::sync::Arc;

use differential_dataflow::AsCollection;
use futures::stream::StreamExt;
use itertools::Itertools;
use mz_ore::cast::CastFrom;
use mz_ore::iter::IteratorExt;
use mz_repr::{Datum, Diff, GlobalId, Row};
use mz_storage_types::errors::DataflowError;
use mz_storage_types::sources::load_generator::{KeyValueLoadGenerator, LoadGeneratorOutput};
use mz_storage_types::sources::{MzOffset, SourceTimestamp};
use mz_timely_util::builder_async::{OperatorBuilder as AsyncOperatorBuilder, PressOnDropButton};
use mz_timely_util::containers::stack::AccountedStackBuilder;
use rand::rngs::StdRng;
use rand::{RngCore, SeedableRng};
use timely::container::CapacityContainerBuilder;
use timely::dataflow::operators::ToStream;
use timely::dataflow::operators::core::Partition;
use timely::dataflow::{Scope, Stream};
use timely::progress::{Antichain, Timestamp};
use tracing::info;

use crate::healthcheck::{HealthStatusMessage, HealthStatusUpdate, StatusNamespace};
use crate::source::types::{SignaledFuture, StackedCollection};
use crate::source::{RawSourceCreationConfig, SourceMessage};

pub fn render<G: Scope<Timestamp = MzOffset>>(
    key_value: KeyValueLoadGenerator,
    scope: &mut G,
    config: RawSourceCreationConfig,
    committed_uppers: impl futures::Stream<Item = Antichain<MzOffset>> + 'static,
    start_signal: impl std::future::Future<Output = ()> + 'static,
    output_map: BTreeMap<LoadGeneratorOutput, Vec<usize>>,
    idx_to_exportid: BTreeMap<usize, GlobalId>,
) -> (
    BTreeMap<GlobalId, StackedCollection<G, Result<SourceMessage, DataflowError>>>,
    Stream<G, Infallible>,
    Stream<G, HealthStatusMessage>,
    Vec<PressOnDropButton>,
) {
    // known and comitted offsets are recorded in the stats operator
    // It's easier to have this operator record the metrics rather than trying to special case it below.
    let stats_button = render_statistics_operator(scope, &config, committed_uppers);

    let mut builder = AsyncOperatorBuilder::new(config.name.clone(), scope.clone());

    let (data_output, stream) = builder.new_output::<AccountedStackBuilder<_>>();
    let partition_count = u64::cast_from(config.source_exports.len());
    let data_streams: Vec<_> = stream.partition::<CapacityContainerBuilder<_>, _, _>(
        partition_count,
        |((output, data), time, diff): &(
            (usize, Result<SourceMessage, DataflowError>),
            MzOffset,
            Diff,
        )| {
            let output = u64::cast_from(*output);
            (output, (data.clone(), time.clone(), diff.clone()))
        },
    );
    let mut data_collections = BTreeMap::new();
    for (id, data_stream) in config.source_exports.keys().zip_eq(data_streams) {
        data_collections.insert(*id, data_stream.as_collection());
    }

    let (_progress_output, progress_stream) = builder.new_output::<CapacityContainerBuilder<_>>();

    let busy_signal = Arc::clone(&config.busy_signal);

    // The key-value load generator only has one 'output' stream, which is the default output
    // that needs to be emitted to all output indexes.
    // Contains the `SourceStatistics` entries for exports that require a snapshot.
    let mut snapshot_export_stats = vec![];

    // We can't just iterate over config.statistics (which came from StorageState.aggregated_statistics)
    // AggregateStatistics will contain SourceStatistics for all exports of key-value load generator, not
    // just the exports of the config.id.
    let mut all_export_stats = vec![];
    let output_indexes = output_map
        .get(&LoadGeneratorOutput::Default)
        .expect("default output")
        .clone();
    for export_id in output_indexes.iter().map(|idx| {
        idx_to_exportid
            .get(idx)
            .expect("mapping of output index to export id")
    }) {
        let export_resume_upper = config
            .source_resume_uppers
            .get(export_id)
            .map(|rows| Antichain::from_iter(rows.iter().map(MzOffset::decode_row)))
            .expect("all source exports must be present in resume uppers");
        tracing::warn!(
            "source_id={} export_id={} worker_id={} resume_upper={:?}",
            config.id,
            export_id,
            config.worker_id,
            export_resume_upper
        );
        let export_stats = config
            .statistics
            .get(export_id)
            .expect("statistics initialized for export")
            .clone();
        if export_resume_upper.as_ref() == &[MzOffset::minimum()] {
            snapshot_export_stats.push(export_stats.clone());
        }
        all_export_stats.push(export_stats);
    }

    let button = builder.build(move |caps| {
        SignaledFuture::new(busy_signal, async move {
            let [mut cap, mut progress_cap]: [_; 2] = caps.try_into().unwrap();
            let stats_worker = config.responsible_for(0);
            let resume_upper = Antichain::from_iter(
                config
                    .source_resume_uppers
                    .values()
                    .flat_map(|f| f.iter().map(MzOffset::decode_row)),
            );

            let Some(resume_offset) = resume_upper.into_option() else {
                return;
            };
            let snapshotting = resume_offset.offset == 0;
            // A worker *must* emit a count even if not responsible for snapshotting a table
            // as statistic summarization will return null if any worker hasn't set a value.
            // This will also reset snapshot stats for any exports not snapshotting.
            if snapshotting {
                for stats in all_export_stats.iter() {
                    stats.set_snapshot_records_known(0);
                    stats.set_snapshot_records_staged(0);
                }
            }
            let mut local_partitions: Vec<_> = (0..key_value.partitions)
                .filter_map(|p| {
                    config
                        .responsible_for(p)
                        .then(|| TransactionalSnapshotProducer::new(p, key_value.clone()))
                })
                .collect();

            // worker has no work to do
            if local_partitions.is_empty() {
                return;
            }

            info!(
                ?config.worker_id,
                "starting key-value load generator at {}",
                resume_offset.offset,
            );
            cap.downgrade(&resume_offset);
            progress_cap.downgrade(&resume_offset);
            start_signal.await;
            info!(?config.worker_id, "received key-value load generator start signal");

            let local_snapshot_size = (u64::cast_from(local_partitions.len()))
                * key_value.keys
                * key_value.transactional_snapshot_rounds()
                / key_value.partitions;

            // Re-usable buffers.
            let mut value_buffer: Vec<u8> = vec![0; usize::cast_from(key_value.value_size)];

            let mut upper_offset = if snapshotting {
                let snapshot_rounds = key_value.transactional_snapshot_rounds();
                if stats_worker {
                    for stats in all_export_stats.iter() {
                        stats.set_offset_known(snapshot_rounds);
                    }
                }

                // Downgrade to the snapshot frontier.
                progress_cap.downgrade(&MzOffset::from(snapshot_rounds));

                let mut emitted = 0;
                for stats in snapshot_export_stats.iter() {
                    stats.set_snapshot_records_known(local_snapshot_size);
                }
                // output_map can contain no outputs, which would leave output_indexes empty
                let num_outputs = u64::cast_from(output_indexes.len()).max(1);
                while local_partitions.iter().any(|si| !si.finished()) {
                    for sp in local_partitions.iter_mut() {
                        let mut emitted_all_exports = 0;
                        for u in sp.produce_batch(&mut value_buffer, &output_indexes) {
                            data_output.give_fueled(&cap, u).await;
                            emitted_all_exports += 1;
                        }
                        // emitted_all_indexes is going to be some multiple of num_outputs;
                        emitted += emitted_all_exports / num_outputs;
                    }
                    for stats in snapshot_export_stats.iter() {
                        stats.set_snapshot_records_staged(emitted);
                    }
                }
                // snapshotting is completed
                snapshot_export_stats.clear();

                cap.downgrade(&MzOffset::from(snapshot_rounds));
                snapshot_rounds
            } else {
                cap.downgrade(&resume_offset);
                progress_cap.downgrade(&resume_offset);
                resume_offset.offset
            };

            let mut local_partitions: Vec<_> = (0..key_value.partitions)
                .filter_map(|p| {
                    config
                        .responsible_for(p)
                        .then(|| UpdateProducer::new(p, upper_offset, key_value.clone()))
                })
                .collect();
            if !local_partitions.is_empty()
                && (key_value.tick_interval.is_some() || !key_value.transactional_snapshot)
            {
                let mut interval = key_value.tick_interval.map(tokio::time::interval);

                loop {
                    if local_partitions.iter().all(|si| si.finished_quick()) {
                        if let Some(interval) = &mut interval {
                            interval.tick().await;
                        } else {
                            break;
                        }
                    }

                    for up in local_partitions.iter_mut() {
                        let (new_upper, iter) =
                            up.produce_batch(&mut value_buffer, &output_indexes);
                        upper_offset = new_upper;
                        for u in iter {
                            data_output.give_fueled(&cap, u).await;
                        }
                    }
                    cap.downgrade(&MzOffset::from(upper_offset));
                    progress_cap.downgrade(&MzOffset::from(upper_offset));
                }
            }
            std::future::pending::<()>().await;
        })
    });

    let status = [HealthStatusMessage {
        id: None,
        namespace: StatusNamespace::Generator,
        update: HealthStatusUpdate::running(),
    }]
    .to_stream(scope);
    (
        data_collections,
        progress_stream,
        status,
        vec![button.press_on_drop(), stats_button],
    )
}

/// An iterator that produces keys belonging to a partition.
struct PartitionKeyIterator {
    /// The partition.
    partition: u64,
    /// The number of partitions.
    partitions: u64,
    /// The key space.
    keys: u64,
    /// The next key.
    next: u64,
}

impl PartitionKeyIterator {
    fn new(partition: u64, partitions: u64, keys: u64, start_key: u64) -> Self {
        assert_eq!(keys % partitions, 0);
        PartitionKeyIterator {
            partition,
            partitions,
            keys,
            next: start_key,
        }
    }
}

impl Iterator for &mut PartitionKeyIterator {
    type Item = u64;

    fn next(&mut self) -> Option<Self::Item> {
        let ret = self.next;
        self.next = (self.next + self.partitions) % self.keys;
        Some(ret)
    }
}

/// Create `StdRng` seeded so identical values can be produced during resumption (during
/// snapshotting or after).
fn create_consistent_rng(source_seed: u64, offset: u64, partition: u64) -> StdRng {
    let mut seed = [0; 32];
    seed[0..8].copy_from_slice(&source_seed.to_le_bytes());
    seed[8..16].copy_from_slice(&offset.to_le_bytes());
    seed[16..24].copy_from_slice(&partition.to_le_bytes());
    StdRng::from_seed(seed)
}

/// A struct that produces batches of data for the snapshotting phase.
struct TransactionalSnapshotProducer {
    /// The key iterator for the partition.
    pi: PartitionKeyIterator,
    /// The batch size.
    batch_size: u64,
    /// The number of batches produced in the current round.
    produced_batches: u64,
    /// The expected number of batches per round.
    expected_batches: u64,
    /// The current round of the offset
    round: u64,
    /// The total number of rounds.
    snapshot_rounds: u64,
    /// The rng for the current round.
    rng: Option<StdRng>,
    /// The source-level seed for the rng.
    seed: u64,
    /// Whether to include the offset or not.
    include_offset: bool,
}

impl TransactionalSnapshotProducer {
    fn new(partition: u64, key_value: KeyValueLoadGenerator) -> Self {
        let snapshot_rounds = key_value.transactional_snapshot_rounds();

        let KeyValueLoadGenerator {
            partitions,
            keys,
            batch_size,
            seed,
            include_offset,
            ..
        } = key_value;

        assert_eq!((keys / partitions) % batch_size, 0);
        let pi = PartitionKeyIterator::new(
            partition, partitions, keys, // The first start key is the partition.
            partition,
        );
        TransactionalSnapshotProducer {
            pi,
            batch_size,
            produced_batches: 0,
            expected_batches: keys / partitions / batch_size,
            round: 0,
            snapshot_rounds,
            rng: None,
            seed,
            include_offset: include_offset.is_some(),
        }
    }

    /// If this partition is done snapshotting.
    fn finished(&self) -> bool {
        self.round >= self.snapshot_rounds
    }

    /// Produce a batch of message into `buffer`, of size `batch_size`. Advances the current
    /// batch and round counter.
    ///
    /// The output iterator must be consumed fully for this method to be used correctly.
    fn produce_batch<'a>(
        &'a mut self,
        value_buffer: &'a mut Vec<u8>,
        output_indexes: &'a [usize],
    ) -> impl Iterator<
        Item = (
            (usize, Result<SourceMessage, DataflowError>),
            MzOffset,
            Diff,
        ),
    > + 'a {
        let finished = self.finished();

        let rng = self
            .rng
            .get_or_insert_with(|| create_consistent_rng(self.seed, self.round, self.pi.partition));

        let partition: u64 = self.pi.partition;
        let iter_round: u64 = self.round;
        let include_offset: bool = self.include_offset;
        let iter = self
            .pi
            .take(if finished {
                0
            } else {
                usize::cast_from(self.batch_size)
            })
            .flat_map(move |key| {
                rng.fill_bytes(value_buffer.as_mut_slice());
                let msg = Ok(SourceMessage {
                    key: Row::pack_slice(&[Datum::UInt64(key)]),
                    value: Row::pack_slice(&[Datum::UInt64(partition), Datum::Bytes(value_buffer)]),
                    metadata: if include_offset {
                        Row::pack(&[Datum::UInt64(iter_round)])
                    } else {
                        Row::default()
                    },
                });
                output_indexes
                    .iter()
                    .repeat_clone(msg)
                    .map(move |(idx, msg)| ((*idx, msg), MzOffset::from(iter_round), Diff::ONE))
            });

        if !finished {
            self.produced_batches += 1;

            if self.produced_batches == self.expected_batches {
                self.round += 1;
                self.produced_batches = 0;
            }
        }

        iter
    }
}

/// A struct that produces batches of data for the post-snapshotting phase.
struct UpdateProducer {
    /// The key iterator for the partition.
    pi: PartitionKeyIterator,
    /// The batch size.
    batch_size: u64,
    /// The next offset to produce updates at.
    next_offset: u64,
    /// The source-level seed for the rng.
    seed: u64,
    /// The number of offsets we expect to be part of the `transactional_snapshot`.
    expected_quick_offsets: u64,
    /// Whether to include the offset or not.
    include_offset: bool,
}

impl UpdateProducer {
    fn new(partition: u64, next_offset: u64, key_value: KeyValueLoadGenerator) -> Self {
        let snapshot_rounds = key_value.transactional_snapshot_rounds();
        let quick_rounds = key_value.non_transactional_snapshot_rounds();
        let KeyValueLoadGenerator {
            partitions,
            keys,
            batch_size,
            seed,
            include_offset,
            ..
        } = key_value;

        // Each snapshot _round_ is associated with an offset, starting at 0. Therefore
        // the `next_offset` - `snapshot_rounds` is the index of the next non-snapshot update
        // batches we must produce. The start key for the that batch is that index
        // multiplied by the batch size and the number of partitions.
        let start_key =
            (((next_offset - snapshot_rounds) * batch_size * partitions) + partition) % keys;

        // We expect to emit 1 batch per offset. A round is emitting the full set of keys (in the
        // partition. The number of keys divided by the batch size (and partitioned, so divided by
        // the partition count) is the number of offsets in each _round_. We also add the number of
        // snapshot rounds, which is also the number of offsets in the snapshot.
        let expected_quick_offsets =
            ((keys / partitions / batch_size) * quick_rounds) + snapshot_rounds;

        let pi = PartitionKeyIterator::new(partition, partitions, keys, start_key);
        UpdateProducer {
            pi,
            batch_size,
            next_offset,
            seed,
            expected_quick_offsets,
            include_offset: include_offset.is_some(),
        }
    }

    /// If this partition is done producing quick updates.
    fn finished_quick(&self) -> bool {
        self.next_offset >= self.expected_quick_offsets
    }

    /// Produce a batch of message into `buffer`, of size `batch_size`. Advances the current
    /// batch and round counter. Also returns the frontier after the batch.
    ///
    /// The output iterator must be consumed fully for this method to be used correctly.
    fn produce_batch<'a>(
        &'a mut self,
        value_buffer: &'a mut Vec<u8>,
        output_indexes: &'a [usize],
    ) -> (
        u64,
        impl Iterator<
            Item = (
                (usize, Result<SourceMessage, DataflowError>),
                MzOffset,
                Diff,
            ),
        > + 'a,
    ) {
        let mut rng = create_consistent_rng(self.seed, self.next_offset, self.pi.partition);

        let partition: u64 = self.pi.partition;
        let iter_offset: u64 = self.next_offset;
        let include_offset: bool = self.include_offset;
        let iter = self
            .pi
            .take(usize::cast_from(self.batch_size))
            .flat_map(move |key| {
                rng.fill_bytes(value_buffer.as_mut_slice());
                let msg = Ok(SourceMessage {
                    key: Row::pack_slice(&[Datum::UInt64(key)]),
                    value: Row::pack_slice(&[Datum::UInt64(partition), Datum::Bytes(value_buffer)]),
                    metadata: if include_offset {
                        Row::pack(&[Datum::UInt64(iter_offset)])
                    } else {
                        Row::default()
                    },
                });
                output_indexes
                    .iter()
                    .repeat_clone(msg)
                    .map(move |(idx, msg)| ((*idx, msg), MzOffset::from(iter_offset), Diff::ONE))
            });

        // Advance to the next offset.
        self.next_offset += 1;
        (self.next_offset, iter)
    }
}

pub fn render_statistics_operator<G: Scope<Timestamp = MzOffset>>(
    scope: &G,
    config: &RawSourceCreationConfig,
    committed_uppers: impl futures::Stream<Item = Antichain<MzOffset>> + 'static,
) -> PressOnDropButton {
    let id = config.id;
    let builder =
        AsyncOperatorBuilder::new(format!("key_value_loadgen_statistics:{id}"), scope.clone());
    let offset_worker = config.responsible_for(0);
    let source_statistics = config.statistics.clone();
    let button = builder.build(move |caps| async move {
        drop(caps);
        if !offset_worker {
            // Emit 0, to mark this worker as having started up correctly.
            for stat in source_statistics.values() {
                stat.set_offset_committed(0);
                stat.set_offset_known(0);
            }
            return;
        }
        tokio::pin!(committed_uppers);
        loop {
            match committed_uppers.next().await {
                Some(frontier) => {
                    if let Some(offset) = frontier.as_option() {
                        for stat in source_statistics.values() {
                            stat.set_offset_committed(offset.offset);
                            stat.set_offset_known(offset.offset);
                        }
                    }
                }
                None => return,
            }
        }
    });
    button.press_on_drop()
}

#[cfg(test)]
mod test {
    use super::*;

    #[mz_ore::test]
    fn test_key_value_loadgen_resume_upper() {
        let up = UpdateProducer::new(
            1, // partition 1
            5, // resume upper of 5
            KeyValueLoadGenerator {
                keys: 126,
                snapshot_rounds: 2,
                transactional_snapshot: true,
                value_size: 1234,
                partitions: 3,
                tick_interval: None,
                batch_size: 2,
                seed: 1234,
                include_offset: None,
            },
        );

        // The first key 3 rounds after the 2 snapshot rounds would be the 7th key (3 rounds of 2
        // beforehand) produced for partition 1, so 13.
        assert_eq!(up.pi.next, 19);

        let up = UpdateProducer::new(
            1,           // partition 1
            5 + 126 / 2, // resume upper of 5 after a full set of keys has been produced.
            KeyValueLoadGenerator {
                keys: 126,
                snapshot_rounds: 2,
                transactional_snapshot: true,
                value_size: 1234,
                partitions: 3,
                tick_interval: None,
                batch_size: 2,
                seed: 1234,
                include_offset: None,
            },
        );

        assert_eq!(up.pi.next, 19);
    }

    #[mz_ore::test]
    fn test_key_value_loadgen_part_iter() {
        let mut pi = PartitionKeyIterator::new(
            1,   // partition 1
            3,   // of 3 partitions
            126, // 126 keys.
            1,   // Start at the beginning
        );

        assert_eq!(1, Iterator::next(&mut &mut pi).unwrap());
        assert_eq!(4, Iterator::next(&mut &mut pi).unwrap());

        // After a full round, ensure we wrap around correctly;
        let _ = pi.take((126 / 3) - 2).count();
        assert_eq!(pi.next, 1);
    }
}

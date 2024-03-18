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

use differential_dataflow::{AsCollection, Collection};
use futures::stream::StreamExt;
use mz_ore::cast::CastFrom;
use mz_repr::{Datum, Diff, Row};
use mz_storage_types::sources::load_generator::KeyValueLoadGenerator;
use mz_storage_types::sources::{MzOffset, SourceTimestamp};
use mz_timely_util::builder_async::{OperatorBuilder as AsyncOperatorBuilder, PressOnDropButton};
use rand::rngs::StdRng;
use rand::{RngCore, SeedableRng};
use timely::dataflow::operators::{Concat, ToStream};
use timely::dataflow::{Scope, Stream};
use timely::progress::Antichain;

use crate::healthcheck::{HealthStatusMessage, HealthStatusUpdate, StatusNamespace};
use crate::source::types::ProgressStatisticsUpdate;
use crate::source::{RawSourceCreationConfig, SourceMessage, SourceReaderError};

pub fn render<G: Scope<Timestamp = MzOffset>>(
    key_value: KeyValueLoadGenerator,
    scope: &mut G,
    config: RawSourceCreationConfig,
    resume_uppers: impl futures::Stream<Item = Antichain<MzOffset>> + 'static,
    start_signal: impl std::future::Future<Output = ()> + 'static,
) -> (
    Collection<G, (usize, Result<SourceMessage, SourceReaderError>), Diff>,
    Option<Stream<G, Infallible>>,
    Stream<G, HealthStatusMessage>,
    Stream<G, ProgressStatisticsUpdate>,
    Vec<PressOnDropButton>,
) {
    let (steady_state_stats_stream, stats_button) =
        render_statistics_operator(scope, config.clone(), resume_uppers);

    let mut builder = AsyncOperatorBuilder::new(config.name.clone(), scope.clone());

    let (mut data_output, stream) = builder.new_output();
    let (_progress_output, progress_stream) = builder.new_output();
    let (mut stats_output, stats_stream) = builder.new_output();

    let button = builder.build(move |caps| async move {
        let [mut cap, mut progress_cap, stats_cap]: [_; 3] = caps.try_into().unwrap();

        let resume_upper = Antichain::from_iter(
            config.source_resume_uppers[&config.id]
                .iter()
                .map(MzOffset::decode_row),
        );

        let Some(resume_offset) = resume_upper.into_option() else {
            return;
        };

        cap.downgrade(&resume_offset);
        progress_cap.downgrade(&resume_offset);
        start_signal.await;

        let snapshotting = resume_offset.offset == 0;

        let mut local_partitions: BTreeMap<_, _> = (0..key_value.partitions)
            .into_iter()
            .filter_map(|p| {
                config.responsible_for(p).then(|| {
                    (
                        p,
                        SnapshotProducer::new(
                            p,
                            key_value.partitions,
                            key_value.keys,
                            key_value.snapshot_rounds,
                            key_value.batch_size,
                            key_value.seed,
                            key_value.include_offset.is_some(),
                        ),
                    )
                })
            })
            .collect();

        let stats_worker = config.responsible_for(0);

        if local_partitions.is_empty() {
            stats_output
                .give(
                    &stats_cap,
                    ProgressStatisticsUpdate::Snapshot {
                        records_known: 0,
                        records_staged: 0,
                    },
                )
                .await;
            return;
        }

        let local_snapshot_size =
            (u64::cast_from(local_partitions.len())) * key_value.keys * key_value.snapshot_rounds
                / key_value.partitions;

        // Re-usable buffers.
        let mut value_buffer: Vec<u8> = vec![0; usize::cast_from(key_value.value_size)];
        let mut updates_buffer = Vec::new();

        // snapshotting
        let mut upper_offset = if snapshotting {
            if stats_worker {
                stats_output
                    .give(
                        &stats_cap,
                        ProgressStatisticsUpdate::SteadyState {
                            offset_known: key_value.snapshot_rounds,
                            offset_committed: 0,
                        },
                    )
                    .await;
            };

            // Downgrade to the snapshot frontier.
            progress_cap.downgrade(&MzOffset::from(key_value.snapshot_rounds));

            let mut emitted = 0;
            stats_output
                .give(
                    &stats_cap,
                    ProgressStatisticsUpdate::Snapshot {
                        records_known: local_snapshot_size,
                        records_staged: emitted,
                    },
                )
                .await;
            while local_partitions.values().any(|si| !si.finished()) {
                for sp in local_partitions.values_mut() {
                    updates_buffer.clear();
                    emitted += sp.produce_batch(&mut updates_buffer, &mut value_buffer);
                    data_output.give_container(&cap, &mut updates_buffer).await;

                    stats_output
                        .give(
                            &stats_cap,
                            ProgressStatisticsUpdate::Snapshot {
                                records_known: local_snapshot_size,
                                records_staged: emitted,
                            },
                        )
                        .await;
                }
                tokio::task::yield_now().await;
            }

            cap.downgrade(&MzOffset::from(key_value.snapshot_rounds));
            key_value.snapshot_rounds
        } else {
            cap.downgrade(&resume_offset);
            progress_cap.downgrade(&resume_offset);
            resume_offset.offset
        };

        let mut local_partitions: BTreeMap<_, _> = (0..key_value.partitions)
            .into_iter()
            .filter_map(|p| {
                config.responsible_for(p).then(|| {
                    (
                        p,
                        UpdateProducer::new(
                            p,
                            key_value.partitions,
                            key_value.keys,
                            key_value.snapshot_rounds,
                            key_value.quick_rounds,
                            key_value.batch_size,
                            key_value.seed,
                            upper_offset,
                            key_value.include_offset.is_some(),
                        ),
                    )
                })
            })
            .collect();
        if !local_partitions.is_empty()
            && (key_value.update_rate.is_some() || key_value.quick_rounds > 0)
        {
            let mut interval = key_value.update_rate.map(tokio::time::interval);

            loop {
                if local_partitions.values().all(|si| si.finished_quick()) {
                    if let Some(interval) = &mut interval {
                        interval.tick().await;
                    } else {
                        break;
                    }
                }

                for up in local_partitions.values_mut() {
                    updates_buffer.clear();
                    upper_offset = up.produce_batch(&mut updates_buffer, &mut value_buffer);
                    data_output.give_container(&cap, &mut updates_buffer).await;
                }
                cap.downgrade(&MzOffset::from(upper_offset));
                progress_cap.downgrade(&MzOffset::from(upper_offset));
                tokio::task::yield_now().await;
            }
        }

        std::future::pending::<()>().await;
    });

    let status = [HealthStatusMessage {
        index: 0,
        namespace: StatusNamespace::Generator,
        update: HealthStatusUpdate::running(),
    }]
    .to_stream(scope);
    let stats_stream = stats_stream.concat(&steady_state_stats_stream);

    (
        stream.as_collection(),
        Some(progress_stream),
        status,
        stats_stream,
        vec![button.press_on_drop(), stats_button],
    )
}

/// An iterator that produces keys belonging to a partition.
struct PartitionKeyIterator {
    // The partition.
    partition: u64,
    // The number of partitions.
    partitions: u64,
    // The key space.
    keys: u64,
    // The next key.
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

/// A struct that produces batches of data for the snapshotting phase.
struct SnapshotProducer {
    // The key iterator for the partition.
    pi: PartitionKeyIterator,
    // The batch size.
    batch_size: u64,
    // The number of batches produced in the current round.
    produced_batches: u64,
    // The expected number of batches per round.
    expected_batches: u64,
    // The current round of the offset
    round: u64,
    // The total number of rounds.
    snapshot_rounds: u64,
    // The rng for the current round.
    rng: Option<StdRng>,
    // The source-level seed for the rng.
    seed: u64,
    // Whether to include the offset or not.
    include_offset: bool,
}

impl SnapshotProducer {
    fn new(
        partition: u64,
        partitions: u64,
        keys: u64,
        snapshot_rounds: u64,
        batch_size: u64,
        seed: u64,
        include_offset: bool,
    ) -> Self {
        assert_eq!((keys / partitions) % batch_size, 0);
        let pi = PartitionKeyIterator::new(
            partition, partitions, keys, // The first start key is the partition.
            partition,
        );
        SnapshotProducer {
            pi,
            batch_size,
            produced_batches: 0,
            expected_batches: keys / partitions / batch_size,
            round: 0,
            snapshot_rounds,
            rng: None,
            seed,
            include_offset,
        }
    }

    /// If this partition is done snapshotting.
    fn finished(&self) -> bool {
        self.round >= self.snapshot_rounds
    }

    /// Produce a batch of message into `buffer`, of size `batch_size`. Advances the current
    /// batch and round counter.
    fn produce_batch(
        &mut self,
        buffer: &mut Vec<(
            (usize, Result<SourceMessage, SourceReaderError>),
            MzOffset,
            Diff,
        )>,
        value_buffer: &mut Vec<u8>,
    ) -> u64 {
        if self.finished() {
            return 0;
        }

        let rng = self.rng.get_or_insert_with(|| {
            // Consistently seeded with the source seed, the round (offset), and the
            // partition.
            let mut seed = [0; 32];
            seed[0..8].copy_from_slice(&self.seed.to_le_bytes());
            seed[8..16].copy_from_slice(&self.round.to_le_bytes());
            seed[16..24].copy_from_slice(&self.pi.partition.to_le_bytes());
            StdRng::from_seed(seed)
        });

        let partition = self.pi.partition;
        buffer.extend(self.pi.take(usize::cast_from(self.batch_size)).map(|key| {
            rng.fill_bytes(value_buffer.as_mut_slice());
            let msg = (
                0,
                Ok(SourceMessage {
                    key: Row::pack_slice(&[Datum::UInt64(key)]),
                    value: Row::pack_slice(&[Datum::UInt64(partition), Datum::Bytes(value_buffer)]),
                    metadata: if self.include_offset {
                        Row::pack(&[Datum::UInt64(self.round)])
                    } else {
                        Row::default()
                    },
                }),
            );
            (msg, MzOffset::from(self.round), 1)
        }));

        self.produced_batches += 1;

        if self.produced_batches == self.expected_batches {
            self.round += 1;
            self.produced_batches = 0;
        }
        self.batch_size
    }
}

/// A struct that produces batches of data for the post-snapshotting phase.
struct UpdateProducer {
    // The key iterator for the partition.
    pi: PartitionKeyIterator,
    // The batch size.
    batch_size: u64,
    // The next offset to produce updates at.
    next_offset: u64,
    // The source-level seed for the rng.
    seed: u64,
    // The number of offsets we expect to be part of the `quick_rounds`.
    expected_quick_offsets: u64,
    // Whether to include the offset or not.
    include_offset: bool,
}

impl UpdateProducer {
    fn new(
        partition: u64,
        partitions: u64,
        keys: u64,
        snapshot_rounds: u64,
        quick_rounds: u64,
        batch_size: u64,
        seed: u64,
        next_offset: u64,
        include_offset: bool,
    ) -> Self {
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
            include_offset,
        }
    }

    /// If this partition is done producing quick updates.
    fn finished_quick(&self) -> bool {
        self.next_offset >= self.expected_quick_offsets
    }

    /// Produce a batch of message into `buffer`, of size `batch_size`. Advances the current
    /// batch and round counter. Returns the frontier after the batch.
    fn produce_batch(
        &mut self,
        buffer: &mut Vec<(
            (usize, Result<SourceMessage, SourceReaderError>),
            MzOffset,
            Diff,
        )>,
        value_buffer: &mut Vec<u8>,
    ) -> u64 {
        // Consistently seeded with the source see, the offset, and the
        // partition.
        let mut seed = [0; 32];
        seed[0..8].copy_from_slice(&self.seed.to_le_bytes());
        seed[8..16].copy_from_slice(&self.next_offset.to_le_bytes());
        seed[16..24].copy_from_slice(&self.pi.partition.to_le_bytes());
        let mut rng = StdRng::from_seed(seed);

        let partition = self.pi.partition;
        buffer.extend(self.pi.take(usize::cast_from(self.batch_size)).map(|key| {
            rng.fill_bytes(value_buffer.as_mut_slice());
            let msg = (
                0,
                Ok(SourceMessage {
                    key: Row::pack_slice(&[Datum::UInt64(key)]),
                    value: Row::pack_slice(&[Datum::UInt64(partition), Datum::Bytes(value_buffer)]),
                    metadata: if self.include_offset {
                        Row::pack(&[Datum::UInt64(self.next_offset)])
                    } else {
                        Row::default()
                    },
                }),
            );
            (msg, MzOffset::from(self.next_offset), 1)
        }));

        // Advance to the next offset.
        self.next_offset += 1;
        self.next_offset
    }
}

pub fn render_statistics_operator<G: Scope<Timestamp = MzOffset>>(
    scope: &mut G,
    config: RawSourceCreationConfig,
    resume_uppers: impl futures::Stream<Item = Antichain<MzOffset>> + 'static,
) -> (Stream<G, ProgressStatisticsUpdate>, PressOnDropButton) {
    let id = config.id;
    let mut builder =
        AsyncOperatorBuilder::new(format!("key_value_loadgen_statistics:{id}"), scope.clone());

    let (mut stats_output, stats_stream) = builder.new_output();

    let button = builder.build(move |caps| async move {
        let [stats_cap]: [_; 1] = caps.try_into().unwrap();

        let offset_worker = config.responsible_for(0);

        if !offset_worker {
            // Emit 0, to mark this worker as having started up correctly.
            stats_output
                .give(
                    &stats_cap,
                    ProgressStatisticsUpdate::SteadyState {
                        offset_known: 0,
                        offset_committed: 0,
                    },
                )
                .await;
            return;
        }

        tokio::pin!(resume_uppers);
        loop {
            match resume_uppers.next().await {
                Some(frontier) => {
                    if let Some(offset) = frontier.as_option() {
                        stats_output
                            .give(
                                &stats_cap,
                                ProgressStatisticsUpdate::SteadyState {
                                    offset_known: offset.offset,
                                    offset_committed: offset.offset,
                                },
                            )
                            .await;
                    }
                }
                None => return,
            }
        }
    });

    (stats_stream, button.press_on_drop())
}

#[cfg(test)]
mod test {
    use super::*;

    #[mz_ore::test]
    fn test_key_value_loadgen_resume_upper() {
        let up = UpdateProducer::new(
            1,     // partition 1
            3,     // of 3 partitions
            126,   // 126 keys.
            2,     // 2 snapshot rounds
            0,     // not important
            2,     // batch size 2
            1234,  //seed
            5,     // resume upper of 5
            false, // not important
        );

        // The first key 3 rounds after the 2 snapshot rounds would be the 7th key (3 rounds of 2
        // beforehand) produced for partition 1, so 13.
        assert_eq!(up.pi.next, 19);

        let up = UpdateProducer::new(
            1,           // partition 1
            3,           // of 3 partitions
            126,         // 126 keys.
            2,           // 2 snapshot rounds
            0,           // not important
            2,           // batch size 2
            1234,        //seed
            5 + 126 / 2, // resume upper of 5 after a full set of keys has been produced.
            false,       // not important
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

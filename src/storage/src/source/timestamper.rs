// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Timestamper using persistent collection
use std::collections::{HashMap, VecDeque};
use std::pin::Pin;
use std::time::Duration;

use anyhow::{bail, Context as _};
use differential_dataflow::lattice::Lattice;
use futures::{Stream, StreamExt};
use mz_dataflow_types::client::controller::storage::CollectionMetadata;
use timely::progress::{Antichain, Timestamp as _};
use timely::PartialOrder;

use mz_dataflow_types::sources::MzOffset;
use mz_expr::PartitionId;
use mz_ore::now::NowFn;
use mz_persist_client::read::{ListenEvent, ReadHandle};
use mz_persist_client::write::WriteHandle;
use mz_persist_client::Upper;
use mz_repr::{Diff, Timestamp};
use tracing::{error, info};

pub struct CreateSourceTimestamper {
    name: String,
    read_progress: Antichain<Timestamp>,
    write_upper: Antichain<Timestamp>,
    persisted_timestamp_bindings: HashMap<PartitionId, VecDeque<(Timestamp, MzOffset)>>,
    read_cursors: HashMap<PartitionId, MzOffset>,
    write_handle: WriteHandle<(), PartitionId, Timestamp, Diff>,
    read_handle: ReadHandle<(), PartitionId, Timestamp, Diff>,
    timestamp_bindings_listener:
        Pin<Box<dyn Stream<Item = ListenEvent<(), PartitionId, Timestamp, Diff>>>>,
    now: NowFn,
}

impl CreateSourceTimestamper {
    pub async fn new(
        name: String,
        CollectionMetadata {
            persist_location,
            timestamp_shard_id,
            tx_timestamp_shard_id: _,
        }: CollectionMetadata,
        now: NowFn,
        mut as_of: Antichain<Timestamp>,
    ) -> anyhow::Result<Self> {
        let persist_client = persist_location
            .open()
            .await
            .with_context(|| "error creating persist client")?;

        let (write_handle, read_handle) = persist_client
            .open(timestamp_shard_id)
            .await
            .expect("persist handles open err");

        let read_progress = read_handle.since().clone();
        let write_upper = write_handle.upper().clone();

        // This is an invariant of the persist collection.
        assert!(
            timely::PartialOrder::less_equal(&read_progress, &write_upper),
            "{:?} PARTIAL ORDER: {:?} {:?}",
            name,
            read_progress,
            write_upper,
        );

        if timely::PartialOrder::less_than(&as_of, &read_progress) {
            bail!(
                "Source AS OF must not be less than since: {:?} vs {:?}",
                as_of,
                read_progress
            );
        }

        // XXX: what do we do if the write_upper is less than the as of? e.g. if we get a non-zero as of on a new source?
        // - Should that get special-cased and just CAA with empty up to the AS OF??
        // - Is it possible in general for a non-new source to have an AS OF that's not between since and upper? What to do?
        if timely::PartialOrder::less_than(&write_upper, &as_of) {
            error!(
                "Source upper must not be less than AS OF: {:?} vs {:?}.  Since: {:?}",
                write_upper, as_of, read_progress,
            );
            as_of.meet_assign(&write_upper);
        }

        let persisted_timestamp_bindings = if read_progress != write_upper {
            let mut snapshot_map = HashMap::new();
            let mut snap_iter = read_handle
                .snapshot(as_of.clone())
                .await
                .unwrap_or_else(|e| panic!("{:?} Read snapshot at handle.since ts {:?}", name, e));

            while let Some(v) = snap_iter.next().await {
                for ((key, value), timestamp, diff) in v {
                    let _: () = key.unwrap();
                    let partition = value.unwrap();
                    let (current_ts, current_offset) = snapshot_map
                        .entry(partition)
                        .or_insert((0, MzOffset::default()));
                    *current_offset += diff;
                    *current_ts = timestamp;
                }
            }

            snapshot_map
                .into_iter()
                .map(|(partition, (ts, offset))| {
                    (
                        partition,
                        VecDeque::from_iter(std::iter::once((ts, offset))),
                    )
                })
                .collect()
        } else {
            HashMap::new()
        };
        let read_cursors = HashMap::new();

        let timestamp_bindings_listener = read_handle
            .listen(as_of)
            .await
            .expect("Initial listen at handle.since ts")
            .into_stream()
            .boxed();

        Ok(Self {
            name,
            read_progress,
            write_upper,
            persisted_timestamp_bindings,
            read_cursors,
            write_handle,
            read_handle,
            timestamp_bindings_listener,
            now,
        })
    }

    pub fn partition_cursors(&self) -> HashMap<PartitionId, MzOffset> {
        self.persisted_timestamp_bindings
            .iter()
            .map(|(partition, bindings)| {
                (
                    partition.clone(),
                    bindings
                        .back()
                        .map(|(_ts, offset)| *offset)
                        .unwrap_or_default(),
                )
            })
            .collect()
    }

    // XXX: actually use this before merging.  Less granularity in ts seems to make it easier to catch bugs so I'll wait
    fn get_time(&self) -> Timestamp {
        let update_interval = 100;
        let new_ts = (self.now)();

        // Mod and then round up
        let remainder = new_ts % update_interval;
        if remainder == 0 {
            new_ts
        } else {
            new_ts - remainder + update_interval
        }
    }

    fn validate_persisted_bindings(&self) -> bool {
        for (partition, bindings) in self.persisted_timestamp_bindings.iter() {
            let mut last_ts = Timestamp::minimum();
            for (ts, _) in bindings.iter() {
                assert!(
                    *ts > last_ts,
                    "TS GOES BACKWARDS: {:?} -> {:?} in PART {:?}; BINDINGS: {:?}",
                    last_ts,
                    ts,
                    partition,
                    self.persisted_timestamp_bindings
                );
                last_ts = *ts;
            }
        }
        true
    }

    pub async fn timestamp_offsets(
        &mut self,
        observed_max_offsets: HashMap<PartitionId, MzOffset>,
    ) -> anyhow::Result<(
        HashMap<PartitionId, (Timestamp, MzOffset)>,
        Antichain<Timestamp>,
    )> {
        // XXX: make handling empty better: want to CAA once before returning
        let mut empty_flag = observed_max_offsets.is_empty();
        let mut matched_offsets = HashMap::new();
        loop {
            // See if we're able to assert a timestamp for any of the input offsets
            for (partition, offset) in observed_max_offsets.iter() {
                if matched_offsets.contains_key(partition) {
                    continue;
                }
                let bindings = match self.persisted_timestamp_bindings.get_mut(&partition) {
                    Some(bindings) => bindings,
                    None => continue,
                };

                // Compact as able, relying on all messages being in ascending offset order
                while bindings.len() > 1
                    && bindings
                        .front()
                        .expect("always at least one binding per partition")
                        .1
                        < *offset
                {
                    let (_old_timestamp, old_max_offset) = bindings.pop_front().unwrap();
                    let (_timestamp, incremental_offset) = bindings.front_mut().unwrap();
                    *incremental_offset += old_max_offset;
                }

                let (timestamp, max_offset) = bindings
                    .front()
                    .expect("always at least one binding per partition");
                if *offset <= *max_offset {
                    let old_offset = self.read_cursors.insert(partition.clone(), offset.clone());
                    match old_offset {
                        Some(old_offset) if old_offset > *offset => bail!(
                            "Offset shouldn't go backwards {:?} -> {:?}",
                            old_offset,
                            offset
                        ),
                        _ => {}
                    }
                    let old_match =
                        matched_offsets.insert(partition.clone(), (*timestamp, *offset));
                    assert_eq!(old_match, None);
                } else {
                    assert_eq!(bindings.len(), 1);
                    // Unable to match any more from this partition
                    break;
                }
            }

            debug_assert!(self.validate_persisted_bindings());

            // TODO(#12267): Properly downgrade `since` of the timestamp collection based on the source data collection.

            let new_bindings: Vec<_> = observed_max_offsets
                .iter()
                .filter(|(partition, _offset)| !matched_offsets.contains_key(partition))
                .map(|(partition, new_max_offset)| {
                    // Attempt to commit up to max offsets at current timestamp
                    let current_offset = self
                        .persisted_timestamp_bindings
                        .get(partition)
                        .map(|v| v.back().map(|(_ts, offset)| *offset))
                        .flatten()
                        .unwrap_or_default();
                    let diff = *new_max_offset - current_offset;
                    assert!(
                        diff > 0,
                        "Diff previously validated to be positive: {:?}",
                        diff
                    );
                    (partition, diff)
                })
                .collect();

            // If we have nothing new to propose, figure out the progress we ought to communicate and return.
            // N.B. If this was called with no new offsets, we need to do have at least one invocation of
            // `compare_and_append` to drive progress so we can't return immediately.
            if new_bindings.is_empty() && !empty_flag {
                assert_eq!(matched_offsets.len(), observed_max_offsets.len());

                // XXX: rewrite this comment (??)
                // Progress is min of:
                // - if partition in request: use ts of the output.  We know we CAN'T go backwards bceause caller must make requests with non-decreasing offsets
                // - if partition NOT in request: use ts for offset of read_cursor.  Highest message written out
                //     - if offset of read cursors MATCHES lowest offset AND there's a second binding, return the timestamp for the second binding -- as it'll eventually get compacted
                //     - if offset of read cursors MATCHES lowest offset AND there's NOT a second binding, we've written everything we know about in that partition so disregard in
                //       calculation (equiv to using `read_progress` for this partition)
                //     - if offset of read cursors is LESS THAN lowest offset, return that timestamp as we could still return a message
                // - Read_progress because anything will come after that

                // This should be a sensible value for the source to downgrade its capability to after emitting the messages that we return
                let progress = self
                    .persisted_timestamp_bindings
                    .iter()
                    .filter_map(|(partition, bindings)| {
                        if let Some((ts, _offset)) = matched_offsets.get(partition) {
                            return Some(*ts);
                        }

                        let (lo_binding_ts, lo_binding_offset) = bindings
                            .get(0)
                            .expect("Always have at least one binding per existing partition");
                        if let Some(read_cursor_offset) = self.read_cursors.get(partition) {
                            assert!(read_cursor_offset <= lo_binding_offset);
                            if read_cursor_offset == lo_binding_offset {
                                return bindings.get(1).map(|(ts, _offset)| *ts);
                            }
                        }
                        Some(*lo_binding_ts)
                    })
                    .chain(std::iter::once(
                        *self.read_progress.elements().first().unwrap_or(&u64::MAX),
                    ))
                    .min()
                    .unwrap_or_else(Timestamp::minimum);
                return Ok((matched_offsets, Antichain::from_elem(progress)));
            }
            empty_flag = false;

            // XXX: should clamp to round timestamp_frequency?? Also make sure it doesn't go backwards??
            let new_ts = (self.now)();
            let new_upper = Antichain::from_elem(new_ts + 1);

            // Can happen if clock skew between replicas.
            // TODO: push handling this into ts generation and remove sleep
            if PartialOrder::less_equal(&new_upper, &self.write_upper) {
                info!(
                    "Current write upper is not less than new upper: {:?} {:?}",
                    self.write_upper, new_upper
                );
                tokio::time::sleep(Duration::from_secs(1)).await;
                continue;
            }
            let compare_and_append_result = self
                .write_handle
                .compare_and_append(
                    new_bindings
                        .iter()
                        .map(|(partition, diff)| ((&(), *partition), &new_ts, diff)),
                    self.write_upper.clone(),
                    new_upper.clone(),
                )
                .await
                .expect("Timestamper CAA")
                .expect("Timestamper CAA 2");

            // We don't just return here on success because the logic to determine what progress to return is complex
            // and should be in one place only.
            self.write_upper = match compare_and_append_result {
                Ok(()) => new_upper,
                Err(Upper(actual_upper)) => actual_upper,
            };

            while PartialOrder::less_than(&self.read_progress, &self.write_upper) {
                match self
                    .timestamp_bindings_listener
                    .next()
                    .await
                    .expect("ListenStream doesn't end")
                {
                    ListenEvent::Progress(progress) => {
                        assert!(
                            timely::PartialOrder::less_equal(&self.read_progress, &progress),
                            "{:?} PARTIAL ORDER: {:?} {:?}",
                            self.name,
                            self.read_progress,
                            progress
                        );
                        self.read_progress = progress;
                    }
                    ListenEvent::Updates(updates) => {
                        for ((_, value), timestamp, diff) in updates {
                            let partition = value.expect("Unable to decode partition id");
                            self.persisted_timestamp_bindings
                                .entry(partition)
                                .or_insert_with(VecDeque::new)
                                .push_back((timestamp, MzOffset { offset: diff }));
                        }
                    }
                }
            }

            debug_assert!(self.validate_persisted_bindings());
        }
    }
}

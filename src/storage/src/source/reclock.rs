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
use mz_persist_client::read::ListenEvent;
use mz_persist_client::write::WriteHandle;
use mz_persist_client::Upper;
use mz_repr::Timestamp;
use tracing::{error, info};

pub struct ReclockOperator {
    name: String,
    // How far on the persist collection we've read
    read_progress: Antichain<Timestamp>,
    // Upper for the underlying persist collection
    write_upper: Antichain<Timestamp>,
    // If a binding is here, it has been persisted.  Previously returned bindings are compacted.
    persisted_timestamp_bindings: HashMap<PartitionId, VecDeque<(Timestamp, MzOffset)>>,
    // Max offset we've returned (or have committed to returning at the end of the current invocation).
    read_cursors: HashMap<PartitionId, MzOffset>,
    write_handle: WriteHandle<(), PartitionId, Timestamp, MzOffset>,
    timestamp_bindings_listener:
        Pin<Box<dyn Stream<Item = ListenEvent<(), PartitionId, Timestamp, MzOffset>>>>,
    now: NowFn,
    update_interval: Duration,
    last_timestamp: Timestamp,
}

impl ReclockOperator {
    pub async fn new(
        name: String,
        CollectionMetadata {
            persist_location,
            timestamp_shard_id,
            persist_shard: _,
        }: CollectionMetadata,
        now: NowFn,
        update_interval: Duration,
        mut as_of: Antichain<Timestamp>,
    ) -> anyhow::Result<Self> {
        let persist_client = persist_location
            .open()
            .await
            .with_context(|| "error creating persist client")?;

        let (write_handle, read_handle) = persist_client
            .open::<_, _, _, MzOffset>(timestamp_shard_id)
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

        assert!(
            timely::PartialOrder::less_equal(&read_progress, &as_of),
            "Source AS OF must not be less than since: {:?} vs {:?}",
            as_of,
            read_progress
        );

        if timely::PartialOrder::less_than(&write_upper, &as_of) {
            // Make an exception for empty sources
            if write_upper != Antichain::from_elem(Timestamp::minimum()) {
                panic!(
                    "Source upper must not be less than AS OF for non-empty source: {:?} vs {:?}.  Since: {:?}",
                    write_upper, as_of, read_progress,
                );
            }
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
            timestamp_bindings_listener,
            now,
            update_interval,
            last_timestamp: Timestamp::minimum(),
        })
    }

    // TODO: require NowFn to be monotonic so we can simplify this function
    fn get_time(&mut self) -> Option<Timestamp> {
        let update_interval: u64 = self
            .update_interval
            .clone()
            .as_millis()
            .try_into()
            .expect("interval millis don't fit into u64");
        let new_ts = (self.now)();

        // Mod and then round up
        let remainder = new_ts % update_interval;
        let new_ts_rounded = if remainder == 0 {
            new_ts
        } else {
            new_ts - remainder + update_interval
        };

        if new_ts_rounded > self.last_timestamp {
            self.last_timestamp = new_ts_rounded;
            Some(new_ts_rounded)
        } else {
            None
        }
    }

    // Ensure our persisted timestamp bindings go forward for a given partition.
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

    /// Given a mapping of highest observed offsets, for each provided partition id, return a `(Timestamp, MzOffset)`
    /// pair as well as an Antichain describing progress made.  Provided offsets must be increasing across calls to this
    /// function.  If `observed_max_offsets` is empty, one attempt will be made to increase the upper of underlying
    /// persist collection.  After that attempt, thre progress will be computed and returned.
    ///
    /// Return values:
    /// - The `Timestamp` returned is a persisted `Timestamp` that will be returned by any replica after any number of
    ///   restarts
    /// - Re: the return Antichain: Once a progress value is returned, all future invocations of this function will
    ///   return bindings not less than that progress value.  The progress value is non-descreasing across invocations.
    ///   The returned value should be a sensible value for the source to downgrade its capability to after emitting
    ///   all messages at offsets less_equal to the returned offsets.
    pub async fn timestamp_offsets<'a>(
        &mut self,
        observed_max_offsets: &'a HashMap<PartitionId, MzOffset>,
    ) -> anyhow::Result<(
        HashMap<&'a PartitionId, (Timestamp, &'a MzOffset)>,
        Antichain<Timestamp>,
    )> {
        let mut need_upper_advance = observed_max_offsets.is_empty();
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
                    let old_match = matched_offsets.insert(partition, (*timestamp, offset));
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
                    let diff = new_max_offset
                        .checked_sub(current_offset)
                        .expect("Diff previously validated to be positive");
                    (partition, diff)
                })
                .collect();

            // If we have nothing new to propose, figure out the progress we ought to communicate and return.
            // N.B. If this was called with no new offsets, we need to do have at least one invocation of
            // `compare_and_append` to drive progress so we can't return immediately.
            if new_bindings.is_empty() && !need_upper_advance {
                assert_eq!(matched_offsets.len(), observed_max_offsets.len());

                // Computing the progress is perhaps the most subtle part of reclocking.  For each partition, we compute
                // the lowest timestamp we could, in the future, return.  We then compute the minimum of those minimums.
                // (To be more general, we'd use `Lattice::meet` rather than the `Iterator::min` but we've already
                // hardcoded 1-D time in many places here.)
                //
                // For each partition, we compute the minimum as follows:
                // - If partition is in the request: use Timestamp we're returning.  We know we CAN'T go backwards
                //   because caller must make requests with non-decreasing offsets.
                // - If partition is NOT in request:
                //     - If offset of read_cursors MATCHES lowest persisted offset AND there's a second binding, return
                //       the timestamp for the second binding. Because we've been asked through `read_cursor` value, we
                //       know we will not ever need to serve anything prior to the second binding's timestamp.
                //     - If offset of read_cursors MATCHES lowest persisted offset AND there's NOT a second binding,
                //       we've returned everything we current about in that partition so disregard in the global minimun
                //       calculation (This is equivalent to using `read_progress` for this partition.)
                //     - If offset of read_cursors is LESS THAN lowest persisted offset, return that timestamp as we
                //       could still return a message.
                //
                // We take the minimum of all of these as well as `read_progress` as we know no new bindings can have a
                // time before that (because we read from the listener in ascending timestamp order).
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
                        *self.read_progress.as_option().unwrap_or(&u64::MAX),
                    ))
                    .min()
                    .unwrap_or_else(Timestamp::minimum);
                return Ok((matched_offsets, Antichain::from_elem(progress)));
            }
            need_upper_advance = false;

            let new_ts = match self.get_time() {
                Some(new_ts) => new_ts,
                None => {
                    // TODO: better handle time going backwards / switch to monotonic time that works across restarts.
                    //
                    // This is an error not because we don't expect it to happen but because we should handle it better.
                    // Importantly, `continue`ing here is _correct_.  Just likely not ideal beacause we'll stall until
                    // `self.now` catches up.
                    error!("Time unexpectedly didn't go forwards for {:?}", self.name);
                    tokio::time::sleep(self.update_interval).await;
                    continue;
                }
            };
            let new_upper = Antichain::from_elem(new_ts + 1);

            // Can happen if clock skew between replicas.
            if PartialOrder::less_equal(&new_upper, &self.write_upper) {
                info!(
                    "Current write upper is not less than new upper for {:?}: {:?} {:?}",
                    self.name, self.write_upper, new_upper
                );
                tokio::time::sleep(self.update_interval).await;
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
                                .push_back((timestamp, diff));
                        }
                    }
                }
            }

            debug_assert!(self.validate_persisted_bindings());
        }
    }
}

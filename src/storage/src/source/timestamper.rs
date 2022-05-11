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
use std::task::{Context, Waker};

use anyhow::Context as _;
use futures::{FutureExt, StreamExt};
use mz_dataflow_types::client::controller::storage::CollectionMetadata;
use timely::progress::{Antichain, Timestamp as _};
use timely::PartialOrder;

use mz_dataflow_types::sources::MzOffset;
use mz_expr::PartitionId;
use mz_ore::now::NowFn;
use mz_persist_client::read::{ListenEvent, ListenStream, ReadHandle};
use mz_persist_client::write::WriteHandle;
use mz_persist_client::Upper;
use mz_repr::{Diff, Timestamp};

pub struct CreateSourceTimestamper {
    name: String,
    read_progress: Antichain<Timestamp>,
    write_upper: Antichain<Timestamp>,
    persisted_timestamp_bindings: HashMap<PartitionId, VecDeque<(Timestamp, MzOffset)>>,
    write_handle: WriteHandle<(), PartitionId, Timestamp, Diff>,
    read_handle: ReadHandle<(), PartitionId, Timestamp, Diff>,
    timestamp_bindings_listener: ListenStream<(), PartitionId, Timestamp, Diff>,
    now: NowFn,
    waker: Waker,
}

impl CreateSourceTimestamper {
    pub async fn initialize(
        name: String,
        CollectionMetadata {
            persist_location,
            timestamp_shard_id,
            tx_timestamp_shard_id: _,
        }: CollectionMetadata,
        now: NowFn,
        waker: Waker,
    ) -> anyhow::Result<Option<Self>> {
        let persist_client = persist_location
            .open()
            .await
            .with_context(|| "error creating persist client")?;

        let mut persisted_timestamp_bindings = HashMap::new();

        let (mut write_handle, read_handle) = persist_client
            .open(timestamp_shard_id)
            .await
            .expect("persist handles open err");

        // Collection is closed.  No source to run
        if read_handle.since().is_empty() || write_handle.upper().is_empty() {
            return Ok(None);
        }

        let read_progress = read_handle.since().clone();

        // If this is the initialization of the persist collection, allow a read at Timestamp::minimum().  Else fetch
        // the current upper.
        let min_plus_one = Antichain::from_elem(Timestamp::minimum() + 1);
        let empty: [(((), PartitionId), Timestamp, Diff); 0] = [];
        let write_upper = write_handle
            .compare_and_append(
                empty,
                Antichain::from_elem(Timestamp::minimum()),
                min_plus_one.clone(),
            )
            .await
            .expect("Initial CAA")
            .with_context(|| "Initial compare and append")?
            .err()
            .map(|Upper(actual_upper)| actual_upper)
            .unwrap_or(min_plus_one);

        assert!(
            timely::PartialOrder::less_equal(&read_progress, &write_upper),
            "{:?} PARTIAL ORDER: {:?} {:?}",
            name,
            read_progress,
            write_upper,
        );

        let mut snap_iter = read_handle
            .snapshot(read_progress.clone())
            .await
            .unwrap_or_else(|e| panic!("{:?} Read snapshot at handle.since ts {:?}", name, e));

        loop {
            match snap_iter.next().await {
                None => break,
                Some(v) => {
                    for ((key, value), timestamp, diff) in v {
                        let _: () = key.unwrap();
                        let partition = value.unwrap();
                        // We can't compact yet because we don't know what offsets we'll need to serve
                        persisted_timestamp_bindings
                            .entry(partition)
                            .or_insert_with(VecDeque::new)
                            .push_back((timestamp, MzOffset { offset: diff }));
                    }
                }
            };
        }

        let timestamp_bindings_listener = read_handle
            .listen(read_progress.clone())
            .await
            .expect("Initial listen at handle.since ts")
            .into_stream();

        Ok(Some(Self {
            name,
            read_progress,
            write_upper,
            persisted_timestamp_bindings,
            write_handle,
            read_handle,
            timestamp_bindings_listener,
            now,
            waker,
        }))
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

    // return value: whether to continue
    pub async fn timestamp_offsets<'a>(
        &mut self,
        observed_max_offsets: HashMap<PartitionId, MzOffset>,
    ) -> anyhow::Result<
        Option<(
            HashMap<PartitionId, (Timestamp, MzOffset)>,
            Antichain<Timestamp>,
        )>,
    > {
        let mut matched_offsets = HashMap::new();
        let mut context = Context::from_waker(&self.waker);
        loop {
            if self.write_upper.is_empty() || self.read_handle.since().is_empty() {
                if !self.write_upper.is_empty() {
                    self.write_upper = Antichain::new();
                }

                // TODO(#12267): Properly downgrade `since` of the timestamp collection based on the source data collection.

                return Ok(None);
            }

            // See how much we can read in from the collection
            //while let Poll::Ready(Some(event)) = self
            //    .timestamp_bindings_listener
            //    .as_mut()
            //    .poll_next(&mut context)
            // XXX: this or while let Poll::Ready(Some(event)) = self.timestamp_bindings_listener.as_mut().poll_next(&mut context_from_timely_activator)??
            while let Some(Some(event)) = self.timestamp_bindings_listener.next().now_or_never() {
                match event {
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

            // Decide if we're up to date enough to attempt to persist new bindings!  If not, don't bother trying and shortcut to reading in again
            if !PartialOrder::less_equal(&self.write_upper, &self.read_progress) {
                // Use intentionally wrong value of `expected_upper` in order to update the upper for next iteration.
                let empty: [(((), PartitionId), Timestamp, Diff); 0] = [];
                self.write_upper = self
                    .write_handle
                    .compare_and_append(
                        empty,
                        Antichain::from_elem(Timestamp::minimum()),
                        Antichain::from_elem(Timestamp::minimum() + 1),
                    )
                    .await
                    .expect("Timestamper upper update CAA")
                    .with_context(|| "Timestamper upper update compare and append")?
                    .map_err(|Upper(actual_upper)| actual_upper)
                    .expect_err("Antichain was advanced past min on initialization");

                continue;
            }

            // Compact bindings we've read in and see if we're able to assert a timestamp for any of the input offsets
            for (partition, offset) in observed_max_offsets.iter() {
                if matched_offsets.contains_key(partition) {
                    continue;
                }
                let bindings = match self.persisted_timestamp_bindings.get_mut(&partition) {
                    Some(bindings) => bindings,
                    None => continue,
                };
                if bindings.is_empty() {
                    continue;
                }

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
                    let old_value =
                        matched_offsets.insert(partition.clone(), (*timestamp, *offset));
                    assert_eq!(old_value, None);
                } else {
                    assert_eq!(bindings.len(), 1);
                    // Unable to match any more from this partition
                    break;
                }
            }

            // TODO(#12267): Properly downgrade `since` of the timestamp collection based on the source data collection.

            // XXX: should clamp to round timestamp_frequency??
            let new_ts = (self.now)();
            let new_upper = Antichain::from_elem(new_ts + 1);

            let new_bindings: Vec<_> = observed_max_offsets
                .iter()
                .filter(|(partition, _offset)| !matched_offsets.contains_key(partition))
                .map(|(partition, new_max_offset)| {
                    // Attempt to commit up to max offsets at current timestamp
                    let current_offset = self
                        .persisted_timestamp_bindings
                        .entry(partition.clone())
                        .or_insert_with(VecDeque::new)
                        .back()
                        .map(|(_ts, offset)| *offset)
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

            if new_bindings.is_empty() {
                assert_eq!(matched_offsets.len(), observed_max_offsets.len());
                // This should be a sensible value for the source to downgrade its capability to after emitting the messages that we return
                let progress = matched_offsets
                    .values()
                    .map(|(ts, _offset)| *ts)
                    .chain(std::iter::once(
                        *self.read_progress.elements().first().unwrap_or(&u64::MAX),
                    ))
                    .min()
                    .unwrap_or_else(Timestamp::minimum);
                return Ok(Some((matched_offsets, Antichain::from_elem(progress))));
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

            self.write_upper = match compare_and_append_result {
                Ok(()) => new_upper,
                Err(Upper(actual_upper)) => actual_upper,
            }
        }
    }
}

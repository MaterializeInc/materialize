// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Timestamper using persistent collection
use std::collections::HashMap;
use std::time::Duration;

use anyhow::Context as _;
use mz_dataflow_types::client::controller::storage::CollectionMetadata;
use timely::progress::{Antichain, Timestamp as _};
use timely::PartialOrder;
use tokio::sync::mpsc::Receiver;

use mz_dataflow_types::sources::MzOffset;
use mz_expr::PartitionId;
use mz_ore::now::NowFn;
use mz_persist_client::read::{Listen, ListenEvent, ReadHandle};
use mz_persist_client::write::WriteHandle;
use mz_persist_client::Upper;
use mz_repr::{Diff, Timestamp};

pub struct CreateSourceTimestamper {
    name: String,
    read_progress: Antichain<Timestamp>,
    write_upper: Antichain<Timestamp>,
    partition_cursors: HashMap<PartitionId, MzOffset>,
    proposed_offsets: HashMap<PartitionId, MzOffset>,
    write_handle: WriteHandle<(), PartitionId, Timestamp, Diff>,
    read_handle: ReadHandle<(), PartitionId, Timestamp, Diff>,
    timestamp_bindings_listener: Listen<(), PartitionId, Timestamp, Diff>,
    now: NowFn,
    timestamp_frequency: Duration,
    bindings_channel: Option<Receiver<(PartitionId, MzOffset)>>,
}

impl CreateSourceTimestamper {
    pub async fn initialize(
        name: String,
        CollectionMetadata {
            persist_location,
            timestamp_shard_id,
        }: CollectionMetadata,
        now: NowFn,
        timestamp_frequency: Duration,
        bindings_channel: Receiver<(PartitionId, MzOffset)>,
    ) -> anyhow::Result<Option<Self>> {
        let persist_client = persist_location
            .open()
            .await
            .with_context(|| "error creating persist client")?;

        let mut partition_cursors = HashMap::new();
        let proposed_offsets = HashMap::new();

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
                    for ((key, value), _timestamp, diff) in v {
                        let _: () = key.unwrap();
                        let partition = value.unwrap();
                        let current_offset = partition_cursors
                            .entry(partition)
                            .or_insert_with(MzOffset::default);
                        *current_offset += diff;
                    }
                }
            };
        }

        let timestamp_bindings_listener = read_handle
            .listen(read_progress.clone())
            .await
            .expect("Initial listen at handle.since ts");

        Ok(Some(Self {
            name,
            read_progress,
            write_upper,
            partition_cursors,
            proposed_offsets,
            write_handle,
            read_handle,
            timestamp_bindings_listener,
            now,
            timestamp_frequency,
            bindings_channel: Some(bindings_channel),
        }))
    }

    // return value: whether to continue
    pub async fn invoke<'a>(&mut self) -> anyhow::Result<bool> {
        if self.write_upper.is_empty() || self.read_handle.since().is_empty() {
            eprintln!(
                "{:?} TS CLOSING {:?} {:?}",
                self.name,
                self.write_upper,
                self.read_handle.since()
            );
            if let Some(ref mut channel) = self.bindings_channel {
                channel.close();
            }

            if !self.write_upper.is_empty() {
                self.write_upper = Antichain::new();
            }

            // TODO(#12267): Properly downgrade `since` of the timestamp collection based on the source data collection.

            return Ok(false);
        }

        for event in self.timestamp_bindings_listener.next().await {
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
                    eprintln!(
                        "{:?} TIMESTAMPER READING UPDATES {:?}",
                        self.name,
                        updates.len()
                    );
                    for ((_, value), _timestamp, diff) in updates {
                        let partition = value.expect("Unable to decode partition id");
                        eprintln!(
                            "{:?} TIMESTAMPER READING UPDATE {:?} {:?}",
                            self.name, partition, diff
                        );
                        let cursor_offset = self
                            .partition_cursors
                            .entry(partition)
                            .or_insert_with(MzOffset::default);
                        *cursor_offset += diff;
                    }
                }
            }
        }

        // If we didn't persist `proposed_offsets` in the last invocation, they may be behind what we just read from the reader.
        self.proposed_offsets.retain(|partition, offset| {
            match self.partition_cursors.get(&partition) {
                Some(partition_offset) => *offset > *partition_offset,
                None => true,
            }
        });

        eprintln!(
            "{:?} TS PROPOSED OFFSETS PRE {:?}",
            self.name, self.proposed_offsets
        );

        // TODO(#12267): Properly downgrade `since` of the timestamp collection based on the source data collection.

        // XXX: should clamp to round timestamp_frequency??
        let recv_deadline = tokio::time::Instant::now() + self.timestamp_frequency;
        if let Some(ref mut bindings_channel) = self.bindings_channel {
            loop {
                match tokio::time::timeout_at(recv_deadline, bindings_channel.recv()).await {
                    Ok(Some((incoming_partition, incoming_offset))) => {
                        if let Some(proposed_offset) =
                            self.proposed_offsets.get(&incoming_partition)
                        {
                            if *proposed_offset >= incoming_offset {
                                continue;
                            }
                        };
                        if let Some(partition_offset) =
                            self.partition_cursors.get(&incoming_partition)
                        {
                            if *partition_offset >= incoming_offset {
                                continue;
                            }
                        }
                        self.proposed_offsets
                            .insert(incoming_partition.clone(), incoming_offset);
                    }
                    Ok(None) => {
                        // All senders dropped means we should start shutting down
                        self.bindings_channel = None;
                        break;
                    }
                    Err(_) => {
                        break;
                    }
                }
            }
        }
        eprintln!(
            "{:?} TS PROPOSED OFFSETS POST {:?}; UPPER: {:?}; PROGRESS: {:?}",
            self.name, self.proposed_offsets, self.write_upper, self.read_progress,
        );

        // Decide if we're up to date enough to drain!
        if !PartialOrder::less_equal(&self.write_upper, &self.read_progress) {
            eprintln!("{:?} TS NOT READ UP TO DATE", self.name);
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

            return Ok(true);
        }

        // XXX: should clamp to round timestamp_frequency??
        let new_ts = (self.now)();
        let new_upper = Antichain::from_elem(new_ts + 1);

        let new_bindings: Vec<_> = self
            .proposed_offsets
            .iter()
            .map(|(partition, new_max_offset)| {
                let current_offset = self
                    .partition_cursors
                    .entry(partition.clone())
                    .or_insert_with(MzOffset::default);
                let diff = *new_max_offset - *current_offset;
                assert!(
                    diff > 0,
                    "Diff previously validated to be positive: {:?}",
                    diff
                );
                (partition, diff)
            })
            .collect();

        eprintln!(
            "{:?} TS NEW BINDINGS {:?} AT {:?}",
            self.name, new_bindings, new_ts
        );

        // If we've drained all bindings and all senders have dropped, we should begin shutting down.
        if new_bindings.is_empty() && self.bindings_channel.is_none() {
            self.write_upper = Antichain::new();
            return Ok(true);
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
        };

        Ok(true)
    }
}

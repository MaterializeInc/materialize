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
use std::task::{Context, Poll, Waker};
use std::time::{Duration, Instant};

use mz_dataflow_types::SourceError;
use timely::dataflow::channels::pushers::TeeCore;
use timely::dataflow::operators::generic::OutputHandle;
use timely::dataflow::operators::Capability;
use timely::scheduling::Activator;
use tokio::sync::mpsc::Sender;

use mz_dataflow_types::sources::MzOffset;
use mz_expr::PartitionId;
use mz_ore::now::NowFn;
use mz_repr::{GlobalId, Timestamp};
use tracing::info;

use crate::source::{
    SourceMessage, SourceReader, SourceReaderStream, SourceStatus, YIELD_INTERVAL,
};

pub struct CreateSourceIngestor<S: SourceReader> {
    name: String,
    source_reader: Option<SourceReaderStream<S>>,
    activator: Activator,
    waker: Waker,
    partition_cursors: HashMap<PartitionId, MzOffset>,
    timestamp_frequency: Duration,
    source_id: GlobalId,
    bindings_channel: Sender<(PartitionId, MzOffset)>,
    now: NowFn,
}

impl<S: SourceReader> CreateSourceIngestor<S> {
    pub fn initialize(
        name: String,
        source_reader: Option<SourceReaderStream<S>>,
        activator: Activator,
        waker: Waker,
        timestamp_frequency: Duration,
        source_id: GlobalId,
        bindings_channel: Sender<(PartitionId, MzOffset)>,
        now: NowFn,
    ) -> anyhow::Result<Option<Self>> {
        let partition_cursors = HashMap::new();
        Ok(Some(Self {
            name,
            source_reader,
            activator,
            waker,
            partition_cursors,
            timestamp_frequency,
            source_id,
            bindings_channel,
            now,
        }))
    }

    pub async fn invoke<'a>(
        &mut self,
        mut output: OutputHandle<
            'a,
            Timestamp,
            Option<
                Result<
                    SourceMessage<<S as SourceReader>::Key, <S as SourceReader>::Value>,
                    SourceError,
                >,
            >,
            TeeCore<
                Timestamp,
                Vec<
                    Option<
                        Result<
                            SourceMessage<<S as SourceReader>::Key, <S as SourceReader>::Value>,
                            SourceError,
                        >,
                    >,
                >,
            >,
        >,
        cap: &mut Capability<Timestamp>,
    ) -> anyhow::Result<SourceStatus> {
        // Bound execution of operator to prevent a single operator from hogging
        // the CPU if there are many messages to process
        let timer = Instant::now();
        let mut context = Context::from_waker(&self.waker);

        // We need to move the capability forward so that downstream timely operators can move forward -- even though
        // we don't really care about the value of the time here.
        let new_cap = cap.delayed(&(self.now)());
        let mut session = output.session(&new_cap);

        match self.source_reader {
            Some(ref mut reader) => {
                while let Poll::Ready(item) = reader.as_mut().poll_next(&mut context) {
                    match item {
                        Some(Ok(message)) => {
                            // N.B. Messages must arrive in increasing offset order
                            if let Some(old_offset) = self
                                .partition_cursors
                                .insert(message.partition.clone(), message.offset.clone())
                            {
                                assert!(
                                    message.offset >= old_offset,
                                    "Offsets must arrive in increasing order: {:?} -> {:?}",
                                    old_offset,
                                    message.offset
                                );
                            }
                            session.give(Some(Ok(message)));
                        }
                        Some(Err(e)) => {
                            output.session(&cap).give(Some(Err(SourceError {
                                source_id: self.source_id,
                                error: e.inner,
                            })));
                            self.source_reader = None;
                            break;
                        }
                        None => {
                            session.give(None);
                            self.source_reader = None;
                            break;
                        }
                    }
                    if timer.elapsed() > YIELD_INTERVAL {
                        // We didn't drain the entire queue, so indicate that we
                        // should run again but yield the CPU to other operators.
                        self.activator.activate();
                        break;
                    }
                }
            }
            None => return Ok(SourceStatus::Done),
        }

        if self.bindings_channel.is_closed() {
            info!(
                "Bindings channel for ingestor for {:?} is closed",
                self.name
            );
            self.source_reader = None;
        }

        // Let the timestamper know about the new max bindings
        for (partition, offset) in self.partition_cursors.drain() {
            if let Err(_) = self.bindings_channel.send((partition, offset)).await {
                self.source_reader = None;
                break;
            }
        }

        if self.source_reader.is_some() {
            cap.downgrade(&new_cap);
            self.activator.activate_after(self.timestamp_frequency);
            Ok(SourceStatus::Alive)
        } else {
            info!("Shutting down ingestor for {:?}", self.name);
            cap.downgrade(&u64::MAX);
            Ok(SourceStatus::Done)
        }
    }
}

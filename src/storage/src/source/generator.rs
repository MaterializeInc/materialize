// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::time::{Duration, Instant};

use timely::scheduling::SyncActivator;

use mz_expr::PartitionId;
use mz_repr::{Diff, GlobalId, Row};

use super::metrics::SourceBaseMetrics;
use super::{SourceMessage, SourceMessageType};
use crate::source::{NextMessage, SourceReader, SourceReaderError};
use crate::types::connections::ConnectionContext;
use crate::types::sources::{encoding::SourceDataEncoding, MzOffset, SourceConnection};
use crate::types::sources::{Generator, LoadGenerator};

mod auction;
mod counter;

pub use auction::Auction;
pub use counter::Counter;

pub fn as_generator(g: &LoadGenerator) -> Box<dyn Generator> {
    match g {
        LoadGenerator::Auction => Box::new(Auction {}),
        LoadGenerator::Counter => Box::new(Counter {}),
    }
}

pub struct LoadGeneratorSourceReader {
    rows: Box<dyn Iterator<Item = Row>>,
    last: Instant,
    tick: Duration,
    offset: MzOffset,
    // Load-generator sources support single-threaded ingestion only, so only
    // one of the `LoadGeneratorSourceReader`s will actually produce data.
    active_read_worker: bool,
}

impl SourceReader for LoadGeneratorSourceReader {
    type Key = ();
    type Value = Row;
    // LoadGenerator can produce deletes that cause retractions
    type Diff = Diff;

    fn new(
        _source_name: String,
        source_id: GlobalId,
        worker_id: usize,
        worker_count: usize,
        _consumer_activator: SyncActivator,
        connection: SourceConnection,
        start_offsets: Vec<(PartitionId, Option<MzOffset>)>,
        _encoding: SourceDataEncoding,
        _metrics: SourceBaseMetrics,
        _connection_context: ConnectionContext,
    ) -> Result<Self, anyhow::Error> {
        let connection = match connection {
            SourceConnection::LoadGenerator(lg) => lg,
            _ => {
                panic!("LoadGenerator is the only legitimate SourceConnection for LoadGeneratorSourceReader")
            }
        };

        let active_read_worker =
            crate::source::responsible_for(&source_id, worker_id, worker_count, &PartitionId::None);

        let offset = start_offsets
            .into_iter()
            .find_map(|(pid, offset)| {
                if pid == PartitionId::None {
                    offset
                } else {
                    None
                }
            })
            .unwrap_or_default();

        let mut rows = as_generator(&connection.load_generator)
            .by_seed(mz_ore::now::SYSTEM_TIME.clone(), None);

        // Skip forward to the requested offset.
        for _ in 0..offset.offset {
            rows.next();
        }

        Ok(Self {
            rows,
            last: Instant::now(),
            tick: Duration::from_micros(connection.tick_micros.unwrap_or(1_000_000)),
            offset,
            active_read_worker,
        })
    }

    fn get_next_message(
        &mut self,
    ) -> Result<NextMessage<Self::Key, Self::Value, Self::Diff>, SourceReaderError> {
        if !self.active_read_worker {
            return Ok(NextMessage::Finished);
        }

        if self.last.elapsed() < self.tick {
            return Ok(NextMessage::Pending);
        }
        self.last += self.tick;
        self.offset += 1;
        match self.rows.next() {
            Some(value) => Ok(NextMessage::Ready(SourceMessageType::Finalized(
                SourceMessage {
                    partition: PartitionId::None,
                    offset: self.offset,
                    upstream_time_millis: None,
                    key: (),
                    value,
                    headers: None,
                    specific_diff: 1,
                },
            ))),
            None => Ok(NextMessage::Finished),
        }
    }

    fn unconsumed_partitions(&self) -> Vec<PartitionId> {
        // Only the active reader is consuming from the one "partition". All
        // others are aware of it but are not doing anything.
        if !self.active_read_worker {
            vec![PartitionId::None]
        } else {
            vec![]
        }
    }
}

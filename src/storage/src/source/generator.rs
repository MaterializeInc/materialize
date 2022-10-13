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
use crate::source::commit::LogCommitter;
use crate::source::{NextMessage, SourceReader, SourceReaderError};
use crate::types::connections::ConnectionContext;
use crate::types::sources::GeneratorMessageType;
use crate::types::sources::{
    encoding::SourceDataEncoding, Generator, LoadGenerator, LoadGeneratorSourceConnection, MzOffset,
};

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
    rows: Box<dyn Iterator<Item = (usize, GeneratorMessageType, Row)>>,
    last: Instant,
    tick: Duration,
    offset: MzOffset,
    // Load-generator sources support single-threaded ingestion only, so only
    // one of the `LoadGeneratorSourceReader`s will actually produce data.
    active_read_worker: bool,
    // The non-active reader (see above `active_read_worker`) has to report back
    // that is is not consuming from the one [`PartitionId:None`] partition.
    // Before it can return a [`NextMessage::Finished`]. This is keeping track
    // of that.
    reported_unconsumed_partitions: bool,
}

impl SourceReader for LoadGeneratorSourceReader {
    type Key = ();
    type Value = Row;
    // LoadGenerator can produce deletes that cause retractions
    type Diff = Diff;
    type OffsetCommitter = LogCommitter;
    type Connection = LoadGeneratorSourceConnection;

    fn new(
        _source_name: String,
        source_id: GlobalId,
        worker_id: usize,
        worker_count: usize,
        _consumer_activator: SyncActivator,
        connection: Self::Connection,
        start_offsets: Vec<(PartitionId, Option<MzOffset>)>,
        _encoding: SourceDataEncoding,
        _metrics: SourceBaseMetrics,
        _connection_context: ConnectionContext,
    ) -> Result<(Self, Self::OffsetCommitter), anyhow::Error> {
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

        let tick = Duration::from_micros(connection.tick_micros.unwrap_or(1_000_000));
        Ok((
            Self {
                rows: Box::new(rows),
                // Subtract tick so we immediately produce a row.
                last: Instant::now() - tick,
                tick,
                offset,
                active_read_worker,
                reported_unconsumed_partitions: false,
            },
            LogCommitter {
                source_id,
                worker_id,
                worker_count,
            },
        ))
    }

    fn get_next_message(
        &mut self,
    ) -> Result<NextMessage<Self::Key, Self::Value, Self::Diff>, SourceReaderError> {
        if !self.active_read_worker {
            if !self.reported_unconsumed_partitions {
                self.reported_unconsumed_partitions = true;
                return Ok(NextMessage::Ready(
                    SourceMessageType::DropPartitionCapabilities(vec![PartitionId::None]),
                ));
            }
            return Ok(NextMessage::Finished);
        }

        if self.last.elapsed() < self.tick {
            return Ok(NextMessage::Pending);
        }

        let (output, typ, value) = match self.rows.next() {
            Some(row) => row,
            None => return Ok(NextMessage::Finished),
        };

        let message = SourceMessage {
            output,
            partition: PartitionId::None,
            offset: self.offset,
            upstream_time_millis: None,
            key: (),
            value,
            headers: None,
            specific_diff: 1,
        };
        let message = match typ {
            GeneratorMessageType::Finalized => {
                self.last += self.tick;
                self.offset += 1;
                SourceMessageType::Finalized(message)
            }
            GeneratorMessageType::InProgress => SourceMessageType::InProgress(message),
        };
        Ok(NextMessage::Ready(message))
    }
}

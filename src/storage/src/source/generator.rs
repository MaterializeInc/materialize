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
use mz_storage_client::types::connections::ConnectionContext;
use mz_storage_client::types::sources::GeneratorMessageType;
use mz_storage_client::types::sources::{
    encoding::SourceDataEncoding, Generator, LoadGenerator, LoadGeneratorSourceConnection, MzOffset,
};

use super::metrics::SourceBaseMetrics;
use super::{SourceMessage, SourceMessageType};
use crate::source::commit::LogCommitter;
use crate::source::types::SourceConnectionBuilder;
use crate::source::{NextMessage, SourceReader};

mod auction;
mod counter;
mod datums;
mod tpch;

pub use auction::Auction;
pub use counter::Counter;
pub use datums::Datums;
pub use tpch::Tpch;

pub fn as_generator(g: &LoadGenerator, tick_micros: Option<u64>) -> Box<dyn Generator> {
    match g {
        LoadGenerator::Auction => Box::new(Auction {}),
        LoadGenerator::Counter => Box::new(Counter {}),
        LoadGenerator::Datums => Box::new(Datums {}),
        LoadGenerator::Tpch {
            count_supplier,
            count_part,
            count_customer,
            count_orders,
            count_clerk,
        } => Box::new(Tpch {
            count_supplier: *count_supplier,
            count_part: *count_part,
            count_customer: *count_customer,
            count_orders: *count_orders,
            count_clerk: *count_clerk,
            // The default tick behavior 1s. For tpch we want to disable ticking
            // completely.
            tick: Duration::from_micros(tick_micros.unwrap_or(0)),
        }),
    }
}

pub struct LoadGeneratorSourceReader {
    rows: Box<dyn Iterator<Item = (usize, GeneratorMessageType, Row, i64)>>,
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

impl SourceConnectionBuilder for LoadGeneratorSourceConnection {
    type Reader = LoadGeneratorSourceReader;
    type OffsetCommitter = LogCommitter;

    fn into_reader(
        self,
        _source_name: String,
        source_id: GlobalId,
        worker_id: usize,
        worker_count: usize,
        _consumer_activator: SyncActivator,
        start_offsets: Vec<(PartitionId, Option<MzOffset>)>,
        _encoding: SourceDataEncoding,
        _metrics: SourceBaseMetrics,
        _connection_context: ConnectionContext,
    ) -> Result<(Self::Reader, Self::OffsetCommitter), anyhow::Error> {
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

        let mut rows = as_generator(&self.load_generator, self.tick_micros)
            .by_seed(mz_ore::now::SYSTEM_TIME.clone(), None);

        // Skip forward to the requested offset.
        for _ in 0..offset.offset {
            rows.next();
        }

        let tick = Duration::from_micros(self.tick_micros.unwrap_or(1_000_000));
        Ok((
            LoadGeneratorSourceReader {
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
}

impl SourceReader for LoadGeneratorSourceReader {
    type Key = ();
    type Value = Row;
    // LoadGenerator can produce deletes that cause retractions
    type Time = MzOffset;
    type Diff = Diff;

    fn get_next_message(&mut self) -> NextMessage<Self::Key, Self::Value, Self::Diff> {
        if !self.active_read_worker {
            if !self.reported_unconsumed_partitions {
                self.reported_unconsumed_partitions = true;
                return NextMessage::Ready(SourceMessageType::DropPartitionCapabilities(vec![
                    PartitionId::None,
                ]));
            }
            return NextMessage::Finished;
        }

        if self.last.elapsed() < self.tick {
            return NextMessage::Pending;
        }

        let (output, typ, value, specific_diff) = match self.rows.next() {
            Some(row) => row,
            None => return NextMessage::Finished,
        };

        let message = SourceMessage {
            output,
            upstream_time_millis: None,
            key: (),
            value,
            headers: None,
        };
        let ts = (PartitionId::None, self.offset);
        let message = match typ {
            GeneratorMessageType::Finalized => {
                self.last += self.tick;
                self.offset += 1;
                SourceMessageType::Finalized(Ok(message), ts, specific_diff)
            }
            GeneratorMessageType::InProgress => {
                SourceMessageType::InProgress(Ok(message), ts, specific_diff)
            }
        };
        NextMessage::Ready(message)
    }
}

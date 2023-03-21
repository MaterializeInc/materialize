// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::time::{Duration, Instant};

use timely::dataflow::operators::Capability;
use timely::progress::Antichain;
use timely::scheduling::SyncActivator;

use mz_ore::cast::CastFrom;
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
        LoadGenerator::Counter { max_cardinality } => Box::new(Counter {
            max_cardinality: max_cardinality.clone(),
        }),
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
    /// Capabilities used to produce messages
    data_capability: Capability<MzOffset>,
    upper_capability: Capability<MzOffset>,
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
        mut data_capability: Capability<MzOffset>,
        mut upper_capability: Capability<MzOffset>,
        resume_upper: Antichain<MzOffset>,
        _encoding: SourceDataEncoding,
        _metrics: SourceBaseMetrics,
        _connection_context: ConnectionContext,
    ) -> Result<(Self::Reader, Self::OffsetCommitter), anyhow::Error> {
        let active_read_worker =
            crate::source::responsible_for(&source_id, worker_id, worker_count, ());

        // TODO(petrosagg): handle the empty frontier correctly. Currenty the framework code never
        // constructs a reader when the resumption frontier is the empty antichain
        let offset = resume_upper.into_option().unwrap();
        data_capability.downgrade(&offset);
        upper_capability.downgrade(&offset);

        let mut rows = as_generator(&self.load_generator, self.tick_micros)
            .by_seed(mz_ore::now::SYSTEM_TIME.clone(), None);

        // Skip forward to the requested offset.
        let tx_count = usize::cast_from(offset.offset);
        let txns = rows
            .by_ref()
            .filter(|(_, typ, _, _)| matches!(typ, GeneratorMessageType::Finalized))
            .take(tx_count)
            .count();
        assert_eq!(txns, tx_count, "produced wrong number of transactions");

        let tick = Duration::from_micros(self.tick_micros.unwrap_or(1_000_000));
        Ok((
            LoadGeneratorSourceReader {
                rows: Box::new(rows),
                // Subtract tick so we immediately produce a row.
                #[allow(clippy::unchecked_duration_subtraction)]
                last: Instant::now() - tick,
                tick,
                offset,
                active_read_worker,
                data_capability,
                upper_capability,
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
    type Time = MzOffset;
    type Diff = Diff;

    fn get_next_message(&mut self) -> NextMessage<Self::Key, Self::Value, Self::Time, Self::Diff> {
        if !self.active_read_worker {
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
        let cap = self.data_capability.delayed(&self.offset);
        let next_ts = self.offset + 1;
        self.upper_capability.downgrade(&next_ts);
        if matches!(typ, GeneratorMessageType::Finalized) {
            self.last += self.tick;
            self.offset += 1;
            self.data_capability.downgrade(&next_ts);
        }
        NextMessage::Ready(SourceMessageType::Message(Ok(message), cap, specific_diff))
    }
}

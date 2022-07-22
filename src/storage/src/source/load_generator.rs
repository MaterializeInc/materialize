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
use crate::types::sources::Generator;
use crate::types::sources::{encoding::SourceDataEncoding, MzOffset, SourceConnection};

pub struct LoadGeneratorSourceReader {
    generator: Generator,
    last: Instant,
    tick: Duration,
    offset: MzOffset,
}

impl SourceReader for LoadGeneratorSourceReader {
    type Key = ();
    type Value = Row;
    // LoadGenerator can produce deletes that cause retractions
    type Diff = Diff;

    fn new(
        _source_name: String,
        _source_id: GlobalId,
        _worker_id: usize,
        _worker_count: usize,
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

        Ok(Self {
            generator: connection.generator,
            last: Instant::now(),
            tick: Duration::from_micros(connection.tick_micros.unwrap_or(1_000_000)),
            offset,
        })
    }
    fn get_next_message(
        &mut self,
    ) -> Result<NextMessage<Self::Key, Self::Value, Self::Diff>, SourceReaderError> {
        if self.last.elapsed() < self.tick {
            return Ok(NextMessage::Pending);
        }
        self.last += self.tick;
        self.offset += 1;
        let value = self.generator.by_offset(self.offset);
        Ok(NextMessage::Ready(SourceMessageType::Finalized(
            SourceMessage {
                partition: PartitionId::None,
                offset: self.offset,
                upstream_time_millis: None,
                key: (),
                value,
                headers: None,
                specific_diff: 1,
            },
        )))
    }
}

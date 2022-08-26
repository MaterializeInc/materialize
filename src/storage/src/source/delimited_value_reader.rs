// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! A wrapper that turns a regular [`SourceReader`] into a delimited source
//! reader.

use timely::scheduling::activate::SyncActivator;

use mz_expr::PartitionId;
use mz_repr::GlobalId;

use crate::source::types::SourceMessageType;
use crate::source::types::SourceReaderError;
use crate::source::types::{NextMessage, SourceMessage, SourceReader};
use crate::types::connections::ConnectionContext;
use crate::types::sources::encoding::SourceDataEncoding;
use crate::types::sources::{MzOffset, SourceConnection};

/// A wrapper that converts a delimited source reader that only provides
/// values into a key/value reader whose key is always None
pub struct DelimitedValueSource<S>(S);

impl<S, D: timely::Data> SourceReader for DelimitedValueSource<S>
where
    S: SourceReader<Key = (), Value = Option<Vec<u8>>, Diff = D>,
{
    type Key = Option<Vec<u8>>;
    type Value = Option<Vec<u8>>;
    type Diff = D;

    fn new(
        source_name: String,
        source_id: GlobalId,
        worker_id: usize,
        worker_count: usize,
        consumer_activator: SyncActivator,
        connection: SourceConnection,
        restored_offsets: Vec<(PartitionId, Option<MzOffset>)>,
        encoding: SourceDataEncoding,
        metrics: crate::source::metrics::SourceBaseMetrics,
        connection_context: ConnectionContext,
    ) -> Result<Self, anyhow::Error> {
        S::new(
            source_name,
            source_id,
            worker_id,
            worker_count,
            consumer_activator,
            connection,
            restored_offsets,
            encoding,
            metrics,
            connection_context,
        )
        .map(Self)
    }

    fn get_next_message(
        &mut self,
    ) -> Result<NextMessage<Self::Key, Self::Value, Self::Diff>, SourceReaderError> {
        match self.0.get_next_message()? {
            NextMessage::Ready(SourceMessageType::Finalized(SourceMessage {
                key: _,
                value,
                partition,
                offset,
                upstream_time_millis,
                headers,
                specific_diff,
            })) => Ok(NextMessage::Ready(SourceMessageType::Finalized(
                SourceMessage {
                    key: None,
                    value,
                    partition,
                    offset,
                    upstream_time_millis,
                    headers,
                    specific_diff,
                },
            ))),
            NextMessage::Ready(SourceMessageType::InProgress(SourceMessage {
                key: _,
                value,
                partition,
                offset,
                upstream_time_millis,
                headers,
                specific_diff,
            })) => Ok(NextMessage::Ready(SourceMessageType::InProgress(
                SourceMessage {
                    key: None,
                    value,
                    partition,
                    offset,
                    upstream_time_millis,
                    headers,
                    specific_diff,
                },
            ))),
            NextMessage::Ready(SourceMessageType::DropPartitionCapabilities(pids)) => Ok(
                NextMessage::Ready(SourceMessageType::DropPartitionCapabilities(pids)),
            ),
            NextMessage::Ready(SourceMessageType::SourceStatus(update)) => {
                Ok(NextMessage::Ready(SourceMessageType::SourceStatus(update)))
            }
            NextMessage::Pending => Ok(NextMessage::Pending),
            NextMessage::TransientDelay => Ok(NextMessage::TransientDelay),
            NextMessage::Finished => Ok(NextMessage::Finished),
        }
    }
}

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

use timely::dataflow::operators::Capability;
use timely::progress::Antichain;
use timely::scheduling::activate::SyncActivator;

use mz_repr::{ColumnType, GlobalId, RelationDesc};
use mz_storage_client::types::connections::ConnectionContext;
use mz_storage_client::types::sources::encoding::SourceDataEncoding;
use mz_storage_client::types::sources::{IncludedColumnSource, SourceConnection};

use crate::source::types::{
    NextMessage, SourceConnectionBuilder, SourceMessage, SourceMessageType, SourceReader,
};

/// A wrapper that converts a delimited source connection that only provides
/// values into a key/value reader whose key is always None
#[derive(Clone)]
pub struct DelimitedValueSourceConnection<C>(pub(crate) C);

/// A wrapper that converts a delimited source reader that only provides
/// values into a key/value reader whose key is always None
pub struct DelimitedValueSourceReader<S>(S);

impl<C: SourceConnection> SourceConnection for DelimitedValueSourceConnection<C> {
    fn name(&self) -> &'static str {
        self.0.name()
    }
    fn upstream_name(&self) -> Option<&str> {
        self.0.upstream_name()
    }
    fn timestamp_desc(&self) -> RelationDesc {
        self.0.timestamp_desc()
    }
    fn num_outputs(&self) -> usize {
        self.0.num_outputs()
    }
    fn connection_id(&self) -> Option<GlobalId> {
        self.0.connection_id()
    }
    fn metadata_columns(&self) -> Vec<(&str, ColumnType)> {
        self.0.metadata_columns()
    }
    fn metadata_column_types(&self) -> Vec<IncludedColumnSource> {
        self.0.metadata_column_types()
    }
}

impl<C: SourceConnectionBuilder> SourceConnectionBuilder for DelimitedValueSourceConnection<C>
where
    C::Reader: SourceReader<Key = (), Value = Option<Vec<u8>>>,
{
    type Reader = DelimitedValueSourceReader<C::Reader>;
    type OffsetCommitter = C::OffsetCommitter;

    fn into_reader(
        self,
        source_name: String,
        source_id: GlobalId,
        worker_id: usize,
        worker_count: usize,
        consumer_activator: SyncActivator,
        data_capability: Capability<<Self::Reader as SourceReader>::Time>,
        upper_capability: Capability<<Self::Reader as SourceReader>::Time>,
        resume_upper: Antichain<<Self::Reader as SourceReader>::Time>,
        encoding: SourceDataEncoding,
        metrics: crate::source::metrics::SourceBaseMetrics,
        connection_context: ConnectionContext,
    ) -> Result<(Self::Reader, Self::OffsetCommitter), anyhow::Error> {
        self.0
            .into_reader(
                source_name,
                source_id,
                worker_id,
                worker_count,
                consumer_activator,
                data_capability,
                upper_capability,
                resume_upper,
                encoding,
                metrics,
                connection_context,
            )
            .map(|(s, sc)| (DelimitedValueSourceReader(s), sc))
    }
}

impl<S> SourceReader for DelimitedValueSourceReader<S>
where
    S: SourceReader<Key = (), Value = Option<Vec<u8>>>,
{
    type Key = Option<Vec<u8>>;
    type Value = Option<Vec<u8>>;
    type Time = S::Time;
    type Diff = S::Diff;

    fn get_next_message(&mut self) -> NextMessage<Self::Key, Self::Value, Self::Time, Self::Diff> {
        match self.0.get_next_message() {
            NextMessage::Ready(SourceMessageType::Message(
                Ok(SourceMessage {
                    output,
                    key: _,
                    value,
                    upstream_time_millis,
                    headers,
                }),
                cap,
                diff,
            )) => NextMessage::Ready(SourceMessageType::Message(
                Ok(SourceMessage {
                    output,
                    key: None,
                    value,
                    upstream_time_millis,
                    headers,
                }),
                cap,
                diff,
            )),
            NextMessage::Ready(SourceMessageType::Message(Err(err), cap, diff)) => {
                NextMessage::Ready(SourceMessageType::Message(Err(err), cap, diff))
            }
            NextMessage::Ready(SourceMessageType::SourceStatus(update)) => {
                NextMessage::Ready(SourceMessageType::SourceStatus(update))
            }
            NextMessage::Pending => NextMessage::Pending,
            NextMessage::TransientDelay => NextMessage::TransientDelay,
            NextMessage::Finished => NextMessage::Finished,
        }
    }
}

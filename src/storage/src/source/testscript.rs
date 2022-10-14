// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::time::Duration;

use timely::scheduling::SyncActivator;
use tokio::time::sleep;

use mz_expr::PartitionId;
use mz_repr::GlobalId;

use crate::source::commit::LogCommitter;
use crate::source::{SourceMessage, SourceMessageType, SourceReader, SourceReaderError};
use crate::types::connections::ConnectionContext;
use crate::types::sources::encoding::SourceDataEncoding;
use crate::types::sources::{MzOffset, TestScriptSourceConnection};

#[derive(serde::Serialize, serde::Deserialize)]
#[serde(tag = "command")]
#[serde(rename_all = "lowercase")]
pub enum ScriptCommand {
    /// Emit a value (and possibly a key, which may be required
    /// by the envelope), at an offset.
    Emit {
        key: Option<String>,
        value: String,
        offset: u64,
    },
    /// Terminate the source. Commands after this command
    /// are ignored. Absence of this command causes the source
    /// to pend forever after all commands are processed, like
    /// a kafka source for a topic with no new messages.
    Terminate,
}

struct Script {
    commands: std::vec::IntoIter<ScriptCommand>,
}

pub struct TestScriptSourceReader {
    script: Script,
}

#[async_trait::async_trait(?Send)]
impl SourceReader for TestScriptSourceReader {
    type Key = Option<Vec<u8>>;
    type Value = Option<Vec<u8>>;
    type Diff = ();
    type OffsetCommitter = LogCommitter;
    type Connection = TestScriptSourceConnection;

    fn new(
        _source_name: String,
        source_id: GlobalId,
        worker_id: usize,
        worker_count: usize,
        _consumer_activator: SyncActivator,
        conn: Self::Connection,
        _restored_offsets: Vec<(PartitionId, Option<MzOffset>)>,
        _encoding: SourceDataEncoding,
        _metrics: crate::source::metrics::SourceBaseMetrics,
        _connection_context: ConnectionContext,
    ) -> Result<(Self, Self::OffsetCommitter), anyhow::Error> {
        let commands: Vec<ScriptCommand> = serde_json::from_str(&conn.desc_json)?;
        Ok((
            TestScriptSourceReader {
                script: Script {
                    commands: commands.into_iter(),
                },
            },
            LogCommitter {
                source_id,
                worker_id,
                worker_count,
            },
        ))
    }
    async fn next(
        &mut self,
        timestamp_granularity: Duration,
    ) -> Option<Result<SourceMessageType<Self::Key, Self::Value, Self::Diff>, SourceReaderError>>
    {
        if let Some(command) = self.script.commands.next() {
            match command {
                ScriptCommand::Terminate => {
                    return None;
                }
                ScriptCommand::Emit { key, value, offset } => {
                    // For now we only support `Finalized` messages
                    return Some(Ok(SourceMessageType::Finalized(SourceMessage {
                        // For now, we only support single-output, single partition
                        output: 0,
                        partition: PartitionId::None,
                        offset: MzOffset { offset },
                        upstream_time_millis: None,
                        key: key.map(|k| k.into_bytes()),
                        value: Some(value.into_bytes()),
                        headers: None,
                        specific_diff: (),
                    })));
                }
            }
        } else {
            loop {
                // sleep continuously
                sleep(timestamp_granularity).await;
            }
        }
    }
}

// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::any::Any;
use std::convert::Infallible;
use std::rc::Rc;

use differential_dataflow::{AsCollection, Collection};
use mz_ore::collections::CollectionExt;
use mz_repr::Diff;
use mz_storage_client::types::connections::ConnectionContext;
use mz_storage_client::types::sources::{MzOffset, TestScriptSourceConnection};
use mz_timely_util::builder_async::OperatorBuilder as AsyncOperatorBuilder;
use timely::dataflow::operators::ToStream;
use timely::dataflow::{Scope, Stream};
use timely::progress::Antichain;

use crate::source::types::{HealthStatus, HealthStatusUpdate, SourceRender};
use crate::source::{RawSourceCreationConfig, SourceMessage, SourceReaderError};

#[derive(serde::Serialize, serde::Deserialize, Clone)]
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

impl SourceRender for TestScriptSourceConnection {
    type Key = Option<Vec<u8>>;
    type Value = Option<Vec<u8>>;
    type Time = MzOffset;

    fn render<G: Scope<Timestamp = MzOffset>>(
        self,
        scope: &mut G,
        config: RawSourceCreationConfig,
        _connection_context: ConnectionContext,
        _resume_uppers: impl futures::Stream<Item = Antichain<MzOffset>> + 'static,
    ) -> (
        Collection<
            G,
            (
                usize,
                Result<SourceMessage<Self::Key, Self::Value>, SourceReaderError>,
            ),
            Diff,
        >,
        Option<Stream<G, Infallible>>,
        Stream<G, (usize, HealthStatusUpdate)>,
        Rc<dyn Any>,
    ) {
        let mut builder = AsyncOperatorBuilder::new(config.name, scope.clone());

        let (mut data_output, stream) = builder.new_output();

        let button = builder.build(move |caps| async move {
            let mut cap = caps.into_element();
            let commands: Vec<ScriptCommand> =
                serde_json::from_str(&self.desc_json).expect("Invalid command description");

            for command in commands {
                match command {
                    ScriptCommand::Emit { key, value, offset } => {
                        // For now we only support `Finalized` messages
                        let msg = Ok(SourceMessage {
                            upstream_time_millis: None,
                            key: key.map(|k| k.into_bytes()),
                            value: Some(value.into_bytes()),
                            headers: None,
                        });
                        let ts = MzOffset::from(offset);

                        // For now, we only support single-output, single partition, so output
                        // to the 0th output.
                        data_output.give(&cap.delayed(&ts), ((0, msg), ts, 1)).await;
                        cap.downgrade(&(ts + 1));
                    }
                    ScriptCommand::Terminate => return,
                }
            }
            // Keep the source alive if we didn't get an explicit Terminate command
            futures::future::pending::<()>().await;
        });

        let status = [(0, HealthStatusUpdate::status(HealthStatus::Running))].to_stream(scope);
        (
            stream.as_collection(),
            None,
            status,
            Rc::new(button.press_on_drop()),
        )
    }
}

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
use std::time::Duration;

use differential_dataflow::{AsCollection, Collection};
use timely::dataflow::operators::ToStream;
use timely::dataflow::{Scope, Stream};
use timely::progress::Antichain;

use mz_ore::cast::CastFrom;
use mz_ore::collections::CollectionExt;
use mz_repr::{Diff, Row};
use mz_storage_client::types::connections::ConnectionContext;
use mz_storage_client::types::sources::GeneratorMessageType;
use mz_storage_client::types::sources::{
    Generator, LoadGenerator, LoadGeneratorSourceConnection, MzOffset, SourceTimestamp,
};
use mz_timely_util::builder_async::OperatorBuilder as AsyncOperatorBuilder;

use crate::source::types::{HealthStatus, HealthStatusUpdate, SourceRender};
use crate::source::{RawSourceCreationConfig, SourceMessage, SourceReaderError};

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

impl SourceRender for LoadGeneratorSourceConnection {
    type Key = ();
    type Value = Row;
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

            let responsible = crate::source::responsible_for(
                &config.id,
                config.worker_id,
                config.worker_count,
                (),
            );
            if !responsible {
                return;
            }

            let resume_upper =
                Antichain::from_iter(config.source_resume_upper.iter().map(MzOffset::decode_row));

            let Some(mut offset) = resume_upper.into_option() else {
                return
            };
            cap.downgrade(&offset);

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

            while let Some((output, typ, value, diff)) = rows.next() {
                let message = (
                    output,
                    Ok(SourceMessage {
                        upstream_time_millis: None,
                        key: (),
                        value,
                        headers: None,
                    }),
                );

                data_output.give(&cap, (message, offset, diff)).await;

                if matches!(typ, GeneratorMessageType::Finalized) {
                    offset += 1;
                    cap.downgrade(&offset);
                    tokio::time::sleep(tick).await;
                }
            }
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

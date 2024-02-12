// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::convert::Infallible;
use std::time::Duration;

use differential_dataflow::{AsCollection, Collection};
use futures::StreamExt;
use mz_repr::{Diff, Row};
use mz_storage_types::sources::load_generator::{
    Generator, LoadGenerator, LoadGeneratorSourceConnection,
};
use mz_storage_types::sources::{MzOffset, SourceTimestamp};
use mz_timely_util::builder_async::{OperatorBuilder as AsyncOperatorBuilder, PressOnDropButton};
use timely::dataflow::operators::to_stream::Event;
use timely::dataflow::operators::ToStream;
use timely::dataflow::{Scope, Stream};
use timely::progress::Antichain;

use crate::healthcheck::{HealthStatusMessage, HealthStatusUpdate, StatusNamespace};
use crate::source::types::{ProgressStatisticsUpdate, SourceRender};
use crate::source::{RawSourceCreationConfig, SourceMessage, SourceReaderError};

mod auction;
mod counter;
mod datums;
mod marketing;
mod tpch;

pub use auction::Auction;
pub use counter::Counter;
pub use datums::Datums;
pub use tpch::Tpch;

use self::marketing::Marketing;

pub fn as_generator(g: &LoadGenerator, tick_micros: Option<u64>) -> Box<dyn Generator> {
    match g {
        LoadGenerator::Auction => Box::new(Auction {}),
        LoadGenerator::Counter { max_cardinality } => Box::new(Counter {
            max_cardinality: max_cardinality.clone(),
        }),
        LoadGenerator::Datums => Box::new(Datums {}),
        LoadGenerator::Marketing => Box::new(Marketing {}),
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
    type Time = MzOffset;

    const STATUS_NAMESPACE: StatusNamespace = StatusNamespace::Generator;

    fn render<G: Scope<Timestamp = MzOffset>>(
        self,
        scope: &mut G,
        config: RawSourceCreationConfig,
        resume_uppers: impl futures::Stream<Item = Antichain<MzOffset>> + 'static,
        _start_signal: impl std::future::Future<Output = ()> + 'static,
    ) -> (
        Collection<G, (usize, Result<SourceMessage, SourceReaderError>), Diff>,
        Option<Stream<G, Infallible>>,
        Stream<G, HealthStatusMessage>,
        Stream<G, ProgressStatisticsUpdate>,
        Vec<PressOnDropButton>,
    ) {
        let mut builder = AsyncOperatorBuilder::new(config.name.clone(), scope.clone());

        let (mut data_output, stream) = builder.new_output();
        let (mut stats_output, stats_stream) = builder.new_output();

        let button = builder.build(move |caps| async move {
            let [mut cap, stats_cap]: [_; 2] = caps.try_into().unwrap();

            if !config.responsible_for(()) {
                return;
            }

            let resume_upper = Antichain::from_iter(
                config.source_resume_uppers[&config.id]
                    .iter()
                    .map(MzOffset::decode_row),
            );

            let Some(resume_offset) = resume_upper.into_option() else {
                return;
            };

            let mut rows = as_generator(&self.load_generator, self.tick_micros).by_seed(
                mz_ore::now::SYSTEM_TIME.clone(),
                None,
                resume_offset,
            );

            let tick = Duration::from_micros(self.tick_micros.unwrap_or(1_000_000));

            let mut resume_uppers = std::pin::pin!(resume_uppers);
            let mut statistics = ProgressStatisticsUpdate {
                upstream_values: 0,
                committed_values: 0,
            };

            while let Some((output, event)) = rows.next() {
                match event {
                    Event::Message(offset, (value, diff)) => {
                        let message = (
                            output,
                            Ok(SourceMessage {
                                key: Row::default(),
                                value,
                                metadata: Row::default(),
                            }),
                        );

                        // Some generators always reproduce their TVC from the beginning which can
                        // generate a significant amount of data that will overwhelm the dataflow.
                        // Since those are not required downstream we eagerly ignore them here.
                        if resume_offset <= offset {
                            data_output.give(&cap, (message, offset, diff)).await;
                        }

                        if offset.offset > statistics.upstream_values {
                            statistics.upstream_values = offset.offset;
                        }
                    }
                    Event::Progress(Some(offset)) => {
                        cap.downgrade(&offset);

                        // We only sleep if we have surpassed the resume offset so that we can
                        // quickly go over any historical updates that a generator might choose to
                        // emit.
                        // TODO(petrosagg): Remove the sleep below and make generators return an
                        // async stream so that they can drive the rate of production directly
                        if resume_offset < offset {
                            let mut sleep = std::pin::pin!(tokio::time::sleep(tick));

                            loop {
                                tokio::select! {
                                    _ = &mut sleep => {
                                        break;
                                    }
                                    Some(frontier) = resume_uppers.next() => {
                                        if let Some(offset) = frontier.as_option() {
                                            let total = offset.offset.saturating_sub(1);
                                            if total > statistics.committed_values{
                                                // Note we don't subtract from the upper, as we
                                                // want to report total number of offsets we have processed.
                                                statistics.committed_values = total;
                                            }
                                        }
                                    }
                                }
                            }
                            stats_output.give(&stats_cap, statistics.clone()).await;
                        }
                    }
                    Event::Progress(None) => return,
                }
            }
        });

        let status = [HealthStatusMessage {
            index: 0,
            namespace: Self::STATUS_NAMESPACE.clone(),
            update: HealthStatusUpdate::running(),
        }]
        .to_stream(scope);
        (
            stream.as_collection(),
            None,
            status,
            stats_stream,
            vec![button.press_on_drop()],
        )
    }
}

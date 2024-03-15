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
    Event, Generator, KeyValueLoadGenerator, LoadGenerator, LoadGeneratorSourceConnection,
};
use mz_storage_types::sources::{MzOffset, SourceTimestamp};
use mz_timely_util::builder_async::{OperatorBuilder as AsyncOperatorBuilder, PressOnDropButton};
use timely::dataflow::operators::ToStream;
use timely::dataflow::{Scope, Stream};
use timely::progress::Antichain;

use crate::healthcheck::{HealthStatusMessage, HealthStatusUpdate, StatusNamespace};
use crate::source::types::{ProgressStatisticsUpdate, SourceRender};
use crate::source::{RawSourceCreationConfig, SourceMessage, SourceReaderError};

mod auction;
mod counter;
mod datums;
mod key_value;
mod marketing;
mod tpch;

pub use auction::Auction;
pub use counter::Counter;
pub use datums::Datums;
pub use tpch::Tpch;

use self::marketing::Marketing;

enum GeneratorKind {
    Simple {
        generator: Box<dyn Generator>,
        tick_micros: Option<u64>,
        // Load generators cannot be rendered until all of their exports are
        // present.
        //
        // TODO: can this limitation be removed?
        required_exports: usize,
    },
    KeyValue(KeyValueLoadGenerator),
}

impl GeneratorKind {
    fn new(g: &LoadGenerator, tick_micros: Option<u64>) -> Self {
        let required_exports = g.views().len() + 1;

        match g {
            LoadGenerator::Auction => GeneratorKind::Simple {
                generator: Box::new(Auction {}),
                tick_micros,
                required_exports,
            },
            LoadGenerator::Counter { max_cardinality } => GeneratorKind::Simple {
                generator: Box::new(Counter {
                    max_cardinality: max_cardinality.clone(),
                }),
                tick_micros,
                required_exports,
            },
            LoadGenerator::Datums => GeneratorKind::Simple {
                generator: Box::new(Datums {}),
                tick_micros,
                required_exports,
            },
            LoadGenerator::Marketing => GeneratorKind::Simple {
                generator: Box::new(Marketing {}),
                tick_micros,
                required_exports,
            },
            LoadGenerator::Tpch {
                count_supplier,
                count_part,
                count_customer,
                count_orders,
                count_clerk,
            } => GeneratorKind::Simple {
                generator: Box::new(Tpch {
                    count_supplier: *count_supplier,
                    count_part: *count_part,
                    count_customer: *count_customer,
                    count_orders: *count_orders,
                    count_clerk: *count_clerk,
                    // The default tick behavior 1s. For tpch we want to disable ticking
                    // completely.
                    tick: Duration::from_micros(tick_micros.unwrap_or(0)),
                }),
                tick_micros,
                required_exports,
            },
            LoadGenerator::KeyValue(kv) => {
                mz_ore::soft_assert_eq_or_log!(
                    required_exports,
                    1,
                    "KeyValue generators should not have any additional views"
                );

                GeneratorKind::KeyValue(kv.clone())
            }
        }
    }

    fn render<G: Scope<Timestamp = MzOffset>>(
        self,
        scope: &mut G,
        config: RawSourceCreationConfig,
        committed_uppers: impl futures::Stream<Item = Antichain<MzOffset>> + 'static,
        start_signal: impl std::future::Future<Output = ()> + 'static,
    ) -> (
        Collection<G, (usize, Result<SourceMessage, SourceReaderError>), Diff>,
        Option<Stream<G, Infallible>>,
        Stream<G, HealthStatusMessage>,
        Stream<G, ProgressStatisticsUpdate>,
        Vec<PressOnDropButton>,
    ) {
        match self {
            GeneratorKind::Simple {
                tick_micros,
                generator,
                required_exports,
            } => render_simple_generator(
                generator,
                tick_micros,
                scope,
                config,
                committed_uppers,
                required_exports,
            ),
            GeneratorKind::KeyValue(kv) => {
                key_value::render(kv, scope, config, committed_uppers, start_signal)
            }
        }
    }
}

impl SourceRender for LoadGeneratorSourceConnection {
    type Time = MzOffset;

    const STATUS_NAMESPACE: StatusNamespace = StatusNamespace::Generator;

    fn render<G: Scope<Timestamp = MzOffset>>(
        self,
        scope: &mut G,
        config: RawSourceCreationConfig,
        committed_uppers: impl futures::Stream<Item = Antichain<MzOffset>> + 'static,
        start_signal: impl std::future::Future<Output = ()> + 'static,
    ) -> (
        Collection<G, (usize, Result<SourceMessage, SourceReaderError>), Diff>,
        Option<Stream<G, Infallible>>,
        Stream<G, HealthStatusMessage>,
        Stream<G, ProgressStatisticsUpdate>,
        Vec<PressOnDropButton>,
    ) {
        let generator_kind = GeneratorKind::new(&self.load_generator, self.tick_micros);
        generator_kind.render(scope, config, committed_uppers, start_signal)
    }
}

fn render_simple_generator<G: Scope<Timestamp = MzOffset>>(
    generator: Box<dyn Generator>,
    tick_micros: Option<u64>,
    scope: &mut G,
    config: RawSourceCreationConfig,
    committed_uppers: impl futures::Stream<Item = Antichain<MzOffset>> + 'static,
    required_exports: usize,
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
        // Do not run the load generator until we have all of our source
        // exports. Waiting here is fine because we know that their creation
        // and scheduling of this dataflow is imminent.
        //
        // TODO: can this limitation be removed?
        if required_exports != config.source_exports.len() {
            std::future::pending().await
        }

        let [mut cap, stats_cap]: [_; 2] = caps.try_into().unwrap();

        if !config.responsible_for(()) {
            // Emit 0, to mark this worker as having started up correctly.
            stats_output
                .give(
                    &stats_cap,
                    ProgressStatisticsUpdate::SteadyState {
                        offset_known: 0,
                        offset_committed: 0,
                    },
                )
                .await;
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

        let mut rows = generator.by_seed(mz_ore::now::SYSTEM_TIME.clone(), None, resume_offset);

        let tick = Duration::from_micros(tick_micros.unwrap_or(1_000_000));

        let mut committed_uppers = std::pin::pin!(committed_uppers);

        // If we are just starting up, report 0 as our `offset_committed`.
        let mut offset_committed = if resume_offset.offset == 0 {
            Some(0)
        } else {
            None
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
                                Some(frontier) = committed_uppers.next() => {
                                    if let Some(offset) = frontier.as_option() {
                                        // Offset N means we have committed N offsets (offsets are
                                        // 0-indexed)
                                        offset_committed = Some(offset.offset);
                                    }
                                }
                            }
                        }

                        // TODO(guswynn): generators have various definitions of "snapshot", so
                        // we are not going to implement snapshot progress statistics for them
                        // right now, but will come back to it.
                        if let Some(offset_committed) = offset_committed {
                            stats_output
                                .give(
                                    &stats_cap,
                                    ProgressStatisticsUpdate::SteadyState {
                                        // technically we could have _known_ a larger offset
                                        // than the one that has been committed, but we can
                                        // never recover that known amount on restart, so we
                                        // just advance these in lock step.
                                        offset_known: offset_committed,
                                        offset_committed,
                                    },
                                )
                                .await;
                        }
                    }
                }
                Event::Progress(None) => return,
            }
        }
    });

    let status = [HealthStatusMessage {
        index: 0,
        namespace: StatusNamespace::Generator,
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

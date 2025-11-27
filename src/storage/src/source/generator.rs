// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::BTreeMap;
use std::convert::Infallible;
use std::ops::Rem;
use std::sync::Arc;
use std::time::Duration;

use differential_dataflow::AsCollection;
use futures::StreamExt;
use itertools::Itertools;
use mz_ore::cast::CastFrom;
use mz_ore::iter::IteratorExt;
use mz_repr::{Diff, GlobalId, Row};
use mz_storage_types::errors::DataflowError;
use mz_storage_types::sources::load_generator::{
    Event, Generator, KeyValueLoadGenerator, LoadGenerator, LoadGeneratorOutput,
    LoadGeneratorSourceConnection,
};
use mz_storage_types::sources::{MzOffset, SourceExportDetails, SourceTimestamp};
use mz_timely_util::builder_async::{OperatorBuilder as AsyncOperatorBuilder, PressOnDropButton};
use mz_timely_util::containers::stack::AccountedStackBuilder;
use rand::SeedableRng;
use timely::container::CapacityContainerBuilder;
use timely::dataflow::operators::core::Partition;
use timely::dataflow::{Scope, Stream};
use timely::progress::Antichain;
use tokio::time::{Instant, interval_at};

use crate::healthcheck::{HealthStatusMessage, HealthStatusUpdate, StatusNamespace};
use crate::source::types::{Probe, SignaledFuture, SourceRender, StackedCollection};
use crate::source::{RawSourceCreationConfig, SourceMessage};

mod auction;
mod clock;
mod counter;
mod datums;
mod key_value;
mod marketing;
mod tpch;

pub use auction::Auction;
pub use clock::Clock;
pub use counter::Counter;
pub use datums::Datums;
pub use tpch::Tpch;

use self::marketing::Marketing;

enum GeneratorKind {
    Simple {
        generator: Box<dyn Generator>,
        tick_micros: Option<u64>,
        as_of: u64,
        up_to: u64,
    },
    KeyValue(KeyValueLoadGenerator),
}

impl GeneratorKind {
    fn new(g: &LoadGenerator, tick_micros: Option<u64>, as_of: u64, up_to: u64) -> Self {
        match g {
            LoadGenerator::Auction => GeneratorKind::Simple {
                generator: Box::new(Auction {}),
                tick_micros,
                as_of,
                up_to,
            },
            LoadGenerator::Clock => GeneratorKind::Simple {
                generator: Box::new(Clock {
                    tick_ms: tick_micros
                        .map(Duration::from_micros)
                        .unwrap_or(Duration::from_secs(1))
                        .as_millis()
                        .try_into()
                        .expect("reasonable tick interval"),
                    as_of_ms: as_of,
                }),
                tick_micros,
                as_of,
                up_to,
            },
            LoadGenerator::Counter { max_cardinality } => GeneratorKind::Simple {
                generator: Box::new(Counter {
                    max_cardinality: max_cardinality.clone(),
                }),
                tick_micros,
                as_of,
                up_to,
            },
            LoadGenerator::Datums => GeneratorKind::Simple {
                generator: Box::new(Datums {}),
                tick_micros,
                as_of,
                up_to,
            },
            LoadGenerator::Marketing => GeneratorKind::Simple {
                generator: Box::new(Marketing {}),
                tick_micros,
                as_of,
                up_to,
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
                as_of,
                up_to,
            },
            LoadGenerator::KeyValue(kv) => GeneratorKind::KeyValue(kv.clone()),
        }
    }

    fn render<G: Scope<Timestamp = MzOffset>>(
        self,
        scope: &mut G,
        config: &RawSourceCreationConfig,
        committed_uppers: impl futures::Stream<Item = Antichain<MzOffset>> + 'static,
        start_signal: impl std::future::Future<Output = ()> + 'static,
    ) -> (
        BTreeMap<GlobalId, StackedCollection<G, Result<SourceMessage, DataflowError>>>,
        Stream<G, Infallible>,
        Stream<G, HealthStatusMessage>,
        Vec<PressOnDropButton>,
    ) {
        // figure out which output types from the generator belong to which output indexes
        let mut output_map = BTreeMap::new();
        // Maps the output index to export_id for statistics.
        let mut idx_to_exportid = BTreeMap::new();
        // Make sure that there's an entry for the default output, even if there are no exports
        // that need data output. Certain implementations rely on it (at the time of this comment
        // that includes the key-value load gen source).
        output_map.insert(LoadGeneratorOutput::Default, Vec::new());
        for (idx, (export_id, export)) in config.source_exports.iter().enumerate() {
            let output_type = match &export.details {
                SourceExportDetails::LoadGenerator(details) => details.output,
                // This is an export that doesn't need any data output to it.
                SourceExportDetails::None => continue,
                _ => panic!("unexpected source export details: {:?}", export.details),
            };
            output_map
                .entry(output_type)
                .or_insert_with(Vec::new)
                .push(idx);
            idx_to_exportid.insert(idx, export_id.clone());
        }

        match self {
            GeneratorKind::Simple {
                tick_micros,
                as_of,
                up_to,
                generator,
            } => render_simple_generator(
                generator,
                tick_micros,
                as_of.into(),
                up_to.into(),
                scope,
                config,
                committed_uppers,
                output_map,
            ),
            GeneratorKind::KeyValue(kv) => key_value::render(
                kv,
                scope,
                config.clone(),
                committed_uppers,
                start_signal,
                output_map,
                idx_to_exportid,
            ),
        }
    }
}

impl SourceRender for LoadGeneratorSourceConnection {
    type Time = MzOffset;

    const STATUS_NAMESPACE: StatusNamespace = StatusNamespace::Generator;

    fn render<G: Scope<Timestamp = MzOffset>>(
        self,
        scope: &mut G,
        config: &RawSourceCreationConfig,
        committed_uppers: impl futures::Stream<Item = Antichain<MzOffset>> + 'static,
        start_signal: impl std::future::Future<Output = ()> + 'static,
    ) -> (
        BTreeMap<GlobalId, StackedCollection<G, Result<SourceMessage, DataflowError>>>,
        Stream<G, Infallible>,
        Stream<G, HealthStatusMessage>,
        Option<Stream<G, Probe<MzOffset>>>,
        Vec<PressOnDropButton>,
    ) {
        let generator_kind = GeneratorKind::new(
            &self.load_generator,
            self.tick_micros,
            self.as_of,
            self.up_to,
        );
        let (updates, uppers, health, button) =
            generator_kind.render(scope, config, committed_uppers, start_signal);

        (updates, uppers, health, None, button)
    }
}

fn render_simple_generator<G: Scope<Timestamp = MzOffset>>(
    generator: Box<dyn Generator>,
    tick_micros: Option<u64>,
    as_of: MzOffset,
    up_to: MzOffset,
    scope: &G,
    config: &RawSourceCreationConfig,
    committed_uppers: impl futures::Stream<Item = Antichain<MzOffset>> + 'static,
    output_map: BTreeMap<LoadGeneratorOutput, Vec<usize>>,
) -> (
    BTreeMap<GlobalId, StackedCollection<G, Result<SourceMessage, DataflowError>>>,
    Stream<G, Infallible>,
    Stream<G, HealthStatusMessage>,
    Vec<PressOnDropButton>,
) {
    let mut builder = AsyncOperatorBuilder::new(config.name.clone(), scope.clone());

    let (data_output, stream) = builder.new_output::<AccountedStackBuilder<_>>();
    let export_ids: Vec<_> = config.source_exports.keys().copied().collect();
    let partition_count = u64::cast_from(export_ids.len());
    let data_streams: Vec<_> = stream.partition::<CapacityContainerBuilder<_>, _, _>(
        partition_count,
        |((output, data), time, diff): &(
            (usize, Result<SourceMessage, DataflowError>),
            MzOffset,
            Diff,
        )| {
            let output = u64::cast_from(*output);
            (output, (data.clone(), time.clone(), diff.clone()))
        },
    );
    let mut data_collections = BTreeMap::new();
    for (id, data_stream) in export_ids.iter().zip_eq(data_streams) {
        data_collections.insert(*id, data_stream.as_collection());
    }

    let (_progress_output, progress_stream) = builder.new_output::<CapacityContainerBuilder<_>>();
    let (health_output, health_stream) = builder.new_output::<CapacityContainerBuilder<_>>();

    let busy_signal = Arc::clone(&config.busy_signal);
    let source_resume_uppers = config.source_resume_uppers.clone();
    let is_active_worker = config.responsible_for(());
    let source_statistics = config.statistics.clone();
    let button = builder.build(move |caps| {
        SignaledFuture::new(busy_signal, async move {
            let [mut cap, mut progress_cap, health_cap] = caps.try_into().unwrap();

            // We only need this until we reported ourselves as Running.
            let mut health_cap = Some(health_cap);

            if !is_active_worker {
                // Emit 0, to mark this worker as having started up correctly.
                for stats in source_statistics.values() {
                    stats.set_offset_known(0);
                    stats.set_offset_committed(0);
                }
                return;
            }

            let resume_upper = Antichain::from_iter(
                source_resume_uppers
                    .values()
                    .flat_map(|f| f.iter().map(MzOffset::decode_row)),
            );

            let Some(resume_offset) = resume_upper.into_option() else {
                return;
            };

            let now_fn = mz_ore::now::SYSTEM_TIME.clone();

            let start_instant = {
                // We want to have our interval start at a nice round number...
                // for example, if our tick interval is one minute, to start at a minute boundary.
                // However, the `Interval` type from tokio can't be "floored" in that way.
                // Instead, figure out the amount we should step forward based on the wall clock,
                // then apply that to our monotonic clock to make things start at approximately the
                // right time.
                let now_millis = now_fn();
                let now_instant = Instant::now();
                let delay_millis = tick_micros
                    .map(|tick_micros| tick_micros / 1000)
                    .filter(|tick_millis| *tick_millis > 0)
                    .map(|tick_millis| tick_millis - now_millis.rem(tick_millis))
                    .unwrap_or(0);
                now_instant + Duration::from_millis(delay_millis)
            };
            let tick = Duration::from_micros(tick_micros.unwrap_or(1_000_000));
            let mut tick_interval = interval_at(start_instant, tick);

            let mut rows = generator.by_seed(now_fn, None, resume_offset);

            let mut committed_uppers = std::pin::pin!(committed_uppers);

            // If we are just starting up, report 0 as our `offset_committed`.
            let mut offset_committed = if resume_offset.offset == 0 {
                Some(0)
            } else {
                None
            };

            while let Some((output_type, event)) = rows.next() {
                match event {
                    Event::Message(mut offset, (value, diff)) => {
                        // Fast forward any data before the requested as of.
                        if offset <= as_of {
                            offset = as_of;
                        }

                        // If the load generator produces data at or beyond the
                        // requested `up_to`, drop it. We'll terminate the load
                        // generator when the capability advances to the `up_to`,
                        // but the load generator might produce data far in advance
                        // of its capability.
                        if offset >= up_to {
                            continue;
                        }

                        // Once we see the load generator start producing data for some offset,
                        // we report progress beyond that offset, to ensure that a binding can be
                        // minted for the data and it doesn't accumulate in reclocking.
                        let _ = progress_cap.try_downgrade(&(offset + 1));

                        let outputs = match output_map.get(&output_type) {
                            Some(outputs) => outputs,
                            // We don't have an output index for this output type, so drop it
                            None => continue,
                        };

                        let message: Result<SourceMessage, DataflowError> = Ok(SourceMessage {
                            key: Row::default(),
                            value,
                            metadata: Row::default(),
                        });

                        // Some generators always reproduce their TVC from the beginning which can
                        // generate a significant amount of data that will overwhelm the dataflow.
                        // Since those are not required downstream we eagerly ignore them here.
                        if resume_offset <= offset {
                            for (&output, message) in outputs.iter().repeat_clone(message) {
                                data_output
                                    .give_fueled(&cap, ((output, message), offset, diff))
                                    .await;
                            }
                        }
                    }
                    Event::Progress(Some(offset)) => {
                        if resume_offset <= offset && health_cap.is_some() {
                            let health_cap = health_cap.take().expect("known to exist");
                            let export_ids = export_ids.iter().copied();
                            for id in export_ids.map(Some).chain(std::iter::once(None)) {
                                health_output.give(
                                    &health_cap,
                                    HealthStatusMessage {
                                        id,
                                        namespace: StatusNamespace::Generator,
                                        update: HealthStatusUpdate::running(),
                                    },
                                );
                            }
                        }

                        // If we've reached the requested maximum offset, cease.
                        if offset >= up_to {
                            break;
                        }

                        // If the offset is at or below the requested `as_of`, don't
                        // downgrade the capability.
                        if offset <= as_of {
                            continue;
                        }

                        cap.downgrade(&offset);
                        let _ = progress_cap.try_downgrade(&offset);

                        // We only sleep if we have surpassed the resume offset so that we can
                        // quickly go over any historical updates that a generator might choose to
                        // emit.
                        // TODO(petrosagg): Remove the sleep below and make generators return an
                        // async stream so that they can drive the rate of production directly
                        if resume_offset < offset {
                            loop {
                                tokio::select! {
                                    _tick = tick_interval.tick() => {
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
                                for stats in source_statistics.values() {
                                    stats.set_offset_committed(offset_committed);
                                    // technically we could have _known_ a larger offset
                                    // than the one that has been committed, but we can
                                    // never recover that known amount on restart, so we
                                    // just advance these in lock step.
                                    stats.set_offset_known(offset_committed);
                                }
                            }
                        }
                    }
                    Event::Progress(None) => return,
                }
            }
        })
    });

    (
        data_collections,
        progress_stream,
        health_stream,
        vec![button.press_on_drop()],
    )
}

/// Create a portable version of [`rand::rngs::SmallRng`].
///
/// This returns a `Xoshiro256PlusPlus` RNG whose seed has been generated using PCG32. This matches
/// the behavior of `rand` 0.8. Later versions of `rand` are allowed to change these implementation
/// details, as `SmallRng` is documented as non-portable. So to ensure load generator output
/// doesn't change across version upgrade, we reproduce those details here.
///
/// Note that we can't simply call `Xoshiro256PlusPlus::seed_from_u64` instead because that uses a
/// different seed generation algorithm (Splitmix64), so the RNG output would change.
fn small_rng(seed: u64) -> rand_xoshiro::Xoshiro256PlusPlus {
    let mut state = seed;

    // The below code is taken from
    // <https://github.com/rust-random/rand/tree/0.6.4/rand_core>
    //
    // Copyright 2018 Developers of the Rand project.
    // Copyright 2017-2018 The Rust Project Developers.
    //
    // Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
    // https://www.apache.org/licenses/LICENSE-2.0> or the MIT license
    // <LICENSE-MIT or https://opensource.org/licenses/MIT>, at your
    // option. This file may not be copied, modified, or distributed
    // except according to those terms.

    // We use PCG32 to generate a u32 sequence, and copy to the seed
    #[allow(clippy::as_conversions)]
    fn pcg32(state: &mut u64) -> [u8; 4] {
        const MUL: u64 = 6364136223846793005;
        const INC: u64 = 11634580027462260723;

        // We advance the state first (to get away from the input value,
        // in case it has low Hamming Weight).
        *state = state.wrapping_mul(MUL).wrapping_add(INC);
        let state = *state;

        // Use PCG output function with to_le to generate x:
        let xorshifted = (((state >> 18) ^ state) >> 27) as u32;
        let rot = (state >> 59) as u32;
        let x = xorshifted.rotate_right(rot);
        x.to_le_bytes()
    }

    let mut seed = [0; 32];
    let mut iter = seed.as_mut().chunks_exact_mut(4);
    for chunk in &mut iter {
        chunk.copy_from_slice(&pcg32(&mut state));
    }
    let rem = iter.into_remainder();
    if !rem.is_empty() {
        rem.copy_from_slice(&pcg32(&mut state)[..rem.len()]);
    }

    rand_xoshiro::Xoshiro256PlusPlus::from_seed(seed)
}

#[cfg(test)]
mod tests {
    use super::*;
    use rand::rngs::{SmallRng, StdRng};
    use rand::{Rng, SeedableRng};

    #[mz_ore::test]
    fn smallrng_to_xoshiro() {
        let mut small = SmallRng::seed_from_u64(123);
        let mut xoshiro = small_rng(123);

        assert_eq!(small.r#gen::<u64>(), xoshiro.r#gen::<u64>());
        assert_eq!(small.r#gen::<u64>(), xoshiro.r#gen::<u64>());
    }

    #[mz_ore::test]
    fn stdrng_to_chacha() {
        let mut std = StdRng::seed_from_u64(123);
        let mut chacha = rand_chacha::ChaCha12Rng::seed_from_u64(123);

        assert_eq!(std.r#gen::<u64>(), chacha.r#gen::<u64>());
        assert_eq!(std.r#gen::<u64>(), chacha.r#gen::<u64>());
    }
}

// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Simplified model of a persist_sink that shards the work of writing updates
//! across multiple timely workers.
//!
//! Where a real implementation would interact with the persist API to a) write
//! updates as batches, and b) append those batches this example instead logs
//! appended batches.

use std::collections::HashMap;
use std::time::Duration;

use mz_ore::now::{NowFn, SYSTEM_TIME};
use serde::{Deserialize, Serialize};
use timely::dataflow::channels::pact::{Exchange, Pipeline};
use timely::dataflow::channels::pushers::Tee;
use timely::dataflow::operators::generic::builder_rc::OperatorBuilder;
use timely::dataflow::operators::generic::OutputHandle;
use timely::dataflow::operators::{Broadcast, CapabilitySet, Inspect, Operator};
use timely::dataflow::{Scope, Stream};
use timely::progress::Antichain;
use timely::progress::Timestamp;
use timely::{Config, PartialOrder};
use tracing::info;

/// Simplified model of a persist_sink that shards the work of writing updates
/// to Blob across multiple timely workers.
#[derive(Debug, Clone, clap::Parser)]
pub struct Args {
    /// Number of timely workers.
    #[clap(long, default_value_t = 2)]
    num_workers: usize,
}

pub async fn run(args: Args) -> Result<(), anyhow::Error> {
    let source_interval_ms = 1000;
    let now_fn = SYSTEM_TIME.clone();

    let config = Config::process(args.num_workers);

    let worker_guards = timely::execute(config, move |worker| {
        worker.dataflow(|scope| {
            let ok_stream = read_source(scope, now_fn.clone(), source_interval_ms);

            sink(ok_stream);
        });

        Ok::<_, String>(())
    })
    .unwrap();

    let _result = worker_guards
        .join()
        .into_iter()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();

    Ok(())
}

/// A source that periodically emits updates.
fn read_source<G>(
    scope: &mut G,
    now_fn: NowFn,
    emission_interval_ms: u64,
) -> Stream<G, ((String, String), u64, isize)>
where
    G: Scope<Timestamp = u64>,
{
    let worker_index = scope.index();

    let mut source_op = OperatorBuilder::new("read_source".to_string(), scope.clone());
    let activator = scope.activator_for(&source_op.operator_info().address[..]);

    let (mut records_out, records) = source_op.new_output();

    source_op.build(move |mut capabilities| {
        let mut records_capability = capabilities.remove(0);

        let mut count = 0;

        move |_frontiers| {
            let now = now_fn();
            let now_clamped = now - (now % emission_interval_ms);
            records_capability.downgrade(&now_clamped);

            let mut records_out = records_out.activate();
            let mut records_session = records_out.session(&records_capability);
            records_session.give((
                (format!("worker-{}", worker_index), format!("{}", count)),
                now_clamped,
                1,
            ));

            count += 1;

            activator.activate_after(Duration::from_millis(emission_interval_ms));
        }
    });

    records
}

/// Simpler variant of differential `Description` with just a lower and upper.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, Hash)]
struct Description {
    lower: Antichain<u64>,
    upper: Antichain<u64>,
}

/// A batch of updates that have been written to `Blob`.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, Hash)]
struct WrittenBatch {
    description: Description,
    data: Vec<((String, String), u64, isize)>,
}

/// A batch of [`WrittenBatches`](WrittenBatch) that have been appended to `Consensus`.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, Hash)]
struct AppendedBatch {
    description: Description,
    batches: Vec<WrittenBatch>,
}

/// Model `persist_sink` that logs instead of writing to persist. This
/// visualizes what the individual parts of the pipeline would do.
fn sink<G>(to_sink: Stream<G, ((String, String), u64, isize)>)
where
    G: Scope<Timestamp = u64>,
{
    let scope = to_sink.scope();
    let worker_index = scope.index();

    // Only one worker is responsible for determining batch boundaries. All
    // workers must write batches with the same boundaries, to ensure that they
    // can be combined into one batch that gets appended to Consensus state.

    let write_boundaries =
        to_sink.unary_frontier(Pipeline, "write_boundaries", move |cap, _info| {
            let mut current_frontier = Antichain::from_elem(u64::minimum());
            let mut cap_set = CapabilitySet::from_elem(cap);

            move |input, output: &mut OutputHandle<u64, _, Tee<u64, _>>| {
                while let Some((_time, _data)) = input.next() {
                    // Just read away data.
                }

                // When the frontier advances, we emit a new description to
                // signal to the writer operators that they can start writing
                // updates to persist. In the real-world implementation we might
                // eventually want to change this to something smarter.
                if PartialOrder::less_than(&current_frontier.borrow(), &input.frontier().frontier())
                {
                    if worker_index == 0 {
                        let write_boundaries = Description {
                            lower: current_frontier.clone(),
                            upper: input.frontier().frontier().to_owned(),
                        };
                        let mut session = output.session(cap_set.first().unwrap());
                        session.give(write_boundaries);
                    }

                    current_frontier.clear();
                    current_frontier.extend(input.frontier().frontier().iter().cloned());
                }

                cap_set.downgrade(input.frontier().frontier().iter());
            }
        });

    write_boundaries.inspect(|d| info!("ok: {:?}", d));

    // Send the same write boundaries to all workers.
    let write_boundaries = write_boundaries.broadcast();

    // All workers write updates that fit into the boundaries that they got from
    // the one "coordination" operator/worker and forward them. In the
    // real-world implementation this operator would write to `Blob` (via the
    // Batch writer API) and forward just the token that represents the batch
    // (that is, a `HollowBatch`).

    let written_batches = to_sink.binary_frontier(
        &write_boundaries,
        Pipeline,
        Pipeline,
        "write_batches",
        move |_cap, _info| {
            let mut in_flight_batches: HashMap<_, (_, Vec<_>)> = HashMap::new();
            let mut pending_updates = Vec::new();

            let mut data_buffer = Vec::new();
            let mut boundary_buffer = Vec::new();

            move |input_data,
                  input_write_boundaries,
                  output: &mut OutputHandle<u64, WrittenBatch, Tee<u64, _>>| {
                while let Some((cap, data)) = input_write_boundaries.next() {
                    data.swap(&mut boundary_buffer);
                    for boundary in boundary_buffer.drain(..) {
                        let existing = in_flight_batches
                            .insert(boundary, (cap.delayed(cap.time()), Vec::new()));
                        assert!(existing.is_none());
                    }
                }

                while let Some((_cap, data)) = input_data.next() {
                    data.swap(&mut data_buffer);
                    pending_updates.extend(data_buffer.drain(..));
                }

                // Peel off any pending updates that fall into an in-flight
                // batch.
                // NONE: This is probably not the smartest way of doing
                // things...
                for (metadata, (_cap, batch)) in in_flight_batches.iter_mut() {
                    let (mut matching_updates, mut remaining_updates): (Vec<_>, Vec<_>) =
                        pending_updates.drain(..).partition(|(_update, ts, _diff)| {
                            metadata.lower.less_equal(ts) && !metadata.upper.less_equal(ts)
                        });
                    pending_updates.extend(remaining_updates.drain(..));
                    batch.extend(matching_updates.drain(..));
                }

                // Emit finished batches.
                let done_batches = in_flight_batches
                    .keys()
                    .filter(|metadata| {
                        !PartialOrder::less_equal(
                            &input_write_boundaries.frontier.frontier(),
                            &metadata.upper.borrow(),
                        ) && !PartialOrder::less_equal(
                            &input_data.frontier.frontier(),
                            &metadata.upper.borrow(),
                        )
                    })
                    .cloned()
                    .collect::<Vec<_>>();

                for done_batch_metadata in done_batches.into_iter() {
                    let (cap, batch) = in_flight_batches.remove(&done_batch_metadata).unwrap();
                    let mut session = output.session(&cap);
                    let written_batch = WrittenBatch {
                        description: done_batch_metadata,
                        data: batch,
                    };
                    session.give(written_batch);
                }
            }
        },
    );

    // written_batches.inspect(|d| info!("ok: {:?}", d));

    // Route all written batches to one worker, which fuses them and "appends" to
    // Consensus. We know that a batch is finished when the frontier advances.
    // That's when the real-world implementation would append the combined
    // batches to persist.
    let appended_batches = written_batches.unary_frontier(
        Exchange::new(|_| 0),
        "append_batches",
        move |_cap, _info| {
            let mut in_flight_batches: HashMap<_, (_, Vec<_>)> = HashMap::new();

            let mut batch_buffer = Vec::new();

            move |input, output: &mut OutputHandle<u64, AppendedBatch, Tee<u64, _>>| {
                while let Some((cap, data)) = input.next() {
                    data.swap(&mut batch_buffer);
                    for batch in batch_buffer.drain(..) {
                        let existing = in_flight_batches
                            .entry(batch.description.clone())
                            .or_insert_with(|| (cap.delayed(cap.time()), Vec::new()));
                        existing.1.push(batch);
                    }
                }

                // Emit finished batches.
                let done_batches = in_flight_batches
                    .keys()
                    .filter(|metadata| {
                        !PartialOrder::less_equal(
                            &input.frontier.frontier(),
                            &metadata.upper.borrow(),
                        )
                    })
                    .cloned()
                    .collect::<Vec<_>>();

                for done_batch_metadata in done_batches.into_iter() {
                    let (cap, batches) = in_flight_batches.remove(&done_batch_metadata).unwrap();
                    let mut session = output.session(&cap);
                    let appended_batch = AppendedBatch {
                        description: done_batch_metadata,
                        batches,
                    };
                    session.give(appended_batch);
                }
            }
        },
    );

    appended_batches.inspect(|d| info!("ok: {:?}", d));
}

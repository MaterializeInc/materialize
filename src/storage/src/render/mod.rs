// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Renders a plan into a timely/differential dataflow computation.
//!
//! ## Error handling
//!
//! Timely and differential have no idioms for computations that can error. The
//! philosophy is, reasonably, to define the semantics of the computation such
//! that errors are unnecessary: e.g., by using wrap-around semantics for
//! integer overflow.
//!
//! Unfortunately, SQL semantics are not nearly so elegant, and require errors
//! in myriad cases. The classic example is a division by zero, but invalid
//! input for casts, overflowing integer operations, and dozens of other
//! functions need the ability to produce errors ar runtime.
//!
//! At the moment, only *scalar* expression evaluation can fail, so only
//! operators that evaluate scalar expressions can fail. At the time of writing,
//! that includes map, filter, reduce, and join operators. Constants are a bit
//! of a special case: they can be either a constant vector of rows *or* a
//! constant, singular error.
//!
//! The approach taken is to build two parallel trees of computation: one for
//! the rows that have been successfully evaluated (the "oks tree"), and one for
//! the errors that have been generated (the "errs tree"). For example:
//!
//! ```text
//!    oks1  errs1       oks2  errs2
//!      |     |           |     |
//!      |     |           |     |
//!   project  |           |     |
//!      |     |           |     |
//!      |     |           |     |
//!     map    |           |     |
//!      |\    |           |     |
//!      | \   |           |     |
//!      |  \  |           |     |
//!      |   \ |           |     |
//!      |    \|           |     |
//!   project  +           +     +
//!      |     |          /     /
//!      |     |         /     /
//!    join ------------+     /
//!      |     |             /
//!      |     | +----------+
//!      |     |/
//!     oks   errs
//! ```
//!
//! The project operation cannot fail, so errors from errs1 are propagated
//! directly. Map operators are fallible and so can inject additional errors
//! into the stream. Join operators combine the errors from each of their
//! inputs.
//!
//! The semantics of the error stream are minimal. From the perspective of SQL,
//! a dataflow is considered to be in an error state if there is at least one
//! element in the final errs collection. The error value returned to the user
//! is selected arbitrarily; SQL only makes provisions to return one error to
//! the user at a time. There are plans to make the err collection accessible to
//! end users, so they can see all errors at once.
//!
//! To make errors transient, simply ensure that the operator can retract any
//! produced errors when corrected data arrives. To make errors permanent, write
//! the operator such that it never retracts the errors it produced. Future work
//! will likely want to introduce some sort of sort order for errors, so that
//! permanent errors are returned to the user ahead of transient errors—probably
//! by introducing a new error type a la:
//!
//! ```no_run
//! # struct EvalError;
//! # struct SourceError;
//! enum DataflowError {
//!     Transient(EvalError),
//!     Permanent(SourceError),
//! }
//! ```
//!
//! If the error stream is empty, the oks stream must be correct. If the error
//! stream is non-empty, then there are no semantics for the oks stream. This is
//! sufficient to support SQL in its current form, but is likely to be
//! unsatisfactory long term. We suspect that we can continue to imbue the oks
//! stream with semantics if we are very careful in describing what data should
//! and should not be produced upon encountering an error. Roughly speaking, the
//! oks stream could represent the correct result of the computation where all
//! rows that caused an error have been pruned from the stream. There are
//! strange and confusing questions here around foreign keys, though: what if
//! the optimizer proves that a particular key must exist in a collection, but
//! the key gets pruned away because its row participated in a scalar expression
//! evaluation that errored?
//!
//! In the meantime, it is probably wise for operators to keep the oks stream
//! roughly "as correct as possible" even when errors are present in the errs
//! stream. This reduces the amount of recomputation that must be performed
//! if/when the errors are retracted.

use std::any::Any;
use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;
use std::sync::Arc;
use std::time::Duration;

use differential_dataflow::{AsCollection, Hashable};
use timely::communication::Allocate;
use timely::dataflow::channels::pact::Exchange;
use timely::dataflow::operators::generic::builder_rc::OperatorBuilder;
use timely::dataflow::Scope;
use timely::progress::Antichain;
use timely::worker::Worker as TimelyWorker;

use mz_dataflow_types::{sources::SourceDesc, client::controller::storage::CollectionMetadata, DataflowError};
use mz_ore::collections::CollectionExt as IteratorExt;
use mz_persist_client::ShardId;
use mz_repr::{Diff, GlobalId, Row, Timestamp};
use mz_timely_util::operators_async_ext::OperatorBuilderExt;

use crate::storage_state::StorageState;

mod debezium;
pub mod sources;
mod upsert;

/// Assemble the "storage" side of a dataflow, i.e. the sources.
///
/// This method creates a new dataflow to host the implementations of sources for the `dataflow`
/// argument, and returns assets for each source that can import the results into a new dataflow.
pub fn build_storage_dataflow<A: Allocate>(
    timely_worker: &mut TimelyWorker<A>,
    storage_state: &mut StorageState,
    debug_name: &str,
    as_of: Option<Antichain<mz_repr::Timestamp>>,
    source_import: (GlobalId, SourceDesc),
    persist_shard: ShardId,
) {
    let worker_logging = timely_worker.log_register().get("timely");
    let name = format!("Source dataflow: {debug_name}");

    timely_worker.dataflow_core(&name, worker_logging, Box::new(()), |_, scope| {
        // The scope.clone() occurs to allow import in the region.
        // We build a region here to establish a pattern of a scope inside the dataflow,
        // so that other similar uses (e.g. with iterative scopes) do not require weird
        // alternate type signatures.
        scope.clone().region_named(&name, |region| {
            let as_of = as_of.clone().unwrap();
            let debug_name = format!("{debug_name}-sources");

            // Import declared source into the rendering context.
            let (ref src_id, ref source) = source_import;
            // If `as_of` is `None`, the rendering request is invalid. We still need to satisfy it,
            // but we will do this with an empty source.
            let valid = storage_state.source_uppers.contains_key(src_id);
            let ((ok, err), token) = if valid {
                let ((ok, err), token) = crate::render::sources::render_source(
                    &debug_name,
                    &as_of,
                    source.clone(),
                    storage_state,
                    region,
                    src_id.clone(),
                );

                // Capture the frontier of `ok` to present as the "source upper".
                // TODO: remove this code when storage has a better holistic take on source progress.
                // This shared frontier is set up in `CreateSource`, and must be present by the time we render as source.
                let shared_frontier = Rc::clone(&storage_state.source_uppers[src_id]);
                let weak_token = std::sync::Arc::downgrade(&token);
                use timely::dataflow::operators::Operator;
                ok.inner.sink(
                    timely::dataflow::channels::pact::Pipeline,
                    "frontier monitor",
                    move |input| {
                        // Drain the input; we don't need it.
                        input.for_each(|_, _| {});

                        // Only attempt the frontier update if the source is still live.
                        // If it is shutting down, we shouldn't treat the frontier as correct.
                        if let Some(_) = weak_token.upgrade() {
                            // Read the input frontier, and join with the shared frontier.
                            let mut joined_frontier = Antichain::new();
                            let mut borrow = shared_frontier.borrow_mut();
                            for time1 in borrow.iter() {
                                for time2 in &input.frontier.frontier() {
                                    use differential_dataflow::lattice::Lattice;
                                    joined_frontier.insert(time1.join(time2));
                                }
                            }
                            *borrow = joined_frontier;
                        }
                    },
                );

                ((ok, err), token)
            } else {
                // This branch exists only to set up a non-source that can be captured and replayed.
                use timely::dataflow::operators::generic::operator::source;
                use timely::dataflow::operators::ActivateCapability;
                use timely::scheduling::Scheduler;

                let mut tokens = Vec::new();
                let ok = source(region, "InvalidSource", |cap, info| {
                    let mut act_cap = Some(ActivateCapability::new(
                        cap,
                        &info.address,
                        region.activations(),
                    ));

                    let drop_activator = Arc::new(scope.sync_activator_for(&info.address[..]));
                    let drop_activator_weak = Arc::downgrade(&drop_activator);

                    tokens.push(drop_activator);

                    move |_handle| {
                        if drop_activator_weak.upgrade().is_some() {
                            act_cap.take();
                        }
                    }
                })
                .as_collection();
                let err = source(region, "InvalidSource", |cap, info| {
                    let mut act_cap = Some(ActivateCapability::new(
                        cap,
                        &info.address,
                        region.activations(),
                    ));

                    let drop_activator = Arc::new(scope.sync_activator_for(&info.address[..]));
                    let drop_activator_weak = Arc::downgrade(&drop_activator);

                    tokens.push(drop_activator);

                    move |_handle| {
                        if drop_activator_weak.upgrade().is_some() {
                            act_cap.take();
                        }
                    }
                })
                .as_collection();
                ((ok, err), Arc::new(tokens) as Arc<dyn Any + Send + Sync>)
            };

            storage_state.source_tokens.insert(source_import.0, token);

            //TODO handle err collection too
            let sinked_collection = ok.map(Ok).concat(&err.map(Err));
            let operator_name = format!("persist_sink({})", persist_shard);
            let mut persist_op = OperatorBuilder::new(operator_name, region.clone());

            // We want exactly one worker (in the cluster) to send all the data to persist. It's fine
            // if other workers from replicated clusters write the same data, though. In the real
            // implementation, we would use a storage client that transparently handles writing to
            // multiple shards. One shard would then only be written to by one worker but we get
            // parallelism from the sharding.
            // TODO(aljoscha): Storage must learn to keep track of collections, which consist of
            // multiple persist shards. Then we should set it up such that each worker can write to one
            // shard.
            let hashed_id = source_import.0.hashed();
            let active_write_worker = (hashed_id as usize) % scope.peers() == scope.index();

            let mut input =
                persist_op.new_input(&sinked_collection.inner, Exchange::new(move |_| hashed_id));

            // TODO(aljoscha): Configurable timeout? Or no timeout in the persist API?
            let timeout = Duration::from_secs(60);

            // TODO(aljoscha): We need to figure out what to do with error results from these calls.

            let (write, _read) = futures_executor::block_on(
                storage_state.persist_client.open(timeout, persist_shard),
            )
            .expect("could not open persist shard");

            let write = Rc::new(RefCell::new(Some(write)));

            persist_op.build_async(
                region.clone(),
                move |mut capabilities, frontiers, scheduler| async move {
                    capabilities.clear();
                    let mut buffer = Vec::new();
                    let mut stash: HashMap<
                        Timestamp,
                        Vec<(Result<Row, DataflowError>, Timestamp, Diff)>,
                    > = HashMap::new();

                    while scheduler.notified().await {
                        let frontier = frontiers.borrow()[0].clone();

                        if !active_write_worker {
                            return;
                        }

                        let mut write = write.borrow_mut();
                        let write = match &mut *write {
                            Some(write) => write,
                            None => {
                                // We have been cancelled!
                                return;
                            }
                        };

                        while let Some((_cap, data)) = input.next() {
                            data.swap(&mut buffer);

                            for (row, ts, diff) in buffer.drain(..) {
                                stash.entry(ts).or_default().push((row, ts, diff));
                            }
                        }

                        let empty = Vec::new();
                        let updates = stash
                            .iter()
                            .flat_map(|(ts, updates)| {
                                if !frontier.less_equal(ts) {
                                    updates.iter()
                                } else {
                                    empty.iter()
                                }
                            })
                            .map(|&(ref row, ref ts, ref diff)| ((&(), row), ts, diff));

                        // We always append, even in case we don't have any updates, because appending
                        // also advances the frontier.
                        // TODO(aljoscha): Figure out how errors from this should be reported.
                        write
                            .append(timeout, updates, frontier.clone())
                            .await
                            .expect("cannot append updates")
                            .expect("cannot append updates")
                            .expect("invalid/outdated upper");

                        stash.retain(|ts, _updates| frontier.less_equal(ts));
                    }
                },
            )
        })
    });
}

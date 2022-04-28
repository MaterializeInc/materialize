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

use std::collections::HashMap;
use std::rc::Rc;
use std::sync::Arc;

use differential_dataflow::Hashable;
use timely::communication::Allocate;
use timely::dataflow::channels::pact::Exchange;
use timely::dataflow::operators::generic::builder_rc::OperatorBuilder;
use timely::dataflow::Scope;
use timely::progress::Antichain;
use timely::worker::Worker as TimelyWorker;
use timely::PartialOrder;

use mz_dataflow_types::{client::CreateSourceCommand, sources::SourceData};
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
    as_of: Antichain<mz_repr::Timestamp>,
    source: CreateSourceCommand<mz_repr::Timestamp>,
) {
    let worker_logging = timely_worker.log_register().get("timely");
    let name = format!("Source dataflow: {debug_name}");
    timely_worker.dataflow_core(&name, worker_logging, Box::new(()), |_, scope| {
        // The scope.clone() occurs to allow import in the region.
        // We build a region here to establish a pattern of a scope inside the dataflow,
        // so that other similar uses (e.g. with iterative scopes) do not require weird
        // alternate type signatures.
        scope.clone().region_named(&name, |region| {
            let debug_name = format!("{debug_name}-sources");

            let ((ok, err), token) = crate::render::sources::render_source(
                region,
                &debug_name,
                &as_of,
                source.id,
                source.desc.clone(),
                source.storage_metadata.clone(),
                storage_state,
            );
            storage_state
                .source_tokens
                .insert(source.id, Arc::clone(&token));

            let operator_name = format!("persist_sink({})", source.storage_metadata.persist_shard);
            let mut persist_op = OperatorBuilder::new(operator_name, region.clone());

            // We want exactly one worker (in the cluster) to send all the data to persist. It's fine
            // if other workers from replicated clusters write the same data, though. In the real
            // implementation, we would use a storage client that transparently handles writing to
            // multiple shards. One shard would then only be written to by one worker but we get
            // parallelism from the sharding.
            // TODO(aljoscha): Storage must learn to keep track of collections, which consist of
            // multiple persist shards. Then we should set it up such that each worker can write to one
            // shard.
            let hashed_id = source.id.hashed();
            let active_write_worker = (hashed_id as usize) % scope.peers() == scope.index();

            let source_data = ok.map(Ok).concat(&err.map(Err)).inner;
            let mut input = persist_op.new_input(&source_data, Exchange::new(move |_| hashed_id));

            let shared_frontier = Rc::clone(&storage_state.source_uppers[&source.id]);

            let weak_token = Arc::downgrade(&token);

            persist_op.build_async(
                region.clone(),
                move |mut capabilities, frontiers, scheduler| async move {
                    capabilities.clear();
                    let mut buffer = Vec::new();
                    let mut stash: HashMap<_, Vec<_>> = HashMap::new();

                    let mut write = crate::persist_cache::open_writer::<
                        (),
                        SourceData,
                        mz_repr::Timestamp,
                        mz_repr::Diff,
                    >(
                        source.storage_metadata.persist_location,
                        source.storage_metadata.persist_shard,
                    )
                    .await
                    .expect("could not open persist shard");

                    while scheduler.notified().await {
                        let input_frontier = frontiers.borrow()[0].clone();

                        if !active_write_worker
                            || weak_token.upgrade().is_none()
                            || shared_frontier.borrow().is_empty()
                        {
                            return;
                        }

                        while let Some((_cap, data)) = input.next() {
                            data.swap(&mut buffer);

                            for (row, ts, diff) in buffer.drain(..) {
                                stash
                                    .entry(ts)
                                    .or_default()
                                    .push((SourceData(row), ts, diff));
                            }
                        }

                        let empty = Vec::new();
                        let updates = stash
                            .iter()
                            .flat_map(|(ts, updates)| {
                                if !input_frontier.less_equal(ts) {
                                    updates.iter()
                                } else {
                                    empty.iter()
                                }
                            })
                            .map(|&(ref row, ref ts, ref diff)| ((&(), row), ts, diff));

                        if PartialOrder::less_than(&*shared_frontier.borrow(), &input_frontier) {
                            // We always append, even in case we don't have any updates, because appending
                            // also advances the frontier.
                            // TODO(aljoscha): Figure out how errors from this should be reported.
                            let expected_upper = shared_frontier.borrow().clone();
                            write
                                .append(updates, expected_upper, input_frontier.clone())
                                .await
                                .expect("cannot append updates")
                                .expect("cannot append updates")
                                .expect("invalid/outdated upper");

                            *shared_frontier.borrow_mut() = input_frontier.clone();
                        }

                        stash.retain(|ts, _updates| input_frontier.less_equal(ts));
                    }
                },
            )
        })
    });
}

// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! A continual task presents as something like a `TRIGGER`: it watches some
//! _input_ and whenever it changes at time `T`, executes a SQL txn, writing to
//! some _output_ at the same time `T`. It can also read anything in materialize
//! as a _reference_, most notably including the output.
//!
//! Only reacting to new inputs (and not the full history) makes a CT's
//! rehydration time independent of the size of the inputs (NB this is not true
//! for references), enabling things like writing UPSERT on top of an
//! append-only shard in SQL (ignore the obvious bug with my upsert impl):
//!
//! ```sql
//! CREATE CONTINUAL TASK upsert (key INT, val INT) ON INPUT append_only AS (
//!     DELETE FROM upsert WHERE key IN (SELECT key FROM append_only);
//!     INSERT INTO upsert SELECT key, max(val) FROM append_only GROUP BY key;
//! )
//! ```
//!
//! Unlike a materialized view, the continual task does not update outputs if
//! references later change. This enables things like auditing:
//!
//! ```sql
//! CREATE CONTINUAL TASK audit_log (count INT8) ON INPUT anomalies AS (
//!     INSERT INTO audit_log SELECT * FROM anomalies;
//! )
//! ```
//!
//! Rough implementation overview:
//! - A CT is created and starts at some `start_ts` optionally later dropped and
//!   stopped at some `end_ts`.
//! - A CT takes one or more _input_s. These must be persist shards (i.e. TABLE,
//!   SOURCE, MV, but not VIEW).
//! - A CT has one or more _output_s. The outputs are (initially) owned by the
//!   task and cannot be written to by other parts of the system.
//! - The task is run for each time one of the inputs changes starting at
//!   `start_ts`.
//! - It is given the changes in its inputs at time `T` as diffs.
//!   - These are presented as two SQL relations with just the inserts/deletes.
//!   - NB: A full collection for the input can always be recovered by also
//!     using the input as a "reference" (see below) and applying the diffs.
//! - The task logic is expressed as a SQL transaction that does all reads at
//!   commits all writes at `T`
//!   - The notable exception to this is self-referential reads of the CT
//!     output. See below for how that works.
//! - This logic can _reference_ any nameable object in the system, not just the
//!   inputs.
//!   - However, the logic/transaction can mutate only the outputs.
//! - Summary of differences between inputs and references:
//!   - The task receives snapshot + changes for references (like regular
//!     dataflow inputs today) but only changes for inputs.
//!   - The task only produces output in response to changes in the inputs but
//!     not in response to changes in the references.
//! - Instead of re-evaluating the task logic from scratch for each input time,
//!   we maintain the collection representing desired writes to the output(s) as
//!   a dataflow.
//! - The task dataflow is tied to a `CLUSTER` and runs on each `REPLICA`.
//!   - HA strategy: multi-replica clusters race to commit and the losers throw
//!     away the result.
//!
//! ## Self-References
//!
//! Self-references must be handled differently from other reads. When computing
//! the proposed write to some output at `T`, we can only know the contents of
//! it through `T-1` (the exclusive upper is `T`).
//!
//! We address this by initially assuming that the output contains no changes at
//! `T`, then evaluating each of the statements in order, allowing them to see
//! the proposed output changes made by the previous statements. By default,
//! this is stopped after one iteration and proposed output diffs are committed
//! if possible. (We could also add options for iterating to a fixpoint,
//! stop/error after N iters, etc.) Then to compute the changes at `T+1`, we
//! read in what was actually written to the output at `T` (maybe some other
//! replica wrote something different) and begin again.
//!
//! The above is very similar to how timely/differential dataflow iteration
//! works, except that our feedback loop goes through persist and the loop
//! timestamp is already `mz_repr::Timestamp`.
//!
//! This is implemented as follows:
//! - `let I = persist_source(self-reference)`
//! - Transform `I` such that the contents at `T-1` are presented at `T` (i.e.
//!   initially assume `T` is unchanged from `T-1`).
//! - TODO(ct3): Actually implement the following.
//! - In an iteration sub-scope:
//!   - Bring `I` into the sub-scope and `let proposed = Variable`.
//!   - We need a collection that at `(T, 0)` is always the contents of `I` at
//!     `T`, but at `(T, 1...)` contains the proposed diffs by the CT logic. We
//!     can construct it by concatenating `I` with `proposed` except that we
//!     also need to retract everything in `proposed` for the next `(T+1, 0)`
//!     (because `I` is the source of truth for what actually committed).
//!  - `let R = retract_at_next_outer_ts(proposed)`
//!  - `let result = logic(concat(I, proposed, R))`
//!  - `proposed.set(result)`
//! - Then we return `proposed.leave()` for attempted write to persist.
//!
//! ## As Ofs and Output Uppers
//!
//! - A continual task is first created with an initial as_of `I`. It is
//!   initially rendered at as_of `I==A` but as it makes progress, it may be
//!   rendered at later as_ofs `I<A`.
//! - It is required that the output collection springs into existence at `I`
//!   (i.e. receives the initial contents at `I`).
//!   - For a snapshot CT, the full contents of the input at `I` are run through
//!     the CT logic and written at `I`.
//!   - For a non-snapshot CT, the collection is defined to be empty at `I`
//!     (i.e. if the input happened to be written exactly at `I`, we'd ignore
//!     it) and then start writing at `I+1`.
//! - As documented in [DataflowDescription::as_of], `A` is the time we render
//!   the inputs.
//!   - An MV with an as_of of `A` will both have inputs rendered at `A` and
//!     also the first time it could write is also `A`.
//!   - A CT is the same on the initial render (`I==A`), but on renders after it
//!     has made progress (`I<A`) the first time that  it could potentially
//!     write is `A+1`. This is because a persist_source started with
//!     SnapshotMode::Exclude can only start emitting diffs at `as_of+1`.
//!   - As a result, we hold back the since on inputs to be strictly less than
//!     the upper of the output. (This is only necessary for CTs, but we also do
//!     it for MVs to avoid the special case.)
//!   - For CT "inputs" (which are disallowed from being the output), we render
//!     the persist_source with as_of `A`.
//!     - When `I==A` we include the snapshot iff the snapshot option is used.
//!     - When `I<A` we always exclude the snapshot. It would be unnecessary and
//!       this is an absolutely critical performance optimization to make CT
//!       rehydration times independent of input size.
//!   - For CT "references", we render the persist_source with as_of `A` and
//!     always include the snapshot.
//!     - There is one subtlety: self-references on the initial render. We need
//!       the contents to be available at `A-1`, so that we can do the
//!       step_forward described above to get it at `A`. However, the collection
//!       springs into existence at `I`, so we when `I==A`, we're not allowed to
//!       read it as_of `A-1` (the since of the shard may have advanced past
//!       that). We address this by rendering the persist_source as normal at
//!       `A`. On startup, persist_source immediately downgrades its frontier to
//!       `A` (making `A-1` readable). Combined with step_forward, this is
//!       enough to unblock the CT self-reference. We do however have to tweak
//!       the `suppress_early_progress` operator to use `A-1` instead of `A` for
//!       this case.
//!     - On subsequent renders, self-references work as normal.

use std::any::Any;
use std::cell::RefCell;
use std::collections::BTreeSet;
use std::rc::Rc;
use std::sync::Arc;

use differential_dataflow::consolidation::ConsolidatingContainerBuilder;
use differential_dataflow::difference::Semigroup;
use differential_dataflow::lattice::Lattice;
use differential_dataflow::{AsCollection, Collection, Hashable};
use futures::{Future, FutureExt, StreamExt};
use mz_compute_types::dataflows::DataflowDescription;
use mz_compute_types::sinks::{ComputeSinkConnection, ComputeSinkDesc, ContinualTaskConnection};
use mz_ore::cast::CastFrom;
use mz_ore::collections::HashMap;
use mz_persist_client::error::UpperMismatch;
use mz_persist_client::operators::shard_source::SnapshotMode;
use mz_persist_client::write::WriteHandle;
use mz_persist_client::Diagnostics;
use mz_persist_types::codec_impls::UnitSchema;
use mz_repr::{Diff, GlobalId, Row, Timestamp};
use mz_storage_types::controller::CollectionMetadata;
use mz_storage_types::errors::DataflowError;
use mz_storage_types::sources::SourceData;
use mz_timely_util::builder_async::{Button, Event, OperatorBuilder as AsyncOperatorBuilder};
use mz_timely_util::operator::CollectionExt;
use timely::dataflow::channels::pact::{Exchange, Pipeline};
use timely::dataflow::operators::generic::builder_rc::OperatorBuilder;
use timely::dataflow::operators::{Filter, FrontierNotificator, Map, Operator};
use timely::dataflow::{ProbeHandle, Scope};
use timely::progress::frontier::AntichainRef;
use timely::progress::{Antichain, Timestamp as _};
use timely::{Data, PartialOrder};
use tracing::debug;

use crate::compute_state::ComputeState;
use crate::render::sinks::SinkRender;
use crate::render::StartSignal;
use crate::sink::ConsolidatingVec;

pub(crate) struct ContinualTaskCtx<G: Scope<Timestamp = Timestamp>> {
    name: Option<String>,
    dataflow_as_of: Option<Antichain<Timestamp>>,
    inputs_with_snapshot: Option<bool>,
    ct_inputs: BTreeSet<GlobalId>,
    ct_outputs: BTreeSet<GlobalId>,
    pub ct_times: Vec<Collection<G, (), Diff>>,
}

/// An encapsulation of the transformation logic necessary on data coming into a
/// continual task.
///
/// NB: In continual task jargon, an "input" contains diffs and a "reference" is
/// a normal source/collection.
pub(crate) enum ContinualTaskSourceTransformer {
    /// A collection containing, at each time T, exactly the inserts at time T
    /// in the transformed collection.
    ///
    /// For example:
    /// - Input: {} at 0, {1} at 1, {1} at 2, ...
    /// - Output: {} at 0, {1} at 1, {} at 2, ...
    ///
    /// We'll presumably have the same for deletes eventually, but it's not
    /// exposed in the SQL frontend yet.
    InsertsInput {
        source_id: GlobalId,
        with_snapshot: bool,
    },
    /// A self-reference to the continual task's output. This is essentially a
    /// timely feedback loop via the persist shard. See module rustdoc for how
    /// this works.
    SelfReference { source_id: GlobalId },
    /// A normal collection (no-op transformation).
    NormalReference,
}

impl ContinualTaskSourceTransformer {
    /// The persist_source `SnapshotMode` to use when reading this source.
    pub fn snapshot_mode(&self) -> SnapshotMode {
        use ContinualTaskSourceTransformer::*;
        match self {
            InsertsInput {
                with_snapshot: false,
                ..
            } => SnapshotMode::Exclude,
            InsertsInput {
                with_snapshot: true,
                ..
            }
            | SelfReference { .. }
            | NormalReference => SnapshotMode::Include,
        }
    }

    /// Returns the as_of to use with the suppress_early_progress operator for
    /// this source. See the module rustdoc for context.
    pub fn suppress_early_progress_as_of(
        &self,
        as_of: Antichain<Timestamp>,
    ) -> Antichain<Timestamp> {
        use ContinualTaskSourceTransformer::*;
        match self {
            InsertsInput { .. } => as_of,
            SelfReference { .. } => as_of
                .iter()
                .map(|x| x.step_back().unwrap_or_else(Timestamp::minimum))
                .collect(),
            NormalReference => as_of,
        }
    }

    /// Performs the necessary transformation on the source collection.
    ///
    /// Returns the transformed "oks" and "errs" collections. Also returns the
    /// appropriate `ct_times` collection used to inform the sink which times
    /// were changed in the inputs.
    pub fn transform<S: Scope<Timestamp = Timestamp>>(
        &self,
        oks: Collection<S, Row, Diff>,
        errs: Collection<S, DataflowError, Diff>,
    ) -> (
        Collection<S, Row, Diff>,
        Collection<S, DataflowError, Diff>,
        Collection<S, (), Diff>,
    ) {
        use ContinualTaskSourceTransformer::*;
        match self {
            // Make a collection s.t, for each time T in the input, the output
            // contains the inserts at T.
            InsertsInput { source_id, .. } => {
                let name = source_id.to_string();
                // Keep only the inserts.
                let oks = oks.inner.filter(|(_, _, diff)| *diff > 0);
                // Grab the original times for use in the sink operator.
                let (oks, times) = oks.as_collection().times_extract(&name);
                // Then retract everything at the next timestamp.
                let oks = oks.inner.flat_map(|(row, ts, diff)| {
                    let retract_ts = ts.step_forward();
                    let negation = -diff;
                    [(row.clone(), ts, diff), (row, retract_ts, negation)]
                });
                (oks.as_collection(), errs, times)
            }
            NormalReference => {
                let times = Collection::empty(&oks.scope());
                (oks, errs, times)
            }
            // When computing an self-referential output at `T`, start by
            // assuming there are no changes from the contents at `T-1`. See the
            // module rustdoc for how this fits into the larger picture.
            SelfReference { source_id } => {
                let name = source_id.to_string();
                let times = Collection::empty(&oks.scope());
                // step_forward will panic at runtime if it receives a data or
                // capability with a time that cannot be stepped forward (i.e.
                // because it is already the max). We're safe here because this
                // is stepping `T-1` forward to `T`.
                let oks = oks.step_forward(&name);
                let errs = errs.step_forward(&name);
                (oks, errs, times)
            }
        }
    }
}

impl<G: Scope<Timestamp = Timestamp>> ContinualTaskCtx<G> {
    pub fn new<P, S>(dataflow: &DataflowDescription<P, S, Timestamp>) -> Self {
        let mut name = None;
        let mut ct_inputs = BTreeSet::new();
        let mut ct_outputs = BTreeSet::new();
        let mut inputs_with_snapshot = None;
        for (sink_id, sink) in &dataflow.sink_exports {
            match &sink.connection {
                ComputeSinkConnection::ContinualTask(ContinualTaskConnection {
                    input_id, ..
                }) => {
                    ct_outputs.insert(*sink_id);
                    ct_inputs.insert(*input_id);
                    // There's only one CT sink per dataflow at this point.
                    assert_eq!(name, None);
                    name = Some(sink_id.to_string());
                    assert_eq!(inputs_with_snapshot, None);
                    match (
                        sink.with_snapshot,
                        dataflow.as_of.as_ref(),
                        dataflow.initial_storage_as_of.as_ref(),
                    ) {
                        // User specified no snapshot when creating the CT.
                        (false, _, _) => inputs_with_snapshot = Some(false),
                        // User specified a snapshot but we're past the initial
                        // as_of.
                        (true, Some(as_of), Some(initial_as_of))
                            if PartialOrder::less_than(initial_as_of, as_of) =>
                        {
                            inputs_with_snapshot = Some(false)
                        }
                        // User specified a snapshot and we're either at the
                        // initial creation, or we don't know (builtin CTs). If
                        // we don't know, it's always safe to fall back to
                        // snapshotting, at worst it's wasted work and will get
                        // filtered.
                        (true, _, _) => inputs_with_snapshot = Some(true),
                    }
                }
                _ => continue,
            }
        }
        let mut ret = ContinualTaskCtx {
            name,
            dataflow_as_of: None,
            inputs_with_snapshot,
            ct_inputs,
            ct_outputs,
            ct_times: Vec::new(),
        };
        // Only clone the as_of if we're in a CT dataflow.
        if ret.is_ct_dataflow() {
            ret.dataflow_as_of = dataflow.as_of.clone();
            // Sanity check that we have a name if we're in a CT dataflow.
            assert!(ret.name.is_some());
        }
        ret
    }

    pub fn is_ct_dataflow(&self) -> bool {
        // Inputs are non-empty iff outputs are non-empty.
        assert_eq!(self.ct_inputs.is_empty(), self.ct_outputs.is_empty());
        !self.ct_outputs.is_empty()
    }

    pub fn get_ct_source_transformer(
        &self,
        source_id: GlobalId,
    ) -> Option<ContinualTaskSourceTransformer> {
        let Some(inputs_with_snapshot) = self.inputs_with_snapshot else {
            return None;
        };
        let transformer = match (
            self.ct_inputs.contains(&source_id),
            self.ct_outputs.contains(&source_id),
        ) {
            (false, false) => ContinualTaskSourceTransformer::NormalReference,
            (false, true) => ContinualTaskSourceTransformer::SelfReference { source_id },
            (true, false) => ContinualTaskSourceTransformer::InsertsInput {
                source_id,
                with_snapshot: inputs_with_snapshot,
            },
            (true, true) => panic!("ct output is not allowed to be an input"),
        };
        Some(transformer)
    }

    pub fn input_times(&self, scope: &G) -> Option<Collection<G, (), Diff>> {
        // We have a name iff this is a CT dataflow.
        assert_eq!(self.is_ct_dataflow(), self.name.is_some());
        let Some(name) = self.name.as_ref() else {
            return None;
        };
        // Note that self.ct_times might be empty (if the user didn't reference
        // the input), but this still does the correct, though maybe useless,
        // thing: no diffs coming into the input means no times to write at.
        let ct_times = differential_dataflow::collection::concatenate(
            &mut scope.clone(),
            self.ct_times.iter().cloned(),
        );
        // Reduce this down to one update per-time-per-worker before exchanging
        // it, so we don't waste work on unnecessarily high data volumes.
        let ct_times = ct_times.times_reduce(name);
        Some(ct_times)
    }
}

impl<G> SinkRender<G> for ContinualTaskConnection<CollectionMetadata>
where
    G: Scope<Timestamp = Timestamp>,
{
    fn render_sink(
        &self,
        compute_state: &mut ComputeState,
        _sink: &ComputeSinkDesc<CollectionMetadata>,
        sink_id: GlobalId,
        as_of: Antichain<Timestamp>,
        start_signal: StartSignal,
        oks: Collection<G, Row, Diff>,
        errs: Collection<G, DataflowError, Diff>,
        append_times: Option<Collection<G, (), Diff>>,
    ) -> Option<Rc<dyn Any>> {
        let name = sink_id.to_string();

        let to_append = oks
            .map(|x| SourceData(Ok(x)))
            .concat(&errs.map(|x| SourceData(Err(x))));
        let append_times = append_times.expect("should be provided by ContinualTaskCtx");

        let write_handle = {
            let clients = Arc::clone(&compute_state.persist_clients);
            let metadata = self.storage_metadata.clone();
            let handle_purpose = format!("ct_sink({})", name);
            async move {
                let client = clients
                    .open(metadata.persist_location)
                    .await
                    .expect("valid location");
                client
                    .open_writer(
                        metadata.data_shard,
                        metadata.relation_desc.into(),
                        UnitSchema.into(),
                        Diagnostics {
                            shard_name: sink_id.to_string(),
                            handle_purpose,
                        },
                    )
                    .await
                    .expect("codecs should match")
            }
        };

        let collection = compute_state.expect_collection_mut(sink_id);
        let mut probe = ProbeHandle::default();
        let to_append = to_append.probe_with(&mut probe);
        collection.compute_probe = Some(probe);
        let sink_write_frontier = Rc::new(RefCell::new(Antichain::from_elem(Timestamp::minimum())));
        collection.sink_write_frontier = Some(Rc::clone(&sink_write_frontier));

        // TODO(ct1): Obey `compute_state.read_only_rx`
        //
        // Seemingly, the read-only env needs to tail the output shard and keep
        // historical updates around until it sees that the output frontier
        // advances beyond their times.
        let sink_button = continual_task_sink(
            &name,
            to_append,
            append_times,
            as_of,
            write_handle,
            start_signal,
            sink_write_frontier,
        );
        Some(Rc::new(sink_button.press_on_drop()))
    }
}

fn continual_task_sink<G: Scope<Timestamp = Timestamp>>(
    name: &str,
    to_append: Collection<G, SourceData, Diff>,
    append_times: Collection<G, (), Diff>,
    as_of: Antichain<Timestamp>,
    write_handle: impl Future<Output = WriteHandle<SourceData, (), Timestamp, Diff>> + Send + 'static,
    start_signal: StartSignal,
    output_frontier: Rc<RefCell<Antichain<Timestamp>>>,
) -> Button {
    let scope = to_append.scope();
    let mut op = AsyncOperatorBuilder::new(format!("ct_sink({})", name), scope.clone());

    // TODO(ct2): This all works perfectly well data parallel (assuming we
    // broadcast the append_times). We just need to hook it up to the
    // multi-worker persist-sink, but that requires some refactoring. This would
    // also remove the need for this to be an async timely operator.
    let active_worker = name.hashed();
    let to_append_input =
        op.new_input_for_many(&to_append.inner, Exchange::new(move |_| active_worker), []);
    let append_times_input = op.new_input_for_many(
        &append_times.inner,
        Exchange::new(move |_| active_worker),
        [],
    );

    let active_worker = usize::cast_from(active_worker) % scope.peers() == scope.index();
    let button = op.build(move |_capabilities| async move {
        if !active_worker {
            output_frontier.borrow_mut().clear();
            return;
        }

        // SUBTLE: The start_signal below may not be unblocked by the compute
        // controller until it thinks the inputs are "ready" (i.e. readable at
        // the as_of), but if the CT is self-referential, one of the inputs will
        // be the output (which starts at `T::minimum()`, not the as_of). To
        // break this cycle, before we even get the start signal, go ahead and
        // advance the output's (exclusive) upper to the first time that this CT
        // might write: `as_of+1`. Because we don't want this to happen on
        // restarts, only do it if the upper is `T::minimum()`.
        let mut write_handle = write_handle.await;
        {
            let res = write_handle
                .compare_and_append_batch(
                    &mut [],
                    Antichain::from_elem(Timestamp::minimum()),
                    as_of.clone(),
                )
                .await
                .expect("usage was valid");
            match res {
                // We advanced the upper.
                Ok(()) => {}
                // Someone else advanced the upper.
                Err(UpperMismatch { .. }) => {}
            }
        }

        let () = start_signal.await;

        #[derive(Debug)]
        enum OpEvent<C> {
            ToAppend(Event<Timestamp, C, Vec<(SourceData, Timestamp, Diff)>>),
            AppendTimes(Event<Timestamp, C, Vec<((), Timestamp, Diff)>>),
        }

        impl<C: std::fmt::Debug> OpEvent<C> {
            fn apply(self, state: &mut SinkState<SourceData, Timestamp>) {
                debug!("ct_sink event {:?}", self);
                match self {
                    OpEvent::ToAppend(Event::Data(_cap, x)) => {
                        for (k, t, d) in x {
                            state.to_append.push(((k, t), d));
                        }
                    }
                    OpEvent::ToAppend(Event::Progress(x)) => state.to_append_progress = x,
                    OpEvent::AppendTimes(Event::Data(_cap, x)) => state
                        .append_times
                        .extend(x.into_iter().map(|((), t, _d)| t)),
                    OpEvent::AppendTimes(Event::Progress(x)) => state.append_times_progress = x,
                }
            }
        }

        let to_insert_input = to_append_input.map(OpEvent::ToAppend);
        let append_times_input = append_times_input.map(OpEvent::AppendTimes);
        let mut op_inputs = futures::stream::select(to_insert_input, append_times_input);

        let mut state = SinkState::new();
        loop {
            // Loop until we've processed all the work we can.
            loop {
                if PartialOrder::less_than(&*output_frontier.borrow(), &state.output_progress) {
                    output_frontier.borrow_mut().clear();
                    output_frontier
                        .borrow_mut()
                        .extend(state.output_progress.iter().cloned());
                }

                debug!("ct_sink about to process {:?}", state);
                let Some((new_upper, to_append)) = state.process() else {
                    break;
                };
                debug!("ct_sink got write {:?}: {:?}", new_upper, to_append);
                state.output_progress =
                    truncating_compare_and_append(&mut write_handle, to_append, new_upper).await;
            }

            // Then try to generate some more work by reading inputs.
            let Some(event) = op_inputs.next().await else {
                // Inputs exhausted, shutting down.
                output_frontier.borrow_mut().clear();
                return;
            };
            event.apply(&mut state);
            // Also drain any other events that may be ready.
            while let Some(Some(event)) = op_inputs.next().now_or_never() {
                event.apply(&mut state);
            }
        }
    });

    button
}

/// Writes the given data to the shard, truncating it as necessary.
///
/// Returns the latest known upper for the shard.
async fn truncating_compare_and_append(
    write_handle: &mut WriteHandle<SourceData, (), Timestamp, Diff>,
    to_append: Vec<((&SourceData, &()), &Timestamp, &Diff)>,
    new_upper: Antichain<Timestamp>,
) -> Antichain<Timestamp> {
    let mut expected_upper = write_handle.shared_upper();
    loop {
        if !PartialOrder::less_than(&expected_upper, &new_upper) {
            debug!("ct_sink skipping {:?}", new_upper.elements());
            return expected_upper;
        }
        let res = write_handle
            .compare_and_append(&to_append, expected_upper.clone(), new_upper.clone())
            .await
            .expect("usage was valid");
        debug!(
            "ct_sink write res {:?}-{:?}: {:?}",
            expected_upper.elements(),
            new_upper.elements(),
            res
        );
        match res {
            Ok(()) => return new_upper,
            Err(err) => {
                expected_upper = err.current;
                continue;
            }
        }
    }
}

#[derive(Debug)]
struct SinkState<D, T> {
    /// The known times at which we're going to write data to the output. This
    /// is guaranteed to include all times < append_times_progress, except that
    /// ones < output_progress may have been truncated.
    append_times: BTreeSet<T>,
    append_times_progress: Antichain<T>,

    /// The data we've collected to append to the output. This is often
    /// compacted to advancing times and is expected to be ~empty in the steady
    /// state.
    to_append: ConsolidatingVec<(D, T)>,
    to_append_progress: Antichain<T>,

    /// A lower bound on the upper of the output.
    output_progress: Antichain<T>,
}

impl<D: Ord> SinkState<D, Timestamp> {
    fn new() -> Self {
        SinkState {
            append_times: BTreeSet::new(),
            append_times_progress: Antichain::from_elem(Timestamp::minimum()),
            to_append: ConsolidatingVec::new(128, 0),
            to_append_progress: Antichain::from_elem(Timestamp::minimum()),
            output_progress: Antichain::from_elem(Timestamp::minimum()),
        }
    }

    /// Returns data to write to the output, if any, and the new upper to use.
    fn process(&mut self) -> Option<(Antichain<Timestamp>, Vec<((&D, &()), &Timestamp, &Diff)>)> {
        // We can only append at times >= the output_progress, so pop off
        // anything unnecessary.
        while let Some(x) = self.append_times.first() {
            if self.output_progress.less_equal(x) {
                break;
            }
            self.append_times.pop_first();
        }

        // Find the smallest append_time before append_time_progress. This is
        // the next time we might need to write data at. Note that we can only
        // act on append_times once the progress has passed them, because they
        // could come out of order.
        let write_ts = match self.append_times.first() {
            Some(x) if !self.append_times_progress.less_equal(x) => x,
            Some(_) | None => {
                // The CT sink's contract is that it only writes data at times
                // we received an input diff. There are none in
                // `[output_progress, append_times_progress)`, so we can go
                // ahead and advance the upper of the output, if it's not
                // already.
                //
                // We could instead ensure liveness by basing this off of
                // to_append, but for any CTs reading the output (expected to be
                // a common case) we'd end up looping each timestamp through
                // persist one-by-one.
                if PartialOrder::less_than(&self.output_progress, &self.append_times_progress) {
                    return Some((self.append_times_progress.clone(), Vec::new()));
                }
                // Otherwise, nothing to do!
                return None;
            }
        };

        if self.to_append_progress.less_equal(write_ts) {
            // Don't have all the necessary data yet.
            if self.output_progress.less_than(write_ts) {
                // We can advance the output upper up to the write_ts. For
                // self-referential CTs this might be necessary to ensure
                // dataflow progress.
                return Some((Antichain::from_elem(write_ts.clone()), Vec::new()));
            }
            return None;
        }

        // Time to write some data! Produce the collection as of write_ts by
        // advancing timestamps, consolidating, and filtering out anything at
        // future timestamps.
        let as_of = &[write_ts.clone()];
        for ((_, t), _) in self.to_append.iter_mut() {
            t.advance_by(AntichainRef::new(as_of))
        }
        // TODO(ct2): Metrics for vec len and cap.
        self.to_append.consolidate();

        let append_data = self
            .to_append
            .iter()
            .filter_map(|((k, t), d)| (t <= write_ts).then_some(((k, &()), t, d)))
            .collect();
        Some((Antichain::from_elem(write_ts.step_forward()), append_data))
    }
}

trait StepForward<G: Scope, D, R> {
    /// Translates a collection one timestamp "forward" (i.e. `T` -> `T+1` as
    /// defined by `TimestampManipulation::step_forward`).
    ///
    /// This includes:
    /// - The differential timestamps in each data.
    /// - The capabilities paired with that data.
    /// - (As a consequence of the previous) the output frontier is one step forward
    ///   of the input frontier.
    ///
    /// The caller is responsible for ensuring that all data and capabilities given
    /// to this operator can be stepped forward without panicking, otherwise the
    /// operator will panic at runtime.
    fn step_forward(&self, name: &str) -> Collection<G, D, R>;
}

impl<G, D, R> StepForward<G, D, R> for Collection<G, D, R>
where
    G: Scope<Timestamp = Timestamp>,
    D: Data,
    R: Semigroup + 'static,
{
    fn step_forward(&self, name: &str) -> Collection<G, D, R> {
        let name = format!("ct_step_forward({})", name);
        let mut builder = OperatorBuilder::new(name, self.scope());
        let (mut output, output_stream) = builder.new_output();
        // We step forward (by one) each data timestamp and capability. As a
        // result the output's frontier is guaranteed to be one past the input
        // frontier, so make this promise to timely.
        let step_forward_summary = Timestamp::from(1);
        let mut input = builder.new_input_connection(
            &self.inner,
            Pipeline,
            vec![Antichain::from_elem(step_forward_summary)],
        );
        builder.set_notify(false);
        builder.build(move |_caps| {
            move |_frontiers| {
                let mut output = output.activate();
                while let Some((cap, data)) = input.next() {
                    for (_, ts, _) in data.iter_mut() {
                        *ts = ts.step_forward();
                    }
                    let cap = cap.delayed(&cap.time().step_forward());
                    output.session(&cap).give_container(data);
                }
            }
        });

        output_stream.as_collection()
    }
}

trait TimesExtract<G: Scope, D, R> {
    /// Returns a collection with the times changed in the input collection.
    ///
    /// This works by mapping the data piece of the differential tuple to `()`.
    /// It is essentially the same as the following, but without cloning
    /// everything in the input.
    ///
    /// ```ignore
    /// input.map(|(_data, ts, diff)| ((), ts, diff))
    /// ```
    ///
    /// The output may be partially consolidated, but no consolidation
    /// guarantees are made.
    fn times_extract(&self, name: &str) -> (Collection<G, D, R>, Collection<G, (), R>);
}

impl<G, D, R> TimesExtract<G, D, R> for Collection<G, D, R>
where
    G: Scope<Timestamp = Timestamp>,
    D: Clone + 'static,
    R: Semigroup + 'static + std::fmt::Debug,
{
    fn times_extract(&self, name: &str) -> (Collection<G, D, R>, Collection<G, (), R>) {
        let name = format!("ct_times_extract({})", name);
        let mut builder = OperatorBuilder::new(name, self.scope());
        let (mut passthrough, passthrough_stream) = builder.new_output();
        let (mut times, times_stream) = builder.new_output::<ConsolidatingContainerBuilder<_>>();
        let mut input = builder.new_input(&self.inner, Pipeline);
        builder.set_notify(false);
        builder.build(|_caps| {
            move |_frontiers| {
                let mut passthrough = passthrough.activate();
                let mut times = times.activate();
                while let Some((cap, data)) = input.next() {
                    let times_iter = data.iter().map(|(_data, ts, diff)| ((), *ts, diff.clone()));
                    times.session_with_builder(&cap).give_iterator(times_iter);
                    passthrough.session(&cap).give_container(data);
                }
            }
        });
        (
            passthrough_stream.as_collection(),
            times_stream.as_collection(),
        )
    }
}

trait TimesReduce<G: Scope, R> {
    /// This is essentially a specialized impl of consolidate, with a HashMap
    /// instead of the Trace.
    fn times_reduce(&self, name: &str) -> Collection<G, (), R>;
}

impl<G, R> TimesReduce<G, R> for Collection<G, (), R>
where
    G: Scope<Timestamp = Timestamp>,
    R: Semigroup + 'static + std::fmt::Debug,
{
    fn times_reduce(&self, name: &str) -> Collection<G, (), R> {
        let name = format!("ct_times_reduce({})", name);
        self.inner
            .unary_frontier(Pipeline, &name, |_caps, _info| {
                let mut notificator = FrontierNotificator::default();
                let mut stash = HashMap::<_, R>::new();
                move |input, output| {
                    while let Some((cap, data)) = input.next() {
                        for ((), ts, diff) in data.drain(..) {
                            notificator.notify_at(cap.delayed(&ts));
                            if let Some(sum) = stash.get_mut(&ts) {
                                sum.plus_equals(&diff);
                            } else {
                                stash.insert(ts, diff);
                            }
                        }
                    }
                    notificator.for_each(&[input.frontier()], |cap, _not| {
                        if let Some(diff) = stash.remove(cap.time()) {
                            output.session(&cap).give(((), cap.time().clone(), diff));
                        }
                    });
                }
            })
            .as_collection()
    }
}

#[cfg(test)]
mod tests {
    use differential_dataflow::AsCollection;
    use mz_repr::Timestamp;
    use timely::dataflow::operators::capture::Extract;
    use timely::dataflow::operators::{Capture, Input, ToStream};
    use timely::dataflow::ProbeHandle;
    use timely::progress::Antichain;
    use timely::Config;

    use super::*;

    #[mz_ore::test]
    fn step_forward() {
        timely::execute(Config::thread(), |worker| {
            let (mut input, probe, output) = worker.dataflow(|scope| {
                let (handle, input) = scope.new_input();
                let mut probe = ProbeHandle::<Timestamp>::new();
                let output = input
                    .as_collection()
                    .step_forward("test")
                    .probe_with(&mut probe)
                    .inner
                    .capture();
                (handle, probe, output)
            });

            let mut expected = Vec::new();
            for i in 0u64..10 {
                let in_ts = Timestamp::new(i);
                let out_ts = in_ts.step_forward();
                input.send((i, in_ts, 1));
                input.advance_to(in_ts.step_forward());

                // We should get the data out advanced by `step_forward` and
                // also, crucially, the output frontier should do the same (i.e.
                // this is why we can't simply use `Collection::delay`).
                worker.step_while(|| probe.less_than(&out_ts.step_forward()));
                expected.push((i, out_ts, 1));
            }
            // Closing the input should allow the output to advance and the
            // dataflow to shut down.
            input.close();
            while worker.step() {}

            let actual = output
                .extract()
                .into_iter()
                .flat_map(|x| x.1)
                .collect::<Vec<_>>();
            assert_eq!(actual, expected);
        })
        .unwrap();
    }

    #[mz_ore::test]
    fn times_extract() {
        struct PanicOnClone;

        impl Clone for PanicOnClone {
            fn clone(&self) -> Self {
                panic!("boom")
            }
        }

        let output = timely::execute_directly(|worker| {
            worker.dataflow(|scope| {
                let input = [
                    (PanicOnClone, Timestamp::new(0), 0),
                    (PanicOnClone, Timestamp::new(1), 1),
                    (PanicOnClone, Timestamp::new(1), 1),
                    (PanicOnClone, Timestamp::new(2), -2),
                    (PanicOnClone, Timestamp::new(2), 1),
                ]
                .to_stream(scope)
                .as_collection();
                let (_passthrough, times) = input.times_extract("test");
                times.inner.capture()
            })
        });
        let expected = vec![((), Timestamp::new(1), 2), ((), Timestamp::new(2), -1)];
        let actual = output
            .extract()
            .into_iter()
            .flat_map(|x| x.1)
            .collect::<Vec<_>>();
        assert_eq!(actual, expected);
    }

    #[mz_ore::test]
    fn times_reduce() {
        let output = timely::execute_directly(|worker| {
            worker.dataflow(|scope| {
                let input = [
                    ((), Timestamp::new(3), 1),
                    ((), Timestamp::new(2), 1),
                    ((), Timestamp::new(1), 1),
                    ((), Timestamp::new(2), 1),
                    ((), Timestamp::new(3), 1),
                    ((), Timestamp::new(3), 1),
                ]
                .to_stream(scope)
                .as_collection();
                input.times_reduce("test").inner.capture()
            })
        });
        let expected = vec![
            ((), Timestamp::new(1), 1),
            ((), Timestamp::new(2), 2),
            ((), Timestamp::new(3), 3),
        ];
        let actual = output
            .extract()
            .into_iter()
            .flat_map(|x| x.1)
            .collect::<Vec<_>>();
        assert_eq!(actual, expected);
    }

    #[mz_ore::test]
    fn ct_sink_state() {
        #[track_caller]
        fn assert_noop(state: &mut super::SinkState<&'static str, Timestamp>) {
            if let Some(ret) = state.process() {
                panic!("should be nothing to write: {:?}", ret);
            }
        }

        #[track_caller]
        fn assert_write(
            state: &mut super::SinkState<&'static str, Timestamp>,
            expected_upper: u64,
            expected_append: &[&str],
        ) {
            let (new_upper, to_append) = state.process().expect("should be something to write");
            assert_eq!(
                new_upper,
                Antichain::from_elem(Timestamp::new(expected_upper))
            );
            let to_append = to_append
                .into_iter()
                .map(|((k, ()), _ts, _diff)| *k)
                .collect::<Vec<_>>();
            assert_eq!(to_append, expected_append);
        }

        let mut s = super::SinkState::new();

        // Nothing to do at the initial state.
        assert_noop(&mut s);

        // Getting data to append is not enough to do anything yet.
        s.to_append.push((("a", 1.into()), 1));
        s.to_append.push((("b", 1.into()), 1));
        assert_noop(&mut s);

        // Knowing that this is the only data we'll get for that timestamp is
        // still not enough.
        s.to_append_progress = Antichain::from_elem(2.into());
        assert_noop(&mut s);

        // Even knowing that we got input at that time is not quite enough yet
        // (we could be getting these out of order).
        s.append_times.insert(1.into());
        assert_noop(&mut s);

        // Indeed, it did come out of order. Also note that this checks the ==
        // case for time vs progress.
        s.append_times.insert(0.into());
        assert_noop(&mut s);

        // Okay, now we know that we've seen all the times we got input up to 3.
        // This is enough to allow the empty write of `[0,1)`.
        s.append_times_progress = Antichain::from_elem(3.into());
        assert_write(&mut s, 1, &[]);

        // That succeeded, now we can write the data at 1.
        s.output_progress = Antichain::from_elem(1.into());
        assert_write(&mut s, 2, &["a", "b"]);

        // That succeeded, now we know about some empty time.
        s.output_progress = Antichain::from_elem(2.into());
        assert_write(&mut s, 3, &[]);

        // That succeeded, now nothing to do.
        s.output_progress = Antichain::from_elem(3.into());
        assert_noop(&mut s);

        // Find out about a new time to write at. Even without the data, we can
        // do an empty write up to that time.
        s.append_times.insert(5.into());
        s.append_times_progress = Antichain::from_elem(6.into());
        assert_write(&mut s, 5, &[]);

        // That succeeded, now nothing to do again.
        s.output_progress = Antichain::from_elem(5.into());

        // Retract one of the things currently in the collection and add a new
        // thing, to verify the consolidate.
        s.to_append.push((("a", 5.into()), -1));
        s.to_append.push((("c", 5.into()), 1));
        s.to_append_progress = Antichain::from_elem(6.into());
        assert_write(&mut s, 6, &["b", "c"]);
    }
}

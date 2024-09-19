// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! A continual task presents as something like a `BEFORE TRIGGER`: it watches
//! some _input_ and whenever it changes at time `T`, executes a SQL txn,
//! writing to some _output_ at the same time `T`. It can also read anything in
//! materialize as a _reference_, most notably including the output.
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
//!   time `T-1` and commits all writes at `T`
//!   - Intuition is that the logic runs before the input is written, like a
//!     `CREATE TRIGGER ... BEFORE`.
//! - This logic can _reference_ any nameable object in the system, not just the
//!   inputs.
//!   - However, the logic/transaction can mutate only the outputs.
//! - Summary of differences between inputs and references:
//!   - The task receives snapshot + changes for references (like regular
//!     dataflow inputs today) but only changes for inputs.
//!   - The task only produces output in response to changes in the inputs but
//!     not in response to changes in the references.
//!   - Inputs are reclocked by subtracting 1 from their timestamps, references
//!     are not.
//! - Instead of re-evaluating the task logic from scratch for each input time,
//!   we maintain the collection representing desired writes to the output(s) as
//!   a dataflow.
//! - The task dataflow is tied to a `CLUSTER` and runs on each `REPLICA`.
//!   - HA strategy: multi-replica clusters race to commit and the losers throw
//!     away the result.

use std::any::Any;
use std::cell::RefCell;
use std::collections::BTreeSet;
use std::rc::Rc;
use std::sync::Arc;

use differential_dataflow::consolidation::consolidate_updates;
use differential_dataflow::difference::Semigroup;
use differential_dataflow::lattice::Lattice;
use differential_dataflow::{AsCollection, Collection, Hashable};
use futures::{Future, FutureExt, StreamExt};
use mz_compute_types::dataflows::DataflowDescription;
use mz_compute_types::sinks::{ComputeSinkConnection, ComputeSinkDesc, ContinualTaskConnection};
use mz_ore::cast::CastFrom;
use mz_ore::collections::HashMap;
use mz_persist_client::error::UpperMismatch;
use mz_persist_client::write::WriteHandle;
use mz_persist_client::Diagnostics;
use mz_persist_types::codec_impls::UnitSchema;
use mz_repr::{Diff, GlobalId, Row, Timestamp};
use mz_storage_types::controller::CollectionMetadata;
use mz_storage_types::errors::DataflowError;
use mz_storage_types::sources::SourceData;
use mz_timely_util::builder_async::{Button, Event, OperatorBuilder as AsyncOperatorBuilder};
use timely::dataflow::channels::pact::{Exchange, Pipeline};
use timely::dataflow::operators::{Filter, FrontierNotificator, Map, Operator};
use timely::dataflow::{ProbeHandle, Scope};
use timely::progress::frontier::AntichainRef;
use timely::progress::{Antichain, Timestamp as _};
use timely::{Data, PartialOrder};
use tracing::debug;

use crate::compute_state::ComputeState;
use crate::render::sinks::SinkRender;
use crate::render::StartSignal;

pub(crate) struct ContinualTaskCtx<G: Scope<Timestamp = Timestamp>> {
    name: Option<String>,
    dataflow_as_of: Option<Antichain<Timestamp>>,
    ct_inputs: BTreeSet<GlobalId>,
    pub ct_times: Vec<Collection<G, (), Diff>>,
}

impl<G: Scope<Timestamp = Timestamp>> ContinualTaskCtx<G> {
    pub fn new<P, S>(dataflow: &DataflowDescription<P, S, Timestamp>) -> Self {
        let mut name = None;
        let mut ct_inputs = BTreeSet::new();
        for (sink_id, sink) in &dataflow.sink_exports {
            match &sink.connection {
                ComputeSinkConnection::ContinualTask(ContinualTaskConnection {
                    input_id, ..
                }) => {
                    ct_inputs.insert(*input_id);
                    // There's only one CT sink per dataflow at this point.
                    assert_eq!(name, None);
                    name = Some(sink_id.to_string());
                }
                _ => continue,
            }
        }
        let mut ret = ContinualTaskCtx {
            name,
            dataflow_as_of: None,
            ct_inputs,
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
        !self.ct_inputs.is_empty()
    }

    pub fn get_ct_inserts_transformer<S: Scope<Timestamp = Timestamp>>(
        &self,
        source_id: GlobalId,
    ) -> Option<
        impl FnOnce(
            Collection<S, Row, Diff>,
            Collection<S, DataflowError, Diff>,
        ) -> (
            Collection<S, Row, Diff>,
            Collection<S, DataflowError, Diff>,
            Collection<S, (), Diff>,
        ),
    > {
        if !self.ct_inputs.contains(&source_id) {
            return None;
        }

        // Make a collection s.t, for each time T in the input, the output
        // contains the inserts at T.
        let inserts_source_fn =
            move |oks: Collection<S, Row, Diff>, errs: Collection<S, DataflowError, Diff>| {
                let name = source_id.to_string();
                // Keep only the inserts.
                //
                // TODO(ct): At some point this will become a user option to instead
                // keep only deletes.
                let oks = oks.inner.filter(|(_, _, diff)| *diff > 0);
                // Grab the original times for use in the sink operator.
                let times = oks.map(|(_row, ts, diff)| ((), ts, diff));
                // SUBTLE: See the big module rustdoc for what's going on here.
                let oks = step_backward(&name, oks.as_collection());
                let errs = step_backward(&name, errs);
                // Then retract everything at the next timestamp.
                let oks = oks.inner.flat_map(|(row, ts, diff)| {
                    let mut negation = diff.clone();
                    differential_dataflow::Diff::negate(&mut negation);
                    [(row.clone(), ts.step_forward(), negation), (row, ts, diff)]
                });
                (oks.as_collection(), errs, times.as_collection())
            };
        Some(inserts_source_fn)
    }

    pub fn input_times(&self) -> Option<Collection<G, (), Diff>> {
        let (Some(name), Some(first)) = (self.name.as_ref(), self.ct_times.first()) else {
            return None;
        };
        let ct_times = differential_dataflow::collection::concatenate(
            &mut first.scope(),
            self.ct_times.iter().cloned(),
        );
        // Reduce this down to one update per-time-per-worker before exchanging
        // it, so we don't waste work on unnecessarily high data volumes.
        let ct_times = times_reduce(name, ct_times);
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

        // SUBTLE: See the big module rustdoc for what's going on here.
        let oks = step_forward(&name, oks);
        let errs = step_forward(&name, errs);

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

        // TODO(ct): Obey `compute_state.read_only_rx`
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

    // TODO(ct): This all works perfectly well data parallel (assuming we
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
            let new_upper = as_of.into_iter().map(|x| x.step_forward()).collect();
            let res = write_handle
                .compare_and_append_batch(
                    &mut [],
                    Antichain::from_elem(Timestamp::minimum()),
                    new_upper,
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
            fn apply(self, state: &mut SinkState<SourceData, (), Timestamp, Diff>) {
                debug!("ct_sink event {:?}", self);
                match self {
                    OpEvent::ToAppend(Event::Data(_cap, x)) => state
                        .to_append
                        .extend(x.into_iter().map(|(k, t, d)| ((k, ()), t, d))),
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
            if PartialOrder::less_than(&*output_frontier.borrow(), &state.output_progress) {
                output_frontier.borrow_mut().clear();
                output_frontier
                    .borrow_mut()
                    .extend(state.output_progress.iter().cloned());
            }
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
            debug!("ct_sink about to process {:?}", state);
            let Some((new_upper, to_append)) = state.process() else {
                continue;
            };
            debug!("ct_sink got write {:?}: {:?}", new_upper, to_append);

            let mut expected_upper = write_handle.shared_upper();
            while PartialOrder::less_than(&expected_upper, &new_upper) {
                let res = write_handle
                    .compare_and_append(&to_append, expected_upper.clone(), new_upper.clone())
                    .await
                    .expect("usage was valid");
                match res {
                    Ok(()) => {
                        state.output_progress = new_upper;
                        break;
                    }
                    Err(err) => {
                        expected_upper = err.current;
                        continue;
                    }
                }
            }
        }
    });

    button
}

#[derive(Debug)]
struct SinkState<K, V, T, D> {
    /// The known times at which we're going to write data to the output. This
    /// is guaranteed to include all times < append_times_progress, except that
    /// ones < output_progress may have been truncated.
    append_times: BTreeSet<T>,
    append_times_progress: Antichain<T>,

    /// The data we've collected to append to the output. This is often
    /// compacted to advancing times and is expected to be ~empty in the steady
    /// state.
    to_append: Vec<((K, V), T, D)>,
    to_append_progress: Antichain<T>,

    /// A lower bound on the upper of the output.
    output_progress: Antichain<T>,
}

impl<K: Ord, V: Ord, D: Semigroup> SinkState<K, V, Timestamp, D> {
    fn new() -> Self {
        SinkState {
            append_times: BTreeSet::new(),
            append_times_progress: Antichain::from_elem(Timestamp::minimum()),
            to_append: Vec::new(),
            to_append_progress: Antichain::from_elem(Timestamp::minimum()),
            output_progress: Antichain::from_elem(Timestamp::minimum()),
        }
    }

    /// Returns data to write to the output, if any, and the new upper to use.
    ///
    /// TODO(ct): Remove the Vec allocation here.
    fn process(&mut self) -> Option<(Antichain<Timestamp>, Vec<((&K, &V), &Timestamp, &D)>)> {
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
        for (_, t, _) in &mut self.to_append {
            t.advance_by(AntichainRef::new(as_of))
        }
        // TODO(ct): Metrics for vec len and cap.
        consolidate_updates(&mut self.to_append);
        // TODO(ct): Resize the vec down if cap >> len? Or perhaps `Correction`
        // might already be very close to what we need.

        let append_data = self
            .to_append
            .iter()
            .filter_map(|((k, v), t, d)| (t <= write_ts).then_some(((k, v), t, d)))
            .collect();
        Some((Antichain::from_elem(write_ts.step_forward()), append_data))
    }
}

// TODO(ct): Write this as a non-async operator.
//
// Unfortunately, we can _almost_ use the stock `delay` operator, but not quite.
// This must advance both data and the output frontier forward, while delay only
// advances the data.
fn step_backward<G, D, R>(name: &str, input: Collection<G, D, R>) -> Collection<G, D, R>
where
    G: Scope<Timestamp = Timestamp>,
    D: Data,
    R: Semigroup + 'static,
{
    let name = format!("ct_step_backward({})", name);
    let mut builder = AsyncOperatorBuilder::new(name, input.scope());
    let (mut output, output_stream) = builder.new_output();
    let mut input = builder.new_input_for(&input.inner, Pipeline, &output);
    builder.build(move |caps| async move {
        let [mut cap]: [_; 1] = caps.try_into().expect("one capability per output");
        loop {
            let Some(event) = input.next().await else {
                return;
            };
            match event {
                Event::Data(_data_cap, mut data) => {
                    for (_, ts, _) in &mut data {
                        *ts = ts
                            .step_back()
                            .expect("should only receive data at times past the as_of");
                    }
                    output.give_container(&cap, &mut data);
                }
                Event::Progress(new_progress) => {
                    let new_progress = new_progress.into_option().and_then(|x| x.step_back());
                    let Some(new_progress) = new_progress else {
                        continue;
                    };
                    if cap.time() < &new_progress {
                        cap.downgrade(&new_progress);
                    }
                }
            }
        }
    });

    output_stream.as_collection()
}

// TODO(ct): Write this as a non-async operator.
fn step_forward<G, D, R>(name: &str, input: Collection<G, D, R>) -> Collection<G, D, R>
where
    G: Scope<Timestamp = Timestamp>,
    D: Data,
    R: Semigroup + 'static,
{
    let name = format!("ct_step_forward({})", name);
    let mut builder = AsyncOperatorBuilder::new(name.clone(), input.scope());
    let mut input = builder.new_disconnected_input(&input.inner, Pipeline);
    let (mut output, output_stream) = builder.new_output();
    builder.build(move |caps| async move {
        let [mut cap]: [_; 1] = caps.try_into().expect("one capability per output");
        loop {
            let Some(event) = input.next().await else {
                return;
            };
            match event {
                Event::Data(_data_cap, mut data) => {
                    for (_, ts, _) in &mut data {
                        *ts = ts.step_forward();
                    }
                    output.give_container(&cap, &mut data);
                }
                Event::Progress(progress) => {
                    if let Some(progress) = progress.into_option() {
                        cap.downgrade(&progress.step_forward());
                    }
                }
            }
        }
    });

    output_stream.as_collection()
}

// This is essentially a specialized impl of consolidate, with a HashMap instead
// of the Trace.
fn times_reduce<G, R>(name: &str, input: Collection<G, (), R>) -> Collection<G, (), R>
where
    G: Scope<Timestamp = Timestamp>,
    R: Semigroup + 'static + std::fmt::Debug,
{
    let name = format!("ct_times_reduce({})", name);
    input
        .inner
        .unary_frontier(Pipeline, &name, |_caps, _info| {
            let mut notificator = FrontierNotificator::new();
            let mut stash = HashMap::<_, R>::new();
            let mut buf = Vec::new();
            move |input, output| {
                while let Some((cap, data)) = input.next() {
                    data.swap(&mut buf);
                    for ((), ts, diff) in buf.drain(..) {
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

#[cfg(test)]
mod tests {
    use differential_dataflow::AsCollection;
    use mz_repr::Timestamp;
    use timely::dataflow::operators::capture::Extract;
    use timely::dataflow::operators::{Capture, Input, ToStream};
    use timely::dataflow::ProbeHandle;
    use timely::progress::Antichain;
    use timely::Config;

    #[mz_ore::test]
    fn step_forward() {
        timely::execute(Config::thread(), |worker| {
            let (mut input, probe, output) = worker.dataflow(|scope| {
                let (handle, input) = scope.new_input();
                let mut probe = ProbeHandle::<Timestamp>::new();
                let output = super::step_forward("test", input.as_collection())
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
    fn step_backward() {
        timely::execute(Config::thread(), |worker| {
            let (mut input, probe, output) = worker.dataflow(|scope| {
                let (handle, input) = scope.new_input();
                let mut probe = ProbeHandle::<Timestamp>::new();
                let output = super::step_backward("test", input.as_collection())
                    .probe_with(&mut probe)
                    .inner
                    .capture();
                (handle, probe, output)
            });

            let mut expected = Vec::new();
            for i in 0u64..10 {
                // Notice that these are declared backward: out_ts < in_ts
                let out_ts = Timestamp::new(i);
                let in_ts = out_ts.step_forward();
                input.send((i, in_ts, 1));
                input.advance_to(in_ts.step_forward());

                // We should get the data out regressed by `step_backward`.
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
                super::times_reduce("test", input).inner.capture()
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
        fn assert_noop(state: &mut super::SinkState<&'static str, (), Timestamp, i64>) {
            if let Some(ret) = state.process() {
                panic!("should be nothing to write: {:?}", ret);
            }
        }

        #[track_caller]
        fn assert_write(
            state: &mut super::SinkState<&'static str, (), Timestamp, i64>,
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
        s.to_append.push((("a", ()), 1.into(), 1));
        s.to_append.push((("b", ()), 1.into(), 1));
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
        s.to_append.push((("a", ()), 5.into(), -1));
        s.to_append.push((("c", ()), 5.into(), 1));
        s.to_append_progress = Antichain::from_elem(6.into());
        assert_write(&mut s, 6, &["b", "c"]);
    }
}

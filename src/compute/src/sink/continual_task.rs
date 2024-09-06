// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

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
use mz_expr::MfpPlan;
use mz_ore::cast::CastFrom;
use mz_persist_client::operators::shard_source::SnapshotMode;
use mz_persist_client::write::WriteHandle;
use mz_persist_client::Diagnostics;
use mz_persist_types::codec_impls::UnitSchema;
use mz_repr::{Diff, GlobalId, Row, Timestamp};
use mz_storage_operators::persist_source;
use mz_storage_types::controller::CollectionMetadata;
use mz_storage_types::errors::DataflowError;
use mz_storage_types::sources::SourceData;
use mz_timely_util::builder_async::{
    Button, Event, OperatorBuilder as AsyncOperatorBuilder, PressOnDropButton,
};
use timely::dataflow::channels::pact::{Exchange, Pipeline};
use timely::dataflow::operators::{Filter, Map};
use timely::dataflow::{Scope, Stream};
use timely::progress::frontier::AntichainRef;
use timely::progress::{Antichain, Timestamp as _};

use crate::compute_state::ComputeState;
use crate::render::sinks::SinkRender;
use crate::render::StartSignal;

pub(crate) struct ContinualTaskCtx<G: Scope<Timestamp = Timestamp>> {
    dataflow_as_of: Option<Antichain<Timestamp>>,
    ct_inputs: BTreeSet<GlobalId>,
    pub ct_times: Vec<Collection<G, (), Diff>>,
}

impl<G: Scope<Timestamp = Timestamp>> ContinualTaskCtx<G> {
    pub fn new<P, S>(dataflow: &DataflowDescription<P, S, Timestamp>) -> Self {
        let mut ct_inputs = BTreeSet::new();
        for sink in dataflow.sink_exports.values() {
            match &sink.connection {
                ComputeSinkConnection::ContinualTask(ContinualTaskConnection {
                    input_id, ..
                }) => {
                    ct_inputs.insert(*input_id);
                }
                _ => continue,
            }
        }
        let mut ret = ContinualTaskCtx {
            dataflow_as_of: None,
            ct_inputs,
            ct_times: Vec::new(),
        };
        // Only clone the as_of if we're in a CT dataflow.
        if !ret.ct_inputs.is_empty() {
            ret.dataflow_as_of = dataflow.as_of.clone();
        }
        ret
    }

    pub fn render_source<S: Scope<Timestamp = Timestamp>>(
        &mut self,
        scope: &mut S,
        source_id: GlobalId,
        source_meta: &CollectionMetadata,
        compute_state: &ComputeState,
        map_filter_project: Option<&mut MfpPlan>,
        start_signal: &StartSignal,
    ) -> Option<(
        Collection<S, (), Diff>,
        Stream<S, (Row, Timestamp, Diff)>,
        Stream<S, (DataflowError, Timestamp, Diff)>,
        Vec<PressOnDropButton>,
    )> {
        if !self.ct_inputs.contains(&source_id) {
            return None;
        }

        let (ok_stream, err_stream, mut tokens) = persist_source::persist_source(
            scope,
            source_id,
            Arc::clone(&compute_state.persist_clients),
            &compute_state.txns_ctx,
            &compute_state.worker_config,
            source_meta.clone(),
            self.dataflow_as_of.clone(),
            SnapshotMode::Exclude,
            Antichain::new(),
            map_filter_project,
            compute_state.dataflow_max_inflight_bytes(),
            start_signal.clone(),
            |error| panic!("compute_import: {error}"),
        );
        let ok_stream = ok_stream.filter(|(data, ts, diff)| {
            if *diff < 0 {
                tracing::info!("WIP dropping {:?}", (data, ts, diff));
                false
            } else {
                true
            }
        });
        // WIP reduce this down to one update per timestamp before exchanging.
        let ct_times = ok_stream.map(|(_row, ts, _diff)| ((), ts, 1));
        let (token, ok_stream) = crate::sink::continual_task::step_backward(
            &source_id.to_string(),
            Collection::new(ok_stream),
        );
        tokens.push(token);
        Some((
            ct_times.as_collection(),
            ok_stream.inner,
            err_stream,
            tokens,
        ))
    }

    pub fn input_times(&self) -> Option<Collection<G, (), Diff>> {
        let Some(first) = self.ct_times.first() else {
            return None;
        };
        let mut scope = first.scope();
        let diff_times =
            differential_dataflow::collection::concatenate(&mut scope, self.ct_times.to_vec());
        Some(diff_times)
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
        _as_of: Antichain<Timestamp>,
        start_signal: StartSignal,
        oks: Collection<G, Row, Diff>,
        errs: Collection<G, DataflowError, Diff>,
        append_times: Option<Collection<G, (), Diff>>,
    ) -> Option<Rc<dyn Any>> {
        let to_append = oks
            .map(|x| SourceData(Ok(x)))
            .concat(&errs.map(|x| SourceData(Err(x))));
        let append_times = append_times.expect("should be provided by ContinualTaskCtx");

        // WIP do something with this
        let active_worker = true;
        let shared_frontier = Rc::new(RefCell::new(if active_worker {
            Antichain::from_elem(Timestamp::minimum())
        } else {
            Antichain::new()
        }));
        let collection = compute_state.expect_collection_mut(sink_id);
        collection.sink_write_frontier = Some(Rc::clone(&shared_frontier));

        let write_handle = {
            let clients = Arc::clone(&compute_state.persist_clients);
            let metadata = self.output_metadata.clone();
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
                            handle_purpose: "WIP".into(),
                        },
                    )
                    .await
                    .expect("codecs should match")
            }
        };

        // TODO(ct): Obey `compute_state.read_only_rx`
        let button = continual_task_sink(
            &sink_id.to_string(),
            to_append,
            append_times,
            write_handle,
            start_signal,
        );
        Some(Rc::new(button.press_on_drop()))
    }
}

fn continual_task_sink<G: Scope<Timestamp = Timestamp>>(
    name: &str,
    to_append: Collection<G, SourceData, Diff>,
    append_times: Collection<G, (), Diff>,
    write_handle: impl Future<Output = WriteHandle<SourceData, (), Timestamp, Diff>> + Send + 'static,
    start_signal: StartSignal,
) -> Button {
    let scope = to_append.scope();
    let operator_name = format!("ContinualTask({})", name);
    let mut op = AsyncOperatorBuilder::new(operator_name, scope.clone());

    // TODO(ct): This all works perfectly well data parallel (assuming we
    // broadcast the append_times). We just need to hook it up to the
    // multi-worker persist-sink, but that requires some refactoring. This would
    // also remove the need for this to be an async timely operator.
    let active_worker = name.hashed();
    let to_append_input =
        op.new_disconnected_input(&to_append.inner, Exchange::new(move |_| active_worker));
    let append_times_input =
        op.new_disconnected_input(&append_times.inner, Exchange::new(move |_| active_worker));

    let active_worker = usize::cast_from(active_worker) % scope.peers() == scope.index();
    let button = op.build(move |_capabilities| async move {
        if !active_worker {
            return;
        }
        let () = start_signal.await;
        let mut write_handle = write_handle.await;

        #[derive(Debug)]
        enum OpEvent<C> {
            ToAppend(Event<Timestamp, C, Vec<(SourceData, Timestamp, Diff)>>),
            AppendTimes(Event<Timestamp, C, Vec<((), Timestamp, Diff)>>),
        }

        impl<C: std::fmt::Debug> OpEvent<C> {
            fn apply(self, state: &mut SinkState<SourceData, (), Timestamp, Diff>) {
                tracing::info!("WIP event {:?}", self);
                match self {
                    // WIP big comment on the step_forward calls
                    OpEvent::ToAppend(Event::Data(_cap, x)) => state.to_append.extend(
                        x.into_iter()
                            .map(|(k, t, d)| ((k, ()), t.step_forward(), d)),
                    ),
                    OpEvent::ToAppend(Event::Progress(x)) => {
                        state.to_append_progress = x.into_option().expect("WIP").step_forward()
                    }
                    OpEvent::AppendTimes(Event::Data(_cap, x)) => state
                        .append_times
                        .extend(x.into_iter().map(|((), t, _d)| t)),
                    OpEvent::AppendTimes(Event::Progress(x)) => {
                        state.append_times_progress = x.into_option().expect("WIP")
                    }
                }
            }
        }

        let to_insert_input = to_append_input.map(OpEvent::ToAppend);
        let append_times_input = append_times_input.map(OpEvent::AppendTimes);
        let mut op_inputs = futures::stream::select(to_insert_input, append_times_input);

        let mut state = SinkState::new();
        loop {
            let Some(event) = op_inputs.next().await else {
                // Inputs exhausted, shutting down.
                return;
            };
            event.apply(&mut state);
            // Also drain any other events that may be ready.
            while let Some(Some(event)) = op_inputs.next().now_or_never() {
                event.apply(&mut state);
            }
            tracing::info!("WIP about to process {:?}", state);
            let Some((new_upper, to_append)) = state.process() else {
                continue;
            };
            tracing::info!("WIP got write {:?}: {:?}", new_upper, to_append);

            let mut expected_upper = write_handle.shared_upper();
            while expected_upper.less_than(&new_upper) {
                let res = write_handle
                    .compare_and_append(
                        &to_append,
                        expected_upper.clone(),
                        Antichain::from_elem(new_upper),
                    )
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
    append_times: BTreeSet<T>,
    append_times_progress: T,
    to_append: Vec<((K, V), T, D)>,
    to_append_progress: T,
    output_progress: T,
}

impl<K: Ord, V: Ord, D: Semigroup> SinkState<K, V, Timestamp, D> {
    fn new() -> Self {
        SinkState {
            // The known times at which we're going to write data to the output.
            // This is guaranteed to include all times < append_times_progress,
            // except that ones < output_progress may have been truncated.
            append_times: BTreeSet::new(),
            append_times_progress: Timestamp::minimum(),

            // The data we've collected to append to the output. This is often
            // compacted to advancing times and is expected to be ~empty in the
            // steady state.
            to_append: Vec::new(),
            to_append_progress: Timestamp::minimum(),

            // A lower bound on the upper of the output.
            output_progress: Timestamp::minimum(),
        }
    }

    /// Returns data to write to the output, if any, and the new upper to use.
    ///
    /// TODO(ct): Remove the Vec allocation here.
    fn process(&mut self) -> Option<(Timestamp, Vec<((&K, &V), &Timestamp, &D)>)> {
        // We can only append at times >= the output_progress, so pop off
        // anything unnecessary.
        while let Some(x) = self.append_times.first() {
            // WIP regression test for >= vs >
            if *x >= self.output_progress {
                break;
            }
            self.append_times.pop_first();
        }

        // WIP bug: not allowed to return anything from append_times until we
        // have progress info, because they could arrive out of order
        let write_ts = match self.append_times.first() {
            Some(x) if self.output_progress < *x => {
                // WIP regression test
                return Some((x.clone(), Vec::new()));
            }
            Some(x) => x,
            // The CT sink's contract is that it only writes data at times we
            // received an input diff. There are none in `[output_progress,
            // append_times_progress)`, so we can go ahead and advance the upper
            // of the output.
            //
            // We could instead ensure liveness by basing this off of to_append,
            // but for any CTs reading the output (expected to be a common case)
            // we'd end up looping each timestamp through persist one-by-one.
            None if self.output_progress < self.append_times_progress => {
                return Some((self.append_times_progress, Vec::new()));
            }
            // Nothing to do!
            None => return None,
        };

        if self.to_append_progress <= *write_ts {
            // Don't have all the necessary data yet.
            return None;
        }

        // Time to write some data! Produce the collection as of write_ts by
        // advancing timestamps, consolidating, and filtering out anything at
        // future timestamps.
        let as_of = &[write_ts.clone()];
        for (_, t, _) in &mut self.to_append {
            t.advance_by(AntichainRef::new(as_of))
        }
        consolidate_updates(&mut self.to_append);
        // WIP resize the vec down if cap >> len?

        let append_data = self
            .to_append
            .iter()
            .filter_map(|((k, v), t, d)| (t <= write_ts).then_some(((k, v), t, d)))
            .collect();
        Some((write_ts.step_forward(), append_data))
    }
}

pub(crate) fn step_backward<G>(
    name: &str,
    input: Collection<G, Row, Diff>,
) -> (PressOnDropButton, Collection<G, Row, Diff>)
where
    G: Scope<Timestamp = Timestamp>,
{
    let name_orig = name.to_string();
    let name = format!("StepBackward({})", name);
    let mut builder = AsyncOperatorBuilder::new(name.clone(), input.scope());
    let mut input = builder.new_disconnected_input(&input.inner, Pipeline);
    let (mut output, output_stream) = builder.new_output();
    let button = builder.build(move |caps| async move {
        let [mut cap]: [_; 1] = caps.try_into().expect("one capability per output");
        loop {
            let Some(event) = input.next().await else {
                tracing::info!("WIP StepBackward exhausted, shutting down");
                return;
            };
            match event {
                Event::Data(_data_cap, mut data) => {
                    // tracing::info!("{} input data {:?}", name, data);
                    let mut retractions = Vec::new();
                    for (row, ts, diff) in &mut data {
                        let orig_ts = *ts;
                        *ts = ts.step_back().expect("WIP");
                        let mut negated = *diff;
                        differential_dataflow::Diff::negate(&mut negated);
                        tracing::info!(
                            "WIP {} read ct input\n    {:?} read  {} {:?} {}\n    {:?} diff  {} {:?} {}\n    {:?} diff  {} {:?} {}",
                            name_orig,
                            orig_ts,
                            name_orig,
                            row,
                            diff,
                            ts,
                            name_orig,
                            row,
                            diff,
                            orig_ts,
                            name_orig,
                            row,
                            negated,
                        );
                        retractions.push((row.clone(), orig_ts, negated));
                    }
                    tracing::debug!("{} emitting data {:?}", name, data);
                    output.give_container(&cap, &mut data);
                    output.give_container(&cap, &mut retractions);
                }
                Event::Progress(new_progress) => {
                    let Some(new_progress) = new_progress.into_option() else {
                        return;
                    };
                    let new_progress = new_progress.step_back().expect("WIP");
                    if cap.time() < &new_progress {
                        tracing::debug!("{} downgrading cap to {:?}", name, new_progress);
                        cap.downgrade(&new_progress);
                    }
                }
            }
        }
    });

    (button.press_on_drop(), Collection::new(output_stream))
}

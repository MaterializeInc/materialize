// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Logic related to the creation of dataflow sinks.

use std::any::Any;
use std::collections::{BTreeMap, BTreeSet};
use std::rc::Rc;

use differential_dataflow::VecCollection;
use differential_dataflow::dynamic::pointstamp::PointStamp;
use differential_dataflow::trace::TraceReader;
use mz_compute_types::plan::scalar::mfp_plan_lir_to_mir;
use mz_compute_types::sinks::{ComputeSinkConnection, ComputeSinkDesc};
use mz_expr::{EvalError, SafeMfpPlan};
use mz_ore::soft_assert_or_log;
use mz_ore::str::StrExt;
use mz_ore::vec::PartialOrdVecExt;
use mz_repr::{Diff, GlobalId, Row};
use mz_storage_types::controller::CollectionMetadata;
use mz_timely_util::operator::{CollectionExt, StreamExt};
use mz_timely_util::probe::Handle;
use timely::container::CapacityContainerBuilder;
use timely::dataflow::operators::{Enter, Leave};
use timely::dataflow::{Scope, Stream};
use timely::order::Product;
use timely::progress::Antichain;

use crate::compute_state::SinkToken;
use crate::logging::compute::LogDataflowErrors;
use crate::render::columnar::columnar_to_vec;
use crate::render::context::{
    ArrangementFlavor, CollectionBundle, Context, distinct_errs_collection,
};
use crate::render::errors::DataflowErrorSer;
use crate::render::{RenderTimestamp, StartSignal};
use crate::typedefs::{ErrEnter, RowRowEnter};

impl<'g, T: RenderTimestamp> Context<'g, T> {
    /// Export the sink described by `sink` from the rendering context.
    pub(crate) fn export_sink(
        &self,
        compute_state: &mut crate::compute_state::ComputeState,
        tokens: &BTreeMap<GlobalId, Rc<dyn Any>>,
        dependency_ids: BTreeSet<GlobalId>,
        sink_id: GlobalId,
        sink: &ComputeSinkDesc<CollectionMetadata>,
        start_signal: StartSignal,
        output_probe: &Handle<mz_repr::Timestamp>,
        outer_scope: Scope<'g, mz_repr::Timestamp>,
    ) where
        T: SinkArrangementTimestamp,
    {
        soft_assert_or_log!(
            sink.non_null_assertions.is_strictly_sorted(),
            "non-null assertions not sorted"
        );

        // put together tokens that belong to the export
        let mut needed_tokens = Vec::new();
        for dep_id in dependency_ids {
            if let Some(token) = tokens.get(&dep_id) {
                needed_tokens.push(Rc::clone(token))
            }
        }

        let bundle = self
            .lookup_id(mz_expr::Id::Global(sink.from))
            .expect("Sink source collection not loaded");
        let input = match T::batched_subscribe_arrangement(&bundle, sink) {
            Some(ArrangedInput {
                oks,
                mut errs,
                from_mfp,
            }) => {
                // Error logging and expiration as `flatten_sink_input` attaches them.
                if let Some(logger) = compute_state.compute_logger.clone() {
                    errs = errs.log_dataflow_errors(logger, sink_id);
                }
                let mut oks = oks.leave(outer_scope);
                let mut errs = errs.leave(outer_scope);
                if let Some(&expiration) = self.dataflow_expiration.as_option() {
                    oks = oks.expire_stream_at(
                        &format!("{}_export_sink_oks", self.debug_name),
                        expiration,
                    );
                    errs = errs.expire_stream_at(
                        &format!("{}_export_sink_errs", self.debug_name),
                        expiration,
                    );
                }
                SinkInput::Arranged {
                    oks,
                    errs,
                    from_mfp,
                }
            }
            None => {
                let (oks, errs) =
                    self.flatten_sink_input(&*compute_state, &bundle, sink_id, sink, outer_scope);
                SinkInput::Collection { oks, errs }
            }
        };

        let region_name = match sink.connection {
            ComputeSinkConnection::Subscribe(_) => format!("SubscribeSink({:?})", sink_id),
            ComputeSinkConnection::BatchedSubscribe(_) => {
                format!("BatchedSubscribeSink({:?})", sink_id)
            }
            ComputeSinkConnection::MaterializedView(_) => {
                format!("MaterializedViewSink({:?})", sink_id)
            }
            ComputeSinkConnection::CopyToS3Oneshot(_) => {
                format!("CopyToS3OneshotSink({:?})", sink_id)
            }
            ComputeSinkConnection::MetricSink(_) => format!("MetricSink({:?})", sink_id),
        };
        outer_scope.clone().region_named(&region_name, |inner| {
            let sink_render = get_sink_render_for(&sink.connection);

            let sink_token = sink_render.render_sink(
                compute_state,
                sink,
                sink_id,
                self.as_of_frontier.clone(),
                start_signal,
                input.enter_region(inner),
                output_probe,
            );

            if let Some(sink_token) = sink_token {
                needed_tokens.push(sink_token);
            }

            let collection = compute_state.expect_collection_mut(sink_id);
            collection.sink_token = Some(SinkToken::new(Box::new(needed_tokens)));
        });
    }
}

impl<'g, T: RenderTimestamp> Context<'g, T> {
    /// Flattens the `from` collection of `sink` into rows of its `from_desc` and leaves them to
    /// `outer_scope`.
    fn flatten_sink_input(
        &self,
        compute_state: &crate::compute_state::ComputeState,
        bundle: &CollectionBundle<'g, T>,
        sink_id: GlobalId,
        sink: &ComputeSinkDesc<CollectionMetadata>,
        outer_scope: Scope<'g, mz_repr::Timestamp>,
    ) -> (
        VecCollection<'g, mz_repr::Timestamp, Row, Diff>,
        VecCollection<'g, mz_repr::Timestamp, DataflowErrorSer, Diff>,
    ) {
        // Which arrangement of the `from` collection to consume, and the MFP that reconstructs
        // full rows from it, were decided during LIR lowering and recorded in
        // `sink.from_arrangement`. An unarranged form takes precedence when the bundle provides
        // one. Lowering promises one exactly when `from_arrangement` is `None`, but the renderer
        // can produce one where lowering promised only an arrangement: a snapshot-excluded
        // subscribe imports an index as a filtered collection instead (see `import_index`).
        let (ok_collection, mut err_collection) = match (&bundle.collection, &sink.from_arrangement)
        {
            (Some((oks, errs)), _) => (columnar_to_vec(oks.clone()), errs.clone()),
            (None, Some((key, mfp_plan))) => {
                // The sink serializes rows, so decode to `Vec` here. This is the
                // sanctioned sink leaf, the same seam as the raw-collection arm
                // above.
                let (oks, errs) = bundle.as_collection_core(
                    mfp_plan.clone(),
                    Some((key.clone(), None)),
                    self.until.clone(),
                );
                (columnar_to_vec(oks), errs)
            }
            (None, None) => panic!(
                "sink source {} has neither a collection nor a planned arrangement",
                sink.from
            ),
        };

        // Attach logging of dataflow errors.
        if let Some(logger) = compute_state.compute_logger.clone() {
            err_collection = err_collection.log_dataflow_errors(logger, sink_id);
        }

        let mut ok_collection = ok_collection.leave(outer_scope);
        let mut err_collection = err_collection.leave(outer_scope);

        // Ensure that the frontier does not advance past the expiration time, if set. Otherwise,
        // we might write down incorrect data.
        if let Some(&expiration) = self.dataflow_expiration.as_option() {
            ok_collection = ok_collection
                .expire_collection_at(&format!("{}_export_sink_oks", self.debug_name), expiration);
            err_collection = err_collection
                .expire_collection_at(&format!("{}_export_sink_errs", self.debug_name), expiration);
        }

        let non_null_assertions = sink.non_null_assertions.clone();
        let from_desc = sink.from_desc.clone();
        if !non_null_assertions.is_empty() {
            let name = format!("NullAssertions({sink_id:?})");
            type CB<C> = CapacityContainerBuilder<C>;
            let (oks, null_errs) =
                ok_collection.map_fallible::<CB<_>, CB<_>, _, _, _>(&name, move |row| {
                    let mut idx = 0;
                    let mut iter = row.iter();
                    for &i in &non_null_assertions {
                        let skip = i - idx;
                        let datum = iter.nth(skip).unwrap();
                        idx += skip + 1;
                        if datum.is_null() {
                            return Err(DataflowErrorSer::from(EvalError::MustNotBeNull(
                                format!("column {}", from_desc.get_name(i).quoted()).into(),
                            )));
                        }
                    }
                    Ok(row)
                });
            ok_collection = oks;
            err_collection = err_collection.concat(null_errs);
        }

        // Normalize what the sink persists. A materialized view's error multiplicity is durable
        // state another dataflow reads back verbatim, so leaving it at this dataflow's fan-out both
        // multiplies across the object graph and makes the written value depend on plan shape.
        // Placed after the null assertions so their errors, whose multiplicity follows the ok row's,
        // are covered too. Only the persist-backed sink is re-importable: a subscribe or a one-shot
        // copy is read once by a client, so neither pays for the arrangement.
        if matches!(sink.connection, ComputeSinkConnection::MaterializedView(_)) {
            err_collection = distinct_errs_collection(err_collection);
        }

        (ok_collection, err_collection)
    }
}

/// A type that can be rendered as a dataflow sink.
pub(crate) trait SinkRender<'scope> {
    fn render_sink(
        &self,
        compute_state: &mut crate::compute_state::ComputeState,
        sink: &ComputeSinkDesc<CollectionMetadata>,
        sink_id: GlobalId,
        as_of: Antichain<mz_repr::Timestamp>,
        start_signal: StartSignal,
        input: SinkInput<'scope>,
        output_probe: &Handle<mz_repr::Timestamp>,
    ) -> Option<Rc<dyn Any>>;
}

/// A batch of an imported index's ok arrangement, as a sink reads it.
pub(crate) type IndexOkBatch =
    <RowRowEnter<mz_repr::Timestamp, Diff, mz_repr::Timestamp> as TraceReader>::Batch;
/// A batch of an imported index's error arrangement, as a sink reads it.
pub(crate) type IndexErrBatch =
    <ErrEnter<mz_repr::Timestamp, mz_repr::Timestamp> as TraceReader>::Batch;

/// The input a sink renders from.
pub(crate) enum SinkInput<'scope> {
    /// The rows of the sink's `from_desc`.
    Collection {
        oks: VecCollection<'scope, mz_repr::Timestamp, Row, Diff>,
        errs: VecCollection<'scope, mz_repr::Timestamp, DataflowErrorSer, Diff>,
    },
    /// The batches of the imported index arrangement that `from_arrangement` names, including
    /// those the import replays for its snapshot. Only a batched subscribe receives this.
    ///
    /// The batches advance their times by the dataflow's as-of and omit times at or beyond its
    /// until.
    Arranged {
        oks: Stream<'scope, mz_repr::Timestamp, Vec<IndexOkBatch>>,
        errs: Stream<'scope, mz_repr::Timestamp, Vec<IndexErrBatch>>,
        /// Reconstructs rows of the sink's `from_desc` from the datums of a batch's key followed
        /// by those of its value.
        from_mfp: SafeMfpPlan,
    },
}

impl<'scope> SinkInput<'scope> {
    /// Returns the rows of a sink that only ever receives [`SinkInput::Collection`].
    pub(crate) fn expect_collection(
        self,
    ) -> (
        VecCollection<'scope, mz_repr::Timestamp, Row, Diff>,
        VecCollection<'scope, mz_repr::Timestamp, DataflowErrorSer, Diff>,
    ) {
        match self {
            SinkInput::Collection { oks, errs } => (oks, errs),
            SinkInput::Arranged { .. } => {
                panic!("only a batched subscribe sink receives an arrangement")
            }
        }
    }

    fn enter_region<'inner>(self, inner: Scope<'inner, mz_repr::Timestamp>) -> SinkInput<'inner> {
        match self {
            SinkInput::Collection { oks, errs } => SinkInput::Collection {
                oks: oks.enter_region(inner),
                errs: errs.enter_region(inner),
            },
            SinkInput::Arranged {
                oks,
                errs,
                from_mfp,
            } => SinkInput::Arranged {
                oks: oks.enter(inner),
                errs: errs.enter(inner),
                from_mfp,
            },
        }
    }
}

/// The [`SinkInput::Arranged`] input of a sink, in the scope the index was imported into.
pub(crate) struct ArrangedInput<'scope> {
    oks: Stream<'scope, mz_repr::Timestamp, Vec<IndexOkBatch>>,
    errs: Stream<'scope, mz_repr::Timestamp, Vec<IndexErrBatch>>,
    from_mfp: SafeMfpPlan,
}

/// Selects the input of a batched subscribe that reads an index arrangement directly.
///
/// A sibling to [`RenderTimestamp`], because only the system timestamp yields index batches
/// that a sink scans at system times.
pub(crate) trait SinkArrangementTimestamp: RenderTimestamp {
    /// Returns the arranged input of `sink`, or `None` if it renders from the flattened
    /// collection.
    fn batched_subscribe_arrangement<'scope>(
        bundle: &CollectionBundle<'scope, Self>,
        sink: &ComputeSinkDesc<CollectionMetadata>,
    ) -> Option<ArrangedInput<'scope>>;
}

impl SinkArrangementTimestamp for mz_repr::Timestamp {
    fn batched_subscribe_arrangement<'scope>(
        bundle: &CollectionBundle<'scope, Self>,
        sink: &ComputeSinkDesc<CollectionMetadata>,
    ) -> Option<ArrangedInput<'scope>> {
        // An unarranged form takes precedence, as it does when flattening. A dataflow-local
        // arrangement takes the general path, and so do null assertions, which apply to rows.
        let ComputeSinkConnection::BatchedSubscribe(_) = &sink.connection else {
            return None;
        };
        if bundle.collection.is_some() || !sink.non_null_assertions.is_empty() {
            return None;
        }
        let (key, from_mfp) = sink.from_arrangement.as_ref()?;
        let Some(ArrangementFlavor::Trace(_, oks, errs)) = bundle.arrangement(key) else {
            return None;
        };
        // The scan evaluates only non-temporal plans. Lowering builds `from_mfp` as a
        // permutation, so it is never temporal, and a temporal one takes the general path.
        let from_mfp = mfp_plan_lir_to_mir(from_mfp.clone())
            .into_nontemporal()
            .ok()?;
        Some(ArrangedInput {
            oks: oks.stream,
            errs: errs.stream,
            from_mfp,
        })
    }
}

impl SinkArrangementTimestamp for Product<mz_repr::Timestamp, PointStamp<u64>> {
    fn batched_subscribe_arrangement<'scope>(
        _bundle: &CollectionBundle<'scope, Self>,
        _sink: &ComputeSinkDesc<CollectionMetadata>,
    ) -> Option<ArrangedInput<'scope>> {
        None
    }
}

fn get_sink_render_for<'scope>(
    connection: &ComputeSinkConnection<CollectionMetadata>,
) -> Box<dyn SinkRender<'scope>> {
    match connection {
        ComputeSinkConnection::Subscribe(connection) => Box::new(connection.clone()),
        ComputeSinkConnection::BatchedSubscribe(connection) => Box::new(connection.clone()),
        ComputeSinkConnection::MaterializedView(connection) => Box::new(connection.clone()),
        ComputeSinkConnection::CopyToS3Oneshot(connection) => Box::new(connection.clone()),
        ComputeSinkConnection::MetricSink(connection) => Box::new(connection.clone()),
    }
}

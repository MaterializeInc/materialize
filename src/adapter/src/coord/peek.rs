// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Logic and types for creating, executing, and tracking peeks.
//!
//! This module determines if a dataflow can be short-cut, by returning constant values
//! or by reading out of existing arrangements, and implements the appropriate plan.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::num::NonZeroUsize;

use futures::TryFutureExt;
use mz_compute_client::controller::{ComputeInstanceId, ReplicaId};
use mz_compute_client::protocol::response::PeekResponse;
use mz_compute_client::types::dataflows::DataflowDescription;
use mz_controller::clusters::ClusterId;
use mz_expr::{
    EvalError, Id, MirRelationExpr, MirScalarExpr, OptimizedMirRelationExpr, RowSetFinishing,
};
use mz_ore::cast::CastFrom;
use mz_ore::str::{separated, Indent, StrExt};
use mz_ore::tracing::OpenTelemetryContext;
use mz_repr::explain::text::{fmt_text_constant_rows, DisplayText};
use mz_repr::explain::{CompactScalarSeq, ExprHumanizer, Indices};
use mz_repr::{Diff, GlobalId, RelationType, Row};
use serde::{Deserialize, Serialize};
use timely::progress::Timestamp;
use tokio::sync::oneshot;
use uuid::Uuid;

use crate::client::ConnectionId;
use crate::coord::id_bundle::CollectionIdBundle;
use crate::coord::timestamp_selection::TimestampContext;
use crate::util::{send_immediate_rows, ResultExt};
use crate::AdapterError;

#[derive(Debug)]
pub(crate) struct PendingPeek {
    pub(crate) sender: oneshot::Sender<PeekResponse>,
    pub(crate) conn_id: ConnectionId,
    pub(crate) cluster_id: ClusterId,
    /// All `GlobalId`s that the peek depend on.
    pub(crate) depends_on: BTreeSet<GlobalId>,
}

/// The response from a `Peek`, with row multiplicities represented in unary.
///
/// Note that each `Peek` expects to generate exactly one `PeekResponse`, i.e.
/// we expect a 1:1 contract between `Peek` and `PeekResponseUnary`.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub enum PeekResponseUnary {
    Rows(Vec<Row>),
    Error(String),
    Canceled,
}

#[derive(Debug)]
pub struct PeekDataflowPlan<T = mz_repr::Timestamp> {
    desc: DataflowDescription<mz_compute_client::plan::Plan<T>, (), T>,
    id: GlobalId,
    key: Vec<MirScalarExpr>,
    permutation: BTreeMap<usize, usize>,
    thinned_arity: usize,
}

#[derive(Debug, Serialize, Deserialize)]
pub enum FastPathPlan {
    /// The view evaluates to a constant result that can be returned.
    ///
    /// The [RelationType] is unnecessary for evaluating the constant result but
    /// may be helpful when printing out an explanation.
    Constant(Result<Vec<(Row, Diff)>, EvalError>, RelationType),
    /// The view can be read out of an existing arrangement.
    PeekExisting(GlobalId, Option<Vec<Row>>, mz_expr::SafeMfpPlan),
}

impl<'a, C> DisplayText<C> for FastPathPlan
where
    C: AsMut<Indent> + AsRef<&'a dyn ExprHumanizer>,
{
    fn fmt_text(&self, f: &mut fmt::Formatter<'_>, ctx: &mut C) -> fmt::Result {
        match self {
            FastPathPlan::Constant(Ok(rows), _) => {
                if !rows.is_empty() {
                    writeln!(f, "{}Constant", ctx.as_mut())?;
                    *ctx.as_mut() += 1;
                    fmt_text_constant_rows(
                        f,
                        rows.iter().map(|(row, diff)| (row, diff)),
                        ctx.as_mut(),
                    )?;
                    *ctx.as_mut() -= 1;
                } else {
                    writeln!(f, "{}Constant <empty>", ctx.as_mut())?;
                }
                Ok(())
            }
            FastPathPlan::Constant(Err(err), _) => {
                writeln!(f, "{}Error {}", ctx.as_mut(), err.to_string().quoted())
            }
            FastPathPlan::PeekExisting(id, literal_constraints, mfp) => {
                ctx.as_mut().set();
                let (map, filter, project) = mfp.as_map_filter_project();
                if project.len() != mfp.input_arity + map.len()
                    || !project.iter().enumerate().all(|(i, o)| i == *o)
                {
                    let outputs = Indices(&project);
                    writeln!(f, "{}Project ({})", ctx.as_mut(), outputs)?;
                    *ctx.as_mut() += 1;
                }
                if !filter.is_empty() {
                    let predicates = separated(" AND ", filter);
                    writeln!(f, "{}Filter {}", ctx.as_mut(), predicates)?;
                    *ctx.as_mut() += 1;
                }
                if !map.is_empty() {
                    let scalars = CompactScalarSeq(&map);
                    writeln!(f, "{}Map ({})", ctx.as_mut(), scalars)?;
                    *ctx.as_mut() += 1;
                }
                MirRelationExpr::fmt_indexed_filter(f, ctx, id, literal_constraints.clone())?;
                writeln!(f)?;
                ctx.as_mut().reset();
                Ok(())
            }
        }?;
        Ok(())
    }
}

#[derive(Debug)]
pub struct PlannedPeek {
    pub plan: PeekPlan,
    pub timestamp_context: TimestampContext<mz_repr::Timestamp>,
    pub conn_id: ConnectionId,
    pub source_arity: usize,
    pub id_bundle: CollectionIdBundle,
    pub source_ids: BTreeSet<GlobalId>,
}

/// Possible ways in which the coordinator could produce the result for a goal view.
#[derive(Debug)]
pub enum PeekPlan<T = mz_repr::Timestamp> {
    FastPath(FastPathPlan),
    /// The view must be installed as a dataflow and then read.
    SlowPath(PeekDataflowPlan<T>),
}

fn permute_oneshot_mfp_around_index(
    mfp: mz_expr::MapFilterProject,
    key: &[MirScalarExpr],
) -> Result<mz_expr::SafeMfpPlan, AdapterError> {
    // Convert `mfp` to an executable, non-temporal plan.
    // It should be non-temporal, as OneShot preparation populates `mz_now`.
    let mut safe_mfp = mfp
        .clone()
        .into_plan()
        .map_err(|e| AdapterError::Unstructured(::anyhow::anyhow!(e)))?
        .into_nontemporal()
        .map_err(|_e| {
            AdapterError::Unstructured(::anyhow::anyhow!("OneShot plan has temporal constraints"))
        })?;
    let (permute, thinning) = mz_expr::permutation_for_arrangement(key, mfp.input_arity);
    safe_mfp.permute(permute, key.len() + thinning.len());
    Ok(safe_mfp)
}

/// Determine if the dataflow plan can be implemented without an actual dataflow.
///
/// If the optimized plan is a `Constant` or a `Get` of a maintained arrangement,
/// we can avoid building a dataflow (and either just return the results, or peek
/// out of the arrangement, respectively).
pub fn create_fast_path_plan<T: timely::progress::Timestamp>(
    dataflow_plan: &mut DataflowDescription<mz_expr::OptimizedMirRelationExpr, (), T>,
    view_id: GlobalId,
) -> Result<Option<FastPathPlan>, AdapterError> {
    // At this point, `dataflow_plan` contains our best optimized dataflow.
    // We will check the plan to see if there is a fast path to escape full dataflow construction.

    // We need to restrict ourselves to settings where the inserted transient view is the first thing
    // to build (no dependent views). There is likely an index to build as well, but we may not be sure.
    if dataflow_plan.objects_to_build.len() >= 1 && dataflow_plan.objects_to_build[0].id == view_id
    {
        let mir = &dataflow_plan.objects_to_build[0].plan.as_inner_mut();
        if let Some((rows, ..)) = mir.as_const() {
            // In the case of a constant, we can return the result now.
            return Ok(Some(FastPathPlan::Constant(
                rows.clone()
                    .map(|rows| rows.into_iter().map(|(row, diff)| (row, diff)).collect()),
                // For best accuracy, we need to recalculate typ.
                mir.typ(),
            )));
        } else {
            // In the case of a linear operator around an indexed view, we
            // can skip creating a dataflow and instead pull all the rows in
            // index and apply the linear operator against them.
            let (mfp, mir) = mz_expr::MapFilterProject::extract_from_expression(mir);
            match mir {
                mz_expr::MirRelationExpr::Get { id, .. } => {
                    // Just grab any arrangement
                    // Nothing to be done if an arrangement does not exist
                    for (index_id, (desc, _typ, _monotonic)) in dataflow_plan.index_imports.iter() {
                        if Id::Global(desc.on_id) == *id {
                            return Ok(Some(FastPathPlan::PeekExisting(
                                *index_id,
                                None,
                                permute_oneshot_mfp_around_index(mfp, &desc.key)?,
                            )));
                        }
                    }
                }
                mz_expr::MirRelationExpr::Join { implementation, .. } => {
                    if let mz_expr::JoinImplementation::IndexedFilter(id, key, vals) =
                        implementation
                    {
                        // We should only get excited if we can track down an index for `id`.
                        // If `keys` is non-empty, that means we think one exists.
                        for (index_id, (desc, _typ, _monotonic)) in
                            dataflow_plan.index_imports.iter()
                        {
                            if desc.on_id == *id && &desc.key == key {
                                // Indicate an early exit with a specific index and key value.
                                return Ok(Some(FastPathPlan::PeekExisting(
                                    *index_id,
                                    Some(vals.clone()),
                                    permute_oneshot_mfp_around_index(mfp, key)?,
                                )));
                            }
                        }
                    }
                }
                // nothing can be done for non-trivial expressions.
                _ => {}
            }
        }
    }
    Ok(None)
}

impl crate::coord::Coordinator {
    /// Creates a [`PeekPlan`] for the given `dataflow`.
    ///
    /// The result will be a [`PeekPlan::FastPath`] plan iff the [`create_fast_path_plan`]
    /// call succeeds, or a [`PeekPlan::SlowPath`] plan wrapping a [`PeekDataflowPlan`]
    /// otherwise.
    pub(crate) fn create_peek_plan(
        &self,
        mut dataflow: DataflowDescription<OptimizedMirRelationExpr>,
        view_id: GlobalId,
        compute_instance: ComputeInstanceId,
        index_id: GlobalId,
        key: Vec<MirScalarExpr>,
        permutation: BTreeMap<usize, usize>,
        thinned_arity: usize,
    ) -> Result<PeekPlan, AdapterError> {
        // try to produce a `FastPathPlan`
        let fast_path_plan = create_fast_path_plan(&mut dataflow, view_id)?;
        // derive a PeekPlan from the optional FastPathPlan
        let peek_plan = fast_path_plan.map_or_else(
            // finalize the dataflow and produce a PeekPlan::SlowPath as a default
            || {
                // We have the opportunity to name an `until` frontier that will prevent work we needn't perform.
                // By default, `until` will be `Antichain::new()`, which prevents no updates and is safe.
                if let Some(as_of) = dataflow.as_of.as_ref() {
                    if !as_of.is_empty() {
                        if let Some(next) = as_of.as_option().and_then(|as_of| as_of.checked_add(1))
                        {
                            dataflow.until = timely::progress::Antichain::from_elem(next);
                        }
                    }
                }
                let desc = self.finalize_dataflow(dataflow, compute_instance)?;

                Ok::<_, AdapterError>(PeekPlan::SlowPath(PeekDataflowPlan {
                    desc,
                    id: index_id,
                    key,
                    permutation,
                    thinned_arity,
                }))
            },
            // produce a PeekPlan::FastPath if possible
            |plan| Ok::<_, AdapterError>(PeekPlan::FastPath(plan)),
        )?;
        Ok(peek_plan)
    }

    /// Implements a peek plan produced by `create_plan` above.
    #[tracing::instrument(level = "debug", skip(self))]
    pub async fn implement_peek_plan(
        &mut self,
        plan: PlannedPeek,
        finishing: RowSetFinishing,
        compute_instance: ComputeInstanceId,
        target_replica: Option<ReplicaId>,
    ) -> Result<crate::ExecuteResponse, AdapterError> {
        let PlannedPeek {
            plan: fast_path,
            timestamp_context,
            conn_id,
            source_arity,
            id_bundle: _,
            source_ids,
        } = plan;

        // If the dataflow optimizes to a constant expression, we can immediately return the result.
        if let PeekPlan::FastPath(FastPathPlan::Constant(rows, _)) = fast_path {
            let rows = match rows {
                Ok(rows) => rows,
                Err(e) => return Err(e.into()),
            };
            // Consolidate down the results to get correct totals.
            let rows = consolidate_constant_updates(rows);

            let mut results = Vec::new();
            for (row, count) in rows {
                if count < 0 {
                    Err(EvalError::InvalidParameterValue(format!(
                        "Negative multiplicity in constant result: {}",
                        count
                    )))?
                };
                if count > 0 {
                    let count = usize::cast_from(
                        u64::try_from(count).expect("known to be positive from check above"),
                    );
                    results.push((
                        row,
                        NonZeroUsize::new(count).expect("known to be non-zero from check above"),
                    ));
                }
            }
            let results =
                finishing.finish(results, self.catalog().system_config().max_result_size());
            return match results {
                Ok(rows) => Ok(send_immediate_rows(rows)),
                Err(e) => Err(AdapterError::ResultSize(e)),
            };
        }

        let timestamp = timestamp_context.timestamp_or_default();

        // The remaining cases are a peek into a maintained arrangement, or building a dataflow.
        // In both cases we will want to peek, and the main difference is that we might want to
        // build a dataflow and drop it once the peek is issued. The peeks are also constructed
        // differently.

        // If we must build the view, ship the dataflow.
        let (peek_command, drop_dataflow) = match fast_path {
            PeekPlan::FastPath(FastPathPlan::PeekExisting(
                id,
                literal_constraints,
                map_filter_project,
            )) => (
                (id, literal_constraints, timestamp, map_filter_project),
                None,
            ),
            PeekPlan::SlowPath(PeekDataflowPlan {
                desc: dataflow,
                // n.b. this index_id identifies a transient index the
                // caller created, so it is guaranteed to be on
                // `compute_instance`.
                id: index_id,
                key: index_key,
                permutation: index_permutation,
                thinned_arity: index_thinned_arity,
            }) => {
                let output_ids = dataflow.export_ids().collect();

                // Very important: actually create the dataflow (here, so we can destructure).
                self.controller
                    .active_compute()
                    .create_dataflows(compute_instance, vec![dataflow])
                    .unwrap_or_terminate("cannot fail to create dataflows");
                self.initialize_compute_read_policies(
                    output_ids,
                    compute_instance,
                    // Disable compaction by using None as the compaction window so that nothing
                    // can compact before the peek occurs below.
                    None,
                )
                .await;

                // Create an identity MFP operator.
                let mut map_filter_project = mz_expr::MapFilterProject::new(source_arity);
                map_filter_project
                    .permute(index_permutation, index_key.len() + index_thinned_arity);
                let map_filter_project = map_filter_project
                    .into_plan()
                    .map_err(|e| crate::error::AdapterError::Unstructured(::anyhow::anyhow!(e)))?
                    .into_nontemporal()
                    .map_err(|_e| {
                        crate::error::AdapterError::Unstructured(::anyhow::anyhow!(
                            "OneShot plan has temporal constraints"
                        ))
                    })?;
                (
                    (
                        index_id, // transient identifier produced by `dataflow_plan`.
                        None,
                        timestamp,
                        map_filter_project,
                    ),
                    Some(index_id),
                )
            }
            _ => {
                unreachable!()
            }
        };

        // Endpoints for sending and receiving peek responses.
        let (rows_tx, rows_rx) = tokio::sync::oneshot::channel();

        // Generate unique UUID. Guaranteed to be unique to all pending peeks, there's an very
        // small but unlikely chance that it's not unique to completed peeks.
        let mut uuid = Uuid::new_v4();
        while self.pending_peeks.contains_key(&uuid) {
            uuid = Uuid::new_v4();
        }

        // The peek is ready to go for both cases, fast and non-fast.
        // Stash the response mechanism, and broadcast dataflow construction.
        self.pending_peeks.insert(
            uuid,
            PendingPeek {
                sender: rows_tx,
                conn_id,
                cluster_id: compute_instance,
                depends_on: source_ids,
            },
        );
        self.client_pending_peeks
            .entry(conn_id)
            .or_default()
            .insert(uuid, compute_instance);
        let (id, literal_constraints, timestamp, map_filter_project) = peek_command;

        self.controller
            .active_compute()
            .peek(
                compute_instance,
                id,
                literal_constraints,
                uuid,
                timestamp,
                finishing.clone(),
                map_filter_project,
                target_replica,
            )
            .unwrap_or_terminate("cannot fail to peek");

        // Prepare the receiver to return as a response.
        let max_result_size = self.catalog().system_config().max_result_size();
        let rows_rx = rows_rx.map_ok_or_else(
            |e| PeekResponseUnary::Error(e.to_string()),
            move |resp| match resp {
                PeekResponse::Rows(rows) => match finishing.finish(rows, max_result_size) {
                    Ok(rows) => PeekResponseUnary::Rows(rows),
                    Err(e) => PeekResponseUnary::Error(e),
                },
                PeekResponse::Canceled => PeekResponseUnary::Canceled,
                PeekResponse::Error(e) => PeekResponseUnary::Error(e),
            },
        );

        // If it was created, drop the dataflow once the peek command is sent.
        if let Some(index_id) = drop_dataflow {
            self.remove_compute_ids_from_timeline(vec![(compute_instance, index_id)]);
            self.drop_indexes(vec![(compute_instance, index_id)]);
        }

        Ok(crate::ExecuteResponse::SendingRows {
            future: Box::pin(rows_rx),
            span: tracing::Span::current(),
        })
    }

    /// Cancel and remove all pending peeks that were initiated by the client with `conn_id`.
    #[tracing::instrument(level = "debug", skip(self))]
    pub(crate) fn cancel_pending_peeks(&mut self, conn_id: &ConnectionId) -> Vec<PendingPeek> {
        // The peek is present on some specific compute instance.
        // Allow dataflow to cancel any pending peeks.
        if let Some(uuids) = self.client_pending_peeks.remove(conn_id) {
            let mut inverse: BTreeMap<ComputeInstanceId, BTreeSet<Uuid>> = Default::default();
            for (uuid, compute_instance) in &uuids {
                inverse.entry(*compute_instance).or_default().insert(*uuid);
            }
            for (compute_instance, uuids) in inverse {
                // It's possible that this compute instance no longer exists because it was dropped
                // while the peek was in progress. In this case we ignore the error and move on
                // because the dataflow no longer exists.
                // TODO(jkosh44) Dropping a cluster should actively cancel all pending queries.
                let _ = self
                    .controller
                    .active_compute()
                    .cancel_peeks(compute_instance, uuids);
            }

            uuids
                .iter()
                .filter_map(|(uuid, _)| self.pending_peeks.remove(uuid))
                .collect()
        } else {
            Vec::new()
        }
    }

    pub(crate) fn send_peek_response(
        &mut self,
        uuid: Uuid,
        response: PeekResponse,
        otel_ctx: OpenTelemetryContext,
    ) {
        // We expect exactly one peek response, which we forward. Then we clean up the
        // peek's state in the coordinator.
        if let Some(PendingPeek {
            sender: rows_tx,
            conn_id: _,
            cluster_id: _,
            depends_on: _,
        }) = self.remove_pending_peek(&uuid)
        {
            otel_ctx.attach_as_parent();
            // Peek cancellations are best effort, so we might still
            // receive a response, even though the recipient is gone.
            let _ = rows_tx.send(response);
        }
        // Cancellation may cause us to receive responses for peeks no
        // longer in `self.pending_peeks`, so we quietly ignore them.
    }

    /// Clean up a peek's state.
    pub(crate) fn remove_pending_peek(&mut self, uuid: &Uuid) -> Option<PendingPeek> {
        let pending_peek = self.pending_peeks.remove(uuid);
        if let Some(pending_peek) = &pending_peek {
            let uuids = self
                .client_pending_peeks
                .get_mut(&pending_peek.conn_id)
                .expect("coord peek state is inconsistent");
            uuids.remove(uuid);
            if uuids.is_empty() {
                self.client_pending_peeks.remove(&pending_peek.conn_id);
            }
        }
        pending_peek
    }
}

fn consolidate_constant_updates(rows: Vec<(Row, Diff)>) -> Vec<(Row, Diff)> {
    // The consolidate API requires timestamps for all rows, so we assigned every row the
    // same timestamp. The actual value of that timestamp doesn't matter.
    let mut rows = rows
        .into_iter()
        .map(|(row, diff)| (row, mz_repr::Timestamp::minimum(), diff))
        .collect();
    differential_dataflow::consolidation::consolidate_updates(&mut rows);
    rows.into_iter()
        .map(|(row, _time, diff)| (row, diff))
        .collect()
}

#[cfg(test)]
mod tests {
    use mz_expr::func::IsNull;
    use mz_expr::{MapFilterProject, UnaryFunc};
    use mz_ore::str::Indent;
    use mz_repr::explain::text::text_string_at;
    use mz_repr::explain::{DummyHumanizer, RenderingContext};
    use mz_repr::{ColumnType, Datum, ScalarType};

    use super::*;

    #[test]
    fn test_fast_path_plan_as_text() {
        let typ = RelationType::new(vec![ColumnType {
            scalar_type: ScalarType::String,
            nullable: false,
        }]);
        let constant_err = FastPathPlan::Constant(Err(EvalError::DivisionByZero), typ.clone());
        let no_lookup = FastPathPlan::PeekExisting(
            GlobalId::User(10),
            None,
            MapFilterProject::new(4)
                .map(Some(MirScalarExpr::column(0).or(MirScalarExpr::column(2))))
                .project([1, 4])
                .into_plan()
                .expect("invalid plan")
                .into_nontemporal()
                .expect("invalid nontemporal"),
        );
        let lookup = FastPathPlan::PeekExisting(
            GlobalId::User(11),
            Some(vec![Row::pack(Some(Datum::Int32(5)))]),
            MapFilterProject::new(3)
                .filter(Some(
                    MirScalarExpr::column(0).call_unary(UnaryFunc::IsNull(IsNull)),
                ))
                .into_plan()
                .expect("invalid plan")
                .into_nontemporal()
                .expect("invalid nontemporal"),
        );

        let humanizer = DummyHumanizer;
        let ctx_gen = || RenderingContext::new(Indent::default(), &humanizer);

        let constant_err_exp = "Error \"division by zero\"\n";
        let no_lookup_exp = "Project (#1, #4)\n  Map ((#0 OR #2))\n    ReadExistingIndex u10\n";
        let lookup_exp = "Filter (#0) IS NULL\n  ReadExistingIndex u11 lookup_value=(5)\n";

        assert_eq!(text_string_at(&constant_err, ctx_gen), constant_err_exp);
        assert_eq!(text_string_at(&no_lookup, ctx_gen), no_lookup_exp);
        assert_eq!(text_string_at(&lookup, ctx_gen), lookup_exp);

        let mut constant_rows = vec![
            (Row::pack(Some(Datum::String("hello"))), 1),
            (Row::pack(Some(Datum::String("world"))), 2),
            (Row::pack(Some(Datum::String("star"))), 500),
        ];
        let constant_exp1 =
            "Constant\n  - (\"hello\")\n  - ((\"world\") x 2)\n  - ((\"star\") x 500)\n";
        assert_eq!(
            text_string_at(
                &FastPathPlan::Constant(Ok(constant_rows.clone()), typ.clone()),
                ctx_gen
            ),
            constant_exp1
        );
        constant_rows.extend((0..20).map(|i| (Row::pack(Some(Datum::String(&i.to_string()))), 1)));
        let constant_exp2 =
            "Constant\n  total_rows (diffs absed): 523\n  first_rows:\n    - (\"hello\")\
        \n    - ((\"world\") x 2)\n    - ((\"star\") x 500)\n    - (\"0\")\n    - (\"1\")\
        \n    - (\"2\")\n    - (\"3\")\n    - (\"4\")\n    - (\"5\")\n    - (\"6\")\
        \n    - (\"7\")\n    - (\"8\")\n    - (\"9\")\n    - (\"10\")\n    - (\"11\")\
        \n    - (\"12\")\n    - (\"13\")\n    - (\"14\")\n    - (\"15\")\n    - (\"16\")\n";
        assert_eq!(
            text_string_at(&FastPathPlan::Constant(Ok(constant_rows), typ), ctx_gen),
            constant_exp2
        );
    }
}

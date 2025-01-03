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

use differential_dataflow::consolidation::consolidate;
use futures::TryFutureExt;
use mz_adapter_types::compaction::CompactionWindow;
use mz_adapter_types::connection::ConnectionId;
use mz_cluster_client::ReplicaId;
use mz_compute_client::controller::PeekNotification;
use mz_compute_client::protocol::command::PeekTarget;
use mz_compute_client::protocol::response::PeekResponse;
use mz_compute_types::dataflows::{DataflowDescription, IndexImport};
use mz_compute_types::ComputeInstanceId;
use mz_controller_types::ClusterId;
use mz_expr::explain::{fmt_text_constant_rows, HumanizedExplain, HumanizerMode};
use mz_expr::row::RowCollection;
use mz_expr::{
    permutation_for_arrangement, EvalError, Id, MirRelationExpr, MirScalarExpr,
    OptimizedMirRelationExpr, RowSetFinishing,
};
use mz_ore::cast::CastFrom;
use mz_ore::str::{separated, StrExt};
use mz_ore::tracing::OpenTelemetryContext;
use mz_repr::explain::text::DisplayText;
use mz_repr::explain::{CompactScalars, IndexUsageType, PlanRenderingContext, UsedIndexes};
use mz_repr::{Diff, GlobalId, IntoRowIterator, RelationType, Row, RowIterator};
use serde::{Deserialize, Serialize};
use timely::progress::Timestamp;
use uuid::Uuid;

use crate::coord::timestamp_selection::TimestampDetermination;
use crate::optimize::OptimizerError;
use crate::statement_logging::{StatementEndedExecutionReason, StatementExecutionStrategy};
use crate::util::ResultExt;
use crate::{AdapterError, ExecuteContextExtra, ExecuteResponse};

/// A peek is a request to read data from a maintained arrangement.
#[derive(Debug)]
pub(crate) struct PendingPeek {
    /// The connection that initiated the peek.
    pub(crate) conn_id: ConnectionId,
    /// The cluster that the peek is being executed on.
    pub(crate) cluster_id: ClusterId,
    /// All `GlobalId`s that the peek depend on.
    pub(crate) depends_on: BTreeSet<GlobalId>,
    /// Context about the execute that produced this peek,
    /// needed by the coordinator for retiring it.
    pub(crate) ctx_extra: ExecuteContextExtra,
    /// Is this a fast-path peek, i.e. one that doesn't require a dataflow?
    pub(crate) is_fast_path: bool,
}

/// The response from a `Peek`, with row multiplicities represented in unary.
///
/// Note that each `Peek` expects to generate exactly one `PeekResponse`, i.e.
/// we expect a 1:1 contract between `Peek` and `PeekResponseUnary`.
#[derive(Debug)]
pub enum PeekResponseUnary {
    Rows(Box<dyn RowIterator + Send + Sync>),
    Error(String),
    Canceled,
}

#[derive(Clone, Debug)]
pub struct PeekDataflowPlan<T = mz_repr::Timestamp> {
    pub(crate) desc: DataflowDescription<mz_compute_types::plan::Plan<T>, (), T>,
    pub(crate) id: GlobalId,
    key: Vec<MirScalarExpr>,
    permutation: Vec<usize>,
    thinned_arity: usize,
}

impl<T> PeekDataflowPlan<T> {
    pub fn new(
        desc: DataflowDescription<mz_compute_types::plan::Plan<T>, (), T>,
        id: GlobalId,
        typ: &RelationType,
    ) -> Self {
        let arity = typ.arity();
        let key = typ
            .default_key()
            .into_iter()
            .map(MirScalarExpr::Column)
            .collect::<Vec<_>>();
        let (permutation, thinning) = permutation_for_arrangement(&key, arity);
        Self {
            desc,
            id,
            key,
            permutation,
            thinned_arity: thinning.len(),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, Ord, PartialOrd)]
pub enum FastPathPlan {
    /// The view evaluates to a constant result that can be returned.
    ///
    /// The [RelationType] is unnecessary for evaluating the constant result but
    /// may be helpful when printing out an explanation.
    Constant(Result<Vec<(Row, Diff)>, EvalError>, RelationType),
    /// The view can be read out of an existing arrangement.
    /// (coll_id, idx_id, values to look up, mfp to apply)
    PeekExisting(GlobalId, GlobalId, Option<Vec<Row>>, mz_expr::SafeMfpPlan),
    /// The view can be read directly out of Persist.
    PeekPersist(GlobalId, mz_expr::SafeMfpPlan),
}

impl<'a, T: 'a> DisplayText<PlanRenderingContext<'a, T>> for FastPathPlan {
    fn fmt_text(
        &self,
        f: &mut fmt::Formatter<'_>,
        ctx: &mut PlanRenderingContext<'a, T>,
    ) -> fmt::Result {
        let redacted = ctx.config.redacted;
        let mode = HumanizedExplain::new(ctx.config.redacted);

        // TODO(aalexandrov): factor out common PeekExisting and PeekPersist
        // code.
        match self {
            FastPathPlan::Constant(Ok(rows), _) => {
                if !rows.is_empty() {
                    writeln!(f, "{}Constant", ctx.indent)?;
                    *ctx.as_mut() += 1;
                    fmt_text_constant_rows(
                        f,
                        rows.iter().map(|(row, diff)| (row, diff)),
                        ctx.as_mut(),
                        redacted,
                    )?;
                    *ctx.as_mut() -= 1;
                } else {
                    writeln!(f, "{}Constant <empty>", ctx.as_mut())?;
                }
                Ok(())
            }
            FastPathPlan::Constant(Err(err), _) => {
                if redacted {
                    writeln!(f, "{}Error █", ctx.as_mut())
                } else {
                    writeln!(f, "{}Error {}", ctx.as_mut(), err.to_string().escaped())
                }
            }
            FastPathPlan::PeekExisting(coll_id, idx_id, literal_constraints, mfp) => {
                ctx.as_mut().set();
                let (map, filter, project) = mfp.as_map_filter_project();

                let cols = if !ctx.config.humanized_exprs {
                    None
                } else if let Some(cols) = ctx.humanizer.column_names_for_id(*idx_id) {
                    // FIXME: account for thinning and permutation
                    // See mz_expr::permutation_for_arrangement
                    // See permute_oneshot_mfp_around_index
                    let cols = itertools::chain(
                        cols.iter().cloned(),
                        std::iter::repeat(String::new()).take(map.len()),
                    )
                    .collect();
                    Some(cols)
                } else {
                    None
                };

                if project.len() != mfp.input_arity + map.len()
                    || !project.iter().enumerate().all(|(i, o)| i == *o)
                {
                    let outputs = mode.seq(&project, cols.as_ref());
                    let outputs = CompactScalars(outputs);
                    writeln!(f, "{}Project ({})", ctx.as_mut(), outputs)?;
                    *ctx.as_mut() += 1;
                }
                if !filter.is_empty() {
                    let predicates = separated(" AND ", mode.seq(&filter, cols.as_ref()));
                    writeln!(f, "{}Filter {}", ctx.as_mut(), predicates)?;
                    *ctx.as_mut() += 1;
                }
                if !map.is_empty() {
                    let scalars = mode.seq(&map, cols.as_ref());
                    let scalars = CompactScalars(scalars);
                    writeln!(f, "{}Map ({})", ctx.as_mut(), scalars)?;
                    *ctx.as_mut() += 1;
                }
                MirRelationExpr::fmt_indexed_filter(
                    f,
                    ctx,
                    coll_id,
                    idx_id,
                    literal_constraints.clone(),
                    None,
                )?;
                writeln!(f)?;
                ctx.as_mut().reset();
                Ok(())
            }
            FastPathPlan::PeekPersist(gid, mfp) => {
                ctx.as_mut().set();
                let (map, filter, project) = mfp.as_map_filter_project();

                let cols = if !ctx.config.humanized_exprs {
                    None
                } else if let Some(cols) = ctx.humanizer.column_names_for_id(*gid) {
                    let cols = itertools::chain(
                        cols.iter().cloned(),
                        std::iter::repeat(String::new()).take(map.len()),
                    )
                    .collect::<Vec<_>>();
                    Some(cols)
                } else {
                    None
                };

                if project.len() != mfp.input_arity + map.len()
                    || !project.iter().enumerate().all(|(i, o)| i == *o)
                {
                    let outputs = mode.seq(&project, cols.as_ref());
                    let outputs = CompactScalars(outputs);
                    writeln!(f, "{}Project ({})", ctx.as_mut(), outputs)?;
                    *ctx.as_mut() += 1;
                }
                if !filter.is_empty() {
                    let predicates = separated(" AND ", mode.seq(&filter, cols.as_ref()));
                    writeln!(f, "{}Filter {}", ctx.as_mut(), predicates)?;
                    *ctx.as_mut() += 1;
                }
                if !map.is_empty() {
                    let scalars = mode.seq(&map, cols.as_ref());
                    let scalars = CompactScalars(scalars);
                    writeln!(f, "{}Map ({})", ctx.as_mut(), scalars)?;
                    *ctx.as_mut() += 1;
                }
                let human_id = ctx
                    .humanizer
                    .humanize_id(*gid)
                    .unwrap_or_else(|| gid.to_string());
                writeln!(f, "{}PeekPersist {human_id}", ctx.as_mut())?;
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
    pub determination: TimestampDetermination<mz_repr::Timestamp>,
    pub conn_id: ConnectionId,
    pub source_arity: usize,
    pub source_ids: BTreeSet<GlobalId>,
}

/// Possible ways in which the coordinator could produce the result for a goal view.
#[derive(Clone, Debug)]
pub enum PeekPlan<T = mz_repr::Timestamp> {
    FastPath(FastPathPlan),
    /// The view must be installed as a dataflow and then read.
    SlowPath(PeekDataflowPlan<T>),
}

/// Convert `mfp` to an executable, non-temporal plan.
/// It should be non-temporal, as OneShot preparation populates `mz_now`.
fn mfp_to_safe_plan(
    mfp: mz_expr::MapFilterProject,
) -> Result<mz_expr::SafeMfpPlan, OptimizerError> {
    mfp.into_plan()
        .map_err(OptimizerError::Internal)?
        .into_nontemporal()
        .map_err(|_e| OptimizerError::UnsafeMfpPlan)
}

fn permute_oneshot_mfp_around_index(
    mfp: mz_expr::MapFilterProject,
    key: &[MirScalarExpr],
) -> Result<mz_expr::SafeMfpPlan, OptimizerError> {
    let input_arity = mfp.input_arity;
    let mut safe_mfp = mfp_to_safe_plan(mfp)?;
    let (permute, thinning) = mz_expr::permutation_for_arrangement(key, input_arity);
    safe_mfp.permute_fn(|c| permute[c], key.len() + thinning.len());
    Ok(safe_mfp)
}

/// Determine if the dataflow plan can be implemented without an actual dataflow.
///
/// If the optimized plan is a `Constant` or a `Get` of a maintained arrangement,
/// we can avoid building a dataflow (and either just return the results, or peek
/// out of the arrangement, respectively).
pub fn create_fast_path_plan<T: Timestamp>(
    dataflow_plan: &mut DataflowDescription<OptimizedMirRelationExpr, (), T>,
    view_id: GlobalId,
    finishing: Option<&RowSetFinishing>,
    persist_fast_path_limit: usize,
) -> Result<Option<FastPathPlan>, OptimizerError> {
    // At this point, `dataflow_plan` contains our best optimized dataflow.
    // We will check the plan to see if there is a fast path to escape full dataflow construction.

    // We need to restrict ourselves to settings where the inserted transient view is the first thing
    // to build (no dependent views). There is likely an index to build as well, but we may not be sure.
    if dataflow_plan.objects_to_build.len() >= 1 && dataflow_plan.objects_to_build[0].id == view_id
    {
        let mut mir = &*dataflow_plan.objects_to_build[0].plan.as_inner_mut();
        if let Some((rows, found_typ)) = mir.as_const() {
            // In the case of a constant, we can return the result now.
            return Ok(Some(FastPathPlan::Constant(
                rows.clone()
                    .map(|rows| rows.into_iter().map(|(row, diff)| (row, diff)).collect()),
                found_typ.clone(),
            )));
        } else {
            // If there is a TopK that would be completely covered by the finishing, then jump
            // through the TopK.
            if let MirRelationExpr::TopK {
                input,
                group_key,
                order_key,
                limit,
                offset,
                monotonic: _,
                expected_group_size: _,
            } = mir
            {
                if let Some(finishing) = finishing {
                    if group_key.is_empty() && *order_key == finishing.order_by && *offset == 0 {
                        // The following is roughly `limit >= finishing.limit`, but with Options.
                        let finishing_limits_at_least_as_topk = match (limit, finishing.limit) {
                            (None, _) => true,
                            (Some(..), None) => false,
                            (Some(topk_limit), Some(finishing_limit)) => {
                                if let Some(l) = topk_limit.as_literal_int64() {
                                    l >= *finishing_limit
                                } else {
                                    false
                                }
                            }
                        };
                        if finishing_limits_at_least_as_topk {
                            mir = input;
                        }
                    }
                }
            }
            // In the case of a linear operator around an indexed view, we
            // can skip creating a dataflow and instead pull all the rows in
            // index and apply the linear operator against them.
            let (mfp, mir) = mz_expr::MapFilterProject::extract_from_expression(mir);
            match mir {
                MirRelationExpr::Get {
                    id: Id::Global(get_id),
                    ..
                } => {
                    // Just grab any arrangement if an arrangement exists
                    for (index_id, IndexImport { desc, .. }) in dataflow_plan.index_imports.iter() {
                        if desc.on_id == *get_id {
                            return Ok(Some(FastPathPlan::PeekExisting(
                                *get_id,
                                *index_id,
                                None,
                                permute_oneshot_mfp_around_index(mfp, &desc.key)?,
                            )));
                        }
                    }
                    // If there is no arrangement, consider peeking the persist shard directly
                    let safe_mfp = mfp_to_safe_plan(mfp)?;
                    let (_m, filters, _p) = safe_mfp.as_map_filter_project();
                    let small_finish = match &finishing {
                        None => false,
                        Some(RowSetFinishing {
                            order_by,
                            limit,
                            offset,
                            ..
                        }) => {
                            order_by.is_empty()
                                && limit.iter().any(|l| {
                                    usize::cast_from(*l) + *offset < persist_fast_path_limit
                                })
                        }
                    };
                    if filters.is_empty() && small_finish {
                        return Ok(Some(FastPathPlan::PeekPersist(*get_id, safe_mfp)));
                    }
                }
                MirRelationExpr::Join { implementation, .. } => {
                    if let mz_expr::JoinImplementation::IndexedFilter(coll_id, idx_id, key, vals) =
                        implementation
                    {
                        return Ok(Some(FastPathPlan::PeekExisting(
                            *coll_id,
                            *idx_id,
                            Some(vals.clone()),
                            permute_oneshot_mfp_around_index(mfp, key)?,
                        )));
                    }
                }
                // nothing can be done for non-trivial expressions.
                _ => {}
            }
        }
    }
    Ok(None)
}

impl FastPathPlan {
    pub fn used_indexes(&self, finishing: Option<&RowSetFinishing>) -> UsedIndexes {
        match self {
            FastPathPlan::Constant(..) => UsedIndexes::default(),
            FastPathPlan::PeekExisting(_coll_id, idx_id, literal_constraints, _mfp) => {
                if literal_constraints.is_some() {
                    UsedIndexes::new([(*idx_id, vec![IndexUsageType::Lookup(*idx_id)])].into())
                } else if finishing.map_or(false, |f| f.limit.is_some() && f.order_by.is_empty()) {
                    UsedIndexes::new([(*idx_id, vec![IndexUsageType::FastPathLimit])].into())
                } else {
                    UsedIndexes::new([(*idx_id, vec![IndexUsageType::FullScan])].into())
                }
            }
            FastPathPlan::PeekPersist(..) => UsedIndexes::default(),
        }
    }
}

impl crate::coord::Coordinator {
    /// Implements a peek plan produced by `create_plan` above.
    #[mz_ore::instrument(level = "debug")]
    pub async fn implement_peek_plan(
        &mut self,
        ctx_extra: &mut ExecuteContextExtra,
        plan: PlannedPeek,
        finishing: RowSetFinishing,
        compute_instance: ComputeInstanceId,
        target_replica: Option<ReplicaId>,
        max_result_size: u64,
        max_returned_query_size: Option<u64>,
    ) -> Result<crate::ExecuteResponse, AdapterError> {
        let PlannedPeek {
            plan: fast_path,
            determination,
            conn_id,
            source_arity,
            source_ids,
        } = plan;

        // If the dataflow optimizes to a constant expression, we can immediately return the result.
        if let PeekPlan::FastPath(FastPathPlan::Constant(rows, _)) = fast_path {
            let mut rows = match rows {
                Ok(rows) => rows,
                Err(e) => return Err(e.into()),
            };
            // Consolidate down the results to get correct totals.
            consolidate(&mut rows);

            let mut results = Vec::new();
            for (row, count) in rows {
                if count < 0 {
                    Err(EvalError::InvalidParameterValue(
                        format!("Negative multiplicity in constant result: {}", count).into(),
                    ))?
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
            let row_collection = RowCollection::new(results, &finishing.order_by);
            let duration_histogram = self.metrics.row_set_finishing_seconds();

            let (ret, reason) = match finishing.finish(
                row_collection,
                max_result_size,
                max_returned_query_size,
                &duration_histogram,
            ) {
                Ok((rows, row_size_bytes)) => {
                    let result_size = u64::cast_from(row_size_bytes);
                    let rows_returned = u64::cast_from(rows.count());
                    (
                        Ok(Self::send_immediate_rows(rows)),
                        StatementEndedExecutionReason::Success {
                            result_size: Some(result_size),
                            rows_returned: Some(rows_returned),
                            execution_strategy: Some(StatementExecutionStrategy::Constant),
                        },
                    )
                }
                Err(error) => (
                    Err(AdapterError::ResultSize(error.clone())),
                    StatementEndedExecutionReason::Errored { error },
                ),
            };
            self.retire_execution(reason, std::mem::take(ctx_extra));
            return ret;
        }

        let timestamp = determination.timestamp_context.timestamp_or_default();
        if let Some(id) = ctx_extra.contents() {
            self.set_statement_execution_timestamp(id, timestamp)
        }

        // The remaining cases are a peek into a maintained arrangement, or building a dataflow.
        // In both cases we will want to peek, and the main difference is that we might want to
        // build a dataflow and drop it once the peek is issued. The peeks are also constructed
        // differently.

        // If we must build the view, ship the dataflow.
        let (peek_command, drop_dataflow, is_fast_path, peek_target, strategy) = match fast_path {
            PeekPlan::FastPath(FastPathPlan::PeekExisting(
                _coll_id,
                idx_id,
                literal_constraints,
                map_filter_project,
            )) => (
                (literal_constraints, timestamp, map_filter_project),
                None,
                true,
                PeekTarget::Index { id: idx_id },
                StatementExecutionStrategy::FastPath,
            ),
            PeekPlan::FastPath(FastPathPlan::PeekPersist(coll_id, map_filter_project)) => {
                let peek_command = (None, timestamp, map_filter_project);
                let metadata = self
                    .controller
                    .storage
                    .collection_metadata(coll_id)
                    .expect("storage collection for fast-path peek")
                    .clone();
                (
                    peek_command,
                    None,
                    true,
                    PeekTarget::Persist {
                        id: coll_id,
                        metadata,
                    },
                    StatementExecutionStrategy::PersistFastPath,
                )
            }
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
                    .compute
                    .create_dataflow(compute_instance, dataflow, None)
                    .unwrap_or_terminate("cannot fail to create dataflows");
                self.initialize_compute_read_policies(
                    output_ids,
                    compute_instance,
                    // Disable compaction so that nothing can compact before the peek occurs below.
                    CompactionWindow::DisableCompaction,
                )
                .await;

                // Create an identity MFP operator.
                let mut map_filter_project = mz_expr::MapFilterProject::new(source_arity);
                map_filter_project.permute_fn(
                    |c| index_permutation[c],
                    index_key.len() + index_thinned_arity,
                );
                let map_filter_project = mfp_to_safe_plan(map_filter_project)?;
                (
                    (None, timestamp, map_filter_project),
                    Some(index_id),
                    false,
                    PeekTarget::Index { id: index_id },
                    StatementExecutionStrategy::Standard,
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
                conn_id: conn_id.clone(),
                cluster_id: compute_instance,
                depends_on: source_ids,
                ctx_extra: std::mem::take(ctx_extra),
                is_fast_path,
            },
        );
        self.client_pending_peeks
            .entry(conn_id)
            .or_default()
            .insert(uuid, compute_instance);
        let (literal_constraints, timestamp, map_filter_project) = peek_command;

        self.controller
            .compute
            .peek(
                compute_instance,
                peek_target,
                literal_constraints,
                uuid,
                timestamp,
                finishing.clone(),
                map_filter_project,
                target_replica,
                rows_tx,
            )
            .unwrap_or_terminate("cannot fail to peek");
        let duration_histogram = self.metrics.row_set_finishing_seconds();

        // Prepare the receiver to return as a response.
        let rows_rx = rows_rx.map_ok_or_else(
            |e| PeekResponseUnary::Error(e.to_string()),
            move |resp| match resp {
                PeekResponse::Rows(rows) => {
                    match finishing.finish(
                        rows,
                        max_result_size,
                        max_returned_query_size,
                        &duration_histogram,
                    ) {
                        Ok((rows, _size_bytes)) => PeekResponseUnary::Rows(Box::new(rows)),
                        Err(e) => PeekResponseUnary::Error(e),
                    }
                }
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
            instance_id: compute_instance,
            strategy,
        })
    }

    /// Cancel and remove all pending peeks that were initiated by the client with `conn_id`.
    #[mz_ore::instrument(level = "debug")]
    pub(crate) fn cancel_pending_peeks(&mut self, conn_id: &ConnectionId) {
        if let Some(uuids) = self.client_pending_peeks.remove(conn_id) {
            self.metrics
                .canceled_peeks
                .with_label_values(&[])
                .inc_by(u64::cast_from(uuids.len()));

            let mut inverse: BTreeMap<ComputeInstanceId, BTreeSet<Uuid>> = Default::default();
            for (uuid, compute_instance) in &uuids {
                inverse.entry(*compute_instance).or_default().insert(*uuid);
            }
            for (compute_instance, uuids) in inverse {
                // It's possible that this compute instance no longer exists because it was dropped
                // while the peek was in progress. In this case we ignore the error and move on
                // because the dataflow no longer exists.
                // TODO(jkosh44) Dropping a cluster should actively cancel all pending queries.
                for uuid in uuids {
                    let _ = self.controller.compute.cancel_peek(
                        compute_instance,
                        uuid,
                        PeekResponse::Canceled,
                    );
                }
            }

            let peeks = uuids
                .iter()
                .filter_map(|(uuid, _)| self.pending_peeks.remove(uuid))
                .collect::<Vec<_>>();
            for peek in peeks {
                self.retire_execution(StatementEndedExecutionReason::Canceled, peek.ctx_extra);
            }
        }
    }

    /// Handle a peek notification and retire the corresponding execution. Does nothing for
    /// already-removed peeks.
    pub(crate) fn handle_peek_notification(
        &mut self,
        uuid: Uuid,
        notification: PeekNotification,
        otel_ctx: OpenTelemetryContext,
    ) {
        // We expect exactly one peek response, which we forward. Then we clean up the
        // peek's state in the coordinator.
        if let Some(PendingPeek {
            conn_id: _,
            cluster_id: _,
            depends_on: _,
            ctx_extra,
            is_fast_path,
        }) = self.remove_pending_peek(&uuid)
        {
            let reason = match notification {
                PeekNotification::Success {
                    rows: num_rows,
                    result_size,
                } => {
                    let strategy = if is_fast_path {
                        StatementExecutionStrategy::FastPath
                    } else {
                        StatementExecutionStrategy::Standard
                    };
                    StatementEndedExecutionReason::Success {
                        result_size: Some(result_size),
                        rows_returned: Some(num_rows),
                        execution_strategy: Some(strategy),
                    }
                }
                PeekNotification::Error(error) => StatementEndedExecutionReason::Errored { error },
                PeekNotification::Canceled => StatementEndedExecutionReason::Canceled,
            };
            otel_ctx.attach_as_parent();
            self.retire_execution(reason, ctx_extra);
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

    /// Constructs an [`ExecuteResponse`] that that will send some rows to the
    /// client immediately, as opposed to asking the dataflow layer to send along
    /// the rows after some computation.
    pub(crate) fn send_immediate_rows<I>(rows: I) -> ExecuteResponse
    where
        I: IntoRowIterator,
        I::Iter: Send + Sync + 'static,
    {
        let rows = Box::new(rows.into_row_iter());
        ExecuteResponse::SendingRowsImmediate { rows }
    }
}

#[cfg(test)]
mod tests {
    use mz_expr::func::IsNull;
    use mz_expr::{MapFilterProject, UnaryFunc};
    use mz_ore::str::Indent;
    use mz_repr::explain::text::text_string_at;
    use mz_repr::explain::{DummyHumanizer, ExplainConfig, PlanRenderingContext};
    use mz_repr::{ColumnType, Datum, ScalarType};

    use super::*;

    #[mz_ore::test]
    #[cfg_attr(miri, ignore)] // unsupported operation: can't call foreign function `rust_psm_stack_pointer` on OS `linux`
    fn test_fast_path_plan_as_text() {
        let typ = RelationType::new(vec![ColumnType {
            scalar_type: ScalarType::String,
            nullable: false,
        }]);
        let constant_err = FastPathPlan::Constant(Err(EvalError::DivisionByZero), typ.clone());
        let no_lookup = FastPathPlan::PeekExisting(
            GlobalId::User(8),
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
            GlobalId::User(9),
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
        let config = ExplainConfig {
            redacted: false,
            ..Default::default()
        };
        let ctx_gen = || {
            let indent = Indent::default();
            let annotations = BTreeMap::new();
            PlanRenderingContext::<FastPathPlan>::new(indent, &humanizer, annotations, &config)
        };

        let constant_err_exp = "Error \"division by zero\"\n";
        let no_lookup_exp =
            "Project (#1, #4)\n  Map ((#0 OR #2))\n    ReadIndex on=u8 [DELETED INDEX]=[*** full scan ***]\n";
        let lookup_exp =
            "Filter (#0) IS NULL\n  ReadIndex on=u9 [DELETED INDEX]=[lookup value=(5)]\n";

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

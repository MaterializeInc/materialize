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
use std::ops::Deref;
use std::sync::Arc;

use mz_adapter_types::connection::ConnectionId;
use mz_cluster_client::ReplicaId;
use mz_compute_client::controller::PeekNotification;
use mz_compute_client::protocol::command::PeekTarget;
use mz_compute_client::protocol::response::PeekResponse;
use mz_compute_types::ComputeInstanceId;
use mz_compute_types::dataflows::{DataflowDescription, IndexImport};
use mz_controller_types::ClusterId;
use mz_expr::explain::{HumanizedExplain, HumanizerMode, fmt_text_constant_rows};
use mz_expr::{
    EvalError, Id, MirRelationExpr, MirScalarExpr, OptimizedMirRelationExpr, RowSetFinishing,
    permutation_for_arrangement,
};
use mz_ore::cast::CastFrom;
use mz_ore::soft_assert_eq_or_log;
use mz_ore::str::{StrExt, separated};
use mz_ore::task;
use mz_ore::tracing::OpenTelemetryContext;
use mz_repr::explain::text::DisplayText;
use mz_repr::explain::{CompactScalars, IndexUsageType, PlanRenderingContext, UsedIndexes};
use mz_repr::{
    Diff, GlobalId, IntoRowIterator, RelationDesc, Row, RowIterator, SqlRelationType,
    preserves_order,
};
use serde::{Deserialize, Serialize};
use tokio::sync::oneshot;
use tracing::{Instrument, Span};
use uuid::Uuid;

use crate::active_compute_sink::{ActiveComputeSink, ActiveCopyTo};
use crate::catalog::Catalog;
use crate::coord::timestamp_selection::TimestampDetermination;
use crate::optimize::OptimizerError;
use crate::peek_client::CoordinatorClient;
use crate::statement_logging::WatchSetCreation;
use crate::statement_logging::{StatementEndedExecutionReason, StatementExecutionStrategy};
use crate::{AdapterError, ExecuteContextGuard, ExecuteResponse, PeekClient};

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
    pub(crate) ctx_extra: ExecuteContextGuard,
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
    Error(AdapterError),
    Canceled,
    /// A dependency was dropped during execution.
    DependencyDropped(DroppedDependency),
}

/// A dependency that was dropped while a peek or subscribe was in flight.
///
/// The `name` fields hold the bare name (e.g. `db.schema.t` or `c`); `Display`
/// applies SQL identifier quoting to produce `relation "db.schema.t"` or
/// `cluster "c"` for direct use in error wording.
#[derive(Clone, Debug)]
pub enum DroppedDependency {
    Relation { name: String },
    Cluster { name: String },
}

impl fmt::Display for DroppedDependency {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Relation { name } => write!(f, "relation {}", name.quoted()),
            Self::Cluster { name } => write!(f, "cluster {}", name.quoted()),
        }
    }
}

impl DroppedDependency {
    /// User-facing error for a query (peek or subscribe) that could not finish
    /// because this dependency was dropped mid-flight.
    pub fn query_terminated_error(&self) -> String {
        format!("query could not complete because {self} was dropped")
    }

    /// Convert this dropped dependency into an [`AdapterError::ConcurrentDependencyDrop`].
    pub fn to_concurrent_dependency_drop(&self) -> AdapterError {
        let (kind, name) = match self {
            Self::Relation { name } => ("relation", name.clone()),
            Self::Cluster { name } => ("cluster", name.clone()),
        };
        AdapterError::ConcurrentDependencyDrop {
            dependency_kind: kind,
            dependency_id: name,
        }
    }
}

#[derive(Clone, Debug)]
pub struct PeekDataflowPlan {
    pub(crate) desc: DataflowDescription<mz_compute_types::plan::LirRelationExpr, ()>,
    pub(crate) id: GlobalId,
    key: Vec<MirScalarExpr>,
    permutation: Vec<usize>,
    thinned_arity: usize,
}

impl PeekDataflowPlan {
    pub fn new(
        desc: DataflowDescription<mz_compute_types::plan::LirRelationExpr, ()>,
        id: GlobalId,
        typ: &SqlRelationType,
    ) -> Self {
        let arity = typ.arity();
        let key = typ
            .default_key()
            .into_iter()
            .map(MirScalarExpr::column)
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
    /// The [SqlRelationType] is unnecessary for evaluating the constant result but
    /// may be helpful when printing out an explanation.
    Constant(Result<Vec<(Row, Diff)>, EvalError>, SqlRelationType),
    /// The view can be read out of an existing arrangement.
    /// (coll_id, idx_id, values to look up, mfp to apply)
    PeekExisting(GlobalId, GlobalId, Option<Vec<Row>>, mz_expr::SafeMfpPlan),
    /// The view can be read directly out of Persist.
    PeekPersist(GlobalId, Option<Row>, mz_expr::SafeMfpPlan),
}

impl<'a, T: 'a> DisplayText<PlanRenderingContext<'a, T>> for FastPathPlan {
    fn fmt_text(
        &self,
        f: &mut fmt::Formatter<'_>,
        ctx: &mut PlanRenderingContext<'a, T>,
    ) -> fmt::Result {
        if ctx.config.verbose_syntax {
            self.fmt_verbose_text(f, ctx)
        } else {
            self.fmt_default_text(f, ctx)
        }
    }
}

impl FastPathPlan {
    pub fn fmt_default_text<'a, T>(
        &self,
        f: &mut fmt::Formatter<'_>,
        ctx: &mut PlanRenderingContext<'a, T>,
    ) -> fmt::Result {
        let mode = HumanizedExplain::new(ctx.config.redacted);

        match self {
            FastPathPlan::Constant(rows, _) => {
                write!(f, "{}→Constant ", ctx.indent)?;

                match rows {
                    Ok(rows) => writeln!(f, "({} rows)", rows.len())?,
                    Err(err) => {
                        if mode.redacted() {
                            writeln!(f, "(error: █)")?;
                        } else {
                            writeln!(f, "(error: {})", err.to_string().quoted(),)?;
                        }
                    }
                }
            }
            FastPathPlan::PeekExisting(coll_id, idx_id, literal_constraints, mfp) => {
                let coll = ctx
                    .humanizer
                    .humanize_id(*coll_id)
                    .unwrap_or_else(|| coll_id.to_string());
                let idx = ctx
                    .humanizer
                    .humanize_id(*idx_id)
                    .unwrap_or_else(|| idx_id.to_string());
                writeln!(f, "{}→Map/Filter/Project", ctx.indent)?;
                ctx.indent.set();

                ctx.indent += 1;

                mode.expr(mfp.deref(), None).fmt_default_text(f, ctx)?;
                let printed = !mfp.expressions.is_empty() || !mfp.predicates.is_empty();

                if printed {
                    ctx.indent += 1;
                }
                if let Some(literal_constraints) = literal_constraints {
                    writeln!(f, "{}→Index Lookup on {coll} (using {idx})", ctx.indent)?;
                    ctx.indent += 1;
                    let values = separated("; ", mode.seq(literal_constraints, None));
                    writeln!(f, "{}Lookup values: {values}", ctx.indent)?;
                } else {
                    writeln!(f, "{}→Indexed {coll} (using {idx})", ctx.indent)?;
                }

                ctx.indent.reset();
            }
            FastPathPlan::PeekPersist(global_id, literal_constraint, mfp) => {
                let coll = ctx
                    .humanizer
                    .humanize_id(*global_id)
                    .unwrap_or_else(|| global_id.to_string());
                writeln!(f, "{}→Map/Filter/Project", ctx.indent)?;
                ctx.indent.set();

                ctx.indent += 1;

                mode.expr(mfp.deref(), None).fmt_default_text(f, ctx)?;
                let printed = !mfp.expressions.is_empty() || !mfp.predicates.is_empty();

                if printed {
                    ctx.indent += 1;
                }
                if let Some(literal_constraint) = literal_constraint {
                    writeln!(f, "{}→ReadStorage Lookup on {coll}", ctx.indent)?;
                    ctx.indent += 1;
                    let value = mode.expr(literal_constraint, None);
                    writeln!(f, "{}Lookup value: {value}", ctx.indent)?;
                } else {
                    writeln!(f, "{}→ReadStorage {coll}", ctx.indent)?;
                }

                ctx.indent.reset();
            }
        }

        Ok(())
    }

    pub fn fmt_verbose_text<'a, T>(
        &self,
        f: &mut fmt::Formatter<'_>,
        ctx: &mut PlanRenderingContext<'a, T>,
    ) -> fmt::Result {
        let redacted = ctx.config.redacted;
        let mode = HumanizedExplain::new(redacted);

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
            FastPathPlan::PeekPersist(gid, literal_constraint, mfp) => {
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
                write!(f, "{}PeekPersist {human_id}", ctx.as_mut())?;
                if let Some(literal) = literal_constraint {
                    let value = mode.expr(literal, None);
                    writeln!(f, " [value={}]", value)?;
                } else {
                    writeln!(f, "")?;
                }
                ctx.as_mut().reset();
                Ok(())
            }
        }?;
        Ok(())
    }
}

/// Possible ways in which the coordinator could produce the result for a goal view.
#[derive(Clone, Debug)]
pub enum PeekPlan {
    FastPath(FastPathPlan),
    /// The view must be installed as a dataflow and then read.
    SlowPath(PeekDataflowPlan),
}

/// Convert `mfp` to an executable, non-temporal plan.
/// It should be non-temporal, as OneShot preparation populates `mz_now`.
///
/// If the `mfp` can't be converted into a non-temporal plan, this returns an _internal_ error.
fn mfp_to_safe_plan(
    mfp: mz_expr::MapFilterProject,
) -> Result<mz_expr::SafeMfpPlan, OptimizerError> {
    mfp.into_plan()
        .map_err(OptimizerError::InternalUnsafeMfpPlan)?
        .into_nontemporal()
        .map_err(|e| OptimizerError::InternalUnsafeMfpPlan(format!("{:?}", e)))
}

/// If it can't convert `mfp` into a `SafeMfpPlan`, this returns an _internal_ error.
fn permute_oneshot_mfp_around_index(
    mfp: mz_expr::MapFilterProject,
    key: &[MirScalarExpr],
) -> Result<mz_expr::SafeMfpPlan, OptimizerError> {
    let input_arity = mfp.input_arity;
    let mut safe_mfp = mfp_to_safe_plan(mfp)?;
    let (permute, thinning) = permutation_for_arrangement(key, input_arity);
    safe_mfp.permute_fn(|c| permute[c], key.len() + thinning.len());
    Ok(safe_mfp)
}

/// Determine if the dataflow plan can be implemented without an actual dataflow.
///
/// If the optimized plan is a `Constant` or a `Get` of a maintained arrangement,
/// we can avoid building a dataflow (and either just return the results, or peek
/// out of the arrangement, respectively).
pub fn create_fast_path_plan(
    dataflow_plan: &mut DataflowDescription<OptimizedMirRelationExpr>,
    view_id: GlobalId,
    finishing: Option<&RowSetFinishing>,
    persist_fast_path_limit: usize,
    persist_fast_path_order: bool,
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
            let plan = FastPathPlan::Constant(
                rows.clone(),
                mz_repr::SqlRelationType::from_repr(found_typ),
            );
            return Ok(Some(plan));
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
                        // The following is roughly `limit >= finishing.limit + finishing.offset`,
                        // but with Options.
                        let finishing_limits_at_least_as_topk = match (limit, finishing.limit) {
                            (None, _) => true,
                            (Some(..), None) => false,
                            (Some(topk_limit), Some(finishing_limit)) => {
                                if let Some(l) = topk_limit.as_literal_int64() {
                                    i128::cast_from(l)
                                        >= i128::cast_from(*finishing_limit)
                                            + i128::cast_from(finishing.offset)
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
                    typ: repr_typ,
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

                    // If there is no arrangement, consider peeking the persist shard directly.
                    // Generally, we consider a persist peek when the query can definitely be satisfied
                    // by scanning through a small, constant number of Persist key-values.
                    let safe_mfp = mfp_to_safe_plan(mfp)?;
                    let (_maps, filters, projection) = safe_mfp.as_map_filter_project();

                    let persist_fast_path_order_relation_typ = if persist_fast_path_order {
                        Some(
                            dataflow_plan
                                .source_imports
                                .get(get_id)
                                .expect("Get's ID is also imported")
                                .desc
                                .typ
                                .clone(),
                        )
                    } else {
                        None
                    };

                    let literal_constraint =
                        if let Some(relation_typ) = &persist_fast_path_order_relation_typ {
                            let mut row = Row::default();
                            let mut packer = row.packer();
                            for (idx, col) in relation_typ.column_types.iter().enumerate() {
                                if !preserves_order(&col.scalar_type) {
                                    break;
                                }
                                let col_expr = MirScalarExpr::column(idx);

                                let Some((literal, _)) = filters
                                    .iter()
                                    .filter_map(|f| f.expr_eq_literal(&col_expr))
                                    .next()
                                else {
                                    break;
                                };
                                packer.extend_by_row(&literal);
                            }
                            if row.is_empty() { None } else { Some(row) }
                        } else {
                            None
                        };

                    let finish_ok = match &finishing {
                        None => false,
                        Some(RowSetFinishing {
                            order_by,
                            limit,
                            offset,
                            ..
                        }) => {
                            let order_ok =
                                if let Some(relation_typ) = &persist_fast_path_order_relation_typ {
                                    order_by.iter().enumerate().all(|(idx, order)| {
                                        // Map the ordering column back to the column in the source data.
                                        // (If it's not one of the input columns, we can't make any guarantees.)
                                        let column_idx = projection[order.column];
                                        if column_idx >= safe_mfp.input_arity {
                                            return false;
                                        }
                                        let column_type = &relation_typ.column_types[column_idx];
                                        let index_ok = idx == column_idx;
                                        let nulls_ok = !column_type.nullable || order.nulls_last;
                                        let asc_ok = !order.desc;
                                        let type_ok = preserves_order(&column_type.scalar_type);
                                        index_ok && nulls_ok && asc_ok && type_ok
                                    })
                                } else {
                                    order_by.is_empty()
                                };
                            let limit_ok = limit.map_or(false, |l| {
                                usize::cast_from(l) + *offset < persist_fast_path_limit
                            });
                            order_ok && limit_ok
                        }
                    };

                    let key_constraint = if let Some(literal) = &literal_constraint {
                        let prefix_len = literal.iter().count();
                        repr_typ
                            .keys
                            .iter()
                            .any(|k| k.iter().all(|idx| *idx < prefix_len))
                    } else {
                        false
                    };

                    // We can generate a persist peek when:
                    // - We have a literal constraint that includes an entire key (so we'll return at most one value)
                    // - We can return the first N key values (no filters, small limit, consistent order)
                    if key_constraint || (filters.is_empty() && finish_ok) {
                        return Ok(Some(FastPathPlan::PeekPersist(
                            *get_id,
                            literal_constraint,
                            safe_mfp,
                        )));
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
    /// Returns a [`PeekClient`] for coordinator-owned queries, which have to
    /// run off the main loop because the client calls back into it.
    ///
    /// The client holds no session [`Client`](crate::Client), so it does not
    /// keep the coordinator alive.
    pub(crate) fn background_peek_client(&self, catalog: &Arc<Catalog>) -> PeekClient {
        let build_version = catalog.state().config().build_info.human_version(None);
        PeekClient::new(
            CoordinatorClient::Background {
                tx: self.internal_cmd_tx.clone(),
                metrics: self.metrics.clone(),
            },
            catalog,
            Arc::clone(&self.controller.storage_collections),
            Arc::clone(&self.transient_id_gen),
            self.optimizer_metrics.clone(),
            self.persist_client.clone(),
            self.statement_logging.create_frontend(build_version),
            Arc::clone(&self.occ_write_semaphore),
            self.group_commit_tx.clone(),
            self.controller.read_only(),
        )
    }

    /// Cancel and remove all pending peeks that were initiated by the client with `conn_id`.
    #[mz_ore::instrument(level = "debug")]
    pub(crate) fn cancel_pending_peeks(&mut self, conn_id: &ConnectionId) {
        if let Some(uuids) = self.client_pending_peeks.remove(conn_id) {
            self.metrics
                .canceled_peeks
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
                self.retire_execution(
                    StatementEndedExecutionReason::Canceled,
                    peek.ctx_extra.defuse(),
                );
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
            self.retire_execution(reason, ctx_extra.defuse());
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

    /// Implements a slow-path peek: ships its transient dataflow, peeks the
    /// dataflow's index, and registers the pending peek. This is called from the
    /// command handler for `ExecuteSlowPathPeek`.
    ///
    /// On an error return nothing is registered, and the caller logs the
    /// statement's end. Once registered, `handle_peek_notification` logs it.
    pub(crate) fn implement_slow_path_peek(
        &mut self,
        dataflow_plan: PeekDataflowPlan,
        determination: TimestampDetermination,
        finishing: RowSetFinishing,
        compute_instance: ComputeInstanceId,
        target_replica: Option<ReplicaId>,
        intermediate_result_type: SqlRelationType,
        source_ids: BTreeSet<GlobalId>,
        conn_id: ConnectionId,
        max_result_size: u64,
        max_query_result_size: Option<u64>,
        watch_set: Option<WatchSetCreation>,
    ) -> Result<ExecuteResponse, AdapterError> {
        // Install watch sets for statement lifecycle logging if enabled.
        // This must happen _before_ creating ExecuteContextExtra, so that if it fails,
        // we don't have an ExecuteContextExtra that needs to be retired (the frontend
        // will handle logging for the error case).
        let statement_logging_id = watch_set.as_ref().map(|ws| ws.logging_id);
        if let Some(ws) = watch_set {
            self.install_peek_watch_sets(conn_id.clone(), ws)
                .map_err(|e| {
                    AdapterError::concurrent_dependency_drop_from_watch_set_install_error(e)
                })?;
        }

        let source_arity = intermediate_result_type.arity();
        let timestamp = determination.timestamp_context.timestamp_or_default();

        let PeekDataflowPlan {
            desc: dataflow,
            // n.b. this index_id identifies a transient index the
            // caller created, so it is guaranteed to be on
            // `compute_instance`.
            id: index_id,
            key: index_key,
            permutation: index_permutation,
            thinned_arity: index_thinned_arity,
        } = dataflow_plan;

        // The read-hold strategy below acquires a hold for `index_id` only. That
        // is sufficient today because slow-path peek dataflows have a single
        // export equal to `index_id`. If we ever ship multi-output dataflows on
        // this path, the hold acquisition needs to be revisited.
        let exports: Vec<GlobalId> = dataflow.export_ids().collect();
        soft_assert_eq_or_log!(
            exports.as_slice(),
            &[index_id],
            "slow-path peek dataflow must export exactly [index_id]",
        );
        if exports.as_slice() != [index_id] {
            return Err(AdapterError::internal(
                "peek error",
                format!("slow-path peek dataflow exports {exports:?}, expected [{index_id}]",),
            ));
        }

        // Ship the dataflow, then acquire a read hold for the peek target so its
        // `since` cannot advance past `timestamp` before `compute.peek()` runs: the
        // implied hold from `create_dataflow` pins the new collection's `since` at
        // `as_of`, so the subsequent `acquire_read_hold` lands at
        // `as_of <= timestamp`.
        self.controller
            .compute
            .create_dataflow(compute_instance, dataflow, None)
            .map_err(AdapterError::concurrent_dependency_drop_from_dataflow_creation_error)?;

        // On failure we must drop the dataflow ourselves, otherwise it leaks.
        let acquire_result = self
            .controller
            .compute
            .acquire_read_hold(compute_instance, index_id)
            .map_err(AdapterError::concurrent_dependency_drop_from_collection_update_error);
        let read_hold = match acquire_result {
            Ok(hold) => hold,
            Err(e) => {
                self.drop_compute_collections(vec![(compute_instance, index_id)]);
                return Err(e);
            }
        };

        // Create an identity MFP operator.
        let mut map_filter_project = mz_expr::MapFilterProject::new(source_arity);
        map_filter_project.permute_fn(
            |c| index_permutation[c],
            index_key.len() + index_thinned_arity,
        );
        let map_filter_project = mfp_to_safe_plan(map_filter_project)?;

        // Endpoints for sending and receiving peek responses.
        let (rows_tx, rows_rx) = tokio::sync::oneshot::channel();

        // Generate unique UUID. Guaranteed to be unique to all pending peeks, there's an very
        // small but unlikely chance that it's not unique to completed peeks.
        let mut uuid = Uuid::new_v4();
        while self.pending_peeks.contains_key(&uuid) {
            uuid = Uuid::new_v4();
        }

        // At this stage we don't know column names for the result because we
        // only know the peek's result type as a bare SqlRelationType.
        let peek_result_column_names =
            (0..intermediate_result_type.arity()).map(|i| format!("peek_{i}"));
        let peek_result_desc =
            RelationDesc::new(intermediate_result_type, peek_result_column_names);

        let peek_result = self
            .controller
            .compute
            .peek(
                compute_instance,
                PeekTarget::Index { id: index_id },
                None,
                uuid,
                timestamp,
                peek_result_desc,
                finishing.clone(),
                map_filter_project,
                read_hold,
                target_replica,
                rows_tx,
            )
            .map_err(AdapterError::concurrent_dependency_drop_from_peek_error);
        if let Err(e) = peek_result {
            // Drop the transient dataflow shipped above to avoid leaking it.
            self.drop_compute_collections(vec![(compute_instance, index_id)]);
            return Err(e);
        }

        // Register the pending peek only after compute.peek() succeeds. If it
        // fails (e.g. concurrent replica/cluster drop), inserting first would
        // leak entries in these maps and misattribute statement execution reasons.
        self.pending_peeks.insert(
            uuid,
            PendingPeek {
                conn_id: conn_id.clone(),
                cluster_id: compute_instance,
                depends_on: source_ids,
                ctx_extra: ExecuteContextGuard::new(
                    statement_logging_id,
                    self.internal_cmd_tx.clone(),
                ),
                is_fast_path: false,
            },
        );
        self.client_pending_peeks
            .entry(conn_id)
            .or_default()
            .insert(uuid, compute_instance);

        let duration_histogram = self.metrics.row_set_finishing_seconds();

        // Drop the dataflow now that the peek is queued. This is required:
        // `add_collection` installs implied/warmup holds owned by the controller
        // and only released via `drop_collections`, so without this call the
        // transient dataflow's `since` would stay pinned at `as_of` forever. The
        // peek's own read hold keeps the collection alive on the cluster until
        // the response arrives.
        self.drop_compute_collections(vec![(compute_instance, index_id)]);

        let persist_client = self.persist_client.clone();
        let peek_stash_read_batch_size_bytes =
            mz_compute_types::dyncfgs::PEEK_RESPONSE_STASH_READ_BATCH_SIZE_BYTES
                .get(self.catalog().system_config().dyncfgs());
        let peek_stash_read_memory_budget_bytes =
            mz_compute_types::dyncfgs::PEEK_RESPONSE_STASH_READ_MEMORY_BUDGET_BYTES
                .get(self.catalog().system_config().dyncfgs());

        let peek_response_stream = crate::peek_client::create_peek_response_stream(
            rows_rx,
            finishing,
            max_result_size,
            max_query_result_size,
            duration_histogram,
            persist_client,
            peek_stash_read_batch_size_bytes,
            peek_stash_read_memory_budget_bytes,
        );

        Ok(crate::ExecuteResponse::SendingRowsStreaming {
            rows: Box::pin(peek_response_stream),
            instance_id: compute_instance,
            strategy: StatementExecutionStrategy::Standard,
        })
    }

    /// Implements a `COPY TO` command by installing peek watch sets,
    /// shipping the dataflow, and spawning a background task to wait for completion.
    /// This is called from the command handler for ExecuteCopyTo.
    ///
    /// (The S3 preflight check must be completed successfully via the
    /// `CopyToPreflight` command _before_ calling this method. The preflight is
    /// handled separately to avoid blocking the coordinator's main task with
    /// slow S3 network operations.)
    ///
    /// This method does NOT block waiting for completion. Instead, it spawns a background task that
    /// will send the response through the provided tx channel when the COPY TO completes.
    /// All errors (setup or execution) are sent through tx.
    pub(crate) async fn implement_copy_to(
        &mut self,
        df_desc: DataflowDescription<mz_compute_types::plan::LirRelationExpr>,
        compute_instance: ComputeInstanceId,
        target_replica: Option<ReplicaId>,
        source_ids: BTreeSet<GlobalId>,
        conn_id: ConnectionId,
        watch_set: Option<WatchSetCreation>,
        tx: oneshot::Sender<Result<ExecuteResponse, AdapterError>>,
    ) {
        // Helper to send error and return early
        let send_err = |tx: oneshot::Sender<Result<ExecuteResponse, AdapterError>>,
                        e: AdapterError| {
            let _ = tx.send(Err(e));
        };

        // Install watch sets for statement lifecycle logging if enabled.
        // If this fails, we just send the error back. The frontend will handle logging
        // for the error case (no ExecuteContextExtra is created here).
        if let Some(ws) = watch_set {
            if let Err(e) = self.install_peek_watch_sets(conn_id.clone(), ws) {
                let err = AdapterError::concurrent_dependency_drop_from_watch_set_install_error(e);
                send_err(tx, err);
                return;
            }
        }

        // Note: We don't create an ExecuteContextExtra here because the frontend handles
        // all statement logging for COPY TO operations.

        let sink_id = df_desc.sink_id();

        // Create and register ActiveCopyTo.
        // Note: sink_tx/sink_rx is the channel for the compute sink to notify completion
        // This is different from the command's tx which sends the response to the client
        let (sink_tx, sink_rx) = oneshot::channel();
        let active_copy_to = ActiveCopyTo {
            conn_id: conn_id.clone(),
            tx: sink_tx,
            cluster_id: compute_instance,
            depends_on: source_ids,
        };

        // Add metadata for the new COPY TO. CopyTo returns a `ready` future, so it is safe to drop.
        drop(self.add_active_compute_sink(sink_id, ActiveComputeSink::CopyTo(active_copy_to)));

        // Try to ship the dataflow. We handle errors gracefully because dependencies might have
        // disappeared during sequencing.
        if let Err(e) = self
            .try_ship_dataflow(df_desc, compute_instance, target_replica)
            .await
            .map_err(AdapterError::concurrent_dependency_drop_from_dataflow_creation_error)
        {
            // Clean up the active compute sink that was added above, since the dataflow was never
            // created. If we don't do this, the sink_id remains in drop_sinks but no collection
            // exists in the compute controller, causing a panic when the connection terminates.
            self.remove_active_compute_sink(sink_id).await;
            send_err(tx, e);
            return;
        }

        // Spawn background task to wait for completion
        // We must NOT await sink_rx here directly, as that would block the coordinator's main task
        // from processing the completion message. Instead, we spawn a background task that will
        // send the result through tx when the COPY TO completes.
        let span = Span::current();
        task::spawn(
            || "copy to completion",
            async move {
                let res = sink_rx.await;
                let result = match res {
                    Ok(res) => res,
                    Err(_) => Err(AdapterError::Internal("copy to sender dropped".into())),
                };

                let _ = tx.send(result);
            }
            .instrument(span),
        );
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
    use mz_repr::{Datum, SqlColumnType, SqlScalarType};

    use super::*;

    #[mz_ore::test]
    #[cfg_attr(miri, ignore)] // unsupported operation: can't call foreign function `rust_psm_stack_pointer` on OS `linux`
    fn test_fast_path_plan_as_text() {
        let typ = SqlRelationType::new(vec![SqlColumnType {
            scalar_type: SqlScalarType::String,
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
            verbose_syntax: true,
            ..Default::default()
        };
        let ctx_gen = || {
            let indent = Indent::default();
            let annotations = BTreeMap::new();
            PlanRenderingContext::<FastPathPlan>::new(
                indent,
                &humanizer,
                annotations,
                &config,
                BTreeSet::default(),
            )
        };

        let constant_err_exp = "Error \"division by zero\"\n";
        let no_lookup_exp = "Project (#1, #4)\n  Map ((#0 OR #2))\n    ReadIndex on=u8 [DELETED INDEX]=[*** full scan ***]\n";
        let lookup_exp =
            "Filter (#0) IS NULL\n  ReadIndex on=u9 [DELETED INDEX]=[lookup value=(5)]\n";

        assert_eq!(text_string_at(&constant_err, ctx_gen), constant_err_exp);
        assert_eq!(text_string_at(&no_lookup, ctx_gen), no_lookup_exp);
        assert_eq!(text_string_at(&lookup, ctx_gen), lookup_exp);

        let mut constant_rows = vec![
            (Row::pack(Some(Datum::String("hello"))), Diff::ONE),
            (Row::pack(Some(Datum::String("world"))), 2.into()),
            (Row::pack(Some(Datum::String("star"))), 500.into()),
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
        constant_rows
            .extend((0..20).map(|i| (Row::pack(Some(Datum::String(&i.to_string()))), Diff::ONE)));
        let constant_exp2 = "Constant\n  total_rows (diffs absed): 523\n  first_rows:\n    - (\"hello\")\
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

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
use std::{collections::HashMap, num::NonZeroUsize};

use futures::{FutureExt, StreamExt};
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;
use uuid::Uuid;

use mz_compute_client::command::{DataflowDescription, ReplicaId};
use mz_compute_client::controller::ComputeInstanceId;
use mz_compute_client::response::PeekResponse;
use mz_expr::{EvalError, Id, MirScalarExpr};
use mz_ore::tracing::OpenTelemetryContext;
use mz_repr::{Diff, GlobalId, Row};
use mz_stash::Append;

use crate::client::ConnectionId;
use crate::util::send_immediate_rows;
use crate::AdapterError;

pub(crate) struct PendingPeek {
    pub(crate) sender: mpsc::UnboundedSender<PeekResponse>,
    pub(crate) conn_id: ConnectionId,
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
pub struct PeekDataflowPlan<T> {
    desc: DataflowDescription<mz_compute_client::plan::Plan<T>, (), T>,
    id: GlobalId,
    key: Vec<MirScalarExpr>,
    permutation: HashMap<usize, usize>,
    thinned_arity: usize,
}

/// Possible ways in which the coordinator could produce the result for a goal view.
#[derive(Debug)]
pub enum Plan<T = mz_repr::Timestamp> {
    /// The view evaluates to a constant result that can be returned.
    Constant(Result<Vec<(Row, T, Diff)>, EvalError>),
    /// The view can be read out of an existing arrangement.
    PeekExisting(GlobalId, Option<Row>, mz_expr::SafeMfpPlan),
    /// The view must be installed as a dataflow and then read.
    PeekDataflow(PeekDataflowPlan<T>),
}

/// Determine if the dataflow plan can be implemented without an actual dataflow.
///
/// If the optimized plan is a `Constant` or a `Get` of a maintained arrangement,
/// we can avoid building a dataflow (and either just return the results, or peek
/// out of the arrangement, respectively).
pub fn create_plan(
    dataflow_plan: DataflowDescription<mz_compute_client::plan::Plan>,
    view_id: GlobalId,
    index_id: GlobalId,
    index_key: Vec<MirScalarExpr>,
    index_permutation: HashMap<usize, usize>,
    index_thinned_arity: usize,
) -> Result<Plan, AdapterError> {
    // At this point, `dataflow_plan` contains our best optimized dataflow.
    // We will check the plan to see if there is a fast path to escape full dataflow construction.

    // We need to restrict ourselves to settings where the inserted transient view is the first thing
    // to build (no dependent views). There is likely an index to build as well, but we may not be sure.
    if dataflow_plan.objects_to_build.len() >= 1 && dataflow_plan.objects_to_build[0].id == view_id
    {
        match &dataflow_plan.objects_to_build[0].plan {
            // In the case of a constant, we can return the result now.
            mz_compute_client::plan::Plan::Constant { rows } => {
                return Ok(Plan::Constant(rows.clone()));
            }
            // In the case of a bare `Get`, we may be able to directly index an arrangement.
            mz_compute_client::plan::Plan::Get { id, keys, plan } => {
                match plan {
                    mz_compute_client::plan::GetPlan::PassArrangements => {
                        // An arrangement may or may not exist. If not, nothing to be done.
                        if let Some((key, permute, thinning)) = keys.arbitrary_arrangement() {
                            // Just grab any arrangement, but be sure to de-permute the results.
                            for (index_id, (desc, _typ, _monotonic)) in
                                dataflow_plan.index_imports.iter()
                            {
                                if Id::Global(desc.on_id) == *id && &desc.key == key {
                                    let mut map_filter_project =
                                        mz_expr::MapFilterProject::new(_typ.arity())
                                            .into_plan()
                                            .unwrap()
                                            .into_nontemporal()
                                            .unwrap();
                                    map_filter_project
                                        .permute(permute.clone(), key.len() + thinning.len());
                                    return Ok(Plan::PeekExisting(
                                        *index_id,
                                        None,
                                        map_filter_project,
                                    ));
                                }
                            }
                        }
                    }
                    mz_compute_client::plan::GetPlan::Arrangement(key, val, mfp) => {
                        // Convert `mfp` to an executable, non-temporal plan.
                        // It should be non-temporal, as OneShot preparation populates `mz_logical_timestamp`.
                        let map_filter_project = mfp
                            .clone()
                            .into_plan()
                            .map_err(|e| {
                                crate::error::AdapterError::Unstructured(::anyhow::anyhow!(e))
                            })?
                            .into_nontemporal()
                            .map_err(|_e| {
                                crate::error::AdapterError::Unstructured(::anyhow::anyhow!(
                                    "OneShot plan has temporal constraints"
                                ))
                            })?;
                        // We should only get excited if we can track down an index for `id`.
                        // If `keys` is non-empty, that means we think one exists.
                        for (index_id, (desc, _typ, _monotonic)) in
                            dataflow_plan.index_imports.iter()
                        {
                            if Id::Global(desc.on_id) == *id && &desc.key == key {
                                // Indicate an early exit with a specific index and key_val.
                                return Ok(Plan::PeekExisting(
                                    *index_id,
                                    val.clone(),
                                    map_filter_project,
                                ));
                            }
                        }
                    }
                    mz_compute_client::plan::GetPlan::Collection(_) => {
                        // No arrangement, so nothing to be done here.
                    }
                }
            }
            // nothing can be done for non-trivial expressions.
            _ => {}
        }
    }
    return Ok(Plan::PeekDataflow(PeekDataflowPlan {
        desc: dataflow_plan,
        id: index_id,
        key: index_key,
        permutation: index_permutation,
        thinned_arity: index_thinned_arity,
    }));
}

impl<S: Append + 'static> crate::coord::Coordinator<S> {
    /// Implements a peek plan produced by `create_plan` above.
    #[tracing::instrument(level = "debug", skip(self))]
    pub async fn implement_fast_path_peek(
        &mut self,
        fast_path: Plan,
        timestamp: mz_repr::Timestamp,
        finishing: mz_expr::RowSetFinishing,
        conn_id: ConnectionId,
        source_arity: usize,
        compute_instance: ComputeInstanceId,
        target_replica: Option<ReplicaId>,
    ) -> Result<crate::ExecuteResponse, AdapterError> {
        // If the dataflow optimizes to a constant expression, we can immediately return the result.
        if let Plan::Constant(rows) = fast_path {
            let mut rows = match rows {
                Ok(rows) => rows,
                Err(e) => return Err(e.into()),
            };
            // retain exactly those updates less or equal to `timestamp`.
            for (_, time, diff) in rows.iter_mut() {
                use timely::PartialOrder;
                if time.less_equal(&timestamp) {
                    // clobber the timestamp, so consolidation occurs.
                    *time = timestamp.clone();
                } else {
                    // zero the difference, to prevent a contribution.
                    *diff = 0;
                }
            }
            // Consolidate down the results to get correct totals.
            differential_dataflow::consolidation::consolidate_updates(&mut rows);

            let mut results = Vec::new();
            for (row, _time, count) in rows {
                if count < 0 {
                    Err(EvalError::InvalidParameterValue(format!(
                        "Negative multiplicity in constant result: {}",
                        count
                    )))?
                };
                if count > 0 {
                    results.push((row, NonZeroUsize::new(count as usize).unwrap()));
                }
            }
            let results = finishing.finish(results);
            return Ok(send_immediate_rows(results));
        }

        // The remaining cases are a peek into a maintained arrangement, or building a dataflow.
        // In both cases we will want to peek, and the main difference is that we might want to
        // build a dataflow and drop it once the peek is issued. The peeks are also constructed
        // differently.

        // If we must build the view, ship the dataflow.
        let (peek_command, drop_dataflow) = match fast_path {
            Plan::PeekExisting(id, key, map_filter_project) => (
                (id, key, timestamp, finishing.clone(), map_filter_project),
                None,
            ),
            Plan::PeekDataflow(PeekDataflowPlan {
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
                    .compute_mut(compute_instance)
                    .unwrap()
                    .create_dataflows(vec![dataflow])
                    .await
                    .unwrap();
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
                        finishing.clone(),
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
        let (rows_tx, rows_rx) = tokio::sync::mpsc::unbounded_channel();

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
            },
        );
        self.client_pending_peeks
            .entry(conn_id)
            .or_default()
            .insert(uuid, compute_instance);
        let (id, key, timestamp, _finishing, map_filter_project) = peek_command;

        self.controller
            .compute_mut(compute_instance)
            .unwrap()
            .peek(
                id,
                key,
                uuid,
                timestamp,
                finishing.clone(),
                map_filter_project,
                target_replica,
            )
            .await
            .unwrap();

        // Prepare the receiver to return as a response.
        let rows_rx = tokio_stream::wrappers::UnboundedReceiverStream::new(rows_rx)
            .fold(PeekResponse::Rows(vec![]), |memo, resp| async {
                match (memo, resp) {
                    (PeekResponse::Rows(mut memo), PeekResponse::Rows(rows)) => {
                        memo.extend(rows);
                        PeekResponse::Rows(memo)
                    }
                    (PeekResponse::Error(e), _) | (_, PeekResponse::Error(e)) => {
                        PeekResponse::Error(e)
                    }
                    (PeekResponse::Canceled, _) | (_, PeekResponse::Canceled) => {
                        PeekResponse::Canceled
                    }
                }
            })
            .map(move |resp| match resp {
                PeekResponse::Rows(rows) => PeekResponseUnary::Rows(finishing.finish(rows)),
                PeekResponse::Canceled => PeekResponseUnary::Canceled,
                PeekResponse::Error(e) => PeekResponseUnary::Error(e),
            });

        // If it was created, drop the dataflow once the peek command is sent.
        if let Some(index_id) = drop_dataflow {
            self.remove_compute_ids_from_timeline(vec![(compute_instance, index_id)]);
            self.drop_indexes(vec![(compute_instance, index_id)]).await;
        }

        Ok(crate::ExecuteResponse::SendingRows {
            future: Box::pin(rows_rx),
            span: tracing::Span::current(),
        })
    }

    /// Cancel and remove all pending peeks that were initiated by the client with `conn_id`.
    #[tracing::instrument(level = "debug", skip(self))]
    pub(crate) async fn cancel_pending_peeks(&mut self, conn_id: u32) -> Vec<PendingPeek> {
        // The peek is present on some specific compute instance.
        // Allow dataflow to cancel any pending peeks.
        if let Some(uuids) = self.client_pending_peeks.remove(&conn_id) {
            let mut inverse: BTreeMap<ComputeInstanceId, BTreeSet<Uuid>> = Default::default();
            for (uuid, compute_instance) in &uuids {
                inverse.entry(*compute_instance).or_default().insert(*uuid);
            }
            for (compute_instance, uuids) in inverse {
                self.controller
                    .compute_mut(compute_instance)
                    .unwrap()
                    .cancel_peeks(&uuids)
                    .await
                    .unwrap();
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
            conn_id,
        }) = self.remove_pending_peek(&uuid)
        {
            otel_ctx.attach_as_parent();
            // Peek cancellations are best effort, so we might still
            // receive a response, even though the recipient is gone.
            let _ = rows_tx.send(response);
            if let Some(uuids) = self.client_pending_peeks.get_mut(&conn_id) {
                uuids.remove(&uuid);
                if uuids.is_empty() {
                    self.client_pending_peeks.remove(&conn_id);
                }
            }
        }
        // Cancellation may cause us to receive responses for peeks no
        // longer in `self.pending_peeks`, so we quietly ignore them.
    }

    /// Clean up a peek's state.
    pub(crate) fn remove_pending_peek(&mut self, uuid: &Uuid) -> Option<PendingPeek> {
        self.pending_peeks.remove(uuid)
    }
}

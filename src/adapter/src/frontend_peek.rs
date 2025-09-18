// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::catalog::CatalogState;
use crate::coord::peek::{FastPathPlan, PeekPlan};
use crate::coord::timestamp_selection::TimestampDetermination;
use crate::coord::{
    Coordinator, ExplainContext,
    TargetCluster, catalog_serving,
};
use crate::optimize::Optimize;
use crate::optimize::dataflows::{ComputeInstanceSnapshot, DataflowBuilder};
use crate::session::{Session, TransactionOps, TransactionStatus};
use crate::{
    AdapterError, CollectionIdBundle, ExecuteResponse, ReadHolds, SessionClient,
    TimelineContext, TimestampContext, TimestampProvider, optimize,
};
use mz_adapter_types::dyncfgs::{
    CONSTRAINT_BASED_TIMESTAMP_SELECTION,
};
use mz_adapter_types::timestamp_selection::ConstraintBasedTimestampSelection;
use mz_compute_types::ComputeInstanceId;
use mz_expr::CollectionPlan;
use mz_repr::optimize::OverrideFrom;
use mz_repr::{GlobalId, Timestamp};
use mz_sql::catalog::CatalogCluster;
use mz_sql::plan::{Plan, QueryWhen};
use mz_sql::rbac;
use mz_sql::session::metadata::SessionMetadata;
use mz_sql::session::vars::IsolationLevel;
use mz_transform::EmptyStatisticsOracle;
use std::collections::BTreeMap;
use std::sync::Arc;
use timely::progress::Antichain;
use tracing::Span;
use mz_compute_client::controller::error::CollectionMissing;
use mz_ore::collections::CollectionExt;
////// todo: separate crate imports

impl SessionClient {
    pub(crate) async fn try_frontend_peek_inner(
        &mut self,
        portal_name: &str,
        session: &mut Session,
    ) -> Result<Option<ExecuteResponse>, AdapterError> {
        // # From handle_execute

        /////// todo: if session.vars().emit_trace_id_notice()

        /////// todo: I think we don't need the verify_portal here, but we need to verify things later

        //////let portal = self.session().get_portal_unverified(portal_name).expect("known to exist");

        // This is from handle_execute_inner, but we do it already here because of lifetime issues.
        //////// todo: would be good to not do this if this is not a SELECT
        let catalog = self.catalog_snapshot("try_frontend_peek").await;

        ///////// todo: statement logging
        let (stmt, params) = {
            let portal = session
                .get_portal_unverified(&portal_name)
                .expect("known to exist");
            let params = portal.parameters.clone();
            let stmt = portal.stmt.clone();
            (stmt, params)
        };

        let stmt = match stmt {
            Some(stmt) => stmt,
            None => {
                println!("try_frontend_peek_inner succeeded on an empty query");
                return Ok(Some(ExecuteResponse::EmptyQuery));
            }
        };

        ////// todo: metrics .query_total  (but only if we won't bail out somewhere below to the old code)

        // # From handle_execute_inner

        let conn_catalog = catalog.for_session(&session);
        // `resolved_ids` should be derivable from `stmt`. If `stmt` is transformed to remove/add
        // IDs, then `resolved_ids` should be updated to also remove/add those IDs.
        let (stmt, resolved_ids) = mz_sql::names::resolve(&conn_catalog, (*stmt).clone())?;

        let pcx = session.pcx();
        let plan = mz_sql::plan::plan(Some(pcx), &conn_catalog, stmt, &params, &resolved_ids)?;
        let select_plan = match &plan {
            Plan::Select(select_plan) => select_plan,
            _ => {
                println!("Bailing out from try_frontend_peek_inner, because it's not a SELECT");
                return Ok(None);
            }
        };
        let explain_ctx = ExplainContext::None; // EXPLAIN won't be handled here, only SELECT

        // # From sequence_plan

        ///////// todo: maybe do the plan.allowed_in_read_only() check just to be safe

        ///////// todo: definitely do the waiting_on_startup_appends check

        let target_cluster = match session.transaction().cluster() {
            // Use the current transaction's cluster.
            Some(cluster_id) => TargetCluster::Transaction(cluster_id),
            // If there isn't a current cluster set for a transaction, then try to auto route.
            None => crate::coord::catalog_serving::auto_run_on_catalog_server(
                &conn_catalog,
                &session,
                &plan,
            ),
        };
        let (cluster, target_cluster_id, target_cluster_name) = {
            let cluster = catalog.resolve_target_cluster(target_cluster, &session)?;
            (cluster, cluster.id.clone(), cluster.name.clone()) /////// todo: or just refs instead of clones?
        };
        self.ensure_compute_instance_client(target_cluster_id).await?;

        ////// todo: statement logging: set_statement_execution_cluster

        if let Err(e) =
            catalog_serving::check_cluster_restrictions(&target_cluster_name, &conn_catalog, &plan)
        {
            return Err(e);
        }

        if let Err(e) = rbac::check_plan(
            &conn_catalog,
            |_id| {
                // This is only used by `Plan::SideEffectingFunc`, so it is irrelevant for us here
                /////// todo: refactor `check_plan` to make this nicer
                unreachable!()
            },
            session,
            &plan,
            Some(target_cluster_id),
            &resolved_ids,
        ) {
            return Err(e.into());
        }

        let max_query_result_size = Some(session.vars().max_query_result_size());

        // # From sequence_peek

        if session.vars().emit_plan_insights_notice() {
            /////////// todo: later
            println!("Bailing out from try_frontend_peek_inner, because emit_plan_insights_notice");
            return Ok(None);
        }

        // # From peek_validate

        //let compute_instance_snapshot = self.peek_client().snapshot(cluster.id()).await.unwrap();
        let compute_instance_snapshot = ComputeInstanceSnapshot::new_without_collections(cluster.id());

        let (_, view_id) = self.peek_client().transient_id_gen.allocate_id();
        let (_, index_id) = self.peek_client().transient_id_gen.allocate_id();

        let optimizer_config = optimize::OptimizerConfig::from(catalog.system_config())
            .override_from(&catalog.get_cluster(cluster.id()).config.features());

        if cluster.replicas().next().is_none() {
            return Err(AdapterError::NoClusterReplicasAvailable {
                name: cluster.name.clone(),
                is_managed: cluster.is_managed(),
            });
        }

        let mut optimizer = optimize::peek::Optimizer::new(
            Arc::clone(&catalog),
            compute_instance_snapshot.clone(),
            select_plan.finishing.clone(),
            view_id,
            index_id,
            optimizer_config,
            self.peek_client().optimizer_metrics.clone(),
        );

        let target_replica_name = session.vars().cluster_replica();
        let mut target_replica = target_replica_name
            .map(|name| {
                cluster
                    .replica_id(name)
                    .ok_or(AdapterError::UnknownClusterReplica {
                        cluster_name: cluster.name.clone(),
                        replica_name: name.to_string(),
                    })
            })
            .transpose()?;

        //////// todo: find better names for these dependency sets
        // dimensions:
        // - whether it include indexes
        // - whether its transitive through views?
        let source_ids = select_plan.source.depends_on();
        // todo: validate_timeline_context can be expensive in real scenarios (not in simple
        // benchmarks), because it traverses transitive dependencies even of indexed views and
        // materialized views (also traversing their MIR plans).
        let mut timeline_context = catalog.validate_timeline_context(source_ids.iter().copied())?;
        if matches!(timeline_context, TimelineContext::TimestampIndependent)
            && select_plan.source.contains_temporal()?
        {
            // If the source IDs are timestamp independent but the query contains temporal functions,
            // then the timeline context needs to be upgraded to timestamp dependent. This is
            // required because `source_ids` doesn't contain functions.
            timeline_context = TimelineContext::TimestampDependent;
        }

        let notices = crate::coord::sequencer::inner::check_log_reads(
            &catalog,
            cluster,
            &source_ids,
            &mut target_replica,
            session.vars(),
        )?;
        session.add_notices(notices);

        // # From peek_linearize_timestamp

        let isolation_level = session.vars().transaction_isolation().clone();
        let timeline = Coordinator::get_timeline(&timeline_context);
        let needs_linearized_read_ts =
            Coordinator::needs_linearized_read_ts(&isolation_level, &select_plan.when);

        let oracle_read_ts = match timeline {
            Some(timeline) if needs_linearized_read_ts => {
                let oracle = self
                    .peek_client()
                    .oracles
                    .get(&timeline)
                    .expect("/////// todo");
                let oracle_read_ts = oracle.read_ts().await;
                Some(oracle_read_ts)
            }
            Some(_) | None => None,
        };

        // # From peek_real_time_recency

        /////// todo: do we want to handle the real-time recency case?
        let vars = session.vars();
        if vars.real_time_recency()
            && vars.transaction_isolation() == &IsolationLevel::StrictSerializable
            && !session.contains_read_timestamp()
        {
            println!("Bailing out from try_frontend_peek_inner, because of real time recency");
            return Ok(None);
        }
        let real_time_recency_ts: Option<mz_repr::Timestamp> = None;

        // # From peek_timestamp_read_hold

        let dataflow_builder = DataflowBuilder::new(catalog.state(), compute_instance_snapshot);
        let input_id_bundle = dataflow_builder.sufficient_collections(source_ids.clone());

        let timestamp_provider = FrontendPeekTimestampProvider {
            session_client: &self,
            catalog_state: &catalog.state(),
        };

        // ## From sequence_peek_timestamp

        let in_immediate_multi_stmt_txn = session
            .transaction()
            .in_immediate_multi_stmt_txn(&select_plan.when);

        // Fetch or generate a timestamp for this query and what the read holds would be if we need to set
        // them.
        let (determination, read_holds) = match session.get_transaction_timestamp_determination() {
            // Use the transaction's timestamp if it exists and this isn't an AS OF query.
            Some(
                determination @ TimestampDetermination {
                    timestamp_context: TimestampContext::TimelineTimestamp { .. },
                    ..
                },
            ) if in_immediate_multi_stmt_txn => (determination, None),
            _ => {
                let determine_bundle = if in_immediate_multi_stmt_txn {
                    ////////// todo: needs timedomain_for, which needs DataflowBuilder / index oracle / sufficient_collections
                    println!(
                        "Bailing out from try_frontend_peek_inner, because of in_immediate_multi_stmt_txn"
                    );
                    return Ok(None);
                } else {
                    // If not in a transaction, use the source.
                    &input_id_bundle
                };
                let (determination, read_holds) = timestamp_provider.determine_timestamp(
                    session,
                    determine_bundle,
                    &select_plan.when,
                    target_cluster_id,
                    &timeline_context,
                    oracle_read_ts,
                    real_time_recency_ts,
                ).await?;
                // We only need read holds if the read depends on a timestamp.
                let read_holds = match determination.timestamp_context.timestamp() {
                    Some(_ts) => Some(read_holds),
                    None => {
                        // We don't need the read holds and shouldn't add them
                        // to the txn.
                        //
                        // TODO: Handle this within determine_timestamp.
                        drop(read_holds);
                        None
                    }
                };
                (determination, read_holds)
            }
        };

        // The old peek sequencing's sequence_peek_timestamp does two more things here:
        // the txn_read_holds stuff and session.add_transaction_ops. We do these later in the new
        // peek sequencing code, because at this point we might still bail out from the new peek
        // sequencing, in which case we don't want the mentioned side effects to happen.

        // # From peek_optimize

        // Generate data structures that can be moved to another task where we will perform possibly
        // expensive optimizations.
        let timestamp_context = determination.timestamp_context.clone();
        if session.vars().enable_session_cardinality_estimates() {
            tracing::error!(
                "Bailing out from try_frontend_peek_inner, because of enable_session_cardinality_estimates"
            );
            return Ok(None);
        }
        let stats = Box::new(EmptyStatisticsOracle);
        let session_meta = session.meta();
        let now = catalog.config().now.clone();
        let select_plan = select_plan.clone(); /////// todo: can we avoid cloning?

        ///////// todo: if explain_ctx.needs_plan_insights() ...

        let span = Span::current();

        let (global_lir_plan, optimization_finished_at) = mz_ore::task::spawn_blocking(
            || "optimize peek",
            move || {
                span.in_scope(|| {
                    let raw_expr = select_plan.source.clone();

                    // HIR ⇒ MIR lowering and MIR optimization (local)
                    let local_mir_plan = optimizer.catch_unwind_optimize(raw_expr)?;
                    // Attach resolved context required to continue the pipeline.
                    let local_mir_plan =
                        local_mir_plan.resolve(timestamp_context.clone(), &session_meta, stats);
                    // MIR optimization (global), MIR ⇒ LIR lowering, and LIR optimization (global)
                    let global_lir_plan = optimizer.catch_unwind_optimize(local_mir_plan)?;

                    let optimization_finished_at = now();

                    ///// todo: plan_insights stuff

                    Ok::<_, AdapterError>((global_lir_plan, optimization_finished_at))
                })
            },
        )
        .await
        .expect("///////// todo: handle JoinError")?;

        // # From peek_finish

        ///////// todo: statement logging

        let conn_id = session.conn_id().clone();

        let (peek_plan, df_meta, typ) = global_lir_plan.unapply();
        let source_arity = typ.arity();

        ////// todo: emit_optimizer_notices

        ////// todo: plan_insights stuff

        let fast_path_plan = match peek_plan {
            PeekPlan::SlowPath(_) => {
                println!("Bailing out from try_frontend_peek_inner, because it's a slow-path peek");
                return Ok(None);
            }
            PeekPlan::FastPath(p @ FastPathPlan::Constant(_, _))
            | PeekPlan::FastPath(p @ FastPathPlan::PeekExisting(_, _, _, _)) => {
                // ok, supported by PeekClient::implement_fast_path_peek_plan
                p
            }
            PeekPlan::FastPath(FastPathPlan::PeekPersist(_, _, _)) => {
                println!(
                    "Bailing out from try_frontend_peek_inner, because it's a Persist fast path peek"
                );
                return Ok(None);
            }
        };

        // # We do the second half of sequence_peek_timestamp, as mentioned above.

        //////// todo: txn_read_holds stuff. Work with SessionClient::txn_read_holds.

        // TODO: Checking for only `InTransaction` and not `Implied` (also `Started`?) seems
        // arbitrary and we don't recall why we did it (possibly an error!). Change this to always
        // set the transaction ops. Decide and document what our policy should be on AS OF queries.
        // Maybe they shouldn't be allowed in transactions at all because it's hard to explain
        // what's going on there. This should probably get a small design document.

        // We only track the peeks in the session if the query doesn't use AS
        // OF or we're inside an explicit transaction. The latter case is
        // necessary to support PG's `BEGIN` semantics, whose behavior can
        // depend on whether or not reads have occurred in the txn.
        let mut transaction_determination = determination.clone();
        let requires_linearization = (&explain_ctx).into();
        if select_plan.when.is_transactional() {
            session.add_transaction_ops(TransactionOps::Peeks {
                determination: transaction_determination,
                cluster_id: target_cluster_id,
                requires_linearization,
            })?;
        } else if matches!(session.transaction(), &TransactionStatus::InTransaction(_)) {
            // If the query uses AS OF, then ignore the timestamp.
            transaction_determination.timestamp_context = TimestampContext::NoTimestamp;
            session.add_transaction_ops(TransactionOps::Peeks {
                determination: transaction_determination,
                cluster_id: target_cluster_id,
                requires_linearization,
            })?;
        };

        // Warning: Do not bail out from the new peek sequencing after this point, because the above
        // had side effects.

        // # Now back to peek_finish

        //////// todo: statement logging

        let max_result_size = catalog.system_config().max_result_size();

        // Implement the peek, and capture the response.
        let resp = self
            .peek_client()
            .implement_fast_path_peek_plan(
                fast_path_plan,
                determination.timestamp_context.timestamp_or_default(),
                select_plan.finishing,
                target_cluster_id,
                target_replica,
                typ,
                max_result_size,
                max_query_result_size,
            )
            .await?;

        if session.vars().emit_timestamp_notice() {
            ////////// todo call Coordinator::explain_timestamp
            // let explanation =
            //     self.explain_timestamp(session, target_cluster_id, &input_id_bundle, determination);
            // session
            //     .add_notice(AdapterNotice::QueryTimestamp { explanation });
        }

        Ok(Some(resp))
    }
}

struct FrontendPeekTimestampProvider<'a> {
    session_client: &'a SessionClient,
    catalog_state: &'a CatalogState,
}

/////// todo: reorganize the traits:
// - There are things that are not possible and not needed to implement here.
// - There are asyncness mismatches.
impl TimestampProvider for FrontendPeekTimestampProvider<'_> {
    fn compute_read_frontier(
        &self,
        _instance: ComputeInstanceId,
        _id: GlobalId,
    ) -> Antichain<Timestamp> {
        panic!(); /////// todo: hopefully not needed?
    }

    fn compute_write_frontier(
        &self,
        _instance: ComputeInstanceId,
        _id: GlobalId,
    ) -> Antichain<Timestamp> {
        panic!(); /////// todo: hopefully not needed?
    }

    fn storage_frontiers(
        &self,
        _ids: Vec<GlobalId>,
    ) -> Vec<(GlobalId, Antichain<Timestamp>, Antichain<Timestamp>)> {
        panic!(); /////// todo: hopefully not needed?
    }

    fn catalog_state(&self) -> &CatalogState {
        self.catalog_state
    }

    /////////// todo: unused if we are using acquire_read_holds_and_collection_write_frontiers
    fn acquire_read_holds(&self, id_bundle: &CollectionIdBundle) -> ReadHolds<Timestamp> {
        // println!("FrontendPeekTimestampProvider::acquire_read_holds");
        // let r = tokio::task::block_in_place(|| {
        //     let handle = tokio::runtime::Handle::current();
        //     handle.block_on(async {
        //         self.session_client
        //             .peek_client()
        //             .acquire_read_holds(id_bundle)
        //             .await
        //     })
        // });
        // println!("FrontendPeekTimestampProvider::acquire_read_holds done");
        // r
        
        panic!("Use acquire_read_holds_and_collection_write_frontiers instead");
    }

    /////////// todo: unused if we are using acquire_read_holds_and_collection_write_frontiers
    fn least_valid_write(&self, id_bundle: &CollectionIdBundle) -> Antichain<Timestamp> {
        // println!("FrontendPeekTimestampProvider::least_valid_write");
        // let r = tokio::task::block_in_place(|| {
        //     let handle = tokio::runtime::Handle::current();
        //     handle.block_on(async {
        //         self.session_client
        //             .peek_client()
        //             .least_valid_write(id_bundle)
        //             .await
        //     })
        // });
        // println!("FrontendPeekTimestampProvider::least_valid_write done");
        // r

        panic!("Use acquire_read_holds_and_collection_write_frontiers instead");
    }

    fn determine_timestamp_for(&self, session: &Session, id_bundle: &CollectionIdBundle, when: &QueryWhen, compute_instance: ComputeInstanceId, timeline_context: &TimelineContext, oracle_read_ts: Option<Timestamp>, real_time_recency_ts: Option<Timestamp>, isolation_level: &IsolationLevel, constraint_based: &ConstraintBasedTimestampSelection) -> Result<(TimestampDetermination<Timestamp>, ReadHolds<Timestamp>), AdapterError> {

        // // let read_holds = self.acquire_read_holds(id_bundle);
        // // let upper = self.least_valid_write(id_bundle);
        //
        // let (read_holds, upper) = self.acquire_read_hold_and_collection_write_frontier(id_bundle).expect("missing collection");
        //
        // self.determine_timestamp_for_inner(
        //     session,
        //     id_bundle,
        //     when,
        //     compute_instance,
        //     timeline_context,
        //     oracle_read_ts,
        //     real_time_recency_ts,
        //     isolation_level,
        //     constraint_based,
        //     read_holds,
        //     upper,
        // )

        panic!("use frontend_determine_timestamp_for instead");
    }
}

impl FrontendPeekTimestampProvider<'_> {
    /////////// todo: this is copy-pasted from Coordinator
    pub(crate) async fn determine_timestamp(
        &self,
        session: &Session,
        id_bundle: &CollectionIdBundle,
        when: &QueryWhen,
        compute_instance: ComputeInstanceId,
        timeline_context: &TimelineContext,
        oracle_read_ts: Option<Timestamp>,
        real_time_recency_ts: Option<Timestamp>,
    ) -> Result<(TimestampDetermination<Timestamp>, ReadHolds<Timestamp>), AdapterError> {
        let constraint_based = ConstraintBasedTimestampSelection::from_str(
            &CONSTRAINT_BASED_TIMESTAMP_SELECTION
                .get(self.catalog_state().system_config().dyncfgs()),
        );

        let isolation_level = session.vars().transaction_isolation();
        let (det, read_holds) = self.frontend_determine_timestamp_for(
            session,
            id_bundle,
            when,
            compute_instance,
            timeline_context,
            oracle_read_ts,
            real_time_recency_ts,
            isolation_level,
            &constraint_based,
        ).await?;

        /////////// todo: pass metrics into FrontendPeekTimestampProvider
        // self.metrics
        //     .determine_timestamp
        //     .with_label_values(&[
        //         match det.respond_immediately() {
        //             true => "true",
        //             false => "false",
        //         },
        //         isolation_level.as_str(),
        //         &compute_instance.to_string(),
        //         constraint_based.as_str(),
        //     ])
        //     .inc();
        // if !det.respond_immediately()
        //     && isolation_level == &IsolationLevel::StrictSerializable
        //     && real_time_recency_ts.is_none()
        // {
        //     // Note down the difference between StrictSerializable and Serializable into a metric.
        //     if let Some(strict) = det.timestamp_context.timestamp() {
        //         let (serializable_det, _tmp_read_holds) = self.determine_timestamp_for(
        //             session,
        //             id_bundle,
        //             when,
        //             compute_instance,
        //             timeline_context,
        //             oracle_read_ts,
        //             real_time_recency_ts,
        //             &IsolationLevel::Serializable,
        //             &constraint_based,
        //         )?;
        //
        //         if let Some(serializable) = serializable_det.timestamp_context.timestamp() {
        //             self.metrics
        //                 .timestamp_difference_for_strict_serializable_ms
        //                 .with_label_values(&[
        //                     &compute_instance.to_string(),
        //                     constraint_based.as_str(),
        //                 ])
        //                 .observe(f64::cast_lossy(u64::from(
        //                     strict.saturating_sub(*serializable),
        //                 )));
        //         }
        //     }
        // }

        Ok((det, read_holds))
    }

    async fn frontend_determine_timestamp_for(&self, session: &Session, id_bundle: &CollectionIdBundle, when: &QueryWhen, compute_instance: ComputeInstanceId, timeline_context: &TimelineContext, oracle_read_ts: Option<Timestamp>, real_time_recency_ts: Option<Timestamp>, isolation_level: &IsolationLevel, constraint_based: &ConstraintBasedTimestampSelection) -> Result<(TimestampDetermination<Timestamp>, ReadHolds<Timestamp>), AdapterError> {

        // let read_holds = self.acquire_read_holds(id_bundle);
        // let upper = self.least_valid_write(id_bundle);

        let (read_holds, upper) = self.acquire_read_holds_and_collection_write_frontiers(id_bundle).await.expect("missing collection");

        <Coordinator as TimestampProvider>::determine_timestamp_for_inner(
            session,
            id_bundle,
            when,
            compute_instance,
            timeline_context,
            oracle_read_ts,
            real_time_recency_ts,
            isolation_level,
            constraint_based,
            read_holds,
            upper,
        )
    }
}

impl FrontendPeekTimestampProvider<'_> {
    /////////// todo: make this able to handle multiple collections _and_ not just compute collections, but also storage
    pub async fn acquire_read_holds_and_collection_write_frontiers(&self, id_bundle: &CollectionIdBundle) -> Result<(ReadHolds<Timestamp>, Antichain<Timestamp>), CollectionMissing> {
        // // ///////////// todo: do away with the block_in_place
        // tokio::task::block_in_place(|| {
        //     let handle = tokio::runtime::Handle::current();
        //     handle.block_on(async {
        //         self.session_client
        //             .peek_client()
        //             .acquire_read_holds_and_collection_write_frontiers(id_bundle)
        //             .await
        //     })
        // })

        self.session_client
            .peek_client()
            .acquire_read_holds_and_collection_write_frontiers(id_bundle)
            .await
    }
}

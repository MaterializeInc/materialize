// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

// Prevents anyone from accidentally exporting a method from the `inner` module.
#![allow(clippy::pub_use)]

//! Logic for executing a planned SQL query.

use std::collections::{BTreeMap, BTreeSet};
use std::str::FromStr;
use std::sync::Arc;

use futures::FutureExt;
use futures::future::LocalBoxFuture;
use futures::stream::FuturesOrdered;
use http::Uri;
use inner::return_if_err;
use maplit::btreemap;
use mz_catalog::memory::objects::Cluster;
use mz_controller_types::ReplicaId;
use mz_expr::row::RowCollection;
use mz_expr::{MapFilterProject, MirRelationExpr, ResultSpec, RowSetFinishing};
use mz_ore::cast::CastFrom;
use mz_ore::tracing::OpenTelemetryContext;
use mz_persist_client::stats::SnapshotPartStats;
use mz_repr::explain::{ExprHumanizerExt, TransientItem};
use mz_repr::{CatalogItemId, Datum, Diff, GlobalId, IntoRowIterator, Row, RowArena, Timestamp};
use mz_sql::catalog::{CatalogError, SessionCatalog};
use mz_sql::names::ResolvedIds;
use mz_sql::plan::{
    self, AbortTransactionPlan, CommitTransactionPlan, CopyFromSource, CreateRolePlan,
    CreateSourcePlanBundle, FetchPlan, HirScalarExpr, MutationKind, Params, Plan, PlanKind,
    RaisePlan,
};
use mz_sql::rbac;
use mz_sql::session::metadata::SessionMetadata;
use mz_sql::session::vars;
use mz_sql::session::vars::SessionVars;
use mz_sql_parser::ast::{Raw, Statement};
use mz_storage_client::client::TableData;
use mz_storage_client::storage_collections::StorageCollections;
use mz_storage_types::connections::inline::IntoInlineConnection;
use mz_storage_types::controller::StorageError;
use mz_storage_types::stats::RelationPartStats;
use mz_transform::dataflow::DataflowMetainfo;
use mz_transform::notice::{OptimizerNoticeApi, OptimizerNoticeKind, RawOptimizerNotice};
use mz_transform::{EmptyStatisticsOracle, StatisticsOracle};
use timely::progress::Antichain;
use timely::progress::Timestamp as TimelyTimestamp;
use tokio::sync::oneshot;
use tracing::{Instrument, Level, Span, event, warn};

use crate::ExecuteContext;
use crate::catalog::{Catalog, CatalogState};
use crate::command::{Command, ExecuteResponse, Response};
use crate::coord::appends::{DeferredOp, DeferredPlan};
use crate::coord::validity::PlanValidity;
use crate::coord::{
    Coordinator, DeferredPlanStatement, ExplainPlanContext, Message, PlanStatement, TargetCluster,
    catalog_serving,
};
use crate::error::AdapterError;
use crate::explain::insights::PlanInsightsContext;
use crate::notice::AdapterNotice;
use crate::optimize::dataflows::{EvalTime, ExprPrep, ExprPrepOneShot};
use crate::optimize::peek;
use crate::session::{
    EndTransactionAction, Session, StateRevision, TransactionOps, TransactionStatus, WriteOp,
};
use crate::util::ClientTransmitter;

// DO NOT make this visible in any way, i.e. do not add any version of
// `pub` to this mod. The inner `sequence_X` methods are hidden in this
// private module to prevent anyone from calling them directly. All
// sequencing should be done through the `sequence_plan` method.
// This allows us to add catch-all logic that should be applied to all
// plans in `sequence_plan` and guarantee that no caller can circumvent
// that logic.
//
// The exceptions are:
//
// - Creating a role during connection startup. In this scenario, the session has not been properly
// initialized and we need to skip directly to creating role. We have a specific method,
// `sequence_create_role_for_startup` for this purpose.
// - Methods that continue the execution of some plan that was being run asynchronously, such as
// `sequence_peek_stage` and `sequence_create_connection_stage_finish`.
// - The frontend peek sequencing temporarily reaches into this module for things that are needed
//   by both the old and new peek sequencing. TODO(peek-seq): We plan to eliminate this with a
//   big refactoring after the old peek sequencing is removed.

mod inner;

impl Coordinator {
    /// BOXED FUTURE: As of Nov 2023 the returned Future from this function was 34KB. This would
    /// get stored on the stack which is bad for runtime performance, and blow up our stack usage.
    /// Because of that we purposefully move this Future onto the heap (i.e. Box it).
    pub(crate) fn sequence_plan(
        &mut self,
        mut ctx: ExecuteContext,
        plan: Plan,
        resolved_ids: ResolvedIds,
    ) -> LocalBoxFuture<'_, ()> {
        async move {
            let responses = ExecuteResponse::generated_from(&PlanKind::from(&plan));
            ctx.tx_mut().set_allowed(responses);

            if self.controller.read_only() && !plan.allowed_in_read_only() {
                ctx.retire(Err(AdapterError::ReadOnly));
                return;
            }

            // Check if we're still waiting for any of the builtin table appends from when we
            // started the Session to complete.
            if let Some((dependencies, wait_future)) =
                super::appends::waiting_on_startup_appends(self.catalog(), ctx.session_mut(), &plan)
            {
                let conn_id = ctx.session().conn_id();
                tracing::debug!(%conn_id, "deferring plan for startup appends");

                let role_metadata = ctx.session().role_metadata().clone();
                let validity = PlanValidity::new(
                    self.catalog.transient_revision(),
                    dependencies,
                    None,
                    None,
                    role_metadata,
                );
                let deferred_plan = DeferredPlan {
                    ctx,
                    plan,
                    validity,
                    requires_locks: BTreeSet::default(),
                };
                // Defer op accepts an optional write lock, but there aren't any writes occurring
                // here, since the map to `None`.
                let acquire_future = wait_future.map(|()| None);

                self.defer_op(acquire_future, DeferredOp::Plan(deferred_plan));

                // Return early because our op is deferred on waiting for the builtin writes to
                // complete.
                return;
            };

            // Scope the borrow of the Catalog because we need to mutate the Coordinator state below.
            let target_cluster = match ctx.session().transaction().cluster() {
                // Use the current transaction's cluster.
                Some(cluster_id) => TargetCluster::Transaction(cluster_id),
                // If there isn't a current cluster set for a transaction, then try to auto route.
                None => {
                    let session_catalog = self.catalog.for_session(ctx.session());
                    catalog_serving::auto_run_on_catalog_server(
                        &session_catalog,
                        ctx.session(),
                        &plan,
                    )
                }
            };
            let (target_cluster_id, target_cluster_name) = match self
                .catalog()
                .resolve_target_cluster(target_cluster, ctx.session())
            {
                Ok(cluster) => (Some(cluster.id), Some(cluster.name.clone())),
                Err(_) => (None, None),
            };

            if let (Some(cluster_id), Some(statement_id)) =
                (target_cluster_id, ctx.extra().contents())
            {
                self.set_statement_execution_cluster(statement_id, cluster_id);
            }

            let session_catalog = self.catalog.for_session(ctx.session());

            if let Some(cluster_name) = &target_cluster_name {
                if let Err(e) = catalog_serving::check_cluster_restrictions(
                    cluster_name,
                    &session_catalog,
                    &plan,
                ) {
                    return ctx.retire(Err(e));
                }
            }

            if let Err(e) = rbac::check_plan(
                &session_catalog,
                Some(|id| {
                    // We use linear search through active connections if needed, which is fine
                    // because the RBAC check will call the closure at most once.
                    self.active_conns()
                        .into_iter()
                        .find(|(conn_id, _)| conn_id.unhandled() == id)
                        .map(|(_, conn_meta)| *conn_meta.authenticated_role_id())
                }),
                ctx.session(),
                &plan,
                target_cluster_id,
                &resolved_ids,
            ) {
                return ctx.retire(Err(e.into()));
            }

            match plan {
                Plan::CreateSource(plan) => {
                    let id_ts = self.get_catalog_write_ts().await;
                    let (item_id, global_id) =
                        return_if_err!(self.catalog().allocate_user_id(id_ts).await, ctx);
                    let result = self
                        .sequence_create_source(
                            &mut ctx,
                            vec![CreateSourcePlanBundle {
                                item_id,
                                global_id,
                                plan,
                                resolved_ids,
                                available_source_references: None,
                            }],
                        )
                        .await;
                    ctx.retire(result);
                }
                Plan::CreateSources(plans) => {
                    assert!(
                        resolved_ids.is_empty(),
                        "each plan has separate resolved_ids"
                    );
                    let result = self.sequence_create_source(&mut ctx, plans).await;
                    ctx.retire(result);
                }
                Plan::CreateConnection(plan) => {
                    self.sequence_create_connection(ctx, plan, resolved_ids)
                        .await;
                }
                Plan::CreateDatabase(plan) => {
                    let result = self.sequence_create_database(ctx.session_mut(), plan).await;
                    ctx.retire(result);
                }
                Plan::CreateSchema(plan) => {
                    let result = self.sequence_create_schema(ctx.session_mut(), plan).await;
                    ctx.retire(result);
                }
                Plan::CreateRole(plan) => {
                    let result = self
                        .sequence_create_role(Some(ctx.session().conn_id()), plan)
                        .await;
                    if let Some(notice) = self.should_emit_rbac_notice(ctx.session()) {
                        ctx.session().add_notice(notice);
                    }
                    ctx.retire(result);
                }
                Plan::CreateCluster(plan) => {
                    let result = self.sequence_create_cluster(ctx.session(), plan).await;
                    ctx.retire(result);
                }
                Plan::CreateClusterReplica(plan) => {
                    let result = self
                        .sequence_create_cluster_replica(ctx.session(), plan)
                        .await;
                    ctx.retire(result);
                }
                Plan::CreateTable(plan) => {
                    let result = self
                        .sequence_create_table(&mut ctx, plan, resolved_ids)
                        .await;
                    ctx.retire(result);
                }
                Plan::CreateSecret(plan) => {
                    self.sequence_create_secret(ctx, plan).await;
                }
                Plan::CreateSink(plan) => {
                    self.sequence_create_sink(ctx, plan, resolved_ids).await;
                }
                Plan::CreateView(plan) => {
                    self.sequence_create_view(ctx, plan, resolved_ids).await;
                }
                Plan::CreateMaterializedView(plan) => {
                    self.sequence_create_materialized_view(ctx, plan, resolved_ids)
                        .await;
                }
                Plan::CreateContinualTask(plan) => {
                    let res = self
                        .sequence_create_continual_task(&mut ctx, plan, resolved_ids)
                        .await;
                    ctx.retire(res);
                }
                Plan::CreateIndex(plan) => {
                    self.sequence_create_index(ctx, plan, resolved_ids).await;
                }
                Plan::CreateType(plan) => {
                    let result = self
                        .sequence_create_type(ctx.session(), plan, resolved_ids)
                        .await;
                    ctx.retire(result);
                }
                Plan::CreateNetworkPolicy(plan) => {
                    let res = self
                        .sequence_create_network_policy(ctx.session(), plan)
                        .await;
                    ctx.retire(res);
                }
                Plan::Comment(plan) => {
                    let result = self.sequence_comment_on(ctx.session(), plan).await;
                    ctx.retire(result);
                }
                Plan::CopyTo(plan) => {
                    self.sequence_copy_to(ctx, plan, target_cluster).await;
                }
                Plan::DropObjects(plan) => {
                    let result = self.sequence_drop_objects(&mut ctx, plan).await;
                    ctx.retire(result);
                }
                Plan::DropOwned(plan) => {
                    let result = self.sequence_drop_owned(ctx.session_mut(), plan).await;
                    ctx.retire(result);
                }
                Plan::EmptyQuery => {
                    ctx.retire(Ok(ExecuteResponse::EmptyQuery));
                }
                Plan::ShowAllVariables => {
                    let result = self.sequence_show_all_variables(ctx.session());
                    ctx.retire(result);
                }
                Plan::ShowVariable(plan) => {
                    let result = self.sequence_show_variable(ctx.session(), plan);
                    ctx.retire(result);
                }
                Plan::InspectShard(plan) => {
                    // TODO: Ideally, this await would happen off the main thread.
                    let result = self.sequence_inspect_shard(ctx.session(), plan).await;
                    ctx.retire(result);
                }
                Plan::SetVariable(plan) => {
                    let result = self.sequence_set_variable(ctx.session_mut(), plan);
                    ctx.retire(result);
                }
                Plan::ResetVariable(plan) => {
                    let result = self.sequence_reset_variable(ctx.session_mut(), plan);
                    ctx.retire(result);
                }
                Plan::SetTransaction(plan) => {
                    let result = self.sequence_set_transaction(ctx.session_mut(), plan);
                    ctx.retire(result);
                }
                Plan::StartTransaction(plan) => {
                    if matches!(
                        ctx.session().transaction(),
                        TransactionStatus::InTransaction(_)
                    ) {
                        ctx.session()
                            .add_notice(AdapterNotice::ExistingTransactionInProgress);
                    }
                    let result = ctx.session_mut().start_transaction(
                        self.now_datetime(),
                        plan.access,
                        plan.isolation_level,
                    );
                    ctx.retire(result.map(|_| ExecuteResponse::StartedTransaction))
                }
                Plan::CommitTransaction(CommitTransactionPlan {
                    ref transaction_type,
                })
                | Plan::AbortTransaction(AbortTransactionPlan {
                    ref transaction_type,
                }) => {
                    // Serialize DDL transactions. Statements that use this mode must return false
                    // in `must_serialize_ddl()`.
                    if ctx.session().transaction().is_ddl() {
                        if let Ok(guard) = self.serialized_ddl.try_lock_owned() {
                            let prev = self
                                .active_conns
                                .get_mut(ctx.session().conn_id())
                                .expect("connection must exist")
                                .deferred_lock
                                .replace(guard);
                            assert!(
                                prev.is_none(),
                                "connections should have at most one lock guard"
                            );
                        } else {
                            self.serialized_ddl.push_back(DeferredPlanStatement {
                                ctx,
                                ps: PlanStatement::Plan { plan, resolved_ids },
                            });
                            return;
                        }
                    }

                    let action = match &plan {
                        Plan::CommitTransaction(_) => EndTransactionAction::Commit,
                        Plan::AbortTransaction(_) => EndTransactionAction::Rollback,
                        _ => unreachable!(),
                    };
                    if ctx.session().transaction().is_implicit() && !transaction_type.is_implicit()
                    {
                        // In Postgres, if a user sends a COMMIT or ROLLBACK in an
                        // implicit transaction, a warning is sent warning them.
                        // (The transaction is still closed and a new implicit
                        // transaction started, though.)
                        ctx.session().add_notice(
                            AdapterNotice::ExplicitTransactionControlInImplicitTransaction,
                        );
                    }
                    self.sequence_end_transaction(ctx, action).await;
                }
                Plan::Select(plan) => {
                    let max = Some(ctx.session().vars().max_query_result_size());
                    self.sequence_peek(ctx, plan, target_cluster, max).await;
                }
                Plan::Subscribe(plan) => {
                    self.sequence_subscribe(ctx, plan, target_cluster).await;
                }
                Plan::SideEffectingFunc(plan) => {
                    self.sequence_side_effecting_func(ctx, plan).await;
                }
                Plan::ShowCreate(plan) => {
                    ctx.retire(Ok(Self::send_immediate_rows(plan.row)));
                }
                Plan::ShowColumns(show_columns_plan) => {
                    let max = Some(ctx.session().vars().max_query_result_size());
                    self.sequence_peek(ctx, show_columns_plan.select_plan, target_cluster, max)
                        .await;
                }
                Plan::CopyFrom(plan) => match plan.source {
                    CopyFromSource::Stdin => {
                        let (tx, _, session, ctx_extra) = ctx.into_parts();
                        tx.send(
                            Ok(ExecuteResponse::CopyFrom {
                                target_id: plan.target_id,
                                target_name: plan.target_name,
                                columns: plan.columns,
                                params: plan.params,
                                ctx_extra,
                            }),
                            session,
                        );
                    }
                    CopyFromSource::Url(_) | CopyFromSource::AwsS3 { .. } => {
                        self.sequence_copy_from(ctx, plan, target_cluster).await;
                    }
                },
                Plan::ExplainPlan(plan) => {
                    self.sequence_explain_plan(ctx, plan, target_cluster).await;
                }
                Plan::ExplainPushdown(plan) => {
                    self.sequence_explain_pushdown(ctx, plan, target_cluster)
                        .await;
                }
                Plan::ExplainSinkSchema(plan) => {
                    let result = self.sequence_explain_schema(plan);
                    ctx.retire(result);
                }
                Plan::ExplainTimestamp(plan) => {
                    self.sequence_explain_timestamp(ctx, plan, target_cluster)
                        .await;
                }
                Plan::Insert(plan) => {
                    self.sequence_insert(ctx, plan).await;
                }
                Plan::ReadThenWrite(plan) => {
                    self.sequence_read_then_write(ctx, plan).await;
                }
                Plan::AlterNoop(plan) => {
                    ctx.retire(Ok(ExecuteResponse::AlteredObject(plan.object_type)));
                }
                Plan::AlterCluster(plan) => {
                    self.sequence_alter_cluster_staged(ctx, plan).await;
                }
                Plan::AlterClusterRename(plan) => {
                    let result = self.sequence_alter_cluster_rename(&mut ctx, plan).await;
                    ctx.retire(result);
                }
                Plan::AlterClusterSwap(plan) => {
                    let result = self.sequence_alter_cluster_swap(&mut ctx, plan).await;
                    ctx.retire(result);
                }
                Plan::AlterClusterReplicaRename(plan) => {
                    let result = self
                        .sequence_alter_cluster_replica_rename(ctx.session(), plan)
                        .await;
                    ctx.retire(result);
                }
                Plan::AlterConnection(plan) => {
                    self.sequence_alter_connection(ctx, plan).await;
                }
                Plan::AlterSetCluster(plan) => {
                    let result = self.sequence_alter_set_cluster(ctx.session(), plan).await;
                    ctx.retire(result);
                }
                Plan::AlterRetainHistory(plan) => {
                    let result = self.sequence_alter_retain_history(&mut ctx, plan).await;
                    ctx.retire(result);
                }
                Plan::AlterItemRename(plan) => {
                    let result = self.sequence_alter_item_rename(&mut ctx, plan).await;
                    ctx.retire(result);
                }
                Plan::AlterSchemaRename(plan) => {
                    let result = self.sequence_alter_schema_rename(&mut ctx, plan).await;
                    ctx.retire(result);
                }
                Plan::AlterSchemaSwap(plan) => {
                    let result = self.sequence_alter_schema_swap(&mut ctx, plan).await;
                    ctx.retire(result);
                }
                Plan::AlterRole(plan) => {
                    let result = self.sequence_alter_role(ctx.session_mut(), plan).await;
                    ctx.retire(result);
                }
                Plan::AlterSecret(plan) => {
                    self.sequence_alter_secret(ctx, plan).await;
                }
                Plan::AlterSink(plan) => {
                    self.sequence_alter_sink_prepare(ctx, plan).await;
                }
                Plan::AlterSource(plan) => {
                    let result = self.sequence_alter_source(ctx.session_mut(), plan).await;
                    ctx.retire(result);
                }
                Plan::AlterSystemSet(plan) => {
                    let result = self.sequence_alter_system_set(ctx.session(), plan).await;
                    ctx.retire(result);
                }
                Plan::AlterSystemReset(plan) => {
                    let result = self.sequence_alter_system_reset(ctx.session(), plan).await;
                    ctx.retire(result);
                }
                Plan::AlterSystemResetAll(plan) => {
                    let result = self
                        .sequence_alter_system_reset_all(ctx.session(), plan)
                        .await;
                    ctx.retire(result);
                }
                Plan::AlterTableAddColumn(plan) => {
                    let result = self.sequence_alter_table(&mut ctx, plan).await;
                    ctx.retire(result);
                }
                Plan::AlterMaterializedViewApplyReplacement(plan) => {
                    let result = self
                        .sequence_alter_materialized_view_apply_replacement(&ctx, plan)
                        .await;
                    ctx.retire(result);
                }
                Plan::AlterNetworkPolicy(plan) => {
                    let res = self
                        .sequence_alter_network_policy(ctx.session(), plan)
                        .await;
                    ctx.retire(res);
                }
                Plan::DiscardTemp => {
                    self.drop_temp_items(ctx.session().conn_id()).await;
                    ctx.retire(Ok(ExecuteResponse::DiscardedTemp));
                }
                Plan::DiscardAll => {
                    let ret = if let TransactionStatus::Started(_) = ctx.session().transaction() {
                        self.clear_transaction(ctx.session_mut()).await;
                        self.drop_temp_items(ctx.session().conn_id()).await;
                        ctx.session_mut().reset();
                        Ok(ExecuteResponse::DiscardedAll)
                    } else {
                        Err(AdapterError::OperationProhibitsTransaction(
                            "DISCARD ALL".into(),
                        ))
                    };
                    ctx.retire(ret);
                }
                Plan::Declare(plan) => {
                    self.declare(ctx, plan.name, plan.stmt, plan.sql, plan.params);
                }
                Plan::Fetch(FetchPlan {
                    name,
                    count,
                    timeout,
                }) => {
                    let ctx_extra = std::mem::take(ctx.extra_mut());
                    ctx.retire(Ok(ExecuteResponse::Fetch {
                        name,
                        count,
                        timeout,
                        ctx_extra,
                    }));
                }
                Plan::Close(plan) => {
                    if ctx.session_mut().remove_portal(&plan.name) {
                        ctx.retire(Ok(ExecuteResponse::ClosedCursor));
                    } else {
                        ctx.retire(Err(AdapterError::UnknownCursor(plan.name)));
                    }
                }
                Plan::Prepare(plan) => {
                    if ctx
                        .session()
                        .get_prepared_statement_unverified(&plan.name)
                        .is_some()
                    {
                        ctx.retire(Err(AdapterError::PreparedStatementExists(plan.name)));
                    } else {
                        let state_revision = StateRevision {
                            catalog_revision: self.catalog().transient_revision(),
                            session_state_revision: ctx.session().state_revision(),
                        };
                        ctx.session_mut().set_prepared_statement(
                            plan.name,
                            Some(plan.stmt),
                            plan.sql,
                            plan.desc,
                            state_revision,
                            self.now(),
                        );
                        ctx.retire(Ok(ExecuteResponse::Prepare));
                    }
                }
                Plan::Execute(plan) => {
                    match self.sequence_execute(ctx.session_mut(), plan) {
                        Ok(portal_name) => {
                            let (tx, _, session, extra) = ctx.into_parts();
                            self.internal_cmd_tx
                                .send(Message::Command(
                                    OpenTelemetryContext::obtain(),
                                    Command::Execute {
                                        portal_name,
                                        session,
                                        tx: tx.take(),
                                        outer_ctx_extra: Some(extra),
                                    },
                                ))
                                .expect("sending to self.internal_cmd_tx cannot fail");
                        }
                        Err(err) => ctx.retire(Err(err)),
                    };
                }
                Plan::Deallocate(plan) => match plan.name {
                    Some(name) => {
                        if ctx.session_mut().remove_prepared_statement(&name) {
                            ctx.retire(Ok(ExecuteResponse::Deallocate { all: false }));
                        } else {
                            ctx.retire(Err(AdapterError::UnknownPreparedStatement(name)));
                        }
                    }
                    None => {
                        ctx.session_mut().remove_all_prepared_statements();
                        ctx.retire(Ok(ExecuteResponse::Deallocate { all: true }));
                    }
                },
                Plan::Raise(RaisePlan { severity }) => {
                    ctx.session()
                        .add_notice(AdapterNotice::UserRequested { severity });
                    ctx.retire(Ok(ExecuteResponse::Raised));
                }
                Plan::GrantPrivileges(plan) => {
                    let result = self
                        .sequence_grant_privileges(ctx.session_mut(), plan)
                        .await;
                    ctx.retire(result);
                }
                Plan::RevokePrivileges(plan) => {
                    let result = self
                        .sequence_revoke_privileges(ctx.session_mut(), plan)
                        .await;
                    ctx.retire(result);
                }
                Plan::AlterDefaultPrivileges(plan) => {
                    let result = self
                        .sequence_alter_default_privileges(ctx.session_mut(), plan)
                        .await;
                    ctx.retire(result);
                }
                Plan::GrantRole(plan) => {
                    let result = self.sequence_grant_role(ctx.session_mut(), plan).await;
                    ctx.retire(result);
                }
                Plan::RevokeRole(plan) => {
                    let result = self.sequence_revoke_role(ctx.session_mut(), plan).await;
                    ctx.retire(result);
                }
                Plan::AlterOwner(plan) => {
                    let result = self.sequence_alter_owner(ctx.session_mut(), plan).await;
                    ctx.retire(result);
                }
                Plan::ReassignOwned(plan) => {
                    let result = self.sequence_reassign_owned(ctx.session_mut(), plan).await;
                    ctx.retire(result);
                }
                Plan::ValidateConnection(plan) => {
                    let connection = plan
                        .connection
                        .into_inline_connection(self.catalog().state());
                    let current_storage_configuration = self.controller.storage.config().clone();
                    mz_ore::task::spawn(|| "coord::validate_connection", async move {
                        let res = match connection
                            .validate(plan.id, &current_storage_configuration)
                            .await
                        {
                            Ok(()) => Ok(ExecuteResponse::ValidatedConnection),
                            Err(err) => Err(err.into()),
                        };
                        ctx.retire(res);
                    });
                }
            }
        }
        .instrument(tracing::debug_span!("coord::sequencer::sequence_plan"))
        .boxed_local()
    }

    #[mz_ore::instrument(level = "debug")]
    pub(crate) async fn sequence_execute_single_statement_transaction(
        &mut self,
        ctx: ExecuteContext,
        stmt: Arc<Statement<Raw>>,
        params: Params,
    ) {
        // Put the session into single statement implicit so anything can execute.
        let (tx, internal_cmd_tx, mut session, extra) = ctx.into_parts();
        assert!(matches!(session.transaction(), TransactionStatus::Default));
        session.start_transaction_single_stmt(self.now_datetime());
        let conn_id = session.conn_id().unhandled();

        // Execute the saved statement in a temp transmitter so we can run COMMIT.
        let (sub_tx, sub_rx) = oneshot::channel();
        let sub_ct = ClientTransmitter::new(sub_tx, self.internal_cmd_tx.clone());
        let sub_ctx = ExecuteContext::from_parts(sub_ct, internal_cmd_tx, session, extra);
        self.handle_execute_inner(stmt, params, sub_ctx).await;

        // The response can need off-thread processing. Wait for it elsewhere so the coordinator can
        // continue processing.
        let internal_cmd_tx = self.internal_cmd_tx.clone();
        mz_ore::task::spawn(
            || format!("execute_single_statement:{conn_id}"),
            async move {
                let Ok(Response {
                    result,
                    session,
                    otel_ctx,
                }) = sub_rx.await
                else {
                    // Coordinator went away.
                    return;
                };
                otel_ctx.attach_as_parent();
                let (sub_tx, sub_rx) = oneshot::channel();
                let _ = internal_cmd_tx.send(Message::Command(
                    otel_ctx,
                    Command::Commit {
                        action: EndTransactionAction::Commit,
                        session,
                        tx: sub_tx,
                    },
                ));
                let Ok(commit_response) = sub_rx.await else {
                    // Coordinator went away.
                    return;
                };
                assert!(matches!(
                    commit_response.session.transaction(),
                    TransactionStatus::Default
                ));
                // The fake, generated response was already sent to the user and we don't need to
                // ever send an `Ok(result)` to the user, because they are expecting a response from
                // a `COMMIT`. So, always send the `COMMIT`'s result if the original statement
                // succeeded. If it failed, we can send an error and don't need to wrap it or send a
                // later COMMIT or ROLLBACK.
                let result = match (result, commit_response.result) {
                    (Ok(_), commit) => commit,
                    (Err(result), _) => Err(result),
                };
                // We ignore the resp.result because it's not clear what to do if it failed since we
                // can only send a single ExecuteResponse to tx.
                tx.send(result, commit_response.session);
            }
            .instrument(Span::current()),
        );
    }

    /// Creates a role during connection startup.
    ///
    /// This should not be called from anywhere except connection startup.
    #[mz_ore::instrument(level = "debug")]
    pub(crate) async fn sequence_create_role_for_startup(
        &mut self,
        plan: CreateRolePlan,
    ) -> Result<ExecuteResponse, AdapterError> {
        // This does not set conn_id because it's not yet in active_conns. That is because we can't
        // make a ConnMeta until we have a role id which we don't have until after the catalog txn
        // is committed. Passing None here means the audit log won't have a user set in the event's
        // user field. This seems fine because it is indeed the system that is creating this role,
        // not a user request, and the user name is still recorded in the plan, so we aren't losing
        // information.
        self.sequence_create_role(None, plan).await
    }

    pub(crate) fn allocate_transient_id(&self) -> (CatalogItemId, GlobalId) {
        self.transient_id_gen.allocate_id()
    }

    fn should_emit_rbac_notice(&self, session: &Session) -> Option<AdapterNotice> {
        if !rbac::is_rbac_enabled_for_session(self.catalog.system_config(), session) {
            Some(AdapterNotice::RbacUserDisabled)
        } else {
            None
        }
    }

    /// Inserts the rows from `constants` into the table identified by `target_id`.
    ///
    /// # Panics
    ///
    /// Panics if `target_id` doesn't refer to a table.
    /// Panics if `constants` is not an `MirRelationExpr::Constant`.
    pub(crate) fn insert_constant(
        catalog: &Catalog,
        session: &mut Session,
        target_id: CatalogItemId,
        constants: MirRelationExpr,
    ) -> Result<ExecuteResponse, AdapterError> {
        // Insert can be queued, so we need to re-verify the id exists.
        let desc = match catalog.try_get_entry(&target_id) {
            Some(table) => {
                // Inserts always happen at the latest version of a table.
                table.relation_desc_latest().expect("table has desc")
            }
            None => {
                return Err(AdapterError::Catalog(mz_catalog::memory::error::Error {
                    kind: mz_catalog::memory::error::ErrorKind::Sql(CatalogError::UnknownItem(
                        target_id.to_string(),
                    )),
                }));
            }
        };

        match constants.as_const() {
            Some((rows, ..)) => {
                let rows = rows.clone()?;
                for (row, _) in &rows {
                    for (i, datum) in row.iter().enumerate() {
                        desc.constraints_met(i, &datum)?;
                    }
                }
                let diffs_plan = plan::SendDiffsPlan {
                    id: target_id,
                    updates: rows,
                    kind: MutationKind::Insert,
                    returning: Vec::new(),
                    max_result_size: catalog.system_config().max_result_size(),
                };
                Self::send_diffs(session, diffs_plan)
            }
            None => panic!(
                "tried using sequence_insert_constant on non-constant MirRelationExpr\n{}",
                constants.pretty(),
            ),
        }
    }

    #[mz_ore::instrument(level = "debug")]
    pub(crate) fn send_diffs(
        session: &mut Session,
        mut plan: plan::SendDiffsPlan,
    ) -> Result<ExecuteResponse, AdapterError> {
        let affected_rows = {
            let mut affected_rows = Diff::from(0);
            let mut all_positive_diffs = true;
            // If all diffs are positive, the number of affected rows is just the
            // sum of all unconsolidated diffs.
            for (_, diff) in plan.updates.iter() {
                if diff.is_negative() {
                    all_positive_diffs = false;
                    break;
                }

                affected_rows += diff;
            }

            if !all_positive_diffs {
                // Consolidate rows. This is useful e.g. for an UPDATE where the row
                // doesn't change, and we need to reflect that in the number of
                // affected rows.
                differential_dataflow::consolidation::consolidate(&mut plan.updates);

                affected_rows = Diff::ZERO;
                // With retractions, the number of affected rows is not the number
                // of rows we see, but the sum of the absolute value of their diffs,
                // e.g. if one row is retracted and another is added, the total
                // number of rows affected is 2.
                for (_, diff) in plan.updates.iter() {
                    affected_rows += diff.abs();
                }
            }

            usize::try_from(affected_rows.into_inner()).expect("positive Diff must fit")
        };
        event!(
            Level::TRACE,
            affected_rows,
            id = format!("{:?}", plan.id),
            kind = format!("{:?}", plan.kind),
            updates = plan.updates.len(),
            returning = plan.returning.len(),
        );

        session.add_transaction_ops(TransactionOps::Writes(vec![WriteOp {
            id: plan.id,
            rows: TableData::Rows(plan.updates),
        }]))?;
        if !plan.returning.is_empty() {
            let finishing = RowSetFinishing {
                order_by: Vec::new(),
                limit: None,
                offset: 0,
                project: (0..plan.returning[0].0.iter().count()).collect(),
            };
            let max_returned_query_size = session.vars().max_query_result_size();
            let duration_histogram = session.metrics().row_set_finishing_seconds();

            return match finishing.finish(
                RowCollection::new(plan.returning, &finishing.order_by),
                plan.max_result_size,
                Some(max_returned_query_size),
                duration_histogram,
            ) {
                Ok((rows, _size_bytes)) => Ok(Self::send_immediate_rows(rows)),
                Err(e) => Err(AdapterError::ResultSize(e)),
            };
        }
        Ok(match plan.kind {
            MutationKind::Delete => ExecuteResponse::Deleted(affected_rows),
            MutationKind::Insert => ExecuteResponse::Inserted(affected_rows),
            MutationKind::Update => ExecuteResponse::Updated(affected_rows / 2),
        })
    }
}

/// Checks whether we should emit diagnostic
/// information associated with reading per-replica sources.
///
/// If an unrecoverable error is found (today: an untargeted read on a
/// cluster with a non-1 number of replicas), return that.  Otherwise,
/// return a list of associated notices (today: we always emit exactly
/// one notice if there are any per-replica log dependencies and if
/// `emit_introspection_query_notice` is set, and none otherwise.)
pub(crate) fn check_log_reads(
    catalog: &Catalog,
    cluster: &Cluster,
    source_ids: &BTreeSet<GlobalId>,
    target_replica: &mut Option<ReplicaId>,
    vars: &SessionVars,
) -> Result<impl IntoIterator<Item = AdapterNotice>, AdapterError>
where
{
    let log_names = source_ids
        .iter()
        .map(|gid| catalog.resolve_item_id(gid))
        .flat_map(|item_id| catalog.introspection_dependencies(item_id))
        .map(|item_id| catalog.get_entry(&item_id).name().item.clone())
        .collect::<Vec<_>>();

    if log_names.is_empty() {
        return Ok(None);
    }

    // Reading from log sources on replicated clusters is only allowed if a
    // target replica is selected. Otherwise, we have no way of knowing which
    // replica we read the introspection data from.
    let num_replicas = cluster.replicas().count();
    if target_replica.is_none() {
        if num_replicas == 1 {
            *target_replica = cluster.replicas().map(|r| r.replica_id).next();
        } else {
            return Err(AdapterError::UntargetedLogRead { log_names });
        }
    }

    // Ensure that logging is initialized for the target replica, lest
    // we try to read from a non-existing arrangement.
    let replica_id = target_replica.expect("set to `Some` above");
    let replica = &cluster.replica(replica_id).expect("Replica must exist");
    if !replica.config.compute.logging.enabled() {
        return Err(AdapterError::IntrospectionDisabled { log_names });
    }

    Ok(vars
        .emit_introspection_query_notice()
        .then_some(AdapterNotice::PerReplicaLogRead { log_names }))
}

/// Forward notices that we got from the optimizer.
pub(crate) fn emit_optimizer_notices(
    catalog: &Catalog,
    session: &Session,
    notices: &Vec<RawOptimizerNotice>,
) {
    // `for_session` below is expensive, so return early if there's nothing to do.
    if notices.is_empty() {
        return;
    }
    let humanizer = catalog.for_session(session);
    let system_vars = catalog.system_config();
    for notice in notices {
        let kind = OptimizerNoticeKind::from(notice);
        let notice_enabled = match kind {
            OptimizerNoticeKind::IndexAlreadyExists => {
                system_vars.enable_notices_for_index_already_exists()
            }
            OptimizerNoticeKind::IndexTooWideForLiteralConstraints => {
                system_vars.enable_notices_for_index_too_wide_for_literal_constraints()
            }
            OptimizerNoticeKind::IndexKeyEmpty => system_vars.enable_notices_for_index_empty_key(),
        };
        if notice_enabled {
            // We don't need to redact the notice parts because
            // `emit_optimizer_notices` is only called by the `sequence_~`
            // method for the statement that produces that notice.
            session.add_notice(AdapterNotice::OptimizerNotice {
                notice: notice.message(&humanizer, false).to_string(),
                hint: notice.hint(&humanizer, false).to_string(),
            });
        }
        session
            .metrics()
            .optimization_notices(&[kind.metric_label()])
            .inc_by(1);
    }
}

/// Evaluates a COPY TO target URI expression and validates it.
///
/// This function is shared between the old peek sequencing (sequence_copy_to)
/// and the new frontend peek sequencing to avoid code duplication.
pub fn eval_copy_to_uri(
    to: HirScalarExpr,
    session: &Session,
    catalog_state: &CatalogState,
) -> Result<Uri, AdapterError> {
    let style = ExprPrepOneShot {
        logical_time: EvalTime::NotAvailable,
        session,
        catalog_state,
    };
    let mut to = to.lower_uncorrelated()?;
    style.prep_scalar_expr(&mut to)?;
    let temp_storage = RowArena::new();
    let evaled = to.eval(&[], &temp_storage)?;
    if evaled == Datum::Null {
        coord_bail!("COPY TO target value can not be null");
    }
    let to_url = match Uri::from_str(evaled.unwrap_str()) {
        Ok(url) => {
            if url.scheme_str() != Some("s3") {
                coord_bail!("only 's3://...' urls are supported as COPY TO target");
            }
            url
        }
        Err(e) => coord_bail!("could not parse COPY TO target url: {}", e),
    };
    Ok(to_url)
}

/// Returns a future that will execute EXPLAIN FILTER PUSHDOWN, i.e., compute the filter pushdown
/// statistics for the given collections with the given MFPs.
///
/// (Shared helper fn between the old and new sequencing. This doesn't take the Coordinator as a
/// parameter, but instead just the specifically necessary things are passed in, so that the
/// frontend peek sequencing can also call it.)
pub(crate) async fn explain_pushdown_future_inner<
    I: IntoIterator<Item = (GlobalId, MapFilterProject)>,
>(
    session: &Session,
    catalog: &Catalog,
    storage_collections: &Arc<dyn StorageCollections<Timestamp = Timestamp> + Send + Sync>,
    as_of: Antichain<Timestamp>,
    mz_now: ResultSpec<'static>,
    imports: I,
) -> impl Future<Output = Result<ExecuteResponse, AdapterError>> + use<I> {
    let explain_timeout = *session.vars().statement_timeout();
    let mut futures = FuturesOrdered::new();
    for (id, mfp) in imports {
        let catalog_entry = catalog.get_entry_by_global_id(&id);
        let full_name = catalog
            .for_session(session)
            .resolve_full_name(&catalog_entry.name);
        let name = format!("{}", full_name);
        let relation_desc = catalog_entry
            .relation_desc()
            .expect("source should have a proper desc")
            .into_owned();
        let stats_future = storage_collections
            .snapshot_parts_stats(id, as_of.clone())
            .await;

        let mz_now = mz_now.clone();
        // These futures may block if the source is not yet readable at the as-of;
        // stash them in `futures` and only block on them in a separate task.
        // TODO(peek-seq): This complication won't be needed once this function will only be called
        // from the new peek sequencing, in which case it will be fine to block the current task.
        futures.push_back(async move {
            let snapshot_stats = match stats_future.await {
                Ok(stats) => stats,
                Err(e) => return Err(e),
            };
            let mut total_bytes = 0;
            let mut total_parts = 0;
            let mut selected_bytes = 0;
            let mut selected_parts = 0;
            for SnapshotPartStats {
                encoded_size_bytes: bytes,
                stats,
            } in &snapshot_stats.parts
            {
                let bytes = u64::cast_from(*bytes);
                total_bytes += bytes;
                total_parts += 1u64;
                let selected = match stats {
                    None => true,
                    Some(stats) => {
                        let stats = stats.decode();
                        let stats = RelationPartStats::new(
                            name.as_str(),
                            &snapshot_stats.metrics.pushdown.part_stats,
                            &relation_desc,
                            &stats,
                        );
                        stats.may_match_mfp(mz_now.clone(), &mfp)
                    }
                };

                if selected {
                    selected_bytes += bytes;
                    selected_parts += 1u64;
                }
            }
            Ok(Row::pack_slice(&[
                name.as_str().into(),
                total_bytes.into(),
                selected_bytes.into(),
                total_parts.into(),
                selected_parts.into(),
            ]))
        });
    }

    let fut = async move {
        match tokio::time::timeout(
            explain_timeout,
            futures::TryStreamExt::try_collect::<Vec<_>>(futures),
        )
        .await
        {
            Ok(Ok(rows)) => Ok(ExecuteResponse::SendingRowsImmediate {
                rows: Box::new(rows.into_row_iter()),
            }),
            Ok(Err(err)) => Err(err.into()),
            Err(_) => Err(AdapterError::StatementTimeout),
        }
    };
    fut
}

/// Generates EXPLAIN PLAN output.
/// (Shared helper fn between the old and new sequencing.)
pub(crate) async fn explain_plan_inner(
    session: &Session,
    catalog: &Catalog,
    df_meta: DataflowMetainfo,
    explain_ctx: ExplainPlanContext,
    optimizer: peek::Optimizer,
    insights_ctx: Option<Box<PlanInsightsContext>>,
) -> Result<Vec<Row>, AdapterError> {
    let ExplainPlanContext {
        config,
        format,
        stage,
        desc,
        optimizer_trace,
        ..
    } = explain_ctx;

    let desc = desc.expect("RelationDesc for SelectPlan in EXPLAIN mode");

    let session_catalog = catalog.for_session(session);
    let expr_humanizer = {
        let transient_items = btreemap! {
            optimizer.select_id() => TransientItem::new(
                Some(vec![GlobalId::Explain.to_string()]),
                Some(desc.iter_names().map(|c| c.to_string()).collect()),
            )
        };
        ExprHumanizerExt::new(transient_items, &session_catalog)
    };

    let finishing = if optimizer.finishing().is_trivial(desc.arity()) {
        None
    } else {
        Some(optimizer.finishing().clone())
    };

    let target_cluster = catalog.get_cluster(optimizer.cluster_id());
    let features = optimizer.config().features.clone();

    let rows = optimizer_trace
        .into_rows(
            format,
            &config,
            &features,
            &expr_humanizer,
            finishing,
            Some(target_cluster),
            df_meta,
            stage,
            plan::ExplaineeStatementKind::Select,
            insights_ctx,
        )
        .await?;

    Ok(rows)
}

/// Creates a statistics oracle for query optimization.
///
/// This is a free-standing function that can be called from both the old peek sequencing
/// and the new frontend peek sequencing.
pub(crate) async fn statistics_oracle(
    session: &Session,
    source_ids: &BTreeSet<GlobalId>,
    query_as_of: &Antichain<Timestamp>,
    is_oneshot: bool,
    system_config: &vars::SystemVars,
    storage_collections: &dyn StorageCollections<Timestamp = Timestamp>,
) -> Result<Box<dyn StatisticsOracle>, AdapterError> {
    if !session.vars().enable_session_cardinality_estimates() {
        return Ok(Box::new(EmptyStatisticsOracle));
    }

    let timeout = if is_oneshot {
        // TODO(mgree): ideally, we would shorten the timeout even more if we think the query could take the fast path
        system_config.optimizer_oneshot_stats_timeout()
    } else {
        system_config.optimizer_stats_timeout()
    };

    let cached_stats = mz_ore::future::timeout(
        timeout,
        CachedStatisticsOracle::new(source_ids, query_as_of, storage_collections),
    )
    .await;

    match cached_stats {
        Ok(stats) => Ok(Box::new(stats)),
        Err(mz_ore::future::TimeoutError::DeadlineElapsed) => {
            warn!(
                is_oneshot = is_oneshot,
                "optimizer statistics collection timed out after {}ms",
                timeout.as_millis()
            );

            Ok(Box::new(EmptyStatisticsOracle))
        }
        Err(mz_ore::future::TimeoutError::Inner(e)) => Err(AdapterError::Storage(e)),
    }
}

#[derive(Debug)]
struct CachedStatisticsOracle {
    cache: BTreeMap<GlobalId, usize>,
}

impl CachedStatisticsOracle {
    pub async fn new<T: TimelyTimestamp>(
        ids: &BTreeSet<GlobalId>,
        as_of: &Antichain<T>,
        storage_collections: &dyn mz_storage_client::storage_collections::StorageCollections<Timestamp = T>,
    ) -> Result<Self, StorageError<T>> {
        let mut cache = BTreeMap::new();

        for id in ids {
            let stats = storage_collections.snapshot_stats(*id, as_of.clone()).await;

            match stats {
                Ok(stats) => {
                    cache.insert(*id, stats.num_updates);
                }
                Err(StorageError::IdentifierMissing(id)) => {
                    ::tracing::debug!("no statistics for {id}")
                }
                Err(e) => return Err(e),
            }
        }

        Ok(Self { cache })
    }
}

impl StatisticsOracle for CachedStatisticsOracle {
    fn cardinality_estimate(&self, id: GlobalId) -> Option<usize> {
        self.cache.get(&id).map(|estimate| *estimate)
    }

    fn as_map(&self) -> BTreeMap<GlobalId, usize> {
        self.cache.clone()
    }
}

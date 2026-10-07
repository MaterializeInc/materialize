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

use std::sync::Arc;
use std::time::Duration;

use futures::FutureExt;
use futures::future::LocalBoxFuture;
use futures::stream::FuturesOrdered;
use inner::return_if_err;
use mz_expr::row::RowCollection;
use mz_expr::{MapFilterProject, MirRelationExpr, ResultSpec, RowSetFinishing};
use mz_ore::cast::CastFrom;
use mz_ore::soft_panic_or_log;
use mz_persist_client::stats::SnapshotPartStats;
use mz_repr::{CatalogItemId, Diff, GlobalId, IntoRowIterator, Row, Timestamp};
use mz_sql::catalog::{CatalogError, SessionCatalog};
use mz_sql::names::ResolvedIds;
use mz_sql::plan::{
    self, AbortTransactionPlan, CommitTransactionPlan, CreateRolePlan, CreateSourcePlanBundle,
    FetchPlan, MutationKind, Params, Plan, PlanKind, RaisePlan,
};
use mz_sql::rbac;
use mz_sql::session::metadata::SessionMetadata;
use mz_sql_parser::ast::{Raw, Statement};
use mz_storage_client::client::TableData;
use mz_storage_client::storage_collections::StorageCollections;
use mz_storage_types::connections::inline::IntoInlineConnection;
use mz_storage_types::stats::RelationPartStats;
use mz_transform::notice::{OptimizerNoticeApi, OptimizerNoticeKind, RawOptimizerNotice};
use timely::progress::Antichain;
use tokio::sync::oneshot;
use tracing::{Instrument, Level, Span, event, warn};

use crate::ExecuteContext;
use crate::catalog::Catalog;
use crate::command::{Command, ExecuteResponse, Response};
use crate::coord::{
    Coordinator, DeferredPlanStatement, Message, PlanStatement, TargetCluster, catalog_serving,
};
use crate::error::AdapterError;
use crate::notice::AdapterNotice;
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
// `sequence_staged` and `sequence_create_connection_stage_finish`.
// - The frontend sequencing paths call `emit_optimizer_notices` and
//   `explain_pushdown_future_inner`, which the coordinator uses as well.

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
        sql_impl_resolved_ids: ResolvedIds,
    ) -> LocalBoxFuture<'_, ()> {
        async move {
            let responses = ExecuteResponse::generated_from(&PlanKind::from(&plan));
            ctx.tx_mut().set_allowed(responses);

            if self.controller.read_only() && !plan.allowed_in_read_only() {
                ctx.retire(Err(AdapterError::ReadOnly));
                return;
            }

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

            if let (Some(cluster_id), Some(cluster_name), Some(statement_id)) = (
                target_cluster_id,
                target_cluster_name.clone(),
                ctx.extra().contents(),
            ) {
                self.set_statement_execution_cluster(statement_id, cluster_id, cluster_name);
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

            // The target-connection role only matters for `pg_cancel_backend`,
            // which the frontend sequences.
            if let Err(e) = rbac::check_plan(
                &session_catalog,
                None,
                ctx.session(),
                &plan,
                target_cluster_id,
                &resolved_ids,
                &sql_impl_resolved_ids,
            ) {
                return ctx.retire(Err(e.into()));
            }

            match plan {
                Plan::CreateSource(plan) => {
                    let (item_id, global_id) = return_if_err!(self.allocate_user_id().await, ctx);
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
                Plan::CreateIndex(plan) => {
                    self.sequence_create_index(ctx, plan, resolved_ids).await;
                }
                Plan::CreateMetricSink(plan) => {
                    self.sequence_create_metric_sink(ctx, plan, resolved_ids)
                        .await;
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
                                ps: PlanStatement::Plan {
                                    plan,
                                    resolved_ids,
                                    sql_impl_resolved_ids,
                                },
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
                Plan::ShowCreate(plan) => {
                    ctx.retire(Ok(Self::send_immediate_rows(plan.row)));
                }
                Plan::CopyFrom(plan) => {
                    self.sequence_copy_from(ctx, plan, target_cluster).await;
                }
                Plan::ExplainPlan(plan) => {
                    self.sequence_explain_plan(ctx, plan).await;
                }
                Plan::ExplainPushdown(plan) => {
                    self.sequence_explain_pushdown(ctx, plan).await;
                }
                Plan::ExplainSinkSchema(plan) => {
                    let result = self.sequence_explain_schema(plan);
                    ctx.retire(result);
                }
                // `SessionClient::execute_attempts` unrolls SQL `EXECUTE`, and `try_frontend_peek` and
                // `try_frontend_read_then_write` take over every statement that plans to one of the
                // others.
                // TODO(SQL-760): Drop the manual soft panic once internal errors soft-panic
                // centrally.
                plan @ (Plan::CopyTo(_)
                | Plan::Execute(_)
                | Plan::ExplainTimestamp(_)
                | Plan::Insert(_)
                | Plan::ReadThenWrite(_)
                | Plan::Select(_)
                | Plan::ShowColumns(_)
                | Plan::SideEffectingFunc(_)
                | Plan::Subscribe(_)) => {
                    let msg = format!(
                        "{:?} plan reached the coordinator despite frontend routing",
                        PlanKind::from(&plan)
                    );
                    soft_panic_or_log!("{msg}");
                    ctx.retire(Err(AdapterError::Internal(msg)));
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
                Plan::AlterSourceTimestampInterval(plan) => {
                    let result = self
                        .sequence_alter_source_timestamp_interval(&mut ctx, plan)
                        .await;
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
                    self.sequence_alter_materialized_view_apply_replacement_prepare(ctx, plan)
                        .await;
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
                    // Clearing the transaction would silently discard writes staged by an
                    // earlier statement of the same pipeline.
                    let txn = ctx.session().transaction();
                    let discardable =
                        matches!(txn, TransactionStatus::Started(_)) && !txn.contains_ops();
                    let ret = if discardable {
                        let (_, retire_notify) = self.clear_transaction(ctx.session_mut()).await;
                        ctx.delay_response_until(retire_notify);
                        self.drop_temp_items(ctx.session().conn_id()).await;
                        // NOTE: `reset()` resets session variables durably (see
                        // `SessionVars::reset_all`). This must stay durable and
                        // must not be reordered to depend on a transaction
                        // commit: the transaction was just cleared above, so
                        // there is no commit left to promote a staged reset.
                        let params = ctx.session_mut().reset();
                        Ok(ExecuteResponse::DiscardedAll { params })
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
        let (tx, internal_cmd_tx, mut session, extra, response_barriers) = ctx.into_parts();
        assert!(matches!(session.transaction(), TransactionStatus::Default));
        session.start_transaction_single_stmt(self.now_datetime());
        let conn_id = session.conn_id().unhandled();

        // Execute the saved statement in a temp transmitter so we can run COMMIT.
        let (sub_tx, sub_rx) = oneshot::channel();
        let sub_ct = ClientTransmitter::new(sub_tx, self.internal_cmd_tx.clone());
        let sub_ctx = ExecuteContext::from_parts_with_response_barriers(
            sub_ct,
            internal_cmd_tx,
            session,
            extra,
            response_barriers,
        );
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
                let result = Self::send_diffs(session, diffs_plan);
                // Let schema-fencing tests land an ALTER after packing rows
                // against this descriptor but before the transaction commits.
                fail::fail_point!("insert_after_pack_before_commit");
                result
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
                //
                // NOTE: This differs from PostgreSQL, where `UPDATE t SET x = x`
                // reports the number of rows matching the WHERE clause even when
                // no value changes. Because Materialize works in differential
                // dataflow, the +1 and -1 diffs for an unchanged row cancel out
                // during consolidation, so it reports 0 affected rows. This is
                // longstanding behavior and both read-then-write paths agree on
                // it.
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

/// Forward notices that we got from the optimizer.
pub(crate) fn emit_optimizer_notices(
    catalog: &Catalog,
    session: &Session,
    notices: &[RawOptimizerNotice],
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
            OptimizerNoticeKind::EqualsNull => system_vars.enable_notices_for_equals_null(),
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

/// Returns a future that will execute EXPLAIN FILTER PUSHDOWN, i.e., compute the filter pushdown
/// statistics for the given collections with the given MFPs.
///
/// The coordinator calls this for materialized views and the frontend peek sequencing for
/// SELECTs, so it takes only what it needs instead of the `Coordinator`.
pub(crate) async fn explain_pushdown_future_inner<
    I: IntoIterator<Item = (GlobalId, MapFilterProject)>,
>(
    session: &Session,
    catalog: &Catalog,
    storage_collections: &Arc<dyn StorageCollections + Send + Sync>,
    as_of: Antichain<Timestamp>,
    mz_now: ResultSpec<'static>,
    imports: I,
) -> impl Future<Output = Result<ExecuteResponse, AdapterError>> + use<I> {
    let mut explain_timeout = *session.vars().statement_timeout();
    // Timeout of 0 is equivalent to "off", meaning we will wait "forever."
    if explain_timeout == Duration::ZERO {
        explain_timeout = Duration::MAX;
    }
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
        // TODO(peek-seq): This complication won't be needed once only the frontend calls this
        // function, in which case it will be fine to block the current task.
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
                let selected = match stats.as_ref().and_then(|x| x.try_decode().ok()) {
                    // Also the arm for stats that do not decode, which a
                    // newer writer's stats kind can produce. Both report the
                    // part as selected, matching what a read of it would do.
                    None => true,
                    Some(stats) => {
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

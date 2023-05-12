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

use tracing::{event, Level};

use inner::return_if_err;
use mz_controller::clusters::ClusterId;
use mz_expr::OptimizedMirRelationExpr;
use mz_repr::explain::ExplainFormat;
use mz_repr::{GlobalId, Timestamp};
use mz_sql::catalog::CatalogCluster;
use mz_sql::plan::{
    AbortTransactionPlan, CommitTransactionPlan, CopyRowsPlan, CreateRolePlan, FetchPlan, Plan,
    PlanKind, RaisePlan, RotateKeysPlan,
};

use crate::command::{Command, ExecuteResponse};
use crate::coord::id_bundle::CollectionIdBundle;
use crate::coord::{Coordinator, Message};
use crate::error::AdapterError;
use crate::notice::AdapterNotice;
use crate::rbac;
use crate::session::{EndTransactionAction, PreparedStatement, Session, TransactionStatus};
use crate::util::{send_immediate_rows, ClientTransmitter};

use super::introspection;

// DO NOT make this visible in anyway, i.e. do not add any version of
// `pub` to this mod. The inner `sequence_X` methods are hidden in this
// private module to prevent anyone from calling them directly. All
// sequencing should be done through the `sequence_plan` method.
// This allows us to add catch-all logic that should be applied to all
// plans in `sequence_plan` and guarantee that no caller can circumvent
// that logic.
//
// The one exception is creating a role during connection startup. In
// this scenario, the session has not been properly initialized and we
// need to skip directly to creating role. We have a specific method,
// `sequence_create_role_for_startup` for this purpose.
mod inner;

impl Coordinator {
    #[tracing::instrument(level = "debug", skip_all)]
    pub(crate) async fn sequence_plan(
        &mut self,
        mut tx: ClientTransmitter<ExecuteResponse>,
        mut session: Session,
        plan: Plan,
        depends_on: Vec<GlobalId>,
    ) {
        event!(Level::TRACE, plan = format!("{:?}", plan));
        let responses = ExecuteResponse::generated_from(PlanKind::from(&plan));
        tx.set_allowed(responses);

        let session_catalog = self.catalog.for_session(&session);

        if let Err(e) =
            introspection::user_privilege_hack(&session_catalog, &session, &plan, &depends_on)
        {
            return tx.send(Err(e), session);
        }
        if let Err(e) = introspection::check_cluster_restrictions(&session_catalog, &plan) {
            return tx.send(Err(e), session);
        }

        // If our query only depends on system tables, a LaunchDarkly flag is enabled, and a
        // session var is set, then we automatically run the query on the mz_introspection cluster.
        let target_cluster =
            introspection::auto_run_on_introspection(&self.catalog, &session, &plan);
        let target_cluster_id = self
            .catalog()
            .resolve_target_cluster(target_cluster, &session)
            .ok()
            .map(|cluster| cluster.id());

        if let Err(e) = rbac::check_plan(
            &session_catalog,
            &session,
            &plan,
            target_cluster_id,
            &depends_on,
        ) {
            return tx.send(Err(e), session);
        }

        match plan {
            Plan::CreateSource(plan) => {
                let source_id =
                    return_if_err!(self.catalog_mut().allocate_user_id().await, tx, session);
                tx.send(
                    self.sequence_create_source(&mut session, vec![(source_id, plan, depends_on)])
                        .await,
                    session,
                );
            }
            Plan::CreateSources(plans) => {
                assert!(depends_on.is_empty(), "each plan has separate depends_on");
                let plans = plans
                    .into_iter()
                    .map(|plan| (plan.source_id, plan.plan, plan.depends_on))
                    .collect();
                tx.send(
                    self.sequence_create_source(&mut session, plans).await,
                    session,
                );
            }
            Plan::CreateConnection(plan) => {
                tx.send(
                    self.sequence_create_connection(&mut session, plan, depends_on)
                        .await,
                    session,
                );
            }
            Plan::CreateDatabase(plan) => {
                tx.send(
                    self.sequence_create_database(&mut session, plan).await,
                    session,
                );
            }
            Plan::CreateSchema(plan) => {
                tx.send(
                    self.sequence_create_schema(&mut session, plan).await,
                    session,
                );
            }
            Plan::CreateRole(plan) => {
                let res = self.sequence_create_role(&session, plan).await;
                if res.is_ok() {
                    self.maybe_send_rbac_notice(&session);
                }
                tx.send(res, session);
            }
            Plan::CreateCluster(plan) => {
                tx.send(self.sequence_create_cluster(&session, plan).await, session);
            }
            Plan::CreateClusterReplica(plan) => {
                tx.send(
                    self.sequence_create_cluster_replica(&session, plan).await,
                    session,
                );
            }
            Plan::CreateTable(plan) => {
                tx.send(
                    self.sequence_create_table(&mut session, plan, depends_on)
                        .await,
                    session,
                );
            }
            Plan::CreateSecret(plan) => {
                tx.send(
                    self.sequence_create_secret(&mut session, plan).await,
                    session,
                );
            }
            Plan::CreateSink(plan) => {
                self.sequence_create_sink(session, plan, depends_on, tx)
                    .await;
            }
            Plan::CreateView(plan) => {
                tx.send(
                    self.sequence_create_view(&mut session, plan, depends_on)
                        .await,
                    session,
                );
            }
            Plan::CreateMaterializedView(plan) => {
                tx.send(
                    self.sequence_create_materialized_view(&mut session, plan, depends_on)
                        .await,
                    session,
                );
            }
            Plan::CreateIndex(plan) => {
                tx.send(
                    self.sequence_create_index(&mut session, plan, depends_on)
                        .await,
                    session,
                );
            }
            Plan::CreateType(plan) => {
                tx.send(
                    self.sequence_create_type(&session, plan, depends_on).await,
                    session,
                );
            }
            Plan::DropObjects(plan) => {
                tx.send(
                    self.sequence_drop_objects(&mut session, plan).await,
                    session,
                );
            }
            Plan::DropOwned(plan) => {
                tx.send(self.sequence_drop_owned(&mut session, plan).await, session);
            }
            Plan::EmptyQuery => {
                tx.send(Ok(ExecuteResponse::EmptyQuery), session);
            }
            Plan::ShowAllVariables => {
                tx.send(self.sequence_show_all_variables(&session), session);
            }
            Plan::ShowVariable(plan) => {
                tx.send(self.sequence_show_variable(&session, plan), session);
            }
            Plan::SetVariable(plan) => {
                tx.send(self.sequence_set_variable(&mut session, plan), session);
            }
            Plan::ResetVariable(plan) => {
                tx.send(self.sequence_reset_variable(&mut session, plan), session);
            }
            Plan::StartTransaction(plan) => {
                if matches!(session.transaction(), TransactionStatus::InTransaction(_)) {
                    session.add_notice(AdapterNotice::ExistingTransactionInProgress);
                }
                let (session, result) = session.start_transaction(
                    self.now_datetime(),
                    plan.access,
                    plan.isolation_level,
                );
                tx.send(result.map(|_| ExecuteResponse::StartedTransaction), session)
            }
            Plan::CommitTransaction(CommitTransactionPlan {
                ref transaction_type,
            })
            | Plan::AbortTransaction(AbortTransactionPlan {
                ref transaction_type,
            }) => {
                let action = match &plan {
                    Plan::CommitTransaction(_) => EndTransactionAction::Commit,
                    Plan::AbortTransaction(_) => EndTransactionAction::Rollback,
                    _ => unreachable!(),
                };
                if session.transaction().is_implicit() && !transaction_type.is_implicit() {
                    // In Postgres, if a user sends a COMMIT or ROLLBACK in an
                    // implicit transaction, a warning is sent warning them.
                    // (The transaction is still closed and a new implicit
                    // transaction started, though.)
                    session
                        .add_notice(AdapterNotice::ExplicitTransactionControlInImplicitTransaction);
                }
                self.sequence_end_transaction(tx, session, action);
            }
            Plan::Peek(plan) => {
                self.sequence_peek(tx, session, plan, target_cluster).await;
            }
            Plan::Subscribe(plan) => {
                tx.send(
                    self.sequence_subscribe(&mut session, plan, depends_on, target_cluster)
                        .await,
                    session,
                );
            }
            Plan::ShowCreate(plan) => {
                tx.send(Ok(send_immediate_rows(vec![plan.row])), session);
            }
            Plan::CopyFrom(plan) => {
                tx.send(
                    Ok(ExecuteResponse::CopyFrom {
                        id: plan.id,
                        columns: plan.columns,
                        params: plan.params,
                    }),
                    session,
                );
            }
            Plan::CopyRows(CopyRowsPlan { id, columns, rows }) => {
                self.sequence_copy_rows(tx, session, id, columns, rows);
            }
            Plan::Explain(plan) => {
                self.sequence_explain(tx, session, plan, target_cluster);
            }
            Plan::Insert(plan) => {
                self.sequence_insert(tx, session, plan).await;
            }
            Plan::ReadThenWrite(plan) => {
                self.sequence_read_then_write(tx, session, plan).await;
            }
            Plan::AlterNoop(plan) => {
                tx.send(
                    Ok(ExecuteResponse::AlteredObject(plan.object_type)),
                    session,
                );
            }
            Plan::AlterItemRename(plan) => {
                tx.send(
                    self.sequence_alter_item_rename(&session, plan).await,
                    session,
                );
            }
            Plan::AlterIndexSetOptions(plan) => {
                tx.send(self.sequence_alter_index_set_options(plan), session);
            }
            Plan::AlterIndexResetOptions(plan) => {
                tx.send(self.sequence_alter_index_reset_options(plan), session);
            }
            Plan::AlterRole(plan) => {
                let res = self.sequence_alter_role(&session, plan).await;
                if res.is_ok() {
                    self.maybe_send_rbac_notice(&session);
                }
                tx.send(res, session);
            }
            Plan::AlterSecret(plan) => {
                tx.send(self.sequence_alter_secret(&session, plan).await, session);
            }
            Plan::AlterSink(plan) => {
                tx.send(self.sequence_alter_sink(&session, plan).await, session);
            }
            Plan::AlterSource(plan) => {
                tx.send(self.sequence_alter_source(&session, plan).await, session);
            }
            Plan::AlterSystemSet(plan) => {
                tx.send(
                    self.sequence_alter_system_set(&session, plan).await,
                    session,
                );
            }
            Plan::AlterSystemReset(plan) => {
                tx.send(
                    self.sequence_alter_system_reset(&session, plan).await,
                    session,
                );
            }
            Plan::AlterSystemResetAll(plan) => {
                tx.send(
                    self.sequence_alter_system_reset_all(&session, plan).await,
                    session,
                );
            }
            Plan::DiscardTemp => {
                self.drop_temp_items(&session).await;
                tx.send(Ok(ExecuteResponse::DiscardedTemp), session);
            }
            Plan::DiscardAll => {
                let ret = if let TransactionStatus::Started(_) = session.transaction() {
                    self.drop_temp_items(&session).await;
                    let conn_meta = self
                        .active_conns
                        .get_mut(&session.conn_id())
                        .expect("must exist for active session");
                    let drop_sinks = std::mem::take(&mut conn_meta.drop_sinks);
                    self.drop_compute_sinks(drop_sinks);
                    session.reset();
                    Ok(ExecuteResponse::DiscardedAll)
                } else {
                    Err(AdapterError::OperationProhibitsTransaction(
                        "DISCARD ALL".into(),
                    ))
                };
                tx.send(ret, session);
            }
            Plan::Declare(plan) => {
                let param_types = vec![];
                self.declare(tx, session, plan.name, plan.stmt, param_types);
            }
            Plan::Fetch(FetchPlan {
                name,
                count,
                timeout,
            }) => {
                tx.send(
                    Ok(ExecuteResponse::Fetch {
                        name,
                        count,
                        timeout,
                    }),
                    session,
                );
            }
            Plan::Close(plan) => {
                if session.remove_portal(&plan.name) {
                    tx.send(Ok(ExecuteResponse::ClosedCursor), session);
                } else {
                    tx.send(Err(AdapterError::UnknownCursor(plan.name)), session);
                }
            }
            Plan::Prepare(plan) => {
                if session
                    .get_prepared_statement_unverified(&plan.name)
                    .is_some()
                {
                    tx.send(
                        Err(AdapterError::PreparedStatementExists(plan.name)),
                        session,
                    );
                } else {
                    session.set_prepared_statement(
                        plan.name,
                        PreparedStatement::new(
                            Some(plan.stmt),
                            plan.desc,
                            self.catalog().transient_revision(),
                        ),
                    );
                    tx.send(Ok(ExecuteResponse::Prepare), session);
                }
            }
            Plan::Execute(plan) => {
                match self.sequence_execute(&mut session, plan) {
                    Ok(portal_name) => {
                        self.internal_cmd_tx
                            .send(Message::Command(Command::Execute {
                                portal_name,
                                session,
                                tx: tx.take(),
                                span: tracing::Span::none(),
                            }))
                            .expect("sending to self.internal_cmd_tx cannot fail");
                    }
                    Err(err) => tx.send(Err(err), session),
                };
            }
            Plan::Deallocate(plan) => match plan.name {
                Some(name) => {
                    if session.remove_prepared_statement(&name) {
                        tx.send(Ok(ExecuteResponse::Deallocate { all: false }), session);
                    } else {
                        tx.send(Err(AdapterError::UnknownPreparedStatement(name)), session);
                    }
                }
                None => {
                    session.remove_all_prepared_statements();
                    tx.send(Ok(ExecuteResponse::Deallocate { all: true }), session);
                }
            },
            Plan::Raise(RaisePlan { severity }) => {
                session.add_notice(AdapterNotice::UserRequested { severity });
                tx.send(Ok(ExecuteResponse::Raised), session);
            }
            Plan::RotateKeys(RotateKeysPlan { id }) => {
                tx.send(self.sequence_rotate_keys(&session, id).await, session);
            }
            Plan::GrantPrivilege(plan) => {
                tx.send(
                    self.sequence_grant_privilege(&mut session, plan).await,
                    session,
                );
            }
            Plan::RevokePrivilege(plan) => {
                tx.send(
                    self.sequence_revoke_privilege(&mut session, plan).await,
                    session,
                );
            }
            Plan::GrantRole(plan) => {
                tx.send(self.sequence_grant_role(&mut session, plan).await, session);
            }
            Plan::RevokeRole(plan) => {
                tx.send(self.sequence_revoke_role(&mut session, plan).await, session);
            }
            Plan::AlterOwner(plan) => {
                tx.send(self.sequence_alter_owner(&mut session, plan).await, session);
            }
        }
    }

    /// Creates a role during connection startup.
    ///
    /// This should not be called from anywhere except connection startup.
    #[tracing::instrument(level = "debug", skip(self))]
    pub(crate) async fn sequence_create_role_for_startup(
        &mut self,
        session: &Session,
        plan: CreateRolePlan,
    ) -> Result<ExecuteResponse, AdapterError> {
        self.sequence_create_role(session, plan).await
    }

    pub(crate) fn sequence_explain_timestamp_finish(
        &mut self,
        session: &mut Session,
        format: ExplainFormat,
        cluster_id: ClusterId,
        optimized_plan: OptimizedMirRelationExpr,
        id_bundle: CollectionIdBundle,
        real_time_recency_ts: Option<Timestamp>,
    ) -> Result<ExecuteResponse, AdapterError> {
        self.sequence_explain_timestamp_finish_inner(
            session,
            format,
            cluster_id,
            optimized_plan,
            id_bundle,
            real_time_recency_ts,
        )
    }

    pub(crate) fn allocate_transient_id(&mut self) -> Result<GlobalId, AdapterError> {
        let id = self.transient_id_counter;
        if id == u64::MAX {
            coord_bail!("id counter overflows i64");
        }
        self.transient_id_counter += 1;
        Ok(GlobalId::Transient(id))
    }

    fn maybe_send_rbac_notice(&self, session: &Session) {
        if !rbac::is_rbac_enabled_for_session(self.catalog.system_config(), session) {
            if !self.catalog.system_config().enable_ld_rbac_checks() {
                session.add_notice(AdapterNotice::RbacSystemDisabled);
            } else {
                session.add_notice(AdapterNotice::RbacUserDisabled);
            }
        }
    }
}

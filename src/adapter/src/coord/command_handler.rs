// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Logic for  processing client [`Command`]s. Each [`Command`] is initiated by a
//! client via some external Materialize API (ex: HTTP and psql).

use std::collections::BTreeSet;
use std::sync::Arc;

use mz_adapter_types::connection::{ConnectionId, ConnectionIdType};
use mz_compute_client::protocol::response::PeekResponse;
use mz_ore::now::NowFn;
use mz_ore::task;
use mz_ore::tracing::OpenTelemetryContext;
use mz_repr::role_id::RoleId;
use mz_sql::ast::{
    CopyRelation, CopyStatement, InsertSource, Query, Raw, SetExpr, Statement, SubscribeStatement,
};
use mz_sql::catalog::RoleAttributes;
use mz_sql::names::{PartialItemName, ResolvedIds};
use mz_sql::plan::{
    AbortTransactionPlan, CommitTransactionPlan, CreateRolePlan, Params, Plan, TransactionType,
};
use mz_sql::rbac;
use mz_sql::rbac::CREATE_ITEM_USAGE;
use mz_sql::session::user::User;
use mz_sql::session::vars::{
    EndTransactionAction, OwnedVarInput, Var, STATEMENT_LOGGING_SAMPLE_RATE,
};
use opentelemetry::trace::TraceContextExt;
use tokio::sync::{mpsc, oneshot, watch};
use tracing::Instrument;
use tracing_opentelemetry::OpenTelemetrySpanExt;

use crate::catalog::{CatalogItem, DataSourceDesc, Source};
use crate::command::{
    Canceled, CatalogSnapshot, Command, ExecuteResponse, GetVariablesResponse, StartupResponse,
};
use crate::coord::appends::{Deferred, PendingWriteTxn};
use crate::coord::peek::PendingPeek;
use crate::coord::{ConnMeta, Coordinator, Message, PendingTxn, PurifiedStatementReady};
use crate::error::AdapterError;
use crate::notice::AdapterNotice;
use crate::session::{Session, TransactionOps, TransactionStatus};
use crate::util::{ClientTransmitter, ResultExt};
use crate::webhook::{AppendWebhookResponse, AppendWebhookValidator};
use crate::{catalog, metrics, ExecuteContext};

use super::ExecuteContextExtra;

impl Coordinator {
    pub(crate) async fn handle_command(&mut self, mut cmd: Command) {
        if let Some(session) = cmd.session_mut() {
            session.apply_external_metadata_updates();
        }
        match cmd {
            Command::Startup {
                cancel_tx,
                tx,
                set_setting_keys,
                user,
                conn_id,
                secret_key,
                uuid,
                application_name,
                notice_tx,
            } => {
                // Note: We purposefully do not use a ClientTransmitter here because startup
                // handles errors and cleanup of sessions itself.
                self.handle_startup(
                    cancel_tx,
                    tx,
                    set_setting_keys,
                    user,
                    conn_id,
                    secret_key,
                    uuid,
                    application_name,
                    notice_tx,
                )
                .await;
            }

            Command::Execute {
                portal_name,
                session,
                tx,
                span,
                outer_ctx_extra,
            } => {
                let tx = ClientTransmitter::new(tx, self.internal_cmd_tx.clone());
                let span = span.or_current();
                tracing::Span::current().add_link(span.context().span().span_context().clone());

                self.handle_execute(portal_name, session, tx, outer_ctx_extra)
                    .instrument(span)
                    .await;
            }

            Command::RetireExecute { data, reason } => self.retire_execution(reason, data),

            Command::CancelRequest {
                conn_id,
                secret_key,
            } => {
                self.handle_cancel(conn_id, secret_key);
            }

            Command::PrivilegedCancelRequest { conn_id } => {
                self.handle_privileged_cancel(conn_id);
            }

            Command::AppendWebhook {
                database,
                schema,
                name,
                conn_id,
                tx,
            } => {
                self.handle_append_webhook(database, schema, name, conn_id, tx);
            }

            Command::GetSystemVars { conn_id, tx } => {
                let conn = &self.active_conns[&conn_id];
                let vars =
                    GetVariablesResponse::new(self.catalog.system_config().iter().filter(|var| {
                        var.visible(conn.user(), Some(self.catalog.system_config()))
                            .is_ok()
                    }));
                let _ = tx.send(Ok(vars));
            }

            Command::SetSystemVars { vars, conn_id, tx } => {
                let mut ops = Vec::with_capacity(vars.len());
                let conn = &self.active_conns[&conn_id];

                for (name, value) in vars {
                    if let Err(e) = self.catalog().system_config().get(&name).and_then(|var| {
                        var.visible(conn.user(), Some(self.catalog.system_config()))
                    }) {
                        let _ = tx.send(Err(e.into()));
                        return;
                    }

                    ops.push(catalog::Op::UpdateSystemConfiguration {
                        name,
                        value: OwnedVarInput::Flat(value),
                    });
                }

                let result = self.catalog_transact_conn(Some(&conn_id), ops).await;
                let _ = tx.send(result);
            }

            Command::Terminate { conn_id, tx } => {
                self.handle_terminate(conn_id).await;
                // Note: We purposefully do not use a ClientTransmitter here because we're already
                // terminating the provided session.
                if let Some(tx) = tx {
                    let _ = tx.send(Ok(()));
                }
            }

            Command::Commit {
                action,
                session,
                tx,
                otel_ctx,
            } => {
                let tx = ClientTransmitter::new(tx, self.internal_cmd_tx.clone());
                // We reach here not through a statement execution, but from the
                // "commit" pgwire command. Thus, we just generate a default statement
                // execution context (once statement logging is implemented, this will cause nothing to be logged
                // when the execution finishes.)
                let ctx = ExecuteContext::from_parts(
                    tx,
                    self.internal_cmd_tx.clone(),
                    session,
                    Default::default(),
                );
                let plan = match action {
                    EndTransactionAction::Commit => {
                        Plan::CommitTransaction(CommitTransactionPlan {
                            transaction_type: TransactionType::Implicit,
                        })
                    }
                    EndTransactionAction::Rollback => {
                        Plan::AbortTransaction(AbortTransactionPlan {
                            transaction_type: TransactionType::Implicit,
                        })
                    }
                };
                // TODO: We need a Span that is not none for the otel_ctx to
                // attach the parent relationship to. If we do the TODO to swap
                // otel_ctx in `Command::Commit` for a Span, we can downgrade
                // this to a debug_span.
                let span = tracing::info_span!("message_command (commit)").or_current();
                span.in_scope(|| otel_ctx.attach_as_parent());
                tracing::Span::current().add_link(span.context().span().span_context().clone());

                self.sequence_plan(ctx, plan, ResolvedIds(BTreeSet::new()))
                    .instrument(span)
                    .await;
            }

            Command::CatalogSnapshot { tx } => {
                let _ = tx.send(CatalogSnapshot {
                    catalog: self.owned_catalog(),
                });
            }

            Command::CheckConsistency { tx } => {
                let _ = tx.send(self.check_consistency());
            }
        }
    }

    #[tracing::instrument(level = "debug", skip(self, cancel_tx, tx, secret_key, notice_tx))]
    async fn handle_startup(
        &mut self,
        cancel_tx: Arc<watch::Sender<Canceled>>,
        tx: oneshot::Sender<Result<StartupResponse, AdapterError>>,
        set_setting_keys: Vec<String>,
        user: User,
        conn_id: ConnectionId,
        secret_key: u32,
        uuid: uuid::Uuid,
        application_name: String,
        notice_tx: mpsc::UnboundedSender<AdapterNotice>,
    ) {
        // Early return if successful, otherwise cleanup any possible state.
        match self.handle_startup_inner(&user, &conn_id).await {
            Ok(role_id) => {
                let mut session_vars = Vec::new();
                if !set_setting_keys
                    .iter()
                    .any(|k| k == STATEMENT_LOGGING_SAMPLE_RATE.name())
                {
                    let default = self
                        .catalog()
                        .state()
                        .system_config()
                        .statement_logging_default_sample_rate();
                    session_vars.push((
                        STATEMENT_LOGGING_SAMPLE_RATE.name().to_string(),
                        OwnedVarInput::Flat(default.to_string()),
                    ));
                }
                let role_vars = self
                    .catalog()
                    .get_role(&role_id)
                    .vars()
                    .map(|(name, val)| (name.to_owned(), val.clone()))
                    .collect();

                let session_type = metrics::session_type_label_value(&user);
                self.metrics
                    .active_sessions
                    .with_label_values(&[session_type])
                    .inc();
                let conn = ConnMeta {
                    cancel_tx,
                    secret_key,
                    notice_tx,
                    drop_sinks: Vec::new(),
                    connected_at: self.now(),
                    user,
                    application_name,
                    uuid,
                    conn_id: conn_id.clone(),
                    authenticated_role: role_id,
                };
                let update = self.catalog().state().pack_session_update(&conn, 1);
                self.begin_session_for_statement_logging(&conn);
                self.active_conns.insert(conn_id.clone(), conn);

                // Note: Do NOT await the notify here, we pass this back to whatever requested the
                // startup to prevent blocking the Coordinator on a builtin table update.
                let notify = self.send_builtin_table_updates_defer(vec![update]);

                let resp = Ok(StartupResponse {
                    role_id,
                    write_notify: notify,
                    session_vars,
                    role_vars,
                    catalog: self.owned_catalog(),
                });
                if tx.send(resp).is_err() {
                    // Failed to send to adapter, but everything is setup so we can terminate
                    // normally.
                    self.handle_terminate(conn_id).await;
                }
            }
            Err(_) => {
                // Error during startup or sending to adapter, cleanup possible state created by
                // handle_startup_inner. A user may have been created and it can stay; no need to
                // delete it.
                self.catalog_mut()
                    .drop_temporary_schema(&conn_id)
                    .unwrap_or_terminate("unable to drop temporary schema");
            }
        }
    }

    // Failible startup work that needs to be cleaned up on error.
    async fn handle_startup_inner(
        &mut self,
        user: &User,
        conn_id: &ConnectionId,
    ) -> Result<RoleId, AdapterError> {
        if self.catalog().try_get_role_by_name(&user.name).is_none() {
            // If the user has made it to this point, that means they have been fully authenticated.
            // This includes preventing any user, except a pre-defined set of system users, from
            // connecting to an internal port. Therefore it's ok to always create a new role for the
            // user.
            let attributes = RoleAttributes::new();
            let plan = CreateRolePlan {
                name: user.name.to_string(),
                attributes,
            };
            self.sequence_create_role_for_startup(plan).await?;
        }
        let role_id = self
            .catalog()
            .try_get_role_by_name(&user.name)
            .expect("created above")
            .id;
        self.catalog_mut()
            .create_temporary_schema(conn_id, role_id)?;
        Ok(role_id)
    }

    /// Handles an execute command.
    #[tracing::instrument(level = "debug", skip_all)]
    pub(crate) async fn handle_execute(
        &mut self,
        portal_name: String,
        mut session: Session,
        tx: ClientTransmitter<ExecuteResponse>,
        // If this command was part of another execute command
        // (for example, executing a `FETCH` statement causes an execute to be
        //  issued for the cursor it references),
        // then `outer_context` should be `Some`.
        // This instructs the coordinator that the
        // outer execute should be considered finished once the inner one is.
        outer_context: Option<ExecuteContextExtra>,
    ) {
        if session.vars().emit_trace_id_notice() {
            let span_context = tracing::Span::current()
                .context()
                .span()
                .span_context()
                .clone();
            if span_context.is_valid() {
                session.add_notice(AdapterNotice::QueryTrace {
                    trace_id: span_context.trace_id(),
                });
            }
        }

        if let Err(err) = self.verify_portal(&mut session, &portal_name) {
            // If statement logging hasn't started yet, we don't need
            // to add any "end" event, so just make up a no-op
            // `ExecuteContextExtra` here, via `Default::default`.
            //
            // It's a bit unfortunate because the edge case of failed
            // portal verifications won't show up in statement
            // logging, but there seems to be nothing else we can do,
            // because we need access to the portal to begin logging.
            //
            // Another option would be to log a begin and end event, but just fill in NULLs
            // for everything we get from the portal (prepared statement id, params).
            let extra = outer_context.unwrap_or_else(Default::default);
            let ctx = ExecuteContext::from_parts(tx, self.internal_cmd_tx.clone(), session, extra);
            return ctx.retire(Err(err));
        }

        // The reference to `portal` can't outlive `session`, which we
        // use to construct the context, so scope the reference to this block where we
        // get everything we need from the portal for later.
        let (stmt, ctx, params) = {
            let portal = session
                .get_portal_unverified(&portal_name)
                .expect("known to exist");
            let params = portal.parameters.clone();
            let stmt = portal.stmt.clone();
            let logging = Arc::clone(&portal.logging);

            let extra = if let Some(extra) = outer_context {
                // We are executing in the context of another SQL statement, so we don't
                // want to begin statement logging anew. The context of the actual statement
                // being executed is the one that should be retired once this finishes.
                extra
            } else {
                // This is a new statement, log it and return the context
                let maybe_uuid =
                    self.begin_statement_execution(&mut session, params.clone(), &logging);

                ExecuteContextExtra::new(maybe_uuid)
            };
            let ctx = ExecuteContext::from_parts(tx, self.internal_cmd_tx.clone(), session, extra);
            (stmt, ctx, params)
        };

        let stmt = match stmt {
            Some(stmt) => stmt,
            None => return ctx.retire(Ok(ExecuteResponse::EmptyQuery)),
        };

        let session_type = metrics::session_type_label_value(ctx.session().user());
        let stmt_type = metrics::statement_type_label_value(&stmt);
        self.metrics
            .query_total
            .with_label_values(&[session_type, stmt_type])
            .inc();
        match &stmt {
            Statement::Subscribe(SubscribeStatement { output, .. })
            | Statement::Copy(CopyStatement {
                relation: CopyRelation::Subscribe(SubscribeStatement { output, .. }),
                ..
            }) => {
                self.metrics
                    .subscribe_outputs
                    .with_label_values(&[
                        session_type,
                        metrics::subscribe_output_label_value(output),
                    ])
                    .inc();
            }
            _ => {}
        }

        self.handle_execute_inner(stmt, params, ctx).await
    }

    #[tracing::instrument(level = "debug", skip(self, ctx))]
    pub(crate) async fn handle_execute_inner(
        &mut self,
        stmt: Statement<Raw>,
        params: Params,
        mut ctx: ExecuteContext,
    ) {
        // Verify that this statement type can be executed in the current
        // transaction state.
        match ctx.session().transaction() {
            // By this point we should be in a running transaction.
            TransactionStatus::Default => unreachable!(),

            // Failed transactions have already been checked in pgwire for a safe statement
            // (COMMIT, ROLLBACK, etc.) and can proceed.
            TransactionStatus::Failed(_) => {}

            // Started is a deceptive name, and means different things depending on which
            // protocol was used. It's either exactly one statement (known because this
            // is the simple protocol and the parser parsed the entire string, and it had
            // one statement). Or from the extended protocol, it means *some* query is
            // being executed, but there might be others after it before the Sync (commit)
            // message. Postgres handles this by teaching Started to eagerly commit certain
            // statements that can't be run in a transaction block.
            TransactionStatus::Started(_) => {
                if let Statement::Declare(_) = stmt {
                    // Declare is an exception. Although it's not against any spec to execute
                    // it, it will always result in nothing happening, since all portals will be
                    // immediately closed. Users don't know this detail, so this error helps them
                    // understand what's going wrong. Postgres does this too.
                    return ctx.retire(Err(AdapterError::OperationRequiresTransaction(
                        "DECLARE CURSOR".into(),
                    )));
                }

                // TODO(mjibson): The current code causes DDL statements (well, any statement
                // that doesn't call `add_transaction_ops`) to execute outside of the extended
                // protocol transaction. For example, executing in extended a SELECT, then
                // CREATE, then SELECT, followed by a Sync would register the transaction
                // as read only in the first SELECT, then the CREATE ignores the transaction
                // ops, and the last SELECT will use the timestamp from the first. This isn't
                // correct, but this is an edge case that we can fix later.
            }

            // Implicit or explicit transactions.
            //
            // Implicit transactions happen when a multi-statement query is executed
            // (a "simple query"). However if a "BEGIN" appears somewhere in there,
            // then the existing implicit transaction will be upgraded to an explicit
            // transaction. Thus, we should not separate what implicit and explicit
            // transactions can do unless there's some additional checking to make sure
            // something disallowed in explicit transactions did not previously take place
            // in the implicit portion.
            TransactionStatus::InTransactionImplicit(_) | TransactionStatus::InTransaction(_) => {
                match stmt {
                    // Statements that are safe in a transaction. We still need to verify that we
                    // don't interleave reads and writes since we can't perform those serializably.
                    Statement::Close(_)
                    | Statement::Commit(_)
                    | Statement::Copy(_)
                    | Statement::Deallocate(_)
                    | Statement::Declare(_)
                    | Statement::Discard(_)
                    | Statement::Execute(_)
                    | Statement::ExplainPlan(_)
                    | Statement::ExplainTimestamp(_)
                    | Statement::ExplainSinkSchema(_)
                    | Statement::Fetch(_)
                    | Statement::Prepare(_)
                    | Statement::Rollback(_)
                    | Statement::Select(_)
                    | Statement::SetTransaction(_)
                    | Statement::Show(_)
                    | Statement::SetVariable(_)
                    | Statement::ResetVariable(_)
                    | Statement::StartTransaction(_)
                    | Statement::Subscribe(_)
                    | Statement::Raise(_) => {
                        // Always safe.
                    }

                    Statement::Insert(ref insert_statement)
                        if matches!(
                            insert_statement.source,
                            InsertSource::Query(Query {
                                body: SetExpr::Values(..),
                                ..
                            }) | InsertSource::DefaultValues
                        ) =>
                    {
                        // Inserting from default? values statements
                        // is always safe.
                    }

                    Statement::AlterObjectRename(_) | Statement::AlterObjectSwap(_) => {
                        let state = self.catalog().for_session(ctx.session()).state().clone();
                        let revision = self.catalog().transient_revision();

                        // Initialize our transaction with a set of empty ops, or return an error
                        // if we can't run a DDL transaction
                        let txn_status = ctx.session_mut().transaction_mut();
                        if let Err(err) = txn_status.add_ops(TransactionOps::DDL {
                            ops: vec![],
                            state,
                            revision,
                        }) {
                            return ctx.retire(Err(err));
                        }
                    }

                    // Statements below must by run singly (in Started).
                    Statement::AlterCluster(_)
                    | Statement::AlterConnection(_)
                    | Statement::AlterDefaultPrivileges(_)
                    | Statement::AlterIndex(_)
                    | Statement::AlterSetCluster(_)
                    | Statement::AlterOwner(_)
                    | Statement::AlterRole(_)
                    | Statement::AlterSecret(_)
                    | Statement::AlterSink(_)
                    | Statement::AlterSource(_)
                    | Statement::AlterSystemReset(_)
                    | Statement::AlterSystemResetAll(_)
                    | Statement::AlterSystemSet(_)
                    | Statement::CreateCluster(_)
                    | Statement::CreateClusterReplica(_)
                    | Statement::CreateConnection(_)
                    | Statement::CreateDatabase(_)
                    | Statement::CreateIndex(_)
                    | Statement::CreateMaterializedView(_)
                    | Statement::CreateRole(_)
                    | Statement::CreateSchema(_)
                    | Statement::CreateSecret(_)
                    | Statement::CreateSink(_)
                    | Statement::CreateSource(_)
                    | Statement::CreateSubsource(_)
                    | Statement::CreateTable(_)
                    | Statement::CreateType(_)
                    | Statement::CreateView(_)
                    | Statement::CreateWebhookSource(_)
                    | Statement::Delete(_)
                    | Statement::DropObjects(_)
                    | Statement::DropOwned(_)
                    | Statement::GrantPrivileges(_)
                    | Statement::GrantRole(_)
                    | Statement::Insert(_)
                    | Statement::ReassignOwned(_)
                    | Statement::RevokePrivileges(_)
                    | Statement::RevokeRole(_)
                    | Statement::Update(_)
                    | Statement::ValidateConnection(_)
                    | Statement::Comment(_) => {
                        let txn_status = ctx.session_mut().transaction_mut();

                        // If we're not in an implicit transaction and we could generate exactly one
                        // valid ExecuteResponse, we can delay execution until commit.
                        if !txn_status.is_implicit() {
                            // Statements whose tag is trivial (known only from an unexecuted statement) can
                            // be run in a special single-statement explicit mode. In this mode (`BEGIN;
                            // <stmt>; COMMIT`), we generate the expected tag from a successful <stmt>, but
                            // delay execution until `COMMIT`.
                            if let Ok(resp) = ExecuteResponse::try_from(&stmt) {
                                if let Err(err) = txn_status
                                    .add_ops(TransactionOps::SingleStatement { stmt, params })
                                {
                                    ctx.retire(Err(err));
                                    return;
                                }
                                ctx.retire(Ok(resp));
                                return;
                            }
                        }

                        return ctx.retire(Err(AdapterError::OperationProhibitsTransaction(
                            stmt.to_string(),
                        )));
                    }
                }
            }
        }

        let catalog = self.catalog();
        let catalog = catalog.for_session(ctx.session());
        let original_stmt = stmt.clone();
        let (stmt, resolved_ids) = match mz_sql::names::resolve(&catalog, stmt) {
            Ok(resolved) => resolved,
            Err(e) => return ctx.retire(Err(e.into())),
        };
        // N.B. The catalog can change during purification so we must validate that the dependencies still exist after
        // purification.  This should be done back on the main thread.
        // We do the validation:
        //   - In the handler for `Message::PurifiedStatementReady`, before we handle the purified statement.
        // If we add special handling for more types of `Statement`s, we'll need to ensure similar verification
        // occurs.
        match stmt {
            // `CREATE SOURCE` statements must be purified off the main
            // coordinator thread of control.
            stmt @ (Statement::CreateSource(_)
            | Statement::AlterSource(_)
            | Statement::CreateSink(_)) => {
                let internal_cmd_tx = self.internal_cmd_tx.clone();
                let conn_id = ctx.session().conn_id().clone();
                let catalog = self.owned_catalog();
                let now = self.now();
                let connection_context = self.connection_context.clone();
                let otel_ctx = OpenTelemetryContext::obtain();
                task::spawn(|| format!("purify:{conn_id}"), async move {
                    let catalog = catalog.for_session(ctx.session());

                    // Checks if the session is authorized to purify a statement. Usually
                    // authorization is checked after planning, however purification happens before
                    // planning, which may require the use of some connections and secrets.
                    if let Err(e) = rbac::check_usage(
                        &catalog,
                        ctx.session().role_metadata(),
                        ctx.session().vars(),
                        &resolved_ids,
                        &CREATE_ITEM_USAGE,
                    ) {
                        return ctx.retire(Err(e.into()));
                    }

                    let result =
                        mz_sql::pure::purify_statement(catalog, now, stmt, connection_context)
                            .await
                            .map_err(|e| e.into());
                    // It is not an error for purification to complete after `internal_cmd_rx` is dropped.
                    let result = internal_cmd_tx.send(Message::PurifiedStatementReady(
                        PurifiedStatementReady {
                            ctx,
                            result,
                            params,
                            resolved_ids,
                            original_stmt,
                            otel_ctx,
                        },
                    ));
                    if let Err(e) = result {
                        tracing::warn!("internal_cmd_rx dropped before we could send: {:?}", e);
                    }
                });
            }

            // `CREATE SUBSOURCE` statements are disallowed for users and are only generated
            // automatically as part of purification
            Statement::CreateSubsource(_) => ctx.retire(Err(AdapterError::Unsupported(
                "CREATE SUBSOURCE statements",
            ))),

            // All other statements are handled immediately.
            _ => match self.plan_statement(ctx.session(), stmt, &params, &resolved_ids) {
                Ok(plan) => self.sequence_plan(ctx, plan, resolved_ids).await,
                Err(e) => ctx.retire(Err(e)),
            },
        }
    }

    /// Instruct the dataflow layer to cancel any ongoing, interactive work for
    /// the named `conn_id` if the correct secret key is specified.
    ///
    /// Note: Here we take a [`ConnectionIdType`] as opposed to an owned
    /// `ConnectionId` because this method gets called by external clients when
    /// they request to cancel a request.
    #[tracing::instrument(level = "debug", skip(self, secret_key))]
    fn handle_cancel(&mut self, conn_id: ConnectionIdType, secret_key: u32) {
        if let Some((id_handle, conn_meta)) = self.active_conns.get_key_value(&conn_id) {
            // If the secret key specified by the client doesn't match the
            // actual secret key for the target connection, we treat this as a
            // rogue cancellation request and ignore it.
            if conn_meta.secret_key != secret_key {
                return;
            }

            // Now that we've verified the secret key, this is a privileged
            // cancellation request. We can upgrade the raw connection ID to a
            // proper `IdHandle`.
            self.handle_privileged_cancel(id_handle.clone())
        }
    }

    /// Unconditionally instructs the dataflow layer to cancel any ongoing,
    /// interactive work for the named `conn_id`.
    #[tracing::instrument(level = "debug", skip(self))]
    pub(crate) fn handle_privileged_cancel(&mut self, conn_id: ConnectionId) {
        if let Some(conn_meta) = self.active_conns.get(&conn_id) {
            // Cancel pending writes. There is at most one pending write per session.
            let mut maybe_ctx = None;
            if let Some(idx) = self.pending_writes.iter().position(|pending_write_txn| {
                matches!(pending_write_txn, PendingWriteTxn::User {
                    pending_txn: PendingTxn { ctx, .. },
                    ..
                } if *ctx.session().conn_id() == conn_id)
            }) {
                if let PendingWriteTxn::User {
                    pending_txn: PendingTxn { ctx, .. },
                    ..
                } = self.pending_writes.remove(idx)
                {
                    maybe_ctx = Some(ctx);
                }
            }

            // Cancel deferred writes. There is at most one deferred write per session.
            if let Some(idx) = self
                .write_lock_wait_group
                .iter()
                .position(|ready| matches!(ready, Deferred::Plan(ready) if *ready.ctx.session().conn_id() == conn_id))
            {
                let ready = self.write_lock_wait_group.remove(idx).expect("known to exist from call to `position` above");
                if let Deferred::Plan(ready) = ready {
                    maybe_ctx = Some(ready.ctx);
                }
            }

            // Cancel commands waiting on a real time recency timestamp. There is at most one  per session.
            if let Some(real_time_recency_context) =
                self.pending_real_time_recency_timestamp.remove(&conn_id)
            {
                let ctx = real_time_recency_context.take_context();
                maybe_ctx = Some(ctx);
            }

            if let Some(ctx) = maybe_ctx {
                ctx.retire(Ok(ExecuteResponse::Canceled));
            }

            // Inform the target session (if it asks) about the cancellation.
            let _ = conn_meta.cancel_tx.send(Canceled::Canceled);

            for PendingPeek {
                sender: rows_tx,
                conn_id: _,
                cluster_id: _,
                depends_on: _,
                // We take responsibility for retiring the
                // peek in `self.cancel_pending_peeks`,
                // so we don't need to do anything with `ctx_extra` here.
                ctx_extra: _,
                is_fast_path: _,
            } in self.cancel_pending_peeks(&conn_id)
            {
                // Cancel messages can be sent after the connection has hung
                // up, but before the connection's state has been cleaned up.
                // So we ignore errors when sending the response.
                let _ = rows_tx.send(PeekResponse::Canceled);
            }
        }
    }

    /// Handle termination of a client session.
    ///
    /// This cleans up any state in the coordinator associated with the session.
    #[tracing::instrument(level = "debug", skip(self))]
    async fn handle_terminate(&mut self, conn_id: ConnectionId) {
        if self.active_conns.get(&conn_id).is_none() {
            // If the session doesn't exist in `active_conns`, then this method will panic later on.
            // Instead we explicitly panic here while dumping the entire Coord to the logs to help
            // debug. This panic is very infrequent so we want as much information as possible.
            // See https://github.com/MaterializeInc/materialize/issues/18996.
            panic!("unknown connection: {conn_id:?}\n\n{self:?}")
        }

        // We do not need to call clear_transaction here because there are no side effects to run
        // based on any session transaction state.
        self.clear_connection(&conn_id);

        self.drop_temp_items(&conn_id).await;
        self.catalog_mut()
            .drop_temporary_schema(&conn_id)
            .unwrap_or_terminate("unable to drop temporary schema");
        let conn = self.active_conns.remove(&conn_id).expect("conn must exist");
        let session_type = metrics::session_type_label_value(conn.user());
        self.metrics
            .active_sessions
            .with_label_values(&[session_type])
            .dec();
        self.cancel_pending_peeks(conn.conn_id());
        self.end_session_for_statement_logging(conn.uuid());

        // Queue the builtin table update, but do not wait for it to complete. We explicitly do
        // this to prevent blocking the Coordinator in the case that a lot of connections are
        // closed at once, which occurs regularly in some workflows.
        let update = self.catalog().state().pack_session_update(&conn, -1);
        self.send_builtin_table_updates_defer(vec![update]);
    }

    #[tracing::instrument(level = "debug", skip(self, tx))]
    fn handle_append_webhook(
        &mut self,
        database: String,
        schema: String,
        name: String,
        conn_id: ConnectionId,
        tx: oneshot::Sender<Result<AppendWebhookResponse, AdapterError>>,
    ) {
        /// Attempts to resolve a Webhook source from a provided `database.schema.name` path.
        ///
        /// Returns a struct that can be used to append data to the underlying storate collection, and the
        /// types we should cast the request to.
        fn resolve(
            coord: &Coordinator,
            database: String,
            schema: String,
            name: String,
            conn_id: ConnectionId,
            now_fn: NowFn,
        ) -> Result<AppendWebhookResponse, PartialItemName> {
            // Resolve our collection.
            let name = PartialItemName {
                database: Some(database),
                schema: Some(schema),
                item: name,
            };
            let Ok(entry) = coord
                .catalog()
                .resolve_entry(None, &vec![], &name, &conn_id)
            else {
                return Err(name);
            };

            let (body_ty, header_tys, validator) = match entry.item() {
                CatalogItem::Source(Source {
                    data_source:
                        DataSourceDesc::Webhook {
                            validate_using,
                            headers,
                            ..
                        },
                    desc,
                    ..
                }) => {
                    // Assert we have one column for the body, and how ever many are required for
                    // the headers.
                    let num_columns = headers.num_columns() + 1;
                    mz_ore::soft_assert!(desc.arity() <= num_columns);

                    let body = desc
                        .get_by_name(&"body".into())
                        .map(|(_idx, ty)| ty.clone())
                        .ok_or(name.clone())?;

                    // Create a validator that can be called to validate a webhook request.
                    let validator = validate_using.as_ref().map(|v| {
                        let validation = v.clone();
                        AppendWebhookValidator::new(
                            validation,
                            coord.caching_secrets_reader.clone(),
                            now_fn,
                        )
                    });
                    (body, headers.clone(), validator)
                }
                _ => return Err(name),
            };

            // Get a channel so we can queue updates to be written.
            let row_tx = coord
                .controller
                .storage
                .monotonic_appender(entry.id())
                .map_err(|_| name)?;
            Ok(AppendWebhookResponse {
                tx: row_tx,
                body_ty,
                header_tys,
                validator,
            })
        }

        // Get our current clock for any sources that might use time based validation.
        let now_fn = self.catalog.config().now.clone();
        let response = resolve(self, database, schema, name, conn_id, now_fn).map_err(|name| {
            AdapterError::UnknownWebhookSource {
                database: name.database.expect("provided"),
                schema: name.schema.expect("provided"),
                name: name.item,
            }
        });
        let _ = tx.send(response);
    }
}

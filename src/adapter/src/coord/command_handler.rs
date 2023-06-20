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

use std::sync::Arc;
use std::time::Duration;

use mz_compute_client::protocol::response::PeekResponse;
use mz_ore::task;
use mz_ore::tracing::OpenTelemetryContext;
use mz_repr::ScalarType;
use mz_sql::ast::{
    CopyRelation, CopyStatement, InsertSource, Query, Raw, SetExpr, Statement, SubscribeStatement,
};
use mz_sql::catalog::{RoleAttributes, SessionCatalog};
use mz_sql::plan::{
    AbortTransactionPlan, CommitTransactionPlan, CopyRowsPlan, CreateRolePlan, Params, Plan,
    TransactionType,
};
use mz_sql::session::vars::{EndTransactionAction, OwnedVarInput};
use opentelemetry::trace::TraceContextExt;
use tokio::sync::{oneshot, watch};
use tracing::Instrument;
use tracing_opentelemetry::OpenTelemetrySpanExt;

use crate::client::{ConnectionId, ConnectionIdType};
use crate::command::{
    Canceled, Command, ExecuteResponse, GetVariablesResponse, Response, StartupMessage,
    StartupResponse,
};
use crate::coord::appends::{Deferred, PendingWriteTxn};
use crate::coord::peek::PendingPeek;
use crate::coord::{ConnMeta, Coordinator, CreateSourceStatementReady, Message, PendingTxn};
use crate::error::AdapterError;
use crate::notice::AdapterNotice;
use crate::session::{PreparedStatement, Session, TransactionStatus};
use crate::util::{ClientTransmitter, ResultExt};
use crate::{catalog, metrics, rbac, ExecuteContext};

use super::ExecuteContextExtra;

impl Coordinator {
    pub(crate) fn retire_execute(&mut self, _data: ExecuteContextExtra) {
        // Do nothing, for now.
        // In the future this is where we will log that statement execution finished.
    }
    fn send_error(&mut self, cmd: Command, e: AdapterError) {
        fn send<T>(tx: oneshot::Sender<Response<T>>, session: Session, e: AdapterError) {
            let _ = tx.send(Response::<T> {
                result: Err(e),
                session,
            });
        }
        match cmd {
            Command::Startup { tx, session, .. } => send(tx, session, e),
            Command::Declare { tx, session, .. } => send(tx, session, e),
            Command::Prepare { tx, session, .. } => send(tx, session, e),
            Command::VerifyPreparedStatement { tx, session, .. } => send(tx, session, e),
            Command::Execute { tx, session, .. } => send(tx, session, e),
            Command::Commit { tx, session, .. } => send(tx, session, e),
            Command::CancelRequest { .. } | Command::PrivilegedCancelRequest { .. } => {}
            Command::DumpCatalog { tx, session, .. } => send(tx, session, e),
            Command::CopyRows { tx, session, .. } => send(tx, session, e),
            Command::GetSystemVars { tx, session, .. } => send(tx, session, e),
            Command::SetSystemVars { tx, session, .. } => send(tx, session, e),
            Command::Terminate { tx, session, .. } => {
                if let Some(tx) = tx {
                    send(tx, session, e)
                }
            }
        }
    }

    pub(crate) async fn handle_command(&mut self, mut cmd: Command) {
        if let Some(session) = cmd.session_mut() {
            session.apply_external_metadata_updates();
        }
        if let Err(e) = rbac::check_command(self.catalog(), &cmd) {
            self.send_error(cmd, e.into());
            return;
        }
        match cmd {
            Command::Startup {
                session,
                cancel_tx,
                tx,
            } => {
                // Note: We purposefully do not use a ClientTransmitter here because startup
                // handles errors and cleanup of sessions itself.
                self.handle_startup(session, cancel_tx, tx).await;
            }

            Command::Execute {
                portal_name,
                session,
                tx,
                span,
            } => {
                let tx = ClientTransmitter::new(tx, self.internal_cmd_tx.clone());
                let ctx = ExecuteContext::from_parts(
                    tx,
                    self.internal_cmd_tx.clone(),
                    session,
                    ExecuteContextExtra,
                );

                let span = tracing::debug_span!(parent: &span, "message_command (execute)");
                self.handle_execute(portal_name, ctx).instrument(span).await;
            }

            Command::Declare {
                name,
                stmt,
                param_types,
                session,
                tx,
            } => {
                let tx = ClientTransmitter::new(tx, self.internal_cmd_tx.clone());
                let ctx = ExecuteContext::from_parts(
                    tx,
                    self.internal_cmd_tx.clone(),
                    session,
                    Default::default(),
                );
                self.declare(ctx, name, stmt, param_types);
            }

            Command::Prepare {
                name,
                stmt,
                param_types,
                session,
                tx,
            } => {
                let tx = ClientTransmitter::new(tx, self.internal_cmd_tx.clone());
                self.handle_prepare(tx, session, name, stmt, param_types);
            }

            Command::CancelRequest {
                conn_id,
                secret_key,
            } => {
                self.handle_cancel(conn_id, secret_key);
            }

            Command::PrivilegedCancelRequest { conn_id } => {
                self.handle_privileged_cancel(conn_id);
            }

            Command::DumpCatalog { session, tx } => {
                let tx = ClientTransmitter::new(tx, self.internal_cmd_tx.clone());
                tx.send(Ok(self.catalog().dump()), session);
            }

            Command::CopyRows {
                id,
                columns,
                rows,
                session,
                tx,
                ctx_extra,
            } => {
                let ctx = ExecuteContext::from_parts(
                    ClientTransmitter::new(tx, self.internal_cmd_tx.clone()),
                    self.internal_cmd_tx.clone(),
                    session,
                    ctx_extra,
                );
                self.sequence_plan(
                    ctx,
                    Plan::CopyRows(CopyRowsPlan { id, columns, rows }),
                    Vec::new(),
                )
                .await;
            }

            Command::GetSystemVars { session, tx } => {
                let vars =
                    GetVariablesResponse::new(self.catalog.system_config().iter().filter(|var| {
                        var.visible(session.user(), Some(self.catalog.system_config()))
                            .is_ok()
                    }));
                let tx = ClientTransmitter::new(tx, self.internal_cmd_tx.clone());
                tx.send(Ok(vars), session);
            }

            Command::SetSystemVars { vars, session, tx } => {
                let mut ops = Vec::with_capacity(vars.len());
                let tx = ClientTransmitter::new(tx, self.internal_cmd_tx.clone());

                for (name, value) in vars {
                    if let Err(e) = self.catalog().system_config().get(&name).and_then(|var| {
                        var.visible(session.user(), Some(self.catalog.system_config()))
                    }) {
                        return tx.send(Err(e.into()), session);
                    }

                    ops.push(catalog::Op::UpdateSystemConfiguration {
                        name,
                        value: OwnedVarInput::Flat(value),
                    });
                }

                let result = self.catalog_transact(Some(&session), ops).await;
                tx.send(result, session);
            }

            Command::Terminate { mut session, tx } => {
                self.handle_terminate(&mut session).await;
                // Note: We purposefully do not use a ClientTransmitter here because we're already
                // terminating the provided session.
                if let Some(tx) = tx {
                    let _ = tx.send(Response {
                        result: Ok(()),
                        session,
                    });
                }
            }

            Command::Commit {
                action,
                session,
                tx,
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
                self.sequence_plan(ctx, plan, Vec::new()).await;
            }

            Command::VerifyPreparedStatement {
                name,
                mut session,
                tx,
            } => {
                let tx = ClientTransmitter::new(tx, self.internal_cmd_tx.clone());
                let catalog = self.owned_catalog();
                mz_ore::task::spawn(|| "coord::VerifyPreparedStatement", async move {
                    let result = Self::verify_prepared_statement(&catalog, &mut session, &name);
                    tx.send(result, session);
                });
            }
        }
    }

    async fn handle_startup(
        &mut self,
        mut session: Session,
        cancel_tx: Arc<watch::Sender<Canceled>>,
        tx: oneshot::Sender<Response<StartupResponse>>,
    ) {
        if self
            .catalog()
            .try_get_role_by_name(&session.user().name)
            .is_none()
        {
            // If the user has made it to this point, that means they have been fully authenticated.
            // This includes preventing any user, except a pre-defined set of system users, from
            // connecting to an internal port. Therefore it's ok to always create a new role for
            // the user.
            let attributes = RoleAttributes::new();
            let plan = CreateRolePlan {
                name: session.user().name.to_string(),
                attributes,
            };
            if let Err(err) = self.sequence_create_role_for_startup(&session, plan).await {
                let _ = tx.send(Response {
                    result: Err(err),
                    session,
                });
                return;
            }
        }

        let role_id = self
            .catalog()
            .try_get_role_by_name(&session.user().name)
            .expect("created above")
            .id;
        session.initialize_role_metadata(role_id);

        if let Err(e) = self
            .catalog_mut()
            .create_temporary_schema(session.conn_id(), role_id)
        {
            let _ = tx.send(Response {
                result: Err(e.into()),
                session,
            });
            return;
        }

        let mut messages = vec![];
        let catalog = self.catalog();
        let catalog = catalog.for_session(&session);
        if catalog.active_database().is_none() {
            messages.push(StartupMessage::UnknownSessionDatabase(
                session.vars().database().into(),
            ));
        }

        let session_type = metrics::session_type_label_value(session.user());
        self.metrics
            .active_sessions
            .with_label_values(&[session_type])
            .inc();
        self.active_conns.insert(
            session.conn_id().clone(),
            ConnMeta {
                cancel_tx,
                secret_key: session.secret_key(),
                notice_tx: session.retain_notice_transmitter(),
                drop_sinks: Vec::new(),
            },
        );
        let update = self.catalog().state().pack_session_update(&session, 1);
        self.send_builtin_table_updates(vec![update]).await;

        ClientTransmitter::new(tx, self.internal_cmd_tx.clone())
            .send(Ok(StartupResponse { messages }), session)
    }

    /// Handles an execute command.
    #[tracing::instrument(level = "debug", skip_all)]
    pub(crate) async fn handle_execute(&mut self, portal_name: String, mut ctx: ExecuteContext) {
        if ctx.session().vars().emit_trace_id_notice() {
            let span_context = tracing::Span::current()
                .context()
                .span()
                .span_context()
                .clone();
            if span_context.is_valid() {
                ctx.session().add_notice(AdapterNotice::QueryTrace {
                    trace_id: span_context.trace_id(),
                });
            }
        }

        if let Err(err) = self.verify_portal(ctx.session_mut(), &portal_name) {
            return ctx.retire(Err(err));
        }

        let portal = ctx
            .session()
            .get_portal_unverified(&portal_name)
            .expect("known to exist");

        let stmt = match &portal.stmt {
            Some(stmt) => stmt.clone(),
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

        let params = portal.parameters.clone();
        self.handle_execute_inner(stmt, params, ctx).await
    }

    #[tracing::instrument(level = "trace", skip(self, ctx))]
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
                    | Statement::Explain(_)
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

                    // Statements below must by run singly (in Started).
                    Statement::AlterCluster(_)
                    | Statement::AlterConnection(_)
                    | Statement::AlterIndex(_)
                    | Statement::AlterSecret(_)
                    | Statement::AlterSink(_)
                    | Statement::AlterSource(_)
                    | Statement::AlterObjectRename(_)
                    | Statement::AlterRole(_)
                    | Statement::AlterSystemSet(_)
                    | Statement::AlterSystemReset(_)
                    | Statement::AlterSystemResetAll(_)
                    | Statement::AlterOwner(_)
                    | Statement::CreateConnection(_)
                    | Statement::CreateDatabase(_)
                    | Statement::CreateIndex(_)
                    | Statement::CreateRole(_)
                    | Statement::CreateCluster(_)
                    | Statement::CreateClusterReplica(_)
                    | Statement::CreateSchema(_)
                    | Statement::CreateSecret(_)
                    | Statement::CreateSink(_)
                    | Statement::CreateSource(_)
                    | Statement::CreateSubsource(_)
                    | Statement::CreateTable(_)
                    | Statement::CreateType(_)
                    | Statement::CreateView(_)
                    | Statement::CreateMaterializedView(_)
                    | Statement::Delete(_)
                    | Statement::DropObjects(_)
                    | Statement::DropOwned(_)
                    | Statement::GrantPrivileges(_)
                    | Statement::GrantRole(_)
                    | Statement::Insert(_)
                    | Statement::RevokePrivileges(_)
                    | Statement::AlterDefaultPrivileges(_)
                    | Statement::RevokeRole(_)
                    | Statement::Update(_)
                    | Statement::ReassignOwned(_) => {
                        return ctx.retire(Err(AdapterError::OperationProhibitsTransaction(
                            stmt.to_string(),
                        )))
                    }
                }
            }
        }

        let catalog = self.catalog();
        let catalog = catalog.for_session(ctx.session());
        let original_stmt = stmt.clone();
        let (stmt, depends_on) = match mz_sql::names::resolve(&catalog, stmt) {
            Ok(resolved) => resolved,
            Err(e) => return ctx.retire(Err(e.into())),
        };
        let depends_on = depends_on.into_iter().collect();
        // N.B. The catalog can change during purification so we must validate that the dependencies still exist after
        // purification.  This should be done back on the main thread.
        // We do the validation:
        //   - In the handler for `Message::CreateSourceStatementReady`, before we handle the purified statement.
        // If we add special handling for more types of `Statement`s, we'll need to ensure similar verification
        // occurs.
        match stmt {
            // `CREATE SOURCE` statements must be purified off the main
            // coordinator thread of control.
            Statement::CreateSource(stmt) => {
                let internal_cmd_tx = self.internal_cmd_tx.clone();
                let conn_id = ctx.session().conn_id().clone();
                let purify_fut = mz_sql::pure::purify_create_source(
                    Box::new(catalog.into_owned()),
                    self.now(),
                    stmt,
                    self.connection_context.clone(),
                );
                let otel_ctx = OpenTelemetryContext::obtain();
                task::spawn(|| format!("purify:{conn_id}"), async move {
                    let result = purify_fut.await.map_err(|e| e.into());
                    // It is not an error for purification to complete after `internal_cmd_rx` is dropped.
                    let result = internal_cmd_tx.send(Message::CreateSourceStatementReady(
                        CreateSourceStatementReady {
                            ctx,
                            result,
                            params,
                            depends_on,
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
            _ => match self.plan_statement(ctx.session_mut(), stmt, &params) {
                Ok(plan) => self.sequence_plan(ctx, plan, depends_on).await,
                Err(e) => ctx.retire(Err(e)),
            },
        }
    }

    fn handle_prepare(
        &self,
        tx: ClientTransmitter<()>,
        mut session: Session,
        name: String,
        stmt: Option<Statement<Raw>>,
        param_types: Vec<Option<ScalarType>>,
    ) {
        let catalog = self.owned_catalog();
        mz_ore::task::spawn(|| "coord::handle_prepare", async move {
            // Note: This failpoint is used to simulate a request outliving the external connection
            // that made it.
            let mut async_pause = false;
            (|| {
                fail::fail_point!("async_prepare", |val| {
                    async_pause = val.map_or(false, |val| val.parse().unwrap_or(false))
                });
            })();
            if async_pause {
                tokio::time::sleep(Duration::from_secs(1)).await;
            };

            let res = match Self::describe(&catalog, &session, stmt.clone(), param_types) {
                Ok(desc) => {
                    session.set_prepared_statement(
                        name,
                        PreparedStatement::new(stmt, desc, catalog.transient_revision()),
                    );
                    Ok(())
                }
                Err(err) => Err(err),
            };
            tx.send(res, session);
        });
    }

    /// Instruct the dataflow layer to cancel any ongoing, interactive work for
    /// the named `conn_id` if the correct secret key is specified.
    ///
    /// Note: Here we take a [`ConnectionIdType`] as opposed to an owned
    /// `ConnectionId` because this method gets called by external clients when
    /// they request to cancel a request.
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
    fn handle_privileged_cancel(&mut self, conn_id: ConnectionId) {
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
    async fn handle_terminate(&mut self, session: &mut Session) {
        if self.active_conns.get(session.conn_id()).is_none() {
            // If the session doesn't exist in `active_conns`, then this method will panic later on.
            // Instead we explicitly panic here while dumping the entire Coord to the logs to help
            // debug. This panic is very infrequent so we want as much information as possible.
            // See https://github.com/MaterializeInc/materialize/issues/18996.
            panic!("unknown session: {session:?}\n\n{self:?}")
        }

        self.clear_transaction(session);

        self.drop_temp_items(session).await;
        self.catalog_mut()
            .drop_temporary_schema(session.conn_id())
            .unwrap_or_terminate("unable to drop temporary schema");
        let session_type = metrics::session_type_label_value(session.user());
        self.metrics
            .active_sessions
            .with_label_values(&[session_type])
            .dec();
        self.active_conns.remove(session.conn_id());
        self.cancel_pending_peeks(session.conn_id());
        let update = self.catalog().state().pack_session_update(session, -1);
        self.send_builtin_table_updates(vec![update]).await;
    }
}

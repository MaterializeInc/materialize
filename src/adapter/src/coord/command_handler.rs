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

use std::collections::BTreeMap;
use std::sync::Arc;

use opentelemetry::trace::TraceContextExt;
use tokio::sync::{oneshot, watch};
use tracing::Instrument;
use tracing_opentelemetry::OpenTelemetrySpanExt;

use mz_compute_client::protocol::response::PeekResponse;
use mz_ore::task;
use mz_ore::tracing::OpenTelemetryContext;
use mz_repr::ScalarType;
use mz_sql::ast::{InsertSource, Query, Raw, SetExpr, Statement};
use mz_sql::catalog::{RoleAttributes, SessionCatalog};
use mz_sql::plan::{
    AbortTransactionPlan, CommitTransactionPlan, CopyRowsPlan, CreateRolePlan, Params, Plan,
    TransactionType,
};
use mz_sql::session::vars::{EndTransactionAction, OwnedVarInput};

use crate::client::ConnectionId;
use crate::command::{
    Canceled, Command, ExecuteResponse, Response, StartupMessage, StartupResponse,
};
use crate::coord::appends::{Deferred, PendingWriteTxn};
use crate::coord::peek::PendingPeek;
use crate::coord::{ConnMeta, Coordinator, CreateSourceStatementReady, Message, PendingTxn};
use crate::error::AdapterError;
use crate::notice::AdapterNotice;
use crate::session::{PreparedStatement, Session, TransactionStatus};
use crate::util::{ClientTransmitter, ResultExt};
use crate::{catalog, metrics, rbac};

impl Coordinator {
    pub(crate) async fn handle_command(&mut self, mut cmd: Command) {
        if let Some(session) = cmd.session_mut() {
            session.apply_external_metadata_updates();
        }
        if let Err(e) = rbac::check_command(self.catalog(), &cmd) {
            cmd.send_error(e.into());
            return;
        }
        match cmd {
            Command::Startup {
                session,
                cancel_tx,
                tx,
            } => {
                self.handle_startup(session, cancel_tx, tx).await;
            }

            Command::Execute {
                portal_name,
                session,
                tx,
                span,
            } => {
                let tx = ClientTransmitter::new(tx, self.internal_cmd_tx.clone());

                let span = tracing::debug_span!(parent: &span, "message_command (execute)");
                self.handle_execute(portal_name, session, tx)
                    .instrument(span)
                    .await;
            }

            Command::Declare {
                name,
                stmt,
                param_types,
                session,
                tx,
            } => {
                let tx = ClientTransmitter::new(tx, self.internal_cmd_tx.clone());
                self.declare(tx, session, name, stmt, param_types);
            }

            Command::Describe {
                name,
                stmt,
                param_types,
                session,
                tx,
            } => {
                let tx = ClientTransmitter::new(tx, self.internal_cmd_tx.clone());
                self.handle_describe(tx, session, name, stmt, param_types);
            }

            Command::CancelRequest {
                conn_id,
                secret_key,
            } => {
                self.handle_cancel(conn_id, secret_key);
            }

            Command::DumpCatalog { session, tx } => {
                // TODO(benesch/jkosh44): when we have RBAC, dumping the catalog should
                // require superuser permissions.

                let _ = tx.send(Response {
                    result: Ok(self.catalog().dump()),
                    session,
                });
            }

            Command::CopyRows {
                id,
                columns,
                rows,
                session,
                tx,
            } => {
                self.sequence_plan(
                    ClientTransmitter::new(tx, self.internal_cmd_tx.clone()),
                    session,
                    Plan::CopyRows(CopyRowsPlan { id, columns, rows }),
                    Vec::new(),
                )
                .await;
            }

            Command::GetSystemVars { session, tx } => {
                let mut vars = BTreeMap::new();
                for var in self.catalog().system_config().iter() {
                    vars.insert(var.name().to_string(), var.value());
                }
                let _ = tx.send(Response {
                    result: Ok(vars),
                    session,
                });
            }

            Command::SetSystemVars { vars, session, tx } => {
                let ops = vars
                    .into_iter()
                    .map(|(name, value)| catalog::Op::UpdateSystemConfiguration {
                        name,
                        value: OwnedVarInput::Flat(value),
                    })
                    .collect();
                let result = self.catalog_transact(Some(&session), ops).await;
                let _ = tx.send(Response { result, session });
            }

            Command::Terminate { mut session, tx } => {
                self.handle_terminate(&mut session).await;
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
                self.sequence_plan(tx, session, plan, Vec::new()).await;
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
        session.set_role_id(role_id);

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
            session.conn_id(),
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
    async fn handle_execute(
        &mut self,
        portal_name: String,
        mut session: Session,
        tx: ClientTransmitter<ExecuteResponse>,
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
            return tx.send(Err(err), session);
        }

        let portal = session
            .get_portal_unverified(&portal_name)
            .expect("known to exist");

        let stmt = match &portal.stmt {
            Some(stmt) => stmt.clone(),
            None => return tx.send(Ok(ExecuteResponse::EmptyQuery), session),
        };

        let session_type = metrics::session_type_label_value(session.user());
        let stmt_type = metrics::statement_type_label_value(&stmt);
        self.metrics
            .query_total
            .with_label_values(&[session_type, stmt_type])
            .inc();

        let params = portal.parameters.clone();
        self.handle_execute_inner(stmt, params, session, tx).await
    }

    #[tracing::instrument(level = "trace", skip(self, tx, session))]
    pub(crate) async fn handle_execute_inner(
        &mut self,
        stmt: Statement<Raw>,
        params: Params,
        mut session: Session,
        tx: ClientTransmitter<ExecuteResponse>,
    ) {
        // Verify that this statement type can be executed in the current
        // transaction state.
        match session.transaction() {
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
                    return tx.send(
                        Err(AdapterError::OperationRequiresTransaction(
                            "DECLARE CURSOR".into(),
                        )),
                        session,
                    );
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
                    Statement::AlterConnection(_)
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
                    | Statement::GrantPrivilege(_)
                    | Statement::GrantRole(_)
                    | Statement::Insert(_)
                    | Statement::RevokePrivilege(_)
                    | Statement::RevokeRole(_)
                    | Statement::Update(_) => {
                        return tx.send(
                            Err(AdapterError::OperationProhibitsTransaction(
                                stmt.to_string(),
                            )),
                            session,
                        )
                    }
                }
            }
        }

        let catalog = self.catalog();
        let catalog = catalog.for_session(&session);
        let original_stmt = stmt.clone();
        let (stmt, depends_on) = match mz_sql::names::resolve(&catalog, stmt) {
            Ok(resolved) => resolved,
            Err(e) => return tx.send(Err(e.into()), session),
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
                let conn_id = session.conn_id();
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
                            session,
                            tx,
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
            Statement::CreateSubsource(_) => tx.send(
                Err(AdapterError::Unsupported("CREATE SUBSOURCE statements")),
                session,
            ),

            // All other statements are handled immediately.
            _ => match self.plan_statement(&mut session, stmt, &params) {
                Ok(plan) => self.sequence_plan(tx, session, plan, depends_on).await,
                Err(e) => tx.send(Err(e), session),
            },
        }
    }

    fn handle_describe(
        &self,
        tx: ClientTransmitter<()>,
        mut session: Session,
        name: String,
        stmt: Option<Statement<Raw>>,
        param_types: Vec<Option<ScalarType>>,
    ) {
        let catalog = self.owned_catalog();
        mz_ore::task::spawn(|| "coord::handle_describe", async move {
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
    /// the named `conn_id`.
    fn handle_cancel(&mut self, conn_id: ConnectionId, secret_key: u32) {
        if let Some(conn_meta) = self.active_conns.get(&conn_id) {
            // If the secret key specified by the client doesn't match the
            // actual secret key for the target connection, we treat this as a
            // rogue cancellation request and ignore it.
            if conn_meta.secret_key != secret_key {
                return;
            }

            // Cancel pending writes. There is at most one pending write per session.
            if let Some(idx) = self.pending_writes.iter().position(|pending_write_txn| {
                matches!(pending_write_txn, PendingWriteTxn::User {
                    pending_txn: PendingTxn { session, .. },
                    ..
                } if session.conn_id() == conn_id)
            }) {
                if let PendingWriteTxn::User {
                    pending_txn:
                        PendingTxn {
                            client_transmitter,
                            session,
                            ..
                        },
                    ..
                } = self.pending_writes.remove(idx)
                {
                    let _ = client_transmitter.send(Ok(ExecuteResponse::Canceled), session);
                }
            }

            // Cancel deferred writes. There is at most one deferred write per session.
            if let Some(idx) = self
                .write_lock_wait_group
                .iter()
                .position(|ready| matches!(ready, Deferred::Plan(ready) if ready.session.conn_id() == conn_id))
            {
                let ready = self.write_lock_wait_group.remove(idx).expect("known to exist from call to `position` above");
                if let Deferred::Plan(ready) = ready {
                    ready.tx.send(Ok(ExecuteResponse::Canceled), ready.session);
                }
            }

            // Cancel commands waiting on a real time recency timestamp. There is at most one  per session.
            if let Some(real_time_recency_context) =
                self.pending_real_time_recency_timestamp.remove(&conn_id)
            {
                let (tx, session) = real_time_recency_context.take_tx_and_session();
                tx.send(Ok(ExecuteResponse::Canceled), session);
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
        if self.active_conns.contains_key(&session.conn_id()) {
            self.clear_transaction(session);

            self.drop_temp_items(session).await;
            self.catalog_mut()
                .drop_temporary_schema(&session.conn_id())
                .unwrap_or_terminate("unable to drop temporary schema");
            let session_type = metrics::session_type_label_value(session.user());
            self.metrics
                .active_sessions
                .with_label_values(&[session_type])
                .dec();
            self.active_conns.remove(&session.conn_id());
            self.cancel_pending_peeks(&session.conn_id());
            let update = self.catalog().state().pack_session_update(session, -1);
            self.send_builtin_table_updates(vec![update]).await;
        }
    }
}

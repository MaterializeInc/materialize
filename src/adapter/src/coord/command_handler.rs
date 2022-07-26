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

use rand::Rng;
use tokio::sync::{oneshot, watch};
use tracing::Instrument;

use mz_compute_client::response::PeekResponse;
use mz_ore::task;
use mz_repr::ScalarType;
use mz_sql::ast::{InsertSource, Query, Raw, SetExpr, Statement};
use mz_sql::catalog::SessionCatalog as _;
use mz_sql::plan::{CreateRolePlan, Params};
use mz_stash::Append;

use crate::client::ConnectionId;
use crate::command::{
    Canceled, Command, ExecuteResponse, Response, StartupMessage, StartupResponse,
};
use crate::coord::appends::{Deferred, PendingWriteTxn};
use crate::coord::peek::PendingPeek;
use crate::coord::{ConnMeta, Coordinator, CreateSourceStatementReady, Message};
use crate::error::AdapterError;
use crate::session::{PreparedStatement, Session, TransactionStatus};
use crate::util::ClientTransmitter;

impl<S: Append + 'static> Coordinator<S> {
    pub(crate) async fn handle_command(&mut self, cmd: Command) {
        match cmd {
            Command::Startup {
                session,
                create_user_if_not_exists,
                cancel_tx,
                tx,
            } => {
                self.handle_startup(session, create_user_if_not_exists, cancel_tx, tx)
                    .await;
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
                mut session,
                tx,
            } => {
                let result = self.declare(&mut session, name, stmt, param_types);
                let _ = tx.send(Response { result, session });
            }

            Command::Describe {
                name,
                stmt,
                param_types,
                mut session,
                tx,
            } => {
                let result = self.handle_describe(&mut session, name, stmt, param_types);
                let _ = tx.send(Response { result, session });
            }

            Command::CancelRequest {
                conn_id,
                secret_key,
            } => {
                self.handle_cancel(conn_id, secret_key).await;
            }

            Command::DumpCatalog { session, tx } => {
                // TODO(benesch): when we have RBAC, dumping the catalog should
                // require superuser permissions.

                let _ = tx.send(Response {
                    result: Ok(self.catalog.dump()),
                    session,
                });
            }

            Command::CopyRows {
                id,
                columns,
                rows,
                mut session,
                tx,
            } => {
                let result = self.sequence_copy_rows(&mut session, id, columns, rows);
                let _ = tx.send(Response { result, session });
            }

            Command::Terminate { mut session } => {
                self.handle_terminate(&mut session).await;
            }

            Command::StartTransaction {
                implicit,
                session,
                tx,
            } => {
                let now = self.now_datetime();
                let session = match implicit {
                    None => session.start_transaction(now, None, None),
                    Some(stmts) => session.start_transaction_implicit(now, stmts),
                };
                let _ = tx.send(Response {
                    result: Ok(()),
                    session,
                });
            }

            Command::Commit {
                action,
                session,
                tx,
            } => {
                let tx = ClientTransmitter::new(tx, self.internal_cmd_tx.clone());
                self.sequence_end_transaction(tx, session, action).await;
            }

            Command::VerifyPreparedStatement {
                name,
                mut session,
                tx,
            } => {
                let result = self.verify_prepared_statement(&mut session, &name);
                let _ = tx.send(Response { result, session });
            }
        }
    }

    async fn handle_startup(
        &mut self,
        session: Session,
        create_user_if_not_exists: bool,
        cancel_tx: Arc<watch::Sender<Canceled>>,
        tx: oneshot::Sender<Response<StartupResponse>>,
    ) {
        if let Err(e) = self
            .catalog
            .create_temporary_schema(session.conn_id())
            .await
        {
            let _ = tx.send(Response {
                result: Err(e.into()),
                session,
            });
            return;
        }

        if self
            .catalog
            .for_session(&session)
            .resolve_role(session.user())
            .is_err()
        {
            if !create_user_if_not_exists {
                let _ = tx.send(Response {
                    result: Err(AdapterError::UnknownLoginRole(session.user().into())),
                    session,
                });
                return;
            }
            let plan = CreateRolePlan {
                name: session.user().to_string(),
            };
            if let Err(err) = self.sequence_create_role(&session, plan).await {
                let _ = tx.send(Response {
                    result: Err(err),
                    session,
                });
                return;
            }
        }

        let mut messages = vec![];
        let catalog = self.catalog.for_session(&session);
        if catalog.active_database().is_none() {
            messages.push(StartupMessage::UnknownSessionDatabase(
                session.vars().database().into(),
            ));
        }

        let secret_key = rand::thread_rng().gen();

        self.active_conns.insert(
            session.conn_id(),
            ConnMeta {
                cancel_tx,
                secret_key,
            },
        );

        ClientTransmitter::new(tx, self.internal_cmd_tx.clone()).send(
            Ok(StartupResponse {
                messages,
                secret_key,
            }),
            session,
        )
    }

    /// Handles an execute command.
    #[tracing::instrument(level = "debug", skip_all)]
    async fn handle_execute(
        &mut self,
        portal_name: String,
        mut session: Session,
        tx: ClientTransmitter<ExecuteResponse>,
    ) {
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

            // Started is almost always safe (started means there's a single statement
            // being executed). Failed transactions have already been checked in pgwire for
            // a safe statement (COMMIT, ROLLBACK, etc.) and can also proceed.
            TransactionStatus::Started(_) | TransactionStatus::Failed(_) => {
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
                    | Statement::ShowColumns(_)
                    | Statement::ShowCreateIndex(_)
                    | Statement::ShowCreateSink(_)
                    | Statement::ShowCreateSource(_)
                    | Statement::ShowCreateTable(_)
                    | Statement::ShowCreateView(_)
                    | Statement::ShowCreateMaterializedView(_)
                    | Statement::ShowCreateConnection(_)
                    | Statement::ShowDatabases(_)
                    | Statement::ShowSchemas(_)
                    | Statement::ShowIndexes(_)
                    | Statement::ShowObjects(_)
                    | Statement::ShowVariable(_)
                    | Statement::SetVariable(_)
                    | Statement::ResetVariable(_)
                    | Statement::StartTransaction(_)
                    | Statement::Tail(_)
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
                    Statement::AlterIndex(_)
                    | Statement::AlterSecret(_)
                    | Statement::AlterObjectRename(_)
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
                    | Statement::CreateTable(_)
                    | Statement::CreateType(_)
                    | Statement::CreateView(_)
                    | Statement::CreateViews(_)
                    | Statement::CreateMaterializedView(_)
                    | Statement::Delete(_)
                    | Statement::DropDatabase(_)
                    | Statement::DropSchema(_)
                    | Statement::DropObjects(_)
                    | Statement::DropRoles(_)
                    | Statement::DropClusters(_)
                    | Statement::DropClusterReplicas(_)
                    | Statement::Insert(_)
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

        let catalog = self.catalog.for_session(&session);
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
                task::spawn(|| format!("purify:{conn_id}"), async move {
                    let result = purify_fut.await.map_err(|e| e.into());
                    internal_cmd_tx
                        .send(Message::CreateSourceStatementReady(
                            CreateSourceStatementReady {
                                session,
                                tx,
                                result,
                                params,
                                depends_on,
                                original_stmt,
                            },
                        ))
                        .expect("sending to internal_cmd_tx cannot fail");
                });
            }

            // All other statements are handled immediately.
            _ => match self.plan_statement(&mut session, stmt, &params).await {
                Ok(plan) => self.sequence_plan(tx, session, plan, depends_on).await,
                Err(e) => tx.send(Err(e), session),
            },
        }
    }

    fn handle_describe(
        &self,
        session: &mut Session,
        name: String,
        stmt: Option<Statement<Raw>>,
        param_types: Vec<Option<ScalarType>>,
    ) -> Result<(), AdapterError> {
        let desc = self.describe(session, stmt.clone(), param_types)?;
        session.set_prepared_statement(
            name,
            PreparedStatement::new(stmt, desc, self.catalog.transient_revision()),
        );
        Ok(())
    }

    /// Instruct the dataflow layer to cancel any ongoing, interactive work for
    /// the named `conn_id`.
    async fn handle_cancel(&mut self, conn_id: ConnectionId, secret_key: u32) {
        if let Some(conn_meta) = self.active_conns.get(&conn_id) {
            // If the secret key specified by the client doesn't match the
            // actual secret key for the target connection, we treat this as a
            // rogue cancellation request and ignore it.
            if conn_meta.secret_key != secret_key {
                return;
            }

            // Cancel pending writes. There is at most one pending write per session.
            if let Some(idx) = self
                .pending_writes
                .iter()
                .position(|PendingWriteTxn { session, .. }| session.conn_id() == conn_id)
            {
                let PendingWriteTxn {
                    client_transmitter,
                    session,
                    ..
                } = self.pending_writes.remove(idx);
                let _ = client_transmitter.send(Ok(ExecuteResponse::Canceled), session);
            }

            // Cancel deferred writes. There is at most one deferred write per session.
            if let Some(idx) = self
                .write_lock_wait_group
                .iter()
                .position(|ready| matches!(ready, Deferred::Plan(ready) if ready.session.conn_id() == conn_id))
            {
                let ready = self.write_lock_wait_group.remove(idx).unwrap();
                if let Deferred::Plan(ready) = ready {
                    ready.tx.send(Ok(ExecuteResponse::Canceled), ready.session);
                }
            }

            // Inform the target session (if it asks) about the cancellation.
            let _ = conn_meta.cancel_tx.send(Canceled::Canceled);

            for PendingPeek {
                sender: rows_tx,
                conn_id: _,
            } in self.cancel_pending_peeks(conn_id).await
            {
                rows_tx
                    .send(PeekResponse::Canceled)
                    .expect("Peek endpoint terminated prematurely");
            }
        }
    }

    /// Handle termination of a client session.
    ///
    /// This cleans up any state in the coordinator associated with the session.
    async fn handle_terminate(&mut self, session: &mut Session) {
        self.clear_transaction(session).await;

        self.drop_temp_items(&session).await;
        self.catalog
            .drop_temporary_schema(session.conn_id())
            .expect("unable to drop temporary schema");
        self.active_conns.remove(&session.conn_id());
        self.cancel_pending_peeks(session.conn_id()).await;
    }
}

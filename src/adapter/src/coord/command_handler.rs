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

use differential_dataflow::lattice::Lattice;
use mz_sql::session::metadata::SessionMetadata;
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use futures::future::LocalBoxFuture;
use futures::FutureExt;
use mz_adapter_types::connection::{ConnectionId, ConnectionIdType};
use mz_catalog::memory::objects::{CatalogItem, DataSourceDesc, Source};
use mz_catalog::SYSTEM_CONN_ID;
use mz_ore::instrument;
use mz_ore::task;
use mz_ore::tracing::OpenTelemetryContext;
use mz_repr::role_id::RoleId;
use mz_repr::{ScalarType, Timestamp};
use mz_sql::ast::{
    ConstantVisitor, CopyRelation, CopyStatement, Raw, Statement, SubscribeStatement,
};
use mz_sql::catalog::RoleAttributes;
use mz_sql::names::{Aug, PartialItemName, ResolvedIds};
use mz_sql::plan::{
    AbortTransactionPlan, CommitTransactionPlan, CreateRolePlan, Params, Plan, TransactionType,
};
use mz_sql::pure::{
    materialized_view_option_contains_temporal, purify_create_materialized_view_options,
};
use mz_sql::rbac;
use mz_sql::rbac::CREATE_ITEM_USAGE;
use mz_sql::session::user::User;
use mz_sql::session::vars::{
    EndTransactionAction, OwnedVarInput, Value, Var, STATEMENT_LOGGING_SAMPLE_RATE,
};
use mz_sql_parser::ast::display::AstDisplay;
use mz_sql_parser::ast::{
    CreateMaterializedViewStatement, ExplainPlanStatement, Explainee, InsertStatement,
};
use mz_storage_types::sources::Timeline;
use opentelemetry::trace::TraceContextExt;
use tokio::sync::{mpsc, oneshot};
use tracing::{debug_span, warn, Instrument};
use tracing_opentelemetry::OpenTelemetrySpanExt;

use crate::command::{
    CatalogSnapshot, Command, ExecuteResponse, GetVariablesResponse, StartupResponse,
};
use crate::coord::appends::{Deferred, PendingWriteTxn};
use crate::coord::{ConnMeta, Coordinator, Message, PendingTxn, PurifiedStatementReady};
use crate::error::AdapterError;
use crate::notice::AdapterNotice;
use crate::session::{Session, TransactionOps, TransactionStatus};
use crate::util::{ClientTransmitter, ResultExt};
use crate::webhook::{
    AppendWebhookResponse, AppendWebhookValidator, WebhookAppender, WebhookAppenderInvalidator,
};
use crate::{catalog, metrics, AppendWebhookError, ExecuteContext, TimestampProvider};

use super::ExecuteContextExtra;

impl Coordinator {
    /// BOXED FUTURE: As of Nov 2023 the returned Future from this function was 58KB. This would
    /// get stored on the stack which is bad for runtime performance, and blow up our stack usage.
    /// Because of that we purposefully move this Future onto the heap (i.e. Box it).
    pub(crate) fn handle_command<'a>(&'a mut self, mut cmd: Command) -> LocalBoxFuture<'a, ()> {
        async move {
            if let Some(session) = cmd.session_mut() {
                session.apply_external_metadata_updates();
            }
            match cmd {
                Command::Startup {
                    tx,
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
                        tx,
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
                    outer_ctx_extra,
                } => {
                    let tx = ClientTransmitter::new(tx, self.internal_cmd_tx.clone());

                    self.handle_execute(portal_name, session, tx, outer_ctx_extra)
                        .await;
                }

                Command::RetireExecute { data, reason } => self.retire_execution(reason, data),

                Command::CancelRequest {
                    conn_id,
                    secret_key,
                } => {
                    self.handle_cancel(conn_id, secret_key).await;
                }

                Command::PrivilegedCancelRequest { conn_id } => {
                    self.handle_privileged_cancel(conn_id).await;
                }

                Command::GetWebhook {
                    database,
                    schema,
                    name,
                    tx,
                } => {
                    self.handle_get_webhook(database, schema, name, tx);
                }

                Command::GetSystemVars { conn_id, tx } => {
                    let conn = &self.active_conns[&conn_id];
                    let vars = GetVariablesResponse::new(
                        self.catalog.system_config().iter().filter(|var| {
                            var.visible(conn.user(), Some(self.catalog.system_config()))
                                .is_ok()
                        }),
                    );
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

                    self.sequence_plan(ctx, plan, ResolvedIds(BTreeSet::new()))
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
        .instrument(debug_span!("handle_command"))
        .boxed_local()
    }

    #[mz_ore::instrument(level = "debug")]
    async fn handle_startup(
        &mut self,
        tx: oneshot::Sender<Result<StartupResponse, AdapterError>>,
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
                let mut session_defaults = BTreeMap::new();
                let system_config = self.catalog().state().system_config();

                // Override the session with any system defaults.
                session_defaults.extend(
                    system_config
                        .iter_session()
                        .map(|v| (v.name().to_string(), OwnedVarInput::Flat(v.value()))),
                );

                // Special case.
                let statement_logging_default = system_config
                    .statement_logging_default_sample_rate()
                    .format();
                session_defaults.insert(
                    STATEMENT_LOGGING_SAMPLE_RATE.name().to_string(),
                    OwnedVarInput::Flat(statement_logging_default),
                );

                // Override system defaults with role defaults.
                session_defaults.extend(
                    self.catalog()
                        .get_role(&role_id)
                        .vars()
                        .map(|(name, val)| (name.to_string(), val.clone())),
                );

                let session_type = metrics::session_type_label_value(&user);
                self.metrics
                    .active_sessions
                    .with_label_values(&[session_type])
                    .inc();
                let conn = ConnMeta {
                    secret_key,
                    notice_tx,
                    drop_sinks: BTreeSet::new(),
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
                let notify = self.builtin_table_update().defer(vec![update]);

                let resp = Ok(StartupResponse {
                    role_id,
                    write_notify: Box::pin(notify),
                    session_defaults,
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
    #[instrument(name = "coord::handle_execute", fields(session = session.uuid().to_string()))]
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
        match &*stmt {
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

    #[instrument(name = "coord::handle_execute_inner", fields(stmt = stmt.to_ast_string_redacted()))]
    pub(crate) async fn handle_execute_inner(
        &mut self,
        stmt: Arc<Statement<Raw>>,
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
                if let Statement::Declare(_) = &*stmt {
                    // Declare is an exception. Although it's not against any spec to execute
                    // it, it will always result in nothing happening, since all portals will be
                    // immediately closed. Users don't know this detail, so this error helps them
                    // understand what's going wrong. Postgres does this too.
                    return ctx.retire(Err(AdapterError::OperationRequiresTransaction(
                        "DECLARE CURSOR".into(),
                    )));
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
                match &*stmt {
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
                    | Statement::ExplainPushdown(_)
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

                    Statement::Insert(InsertStatement {
                        source, returning, ..
                    }) if returning.is_empty() && ConstantVisitor::insert_source(source) => {
                        // Inserting from constant values statements that do not need to execute on
                        // any cluster (no RETURNING) is always safe.
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
                            if let Ok(resp) = ExecuteResponse::try_from(&*stmt) {
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
        let original_stmt = Arc::clone(&stmt);
        // `resolved_ids` should be derivable from `stmt`. If `stmt` is transformed to remove/add
        // IDs, then `resolved_ids` should be updated to also remove/add those IDs.
        let (stmt, mut resolved_ids) = match mz_sql::names::resolve(&catalog, (*stmt).clone()) {
            Ok(resolved) => resolved,
            Err(e) => return ctx.retire(Err(e.into())),
        };
        // N.B. The catalog can change during purification so we must validate that the dependencies still exist after
        // purification.  This should be done back on the main thread.
        // We do the validation:
        //   - In the handler for `Message::PurifiedStatementReady`, before we handle the purified statement.
        // If we add special handling for more types of `Statement`s, we'll need to ensure similar verification
        // occurs.
        let (stmt, resolved_ids) = match stmt {
            // `CREATE SOURCE` statements must be purified off the main
            // coordinator thread of control.
            stmt @ (Statement::CreateSource(_)
            | Statement::AlterSource(_)
            | Statement::CreateSink(_)) => {
                let internal_cmd_tx = self.internal_cmd_tx.clone();
                let conn_id = ctx.session().conn_id().clone();
                let catalog = self.owned_catalog();
                let now = self.now();
                let otel_ctx = OpenTelemetryContext::obtain();
                let current_storage_configuration = self.controller.storage.config().clone();
                task::spawn(|| format!("purify:{conn_id}"), async move {
                    let catalog = catalog.for_session(ctx.session());

                    // Checks if the session is authorized to purify a statement. Usually
                    // authorization is checked after planning, however purification happens before
                    // planning, which may require the use of some connections and secrets.
                    if let Err(e) = rbac::check_usage(
                        &catalog,
                        ctx.session(),
                        &resolved_ids,
                        &CREATE_ITEM_USAGE,
                    ) {
                        return ctx.retire(Err(e.into()));
                    }

                    let result = mz_sql::pure::purify_statement(
                        catalog,
                        now,
                        stmt,
                        &current_storage_configuration,
                    )
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
                return;
            }

            // `CREATE SUBSOURCE` statements are disallowed for users and are only generated
            // automatically as part of purification
            Statement::CreateSubsource(_) => {
                ctx.retire(Err(AdapterError::Unsupported(
                    "CREATE SUBSOURCE statements",
                )));
                return;
            }

            Statement::CreateMaterializedView(mut cmvs) => {
                // `CREATE MATERIALIZED VIEW ... AS OF ...` syntax is disallowed for users and is
                // only used for storing initial frontiers in the catalog.
                if cmvs.as_of.is_some() {
                    return ctx.retire(Err(AdapterError::Unsupported(
                        "CREATE MATERIALIZED VIEW ... AS OF statements",
                    )));
                }

                let mz_now = match self
                    .resolve_mz_now_for_create_materialized_view(
                        &cmvs,
                        &resolved_ids,
                        ctx.session_mut(),
                        true,
                    )
                    .await
                {
                    Ok(mz_now) => mz_now,
                    Err(e) => return ctx.retire(Err(e)),
                };

                let owned_catalog = self.owned_catalog();
                let catalog = owned_catalog.for_session(ctx.session());

                purify_create_materialized_view_options(
                    catalog,
                    mz_now,
                    &mut cmvs,
                    &mut resolved_ids,
                );

                let purified_stmt =
                    Statement::CreateMaterializedView(CreateMaterializedViewStatement::<Aug> {
                        if_exists: cmvs.if_exists,
                        name: cmvs.name,
                        columns: cmvs.columns,
                        in_cluster: cmvs.in_cluster,
                        query: cmvs.query,
                        with_options: cmvs.with_options,
                        as_of: None,
                    });

                // (Purifying CreateMaterializedView doesn't happen async, so no need to send
                // `Message::PurifiedStatementReady` here.)
                (purified_stmt, resolved_ids)
            }

            Statement::ExplainPlan(ExplainPlanStatement {
                stage,
                with_options,
                format,
                explainee: Explainee::CreateMaterializedView(box_cmvs, broken),
            }) => {
                let mut cmvs = *box_cmvs;
                let mz_now = match self
                    .resolve_mz_now_for_create_materialized_view(
                        &cmvs,
                        &resolved_ids,
                        ctx.session_mut(),
                        false,
                    )
                    .await
                {
                    Ok(mz_now) => mz_now,
                    Err(e) => return ctx.retire(Err(e)),
                };

                let owned_catalog = self.owned_catalog();
                let catalog = owned_catalog.for_session(ctx.session());

                purify_create_materialized_view_options(
                    catalog,
                    mz_now,
                    &mut cmvs,
                    &mut resolved_ids,
                );

                let purified_stmt = Statement::ExplainPlan(ExplainPlanStatement {
                    stage,
                    with_options,
                    format,
                    explainee: Explainee::CreateMaterializedView(Box::new(cmvs), broken),
                });

                (purified_stmt, resolved_ids)
            }

            // All other statements are handled immediately.
            _ => (stmt, resolved_ids),
        };

        match self.plan_statement(ctx.session(), stmt, &params, &resolved_ids) {
            Ok(plan) => self.sequence_plan(ctx, plan, resolved_ids).await,
            Err(e) => ctx.retire(Err(e)),
        }
    }

    /// Chooses a timestamp for `mz_now()`, if `mz_now()` occurs in the `with_options` of the materialized view.
    /// If `acquire_read_holds` is true, it also grabs read holds on input collections that might possibly be involved
    /// in the MV.
    ///
    /// Note that this is NOT what handles `mz_now()` in the query part of the MV. (handles it only in `with_options`).
    async fn resolve_mz_now_for_create_materialized_view<'a>(
        &mut self,
        cmvs: &CreateMaterializedViewStatement<Aug>,
        resolved_ids: &ResolvedIds,
        session: &mut Session,
        acquire_read_holds: bool,
    ) -> Result<Option<Timestamp>, AdapterError> {
        // (This won't be the same timestamp as the system table inserts, unfortunately.)
        if cmvs
            .with_options
            .iter()
            .any(materialized_view_option_contains_temporal)
        {
            let timeline_context = self.validate_timeline_context(resolved_ids.0.clone())?;

            // We default to EpochMilliseconds, similarly to `determine_timestamp_for`,
            // but even in the TimestampIndependent case.
            // Note that we didn't accurately decide whether we are TimestampDependent
            // or TimestampIndependent, because for this we'd need to also check whether
            // `query.contains_temporal()`, similarly to how `peek_stage_validate` does.
            // However, this doesn't matter here, as we are just going to default to
            // EpochMilliseconds in both cases.
            let timeline = timeline_context
                .timeline()
                .unwrap_or(&Timeline::EpochMilliseconds);

            // Let's start with the timestamp oracle read timestamp.
            let mut timestamp = self.get_timestamp_oracle(timeline).read_ts().await;

            // If `least_valid_read` is later than the oracle, then advance to that time.
            // If we didn't do this, then there would be a danger of missing the first refresh,
            // which might cause the materialized view to be unreadable for hours. This might
            // be what was happening here:
            // https://github.com/MaterializeInc/materialize/issues/24288#issuecomment-1931856361
            //
            // In the long term, it would be good to actually block the MV creation statement
            // until `least_valid_read`. https://github.com/MaterializeInc/materialize/issues/25127
            // Without blocking, we have the problem that a REFRESH AT CREATION is not linearized
            // with the CREATE MATERIALIZED VIEW statement, in the sense that a query from the MV
            // after its creation might see input changes that happened after the CRATE MATERIALIZED
            // VIEW statement returned.
            let catalog = self.catalog().for_session(session);
            let cluster = mz_sql::plan::resolve_cluster_for_materialized_view(&catalog, cmvs)?;
            let ids = self
                .index_oracle(cluster)
                .sufficient_collections(resolved_ids.0.iter());
            let oracle_timestamp = timestamp;
            timestamp.advance_by(self.least_valid_read(&ids).borrow());
            if oracle_timestamp != timestamp {
                warn!(%cmvs.name, %oracle_timestamp, %timestamp, "REFRESH MV's inputs are not readable at the oracle read ts");
            }

            if acquire_read_holds {
                self.acquire_read_holds_auto_cleanup(session, timestamp, &ids, false)
                    .expect("precise==false, so acquiring read holds always succeeds");
            }

            Ok(Some(timestamp))
        } else {
            Ok(None)
        }
    }

    /// Instruct the dataflow layer to cancel any ongoing, interactive work for
    /// the named `conn_id` if the correct secret key is specified.
    ///
    /// Note: Here we take a [`ConnectionIdType`] as opposed to an owned
    /// `ConnectionId` because this method gets called by external clients when
    /// they request to cancel a request.
    #[mz_ore::instrument(level = "debug")]
    async fn handle_cancel(&mut self, conn_id: ConnectionIdType, secret_key: u32) {
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
            self.handle_privileged_cancel(id_handle.clone()).await;
        }
    }

    /// Unconditionally instructs the dataflow layer to cancel any ongoing,
    /// interactive work for the named `conn_id`.
    #[mz_ore::instrument(level = "debug")]
    pub(crate) async fn handle_privileged_cancel(&mut self, conn_id: ConnectionId) {
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

        // Cancel reads waiting on being linearized. There is at most one linearized read per
        // session.
        if let Some(pending_read_txn) = self.pending_linearize_read_txns.remove(&conn_id) {
            let ctx = pending_read_txn.take_context();
            maybe_ctx = Some(ctx);
        }

        if let Some(ctx) = maybe_ctx {
            ctx.retire(Err(AdapterError::Canceled));
        }

        self.cancel_pending_peeks(&conn_id);
        self.cancel_compute_sinks_for_conn(&conn_id).await;
    }

    /// Handle termination of a client session.
    ///
    /// This cleans up any state in the coordinator associated with the session.
    #[mz_ore::instrument(level = "debug")]
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
        self.clear_connection(&conn_id).await;

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
        let _builtin_update_notify = self.builtin_table_update().defer(vec![update]);
    }

    /// Returns the necessary metadata for appending to a webhook source, and a channel to send
    /// rows.
    #[mz_ore::instrument(level = "debug")]
    fn handle_get_webhook(
        &mut self,
        database: String,
        schema: String,
        name: String,
        tx: oneshot::Sender<Result<AppendWebhookResponse, AppendWebhookError>>,
    ) {
        /// Attempts to resolve a Webhook source from a provided `database.schema.name` path.
        ///
        /// Returns a struct that can be used to append data to the underlying storate collection, and the
        /// types we should cast the request to.
        fn resolve(
            coord: &mut Coordinator,
            database: String,
            schema: String,
            name: String,
        ) -> Result<AppendWebhookResponse, PartialItemName> {
            // Resolve our collection.
            let name = PartialItemName {
                database: Some(database),
                schema: Some(schema),
                item: name,
            };
            let Ok(entry) = coord
                .catalog()
                .resolve_entry(None, &vec![], &name, &SYSTEM_CONN_ID)
            else {
                return Err(name);
            };

            let (body_format, header_tys, validator) = match entry.item() {
                CatalogItem::Source(Source {
                    data_source:
                        DataSourceDesc::Webhook {
                            validate_using,
                            body_format,
                            headers,
                            ..
                        },
                    desc,
                    ..
                }) => {
                    // Assert we have one column for the body, and how ever many are required for
                    // the headers.
                    let num_columns = headers.num_columns() + 1;
                    mz_ore::soft_assert_or_log!(
                        desc.arity() <= num_columns,
                        "expected at most {} columns, but got {}",
                        num_columns,
                        desc.arity()
                    );

                    // Double check that the body column of the webhook source matches the type
                    // we're about to deserialize as.
                    let body_column = desc
                        .get_by_name(&"body".into())
                        .map(|(_idx, ty)| ty.clone())
                        .ok_or(name.clone())?;
                    assert!(!body_column.nullable, "webhook body column is nullable!?");
                    assert_eq!(body_column.scalar_type, ScalarType::from(*body_format));

                    // Create a validator that can be called to validate a webhook request.
                    let validator = validate_using.as_ref().map(|v| {
                        let validation = v.clone();
                        AppendWebhookValidator::new(
                            validation,
                            coord.caching_secrets_reader.clone(),
                        )
                    });
                    (*body_format, headers.clone(), validator)
                }
                _ => return Err(name),
            };

            // Get a channel so we can queue updates to be written.
            let row_tx = coord
                .controller
                .storage
                .monotonic_appender(entry.id())
                .map_err(|_| name)?;
            let invalidator = coord
                .active_webhooks
                .entry(entry.id())
                .or_insert_with(WebhookAppenderInvalidator::new);
            let tx = WebhookAppender::new(row_tx, invalidator.guard());

            Ok(AppendWebhookResponse {
                tx,
                body_format,
                header_tys,
                validator,
            })
        }

        let response = resolve(self, database, schema, name).map_err(|name| {
            AppendWebhookError::UnknownWebhook {
                database: name.database.expect("provided"),
                schema: name.schema.expect("provided"),
                name: name.item,
            }
        });
        let _ = tx.send(response);
    }
}

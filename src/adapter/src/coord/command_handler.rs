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
use mz_adapter_types::dyncfgs::ALLOW_USER_SESSIONS;
use mz_auth::password::Password;
use mz_repr::namespaces::MZ_INTERNAL_SCHEMA;
use mz_sql::session::metadata::SessionMetadata;
use std::collections::{BTreeMap, BTreeSet};
use std::net::IpAddr;
use std::sync::Arc;

use futures::FutureExt;
use futures::future::LocalBoxFuture;
use mz_adapter_types::connection::{ConnectionId, ConnectionIdType};
use mz_catalog::SYSTEM_CONN_ID;
use mz_catalog::memory::objects::{CatalogItem, DataSourceDesc, Source, Table, TableDataSource};
use mz_ore::task;
use mz_ore::tracing::OpenTelemetryContext;
use mz_ore::{instrument, soft_panic_or_log};
use mz_repr::role_id::RoleId;
use mz_repr::{Diff, ScalarType, Timestamp};
use mz_sql::ast::{
    AlterConnectionAction, AlterConnectionStatement, AlterSourceAction, AstInfo, ConstantVisitor,
    CopyRelation, CopyStatement, CreateSourceOptionName, Raw, Statement, SubscribeStatement,
};
use mz_sql::catalog::RoleAttributesRaw;
use mz_sql::names::{Aug, PartialItemName, ResolvedIds};
use mz_sql::plan::{
    AbortTransactionPlan, CommitTransactionPlan, CreateRolePlan, Params, Plan,
    StatementClassification, TransactionType,
};
use mz_sql::pure::{
    materialized_view_option_contains_temporal, purify_create_materialized_view_options,
};
use mz_sql::rbac;
use mz_sql::rbac::CREATE_ITEM_USAGE;
use mz_sql::session::user::User;
use mz_sql::session::vars::{
    EndTransactionAction, NETWORK_POLICY, OwnedVarInput, STATEMENT_LOGGING_SAMPLE_RATE, Value, Var,
};
use mz_sql_parser::ast::display::AstDisplay;
use mz_sql_parser::ast::{
    CreateMaterializedViewStatement, ExplainPlanStatement, Explainee, InsertStatement,
    WithOptionValue,
};
use mz_storage_types::sources::Timeline;
use opentelemetry::trace::TraceContextExt;
use tokio::sync::{mpsc, oneshot};
use tracing::{Instrument, debug_span, info, warn};
use tracing_opentelemetry::OpenTelemetrySpanExt;

use crate::command::{AuthResponse, CatalogSnapshot, Command, ExecuteResponse, StartupResponse};
use crate::coord::appends::PendingWriteTxn;
use crate::coord::{
    ConnMeta, Coordinator, DeferredPlanStatement, Message, PendingTxn, PlanStatement, PlanValidity,
    PurifiedStatementReady, validate_ip_with_policy_rules,
};
use crate::error::AdapterError;
use crate::notice::AdapterNotice;
use crate::session::{Session, TransactionOps, TransactionStatus};
use crate::util::{ClientTransmitter, ResultExt};
use crate::webhook::{
    AppendWebhookResponse, AppendWebhookValidator, WebhookAppender, WebhookAppenderInvalidator,
};
use crate::{AppendWebhookError, ExecuteContext, TimestampProvider, catalog, metrics};

use super::ExecuteContextExtra;

impl Coordinator {
    /// BOXED FUTURE: As of Nov 2023 the returned Future from this function was 58KB. This would
    /// get stored on the stack which is bad for runtime performance, and blow up our stack usage.
    /// Because of that we purposefully move this Future onto the heap (i.e. Box it).
    pub(crate) fn handle_command(&mut self, mut cmd: Command) -> LocalBoxFuture<()> {
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
                    client_ip,
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
                        client_ip,
                        application_name,
                        notice_tx,
                    )
                    .await;
                }

                Command::AuthenticatePassword {
                    tx,
                    role_name,
                    password,
                } => {
                    self.handle_authenticate_password(tx, role_name, password)
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

                Command::GetSystemVars { tx } => {
                    let _ = tx.send(self.catalog.system_config().clone());
                }

                Command::SetSystemVars { vars, conn_id, tx } => {
                    let mut ops = Vec::with_capacity(vars.len());
                    let conn = &self.active_conns[&conn_id];

                    for (name, value) in vars {
                        if let Err(e) =
                            self.catalog().system_config().get(&name).and_then(|var| {
                                var.visible(conn.user(), self.catalog.system_config())
                            })
                        {
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

                    let conn_id = ctx.session().conn_id().clone();
                    self.sequence_plan(ctx, plan, ResolvedIds::empty()).await;
                    // Part of the Command::Commit contract is that the Coordinator guarantees that
                    // it has cleared its transaction state for the connection.
                    self.clear_connection(&conn_id).await;
                }

                Command::CatalogSnapshot { tx } => {
                    let _ = tx.send(CatalogSnapshot {
                        catalog: self.owned_catalog(),
                    });
                }

                Command::CheckConsistency { tx } => {
                    let _ = tx.send(self.check_consistency());
                }

                Command::Dump { tx } => {
                    let _ = tx.send(self.dump().await);
                }
            }
        }
        .instrument(debug_span!("handle_command"))
        .boxed_local()
    }

    #[mz_ore::instrument(level = "debug")]
    async fn handle_authenticate_password(
        &mut self,
        tx: oneshot::Sender<Result<AuthResponse, AdapterError>>,
        role_name: String,
        password: Option<Password>,
    ) {
        let Some(password) = password else {
            // The user did not provide a password.
            let _ = tx.send(Err(AdapterError::AuthenticationError));
            return;
        };

        if let Some(role) = self.catalog().try_get_role_by_name(role_name.as_str()) {
            if !role.attributes.login.unwrap_or(false) {
                // The user is not allowed to login.
                let _ = tx.send(Err(AdapterError::AuthenticationError));
                return;
            }
            if let Some(auth) = self.catalog().try_get_role_auth_by_id(&role.id) {
                if let Some(hash) = &auth.password_hash {
                    let _ = match mz_auth::hash::scram256_verify(&password, hash) {
                        Ok(_) => tx.send(Ok(AuthResponse {
                            role_id: role.id,
                            superuser: role.attributes.superuser.unwrap_or(false),
                        })),
                        Err(_) => tx.send(Err(AdapterError::AuthenticationError)),
                    };
                    return;
                }
            }
            // Authentication failed due to incorrect password or missing password hash.
            let _ = tx.send(Err(AdapterError::AuthenticationError));
        } else {
            // The user does not exist.
            let _ = tx.send(Err(AdapterError::AuthenticationError));
        }
    }

    #[mz_ore::instrument(level = "debug")]
    async fn handle_startup(
        &mut self,
        tx: oneshot::Sender<Result<StartupResponse, AdapterError>>,
        user: User,
        conn_id: ConnectionId,
        secret_key: u32,
        uuid: uuid::Uuid,
        client_ip: Option<IpAddr>,
        application_name: String,
        notice_tx: mpsc::UnboundedSender<AdapterNotice>,
    ) {
        // Early return if successful, otherwise cleanup any possible state.
        match self.handle_startup_inner(&user, &conn_id, &client_ip).await {
            Ok((role_id, session_defaults)) => {
                let session_type = metrics::session_type_label_value(&user);
                self.metrics
                    .active_sessions
                    .with_label_values(&[session_type])
                    .inc();
                let conn = ConnMeta {
                    secret_key,
                    notice_tx,
                    drop_sinks: BTreeSet::new(),
                    pending_cluster_alters: BTreeSet::new(),
                    connected_at: self.now(),
                    user,
                    application_name,
                    uuid,
                    client_ip,
                    conn_id: conn_id.clone(),
                    authenticated_role: role_id,
                    deferred_lock: None,
                };
                let update = self.catalog().state().pack_session_update(&conn, Diff::ONE);
                let update = self.catalog().state().resolve_builtin_table_update(update);
                self.begin_session_for_statement_logging(&conn);
                self.active_conns.insert(conn_id.clone(), conn);

                // Note: Do NOT await the notify here, we pass this back to
                // whatever requested the startup to prevent blocking startup
                // and the Coordinator on a builtin table update.
                let updates = vec![update];
                // It's not a hard error if our list is missing a builtin table, but we want to
                // make sure these two things stay in-sync.
                if mz_ore::assert::soft_assertions_enabled() {
                    let required_tables: BTreeSet<_> = super::appends::REQUIRED_BUILTIN_TABLES
                        .iter()
                        .map(|table| self.catalog().resolve_builtin_table(*table))
                        .collect();
                    let updates_tracked = updates
                        .iter()
                        .all(|update| required_tables.contains(&update.id));
                    let all_mz_internal = super::appends::REQUIRED_BUILTIN_TABLES
                        .iter()
                        .all(|table| table.schema == MZ_INTERNAL_SCHEMA);
                    mz_ore::soft_assert_or_log!(
                        updates_tracked,
                        "not tracking all required builtin table updates!"
                    );
                    // TODO(parkmycar): When checking if a query depends on these builtin table
                    // writes we do not check the transitive dependencies of the query, because
                    // we don't support creating views on mz_internal objects. If one of these
                    // tables is promoted out of mz_internal then we'll need to add this check.
                    mz_ore::soft_assert_or_log!(
                        all_mz_internal,
                        "not all builtin tables are in mz_internal! need to check transitive depends",
                    )
                }
                let notify = self.builtin_table_update().background(updates);

                let resp = Ok(StartupResponse {
                    role_id,
                    write_notify: notify,
                    session_defaults,
                    catalog: self.owned_catalog(),
                });
                if tx.send(resp).is_err() {
                    // Failed to send to adapter, but everything is setup so we can terminate
                    // normally.
                    self.handle_terminate(conn_id).await;
                }
            }
            Err(e) => {
                // Error during startup or sending to adapter, cleanup possible state created by
                // handle_startup_inner. A user may have been created and it can stay; no need to
                // delete it.
                self.catalog_mut()
                    .drop_temporary_schema(&conn_id)
                    .unwrap_or_terminate("unable to drop temporary schema");

                // Communicate the error back to the client. No need to
                // handle failures to send the error back; we've already
                // cleaned up all necessary state.
                let _ = tx.send(Err(e));
            }
        }
    }

    // Failible startup work that needs to be cleaned up on error.
    async fn handle_startup_inner(
        &mut self,
        user: &User,
        conn_id: &ConnectionId,
        client_ip: &Option<IpAddr>,
    ) -> Result<(RoleId, BTreeMap<String, OwnedVarInput>), AdapterError> {
        if self.catalog().try_get_role_by_name(&user.name).is_none() {
            // If the user has made it to this point, that means they have been fully authenticated.
            // This includes preventing any user, except a pre-defined set of system users, from
            // connecting to an internal port. Therefore it's ok to always create a new role for the
            // user.
            let attributes = RoleAttributesRaw::new();
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

        if role_id.is_user() && !ALLOW_USER_SESSIONS.get(self.catalog().system_config().dyncfgs()) {
            return Err(AdapterError::UserSessionsDisallowed);
        }

        // Initialize the default session variables for this role.
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

        // Validate network policies for external users. Internal users can only connect on the
        // internal interfaces (internal HTTP/ pgwire). It is up to the person deploying the system
        // to ensure these internal interfaces are well secured.
        //
        // HACKY(parkmycar): We don't have a fully formed session yet for this role, but we want
        // the default network policy for this role, so we read directly out of what the session
        // will get initialized with.
        if !user.is_internal() {
            let network_policy_name = session_defaults
                .get(NETWORK_POLICY.name())
                .and_then(|value| match value {
                    OwnedVarInput::Flat(name) => Some(name.clone()),
                    OwnedVarInput::SqlSet(names) => {
                        tracing::error!(?names, "found multiple network policies");
                        None
                    }
                })
                .unwrap_or_else(|| system_config.default_network_policy_name());
            let maybe_network_policy = self
                .catalog()
                .get_network_policy_by_name(&network_policy_name);

            let Some(network_policy) = maybe_network_policy else {
                // We should prevent dropping the default network policy, or setting the policy
                // to something that doesn't exist, so complain loudly if this occurs.
                tracing::error!(
                    network_policy_name,
                    "default network policy does not exist. All user traffic will be blocked"
                );
                let reason = match client_ip {
                    Some(ip) => super::NetworkPolicyError::AddressDenied(ip.clone()),
                    None => super::NetworkPolicyError::MissingIp,
                };
                return Err(AdapterError::NetworkPolicyDenied(reason));
            };

            if let Some(ip) = client_ip {
                match validate_ip_with_policy_rules(ip, &network_policy.rules) {
                    Ok(_) => {}
                    Err(e) => return Err(AdapterError::NetworkPolicyDenied(e)),
                }
            } else {
                // Only temporary and internal representation of a session
                // should be missing a client_ip. These sessions should not be
                // making requests or going through handle_startup.
                return Err(AdapterError::NetworkPolicyDenied(
                    super::NetworkPolicyError::MissingIp,
                ));
            }
        }

        self.catalog_mut()
            .create_temporary_schema(conn_id, role_id)?;

        Ok((role_id, session_defaults))
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
            let lifecycle_timestamps = portal.lifecycle_timestamps.clone();

            let extra = if let Some(extra) = outer_context {
                // We are executing in the context of another SQL statement, so we don't
                // want to begin statement logging anew. The context of the actual statement
                // being executed is the one that should be retired once this finishes.
                extra
            } else {
                // This is a new statement, log it and return the context
                let maybe_uuid = self.begin_statement_execution(
                    &mut session,
                    &params,
                    &logging,
                    lifecycle_timestamps,
                );

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
        // This comment describes the various ways DDL can execute (the ordered operations: name
        // resolve, purify, plan, sequence), all of which are managed by this function. DDL has
        // three notable properties that all partially interact.
        //
        // 1. Most DDL statements (and a few others) support single-statement transaction delayed
        //    execution. This occurs when a session executes `BEGIN`, a single DDL, then `COMMIT`.
        //    We announce success of the single DDL when it is executed, but do not attempt to plan
        //    or sequence it until `COMMIT`, which is able to error if needed while sequencing the
        //    DDL (this behavior is Postgres-compatible). The purpose of this is because some
        //    drivers or tools wrap all statements in `BEGIN` and `COMMIT` and we would like them to
        //    work. When the single DDL is announced as successful we also put the session's
        //    transaction ops into `SingleStatement` which will produce an error if any other
        //    statement is run in the transaction except `COMMIT`. Additionally, this will cause
        //    `handle_execute_inner` to stop further processing (no planning, etc.) of the
        //    statement.
        // 2. A few other DDL statements (`ALTER .. RENAME/SWAP`) enter the `DDL` ops which allows
        //    any number of only these DDL statements to be executed in a transaction. At sequencing
        //    these generate the `Op::TransactionDryRun` catalog op. When applied with
        //    `catalog_transact`, that op will always produce the `TransactionDryRun` error. The
        //    `catalog_transact_with_ddl_transaction` function intercepts that error and reports
        //    success to the user, but nothing is yet committed to the real catalog. At `COMMIT` all
        //    of the ops but without dry run are applied. The purpose of this is to allow multiple,
        //    atomic renames in the same transaction.
        // 3. Some DDLs do off-thread work during purification or sequencing that is expensive or
        //    makes network calls (interfacing with secrets, optimization of views/indexes, source
        //    purification). These must guarantee correctness when they return to the main
        //    coordinator thread because the catalog state could have changed while they were doing
        //    the off-thread work. Previously we would use `PlanValidity::Checks` to specify a bunch
        //    of IDs that we needed to exist. We discovered the way we were doing that was not
        //    always correct. Instead of attempting to get that completely right, we have opted to
        //    serialize DDL. Getting this right is difficult because catalog changes can affect name
        //    resolution, planning, sequencing, and optimization. Correctly writing logic that is
        //    aware of all possible catalog changes that would affect any of those parts is not
        //    something our current code has been designed to be helpful at. Even if a DDL statement
        //    is doing off-thread work, another DDL must not yet execute at all. Executing these
        //    serially will guarantee that no off-thread work has affected the state of the catalog.
        //    This is done by adding a VecDeque of deferred statements and a lock to the
        //    Coordinator. When a DDL is run in `handle_execute_inner` (after applying whatever
        //    transaction ops are needed to the session as described above), it attempts to own the
        //    lock (a tokio Mutex). If acquired, it stashes the lock in the connection`s `ConnMeta`
        //    struct in `active_conns` and proceeds. The lock is dropped at transaction end in
        //    `clear_transaction` and a message sent to the Coordinator to execute the next queued
        //    DDL. If the lock could not be acquired, the DDL is put into the VecDeque where it
        //    awaits dequeuing caused by the lock being released.

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
                    | Statement::ExplainAnalyze(_)
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

                    // These statements must be kept in-sync with `must_serialize_ddl()`.
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
                    | Statement::AlterRetainHistory(_)
                    | Statement::AlterRole(_)
                    | Statement::AlterSecret(_)
                    | Statement::AlterSink(_)
                    | Statement::AlterSource(_)
                    | Statement::AlterSystemReset(_)
                    | Statement::AlterSystemResetAll(_)
                    | Statement::AlterSystemSet(_)
                    | Statement::AlterTableAddColumn(_)
                    | Statement::AlterNetworkPolicy(_)
                    | Statement::CreateCluster(_)
                    | Statement::CreateClusterReplica(_)
                    | Statement::CreateConnection(_)
                    | Statement::CreateDatabase(_)
                    | Statement::CreateIndex(_)
                    | Statement::CreateMaterializedView(_)
                    | Statement::CreateContinualTask(_)
                    | Statement::CreateRole(_)
                    | Statement::CreateSchema(_)
                    | Statement::CreateSecret(_)
                    | Statement::CreateSink(_)
                    | Statement::CreateSource(_)
                    | Statement::CreateSubsource(_)
                    | Statement::CreateTable(_)
                    | Statement::CreateTableFromSource(_)
                    | Statement::CreateType(_)
                    | Statement::CreateView(_)
                    | Statement::CreateWebhookSource(_)
                    | Statement::CreateNetworkPolicy(_)
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

        // DDLs must be planned and sequenced serially. We do not rely on PlanValidity checking
        // various IDs because we have incorrectly done that in the past. Attempt to acquire the
        // ddl lock. The lock is stashed in the ConnMeta which is dropped at transaction end. If
        // acquired, proceed with sequencing. If not, enqueue and return. This logic assumes that
        // Coordinator::clear_transaction is correctly called when session transactions are ended
        // because that function will release the held lock from active_conns.
        if Self::must_serialize_ddl(&stmt, &ctx) {
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
                if self
                    .active_conns
                    .get(ctx.session().conn_id())
                    .expect("connection must exist")
                    .deferred_lock
                    .is_some()
                {
                    // This session *already* has the lock, and incorrectly tried to execute another
                    // DDL while still holding the lock, violating the assumption documented above.
                    // This is an internal error, probably in some AdapterClient user (pgwire or
                    // http). Because the session is now in some unexpected state, return an error
                    // which should cause the AdapterClient user to fail the transaction.
                    // (Terminating the connection is maybe what we would prefer to do, but is not
                    // currently a thing we can do from the coordinator: calling handle_terminate
                    // cleans up Coordinator state for the session but doesn't inform the
                    // AdapterClient that the session should terminate.)
                    soft_panic_or_log!(
                        "session {} attempted to get ddl lock while already owning it",
                        ctx.session().conn_id()
                    );
                    ctx.retire(Err(AdapterError::Internal(
                        "session attempted to get ddl lock while already owning it".to_string(),
                    )));
                    return;
                }
                self.serialized_ddl.push_back(DeferredPlanStatement {
                    ctx,
                    ps: PlanStatement::Statement { stmt, params },
                });
                return;
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
            // Various statements must be purified off the main coordinator thread of control.
            stmt if Self::must_spawn_purification(&stmt) => {
                let internal_cmd_tx = self.internal_cmd_tx.clone();
                let conn_id = ctx.session().conn_id().clone();
                let catalog = self.owned_catalog();
                let now = self.now();
                let otel_ctx = OpenTelemetryContext::obtain();
                let current_storage_configuration = self.controller.storage.config().clone();
                task::spawn(|| format!("purify:{conn_id}"), async move {
                    let transient_revision = catalog.transient_revision();
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

                    let (result, cluster_id) = mz_sql::pure::purify_statement(
                        catalog,
                        now,
                        stmt,
                        &current_storage_configuration,
                    )
                    .await;
                    let result = result.map_err(|e| e.into());
                    let dependency_ids = resolved_ids.items().copied().collect();
                    let plan_validity = PlanValidity::new(
                        transient_revision,
                        dependency_ids,
                        cluster_id,
                        None,
                        ctx.session().role_metadata().clone(),
                    );
                    // It is not an error for purification to complete after `internal_cmd_rx` is dropped.
                    let result = internal_cmd_tx.send(Message::PurifiedStatementReady(
                        PurifiedStatementReady {
                            ctx,
                            result,
                            params,
                            plan_validity,
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

    /// Whether the statement must be serialized and is DDL.
    fn must_serialize_ddl(stmt: &Statement<Raw>, ctx: &ExecuteContext) -> bool {
        // Non-DDL is not serialized here.
        if !StatementClassification::from(&*stmt).is_ddl() {
            return false;
        }
        // Off-thread, pre-planning purification can perform arbitrarily slow network calls so must
        // not be serialized. These all use PlanValidity for their checking, and we must ensure
        // those checks are sufficient.
        if Self::must_spawn_purification(stmt) {
            return false;
        }

        // Statements that support multiple DDLs in a single transaction aren't serialized here.
        // Their operations are serialized when applied to the catalog, guaranteeing that any
        // off-thread DDLs concurrent with a multiple DDL transaction will have a serial order.
        if ctx.session.transaction().is_ddl() {
            return false;
        }

        // Some DDL is exempt. It is not great that we are matching on Statements here because
        // different plans can be produced from the same top-level statement type (i.e., `ALTER
        // CONNECTION ROTATE KEYS`). But the whole point of this is to prevent things from being
        // planned in the first place, so we accept the abstraction leak.
        match stmt {
            // Secrets have a small and understood set of dependencies, and their off-thread work
            // interacts with k8s.
            Statement::AlterSecret(_) => false,
            Statement::CreateSecret(_) => false,
            Statement::AlterConnection(AlterConnectionStatement { actions, .. })
                if actions
                    .iter()
                    .all(|action| matches!(action, AlterConnectionAction::RotateKeys)) =>
            {
                false
            }

            // The off-thread work that altering a cluster may do (waiting for replicas to spin-up),
            // does not affect its catalog names or ids and so is safe to not serialize. This could
            // change the set of replicas that exist. For queries that name replicas or use the
            // current_replica session var, the `replica_id` field of `PlanValidity` serves to
            // ensure that those replicas exist during the query finish stage. Additionally, that
            // work can take hours (configured by the user), so would also be a bad experience for
            // users.
            Statement::AlterCluster(_) => false,

            // Everything else must be serialized.
            _ => true,
        }
    }

    /// Whether the statement must be purified off of the Coordinator thread.
    fn must_spawn_purification<A: AstInfo>(stmt: &Statement<A>) -> bool {
        // `CREATE` and `ALTER` `SOURCE` and `SINK` statements must be purified off the main
        // coordinator thread.
        if !matches!(
            stmt,
            Statement::CreateSource(_)
                | Statement::AlterSource(_)
                | Statement::CreateSink(_)
                | Statement::CreateTableFromSource(_)
        ) {
            return false;
        }

        // However `ALTER SOURCE RETAIN HISTORY` should be excluded from off-thread purification.
        if let Statement::AlterSource(stmt) = stmt {
            let names: Vec<CreateSourceOptionName> = match &stmt.action {
                AlterSourceAction::SetOptions(options) => {
                    options.iter().map(|o| o.name.clone()).collect()
                }
                AlterSourceAction::ResetOptions(names) => names.clone(),
                _ => vec![],
            };
            if !names.is_empty()
                && names
                    .iter()
                    .all(|n| matches!(n, CreateSourceOptionName::RetainHistory))
            {
                return false;
            }
        }

        true
    }

    /// Chooses a timestamp for `mz_now()`, if `mz_now()` occurs in a REFRESH option of the
    /// materialized view. Additionally, if `acquire_read_holds` is true and the MV has any REFRESH
    /// option, this function grabs read holds at the earliest possible time on input collections
    /// that might be involved in the MV.
    ///
    /// Note that this is NOT what handles `mz_now()` in the query part of the MV. (handles it only
    /// in `with_options`).
    ///
    /// (Note that the chosen timestamp won't be the same timestamp as the system table inserts,
    /// unfortunately.)
    async fn resolve_mz_now_for_create_materialized_view(
        &mut self,
        cmvs: &CreateMaterializedViewStatement<Aug>,
        resolved_ids: &ResolvedIds,
        session: &Session,
        acquire_read_holds: bool,
    ) -> Result<Option<Timestamp>, AdapterError> {
        if cmvs
            .with_options
            .iter()
            .any(|wo| matches!(wo.value, Some(WithOptionValue::Refresh(..))))
        {
            let catalog = self.catalog().for_session(session);
            let cluster = mz_sql::plan::resolve_cluster_for_materialized_view(&catalog, cmvs)?;
            let ids = self
                .index_oracle(cluster)
                .sufficient_collections(resolved_ids.collections().copied());

            // If there is any REFRESH option, then acquire read holds. (Strictly speaking, we'd
            // need this only if there is a `REFRESH AT`, not for `REFRESH EVERY`, because later
            // we want to check the AT times against the read holds that we acquire here. But
            // we do it for any REFRESH option, to avoid having so many code paths doing different
            // things.)
            //
            // It's important that we acquire read holds _before_ we determine the least valid read.
            // Otherwise, we're not guaranteed that the since frontier doesn't
            // advance forward from underneath us.
            let read_holds = self.acquire_read_holds(&ids);

            // Does `mz_now()` occur?
            let mz_now_ts = if cmvs
                .with_options
                .iter()
                .any(materialized_view_option_contains_temporal)
            {
                let timeline_context =
                    self.validate_timeline_context(resolved_ids.collections().copied())?;

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
                // https://github.com/MaterializeInc/database-issues/issues/7265#issuecomment-1931856361
                //
                // In the long term, it would be good to actually block the MV creation statement
                // until `least_valid_read`. https://github.com/MaterializeInc/database-issues/issues/7504
                // Without blocking, we have the problem that a REFRESH AT CREATION is not linearized
                // with the CREATE MATERIALIZED VIEW statement, in the sense that a query from the MV
                // after its creation might see input changes that happened after the CRATE MATERIALIZED
                // VIEW statement returned.
                let oracle_timestamp = timestamp;
                let least_valid_read = self.least_valid_read(&read_holds);
                timestamp.advance_by(least_valid_read.borrow());

                if oracle_timestamp != timestamp {
                    warn!(%cmvs.name, %oracle_timestamp, %timestamp, "REFRESH MV's inputs are not readable at the oracle read ts");
                }

                info!("Resolved `mz_now()` to {timestamp} for REFRESH MV");
                Ok(Some(timestamp))
            } else {
                Ok(None)
            };

            // NOTE: The Drop impl of ReadHolds makes sure that the hold is
            // released when we don't use it.
            if acquire_read_holds {
                self.store_transaction_read_holds(session, read_holds);
            }

            mz_now_ts
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
        let mut maybe_ctx = None;

        // Cancel pending writes. There is at most one pending write per session.
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

        // Cancel deferred writes.
        if let Some(write_op) = self.deferred_write_ops.remove(&conn_id) {
            maybe_ctx = Some(write_op.into_ctx());
        }

        // Cancel deferred statements.
        if let Some(idx) = self
            .serialized_ddl
            .iter()
            .position(|deferred| *deferred.ctx.session().conn_id() == conn_id)
        {
            let deferred = self
                .serialized_ddl
                .remove(idx)
                .expect("known to exist from call to `position` above");
            maybe_ctx = Some(deferred.ctx);
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
        self.cancel_pending_watchsets(&conn_id);
        self.cancel_compute_sinks_for_conn(&conn_id).await;
        self.cancel_cluster_reconfigurations_for_conn(&conn_id)
            .await;
        self.cancel_pending_copy(&conn_id);
        if let Some((tx, _rx)) = self.staged_cancellation.get_mut(&conn_id) {
            let _ = tx.send(true);
        }
    }

    /// Handle termination of a client session.
    ///
    /// This cleans up any state in the coordinator associated with the session.
    #[mz_ore::instrument(level = "debug")]
    async fn handle_terminate(&mut self, conn_id: ConnectionId) {
        if !self.active_conns.contains_key(&conn_id) {
            // If the session doesn't exist in `active_conns`, then this method will panic later on.
            // Instead we explicitly panic here while dumping the entire Coord to the logs to help
            // debug. This panic is very infrequent so we want as much information as possible.
            // See https://github.com/MaterializeInc/database-issues/issues/5627.
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
        self.cancel_pending_watchsets(&conn_id);
        self.cancel_pending_copy(&conn_id);
        self.end_session_for_statement_logging(conn.uuid());

        // Queue the builtin table update, but do not wait for it to complete. We explicitly do
        // this to prevent blocking the Coordinator in the case that a lot of connections are
        // closed at once, which occurs regularly in some workflows.
        let update = self
            .catalog()
            .state()
            .pack_session_update(&conn, Diff::MINUS_ONE);
        let update = self.catalog().state().resolve_builtin_table_update(update);

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

            // Webhooks can be created with `CREATE SOURCE` or `CREATE TABLE`.
            let (data_source, desc, global_id) = match entry.item() {
                CatalogItem::Source(Source {
                    data_source: data_source @ DataSourceDesc::Webhook { .. },
                    desc,
                    global_id,
                    ..
                }) => (data_source, desc.clone(), *global_id),
                CatalogItem::Table(
                    table @ Table {
                        desc,
                        data_source:
                            TableDataSource::DataSource {
                                desc: data_source @ DataSourceDesc::Webhook { .. },
                                ..
                            },
                        ..
                    },
                ) => (data_source, desc.latest(), table.global_id_writes()),
                _ => return Err(name),
            };

            let DataSourceDesc::Webhook {
                validate_using,
                body_format,
                headers,
                ..
            } = data_source
            else {
                mz_ore::soft_panic_or_log!("programming error! checked above for webhook");
                return Err(name);
            };
            let body_format = body_format.clone();
            let header_tys = headers.clone();

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
                .ok_or_else(|| name.clone())?;
            assert!(!body_column.nullable, "webhook body column is nullable!?");
            assert_eq!(body_column.scalar_type, ScalarType::from(body_format));

            // Create a validator that can be called to validate a webhook request.
            let validator = validate_using.as_ref().map(|v| {
                let validation = v.clone();
                AppendWebhookValidator::new(validation, coord.caching_secrets_reader.clone())
            });

            // Get a channel so we can queue updates to be written.
            let row_tx = coord
                .controller
                .storage
                .monotonic_appender(global_id)
                .map_err(|_| name.clone())?;
            let stats = coord
                .controller
                .storage
                .webhook_statistics(global_id)
                .map_err(|_| name)?;
            let invalidator = coord
                .active_webhooks
                .entry(entry.id())
                .or_insert_with(WebhookAppenderInvalidator::new);
            let tx = WebhookAppender::new(row_tx, invalidator.guard(), stats);

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

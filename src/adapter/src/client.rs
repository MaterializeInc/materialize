// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::BTreeMap;
use std::fmt::{Debug, Display, Formatter};
use std::future::Future;
use std::pin;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::{Duration, Instant};

use anyhow::bail;
use chrono::{DateTime, Utc};
use derivative::Derivative;
use differential_dataflow::lattice::Lattice;
use futures::{Stream, StreamExt};
use itertools::Itertools;
use mz_adapter_types::connection::{ConnectionId, ConnectionIdType};
use mz_build_info::BuildInfo;
use mz_compute_client::controller::PeekClient;
use mz_compute_client::protocol::command::PeekTarget;
use mz_compute_types::ComputeInstanceId;
use mz_ore::channel::OneshotReceiverExt;
use mz_ore::collections::CollectionExt;
use mz_ore::id_gen::{org_id_conn_bits, IdAllocator, IdAllocatorInnerBitSet, MAX_ORG_ID};
use mz_ore::instrument;
use mz_ore::metrics::MetricsRegistry;
use mz_ore::now::{to_datetime, EpochMillis, NowFn};
use mz_ore::result::ResultExt;
use mz_ore::task::AbortOnDropHandle;
use mz_ore::thread::JoinOnDropHandle;
use mz_ore::tracing::OpenTelemetryContext;
use mz_repr::{GlobalId, Row, ScalarType, Timestamp};
use mz_sql::ast::{Raw, Statement};
use mz_sql::catalog::{EnvironmentId, SessionCatalog};
use mz_sql::plan::QueryWhen;
use mz_sql::session::hint::ApplicationNameHint;
use mz_sql::session::metadata::SessionMetadata;
use mz_sql::session::user::SUPPORT_USER;
use mz_sql::session::vars::IsolationLevel;
use mz_sql::session::vars::{OwnedVarInput, Var, CLUSTER};
use mz_sql_parser::parser::{ParserStatementError, StatementParseResult};
use mz_storage_types::sources::Timeline;
use mz_timestamp_oracle::TimestampOracle;
use opentelemetry::trace::TraceContextExt;
use prometheus::Histogram;
use serde_json::json;
use timely::progress::frontier::MutableAntichain;
use timely::progress::{Antichain, Timestamp as TimelyTimestamp};
use tokio::sync::{mpsc, oneshot, watch};
use tracing::{error, event, Level};
use tracing_opentelemetry::OpenTelemetrySpanExt;
use uuid::Uuid;

use crate::catalog::{Catalog, CatalogState};
use crate::command::{
    CatalogDump, CatalogSnapshot, Command, ExecuteResponse, GetVariablesResponse, Response,
};
use crate::coord::timestamp_selection::TimestampDetermination;
use crate::coord::{
    introspection, Coordinator, ExecuteContextExtra, ExplainContext, PeekStage, PeekStageFinish,
    PeekStageValidate,
};
use crate::coord::{peek, peek_exec};
use crate::error::AdapterError;
use crate::metrics::Metrics;
use crate::optimize::dataflows::ComputeInstanceSnapshot;
use crate::optimize::metrics::OptimizerMetrics;
use crate::optimize::{self, Optimize};
use crate::session::{
    EndTransactionAction, PreparedStatement, Session, SessionConfig, TransactionId,
};
use crate::statement_logging::StatementEndedExecutionReason;
use crate::telemetry::{self, SegmentClientExt, StatementFailureType};
use crate::webhook::AppendWebhookResponse;
use crate::{
    AdapterNotice, AppendWebhookError, ExecuteContext, PeekResponseUnary, StartupResponse,
    TimestampProvider,
};
use crate::{CollectionIdBundle, TimelineContext, TimestampContext};

/// A handle to a running coordinator.
///
/// The coordinator runs on its own thread. Dropping the handle will wait for
/// the coordinator's thread to exit, which will only occur after all
/// outstanding [`Client`]s for the coordinator have dropped.
pub struct Handle {
    pub(crate) session_id: Uuid,
    pub(crate) start_instant: Instant,
    pub(crate) _thread: JoinOnDropHandle<()>,
}

impl Handle {
    /// Returns the session ID associated with this coordinator.
    ///
    /// The session ID is generated on coordinator boot. It lasts for the
    /// lifetime of the coordinator. Restarting the coordinator will result
    /// in a new session ID.
    pub fn session_id(&self) -> Uuid {
        self.session_id
    }

    /// Returns the instant at which the coordinator booted.
    pub fn start_instant(&self) -> Instant {
        self.start_instant
    }
}

/// A coordinator client.
///
/// A coordinator client is a simple handle to a communication channel with the
/// coordinator. It can be cheaply cloned.
///
/// Clients keep the coordinator alive. The coordinator will not exit until all
/// outstanding clients have dropped.
#[derive(Debug, Clone)]
pub struct Client {
    build_info: &'static BuildInfo,
    inner_cmd_tx: mpsc::UnboundedSender<(OpenTelemetryContext, Command)>,
    id_alloc: IdAllocator<IdAllocatorInnerBitSet>,
    now: NowFn,
    metrics: Metrics,
    environment_id: EnvironmentId,
    segment_client: Option<mz_segment::Client>,
}

impl Client {
    pub(crate) fn new(
        build_info: &'static BuildInfo,
        cmd_tx: mpsc::UnboundedSender<(OpenTelemetryContext, Command)>,
        metrics: Metrics,
        now: NowFn,
        environment_id: EnvironmentId,
        segment_client: Option<mz_segment::Client>,
    ) -> Client {
        // Connection ids are 32 bits and have 3 parts.
        // 1. MSB bit is always 0 because these are interpreted as an i32, and it is possible some
        //    driver will not handle a negative id since postgres has never produced one because it
        //    uses process ids.
        // 2. Next 12 bits are the lower 12 bits of the org id. This allows balancerd to route
        //    incoming cancel messages to a subset of the environments.
        // 3. Last 19 bits are random.
        let env_lower = org_id_conn_bits(&environment_id.organization_id());
        Client {
            build_info,
            inner_cmd_tx: cmd_tx,
            id_alloc: IdAllocator::new(1, MAX_ORG_ID, env_lower),
            now,
            metrics,
            environment_id,
            segment_client,
        }
    }

    /// Allocates a client for an incoming connection.
    pub fn new_conn_id(&self) -> Result<ConnectionId, AdapterError> {
        self.id_alloc.alloc().ok_or(AdapterError::IdExhaustionError)
    }

    /// Creates a new session associated with this client for the given user.
    ///
    /// It is the caller's responsibility to have authenticated the user.
    pub fn new_session(&self, config: SessionConfig) -> Session {
        // We use the system clock to determine when a session connected to Materialize. This is not
        // intended to be 100% accurate and correct, so we don't burden the timestamp oracle with
        // generating a more correct timestamp.
        Session::new(self.build_info, config)
    }

    /// Upgrades this client to a session client.
    ///
    /// A session is a connection that has successfully negotiated parameters,
    /// like the user. Most coordinator operations are available only after
    /// upgrading a connection to a session.
    ///
    /// Returns a new client that is bound to the session and a response
    /// containing various details about the startup.
    #[mz_ore::instrument(level = "debug")]
    pub async fn startup(&self, session: Session) -> Result<SessionClient, AdapterError> {
        let user = session.user().clone();
        let conn_id = session.conn_id().clone();
        let secret_key = session.secret_key();
        let uuid = session.uuid();
        let application_name = session.application_name().into();
        let notice_tx = session.retain_notice_transmitter();

        let (tx, rx) = oneshot::channel();

        // ~~SPOOKY ZONE~~
        //
        // This guard prevents a race where the startup command finishes, but the Future returned
        // by this function is concurrently dropped, so we never create a `SessionClient` and thus
        // never cleanup the initialized Session.
        let rx = rx.with_guard(|_| {
            self.send(Command::Terminate {
                conn_id: conn_id.clone(),
                tx: None,
            });
        });

        self.send(Command::Startup {
            tx,
            user,
            conn_id: conn_id.clone(),
            secret_key,
            uuid,
            application_name,
            notice_tx,
        });

        // When startup fails, no need to call terminate (handle_startup does this). Delay creating
        // the client until after startup to sidestep the panic in its `Drop` implementation.
        let response = rx.await.expect("sender dropped")?;

        // Create the client as soon as startup succeeds (before any await points) so its `Drop` can
        // handle termination.
        let mut client = SessionClient {
            inner: Some(self.clone()),
            session: Some(session),
            timeouts: Timeout::new(),
            environment_id: self.environment_id.clone(),
            segment_client: self.segment_client.clone(),
            catalog_snapshot: None,
            timestamp_oracles: BTreeMap::new(),
            peek_clients: BTreeMap::new(),
            frontiers: FrontiersClient::new(self.clone()).await,
        };

        let StartupResponse {
            role_id,
            write_notify,
            session_defaults,
            catalog,
        } = response;

        // Before we do ANYTHING, we need to wait for our BuiltinTable writes to complete. We wait
        // for the writes here, as opposed to during the Startup command, because we don't want to
        // block the coordinator on a Builtin Table write.
        write_notify.await;

        let session = client.session();
        session.initialize_role_metadata(role_id);
        let vars_mut = session.vars_mut();
        for (name, val) in session_defaults {
            if let Err(err) = vars_mut.set_default(&name, val.borrow()) {
                // Note: erroring here is unexpected, but we don't want to panic if somehow our
                // assumptions are wrong.
                tracing::error!("failed to set peristed default, {err:?}");
            }
        }
        session
            .vars_mut()
            .end_transaction(EndTransactionAction::Commit);

        let catalog = catalog.for_session(session);
        if catalog.active_database().is_none() {
            let db = session.vars().database().into();
            session.add_notice(AdapterNotice::UnknownSessionDatabase(db));
        }

        let cluster_active = session.vars().cluster().to_string();
        if session.vars().welcome_message() {
            let cluster_info = if catalog.resolve_cluster(Some(&cluster_active)).is_err() {
                format!("{cluster_active} (does not exist)")
            } else {
                cluster_active.to_string()
            };

            // Emit a welcome message, optimized for readability by humans using
            // interactive tools. If you change the message, make sure that it
            // formats nicely in both `psql` and the console's SQL shell.
            session.add_notice(AdapterNotice::Welcome(format!(
                "connected to Materialize v{}
  Org ID: {}
  Region: {}
  User: {}
  Cluster: {}
  Database: {}
  {}

Issue a SQL query to get started. Need help?
  View documentation: https://materialize.com/s/docs
  Join our Slack community: https://materialize.com/s/chat
    ",
                session.vars().build_info().semver_version(),
                self.environment_id.organization_id(),
                self.environment_id.region(),
                session.vars().user().name,
                cluster_info,
                session.vars().database(),
                match session.vars().search_path() {
                    [schema] => format!("Schema: {}", schema),
                    schemas => format!(
                        "Search path: {}",
                        schemas.iter().map(|id| id.to_string()).join(", ")
                    ),
                },
            )));
        }

        // Users stub their toe on their default cluster not existing, so we provide a notice to
        // help guide them on what do to.
        let cluster_var = session
            .vars()
            .inspect(CLUSTER.name())
            .expect("cluster should exist");
        if catalog.resolve_cluster(Some(&cluster_active)).is_err() {
            let cluster_notice = 'notice: {
                // If the user provided a cluster via a connection configuration parameter, do not
                // notify them if that cluster does not exist. We omit the notice here because even
                // if they were to update the role or system default, it would not make a
                // difference since the connection parameter takes precedence over the defaults.
                if cluster_var.inspect_session_value().is_some() {
                    break 'notice None;
                }

                let role_default = catalog.get_role(catalog.active_role_id());
                let role_cluster = match role_default.vars().get(CLUSTER.name()) {
                    Some(OwnedVarInput::Flat(name)) => Some(name),
                    None => None,
                    // This is unexpected!
                    Some(v @ OwnedVarInput::SqlSet(_)) => {
                        tracing::warn!(?v, "SqlSet found for cluster Role Default");
                        break 'notice None;
                    }
                };

                let alter_role = "with `ALTER ROLE <role> SET cluster TO <cluster>;`";
                match role_cluster {
                    // If there is no default, suggest a Role default.
                    None => Some(AdapterNotice::DefaultClusterDoesNotExist {
                        name: cluster_active,
                        kind: None,
                        suggested_action: format!(
                            "Set a default cluster for the current role {alter_role}."
                        ),
                    }),
                    // If the default does not exist, suggest to change it.
                    Some(_) => Some(AdapterNotice::DefaultClusterDoesNotExist {
                        name: cluster_active,
                        kind: Some("role"),
                        suggested_action: format!(
                            "Change the default cluster for the current role {alter_role}."
                        ),
                    }),
                }
            };

            if let Some(notice) = cluster_notice {
                session.add_notice(notice);
            }
        }

        Ok(client)
    }

    /// Cancels the query currently running on the specified connection.
    pub fn cancel_request(&mut self, conn_id: ConnectionIdType, secret_key: u32) {
        self.send(Command::CancelRequest {
            conn_id,
            secret_key,
        });
    }

    /// Executes a single SQL statement that returns rows as the
    /// `mz_support` user.
    pub async fn introspection_execute_one(&self, sql: &str) -> Result<Vec<Row>, anyhow::Error> {
        // Connect to the coordinator.
        let conn_id = self.new_conn_id()?;
        let session = self.new_session(SessionConfig {
            conn_id,
            user: SUPPORT_USER.name.clone(),
            external_metadata_rx: None,
        });
        let mut session_client = self.startup(session).await?;

        // Parse the SQL statement.
        let stmts = mz_sql::parse::parse(sql)?;
        if stmts.len() != 1 {
            bail!("must supply exactly one query");
        }
        let StatementParseResult { ast: stmt, sql } = stmts.into_element();

        const EMPTY_PORTAL: &str = "";
        session_client.start_transaction(Some(1))?;
        session_client
            .declare(EMPTY_PORTAL.into(), stmt, sql.to_string())
            .await?;
        match session_client
            .execute(EMPTY_PORTAL.into(), futures::future::pending(), None)
            .await?
        {
            (ExecuteResponse::SendingRows { future }, _) => match future.await {
                PeekResponseUnary::Rows(rows) => Ok(rows),
                PeekResponseUnary::Canceled => bail!("query canceled"),
                PeekResponseUnary::Error(e) => bail!(e),
            },
            r => bail!("unsupported response type: {r:?}"),
        }
    }

    /// Returns the metrics associated with the adapter layer.
    pub fn metrics(&self) -> &Metrics {
        &self.metrics
    }

    /// The current time according to the [`Client`].
    pub fn now(&self) -> DateTime<Utc> {
        to_datetime((self.now)())
    }

    /// Get a metadata and a channel that can be used to append to a webhook source.
    pub async fn get_webhook_appender(
        &self,
        database: String,
        schema: String,
        name: String,
    ) -> Result<AppendWebhookResponse, AppendWebhookError> {
        let (tx, rx) = oneshot::channel();

        // Send our request.
        self.send(Command::GetWebhook {
            database,
            schema,
            name,
            tx,
        });

        // Using our one shot channel to get the result, returning an error if the sender dropped.
        let response = rx
            .await
            .map_err(|_| anyhow::anyhow!("failed to receive webhook response"))?;

        response
    }

    #[instrument(level = "debug")]
    fn send(&self, cmd: Command) {
        // println!("sending: {:?}", cmd);
        self.inner_cmd_tx
            .send((OpenTelemetryContext::obtain(), cmd))
            .expect("coordinator unexpectedly gone");
    }
}

/// A coordinator client that is bound to a connection.
///
/// See also [`Client`].
pub struct SessionClient {
    // Invariant: inner may only be `None` after the session has been terminated.
    // Once the session is terminated, no communication to the Coordinator
    // should be attempted.
    inner: Option<Client>,
    // Invariant: session may only be `None` during a method call. Every public
    // method must ensure that `Session` is `Some` before it returns.
    session: Option<Session>,
    timeouts: Timeout,
    segment_client: Option<mz_segment::Client>,
    environment_id: EnvironmentId,
    catalog_snapshot: Option<Arc<Catalog>>,
    timestamp_oracles: BTreeMap<Timeline, Arc<dyn TimestampOracle<Timestamp> + Send + Sync>>,
    peek_clients: BTreeMap<ComputeInstanceId, PeekClient<Timestamp>>,
    frontiers: FrontiersClient<Timestamp>,
}

pub struct FrontiersClient<T> {
    session_client: Client,
    frontiers: Arc<Mutex<Frontiers<T>>>,
}

struct Frontiers<T> {
    storage: BTreeMap<GlobalId, CollectionState<T>>,
    compute: BTreeMap<GlobalId, CollectionState<T>>,
}

impl<T> Frontiers<T> {
    fn new() -> Self {
        Self {
            storage: BTreeMap::new(),
            compute: BTreeMap::new(),
        }
    }
}

#[derive(Debug)]
pub struct CollectionState<T> {
    pub read_capabilities: MutableAntichain<T>,
    pub implied_capability: Antichain<T>,
    pub write_frontier: Antichain<T>,
}

impl<T> FrontiersClient<T>
where
    T: TimelyTimestamp,
{
    async fn new(session_client: Client) -> Self {
        Self {
            session_client,
            frontiers: Arc::new(Mutex::new(Frontiers::new())),
        }
    }

    pub async fn storage_read_capability(&self, _id: GlobalId) -> Antichain<T> {
        Antichain::from_elem(T::minimum())
    }

    pub async fn storage_write_frontier(&self, _id: GlobalId) -> Antichain<T> {
        Antichain::from_elem(T::minimum())
    }

    pub async fn compute_read_capability(
        &self,
        _instance: ComputeInstanceId,
        _id: GlobalId,
    ) -> Antichain<T> {
        Antichain::from_elem(T::minimum())
    }

    pub async fn compute_write_frontier(
        &self,
        _instance: ComputeInstanceId,
        _id: GlobalId,
    ) -> Antichain<T> {
        Antichain::from_elem(T::minimum())
    }
}

impl SessionClient {
    /// Parses a SQL expression, reporting failures as a telemetry event if
    /// possible.
    pub fn parse<'a>(
        &self,
        sql: &'a str,
    ) -> Result<Result<Vec<StatementParseResult<'a>>, ParserStatementError>, String> {
        match mz_sql::parse::parse_with_limit(sql) {
            Ok(Err(e)) => {
                self.track_statement_parse_failure(&e);
                Ok(Err(e))
            }
            r => r,
        }
    }

    fn track_statement_parse_failure(&self, parse_error: &ParserStatementError) {
        let session = self.session.as_ref().expect("session invariant violated");
        let Some(user_id) = session.user().external_metadata.as_ref().map(|m| m.user_id) else {
            return;
        };
        let Some(segment_client) = &self.segment_client else {
            return;
        };
        let Some(statement_kind) = parse_error.statement else {
            return;
        };
        let Some((action, object_type)) = telemetry::analyze_audited_statement(statement_kind)
        else {
            return;
        };
        let event_type = StatementFailureType::ParseFailure;
        let event_name = format!(
            "{} {} {}",
            object_type.as_title_case(),
            action.as_title_case(),
            event_type.as_title_case(),
        );
        segment_client.environment_track(
            &self.environment_id,
            session.application_name(),
            user_id,
            event_name,
            json!({
                "statement_kind": statement_kind,
                "error": &parse_error.error,
            }),
        );
    }

    // Verify and return the named prepared statement. We need to verify each use
    // to make sure the prepared statement is still safe to use.
    pub async fn get_prepared_statement(
        &mut self,
        name: &str,
    ) -> Result<&PreparedStatement, AdapterError> {
        let catalog = self.catalog_snapshot().await;
        Coordinator::verify_prepared_statement(&catalog, self.session(), name)?;
        Ok(self
            .session()
            .get_prepared_statement_unverified(name)
            .expect("must exist"))
    }

    /// Saves the parsed statement as a prepared statement.
    ///
    /// The prepared statement is saved in the connection's [`crate::session::Session`]
    /// under the specified name.
    pub async fn prepare(
        &mut self,
        name: String,
        stmt: Option<Statement<Raw>>,
        sql: String,
        param_types: Vec<Option<ScalarType>>,
    ) -> Result<(), AdapterError> {
        let catalog = self.catalog_snapshot().await;

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

        let desc = Coordinator::describe(&catalog, self.session(), stmt.clone(), param_types)?;
        let now = self.now();
        self.session().set_prepared_statement(
            name,
            stmt,
            sql,
            desc,
            catalog.transient_revision(),
            now,
        );
        Ok(())
    }

    /// Binds a statement to a portal.
    #[mz_ore::instrument(level = "debug")]
    pub async fn declare(
        &mut self,
        name: String,
        stmt: Statement<Raw>,
        sql: String,
    ) -> Result<(), AdapterError> {
        let catalog = self.catalog_snapshot().await;
        let param_types = vec![];
        let desc =
            Coordinator::describe(&catalog, self.session(), Some(stmt.clone()), param_types)?;
        let params = vec![];
        let result_formats = vec![mz_pgwire_common::Format::Text; desc.arity()];
        let now = self.now();
        let logging = self.session().mint_logging(sql, Some(&stmt), now);
        self.session().set_portal(
            name,
            desc,
            Some(stmt),
            logging,
            params,
            result_formats,
            catalog.transient_revision(),
        )?;
        Ok(())
    }

    /// Executes a previously-bound portal.
    ///
    /// Note: the provided `cancel_future` must be cancel-safe as it's polled in a `select!` loop.
    #[mz_ore::instrument(level = "debug")]
    pub async fn execute(
        &mut self,
        portal_name: String,
        cancel_future: impl Future<Output = std::io::Error> + Send,
        outer_ctx_extra: Option<ExecuteContextExtra>,
    ) -> Result<(ExecuteResponse, Instant), AdapterError> {
        let execute_started = Instant::now();

        let session = self.session.as_ref().expect("session invariant violated");
        let stmt = session
            .get_portal_unverified(&portal_name)
            .expect("known to exist")
            .stmt
            .as_ref();

        match stmt {
            Some(stmt) if matches!(stmt.as_ref(), Statement::Select(_)) => {
                let mut session = self.session.take().expect("session invariant violated");
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
                let catalog = self.catalog_snapshot().await;
                let response = self
                    .handle_select(&catalog, portal_name, &mut session, outer_ctx_extra)
                    .await?;
                self.session.replace(session);
                Ok((response, execute_started))
            }
            _ => {
                let response = self
                    .send_with_cancel(
                        |tx, session| Command::Execute {
                            portal_name,
                            session,
                            tx,
                            outer_ctx_extra,
                        },
                        cancel_future,
                    )
                    .await?;
                Ok((response, execute_started))
            }
        }
    }

    /// Executes a SELECT statement
    #[tracing::instrument(level = "debug", skip_all)]
    pub(crate) async fn handle_select(
        &mut self,
        catalog: &Arc<Catalog>,
        portal_name: String,
        mut session: &mut Session,
        // If this command was part of another execute command
        // (for example, executing a `FETCH` statement causes an execute to be
        //  issued for the cursor it references),
        // then `outer_context` should be `Some`.
        // This instructs the coordinator that the
        // outer execute should be considered finished once the inner one is.
        _outer_context: Option<ExecuteContextExtra>,
    ) -> Result<ExecuteResponse, AdapterError> {
        // println!("handling select locally");
        let session_catalog = catalog.for_session(&session);
        let portal = session
            .get_portal_unverified(&portal_name)
            .expect("known to exist");

        let stmt = portal.stmt.as_ref().map(|stmt| stmt.as_ref());

        let original_stmt = match stmt {
            Some(Statement::Select(select)) => Statement::Select(select.clone()),
            _ => {
                unreachable!();
            }
        };
        let (stmt, resolved_ids) = match mz_sql::names::resolve(&session_catalog, original_stmt) {
            Ok(resolved) => resolved,
            Err(e) => panic!("error: {:?}", e),
        };

        let params = portal.parameters.clone();

        // let planned_stmt = self.plan_statement(&session, stmt, &params, &resolved_ids);
        let pcx = session.pcx();
        let planned_stmt =
            mz_sql::plan::plan(Some(pcx), &session_catalog, stmt, &params, &resolved_ids)?;

        // If our query only depends on system tables, a LaunchDarkly flag is enabled, and a
        // session var is set, then we automatically run the query on the mz_introspection cluster.
        let target_cluster =
            introspection::auto_run_on_introspection(&session_catalog, &session, &planned_stmt);

        let select_plan = match planned_stmt {
            mz_sql::plan::Plan::Select(select_plan) => select_plan,
            _ => unreachable!(),
        };

        let peek_stage_validate = PeekStageValidate {
            plan: select_plan,
            target_cluster,
            copy_to_ctx: None,
            explain_ctx: ExplainContext::None,
        };

        let optimizer_metrics = OptimizerMetrics::register_into(&MetricsRegistry::new());

        let peek_stage_linearize_timestamp = peek_exec::peek_stage_validate(
            &*self,
            &session,
            catalog,
            optimizer_metrics,
            peek_stage_validate,
        )?;

        let peek_stage_real_time_recency = peek_exec::peek_stage_linearize_timestamp(
            &mut *self,
            &session,
            peek_stage_linearize_timestamp,
        )
        .await;

        let peek_stage_timestamps_read_holds =
            peek_exec::peek_stage_real_time_recency(&mut *self, peek_stage_real_time_recency);

        let peek_stage_optimize = peek_exec::peek_stage_timestamp_read_hold(
            &*self,
            &mut session,
            catalog,
            peek_stage_timestamps_read_holds,
        )
        .await?;

        let peek_stage_finish =
            peek_exec::peek_stage_optimize(&*self, session.meta(), peek_stage_optimize).await?;

        let mut peek_stage_finish = match peek_stage_finish {
            PeekStage::Finish(f) => f,
            stage => {
                panic!("unexpected state: {:?}", stage);
            }
        };

        if let Err(err) = peek_stage_finish.validity.check(&catalog) {
            panic!("peek validation error: {}", err);
        }

        let response =
            peek_exec::peek_stage_finish(self, session, &catalog, peek_stage_finish).await?;

        Ok(response)
    }

    fn now(&self) -> EpochMillis {
        (self.inner().now)()
    }

    fn now_datetime(&self) -> DateTime<Utc> {
        to_datetime(self.now())
    }

    /// Starts a transaction based on implicit:
    /// - `None`: InTransaction
    /// - `Some(1)`: Started
    /// - `Some(n > 1)`: InTransactionImplicit
    /// - `Some(0)`: no change
    pub fn start_transaction(&mut self, implicit: Option<usize>) -> Result<(), AdapterError> {
        let now = self.now_datetime();
        let session = self.session.as_mut().expect("session invariant violated");
        let result = match implicit {
            None => session.start_transaction(now, None, None),
            Some(stmts) => {
                session.start_transaction_implicit(now, stmts);
                Ok(())
            }
        };
        result
    }

    /// Ends a transaction.
    #[instrument(level = "debug")]
    pub async fn end_transaction(
        &mut self,
        action: EndTransactionAction,
    ) -> Result<ExecuteResponse, AdapterError> {
        let should_send = match self.session().transaction().inner() {
            Some(txn) => match &txn.ops {
                crate::session::TransactionOps::None => {
                    // nothing to do,
                    false
                }
                crate::session::TransactionOps::Peeks { .. } => {
                    // Also not doing anything for now.
                    false
                }
                crate::session::TransactionOps::Subscribe => true,
                crate::session::TransactionOps::Writes(_) => true,
                crate::session::TransactionOps::SingleStatement { .. } => true,
                crate::session::TransactionOps::DDL { .. } => true,
            },
            None => false,
        };

        if should_send {
            self.send(|tx, session| Command::Commit {
                action,
                session,
                tx,
            })
            .await
        } else {
            Ok(ExecuteResponse::EmptyQuery)
        }
    }

    /// Fails a transaction.
    pub fn fail_transaction(&mut self) {
        let session = self.session.take().expect("session invariant violated");
        let session = session.fail_transaction();
        self.session = Some(session);
    }

    /// Fetches the catalog.
    #[instrument(level = "debug")]
    pub async fn catalog_snapshot(&mut self) -> Arc<Catalog> {
        if let Some(catalog) = &self.catalog_snapshot {
            return Arc::clone(catalog);
        }
        let CatalogSnapshot { catalog } = self
            .send_without_session(|tx| Command::CatalogSnapshot { tx })
            .await;

        self.catalog_snapshot.replace(Arc::clone(&catalog));

        catalog
    }

    /// Fetches the shared `TimestampOracle` for the given timeline.
    #[mz_ore::instrument(level = "debug")]
    pub async fn get_timestamp_oracle(
        &mut self,
        timeline: &Timeline,
    ) -> Arc<dyn TimestampOracle<Timestamp> + Send + Sync> {
        let oracle = self.timestamp_oracles.get_mut(timeline);
        if let Some(oracle) = oracle {
            return Arc::clone(oracle);
        }

        let oracle = self
            .send_without_session(|tx| Command::TimestampOracle {
                timeline: timeline.clone(),
                tx,
            })
            .await;
        self.timestamp_oracles.insert(timeline.clone(), oracle);

        let oracle = self
            .timestamp_oracles
            .get_mut(timeline)
            .expect("known to exist");

        Arc::clone(oracle)
    }

    /// Fetches a `PeekClient` for the given compute instance/cluster.
    pub async fn get_peek_client(
        &mut self,
        instance_id: ComputeInstanceId,
    ) -> PeekClient<Timestamp> {
        let peek_client = self.peek_clients.get_mut(&instance_id);
        if let Some(peek_client) = peek_client {
            return peek_client.clone();
        }

        let peek_client = self
            .send_without_session(|tx| Command::GetPeekClient {
                instance_id: instance_id.clone(),
                tx,
            })
            .await;
        self.peek_clients.insert(instance_id, peek_client);

        self.peek_clients
            .get_mut(&instance_id)
            .expect("known to exist")
            .clone()
    }

    /// The smallest common valid read frontier among the specified collections.
    async fn least_valid_read(
        &self,
        id_bundle: &CollectionIdBundle,
    ) -> Antichain<mz_repr::Timestamp> {
        let mut since = Antichain::from_elem(Timestamp::minimum());
        {
            for id in id_bundle.storage_ids.iter() {
                since.join_assign(&self.frontiers.storage_read_capability(*id).await)
            }
        }
        {
            for (instance, compute_ids) in &id_bundle.compute_ids {
                for id in compute_ids.iter() {
                    since.join_assign(&self.frontiers.compute_read_capability(*instance, *id).await)
                }
            }
        }
        since
    }

    /// The smallest common valid write frontier among the specified collections.
    ///
    /// Times that are not greater or equal to this frontier are complete for all collections
    /// identified as arguments.
    async fn least_valid_write(
        &self,
        id_bundle: &CollectionIdBundle,
    ) -> Antichain<mz_repr::Timestamp> {
        let mut since = Antichain::new();
        {
            for id in id_bundle.storage_ids.iter() {
                since.extend(self.frontiers.storage_write_frontier(*id).await.into_iter());
            }
        }
        {
            for (instance, compute_ids) in &id_bundle.compute_ids {
                for id in compute_ids.iter() {
                    since.extend(
                        self.frontiers
                            .compute_write_frontier(*instance, *id)
                            .await
                            .into_iter(),
                    );
                }
            }
        }
        since
    }

    /// Dumps the catalog to a JSON string.
    ///
    /// No authorization is performed, so access to this function must be limited to internal
    /// servers or superusers.
    pub async fn dump_catalog(&mut self) -> Result<CatalogDump, AdapterError> {
        let catalog = self.catalog_snapshot().await;
        catalog.dump().map_err(AdapterError::from)
    }

    /// Checks the catalog for internal consistency, returning a JSON object describing the
    /// inconsistencies, if there are any.
    ///
    /// No authorization is performed, so access to this function must be limited to internal
    /// servers or superusers.
    pub async fn check_catalog(&mut self) -> Result<(), serde_json::Value> {
        let catalog = self.catalog_snapshot().await;
        catalog.check_consistency()
    }

    /// Checks the coordinator for internal consistency, returning a JSON object describing the
    /// inconsistencies, if there are any. This is a superset of checks that check_catalog performs,
    ///
    /// No authorization is performed, so access to this function must be limited to internal
    /// servers or superusers.
    pub async fn check_coordinator(&mut self) -> Result<(), serde_json::Value> {
        self.send_without_session(|tx| Command::CheckConsistency { tx })
            .await
            .map_err(|inconsistencies| {
                serde_json::to_value(inconsistencies).unwrap_or_else(|_| {
                    serde_json::Value::String("failed to serialize inconsistencies".to_string())
                })
            })
    }

    pub async fn dump_coordinator_state(&mut self) -> Result<serde_json::Value, anyhow::Error> {
        self.send_without_session(|tx| Command::Dump { tx }).await
    }

    /// Tells the coordinator a statement has finished execution, in the cases
    /// where we have no other reason to communicate with the coordinator.
    pub fn retire_execute(
        &mut self,
        data: ExecuteContextExtra,
        reason: StatementEndedExecutionReason,
    ) {
        if !data.is_trivial() {
            let cmd = Command::RetireExecute { data, reason };
            self.inner().send(cmd);
        }
    }

    /// Inserts a set of rows into the given table.
    ///
    /// The rows only contain the columns positions in `columns`, so they
    /// must be re-encoded for adding the default values for the remaining
    /// ones.
    pub async fn insert_rows(
        &mut self,
        id: GlobalId,
        columns: Vec<usize>,
        rows: Vec<Row>,
        ctx_extra: ExecuteContextExtra,
    ) -> Result<ExecuteResponse, AdapterError> {
        // TODO: Remove this clone once we always have the session. It's currently needed because
        // self.session returns a mut ref, so we can't call it twice.
        let pcx = self.session().pcx().clone();

        let catalog = self.catalog_snapshot().await;
        let conn_catalog = catalog.for_session(self.session());

        // Collect optimizer parameters.
        let optimizer_config = optimize::OptimizerConfig::from(conn_catalog.system_vars());
        // Build an optimizer for this VIEW.
        let mut optimizer = optimize::view::Optimizer::new(optimizer_config, None);

        let result: Result<_, AdapterError> =
            mz_sql::plan::plan_copy_from(&pcx, &conn_catalog, id, columns, rows)
                .err_into()
                .and_then(|values| optimizer.optimize(values).err_into())
                .and_then(|values| {
                    // Copied rows must always be constants.
                    Coordinator::insert_constant(&catalog, self.session(), id, values.into_inner())
                });
        self.retire_execute(ctx_extra, (&result).into());
        result
    }

    /// Gets the current value of all system variables.
    pub async fn get_system_vars(&mut self) -> Result<GetVariablesResponse, AdapterError> {
        let conn_id = self.session().conn_id().clone();
        self.send_without_session(|tx| Command::GetSystemVars { conn_id, tx })
            .await
    }

    /// Updates the specified system variables to the specified values.
    pub async fn set_system_vars(
        &mut self,
        vars: BTreeMap<String, String>,
    ) -> Result<(), AdapterError> {
        let conn_id = self.session().conn_id().clone();
        self.send_without_session(|tx| Command::SetSystemVars { vars, conn_id, tx })
            .await
    }

    /// Terminates the client session.
    pub async fn terminate(&mut self) {
        let conn_id = self.session().conn_id().clone();
        let res = self
            .send_without_session(|tx| Command::Terminate {
                conn_id,
                tx: Some(tx),
            })
            .await;
        if let Err(e) = res {
            // Nothing we can do to handle a failed terminate so we just log and ignore it.
            error!("Unable to terminate session: {e:?}");
        }
        // Prevent any communication with Coordinator after session is terminated.
        self.inner = None;
    }

    /// Returns a mutable reference to the session bound to this client.
    pub fn session(&mut self) -> &mut Session {
        self.session.as_mut().expect("session invariant violated")
    }

    /// Returns a reference to the inner client.
    pub fn inner(&self) -> &Client {
        self.inner.as_ref().expect("inner invariant violated")
    }

    async fn send_without_session<T, F>(&self, f: F) -> T
    where
        F: FnOnce(oneshot::Sender<T>) -> Command,
    {
        let (tx, rx) = oneshot::channel();
        self.inner().send(f(tx));
        rx.await.expect("sender dropped")
    }

    #[instrument(level = "debug")]
    async fn send<T, F>(&mut self, f: F) -> Result<T, AdapterError>
    where
        F: FnOnce(oneshot::Sender<Response<T>>, Session) -> Command,
    {
        self.send_with_cancel(f, futures::future::pending()).await
    }

    /// Send a [`Command`] to the Coordinator, with the ability to cancel the command.
    ///
    /// Note: the provided `cancel_future` must be cancel-safe as it's polled in a `select!` loop.
    #[instrument(level = "debug")]
    async fn send_with_cancel<T, F>(
        &mut self,
        f: F,
        cancel_future: impl Future<Output = std::io::Error> + Send,
    ) -> Result<T, AdapterError>
    where
        F: FnOnce(oneshot::Sender<Response<T>>, Session) -> Command,
    {
        let session = self.session.take().expect("session invariant violated");
        let mut typ = None;
        let application_name = session.application_name();
        let name_hint = ApplicationNameHint::from_str(application_name);
        let conn_id = session.conn_id().clone();
        let (tx, rx) = oneshot::channel();

        // Destructure self so we can hold a mutable reference to the inner client and session at
        // the same time.
        let Self {
            inner: inner_client,
            session: client_session,
            ..
        } = self;

        // TODO(parkmycar): Leaking this invariant here doesn't feel great, but calling
        // `self.client()` doesn't work because then Rust takes a borrow on the entirity of self.
        let inner_client = inner_client.as_ref().expect("inner invariant violated");

        // ~~SPOOKY ZONE~~
        //
        // This guard prevents a race where a `Session` is returned on `rx` but never placed
        // back in `self` because the Future returned by this function is concurrently dropped
        // with the Coordinator sending a response.
        let mut guarded_rx = rx.with_guard(|response: Response<_>| {
            *client_session = Some(response.session);
        });

        inner_client.send({
            let cmd = f(tx, session);
            // Measure the success and error rate of certain commands:
            // - declare reports success of SQL statement planning
            // - execute reports success of dataflow execution
            match cmd {
                Command::Execute { .. } => typ = Some("execute"),
                Command::GetWebhook { .. } => typ = Some("webhook"),
                Command::Startup { .. }
                | Command::CatalogSnapshot { .. }
                | Command::TimestampOracle { .. }
                | Command::GetPeekClient { .. }
                | Command::Commit { .. }
                | Command::CancelRequest { .. }
                | Command::PrivilegedCancelRequest { .. }
                | Command::GetSystemVars { .. }
                | Command::SetSystemVars { .. }
                | Command::Terminate { .. }
                | Command::RetireExecute { .. }
                | Command::CheckConsistency { .. }
                | Command::Dump { .. } => {}
            };
            cmd
        });

        let mut cancel_future = pin::pin!(cancel_future);
        let mut cancelled = false;
        loop {
            tokio::select! {
                res = &mut guarded_rx => {
                    // We received a result, so drop our guard to drop our borrows.
                    drop(guarded_rx);

                    let res = res.expect("sender dropped");
                    let status = if res.result.is_ok() {
                        "success"
                    } else {
                        "error"
                    };
                    if let Some(typ) = typ {
                        inner_client
                            .metrics
                            .commands
                            .with_label_values(&[typ, status, name_hint.as_str()])
                            .inc();
                    }
                    *client_session = Some(res.session);
                    return res.result;
                },
                _err = &mut cancel_future, if !cancelled => {
                    cancelled = true;
                    inner_client.send(Command::PrivilegedCancelRequest {
                        conn_id: conn_id.clone(),
                    });
                }
            };
        }
    }

    pub fn add_idle_in_transaction_session_timeout(&mut self) {
        let session = self.session();
        let timeout_dur = session.vars().idle_in_transaction_session_timeout();
        if !timeout_dur.is_zero() {
            let timeout_dur = timeout_dur.clone();
            if let Some(txn) = session.transaction().inner() {
                let txn_id = txn.id.clone();
                let timeout = TimeoutType::IdleInTransactionSession(txn_id);
                self.timeouts.add_timeout(timeout, timeout_dur);
            }
        }
    }

    pub fn remove_idle_in_transaction_session_timeout(&mut self) {
        let session = self.session();
        if let Some(txn) = session.transaction().inner() {
            let txn_id = txn.id.clone();
            self.timeouts
                .remove_timeout(&TimeoutType::IdleInTransactionSession(txn_id));
        }
    }

    /// # Cancel safety
    ///
    /// This method is cancel safe. If `recv` is used as the event in a
    /// `tokio::select!` statement and some other branch
    /// completes first, it is guaranteed that no messages were received on this
    /// channel.
    pub async fn recv_timeout(&mut self) -> Option<TimeoutType> {
        self.timeouts.recv().await
    }
}

impl Drop for SessionClient {
    fn drop(&mut self) {
        // We may not have a session if this client was dropped while awaiting
        // a response. In this case, it is the coordinator's responsibility to
        // terminate the session.
        if let Some(session) = self.session.take() {
            // We may not have a connection to the Coordinator if the session was
            // prematurely terminated, for example due to a timeout.
            if let Some(inner) = &self.inner {
                inner.send(Command::Terminate {
                    conn_id: session.conn_id().clone(),
                    tx: None,
                })
            }
        }
    }
}

#[derive(Hash, PartialEq, Eq, PartialOrd, Ord, Clone, Debug)]
pub enum TimeoutType {
    IdleInTransactionSession(TransactionId),
}

impl Display for TimeoutType {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            TimeoutType::IdleInTransactionSession(txn_id) => {
                writeln!(f, "Idle in transaction session for transaction '{txn_id}'")
            }
        }
    }
}

impl From<TimeoutType> for AdapterError {
    fn from(timeout: TimeoutType) -> Self {
        match timeout {
            TimeoutType::IdleInTransactionSession(_) => {
                AdapterError::IdleInTransactionSessionTimeout
            }
        }
    }
}

struct Timeout {
    tx: mpsc::UnboundedSender<TimeoutType>,
    rx: mpsc::UnboundedReceiver<TimeoutType>,
    active_timeouts: BTreeMap<TimeoutType, AbortOnDropHandle<()>>,
}

impl Timeout {
    fn new() -> Self {
        let (tx, rx) = mpsc::unbounded_channel();
        Timeout {
            tx,
            rx,
            active_timeouts: BTreeMap::new(),
        }
    }

    /// # Cancel safety
    ///
    /// This method is cancel safe. If `recv` is used as the event in a
    /// `tokio::select!` statement and some other branch
    /// completes first, it is guaranteed that no messages were received on this
    /// channel.
    ///
    /// <https://docs.rs/tokio/latest/tokio/sync/mpsc/struct.UnboundedReceiver.html#cancel-safety>
    async fn recv(&mut self) -> Option<TimeoutType> {
        self.rx.recv().await
    }

    fn add_timeout(&mut self, timeout: TimeoutType, duration: Duration) {
        let tx = self.tx.clone();
        let timeout_key = timeout.clone();
        let handle = mz_ore::task::spawn(|| format!("{timeout_key}"), async move {
            tokio::time::sleep(duration).await;
            let _ = tx.send(timeout);
        })
        .abort_on_drop();
        self.active_timeouts.insert(timeout_key, handle);
    }

    fn remove_timeout(&mut self, timeout: &TimeoutType) {
        self.active_timeouts.remove(timeout);

        // Remove the timeout from the rx queue if it exists.
        let mut timeouts = Vec::new();
        while let Ok(pending_timeout) = self.rx.try_recv() {
            if timeout != &pending_timeout {
                timeouts.push(pending_timeout);
            }
        }
        for pending_timeout in timeouts {
            self.tx.send(pending_timeout).expect("rx is in this struct");
        }
    }
}

/// A wrapper around an UnboundedReceiver of PeekResponseUnary that records when it sees the
/// first row data in the given histogram
#[derive(Derivative)]
#[derivative(Debug)]
pub struct RecordFirstRowStream {
    #[derivative(Debug = "ignore")]
    pub rows: Box<dyn Stream<Item = PeekResponseUnary> + Unpin + Send + Sync>,
    pub execute_started: Instant,
    pub time_to_first_row_seconds: Histogram,
    saw_rows: bool,
}

impl RecordFirstRowStream {
    /// Create a new [`RecordFirstRowStream`]
    pub fn new(
        rows: Box<dyn Stream<Item = PeekResponseUnary> + Unpin + Send + Sync>,
        execute_started: Instant,
        client: &SessionClient,
    ) -> Self {
        let histogram = Self::histogram(client);
        Self {
            rows,
            execute_started,
            time_to_first_row_seconds: histogram,
            saw_rows: false,
        }
    }

    fn histogram(client: &SessionClient) -> Histogram {
        let isolation_level = *client
            .session
            .as_ref()
            .expect("session invariant")
            .vars()
            .transaction_isolation();

        client
            .inner()
            .metrics()
            .time_to_first_row_seconds
            .with_label_values(&[isolation_level.as_str()])
    }

    /// If you want to match [`RecordFirstRowStream`]'s logic but don't need
    /// a UnboundedReceiver, you can tell it when to record an observation.
    pub fn record(execute_started: Instant, client: &SessionClient) {
        Self::histogram(client).observe(execute_started.elapsed().as_secs_f64());
    }

    pub async fn recv(&mut self) -> Option<PeekResponseUnary> {
        let msg = self.rows.next().await;
        if !self.saw_rows && matches!(msg, Some(PeekResponseUnary::Rows(_))) {
            self.saw_rows = true;
            self.time_to_first_row_seconds
                .observe(self.execute_started.elapsed().as_secs_f64());
        }
        msg
    }
}

impl peek::PeekIds for &SessionClient {
    fn allocate_transient_id(&self) -> Result<GlobalId, AdapterError> {
        Ok(GlobalId::User(0))
    }
}

#[async_trait::async_trait]
impl peek::PeekTimestampOracle for &mut SessionClient {
    #[mz_ore::instrument(level = "debug")]
    async fn get_timestamp_oracle(
        &mut self,
        timeline: &Timeline,
    ) -> Arc<dyn TimestampOracle<Timestamp> + Send + Sync> {
        SessionClient::get_timestamp_oracle(self, timeline).await
    }
}

impl peek::PeekComputeInstances for &SessionClient {
    fn instance_snapshot(
        &self,
        id: mz_compute_types::ComputeInstanceId,
    ) -> Result<ComputeInstanceSnapshot, mz_compute_client::controller::error::InstanceMissing>
    {
        ComputeInstanceSnapshot::new_without_collections(id)
    }
}

#[async_trait::async_trait]
impl peek::PeekSubmitPeek<Timestamp> for &mut SessionClient {
    async fn peek(
        &mut self,
        instance_id: ComputeInstanceId,
        collection_id: GlobalId,
        literal_constraints: Option<Vec<Row>>,
        uuid: Uuid,
        timestamp: Timestamp,
        finishing: mz_expr::RowSetFinishing,
        map_filter_project: mz_expr::SafeMfpPlan,
        target_replica: Option<mz_cluster_client::ReplicaId>,
        peek_target: PeekTarget,
        result_tx: oneshot::Sender<mz_compute_client::protocol::response::PeekResponse>,
    ) -> Result<(), mz_compute_client::controller::error::PeekError> {
        let mut peek_client = self.get_peek_client(instance_id).await;
        let res = peek_client.peek(
            collection_id,
            literal_constraints,
            uuid,
            timestamp,
            finishing,
            map_filter_project,
            target_replica,
            peek_target,
            result_tx,
        );

        res
    }
}

#[async_trait::async_trait]
impl peek::PeekTimestamps for &SessionClient {
    async fn determine_timestamp(
        &self,
        catalog: &CatalogState,
        session: &Session,
        id_bundle: &CollectionIdBundle,
        when: &QueryWhen,
        compute_instance: ComputeInstanceId,
        timeline_context: &TimelineContext,
        oracle_read_ts: Option<Timestamp>,
        real_time_recency_ts: Option<mz_repr::Timestamp>,
        isolation_level: &IsolationLevel,
    ) -> Result<TimestampDetermination<mz_repr::Timestamp>, AdapterError> {
        // Each involved trace has a validity interval `[since, upper)`.
        // The contents of a trace are only guaranteed to be correct when
        // accumulated at a time greater or equal to `since`, and they
        // are only guaranteed to be currently present for times not
        // greater or equal to `upper`.
        //
        // The plan is to first determine a timestamp, based on the requested
        // timestamp policy, and then determine if it can be satisfied using
        // the compacted arrangements we have at hand. It remains unresolved
        // what to do if it cannot be satisfied (perhaps the query should use
        // a larger timestamp and block, perhaps the user should intervene).

        let since = self.least_valid_read(id_bundle).await;
        let upper = self.least_valid_write(id_bundle).await;

        let largest_not_in_advance_of_upper = Coordinator::largest_not_in_advance_of_upper(&upper);

        let timeline = Coordinator::get_timeline(timeline_context);

        {
            // TODO: We currently split out getting the oracle timestamp because
            // it's a potentially expensive call, but a call that can be done in an
            // async task. TimestampProvider is not Send (nor Sync), so we cannot do
            // the call to `determine_timestamp_for` (including the oracle call) on
            // an async task. If/when TimestampProvider can become Send, we can fold
            // the call to the TimestampOracle back into this function.
            //
            // We assert here that the logic that determines the oracle timestamp
            // matches our expectations.

            if timeline.is_some()
                && <Coordinator as TimestampProvider>::needs_linearized_read_ts(
                    isolation_level,
                    when,
                )
            {
                assert!(
                    oracle_read_ts.is_some(),
                    "should get a timestamp from the oracle for linearized timeline {:?} but didn't",
                    timeline);
            }
        }

        // Initialize candidate to the minimum correct time.
        let mut candidate = Timestamp::minimum();

        if let Some(timestamp) = when.advance_to_timestamp() {
            let ts = Coordinator::evaluate_when(catalog, timestamp, session)?;
            candidate.join_assign(&ts);
        }

        if when.advance_to_since() {
            candidate.advance_by(since.borrow());
        }

        // If we've acquired a read timestamp from the timestamp oracle, use it
        // as the new lower bound for the candidate.
        // In Strong Session Serializable, we ignore the oracle timestamp for now, unless we need
        // to use it.
        if let Some(timestamp) = &oracle_read_ts {
            if isolation_level != &IsolationLevel::StrongSessionSerializable
                || when.must_advance_to_timeline_ts()
            {
                candidate.join_assign(timestamp);
            }
        }

        // We advance to the upper in the following scenarios:
        // - The isolation level is Serializable and the `when` allows us to advance to upper (ex:
        //   queries with no AS OF). We avoid using the upper in Strict Serializable to prevent
        //   reading source data that is being written to in the future.
        // - The isolation level is Strict Serializable but there is no timelines and the `when`
        //   allows us to advance to upper.
        // - The `when` requires us to advance to the upper (ex: read-then-write queries).
        if when.can_advance_to_upper()
            || (when.can_advance_to_upper()
                && (isolation_level == &IsolationLevel::Serializable || timeline.is_none()))
        {
            candidate.join_assign(&largest_not_in_advance_of_upper);
        }

        if let Some(real_time_recency_ts) = real_time_recency_ts {
            assert!(
                session.vars().real_time_recency()
                    && isolation_level == &IsolationLevel::StrictSerializable,
                "real time recency timestamp should only be supplied when real time recency \
                            is enabled and the isolation level is strict serializable"
            );
            candidate.join_assign(&real_time_recency_ts);
        }

        let mut session_oracle_read_ts = None;
        if isolation_level == &IsolationLevel::StrongSessionSerializable {
            if let Some(timeline) = &timeline {
                if let Some(oracle) = session.get_timestamp_oracle(timeline) {
                    let session_ts = oracle.read_ts();
                    candidate.join_assign(&session_ts);
                    session_oracle_read_ts = Some(session_ts);
                }
            }

            // When advancing the read timestamp under Strong Session Serializable, there is a
            // trade-off to make between freshness and latency. We can choose a timestamp close the
            // `upper`, but then later queries might block if the `upper` is too far into the
            // future. We can chose a timestamp close to the current time, but then we may not be
            // getting results that are as fresh as possible. As a heuristic, we choose the minimum
            // of now and the upper, where we use the global timestamp oracle read timestamp as a
            // proxy for now. If upper > now, then we choose now and prevent blocking future
            // queries. If upper < now, then we choose the upper and prevent blocking the current
            // query.
            if when.can_advance_to_upper() && when.can_advance_to_timeline_ts() {
                let mut advance_to = largest_not_in_advance_of_upper;
                if let Some(oracle_read_ts) = oracle_read_ts {
                    advance_to = std::cmp::min(advance_to, oracle_read_ts);
                }
                candidate.join_assign(&advance_to);
            }
        }

        // If the timestamp is greater or equal to some element in `since` we are
        // assured that the answer will be correct.
        //
        // It's ok for this timestamp to be larger than the current timestamp of
        // the timestamp oracle. For Strict Serializable queries, the Coord will
        // linearize the query by holding back the result until the timestamp
        // oracle catches up.
        let timestamp = if since.less_equal(&candidate) {
            event!(
                Level::DEBUG,
                conn_id = format!("{}", session.conn_id()),
                since = format!("{since:?}"),
                largest_not_in_advance_of_upper = format!("{largest_not_in_advance_of_upper}"),
                timestamp = format!("{candidate}")
            );
            candidate
        } else {
            // TODO
            coord_bail!("timestamp not valid");
            // coord_bail!(self.generate_timestamp_not_valid_error_msg(
            //     id_bundle,
            //     compute_instance,
            //     candidate
            // ));
        };

        let timestamp_context = TimestampContext::from_timeline_context(
            timestamp,
            oracle_read_ts,
            timeline,
            timeline_context,
        );

        Ok(TimestampDetermination {
            timestamp_context,
            since,
            upper,
            largest_not_in_advance_of_upper,
            oracle_read_ts,
            session_oracle_read_ts: None,
        })
    }
}

// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::{BTreeMap, BTreeSet};
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use derivative::Derivative;
use enum_kinds::EnumKind;
use futures::future::BoxFuture;
use mz_adapter_types::connection::{ConnectionId, ConnectionIdType};
use mz_ore::collections::CollectionExt;
use mz_ore::soft_assert_no_log;
use mz_ore::tracing::OpenTelemetryContext;
use mz_pgcopy::CopyFormatParams;
use mz_repr::role_id::RoleId;
use mz_repr::{GlobalId, Row};
use mz_sql::ast::{FetchDirection, Raw, Statement};
use mz_sql::catalog::ObjectType;
use mz_sql::plan::{ExecuteTimeout, Plan, PlanKind};
use mz_sql::session::user::User;
use mz_sql::session::vars::{OwnedVarInput, Var};
use mz_sql_parser::ast::{AlterObjectRenameStatement, AlterOwnerStatement, DropObjectsStatement};
use tokio::sync::{mpsc, oneshot};
use uuid::Uuid;

use crate::catalog::Catalog;
use crate::coord::consistency::CoordinatorInconsistencies;
use crate::coord::peek::PeekResponseUnary;
use crate::coord::ExecuteContextExtra;
use crate::error::AdapterError;
use crate::session::{EndTransactionAction, RowBatchStream, Session};
use crate::statement_logging::StatementEndedExecutionReason;
use crate::util::Transmittable;
use crate::webhook::AppendWebhookResponse;
use crate::{AdapterNotice, AppendWebhookError};

#[derive(Debug)]
pub struct CatalogSnapshot {
    pub catalog: Arc<Catalog>,
}

#[derive(Debug)]
pub enum Command {
    CatalogSnapshot {
        tx: oneshot::Sender<CatalogSnapshot>,
    },

    Startup {
        tx: oneshot::Sender<Result<StartupResponse, AdapterError>>,
        user: User,
        conn_id: ConnectionId,
        secret_key: u32,
        uuid: Uuid,
        application_name: String,
        notice_tx: mpsc::UnboundedSender<AdapterNotice>,
    },

    Execute {
        portal_name: String,
        session: Session,
        tx: oneshot::Sender<Response<ExecuteResponse>>,
        outer_ctx_extra: Option<ExecuteContextExtra>,
    },

    Commit {
        action: EndTransactionAction,
        session: Session,
        tx: oneshot::Sender<Response<ExecuteResponse>>,
    },

    CancelRequest {
        conn_id: ConnectionIdType,
        secret_key: u32,
    },

    PrivilegedCancelRequest {
        conn_id: ConnectionId,
    },

    GetWebhook {
        database: String,
        schema: String,
        name: String,
        tx: oneshot::Sender<Result<AppendWebhookResponse, AppendWebhookError>>,
    },

    GetSystemVars {
        conn_id: ConnectionId,
        tx: oneshot::Sender<Result<GetVariablesResponse, AdapterError>>,
    },

    SetSystemVars {
        vars: BTreeMap<String, String>,
        conn_id: ConnectionId,
        tx: oneshot::Sender<Result<(), AdapterError>>,
    },

    Terminate {
        conn_id: ConnectionId,
        tx: Option<oneshot::Sender<Result<(), AdapterError>>>,
    },

    /// Performs any cleanup and logging actions necessary for
    /// finalizing a statement execution.
    ///
    /// Only used for cases that terminate in the protocol layer and
    /// otherwise have no reason to hand control back to the coordinator.
    /// In other cases, we piggy-back on another command.
    RetireExecute {
        data: ExecuteContextExtra,
        reason: StatementEndedExecutionReason,
    },

    CheckConsistency {
        tx: oneshot::Sender<Result<(), CoordinatorInconsistencies>>,
    },

    Dump {
        tx: oneshot::Sender<Result<serde_json::Value, anyhow::Error>>,
    },
}

impl Command {
    pub fn session(&self) -> Option<&Session> {
        match self {
            Command::Execute { session, .. } | Command::Commit { session, .. } => Some(session),
            Command::CancelRequest { .. }
            | Command::Startup { .. }
            | Command::CatalogSnapshot { .. }
            | Command::PrivilegedCancelRequest { .. }
            | Command::GetWebhook { .. }
            | Command::Terminate { .. }
            | Command::GetSystemVars { .. }
            | Command::SetSystemVars { .. }
            | Command::RetireExecute { .. }
            | Command::CheckConsistency { .. }
            | Command::Dump { .. } => None,
        }
    }

    pub fn session_mut(&mut self) -> Option<&mut Session> {
        match self {
            Command::Execute { session, .. } | Command::Commit { session, .. } => Some(session),
            Command::CancelRequest { .. }
            | Command::Startup { .. }
            | Command::CatalogSnapshot { .. }
            | Command::PrivilegedCancelRequest { .. }
            | Command::GetWebhook { .. }
            | Command::Terminate { .. }
            | Command::GetSystemVars { .. }
            | Command::SetSystemVars { .. }
            | Command::RetireExecute { .. }
            | Command::CheckConsistency { .. }
            | Command::Dump { .. } => None,
        }
    }
}

#[derive(Debug)]
pub struct Response<T> {
    pub result: Result<T, AdapterError>,
    pub session: Session,
    pub otel_ctx: OpenTelemetryContext,
}

pub type RowsFuture = Pin<Box<dyn Future<Output = PeekResponseUnary> + Send>>;

/// The response to [`Client::startup`](crate::Client::startup).
#[derive(Derivative)]
#[derivative(Debug)]
pub struct StartupResponse {
    /// RoleId for the user.
    pub role_id: RoleId,
    /// A future that completes when all necessary Builtin Table writes have completed.
    #[derivative(Debug = "ignore")]
    pub write_notify: BoxFuture<'static, ()>,
    /// Map of (name, VarInput::Flat) tuples of session default variables that should be set.
    pub session_defaults: BTreeMap<String, OwnedVarInput>,
    pub catalog: Arc<Catalog>,
}

// Facile implementation for `StartupResponse`, which does not use the `allowed`
// feature of `ClientTransmitter`.
impl Transmittable for StartupResponse {
    type Allowed = bool;
    fn to_allowed(&self) -> Self::Allowed {
        true
    }
}

/// The response to [`SessionClient::dump_catalog`](crate::SessionClient::dump_catalog).
#[derive(Debug, Clone)]
pub struct CatalogDump(String);

impl CatalogDump {
    pub fn new(raw: String) -> Self {
        CatalogDump(raw)
    }

    pub fn into_string(self) -> String {
        self.0
    }
}

impl Transmittable for CatalogDump {
    type Allowed = bool;
    fn to_allowed(&self) -> Self::Allowed {
        true
    }
}

/// The response to [`SessionClient::get_system_vars`](crate::SessionClient::get_system_vars).
#[derive(Debug, Clone)]
pub struct GetVariablesResponse(BTreeMap<String, String>);

impl GetVariablesResponse {
    pub fn new<'a>(vars: impl Iterator<Item = &'a dyn Var>) -> Self {
        GetVariablesResponse(
            vars.map(|var| (var.name().to_string(), var.value()))
                .collect(),
        )
    }

    pub fn get(&self, name: &str) -> Option<&str> {
        self.0.get(name).map(|s| s.as_str())
    }
}

impl Transmittable for GetVariablesResponse {
    type Allowed = bool;
    fn to_allowed(&self) -> Self::Allowed {
        true
    }
}

impl IntoIterator for GetVariablesResponse {
    type Item = (String, String);
    type IntoIter = std::collections::btree_map::IntoIter<String, String>;

    fn into_iter(self) -> Self::IntoIter {
        self.0.into_iter()
    }
}

/// The response to [`SessionClient::execute`](crate::SessionClient::execute).
#[derive(EnumKind, Derivative)]
#[derivative(Debug)]
#[enum_kind(ExecuteResponseKind, derive(PartialOrd, Ord))]
pub enum ExecuteResponse {
    /// The default privileges were altered.
    AlteredDefaultPrivileges,
    /// The requested object was altered.
    AlteredObject(ObjectType),
    /// The role was altered.
    AlteredRole,
    /// The system configuration was altered.
    AlteredSystemConfiguration,
    /// The requested cursor was closed.
    ClosedCursor,
    /// The provided comment was created.
    Comment,
    /// The specified number of rows were copied into the requested output.
    Copied(usize),
    /// The response for a COPY TO STDOUT query.
    CopyTo {
        format: mz_sql::plan::CopyFormat,
        resp: Box<ExecuteResponse>,
    },
    CopyFrom {
        id: GlobalId,
        columns: Vec<usize>,
        params: CopyFormatParams<'static>,
        ctx_extra: ExecuteContextExtra,
    },
    /// The requested connection was created.
    CreatedConnection,
    /// The requested database was created.
    CreatedDatabase,
    /// The requested schema was created.
    CreatedSchema,
    /// The requested role was created.
    CreatedRole,
    /// The requested cluster was created.
    CreatedCluster,
    /// The requested cluster replica was created.
    CreatedClusterReplica,
    /// The requested index was created.
    CreatedIndex,
    /// The requested secret was created.
    CreatedSecret,
    /// The requested sink was created.
    CreatedSink,
    /// The requested source was created.
    CreatedSource,
    /// The requested table was created.
    CreatedTable,
    /// The requested view was created.
    CreatedView,
    /// The requested views were created.
    CreatedViews,
    /// The requested materialized view was created.
    CreatedMaterializedView,
    /// The requested type was created.
    CreatedType,
    /// The requested prepared statement was removed.
    Deallocate { all: bool },
    /// The requested cursor was declared.
    DeclaredCursor,
    /// The specified number of rows were deleted from the requested table.
    Deleted(usize),
    /// The temporary objects associated with the session have been discarded.
    DiscardedTemp,
    /// All state associated with the session has been discarded.
    DiscardedAll,
    /// The requested object was dropped.
    DroppedObject(ObjectType),
    /// The requested objects were dropped.
    DroppedOwned,
    /// The provided query was empty.
    EmptyQuery,
    /// Fetch results from a cursor.
    Fetch {
        /// The name of the cursor from which to fetch results.
        name: String,
        /// The number of results to fetch.
        count: Option<FetchDirection>,
        /// How long to wait for results to arrive.
        timeout: ExecuteTimeout,
        ctx_extra: ExecuteContextExtra,
    },
    /// The requested privilege was granted.
    GrantedPrivilege,
    /// The requested role was granted.
    GrantedRole,
    /// The specified number of rows were inserted into the requested table.
    Inserted(usize),
    /// The specified prepared statement was created.
    Prepare,
    /// A user-requested warning was raised.
    Raised,
    /// The requested objects were reassigned.
    ReassignOwned,
    /// The requested privilege was revoked.
    RevokedPrivilege,
    /// The requested role was revoked.
    RevokedRole,
    /// Rows will be delivered via the specified future.
    SendingRows {
        #[derivative(Debug = "ignore")]
        future: RowsFuture,
    },
    /// Like `SendingRows`, but the rows are known to be available
    /// immediately, and thus the execution is considered ended in the coordinator.
    SendingRowsImmediate { rows: Vec<Row> },
    /// The specified variable was set to a new value.
    SetVariable {
        name: String,
        /// Whether the operation was a `RESET` rather than a set.
        reset: bool,
    },
    /// A new transaction was started.
    StartedTransaction,
    /// Updates to the requested source or view will be streamed to the
    /// contained receiver.
    Subscribing {
        rx: RowBatchStream,
        ctx_extra: ExecuteContextExtra,
    },
    /// The active transaction committed.
    TransactionCommitted {
        /// Session parameters that changed because the transaction ended.
        params: BTreeMap<&'static str, String>,
    },
    /// The active transaction rolled back.
    TransactionRolledBack {
        /// Session parameters that changed because the transaction ended.
        params: BTreeMap<&'static str, String>,
    },
    /// The specified number of rows were updated in the requested table.
    Updated(usize),
    /// A connection was validated.
    ValidatedConnection,
}

impl TryFrom<&Statement<Raw>> for ExecuteResponse {
    type Error = ();

    /// Returns Ok if this Statement always produces a single, trivial ExecuteResponse.
    fn try_from(stmt: &Statement<Raw>) -> Result<Self, Self::Error> {
        let resp_kinds = Plan::generated_from(&stmt.into())
            .iter()
            .map(ExecuteResponse::generated_from)
            .flatten()
            .cloned()
            .collect::<BTreeSet<ExecuteResponseKind>>();
        let resps = resp_kinds
            .iter()
            .map(|r| (*r).try_into())
            .collect::<Result<Vec<ExecuteResponse>, _>>();
        // Check if this statement's possible plans yield exactly one possible ExecuteResponse.
        if let Ok(resps) = resps {
            if resps.len() == 1 {
                return Ok(resps.into_element());
            }
        }
        let resp = match stmt {
            Statement::DropObjects(DropObjectsStatement { object_type, .. }) => {
                ExecuteResponse::DroppedObject((*object_type).into())
            }
            Statement::AlterObjectRename(AlterObjectRenameStatement { object_type, .. })
            | Statement::AlterOwner(AlterOwnerStatement { object_type, .. }) => {
                ExecuteResponse::AlteredObject((*object_type).into())
            }
            _ => return Err(()),
        };
        // Ensure that if the planner ever adds possible plans we complain here.
        soft_assert_no_log!(
            resp_kinds.len() == 1
                && resp_kinds.first().expect("must exist") == &ExecuteResponseKind::from(&resp),
            "ExecuteResponses out of sync with planner"
        );
        Ok(resp)
    }
}

impl TryInto<ExecuteResponse> for ExecuteResponseKind {
    type Error = ();

    /// Attempts to convert into an ExecuteResponse. Returns an error if not possible without
    /// actually executing a statement.
    fn try_into(self) -> Result<ExecuteResponse, Self::Error> {
        match self {
            ExecuteResponseKind::AlteredDefaultPrivileges => {
                Ok(ExecuteResponse::AlteredDefaultPrivileges)
            }
            ExecuteResponseKind::AlteredObject => Err(()),
            ExecuteResponseKind::AlteredRole => Ok(ExecuteResponse::AlteredRole),
            ExecuteResponseKind::AlteredSystemConfiguration => {
                Ok(ExecuteResponse::AlteredSystemConfiguration)
            }
            ExecuteResponseKind::ClosedCursor => Ok(ExecuteResponse::ClosedCursor),
            ExecuteResponseKind::Comment => Ok(ExecuteResponse::Comment),
            ExecuteResponseKind::Copied => Err(()),
            ExecuteResponseKind::CopyTo => Err(()),
            ExecuteResponseKind::CopyFrom => Err(()),
            ExecuteResponseKind::CreatedConnection => Ok(ExecuteResponse::CreatedConnection),
            ExecuteResponseKind::CreatedDatabase => Ok(ExecuteResponse::CreatedDatabase),
            ExecuteResponseKind::CreatedSchema => Ok(ExecuteResponse::CreatedSchema),
            ExecuteResponseKind::CreatedRole => Ok(ExecuteResponse::CreatedRole),
            ExecuteResponseKind::CreatedCluster => Ok(ExecuteResponse::CreatedCluster),
            ExecuteResponseKind::CreatedClusterReplica => {
                Ok(ExecuteResponse::CreatedClusterReplica)
            }
            ExecuteResponseKind::CreatedIndex => Ok(ExecuteResponse::CreatedIndex),
            ExecuteResponseKind::CreatedSecret => Ok(ExecuteResponse::CreatedSecret),
            ExecuteResponseKind::CreatedSink => Ok(ExecuteResponse::CreatedSink),
            ExecuteResponseKind::CreatedSource => Ok(ExecuteResponse::CreatedSource),
            ExecuteResponseKind::CreatedTable => Ok(ExecuteResponse::CreatedTable),
            ExecuteResponseKind::CreatedView => Ok(ExecuteResponse::CreatedView),
            ExecuteResponseKind::CreatedViews => Ok(ExecuteResponse::CreatedViews),
            ExecuteResponseKind::CreatedMaterializedView => {
                Ok(ExecuteResponse::CreatedMaterializedView)
            }
            ExecuteResponseKind::CreatedType => Ok(ExecuteResponse::CreatedType),
            ExecuteResponseKind::Deallocate => Err(()),
            ExecuteResponseKind::DeclaredCursor => Ok(ExecuteResponse::DeclaredCursor),
            ExecuteResponseKind::Deleted => Err(()),
            ExecuteResponseKind::DiscardedTemp => Ok(ExecuteResponse::DiscardedTemp),
            ExecuteResponseKind::DiscardedAll => Ok(ExecuteResponse::DiscardedAll),
            ExecuteResponseKind::DroppedObject => Err(()),
            ExecuteResponseKind::DroppedOwned => Ok(ExecuteResponse::DroppedOwned),
            ExecuteResponseKind::EmptyQuery => Ok(ExecuteResponse::EmptyQuery),
            ExecuteResponseKind::Fetch => Err(()),
            ExecuteResponseKind::GrantedPrivilege => Ok(ExecuteResponse::GrantedPrivilege),
            ExecuteResponseKind::GrantedRole => Ok(ExecuteResponse::GrantedRole),
            ExecuteResponseKind::Inserted => Err(()),
            ExecuteResponseKind::Prepare => Ok(ExecuteResponse::Prepare),
            ExecuteResponseKind::Raised => Ok(ExecuteResponse::Raised),
            ExecuteResponseKind::ReassignOwned => Ok(ExecuteResponse::ReassignOwned),
            ExecuteResponseKind::RevokedPrivilege => Ok(ExecuteResponse::RevokedPrivilege),
            ExecuteResponseKind::RevokedRole => Ok(ExecuteResponse::RevokedRole),
            ExecuteResponseKind::SendingRows => Err(()),
            ExecuteResponseKind::SetVariable => Err(()),
            ExecuteResponseKind::StartedTransaction => Ok(ExecuteResponse::StartedTransaction),
            ExecuteResponseKind::Subscribing => Err(()),
            ExecuteResponseKind::TransactionCommitted => Err(()),
            ExecuteResponseKind::TransactionRolledBack => Err(()),
            ExecuteResponseKind::Updated => Err(()),
            ExecuteResponseKind::ValidatedConnection => Ok(ExecuteResponse::ValidatedConnection),
            ExecuteResponseKind::SendingRowsImmediate => Err(()),
        }
    }
}

impl ExecuteResponse {
    pub fn tag(&self) -> Option<String> {
        use ExecuteResponse::*;
        match self {
            AlteredDefaultPrivileges => Some("ALTER DEFAULT PRIVILEGES".into()),
            AlteredObject(o) => Some(format!("ALTER {}", o)),
            AlteredRole => Some("ALTER ROLE".into()),
            AlteredSystemConfiguration => Some("ALTER SYSTEM".into()),
            ClosedCursor => Some("CLOSE CURSOR".into()),
            Comment => Some("COMMENT".into()),
            Copied(n) => Some(format!("COPY {}", n)),
            CopyTo { .. } => None,
            CopyFrom { .. } => None,
            CreatedConnection { .. } => Some("CREATE CONNECTION".into()),
            CreatedDatabase { .. } => Some("CREATE DATABASE".into()),
            CreatedSchema { .. } => Some("CREATE SCHEMA".into()),
            CreatedRole => Some("CREATE ROLE".into()),
            CreatedCluster { .. } => Some("CREATE CLUSTER".into()),
            CreatedClusterReplica { .. } => Some("CREATE CLUSTER REPLICA".into()),
            CreatedIndex { .. } => Some("CREATE INDEX".into()),
            CreatedSecret { .. } => Some("CREATE SECRET".into()),
            CreatedSink { .. } => Some("CREATE SINK".into()),
            CreatedSource { .. } => Some("CREATE SOURCE".into()),
            CreatedTable { .. } => Some("CREATE TABLE".into()),
            CreatedView { .. } => Some("CREATE VIEW".into()),
            CreatedViews { .. } => Some("CREATE VIEWS".into()),
            CreatedMaterializedView { .. } => Some("CREATE MATERIALIZED VIEW".into()),
            CreatedType => Some("CREATE TYPE".into()),
            Deallocate { all } => Some(format!("DEALLOCATE{}", if *all { " ALL" } else { "" })),
            DeclaredCursor => Some("DECLARE CURSOR".into()),
            Deleted(n) => Some(format!("DELETE {}", n)),
            DiscardedTemp => Some("DISCARD TEMP".into()),
            DiscardedAll => Some("DISCARD ALL".into()),
            DroppedObject(o) => Some(format!("DROP {o}")),
            DroppedOwned => Some("DROP OWNED".into()),
            EmptyQuery => None,
            Fetch { .. } => None,
            GrantedPrivilege => Some("GRANT".into()),
            GrantedRole => Some("GRANT ROLE".into()),
            Inserted(n) => {
                // "On successful completion, an INSERT command returns a
                // command tag of the form `INSERT <oid> <count>`."
                //     -- https://www.postgresql.org/docs/11/sql-insert.html
                //
                // OIDs are a PostgreSQL-specific historical quirk, but we
                // can return a 0 OID to indicate that the table does not
                // have OIDs.
                Some(format!("INSERT 0 {}", n))
            }
            Prepare => Some("PREPARE".into()),
            Raised => Some("RAISE".into()),
            ReassignOwned => Some("REASSIGN OWNED".into()),
            RevokedPrivilege => Some("REVOKE".into()),
            RevokedRole => Some("REVOKE ROLE".into()),
            SendingRows { .. } | SendingRowsImmediate { .. } => None,
            SetVariable { reset: true, .. } => Some("RESET".into()),
            SetVariable { reset: false, .. } => Some("SET".into()),
            StartedTransaction { .. } => Some("BEGIN".into()),
            Subscribing { .. } => None,
            TransactionCommitted { .. } => Some("COMMIT".into()),
            TransactionRolledBack { .. } => Some("ROLLBACK".into()),
            Updated(n) => Some(format!("UPDATE {}", n)),
            ValidatedConnection => Some("VALIDATE CONNECTION".into()),
        }
    }

    /// Expresses which [`PlanKind`] generate which set of [`ExecuteResponseKind`].
    /// `ExecuteResponseKind::Canceled` could be generated at any point as well, but that is
    /// excluded from this function.
    pub fn generated_from(plan: &PlanKind) -> &'static [ExecuteResponseKind] {
        use ExecuteResponseKind::*;
        use PlanKind::*;

        match plan {
            AbortTransaction => &[TransactionRolledBack],
            AlterClusterRename
            | AlterClusterSwap
            | AlterCluster
            | AlterClusterReplicaRename
            | AlterOwner
            | AlterItemRename
            | AlterRetainHistory
            | AlterItemSwap
            | AlterNoop
            | AlterSchemaRename
            | AlterSchemaSwap
            | AlterSecret
            | AlterConnection
            | AlterSource => &[AlteredObject],
            AlterDefaultPrivileges => &[AlteredDefaultPrivileges],
            AlterSetCluster => &[AlteredObject],
            AlterRole => &[AlteredRole],
            AlterSystemSet | AlterSystemReset | AlterSystemResetAll => {
                &[AlteredSystemConfiguration]
            }
            Close => &[ClosedCursor],
            PlanKind::CopyFrom => &[ExecuteResponseKind::CopyFrom],
            PlanKind::CopyTo => &[ExecuteResponseKind::Copied],
            PlanKind::Comment => &[ExecuteResponseKind::Comment],
            CommitTransaction => &[TransactionCommitted, TransactionRolledBack],
            CreateConnection => &[CreatedConnection],
            CreateDatabase => &[CreatedDatabase],
            CreateSchema => &[CreatedSchema],
            CreateRole => &[CreatedRole],
            CreateCluster => &[CreatedCluster],
            CreateClusterReplica => &[CreatedClusterReplica],
            CreateSource | CreateSources => &[CreatedSource],
            CreateSecret => &[CreatedSecret],
            CreateSink => &[CreatedSink],
            CreateTable => &[CreatedTable],
            CreateView => &[CreatedView],
            CreateMaterializedView => &[CreatedMaterializedView],
            CreateIndex => &[CreatedIndex],
            CreateType => &[CreatedType],
            PlanKind::Deallocate => &[ExecuteResponseKind::Deallocate],
            Declare => &[DeclaredCursor],
            DiscardTemp => &[DiscardedTemp],
            DiscardAll => &[DiscardedAll],
            DropObjects => &[DroppedObject],
            DropOwned => &[DroppedOwned],
            PlanKind::EmptyQuery => &[ExecuteResponseKind::EmptyQuery],
            ExplainPlan | ExplainPushdown | ExplainTimestamp | Select | ShowAllVariables
            | ShowCreate | ShowColumns | ShowVariable | InspectShard | ExplainSinkSchema => &[
                ExecuteResponseKind::CopyTo,
                SendingRows,
                SendingRowsImmediate,
            ],
            Execute | ReadThenWrite => &[
                Deleted,
                Inserted,
                SendingRows,
                SendingRowsImmediate,
                Updated,
            ],
            PlanKind::Fetch => &[ExecuteResponseKind::Fetch],
            GrantPrivileges => &[GrantedPrivilege],
            GrantRole => &[GrantedRole],
            Insert => &[Inserted, SendingRowsImmediate],
            PlanKind::Prepare => &[ExecuteResponseKind::Prepare],
            PlanKind::Raise => &[ExecuteResponseKind::Raised],
            PlanKind::ReassignOwned => &[ExecuteResponseKind::ReassignOwned],
            RevokePrivileges => &[RevokedPrivilege],
            RevokeRole => &[RevokedRole],
            PlanKind::SetVariable | ResetVariable | PlanKind::SetTransaction => {
                &[ExecuteResponseKind::SetVariable]
            }
            PlanKind::Subscribe => &[Subscribing, ExecuteResponseKind::CopyTo],
            StartTransaction => &[StartedTransaction],
            SideEffectingFunc => &[SendingRows, SendingRowsImmediate],
            ValidateConnection => &[ExecuteResponseKind::ValidatedConnection],
        }
    }
}

/// This implementation is meant to ensure that we maintain updated information
/// about which types of `ExecuteResponse`s are permitted to be sent, which will
/// be a function of which plan we're executing.
impl Transmittable for ExecuteResponse {
    type Allowed = ExecuteResponseKind;
    fn to_allowed(&self) -> Self::Allowed {
        ExecuteResponseKind::from(self)
    }
}

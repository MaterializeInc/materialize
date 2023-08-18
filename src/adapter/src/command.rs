// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

// `EnumKind` unconditionally introduces a lifetime. TODO: remove this once
// https://github.com/rust-lang/rust-clippy/pull/9037 makes it into stable
#![allow(clippy::extra_unused_lifetimes)]

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use anyhow::Context;
use derivative::Derivative;
use enum_kinds::EnumKind;
use mz_ore::collections::CollectionExt;
use mz_ore::soft_assert;
use mz_ore::tracing::OpenTelemetryContext;
use mz_pgcopy::CopyFormatParams;
use mz_repr::role_id::RoleId;
use mz_repr::statement_logging::StatementEndedExecutionReason;
use mz_repr::{ColumnType, Datum, GlobalId, Row, RowArena};
use mz_secrets::cache::CachingSecretsReader;
use mz_secrets::SecretsReader;
use mz_sql::ast::{FetchDirection, Raw, Statement};
use mz_sql::catalog::ObjectType;
use mz_sql::plan::{ExecuteTimeout, Plan, PlanKind, WebhookValidation, WebhookValidationSecret};
use mz_sql::session::user::User;
use mz_sql::session::vars::Var;
use mz_sql_parser::ast::{AlterObjectRenameStatement, AlterOwnerStatement, DropObjectsStatement};
use mz_storage_client::controller::MonotonicAppender;
use tokio::sync::{oneshot, watch};
use uuid::Uuid;

use crate::catalog::Catalog;
use crate::client::{ConnectionId, ConnectionIdType};
use crate::coord::peek::PeekResponseUnary;
use crate::coord::ExecuteContextExtra;
use crate::error::AdapterError;
use crate::session::{EndTransactionAction, RowBatchStream, Session};
use crate::util::Transmittable;

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
        session: Session,
        cancel_tx: Arc<watch::Sender<Canceled>>,
        tx: oneshot::Sender<Response<StartupResponse>>,
        /// keys of settings that were set on statup, and thus should not be
        /// overridden by defaults.
        set_setting_keys: Vec<String>,
        user: User,
        conn_id: ConnectionId,
        secret_key: u32,
        uuid: Uuid,
        application_name: String,
    },

    Execute {
        portal_name: String,
        session: Session,
        tx: oneshot::Sender<Response<ExecuteResponse>>,
        outer_ctx_extra: Option<ExecuteContextExtra>,
        span: tracing::Span,
    },

    Commit {
        action: EndTransactionAction,
        session: Session,
        tx: oneshot::Sender<Response<ExecuteResponse>>,
        // TODO: Ideally this would just be a tracing::Span, but that seems like
        // it might be tickling a bug in tracing_opentelemetry:
        // https://github.com/tokio-rs/tracing-opentelemetry/issues/14
        otel_ctx: OpenTelemetryContext,
    },

    CancelRequest {
        conn_id: ConnectionIdType,
        secret_key: u32,
    },

    PrivilegedCancelRequest {
        conn_id: ConnectionId,
    },

    DumpCatalog {
        session: Session,
        tx: oneshot::Sender<Response<CatalogDump>>,
    },

    CopyRows {
        id: GlobalId,
        columns: Vec<usize>,
        rows: Vec<Row>,
        session: Session,
        tx: oneshot::Sender<Response<ExecuteResponse>>,
        ctx_extra: ExecuteContextExtra,
    },

    AppendWebhook {
        database: String,
        schema: String,
        name: String,
        conn_id: ConnectionId,
        tx: oneshot::Sender<Result<AppendWebhookResponse, AdapterError>>,
    },

    GetSystemVars {
        session: Session,
        tx: oneshot::Sender<Response<GetVariablesResponse>>,
    },

    SetSystemVars {
        vars: BTreeMap<String, String>,
        session: Session,
        tx: oneshot::Sender<Response<()>>,
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
}

impl Command {
    pub fn session(&self) -> Option<&Session> {
        match self {
            Command::Startup { session, .. }
            | Command::Execute { session, .. }
            | Command::Commit { session, .. }
            | Command::DumpCatalog { session, .. }
            | Command::CopyRows { session, .. }
            | Command::GetSystemVars { session, .. }
            | Command::SetSystemVars { session, .. } => Some(session),
            Command::CancelRequest { .. }
            | Command::CatalogSnapshot { .. }
            | Command::PrivilegedCancelRequest { .. }
            | Command::AppendWebhook { .. }
            | Command::Terminate { .. }
            | Command::RetireExecute { .. } => None,
        }
    }

    pub fn session_mut(&mut self) -> Option<&mut Session> {
        match self {
            Command::Startup { session, .. }
            | Command::Execute { session, .. }
            | Command::Commit { session, .. }
            | Command::DumpCatalog { session, .. }
            | Command::CopyRows { session, .. }
            | Command::GetSystemVars { session, .. }
            | Command::SetSystemVars { session, .. } => Some(session),
            Command::CancelRequest { .. }
            | Command::CatalogSnapshot { .. }
            | Command::PrivilegedCancelRequest { .. }
            | Command::AppendWebhook { .. }
            | Command::Terminate { .. }
            | Command::RetireExecute { .. } => None,
        }
    }
}

#[derive(Debug)]
pub struct Response<T> {
    pub result: Result<T, AdapterError>,
    pub session: Session,
}

pub type RowsFuture = Pin<Box<dyn Future<Output = PeekResponseUnary> + Send>>;

/// The response to [`Client::startup`](crate::Client::startup).
#[derive(Debug)]
pub struct StartupResponse {
    /// RoleId for the user.
    pub role_id: RoleId,
    /// Vec of (name, VarInput::Flat) tuples of session variables that should be set.
    pub set_vars: Vec<(String, String)>,
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

/// Errors returns when running validation of a webhook request.
#[derive(thiserror::Error, Debug)]
pub enum AppendWebhookError {
    // A secret that we need for validation has gone missing.
    #[error("could not read a required secret")]
    MissingSecret,
    #[error("the provided request body is not UTF-8")]
    NonUtf8Body,
    // Note: we should _NEVER_ add more detail to this error, including the actual error we got
    // when running validation. This is because the error messages might contain info about the
    // arguments provided to the validation expression, we could contains user SECRETs. So by
    // including any more detail we might accidentally expose SECRETs.
    #[error("validation failed")]
    ValidationError,
    // Note: we should _NEVER_ add more detail to this error, see above as to why.
    #[error("internal error when validating request")]
    InternalError,
}

/// Contains all of the components necessary for running webhook validation.
///
/// To actually validate a webhook request call [`AppendWebhookValidator::eval`].
pub struct AppendWebhookValidator {
    validation: WebhookValidation,
    secrets_reader: CachingSecretsReader,
}

impl AppendWebhookValidator {
    pub fn new(validation: WebhookValidation, secrets_reader: CachingSecretsReader) -> Self {
        AppendWebhookValidator {
            validation,
            secrets_reader,
        }
    }

    pub async fn eval(
        self,
        body: bytes::Bytes,
        headers: Arc<BTreeMap<String, String>>,
    ) -> Result<bool, AppendWebhookError> {
        let AppendWebhookValidator {
            validation,
            secrets_reader,
        } = self;

        let WebhookValidation {
            expression,
            secrets,
            bodies: body_columns,
            headers: header_columns,
        } = validation;

        // Use the secrets reader to get any secrets.
        let mut secret_contents = BTreeMap::new();
        for WebhookValidationSecret {
            id,
            column_idx,
            use_bytes,
        } in secrets
        {
            let secret = secrets_reader
                .read(id)
                .await
                .map_err(|_| AppendWebhookError::MissingSecret)?;
            secret_contents.insert(column_idx, (secret, use_bytes));
        }

        // Create a closure to run our validation, this allows lifetimes and unwind boundaries to
        // work.
        let validate = move || {
            // Gather our Datums for evaluation
            //
            // TODO(parkmycar): Re-use the RowArena when we implement rate limiting.
            let temp_storage = RowArena::default();
            let mut datums = Vec::with_capacity(
                body_columns.len() + header_columns.len() + secret_contents.len(),
            );

            // Append all of our body columns.
            for (column_idx, use_bytes) in body_columns {
                assert_eq!(column_idx, datums.len(), "body index and datums mismatch!");

                let datum = if use_bytes {
                    Datum::Bytes(&body[..])
                } else {
                    let s = std::str::from_utf8(&body[..])
                        .map_err(|_| AppendWebhookError::NonUtf8Body)?;
                    Datum::String(s)
                };
                datums.push(datum);
            }

            // Append all of our header columns, re-using Row packings.
            //
            // TODO(parkmycar): Use `std::cell::OnceCell` when #20779 merges.
            let headers_byte = once_cell::unsync::OnceCell::new();
            let headers_text = once_cell::unsync::OnceCell::new();
            for (column_idx, use_bytes) in header_columns {
                assert_eq!(column_idx, datums.len(), "index and datums mismatch!");

                let row = if use_bytes {
                    headers_byte.get_or_init(|| {
                        let mut row = Row::with_capacity(1);
                        let mut packer = row.packer();
                        packer.push_dict(
                            headers
                                .iter()
                                .map(|(name, val)| (name.as_str(), Datum::Bytes(val.as_bytes()))),
                        );
                        row
                    })
                } else {
                    headers_text.get_or_init(|| {
                        let mut row = Row::with_capacity(1);
                        let mut packer = row.packer();
                        packer.push_dict(
                            headers
                                .iter()
                                .map(|(name, val)| (name.as_str(), Datum::String(val))),
                        );
                        row
                    })
                };
                datums.push(row.unpack_first());
            }

            // Append all of our secrets to our datums, in the correct column order.
            for column_idx in datums.len()..datums.len() + secret_contents.len() {
                // Get the secret that corresponds with what is the next "column";
                let (secret, use_bytes) = secret_contents
                    .get(&column_idx)
                    .expect("more secrets to provide, but none for the next column");

                if *use_bytes {
                    datums.push(Datum::Bytes(secret));
                } else {
                    let secret_str = std::str::from_utf8(&secret[..]).expect("valid UTF-8");
                    datums.push(Datum::String(secret_str));
                }
            }

            // Run our validation
            let valid = expression
                .eval(&datums[..], &temp_storage)
                .map_err(|_| AppendWebhookError::ValidationError)?;
            match valid {
                Datum::True => Ok::<_, AppendWebhookError>(true),
                Datum::False | Datum::Null => Ok(false),
                _ => unreachable!("Creating a webhook source asserts we return a boolean"),
            }
        };

        // Then run the validation itself.
        let valid = mz_ore::task::spawn_blocking(
            || "webhook-validator-expr",
            move || {
                // Since the validation expression is technically a user defined function, we want to
                // be extra careful and guard against issues taking down the entire process.
                mz_ore::panic::catch_unwind(validate).map_err(|_| {
                    tracing::error!("panic while validating webhook request!");
                    AppendWebhookError::InternalError
                })
            },
        )
        .await
        .context("joining on validation")
        .map_err(|e| {
            tracing::error!("Failed to run validation for webhook, {e}");
            AppendWebhookError::InternalError
        })??;

        valid
    }
}

pub struct AppendWebhookResponse {
    pub tx: MonotonicAppender,
    pub body_ty: ColumnType,
    pub header_ty: Option<ColumnType>,
    pub validator: Option<AppendWebhookValidator>,
}

impl fmt::Debug for AppendWebhookResponse {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("AppendWebhookResponse")
            .field("tx", &self.tx)
            .field("body_ty", &self.body_ty)
            .field("header_ty", &self.header_ty)
            .field("validate_expr", &"(...)")
            .finish()
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
    /// The index was altered.
    AlteredIndexLogicalCompaction,
    /// The role was altered.
    AlteredRole,
    /// The system configuration was altered.
    AlteredSystemConfiguration,
    /// The query was canceled.
    Canceled,
    /// The requested cursor was closed.
    ClosedCursor,
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
    /// The requested HTTP source was created.
    CreatedWebhookSource,
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
        #[derivative(Debug = "ignore")]
        span: tracing::Span,
    },
    /// Like `SendingRows`, but the rows are known to be available
    /// immediately, and thus the execution is considered ended in the coordinator.
    SendingRowsImmediate {
        rows: Vec<Row>,
        #[derivative(Debug = "ignore")]
        span: tracing::Span,
    },
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
        let resp_kinds = Plan::generated_from(stmt.into())
            .into_iter()
            .map(ExecuteResponse::generated_from)
            .flatten()
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
        soft_assert!(
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
            ExecuteResponseKind::AlteredIndexLogicalCompaction => {
                Ok(ExecuteResponse::AlteredIndexLogicalCompaction)
            }
            ExecuteResponseKind::AlteredRole => Ok(ExecuteResponse::AlteredRole),
            ExecuteResponseKind::AlteredSystemConfiguration => {
                Ok(ExecuteResponse::AlteredSystemConfiguration)
            }
            ExecuteResponseKind::Canceled => Ok(ExecuteResponse::Canceled),
            ExecuteResponseKind::ClosedCursor => Ok(ExecuteResponse::ClosedCursor),
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
            ExecuteResponseKind::CreatedWebhookSource => Ok(ExecuteResponse::CreatedWebhookSource),
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
            AlteredIndexLogicalCompaction => Some("ALTER INDEX".into()),
            AlteredRole => Some("ALTER ROLE".into()),
            AlteredSystemConfiguration => Some("ALTER SYSTEM".into()),
            Canceled => None,
            ClosedCursor => Some("CLOSE CURSOR".into()),
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
            CreatedWebhookSource { .. } => Some("CREATE SOURCE".into()),
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
    pub fn generated_from(plan: PlanKind) -> Vec<ExecuteResponseKind> {
        use ExecuteResponseKind::*;
        use PlanKind::*;

        match plan {
            AbortTransaction => vec![TransactionRolledBack],
            AlterClusterRename
            | AlterCluster
            | AlterClusterReplicaRename
            | AlterOwner
            | AlterItemRename
            | AlterNoop
            | AlterSecret
            | AlterSink
            | AlterSource
            | PurifiedAlterSource
            | RotateKeys => {
                vec![AlteredObject]
            }
            AlterDefaultPrivileges => vec![AlteredDefaultPrivileges],
            AlterSetCluster => vec![AlteredObject],
            AlterIndexSetOptions | AlterIndexResetOptions => {
                vec![AlteredObject, AlteredIndexLogicalCompaction]
            }
            AlterRole => vec![AlteredRole],
            AlterSystemSet | AlterSystemReset | AlterSystemResetAll => {
                vec![AlteredSystemConfiguration]
            }
            Close => vec![ClosedCursor],
            PlanKind::CopyFrom => vec![ExecuteResponseKind::CopyFrom],
            CommitTransaction => vec![TransactionCommitted, TransactionRolledBack],
            CreateConnection => vec![CreatedConnection],
            CreateDatabase => vec![CreatedDatabase],
            CreateSchema => vec![CreatedSchema],
            CreateRole => vec![CreatedRole],
            CreateCluster => vec![CreatedCluster],
            CreateClusterReplica => vec![CreatedClusterReplica],
            CreateSource | CreateSources => vec![CreatedSource],
            CreateSecret => vec![CreatedSecret],
            CreateSink => vec![CreatedSink],
            CreateTable => vec![CreatedTable],
            CreateView => vec![CreatedView],
            CreateMaterializedView => vec![CreatedMaterializedView],
            CreateIndex => vec![CreatedIndex],
            CreateType => vec![CreatedType],
            PlanKind::Deallocate => vec![ExecuteResponseKind::Deallocate],
            Declare => vec![DeclaredCursor],
            DiscardTemp => vec![DiscardedTemp],
            DiscardAll => vec![DiscardedAll],
            DropObjects => vec![DroppedObject],
            DropOwned => vec![DroppedOwned],
            PlanKind::EmptyQuery => vec![ExecuteResponseKind::EmptyQuery],
            Explain | Select | ShowAllVariables | ShowCreate | ShowColumns | ShowVariable
            | InspectShard => {
                vec![CopyTo, SendingRows, SendingRowsImmediate]
            }
            Execute | ReadThenWrite => vec![
                Deleted,
                Inserted,
                SendingRows,
                SendingRowsImmediate,
                Updated,
            ],
            PlanKind::Fetch => vec![ExecuteResponseKind::Fetch],
            GrantPrivileges => vec![GrantedPrivilege],
            GrantRole => vec![GrantedRole],
            CopyRows => vec![Inserted],
            Insert => vec![Inserted, SendingRowsImmediate],
            PlanKind::Prepare => vec![ExecuteResponseKind::Prepare],
            PlanKind::Raise => vec![ExecuteResponseKind::Raised],
            PlanKind::ReassignOwned => vec![ExecuteResponseKind::ReassignOwned],
            RevokePrivileges => vec![RevokedPrivilege],
            RevokeRole => vec![RevokedRole],
            PlanKind::SetVariable | ResetVariable | PlanKind::SetTransaction => {
                vec![ExecuteResponseKind::SetVariable]
            }
            PlanKind::Subscribe => vec![Subscribing, CopyTo],
            StartTransaction => vec![StartedTransaction],
            SideEffectingFunc => vec![SendingRowsImmediate],
            ValidateConnection => vec![ExecuteResponseKind::ValidatedConnection],
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

/// The state of a cancellation request.
#[derive(Debug, Clone, Copy)]
pub enum Canceled {
    /// A cancellation request has occurred.
    Canceled,
    /// No cancellation request has yet occurred, or a previous request has been
    /// cleared.
    NotCanceled,
}

// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::BTreeMap;
use std::error::Error;
use std::fmt;
use std::num::TryFromIntError;

use dec::TryFromDecimalError;
use itertools::Itertools;
use mz_catalog::builtin::MZ_CATALOG_SERVER_CLUSTER;
use mz_compute_client::controller::error as compute_error;
use mz_compute_client::controller::error::{CollectionLookupError, InstanceMissing};
use mz_compute_client::controller::instance::PeekError;
use mz_compute_types::ComputeInstanceId;
use mz_expr::EvalError;
use mz_ore::error::ErrorExt;
use mz_ore::stack::RecursionLimitError;
use mz_ore::str::StrExt;
use mz_pgwire_common::{ErrorResponse, Severity};
use mz_repr::adt::timestamp::TimestampError;
use mz_repr::explain::ExplainError;
use mz_repr::{NotNullViolation, Timestamp};
use mz_sql::plan::PlanError;
use mz_sql::rbac;
use mz_sql::session::vars::VarError;
use mz_storage_types::connections::ConnectionValidationError;
use mz_storage_types::controller::StorageError;
use mz_storage_types::errors::CollectionMissing;
use smallvec::SmallVec;
use timely::progress::Antichain;
use tokio::sync::oneshot;
use tokio_postgres::error::SqlState;

use crate::coord::NetworkPolicyError;
use crate::optimize::OptimizerError;

/// Errors that can occur in the coordinator.
#[derive(Debug)]
pub enum AdapterError {
    /// A `SUBSCRIBE` was requested whose `UP TO` bound precedes its `as_of` timestamp
    AbsurdSubscribeBounds {
        as_of: mz_repr::Timestamp,
        up_to: mz_repr::Timestamp,
    },
    /// Attempted to use a potentially ambiguous column reference expression with a system table.
    // We don't allow this until https://github.com/MaterializeInc/database-issues/issues/4824 is
    // resolved because it prevents us from adding columns to system tables.
    AmbiguousSystemColumnReference,
    /// An error occurred in a catalog operation.
    Catalog(mz_catalog::memory::error::Error),
    /// 1. The cached plan or descriptor changed,
    /// 2. or some dependency of a statement disappeared during sequencing.
    /// TODO(ggevay): we should refactor 2. usages to use `ConcurrentDependencyDrop` instead
    /// (e.g., in MV sequencing)
    ChangedPlan(String),
    /// The cursor already exists.
    DuplicateCursor(String),
    /// An error while evaluating an expression.
    Eval(EvalError),
    /// An error occurred while planning the statement.
    Explain(ExplainError),
    /// The ID allocator exhausted all valid IDs.
    IdExhaustionError,
    /// Unexpected internal state was encountered.
    Internal(String),
    /// Attempted to read from log sources of a replica with disabled introspection.
    IntrospectionDisabled {
        log_names: Vec<String>,
    },
    /// Attempted to create an object dependent on log sources that doesn't support
    /// log dependencies.
    InvalidLogDependency {
        object_type: String,
        log_names: Vec<String>,
    },
    /// No such cluster replica size has been configured.
    InvalidClusterReplicaAz {
        az: String,
        expected: Vec<String>,
    },
    /// SET TRANSACTION ISOLATION LEVEL was called in the middle of a transaction.
    InvalidSetIsolationLevel,
    /// SET cluster was called in the middle of a transaction.
    InvalidSetCluster,
    /// No such storage instance size has been configured.
    InvalidStorageClusterSize {
        size: String,
        expected: Vec<String>,
    },
    /// Creating a source or sink without specifying its size is forbidden.
    SourceOrSinkSizeRequired {
        expected: Vec<String>,
    },
    /// The selection value for a table mutation operation refers to an invalid object.
    InvalidTableMutationSelection,
    /// Expression violated a column's constraint
    ConstraintViolation(NotNullViolation),
    /// Transaction cluster was dropped in the middle of a transaction.
    ConcurrentClusterDrop,
    /// A dependency was dropped while sequencing a statement.
    ConcurrentDependencyDrop {
        dependency_kind: &'static str,
        dependency_id: String,
    },
    CollectionUnreadable {
        id: String,
    },
    /// Target cluster has no replicas to service query.
    NoClusterReplicasAvailable {
        name: String,
        is_managed: bool,
    },
    /// The named operation cannot be run in a transaction.
    OperationProhibitsTransaction(String),
    /// The named operation requires an active transaction.
    OperationRequiresTransaction(String),
    /// An error occurred while planning the statement.
    PlanError(PlanError),
    /// The named prepared statement already exists.
    PreparedStatementExists(String),
    /// Wrapper around parsing error
    ParseError(mz_sql_parser::parser::ParserStatementError),
    /// The transaction is in read-only mode.
    ReadOnlyTransaction,
    /// The transaction in in read-only mode and a read already occurred.
    ReadWriteUnavailable,
    /// The recursion limit of some operation was exceeded.
    RecursionLimit(RecursionLimitError),
    /// A query in a transaction referenced a relation outside the first query's
    /// time domain.
    RelationOutsideTimeDomain {
        relations: Vec<String>,
        names: Vec<String>,
    },
    /// A query tried to create more resources than is allowed in the system configuration.
    ResourceExhaustion {
        resource_type: String,
        limit_name: String,
        desired: String,
        limit: String,
        current: String,
    },
    /// Result size of a query is too large.
    ResultSize(String),
    /// The specified feature is not permitted in safe mode.
    SafeModeViolation(String),
    /// The current transaction had the wrong set of write locks.
    WrongSetOfLocks,
    /// Waiting on a query timed out.
    ///
    /// Note this differs slightly from PG's implementation/semantics.
    StatementTimeout,
    /// The user canceled the query
    Canceled,
    /// An idle session in a transaction has timed out.
    IdleInTransactionSessionTimeout,
    /// The transaction is in single-subscribe mode.
    SubscribeOnlyTransaction,
    /// An error occurred in the optimizer.
    Optimizer(OptimizerError),
    /// A query depends on items which are not allowed to be referenced from the current cluster.
    UnallowedOnCluster {
        depends_on: SmallVec<[String; 2]>,
        cluster: String,
    },
    /// A user tried to perform an action that they were unauthorized to do.
    Unauthorized(rbac::UnauthorizedError),
    /// The named cursor does not exist.
    UnknownCursor(String),
    /// The named role does not exist.
    UnknownLoginRole(String),
    UnknownPreparedStatement(String),
    /// The named cluster replica does not exist.
    UnknownClusterReplica {
        cluster_name: String,
        replica_name: String,
    },
    /// The named setting does not exist.
    UnrecognizedConfigurationParam(String),
    /// A generic error occurred.
    //
    // TODO(benesch): convert all those errors to structured errors.
    Unstructured(anyhow::Error),
    /// The named feature is not supported and will (probably) not be.
    Unsupported(&'static str),
    /// Some feature isn't available for a (potentially opaque) reason.
    /// For example, in cloud Self-Managed auth features aren't available,
    /// but we don't want to mention self managed auth.
    UnavailableFeature {
        feature: String,
        docs: Option<String>,
    },
    /// Attempted to read from log sources without selecting a target replica.
    UntargetedLogRead {
        log_names: Vec<String>,
    },
    /// The transaction is in write-only mode.
    WriteOnlyTransaction,
    /// The transaction can only execute a single statement.
    SingleStatementTransaction,
    /// The transaction can only execute simple DDL.
    DDLOnlyTransaction,
    /// Another session modified the Catalog while this transaction was open.
    DDLTransactionRace,
    /// Used to prevent us from durably committing state while a DDL transaction is open, should
    /// never be returned to the user.
    TransactionDryRun {
        /// New operations that were run in the transaction.
        new_ops: Vec<crate::catalog::Op>,
        /// New resulting `CatalogState`.
        new_state: crate::catalog::CatalogState,
    },
    /// An error occurred in the storage layer
    Storage(mz_storage_types::controller::StorageError<mz_repr::Timestamp>),
    /// An error occurred in the compute layer
    Compute(anyhow::Error),
    /// An error in the orchestrator layer
    Orchestrator(anyhow::Error),
    /// A statement tried to drop a role that had dependent objects.
    ///
    /// The map keys are role names and values are detailed error messages.
    DependentObject(BTreeMap<String, Vec<String>>),
    /// When performing an `ALTER` of some variety, re-planning the statement
    /// errored.
    InvalidAlter(&'static str, PlanError),
    /// An error occurred while validating a connection.
    ConnectionValidation(ConnectionValidationError),
    /// We refuse to create the materialized view, because it would never be refreshed, so it would
    /// never be queryable. This can happen when the only specified refreshes are further back in
    /// the past than the initial compaction window of the materialized view.
    MaterializedViewWouldNeverRefresh(Timestamp, Timestamp),
    /// A CREATE MATERIALIZED VIEW statement tried to acquire a read hold at a REFRESH AT time,
    /// but was unable to get a precise read hold.
    InputNotReadableAtRefreshAtTime(Timestamp, Antichain<Timestamp>),
    /// A humanized version of [`StorageError::RtrTimeout`].
    RtrTimeout(String),
    /// A humanized version of [`StorageError::RtrDropFailure`].
    RtrDropFailure(String),
    /// The collection requested to be sinked cannot be read at any timestamp
    UnreadableSinkCollection,
    /// User sessions have been blocked.
    UserSessionsDisallowed,
    /// This use session has been deneid by a NetworkPolicy.
    NetworkPolicyDenied(NetworkPolicyError),
    /// Something attempted a write (to catalog, storage, tables, etc.) while in
    /// read-only mode.
    ReadOnly,
    AlterClusterTimeout,
    AlterClusterWhilePendingReplicas,
    AuthenticationError(AuthenticationError),
}

#[derive(Debug, thiserror::Error)]
pub enum AuthenticationError {
    #[error("invalid credentials")]
    InvalidCredentials,
    #[error("role is not allowed to login")]
    NonLogin,
    #[error("role does not exist")]
    RoleNotFound,
    #[error("password is required")]
    PasswordRequired,
}

impl AdapterError {
    pub fn into_response(self, severity: Severity) -> ErrorResponse {
        ErrorResponse {
            severity,
            code: self.code(),
            message: self.to_string(),
            detail: self.detail(),
            hint: self.hint(),
            position: self.position(),
        }
    }

    pub fn position(&self) -> Option<usize> {
        match self {
            AdapterError::ParseError(err) => Some(err.error.pos),
            _ => None,
        }
    }

    /// Reports additional details about the error, if any are available.
    pub fn detail(&self) -> Option<String> {
        match self {
            AdapterError::AmbiguousSystemColumnReference => {
                Some("This is a current limitation in Materialize".into())
            },
            AdapterError::Catalog(c) => c.detail(),
            AdapterError::Eval(e) => e.detail(),
            AdapterError::RelationOutsideTimeDomain { relations, names } => Some(format!(
                "The following relations in the query are outside the transaction's time domain:\n{}\n{}",
                relations
                    .iter()
                    .map(|r| r.quoted().to_string())
                    .collect::<Vec<_>>()
                    .join("\n"),
                match names.is_empty() {
                    true => "No relations are available.".to_string(),
                    false => format!(
                        "Only the following relations are available:\n{}",
                        names
                            .iter()
                            .map(|name| name.quoted().to_string())
                            .collect::<Vec<_>>()
                            .join("\n")
                    ),
                }
            )),
            AdapterError::SourceOrSinkSizeRequired { .. } => Some(
                "Either specify the cluster that will maintain this object via IN CLUSTER or \
                specify size via SIZE option."
                    .into(),
            ),
            AdapterError::SafeModeViolation(_) => Some(
                "The Materialize server you are connected to is running in \
                 safe mode, which limits the features that are available."
                    .into(),
            ),
            AdapterError::IntrospectionDisabled { log_names }
            | AdapterError::UntargetedLogRead { log_names } => Some(format!(
                "The query references the following log sources:\n    {}",
                log_names.join("\n    "),
            )),
            AdapterError::InvalidLogDependency { log_names, .. } => Some(format!(
                "The object depends on the following log sources:\n    {}",
                log_names.join("\n    "),
            )),
            AdapterError::PlanError(e) => e.detail(),
            AdapterError::Unauthorized(unauthorized) => unauthorized.detail(),
            AdapterError::DependentObject(dependent_objects) => {
                Some(dependent_objects
                    .iter()
                    .map(|(role_name, err_msgs)| err_msgs
                        .iter()
                        .map(|err_msg| format!("{role_name}: {err_msg}"))
                        .join("\n"))
                    .join("\n"))
            },
            AdapterError::Storage(storage_error) => {
                storage_error.source().map(|source_error| source_error.to_string_with_causes())
            }
            AdapterError::ReadOnlyTransaction => Some("SELECT queries cannot be combined with other query types, including SUBSCRIBE.".into()),
            AdapterError::InvalidAlter(_, e) => e.detail(),
            AdapterError::Optimizer(e) => e.detail(),
            AdapterError::ConnectionValidation(e) => e.detail(),
            AdapterError::MaterializedViewWouldNeverRefresh(last_refresh, earliest_possible) => {
                Some(format!(
                    "The specified last refresh is at {}, while the earliest possible time to compute the materialized \
                    view is {}.",
                    last_refresh,
                    earliest_possible,
                ))
            }
            AdapterError::UnallowedOnCluster { cluster, .. } => (cluster == MZ_CATALOG_SERVER_CLUSTER.name).then(||
                format!("The transaction is executing on the {cluster} cluster, maybe having been routed there by the first statement in the transaction.")
            ),
            AdapterError::InputNotReadableAtRefreshAtTime(oracle_read_ts, least_valid_read) => {
                Some(format!(
                    "The requested REFRESH AT time is {}, \
                    but not all input collections are readable earlier than [{}].",
                    oracle_read_ts,
                    if least_valid_read.len() == 1 {
                        format!("{}", least_valid_read.as_option().expect("antichain contains exactly 1 timestamp"))
                    } else {
                        // This can't occur currently
                        format!("{:?}", least_valid_read)
                    }
                ))
            }
            AdapterError::RtrTimeout(name) => Some(format!("{name} failed to ingest data up to the real-time recency point")),
            AdapterError::RtrDropFailure(name) => Some(format!("{name} dropped before ingesting data to the real-time recency point")),
            AdapterError::UserSessionsDisallowed => Some("Your organization has been blocked. Please contact support.".to_string()),
            AdapterError::NetworkPolicyDenied(reason)=> Some(format!("{reason}.")),
            _ => None,
        }
    }

    /// Reports a hint for the user about how the error could be fixed.
    pub fn hint(&self) -> Option<String> {
        match self {
            AdapterError::AmbiguousSystemColumnReference => Some(
                "Rewrite the view to refer to all columns by name. Expand all wildcards and \
                convert all NATURAL JOINs to USING joins."
                    .to_string(),
            ),
            AdapterError::Catalog(c) => c.hint(),
            AdapterError::Eval(e) => e.hint(),
            AdapterError::InvalidClusterReplicaAz { expected, az: _ } => {
                Some(if expected.is_empty() {
                    "No availability zones configured; do not specify AVAILABILITY ZONE".into()
                } else {
                    format!("Valid availability zones are: {}", expected.join(", "))
                })
            }
            AdapterError::InvalidStorageClusterSize { expected, .. } => {
                Some(format!("Valid sizes are: {}", expected.join(", ")))
            }
            AdapterError::SourceOrSinkSizeRequired { expected } => Some(format!(
                "Try choosing one of the smaller sizes to start. Available sizes: {}",
                expected.join(", ")
            )),
            AdapterError::NoClusterReplicasAvailable { is_managed, .. } => {
                Some(if *is_managed {
                    "Use ALTER CLUSTER to adjust the replication factor of the cluster. \
                    Example:`ALTER CLUSTER <cluster-name> SET (REPLICATION FACTOR 1)`".into()
                } else {
                    "Use CREATE CLUSTER REPLICA to attach cluster replicas to the cluster".into()
                })
            }
            AdapterError::UntargetedLogRead { .. } => Some(
                "Use `SET cluster_replica = <replica-name>` to target a specific replica in the \
                 active cluster. Note that subsequent queries will only be answered by \
                 the selected replica, which might reduce availability. To undo the replica \
                 selection, use `RESET cluster_replica`."
                    .into(),
            ),
            AdapterError::ResourceExhaustion { resource_type, .. } => Some(format!(
                "Drop an existing {resource_type} or contact support to request a limit increase."
            )),
            AdapterError::StatementTimeout => Some(
                "Consider increasing the maximum allowed statement duration for this session by \
                 setting the statement_timeout session variable. For example, `SET \
                 statement_timeout = '120s'`."
                    .into(),
            ),
            AdapterError::PlanError(e) => e.hint(),
            AdapterError::UnallowedOnCluster { cluster, .. } => {
                (cluster != MZ_CATALOG_SERVER_CLUSTER.name).then(||
                    "Use `SET CLUSTER = <cluster-name>` to change your cluster and re-run the query."
                    .to_string()
                )
            }
            AdapterError::InvalidAlter(_, e) => e.hint(),
            AdapterError::Optimizer(e) => e.hint(),
            AdapterError::ConnectionValidation(e) => e.hint(),
            AdapterError::InputNotReadableAtRefreshAtTime(_, _) => Some(
                "You can use `REFRESH AT greatest(mz_now(), <explicit timestamp>)` to refresh \
                 either at the explicitly specified timestamp, or now if the given timestamp would \
                 be in the past.".to_string()
            ),
            AdapterError::AlterClusterTimeout => Some(
                "Consider increasing the timeout duration in the alter cluster statement.".into(),
            ),
            AdapterError::DDLTransactionRace => Some(
                "Currently, DDL transactions fail when any other DDL happens concurrently, \
                 even on unrelated schemas/clusters.".into()
            ),
            AdapterError::CollectionUnreadable { .. } => Some(
                "This could be because the collection has recently been dropped.".into()
            ),
            _ => None,
        }
    }

    pub fn code(&self) -> SqlState {
        // TODO(benesch): we should only use `SqlState::INTERNAL_ERROR` for
        // those errors that are truly internal errors. At the moment we have
        // a various classes of uncategorized errors that use this error code
        // inappropriately.
        match self {
            // DATA_EXCEPTION to match what Postgres returns for degenerate
            // range bounds
            AdapterError::AbsurdSubscribeBounds { .. } => SqlState::DATA_EXCEPTION,
            AdapterError::AmbiguousSystemColumnReference => SqlState::FEATURE_NOT_SUPPORTED,
            AdapterError::Catalog(e) => match &e.kind {
                mz_catalog::memory::error::ErrorKind::VarError(e) => match e {
                    VarError::ConstrainedParameter { .. } => SqlState::INVALID_PARAMETER_VALUE,
                    VarError::FixedValueParameter { .. } => SqlState::INVALID_PARAMETER_VALUE,
                    VarError::InvalidParameterType { .. } => SqlState::INVALID_PARAMETER_VALUE,
                    VarError::InvalidParameterValue { .. } => SqlState::INVALID_PARAMETER_VALUE,
                    VarError::ReadOnlyParameter(_) => SqlState::CANT_CHANGE_RUNTIME_PARAM,
                    VarError::UnknownParameter(_) => SqlState::UNDEFINED_OBJECT,
                    VarError::RequiresUnsafeMode { .. } => SqlState::CANT_CHANGE_RUNTIME_PARAM,
                    VarError::RequiresFeatureFlag { .. } => SqlState::CANT_CHANGE_RUNTIME_PARAM,
                },
                _ => SqlState::INTERNAL_ERROR,
            },
            AdapterError::ChangedPlan(_) => SqlState::FEATURE_NOT_SUPPORTED,
            AdapterError::DuplicateCursor(_) => SqlState::DUPLICATE_CURSOR,
            AdapterError::Eval(EvalError::CharacterNotValidForEncoding(_)) => {
                SqlState::PROGRAM_LIMIT_EXCEEDED
            }
            AdapterError::Eval(EvalError::CharacterTooLargeForEncoding(_)) => {
                SqlState::PROGRAM_LIMIT_EXCEEDED
            }
            AdapterError::Eval(EvalError::LengthTooLarge) => SqlState::PROGRAM_LIMIT_EXCEEDED,
            AdapterError::Eval(EvalError::NullCharacterNotPermitted) => {
                SqlState::PROGRAM_LIMIT_EXCEEDED
            }
            AdapterError::Eval(_) => SqlState::INTERNAL_ERROR,
            AdapterError::Explain(_) => SqlState::INTERNAL_ERROR,
            AdapterError::IdExhaustionError => SqlState::INTERNAL_ERROR,
            AdapterError::Internal(_) => SqlState::INTERNAL_ERROR,
            AdapterError::IntrospectionDisabled { .. } => SqlState::FEATURE_NOT_SUPPORTED,
            AdapterError::InvalidLogDependency { .. } => SqlState::FEATURE_NOT_SUPPORTED,
            AdapterError::InvalidClusterReplicaAz { .. } => SqlState::FEATURE_NOT_SUPPORTED,
            AdapterError::InvalidSetIsolationLevel => SqlState::ACTIVE_SQL_TRANSACTION,
            AdapterError::InvalidSetCluster => SqlState::ACTIVE_SQL_TRANSACTION,
            AdapterError::InvalidStorageClusterSize { .. } => SqlState::FEATURE_NOT_SUPPORTED,
            AdapterError::SourceOrSinkSizeRequired { .. } => SqlState::FEATURE_NOT_SUPPORTED,
            AdapterError::InvalidTableMutationSelection => SqlState::INVALID_TRANSACTION_STATE,
            AdapterError::ConstraintViolation(NotNullViolation(_)) => SqlState::NOT_NULL_VIOLATION,
            AdapterError::ConcurrentClusterDrop => SqlState::INVALID_TRANSACTION_STATE,
            AdapterError::ConcurrentDependencyDrop { .. } => SqlState::UNDEFINED_OBJECT,
            AdapterError::CollectionUnreadable { .. } => SqlState::NO_DATA_FOUND,
            AdapterError::NoClusterReplicasAvailable { .. } => SqlState::FEATURE_NOT_SUPPORTED,
            AdapterError::OperationProhibitsTransaction(_) => SqlState::ACTIVE_SQL_TRANSACTION,
            AdapterError::OperationRequiresTransaction(_) => SqlState::NO_ACTIVE_SQL_TRANSACTION,
            AdapterError::ParseError(_) => SqlState::SYNTAX_ERROR,
            AdapterError::PlanError(PlanError::InvalidSchemaName) => SqlState::INVALID_SCHEMA_NAME,
            AdapterError::PlanError(PlanError::ColumnAlreadyExists { .. }) => {
                SqlState::DUPLICATE_COLUMN
            }
            AdapterError::PlanError(PlanError::UnknownParameter(_)) => {
                SqlState::UNDEFINED_PARAMETER
            }
            AdapterError::PlanError(PlanError::ParameterNotAllowed(_)) => {
                SqlState::UNDEFINED_PARAMETER
            }
            AdapterError::PlanError(_) => SqlState::INTERNAL_ERROR,
            AdapterError::PreparedStatementExists(_) => SqlState::DUPLICATE_PSTATEMENT,
            AdapterError::ReadOnlyTransaction => SqlState::READ_ONLY_SQL_TRANSACTION,
            AdapterError::ReadWriteUnavailable => SqlState::INVALID_TRANSACTION_STATE,
            AdapterError::SingleStatementTransaction => SqlState::INVALID_TRANSACTION_STATE,
            AdapterError::WrongSetOfLocks => SqlState::LOCK_NOT_AVAILABLE,
            AdapterError::StatementTimeout => SqlState::QUERY_CANCELED,
            AdapterError::Canceled => SqlState::QUERY_CANCELED,
            AdapterError::IdleInTransactionSessionTimeout => {
                SqlState::IDLE_IN_TRANSACTION_SESSION_TIMEOUT
            }
            AdapterError::RecursionLimit(_) => SqlState::INTERNAL_ERROR,
            AdapterError::RelationOutsideTimeDomain { .. } => SqlState::INVALID_TRANSACTION_STATE,
            AdapterError::ResourceExhaustion { .. } => SqlState::INSUFFICIENT_RESOURCES,
            AdapterError::ResultSize(_) => SqlState::OUT_OF_MEMORY,
            AdapterError::SafeModeViolation(_) => SqlState::INTERNAL_ERROR,
            AdapterError::SubscribeOnlyTransaction => SqlState::INVALID_TRANSACTION_STATE,
            AdapterError::Optimizer(e) => match e {
                OptimizerError::PlanError(PlanError::InvalidSchemaName) => {
                    SqlState::INVALID_SCHEMA_NAME
                }
                OptimizerError::PlanError(PlanError::ColumnAlreadyExists { .. }) => {
                    SqlState::DUPLICATE_COLUMN
                }
                OptimizerError::PlanError(PlanError::UnknownParameter(_)) => {
                    SqlState::UNDEFINED_PARAMETER
                }
                OptimizerError::PlanError(PlanError::ParameterNotAllowed(_)) => {
                    SqlState::UNDEFINED_PARAMETER
                }
                OptimizerError::PlanError(_) => SqlState::INTERNAL_ERROR,
                OptimizerError::RecursionLimitError(e) => {
                    AdapterError::RecursionLimit(e.clone()).code() // Delegate to outer
                }
                OptimizerError::Internal(s) => {
                    AdapterError::Internal(s.clone()).code() // Delegate to outer
                }
                OptimizerError::EvalError(e) => {
                    AdapterError::Eval(e.clone()).code() // Delegate to outer
                }
                OptimizerError::TransformError(_) => SqlState::INTERNAL_ERROR,
                OptimizerError::UnmaterializableFunction(_) => SqlState::FEATURE_NOT_SUPPORTED,
                OptimizerError::UncallableFunction { .. } => SqlState::FEATURE_NOT_SUPPORTED,
                OptimizerError::UnsupportedTemporalExpression(_) => SqlState::FEATURE_NOT_SUPPORTED,
                // This should be handled by peek optimization, so it's an internal error if it
                // reaches the user.
                OptimizerError::InternalUnsafeMfpPlan(_) => SqlState::INTERNAL_ERROR,
            },
            AdapterError::UnallowedOnCluster { .. } => {
                SqlState::S_R_E_PROHIBITED_SQL_STATEMENT_ATTEMPTED
            }
            AdapterError::Unauthorized(_) => SqlState::INSUFFICIENT_PRIVILEGE,
            AdapterError::UnknownCursor(_) => SqlState::INVALID_CURSOR_NAME,
            AdapterError::UnknownPreparedStatement(_) => SqlState::UNDEFINED_PSTATEMENT,
            AdapterError::UnknownLoginRole(_) => SqlState::INVALID_AUTHORIZATION_SPECIFICATION,
            AdapterError::UnknownClusterReplica { .. } => SqlState::UNDEFINED_OBJECT,
            AdapterError::UnrecognizedConfigurationParam(_) => SqlState::UNDEFINED_OBJECT,
            AdapterError::Unsupported(..) => SqlState::FEATURE_NOT_SUPPORTED,
            AdapterError::UnavailableFeature { .. } => SqlState::FEATURE_NOT_SUPPORTED,
            AdapterError::Unstructured(_) => SqlState::INTERNAL_ERROR,
            AdapterError::UntargetedLogRead { .. } => SqlState::FEATURE_NOT_SUPPORTED,
            AdapterError::DDLTransactionRace => SqlState::T_R_SERIALIZATION_FAILURE,
            AdapterError::TransactionDryRun { .. } => SqlState::T_R_SERIALIZATION_FAILURE,
            // It's not immediately clear which error code to use here because a
            // "write-only transaction", "single table write transaction", or "ddl only
            // transaction" are not things in Postgres. This error code is the generic "bad txn
            // thing" code, so it's probably the best choice.
            AdapterError::WriteOnlyTransaction => SqlState::INVALID_TRANSACTION_STATE,
            AdapterError::DDLOnlyTransaction => SqlState::INVALID_TRANSACTION_STATE,
            AdapterError::Storage(_) | AdapterError::Compute(_) | AdapterError::Orchestrator(_) => {
                SqlState::INTERNAL_ERROR
            }
            AdapterError::DependentObject(_) => SqlState::DEPENDENT_OBJECTS_STILL_EXIST,
            AdapterError::InvalidAlter(_, _) => SqlState::FEATURE_NOT_SUPPORTED,
            AdapterError::ConnectionValidation(_) => SqlState::SYSTEM_ERROR,
            // `DATA_EXCEPTION`, similarly to `AbsurdSubscribeBounds`.
            AdapterError::MaterializedViewWouldNeverRefresh(_, _) => SqlState::DATA_EXCEPTION,
            AdapterError::InputNotReadableAtRefreshAtTime(_, _) => SqlState::DATA_EXCEPTION,
            AdapterError::RtrTimeout(_) => SqlState::QUERY_CANCELED,
            AdapterError::RtrDropFailure(_) => SqlState::UNDEFINED_OBJECT,
            AdapterError::UnreadableSinkCollection => SqlState::from_code("MZ009"),
            AdapterError::UserSessionsDisallowed => SqlState::from_code("MZ010"),
            AdapterError::NetworkPolicyDenied(_) => SqlState::from_code("MZ011"),
            // In read-only mode all transactions are implicitly read-only
            // transactions.
            AdapterError::ReadOnly => SqlState::READ_ONLY_SQL_TRANSACTION,
            AdapterError::AlterClusterTimeout => SqlState::QUERY_CANCELED,
            AdapterError::AlterClusterWhilePendingReplicas => SqlState::OBJECT_IN_USE,
            AdapterError::AuthenticationError(AuthenticationError::InvalidCredentials) => {
                SqlState::INVALID_PASSWORD
            }
            AdapterError::AuthenticationError(_) => SqlState::INVALID_AUTHORIZATION_SPECIFICATION,
        }
    }

    pub fn internal<E: std::fmt::Display>(context: &str, e: E) -> AdapterError {
        AdapterError::Internal(format!("{context}: {e}"))
    }

    // We don't want the following error conversions to `ConcurrentDependencyDrop` to happen
    // automatically, because it might depend on the context whether `ConcurrentDependencyDrop`
    // is appropriate, so we want to make the conversion target explicit at the call site.
    // For example, maybe we get an `InstanceMissing` if the user specifies a non-existing cluster,
    // in which case `ConcurrentDependencyDrop` would not be appropriate.
    pub fn concurrent_dependency_drop_from_instance_missing(e: InstanceMissing) -> Self {
        AdapterError::ConcurrentDependencyDrop {
            dependency_kind: "cluster",
            dependency_id: e.0.to_string(),
        }
    }
    pub fn concurrent_dependency_drop_from_collection_missing(e: CollectionMissing) -> Self {
        AdapterError::ConcurrentDependencyDrop {
            dependency_kind: "collection",
            dependency_id: e.0.to_string(),
        }
    }

    pub fn concurrent_dependency_drop_from_collection_lookup_error(
        e: CollectionLookupError,
        compute_instance: ComputeInstanceId,
    ) -> Self {
        match e {
            CollectionLookupError::InstanceMissing(id) => AdapterError::ConcurrentDependencyDrop {
                dependency_kind: "cluster",
                dependency_id: id.to_string(),
            },
            CollectionLookupError::CollectionMissing(id) => {
                AdapterError::ConcurrentDependencyDrop {
                    dependency_kind: "collection",
                    dependency_id: id.to_string(),
                }
            }
            CollectionLookupError::InstanceShutDown => AdapterError::ConcurrentDependencyDrop {
                dependency_kind: "cluster",
                dependency_id: compute_instance.to_string(),
            },
        }
    }

    pub fn concurrent_dependency_drop_from_peek_error(
        e: PeekError,
        compute_instance: ComputeInstanceId,
    ) -> AdapterError {
        match e {
            PeekError::ReplicaMissing(id) => AdapterError::ConcurrentDependencyDrop {
                dependency_kind: "replica",
                dependency_id: id.to_string(),
            },
            PeekError::InstanceShutDown => AdapterError::ConcurrentDependencyDrop {
                dependency_kind: "cluster",
                dependency_id: compute_instance.to_string(),
            },
            e @ PeekError::ReadHoldIdMismatch(_) => AdapterError::internal("peek error", e),
            e @ PeekError::ReadHoldInsufficient(_) => AdapterError::internal("peek error", e),
        }
    }

    pub fn concurrent_dependency_drop_from_dataflow_creation_error(
        e: compute_error::DataflowCreationError,
    ) -> Self {
        use compute_error::DataflowCreationError::*;
        match e {
            InstanceMissing(id) => AdapterError::ConcurrentDependencyDrop {
                dependency_kind: "cluster",
                dependency_id: id.to_string(),
            },
            CollectionMissing(id) => AdapterError::ConcurrentDependencyDrop {
                dependency_kind: "collection",
                dependency_id: id.to_string(),
            },
            ReplicaMissing(id) => AdapterError::ConcurrentDependencyDrop {
                dependency_kind: "replica",
                dependency_id: id.to_string(),
            },
            MissingAsOf | SinceViolation(..) | EmptyAsOfForSubscribe | EmptyAsOfForCopyTo => {
                AdapterError::internal("dataflow creation error", e)
            }
        }
    }
}

impl fmt::Display for AdapterError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            AdapterError::AbsurdSubscribeBounds { as_of, up_to } => {
                assert!(up_to < as_of);
                write!(
                    f,
                    r#"subscription lower ("as of") bound is beyond its upper ("up to") bound: {} < {}"#,
                    up_to, as_of
                )
            }
            AdapterError::AmbiguousSystemColumnReference => {
                write!(
                    f,
                    "cannot use wildcard expansions or NATURAL JOINs in a view that depends on \
                    system objects"
                )
            }
            AdapterError::ChangedPlan(e) => write!(f, "{}", e),
            AdapterError::Catalog(e) => e.fmt(f),
            AdapterError::DuplicateCursor(name) => {
                write!(f, "cursor {} already exists", name.quoted())
            }
            AdapterError::Eval(e) => e.fmt(f),
            AdapterError::Explain(e) => e.fmt(f),
            AdapterError::IdExhaustionError => f.write_str("ID allocator exhausted all valid IDs"),
            AdapterError::Internal(e) => write!(f, "internal error: {}", e),
            AdapterError::IntrospectionDisabled { .. } => write!(
                f,
                "cannot read log sources of replica with disabled introspection"
            ),
            AdapterError::InvalidLogDependency { object_type, .. } => {
                write!(f, "{object_type} objects cannot depend on log sources")
            }
            AdapterError::InvalidClusterReplicaAz { az, expected: _ } => {
                write!(f, "unknown cluster replica availability zone {az}",)
            }
            AdapterError::InvalidSetIsolationLevel => write!(
                f,
                "SET TRANSACTION ISOLATION LEVEL must be called before any query"
            ),
            AdapterError::InvalidSetCluster => {
                write!(f, "SET cluster cannot be called in an active transaction")
            }
            AdapterError::InvalidStorageClusterSize { size, .. } => {
                write!(f, "unknown source size {size}")
            }
            AdapterError::SourceOrSinkSizeRequired { .. } => {
                write!(f, "must specify either cluster or size option")
            }
            AdapterError::InvalidTableMutationSelection => {
                f.write_str("invalid selection: operation may only refer to user-defined tables")
            }
            AdapterError::ConstraintViolation(not_null_violation) => {
                write!(f, "{}", not_null_violation)
            }
            AdapterError::ConcurrentClusterDrop => {
                write!(f, "the transaction's active cluster has been dropped")
            }
            AdapterError::ConcurrentDependencyDrop {
                dependency_kind,
                dependency_id,
            } => {
                write!(f, "{dependency_kind} '{dependency_id}' was dropped")
            }
            AdapterError::CollectionUnreadable { id } => {
                write!(f, "collection '{id}' is not readable at any timestamp")
            }
            AdapterError::NoClusterReplicasAvailable { name, .. } => {
                write!(
                    f,
                    "CLUSTER {} has no replicas available to service request",
                    name.quoted()
                )
            }
            AdapterError::OperationProhibitsTransaction(op) => {
                write!(f, "{} cannot be run inside a transaction block", op)
            }
            AdapterError::OperationRequiresTransaction(op) => {
                write!(f, "{} can only be used in transaction blocks", op)
            }
            AdapterError::ParseError(e) => e.fmt(f),
            AdapterError::PlanError(e) => e.fmt(f),
            AdapterError::PreparedStatementExists(name) => {
                write!(f, "prepared statement {} already exists", name.quoted())
            }
            AdapterError::ReadOnlyTransaction => f.write_str("transaction in read-only mode"),
            AdapterError::SingleStatementTransaction => {
                f.write_str("this transaction can only execute a single statement")
            }
            AdapterError::ReadWriteUnavailable => {
                f.write_str("transaction read-write mode must be set before any query")
            }
            AdapterError::WrongSetOfLocks => {
                write!(f, "internal error, wrong set of locks acquired")
            }
            AdapterError::StatementTimeout => {
                write!(f, "canceling statement due to statement timeout")
            }
            AdapterError::Canceled => {
                write!(f, "canceling statement due to user request")
            }
            AdapterError::IdleInTransactionSessionTimeout => {
                write!(
                    f,
                    "terminating connection due to idle-in-transaction timeout"
                )
            }
            AdapterError::RecursionLimit(e) => e.fmt(f),
            AdapterError::RelationOutsideTimeDomain { .. } => {
                write!(
                    f,
                    "Transactions can only reference objects in the same timedomain. \
                     See https://materialize.com/docs/sql/begin/#same-timedomain-error",
                )
            }
            AdapterError::ResourceExhaustion {
                resource_type,
                limit_name,
                desired,
                limit,
                current,
            } => {
                write!(
                    f,
                    "creating {resource_type} would violate {limit_name} limit (desired: {desired}, limit: {limit}, current: {current})"
                )
            }
            AdapterError::ResultSize(e) => write!(f, "{e}"),
            AdapterError::SafeModeViolation(feature) => {
                write!(f, "cannot create {} in safe mode", feature)
            }
            AdapterError::SubscribeOnlyTransaction => {
                f.write_str("SUBSCRIBE in transactions must be the only read statement")
            }
            AdapterError::Optimizer(e) => e.fmt(f),
            AdapterError::UnallowedOnCluster {
                depends_on,
                cluster,
            } => {
                let items = depends_on.into_iter().map(|item| item.quoted()).join(", ");
                write!(
                    f,
                    "querying the following items {items} is not allowed from the {} cluster",
                    cluster.quoted()
                )
            }
            AdapterError::Unauthorized(unauthorized) => {
                write!(f, "{unauthorized}")
            }
            AdapterError::UnknownCursor(name) => {
                write!(f, "cursor {} does not exist", name.quoted())
            }
            AdapterError::UnknownLoginRole(name) => {
                write!(f, "role {} does not exist", name.quoted())
            }
            AdapterError::Unsupported(features) => write!(f, "{} are not supported", features),
            AdapterError::Unstructured(e) => write!(f, "{}", e.display_with_causes()),
            AdapterError::WriteOnlyTransaction => f.write_str("transaction in write-only mode"),
            AdapterError::UnknownPreparedStatement(name) => {
                write!(f, "prepared statement {} does not exist", name.quoted())
            }
            AdapterError::UnknownClusterReplica {
                cluster_name,
                replica_name,
            } => write!(
                f,
                "cluster replica '{cluster_name}.{replica_name}' does not exist"
            ),
            AdapterError::UnrecognizedConfigurationParam(setting_name) => write!(
                f,
                "unrecognized configuration parameter {}",
                setting_name.quoted()
            ),
            AdapterError::UntargetedLogRead { .. } => {
                f.write_str("log source reads must target a replica")
            }
            AdapterError::DDLOnlyTransaction => f.write_str(
                "transactions which modify objects are restricted to just modifying objects",
            ),
            AdapterError::DDLTransactionRace => f.write_str(
                "another session modified the catalog while this DDL transaction was open",
            ),
            AdapterError::TransactionDryRun { .. } => f.write_str("transaction dry run"),
            AdapterError::Storage(e) => e.fmt(f),
            AdapterError::Compute(e) => e.fmt(f),
            AdapterError::Orchestrator(e) => e.fmt(f),
            AdapterError::DependentObject(dependent_objects) => {
                let role_str = if dependent_objects.keys().count() == 1 {
                    "role"
                } else {
                    "roles"
                };
                write!(
                    f,
                    "{role_str} \"{}\" cannot be dropped because some objects depend on it",
                    dependent_objects.keys().join(", ")
                )
            }
            AdapterError::InvalidAlter(t, e) => {
                write!(f, "invalid ALTER {t}: {e}")
            }
            AdapterError::ConnectionValidation(e) => e.fmt(f),
            AdapterError::MaterializedViewWouldNeverRefresh(_, _) => {
                write!(
                    f,
                    "all the specified refreshes of the materialized view would be too far in the past, and thus they \
                    would never happen"
                )
            }
            AdapterError::InputNotReadableAtRefreshAtTime(_, _) => {
                write!(
                    f,
                    "REFRESH AT requested for a time where not all the inputs are readable"
                )
            }
            AdapterError::RtrTimeout(_) => {
                write!(
                    f,
                    "timed out before ingesting the source's visible frontier when real-time-recency query issued"
                )
            }
            AdapterError::RtrDropFailure(_) => write!(
                f,
                "real-time source dropped before ingesting the upstream system's visible frontier"
            ),
            AdapterError::UnreadableSinkCollection => {
                write!(f, "collection is not readable at any time")
            }
            AdapterError::UserSessionsDisallowed => write!(f, "login blocked"),
            AdapterError::NetworkPolicyDenied(_) => write!(f, "session denied"),
            AdapterError::ReadOnly => write!(f, "cannot write in read-only mode"),
            AdapterError::AlterClusterTimeout => {
                write!(f, "canceling statement, provided timeout lapsed")
            }
            AdapterError::AuthenticationError(e) => {
                write!(f, "authentication error {e}")
            }
            AdapterError::UnavailableFeature { feature, docs } => {
                write!(f, "{} is not supported in this environment.", feature)?;
                if let Some(docs) = docs {
                    write!(
                        f,
                        " For more information consult the documentation at {docs}"
                    )?;
                }
                Ok(())
            }
            AdapterError::AlterClusterWhilePendingReplicas => {
                write!(f, "cannot alter clusters with pending updates")
            }
        }
    }
}

impl From<anyhow::Error> for AdapterError {
    fn from(e: anyhow::Error) -> AdapterError {
        match e.downcast::<PlanError>() {
            Ok(plan_error) => AdapterError::PlanError(plan_error),
            Err(e) => AdapterError::Unstructured(e),
        }
    }
}

impl From<TryFromIntError> for AdapterError {
    fn from(e: TryFromIntError) -> AdapterError {
        AdapterError::Unstructured(e.into())
    }
}

impl From<TryFromDecimalError> for AdapterError {
    fn from(e: TryFromDecimalError) -> AdapterError {
        AdapterError::Unstructured(e.into())
    }
}

impl From<mz_catalog::memory::error::Error> for AdapterError {
    fn from(e: mz_catalog::memory::error::Error) -> AdapterError {
        AdapterError::Catalog(e)
    }
}

impl From<mz_catalog::durable::CatalogError> for AdapterError {
    fn from(e: mz_catalog::durable::CatalogError) -> Self {
        mz_catalog::memory::error::Error::from(e).into()
    }
}

impl From<mz_catalog::durable::DurableCatalogError> for AdapterError {
    fn from(e: mz_catalog::durable::DurableCatalogError) -> Self {
        mz_catalog::durable::CatalogError::from(e).into()
    }
}

impl From<EvalError> for AdapterError {
    fn from(e: EvalError) -> AdapterError {
        AdapterError::Eval(e)
    }
}

impl From<ExplainError> for AdapterError {
    fn from(e: ExplainError) -> AdapterError {
        match e {
            ExplainError::RecursionLimitError(e) => AdapterError::RecursionLimit(e),
            e => AdapterError::Explain(e),
        }
    }
}

impl From<mz_sql::catalog::CatalogError> for AdapterError {
    fn from(e: mz_sql::catalog::CatalogError) -> AdapterError {
        AdapterError::Catalog(mz_catalog::memory::error::Error::from(e))
    }
}

impl From<PlanError> for AdapterError {
    fn from(e: PlanError) -> AdapterError {
        match e {
            PlanError::UnknownCursor(name) => AdapterError::UnknownCursor(name),
            _ => AdapterError::PlanError(e),
        }
    }
}

impl From<OptimizerError> for AdapterError {
    fn from(e: OptimizerError) -> AdapterError {
        use OptimizerError::*;
        match e {
            PlanError(e) => Self::PlanError(e),
            RecursionLimitError(e) => Self::RecursionLimit(e),
            EvalError(e) => Self::Eval(e),
            InternalUnsafeMfpPlan(e) => Self::Internal(e),
            Internal(e) => Self::Internal(e),
            e => Self::Optimizer(e),
        }
    }
}

impl From<NotNullViolation> for AdapterError {
    fn from(e: NotNullViolation) -> AdapterError {
        AdapterError::ConstraintViolation(e)
    }
}

impl From<RecursionLimitError> for AdapterError {
    fn from(e: RecursionLimitError) -> AdapterError {
        AdapterError::RecursionLimit(e)
    }
}

impl From<oneshot::error::RecvError> for AdapterError {
    fn from(e: oneshot::error::RecvError) -> AdapterError {
        AdapterError::Unstructured(e.into())
    }
}

impl From<StorageError<mz_repr::Timestamp>> for AdapterError {
    fn from(e: StorageError<mz_repr::Timestamp>) -> Self {
        AdapterError::Storage(e)
    }
}

impl From<compute_error::InstanceExists> for AdapterError {
    fn from(e: compute_error::InstanceExists) -> Self {
        AdapterError::Compute(e.into())
    }
}

impl From<TimestampError> for AdapterError {
    fn from(e: TimestampError) -> Self {
        let e: EvalError = e.into();
        e.into()
    }
}

impl From<mz_sql_parser::parser::ParserStatementError> for AdapterError {
    fn from(e: mz_sql_parser::parser::ParserStatementError) -> Self {
        AdapterError::ParseError(e)
    }
}

impl From<VarError> for AdapterError {
    fn from(e: VarError) -> Self {
        let e: mz_catalog::memory::error::Error = e.into();
        e.into()
    }
}

impl From<rbac::UnauthorizedError> for AdapterError {
    fn from(e: rbac::UnauthorizedError) -> Self {
        AdapterError::Unauthorized(e)
    }
}

impl From<mz_sql_parser::ast::IdentError> for AdapterError {
    fn from(value: mz_sql_parser::ast::IdentError) -> Self {
        AdapterError::PlanError(PlanError::InvalidIdent(value))
    }
}

impl From<mz_pgwire_common::ConnectionError> for AdapterError {
    fn from(value: mz_pgwire_common::ConnectionError) -> Self {
        match value {
            mz_pgwire_common::ConnectionError::TooManyConnections { current, limit } => {
                AdapterError::ResourceExhaustion {
                    resource_type: "connection".into(),
                    limit_name: "max_connections".into(),
                    desired: (current + 1).to_string(),
                    limit: limit.to_string(),
                    current: current.to_string(),
                }
            }
        }
    }
}

impl From<NetworkPolicyError> for AdapterError {
    fn from(value: NetworkPolicyError) -> Self {
        AdapterError::NetworkPolicyDenied(value)
    }
}

impl From<ConnectionValidationError> for AdapterError {
    fn from(e: ConnectionValidationError) -> AdapterError {
        AdapterError::ConnectionValidation(e)
    }
}

impl Error for AdapterError {}

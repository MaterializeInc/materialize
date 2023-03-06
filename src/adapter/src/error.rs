// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::error::Error;
use std::fmt;
use std::num::TryFromIntError;

use dec::TryFromDecimalError;
use mz_repr::adt::timestamp::TimestampError;
use tokio::sync::oneshot;

use mz_compute_client::controller::error as compute_error;
use mz_expr::{EvalError, UnmaterializableFunc};
use mz_ore::stack::RecursionLimitError;
use mz_ore::str::StrExt;
use mz_repr::explain::ExplainError;
use mz_repr::NotNullViolation;
use mz_sql::names::RoleId;
use mz_sql::plan::PlanError;
use mz_storage_client::controller::StorageError;
use mz_transform::TransformError;

use crate::session::Var;
use crate::{catalog, rbac};

/// Errors that can occur in the coordinator.
#[derive(Debug)]
pub enum AdapterError {
    /// A `SUBSCRIBE` was requested whose `UP TO` bound precedes its `as_of` timestamp
    AbsurdSubscribeBounds {
        as_of: mz_repr::Timestamp,
        up_to: mz_repr::Timestamp,
    },
    /// Attempted to use a potentially ambiguous column reference expression with a system table.
    // We don't allow this until https://github.com/MaterializeInc/materialize/issues/16650 is
    // resolved because it prevents us from adding columns to system tables.
    AmbiguousSystemColumnReference,
    /// An error occurred in a catalog operation.
    Catalog(catalog::Error),
    /// The cached plan or descriptor changed.
    ChangedPlan,
    /// The specified session parameter is constrained to a finite set of values.
    ConstrainedParameter {
        parameter: &'static (dyn Var + Send + Sync),
        values: Vec<String>,
        valid_values: Option<Vec<&'static str>>,
    },
    /// The cursor already exists.
    DuplicateCursor(String),
    /// An error while evaluating an expression.
    Eval(EvalError),
    /// An error occurred while planning the statement.
    Explain(ExplainError),
    /// The specified parameter is fixed to a single specific value.
    ///
    /// We allow setting the parameter to its fixed value for compatibility
    /// with PostgreSQL-based tools.
    FixedValueParameter(&'static (dyn Var + Send + Sync)),
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
    /// The value for the specified parameter does not have the right type.
    InvalidParameterType(&'static (dyn Var + Send + Sync)),
    /// The value of the specified parameter is incorrect
    InvalidParameterValue {
        parameter: &'static (dyn Var + Send + Sync),
        values: Vec<String>,
        reason: String,
    },
    /// No such cluster replica size has been configured.
    InvalidClusterReplicaAz {
        az: String,
        expected: Vec<String>,
    },
    /// No such cluster replica size has been configured.
    InvalidClusterReplicaSize {
        size: String,
        expected: Vec<String>,
    },
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
    /// An operation attempted to modify a linked cluster.
    ModifyLinkedCluster {
        cluster_name: String,
        linked_object_name: String,
    },
    /// An operation attempted to create an illegal item in a
    /// storage-only cluster
    BadItemInStorageCluster {
        cluster_name: String,
    },
    /// Expression violated a column's constraint
    ConstraintViolation(NotNullViolation),
    /// Target cluster has no replicas to service query.
    NoClusterReplicasAvailable(String),
    /// The named operation cannot be run in a transaction.
    OperationProhibitsTransaction(String),
    /// The named operation requires an active transaction.
    OperationRequiresTransaction(String),
    /// An error occurred while planning the statement.
    PlanError(PlanError),
    /// The named prepared statement already exists.
    PreparedStatementExists(String),
    /// Wrapper around parsing error
    ParseError(mz_sql_parser::parser::ParserError),
    /// The transaction is in read-only mode.
    ReadOnlyTransaction,
    /// The specified session parameter is read-only.
    ReadOnlyParameter(&'static (dyn Var + Send + Sync)),
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
        limit: u32,
        current_amount: usize,
        new_instances: i32,
    },
    /// Result size of a query is too large.
    ResultSize(String),
    /// The specified feature is not permitted in safe mode.
    SafeModeViolation(String),
    /// Waiting on a query timed out.
    ///
    /// Note this differs slightly from PG's implementation/semantics.
    StatementTimeout,
    /// An idle session in a transaction has timed out.
    IdleInTransactionSessionTimeout,
    /// An error occurred in a SQL catalog operation.
    SqlCatalog(mz_sql::catalog::CatalogError),
    /// The transaction is in single-subscribe mode.
    SubscribeOnlyTransaction,
    /// An error occurred in the MIR stage of the optimizer.
    Transform(TransformError),
    /// A user tried to perform an action that they were unauthorized to do.
    Unauthorized(rbac::UnauthorizedError),
    /// The specified function cannot be called
    UncallableFunction {
        func: UnmaterializableFunc,
        context: &'static str,
    },
    /// The named cursor does not exist.
    UnknownCursor(String),
    /// The named role does not exist.
    UnknownLoginRole(String),
    /// The named parameter is unknown to the system.
    UnknownParameter(String),
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
    /// The specified function cannot be materialized.
    UnmaterializableFunction(UnmaterializableFunc),
    /// Attempted to create an object that has unstable dependencies.
    UnstableDependency {
        object_type: String,
        unstable_dependencies: Vec<String>,
    },
    /// Attempted to read from log sources without selecting a target replica.
    UntargetedLogRead {
        log_names: Vec<String>,
    },
    /// The transaction is in write-only mode.
    WriteOnlyTransaction,
    /// The transaction only supports single table writes
    MultiTableWriteTransaction,
    /// An error occurred in the storage layer
    Storage(mz_storage_client::controller::StorageError),
    /// An error occurred in the compute layer
    Compute(anyhow::Error),
    /// An error in the orchestrator layer
    Orchestrator(anyhow::Error),
    /// The active role was dropped while a user was logged in.
    ConcurrentRoleDrop(RoleId),
}

impl AdapterError {
    /// Reports additional details about the error, if any are available.
    pub fn detail(&self) -> Option<String> {
        match self {
            AdapterError::AmbiguousSystemColumnReference => {
                Some("This is a limitation in Materialize that will be lifted in a future release. \
                See https://github.com/MaterializeInc/materialize/issues/16650 for details.".to_string())
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
            AdapterError::UnmaterializableFunction(UnmaterializableFunc::CurrentTimestamp) => {
                Some("See: https://materialize.com/docs/sql/functions/now_and_mz_now/".into())
            }
            AdapterError::UnstableDependency { unstable_dependencies, .. } => Some(format!(
                "The object depends on the following unstable objects:\n    {}",
                unstable_dependencies.join("\n    "),
            )),
            AdapterError::PlanError(e) => e.detail(),
            AdapterError::ConcurrentRoleDrop(_) => Some("Please disconnect and re-connect with a valid role.".into()),
            AdapterError::Unauthorized(unauthorized) => Some(unauthorized.detail()),
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
            AdapterError::ConstrainedParameter {
                valid_values: Some(valid_values),
                ..
            } => Some(format!("Available values: {}.", valid_values.join(", "))),
            AdapterError::Eval(e) => e.hint(),
            AdapterError::InvalidClusterReplicaAz { expected, az: _ } => {
                Some(if expected.is_empty() {
                    "No availability zones configured; do not specify AVAILABILITY ZONE".into()
                } else {
                    format!("Valid availability zones are: {}", expected.join(", "))
                })
            }
            AdapterError::InvalidClusterReplicaSize { expected, size: _ } => Some(format!(
                "Valid cluster replica sizes are: {}",
                expected.join(", ")
            )),
            AdapterError::InvalidStorageClusterSize { expected, .. } => {
                Some(format!("Valid sizes are: {}", expected.join(", ")))
            }
            AdapterError::SourceOrSinkSizeRequired { expected } => Some(format!(
                "Try choosing one of the smaller sizes to start. Available sizes: {}",
                expected.join(", ")
            )),
            AdapterError::NoClusterReplicasAvailable(_) => {
                Some("You can create cluster replicas using CREATE CLUSTER REPLICA".into())
            }
            AdapterError::UnmaterializableFunction(UnmaterializableFunc::CurrentTimestamp) => {
                Some("Try using `mz_now()` here instead.".into())
            }
            AdapterError::UntargetedLogRead { .. } => Some(
                "Use `SET cluster_replica = <replica-name>` to target a specific replica in the \
                 active cluster. Note that subsequent queries will only be answered by \
                 the selected replica, which might reduce availability. To undo the replica \
                 selection, use `RESET cluster_replica`."
                    .into(),
            ),
            AdapterError::StatementTimeout => Some(
                "Consider increasing the maximum allowed statement duration for this session by \
                 setting the statement_timeout session variable. For example, `SET \
                 statement_timeout = '60s'`."
                    .into(),
            ),
            AdapterError::PlanError(e) => e.hint(),
            _ => None,
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
            AdapterError::ModifyLinkedCluster { cluster_name, .. } => {
                write!(f, "cannot modify linked cluster {}", cluster_name.quoted())
            }
            AdapterError::ChangedPlan => f.write_str("cached plan must not change result type"),
            AdapterError::Catalog(e) => e.fmt(f),
            AdapterError::ConstrainedParameter {
                parameter, values, ..
            } => write!(
                f,
                "invalid value for parameter {}: {}",
                parameter.name().quoted(),
                values
                    .iter()
                    .map(|v| v.quoted().to_string())
                    .collect::<Vec<_>>()
                    .join(",")
            ),
            AdapterError::DuplicateCursor(name) => {
                write!(f, "cursor {} already exists", name.quoted())
            }
            AdapterError::Eval(e) => e.fmt(f),
            AdapterError::Explain(e) => e.fmt(f),
            AdapterError::FixedValueParameter(p) => write!(
                f,
                "parameter {} can only be set to {}",
                p.name().quoted(),
                p.value().quoted()
            ),
            AdapterError::IdExhaustionError => f.write_str("ID allocator exhausted all valid IDs"),
            AdapterError::Internal(e) => write!(f, "internal error: {}", e),
            AdapterError::IntrospectionDisabled { .. } => write!(
                f,
                "cannot read log sources of replica with disabled introspection"
            ),
            AdapterError::InvalidLogDependency { object_type, .. } => {
                write!(f, "{object_type} objects cannot depend on log sources")
            }
            AdapterError::InvalidParameterType(p) => write!(
                f,
                "parameter {} requires a {} value",
                p.name().quoted(),
                p.type_name().quoted()
            ),
            AdapterError::BadItemInStorageCluster { .. } => f.write_str(
                "cannot create this kind of item in a cluster that contains sources or sinks",
            ),
            AdapterError::InvalidParameterValue {
                parameter,
                values,
                reason,
            } => write!(
                f,
                "parameter {} cannot have value {}: {}",
                parameter.name().quoted(),
                values
                    .iter()
                    .map(|v| v.quoted().to_string())
                    .collect::<Vec<_>>()
                    .join(","),
                reason,
            ),
            AdapterError::InvalidClusterReplicaAz { az, expected: _ } => {
                write!(f, "unknown cluster replica availability zone {az}",)
            }
            AdapterError::InvalidClusterReplicaSize { size, expected: _ } => {
                write!(f, "unknown cluster replica size {size}",)
            }
            AdapterError::InvalidStorageClusterSize { size, .. } => {
                write!(f, "unknown source size {size}")
            }
            AdapterError::SourceOrSinkSizeRequired { .. } => {
                write!(f, "size option is required")
            }
            AdapterError::InvalidTableMutationSelection => {
                f.write_str("invalid selection: operation may only refer to user-defined tables")
            }
            AdapterError::ConstraintViolation(not_null_violation) => {
                write!(f, "{}", not_null_violation)
            }
            AdapterError::NoClusterReplicasAvailable(cluster) => {
                write!(
                    f,
                    "CLUSTER {} has no replicas available to service request",
                    cluster.quoted()
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
            AdapterError::ReadOnlyParameter(p) => {
                write!(f, "parameter {} cannot be changed", p.name().quoted())
            }
            AdapterError::ReadWriteUnavailable => {
                f.write_str("transaction read-write mode must be set before any query")
            }
            AdapterError::StatementTimeout => {
                write!(f, "canceling statement due to statement timeout")
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
                limit,
                current_amount,
                new_instances,
            } => {
                write!(
                    f,
                    "{resource_type} resource limit of {limit} cannot be exceeded. \
                    Current amount is {current_amount} instances, tried to create \
                    {new_instances} new instances."
                )
            }
            AdapterError::ResultSize(e) => write!(f, "{e}"),
            AdapterError::SafeModeViolation(feature) => {
                write!(f, "cannot create {} in safe mode", feature)
            }
            AdapterError::SqlCatalog(e) => e.fmt(f),
            AdapterError::SubscribeOnlyTransaction => {
                f.write_str("SUBSCRIBE in transactions must be the only read statement")
            }
            AdapterError::Transform(e) => e.fmt(f),
            AdapterError::UncallableFunction { func, context } => {
                write!(f, "cannot call {} in {}", func, context)
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
            AdapterError::UnknownParameter(name) => {
                write!(f, "unrecognized configuration parameter {}", name.quoted())
            }
            AdapterError::UnmaterializableFunction(func) => {
                write!(f, "cannot materialize call to {}", func)
            }
            AdapterError::Unsupported(features) => write!(f, "{} are not supported", features),
            AdapterError::Unstructured(e) => write!(f, "{:#}", e),
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
            AdapterError::UnstableDependency { object_type, .. } => {
                write!(f, "cannot create {object_type} with unstable dependencies")
            }
            AdapterError::UntargetedLogRead { .. } => {
                f.write_str("log source reads must target a replica")
            }
            AdapterError::MultiTableWriteTransaction => {
                f.write_str("write transactions only support writes to a single table")
            }
            AdapterError::Storage(e) => e.fmt(f),
            AdapterError::Compute(e) => e.fmt(f),
            AdapterError::Orchestrator(e) => e.fmt(f),
            AdapterError::ConcurrentRoleDrop(role_id) => {
                write!(f, "role {role_id} was concurrently dropped")
            }
        }
    }
}

impl From<anyhow::Error> for AdapterError {
    fn from(e: anyhow::Error) -> AdapterError {
        match e.downcast_ref::<PlanError>() {
            Some(plan_error) => AdapterError::PlanError(plan_error.clone()),
            None => AdapterError::Unstructured(e),
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

impl From<catalog::Error> for AdapterError {
    fn from(e: catalog::Error) -> AdapterError {
        AdapterError::Catalog(e)
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
        AdapterError::SqlCatalog(e)
    }
}

impl From<PlanError> for AdapterError {
    fn from(e: PlanError) -> AdapterError {
        AdapterError::PlanError(e)
    }
}

impl From<TransformError> for AdapterError {
    fn from(e: TransformError) -> AdapterError {
        AdapterError::Transform(e)
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

impl From<StorageError> for AdapterError {
    fn from(e: StorageError) -> Self {
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

impl From<mz_sql_parser::parser::ParserError> for AdapterError {
    fn from(e: mz_sql_parser::parser::ParserError) -> Self {
        AdapterError::ParseError(e)
    }
}

impl From<rbac::UnauthorizedError> for AdapterError {
    fn from(e: rbac::UnauthorizedError) -> Self {
        AdapterError::Unauthorized(e)
    }
}

impl Error for AdapterError {}

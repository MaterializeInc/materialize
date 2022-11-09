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

use mz_compute_client::controller::ComputeError;
use mz_expr::{EvalError, UnmaterializableFunc};
use mz_ore::stack::RecursionLimitError;
use mz_ore::str::StrExt;
use mz_repr::explain_new::ExplainError;
use mz_repr::NotNullViolation;
use mz_sql::plan::PlanError;
use mz_sql::query_model::QGMError;
use mz_storage_client::controller::StorageError;
use mz_transform::TransformError;

use crate::catalog;
use crate::session::Var;

/// Errors that can occur in the coordinator.
#[derive(Debug)]
pub enum AdapterError {
    /// An error occurred in a catalog operation.
    Catalog(catalog::Error),
    /// The cached plan or descriptor changed.
    ChangedPlan,
    /// The specified session parameter is constrained to a finite set of values.
    ConstrainedParameter {
        parameter: &'static (dyn Var + Send + Sync),
        value: String,
        valid_values: Option<Vec<&'static str>>,
    },
    /// The cursor already exists.
    DuplicateCursor(String),
    /// An error while evaluating an expression.
    Eval(EvalError),
    /// An error occurred while planning the statement.
    Explain(ExplainError),
    /// The specified parameter is fixed to a single specific value.
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
        value: String,
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
    InvalidStorageHostSize {
        size: String,
        expected: Vec<String>,
    },
    StorageHostSizeRequired {
        expected: Vec<String>,
    },
    /// The selection value for a table mutation operation refers to an invalid object.
    InvalidTableMutationSelection,
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
    /// An error occurred in the QGM stage of the optimizer.
    QGM(QGMError),
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
    /// An error occurred in a SQL catalog operation.
    SqlCatalog(mz_sql::catalog::CatalogError),
    /// The transaction is in single-subscribe mode.
    SubscribeOnlyTransaction,
    /// An error occurred in the MIR stage of the optimizer.
    Transform(TransformError),
    /// A user tried to perform an action that they were unauthorized to do.
    Unauthorized(String),
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
    /// Attempted to subscribe to a log source.
    TargetedSubscribe {
        log_names: Vec<String>,
    },
    /// The transaction is in write-only mode.
    WriteOnlyTransaction,
    /// The transaction only supports single table writes
    MultiTableWriteTransaction,
    /// An error occurred in the storage layer
    Storage(mz_storage_client::controller::StorageError),
    /// An error occurred in the compute layer
    Compute(mz_compute_client::controller::ComputeError),
}

impl AdapterError {
    /// Reports additional details about the error, if any are available.
    pub fn detail(&self) -> Option<String> {
        match self {
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
            | AdapterError::UntargetedLogRead { log_names }
            | AdapterError::TargetedSubscribe { log_names } => Some(format!(
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
            _ => None,
        }
    }

    /// Reports a hint for the user about how the error could be fixed.
    pub fn hint(&self) -> Option<String> {
        match self {
            AdapterError::Catalog(c) => c.hint(),
            AdapterError::ConstrainedParameter {
                valid_values: Some(valid_values),
                ..
            } => Some(format!("Available values: {}.", valid_values.join(", "))),
            AdapterError::Eval(e) => e.hint(),
            AdapterError::UnknownLoginRole(_) => {
                // TODO(benesch): this will be a bad hint when people are used
                // to creating roles in Materialize, since they might drop the
                // default "materialize" role. Remove it in a few months
                // (say, April 2021) when folks are more used to using roles
                // with Materialize. (We don't want to do something more clever
                // and include the actual roles that exist in the message,
                // because that leaks information to unauthenticated clients.)
                Some("Try connecting as the \"materialize\" user.".into())
            }
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
            AdapterError::InvalidStorageHostSize { expected, .. } => {
                Some(format!("Valid sizes are: {}", expected.join(", ")))
            }
            Self::StorageHostSizeRequired { expected } => Some(format!(
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
                 active cluster. Note that subsequent `SELECT` queries will only be answered by \
                 the selected replica, which might reduce availability. To undo the replica \
                 selection, use `RESET cluster_replica`."
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
            AdapterError::ChangedPlan => f.write_str("cached plan must not change result type"),
            AdapterError::Catalog(e) => e.fmt(f),
            AdapterError::ConstrainedParameter {
                parameter, value, ..
            } => write!(
                f,
                "invalid value for parameter {}: {}",
                parameter.name().quoted(),
                value.quoted()
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
            AdapterError::InvalidParameterValue {
                parameter,
                value,
                reason,
            } => write!(
                f,
                "parameter {} cannot have value {}: {}",
                parameter.name().quoted(),
                value.quoted(),
                reason,
            ),
            AdapterError::InvalidClusterReplicaAz { az, expected: _ } => {
                write!(f, "unknown cluster replica availability zone {az}",)
            }
            AdapterError::InvalidClusterReplicaSize { size, expected: _ } => {
                write!(f, "unknown cluster replica size {size}",)
            }
            AdapterError::InvalidStorageHostSize { size, .. } => {
                write!(f, "unknown source size {size}")
            }
            Self::StorageHostSizeRequired { .. } => {
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
            AdapterError::PlanError(e) => e.fmt(f),
            AdapterError::PreparedStatementExists(name) => {
                write!(f, "prepared statement {} already exists", name.quoted())
            }
            AdapterError::QGM(e) => e.fmt(f),
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
            AdapterError::Unauthorized(msg) => {
                write!(f, "unauthorized: {msg}")
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
            AdapterError::UnstableDependency { object_type, .. } => {
                write!(f, "cannot create {object_type} with unstable dependencies")
            }
            AdapterError::UntargetedLogRead { .. } => {
                f.write_str("log source reads must target a replica")
            }
            AdapterError::TargetedSubscribe { .. } => {
                f.write_str("SUBSCRIBE cannot reference a log source")
            }
            AdapterError::MultiTableWriteTransaction => {
                f.write_str("write transactions only support writes to a single table")
            }
            AdapterError::Storage(e) => e.fmt(f),
            AdapterError::Compute(e) => e.fmt(f),
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

impl From<QGMError> for AdapterError {
    fn from(e: QGMError) -> AdapterError {
        AdapterError::QGM(e)
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

impl From<ComputeError> for AdapterError {
    fn from(e: ComputeError) -> Self {
        AdapterError::Compute(e)
    }
}

impl From<TimestampError> for AdapterError {
    fn from(e: TimestampError) -> Self {
        let e: EvalError = e.into();
        e.into()
    }
}

impl Error for AdapterError {}

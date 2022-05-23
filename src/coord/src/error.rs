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

use mz_dataflow_types::sources::{ExternalSourceConnector, SourceConnector};
use mz_expr::{EvalError, UnmaterializableFunc};
use mz_ore::stack::RecursionLimitError;
use mz_ore::str::StrExt;
use mz_repr::NotNullViolation;
use mz_sql::query_model::QGMError;
use mz_transform::TransformError;

use crate::catalog;
use crate::session::Var;

/// Errors that can occur in the coordinator.
#[derive(Debug)]
pub enum CoordError {
    /// Query needs AS OF <time> or indexes to succeed.
    AutomaticTimestampFailure {
        /// The names of any unmaterialized sources.
        unmaterialized: Vec<String>,
    },
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
    /// The specified parameter is fixed to a single specific value.
    FixedValueParameter(&'static (dyn Var + Send + Sync)),
    /// The ID allocator exhausted all valid IDs.
    IdExhaustionError,
    /// Unexpected internal state was encountered.
    Internal(String),
    /// Attempted to build a materialization on a source that does not allow multiple materializations
    InvalidRematerialization {
        base_source: String,
        existing_indexes: Vec<String>,
        source_type: RematerializedSourceType,
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
    /// The named prepared statement already exists.
    PreparedStatementExists(String),
    /// An error occurred in the QGM stage of the optimizer.
    QGM(QGMError),
    /// The transaction is in read-only mode.
    ReadOnlyTransaction,
    /// The specified session parameter is read-only.
    ReadOnlyParameter(&'static (dyn Var + Send + Sync)),
    /// The recursion limit of some operation was exceeded.
    RecursionLimit(RecursionLimitError),
    /// A query in a transaction referenced a relation outside the first query's
    /// time domain.
    RelationOutsideTimeDomain {
        relations: Vec<String>,
        names: Vec<String>,
    },
    /// The specified feature is not permitted in safe mode.
    SafeModeViolation(String),
    /// An error occurred in a SQL catalog operation.
    SqlCatalog(mz_sql::catalog::CatalogError),
    /// The transaction is in single-tail mode.
    TailOnlyTransaction,
    /// An error occurred in the MIR stage of the optimizer.
    Transform(TransformError),
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
    /// The transaction is in write-only mode.
    WriteOnlyTransaction,
    /// The transaction only supports single table writes
    MultiTableWriteTransaction,
    /// The transaction is in secrets-only mode.
    SecretsOnlyTransaction,
}

impl CoordError {
    /// Reports additional details about the error, if any are available.
    pub fn detail(&self) -> Option<String> {
        match self {
            CoordError::AutomaticTimestampFailure {
                unmaterialized,
            } => {

                Some(format!(
                    "The query transitively depends on the following unmaterialized sources:\n\t{}",
                        itertools::join(unmaterialized, "\n\t")
                ))
            }
            CoordError::Catalog(c) => c.detail(),
            CoordError::Eval(e) => e.detail(),
            CoordError::RelationOutsideTimeDomain { relations, names } => Some(format!(
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
            CoordError::SafeModeViolation(_) => Some(
                "The Materialize server you are connected to is running in \
                 safe mode, which limits the features that are available."
                    .into(),
            ),
            CoordError::InvalidRematerialization {
                existing_indexes, source_type, ..
            } => {
                let source_name = match source_type {
                    RematerializedSourceType::Postgres => "Postgres",
                    RematerializedSourceType::S3 => "S3 with SQS notification ",
                };
                Some(format!(
                    "{} sources can be materialized by only one set of indexes at a time. \
                     The following indexes have already materialized this source:\n    {}",
                    source_name,
                    existing_indexes.join("\n    ")))
            }
            _ => None,
        }
    }

    /// Reports a hint for the user about how the error could be fixed.
    pub fn hint(&self) -> Option<String> {
        match self {
            CoordError::AutomaticTimestampFailure {..} => {
                Some("\n- Use `SELECT ... AS OF` to manually choose a timestamp for your query.
                - Create indexes on the listed unmaterialized sources or on the views derived from those sources".into())
            }
            CoordError::Catalog(c) => c.hint(),
            CoordError::ConstrainedParameter {
                valid_values: Some(valid_values),
                ..
            } => Some(format!("Available values: {}.", valid_values.join(", "))),
            CoordError::Eval(e) => e.hint(),
            CoordError::UnknownLoginRole(_) => {
                // TODO(benesch): this will be a bad hint when people are used
                // to creating roles in Materialize, since they might drop the
                // default "materialize" role. Remove it in a few months
                // (say, April 2021) when folks are more used to using roles
                // with Materialize. (We don't want to do something more clever
                // and include the actual roles that exist in the message,
                // because that leaks information to unauthenticated clients.)
                Some("Try connecting as the \"materialize\" user.".into())
            }
            CoordError::InvalidRematerialization { source_type, .. } => {
                let doc_page = match source_type {
                    RematerializedSourceType::Postgres => "postgres",
                    RematerializedSourceType::S3 => "text-s3",
                };
                Some(format!(
                    "See the documentation at https://materialize.com/docs/sql/create-source/{}",
                    doc_page
                ))
            }
            CoordError::InvalidClusterReplicaAz { expected, az: _ } => {
                Some(if expected.is_empty() {
                    "No availability zones configured; do not specify AVAILABILITY ZONE".into()
                } else {
                    format!("Valid availability zones are: {}", expected.join(", "))
                })
            }
            CoordError::InvalidClusterReplicaSize { expected, size: _ } => Some(format!(
                "Valid cluster replica sizes are: {}",
                expected.join(", ")
            )),
            CoordError::NoClusterReplicasAvailable(_) => {
                Some("You can create cluster replicas using CREATE CLUSTER REPLICA".into())
            }
            _ => None,
        }
    }
}

impl fmt::Display for CoordError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            CoordError::AutomaticTimestampFailure { .. } => {
                f.write_str("unable to automatically determine a query timestamp")
            }
            CoordError::ChangedPlan => f.write_str("cached plan must not change result type"),
            CoordError::Catalog(e) => e.fmt(f),
            CoordError::ConstrainedParameter {
                parameter, value, ..
            } => write!(
                f,
                "invalid value for parameter {}: {}",
                parameter.name().quoted(),
                value.quoted()
            ),
            CoordError::DuplicateCursor(name) => {
                write!(f, "cursor {} already exists", name.quoted())
            }
            CoordError::Eval(e) => e.fmt(f),
            CoordError::FixedValueParameter(p) => write!(
                f,
                "parameter {} can only be set to {}",
                p.name().quoted(),
                p.value().quoted()
            ),
            CoordError::IdExhaustionError => f.write_str("ID allocator exhausted all valid IDs"),
            CoordError::Internal(e) => write!(f, "internal error: {}", e),
            CoordError::InvalidRematerialization {
                base_source,
                existing_indexes: _,
                source_type: _,
            } => {
                write!(f, "Cannot re-materialize source {}", base_source)
            }
            CoordError::InvalidParameterType(p) => write!(
                f,
                "parameter {} requires a {} value",
                p.name().quoted(),
                p.type_name().quoted()
            ),
            CoordError::InvalidParameterValue {
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
            CoordError::InvalidClusterReplicaAz { az, expected: _ } => {
                write!(f, "unknown cluster replica availability zone {az}",)
            }
            CoordError::InvalidClusterReplicaSize { size, expected: _ } => {
                write!(f, "unknown cluster replica size {size}",)
            }
            CoordError::InvalidTableMutationSelection => {
                f.write_str("invalid selection: operation may only refer to user-defined tables")
            }
            CoordError::ConstraintViolation(not_null_violation) => {
                write!(f, "{}", not_null_violation)
            }
            CoordError::NoClusterReplicasAvailable(cluster) => {
                write!(
                    f,
                    "CLUSTER {} has no replicas available to service request",
                    cluster.quoted()
                )
            }
            CoordError::OperationProhibitsTransaction(op) => {
                write!(f, "{} cannot be run inside a transaction block", op)
            }
            CoordError::OperationRequiresTransaction(op) => {
                write!(f, "{} can only be used in transaction blocks", op)
            }
            CoordError::PreparedStatementExists(name) => {
                write!(f, "prepared statement {} already exists", name.quoted())
            }
            CoordError::QGM(e) => e.fmt(f),
            CoordError::ReadOnlyTransaction => f.write_str("transaction in read-only mode"),
            CoordError::ReadOnlyParameter(p) => {
                write!(f, "parameter {} cannot be changed", p.name().quoted())
            }
            CoordError::RecursionLimit(e) => e.fmt(f),
            CoordError::RelationOutsideTimeDomain { .. } => {
                write!(
                    f,
                    "Transactions can only reference objects in the same timedomain. \
                     See https://materialize.com/docs/sql/begin/#same-timedomain-error",
                )
            }
            CoordError::SafeModeViolation(feature) => {
                write!(f, "cannot create {} in safe mode", feature)
            }
            CoordError::SqlCatalog(e) => e.fmt(f),
            CoordError::TailOnlyTransaction => {
                f.write_str("TAIL in transactions must be the only read statement")
            }
            CoordError::Transform(e) => e.fmt(f),
            CoordError::UncallableFunction { func, context } => {
                write!(f, "cannot call {} in {}", func, context)
            }
            CoordError::UnknownCursor(name) => {
                write!(f, "cursor {} does not exist", name.quoted())
            }
            CoordError::UnknownLoginRole(name) => {
                write!(f, "role {} does not exist", name.quoted())
            }
            CoordError::UnknownParameter(name) => {
                write!(f, "unrecognized configuration parameter {}", name.quoted())
            }
            CoordError::UnmaterializableFunction(func) => {
                write!(f, "cannot materialize call to {}", func)
            }
            CoordError::Unsupported(features) => write!(f, "{} are not supported", features),
            CoordError::Unstructured(e) => write!(f, "{:#}", e),
            CoordError::WriteOnlyTransaction => f.write_str("transaction in write-only mode"),
            CoordError::UnknownPreparedStatement(name) => {
                write!(f, "prepared statement {} does not exist", name.quoted())
            }
            CoordError::UnknownClusterReplica {
                cluster_name,
                replica_name,
            } => write!(
                f,
                "cluster replica '{cluster_name}.{replica_name}' does not exist"
            ),
            CoordError::MultiTableWriteTransaction => {
                f.write_str("write transactions only support writes to a single table")
            }
            CoordError::SecretsOnlyTransaction => f.write_str("transaction in secrets-only mode"),
        }
    }
}

impl From<anyhow::Error> for CoordError {
    fn from(e: anyhow::Error) -> CoordError {
        CoordError::Unstructured(e)
    }
}

impl From<TryFromIntError> for CoordError {
    fn from(e: TryFromIntError) -> CoordError {
        CoordError::Unstructured(e.into())
    }
}

impl From<TryFromDecimalError> for CoordError {
    fn from(e: TryFromDecimalError) -> CoordError {
        CoordError::Unstructured(e.into())
    }
}

impl From<catalog::Error> for CoordError {
    fn from(e: catalog::Error) -> CoordError {
        CoordError::Catalog(e)
    }
}

impl From<EvalError> for CoordError {
    fn from(e: EvalError) -> CoordError {
        CoordError::Eval(e)
    }
}

impl From<mz_sql::catalog::CatalogError> for CoordError {
    fn from(e: mz_sql::catalog::CatalogError) -> CoordError {
        CoordError::SqlCatalog(e)
    }
}

impl From<QGMError> for CoordError {
    fn from(e: QGMError) -> CoordError {
        CoordError::QGM(e)
    }
}

impl From<TransformError> for CoordError {
    fn from(e: TransformError) -> CoordError {
        CoordError::Transform(e)
    }
}

impl From<NotNullViolation> for CoordError {
    fn from(e: NotNullViolation) -> CoordError {
        CoordError::ConstraintViolation(e)
    }
}

impl From<RecursionLimitError> for CoordError {
    fn from(e: RecursionLimitError) -> CoordError {
        CoordError::RecursionLimit(e)
    }
}

impl Error for CoordError {}

/// Represent a source that is not allowed to be rematerialized
#[derive(Debug)]
pub enum RematerializedSourceType {
    Postgres,
    S3,
}

impl RematerializedSourceType {
    /// Create a RematerializedSourceType error helper
    ///
    /// # Panics
    ///
    /// If the source is of a type that is allowed to be rematerialized
    pub fn for_source(source: &catalog::Source) -> RematerializedSourceType {
        match &source.connector {
            SourceConnector::External { connector, .. } => match connector {
                ExternalSourceConnector::S3(_) => RematerializedSourceType::S3,
                ExternalSourceConnector::Postgres(_) => RematerializedSourceType::Postgres,
                _ => unreachable!(),
            },
            _ => unreachable!(),
        }
    }
}

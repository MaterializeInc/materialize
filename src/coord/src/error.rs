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

use expr::EvalError;
use ore::stack::RecursionLimitError;
use ore::str::StrExt;
use repr::NotNullViolation;
use transform::TransformError;

use crate::catalog;
use crate::session::Var;

/// Errors that can occur in the coordinator.
#[derive(Debug)]
pub enum CoordError {
    /// Query needs AS OF <time> or indexes to succeed.
    // Embeded object is meant to be of structure Vec<(Objectname, Vec<Index names w/ enabled stats>)>.
    AutomaticTimestampFailure {
        unmaterialized: Vec<String>,
        disabled_indexes: Vec<(String, Vec<String>)>,
    },
    /// An error occurred in a catalog operation.
    Catalog(catalog::Error),
    /// The cached plan or descriptor changed.
    ChangedPlan,
    /// The specified session parameter is constrained to its current value.
    ConstrainedParameter(&'static (dyn Var + Send + Sync)),
    /// The cursor already exists.
    DuplicateCursor(String),
    /// An error while evaluating an expression.
    Eval(EvalError),
    /// The ID allocator exhausted all valid IDs.
    IdExhaustionError,
    /// At least one input has no complete timestamps yet
    IncompleteTimestamp(Vec<expr::GlobalId>),
    /// Specified index is disabled, but received non-enabling update request
    InvalidAlterOnDisabledIndex(String),
    /// The value for the specified parameter does not have the right type.
    InvalidParameterType(&'static (dyn Var + Send + Sync)),
    /// The value of the specified parameter is incorrect
    InvalidParameterValue {
        parameter: &'static (dyn Var + Send + Sync),
        value: String,
        reason: String,
    },
    /// The selection value for a table mutation operation refers to an invalid object.
    InvalidTableMutationSelection,
    /// Expression violated a column's constraint
    ConstraintViolation(NotNullViolation),
    /// The named operation cannot be run in a transaction.
    OperationProhibitsTransaction(String),
    /// The named operation requires an active transaction.
    OperationRequiresTransaction(String),
    /// The named prepared statement already exists.
    PreparedStatementExists(String),
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
    SqlCatalog(sql::catalog::CatalogError),
    /// The transaction is in single-tail mode.
    TailOnlyTransaction,
    /// An error occurred in the optimizer.
    Transform(TransformError),
    /// The named cursor does not exist.
    UnknownCursor(String),
    /// The named role does not exist.
    UnknownLoginRole(String),
    /// The named parameter is unknown to the system.
    UnknownParameter(String),
    UnknownPreparedStatement(String),
    /// A generic error occurred.
    //
    // TODO(benesch): convert all those errors to structured errors.
    Unstructured(anyhow::Error),
    /// The named feature is not supported and will (probably) not be.
    Unsupported(&'static str),
    /// The transaction is in write-only mode.
    WriteOnlyTransaction,
}

impl CoordError {
    /// Reports additional details about the error, if any are available.
    pub fn detail(&self) -> Option<String> {
        match self {
            CoordError::AutomaticTimestampFailure {
                unmaterialized,
                disabled_indexes,
            } => {
                let unmaterialized_err = if unmaterialized.is_empty() {
                    "".into()
                } else {
                    format!(
                        "\nUnmaterialized sources:\n\t{}",
                        itertools::join(unmaterialized, "\n\t")
                    )
                };

                let disabled_indexes_err = if disabled_indexes.is_empty() {
                    "".into()
                } else {
                    let d = disabled_indexes.iter().fold(
                        String::default(),
                        |acc, (object_name, disabled_indexes)| {
                            format!(
                                "{}\n\n\t{}\n\tDisabled indexes:\n\t\t{}",
                                acc,
                                object_name,
                                itertools::join(disabled_indexes, "\n\t\t")
                            )
                        },
                    );
                    format!("\nSources w/ disabled indexes:{}", d)
                };

                Some(format!(
                    "The query transitively depends on the following:{}{}",
                    unmaterialized_err, disabled_indexes_err
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
            _ => None,
        }
    }

    /// Reports a hint for the user about how the error could be fixed.
    pub fn hint(&self) -> Option<String> {
        match self {
            CoordError::AutomaticTimestampFailure {
                unmaterialized,
                disabled_indexes,
            } => {
                let unmaterialized_hint = if unmaterialized.is_empty() {
                    ""
                } else {
                    "\n- Use `SELECT ... AS OF` to manually choose a timestamp for your query.
- Create indexes on the listed unmaterialized sources or on the views derived from those sources"
                };
                let disabled_indexes_hint = if disabled_indexes.is_empty() {
                    ""
                } else {
                    "ALTER INDEX ... SET ENABLED to enable indexes"
                };

                Some(format!(
                    "{}{}{}",
                    unmaterialized_hint,
                    if !unmaterialized_hint.is_empty() {
                        "\n-"
                    } else {
                        ""
                    },
                    disabled_indexes_hint
                ))
            }
            CoordError::Catalog(c) => c.hint(),
            CoordError::Eval(e) => e.hint(),
            CoordError::InvalidAlterOnDisabledIndex(idx) => Some(format!(
                "To perform this ALTER, first enable the index using ALTER \
                INDEX {} SET ENABLED",
                idx.quoted()
            )),
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
            CoordError::ConstrainedParameter(p) => write!(
                f,
                "parameter {} can only be set to {}",
                p.name().quoted(),
                p.value().quoted()
            ),
            CoordError::DuplicateCursor(name) => {
                write!(f, "cursor {} already exists", name.quoted())
            }
            CoordError::Eval(e) => e.fmt(f),
            CoordError::IdExhaustionError => f.write_str("ID allocator exhausted all valid IDs"),
            CoordError::IncompleteTimestamp(unstarted) => write!(
                f,
                "At least one input has no complete timestamps yet: {:?}",
                unstarted
            ),
            CoordError::InvalidAlterOnDisabledIndex(name) => {
                write!(f, "invalid ALTER on disabled index {}", name.quoted())
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
            CoordError::InvalidTableMutationSelection => {
                f.write_str("invalid selection: operation may only refer to user-defined tables")
            }
            CoordError::ConstraintViolation(not_null_violation) => {
                write!(f, "{}", not_null_violation)
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
            CoordError::ReadOnlyTransaction => f.write_str("transaction in read-only mode"),
            CoordError::ReadOnlyParameter(p) => {
                write!(f, "parameter {} cannot be changed", p.name().quoted())
            }
            CoordError::RecursionLimit(e) => e.fmt(f),
            CoordError::RelationOutsideTimeDomain { .. } => {
                write!(
                    f,
                    "Transactions can only reference objects in the same timedomain. See {}",
                    "https://materialize.com/docs/sql/begin/#same-timedomain-error",
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
            CoordError::UnknownCursor(name) => {
                write!(f, "cursor {} does not exist", name.quoted())
            }
            CoordError::UnknownLoginRole(name) => {
                write!(f, "role {} does not exist", name.quoted())
            }
            CoordError::UnknownParameter(name) => {
                write!(f, "unrecognized configuration parameter {}", name.quoted())
            }
            CoordError::Unsupported(features) => write!(f, "{} are not supported", features),
            CoordError::Unstructured(e) => write!(f, "{:#}", e),
            CoordError::WriteOnlyTransaction => f.write_str("transaction in write-only mode"),
            CoordError::UnknownPreparedStatement(name) => {
                write!(f, "prepared statement {} does not exist", name.quoted())
            }
        }
    }
}

impl From<anyhow::Error> for CoordError {
    fn from(e: anyhow::Error) -> CoordError {
        CoordError::Unstructured(e)
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

impl From<sql::catalog::CatalogError> for CoordError {
    fn from(e: sql::catalog::CatalogError) -> CoordError {
        CoordError::SqlCatalog(e)
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

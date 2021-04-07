// Copyright Materialize, Inc. All rights reserved.
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
use ore::str::StrExt;
use transform::TransformError;

use crate::catalog;
use crate::session::Var;

/// Errors that can occur in the coordinator.
#[derive(Debug)]
pub enum CoordError {
    /// An error occurred in a catalog operation.
    Catalog(catalog::Error),
    /// The specified session parameter is constrained to its current value.
    ConstrainedParameter(&'static (dyn Var + Send + Sync)),
    /// The cursor already exists.
    DuplicateCursor(String),
    /// An error while evaluating an expression.
    Eval(EvalError),
    /// The ID allocator exhausted all valid IDs.
    IdExhaustionError,
    /// The value for the specified parameter does not have the right type.
    InvalidParameterType(&'static (dyn Var + Send + Sync)),
    /// The named operation cannot be run in a transaction.
    OperationProhibitsTransaction(String),
    /// The named operation requires an active transaction.
    OperationRequiresTransaction(String),
    /// The transaction is in read-only mode.
    ReadOnlyTransaction,
    /// The specified session parameter is read-only.
    ReadOnlyParameter(&'static (dyn Var + Send + Sync)),
    /// The specified feature is not permitted in safe mode.
    SafeModeViolation(String),
    /// An error occurred in a SQL catalog operation.
    SqlCatalog(sql::catalog::CatalogError),
    /// An error occurred in the optimizer.
    Transform(TransformError),
    /// The named cursor does not exist.
    UnknownCursor(String),
    /// The named role does not exist.
    UnknownLoginRole(String),
    /// The named parameter is unknown to the system.
    UnknownParameter(String),
    /// A generic error occurred.
    //
    // TODO(benesch): convert all those errors to structured errors.
    Unstructured(anyhow::Error),
    /// The transaction is in write-only mode.
    WriteOnlyTransaction,
}

impl CoordError {
    /// Reports additional details about the error, if any are available.
    pub fn detail(&self) -> Option<String> {
        match self {
            CoordError::Catalog(c) => c.detail(),
            CoordError::Eval(e) => e.detail(),
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
            CoordError::Catalog(c) => c.hint(),
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
            _ => None,
        }
    }
}

impl fmt::Display for CoordError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
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
            CoordError::InvalidParameterType(p) => write!(
                f,
                "parameter {} requires a {} value",
                p.name().quoted(),
                p.type_name().quoted()
            ),
            CoordError::OperationProhibitsTransaction(op) => {
                write!(f, "{} cannot be run inside a transaction block", op)
            }
            CoordError::OperationRequiresTransaction(op) => {
                write!(f, "{} can only be used in transaction blocks", op)
            }
            CoordError::ReadOnlyTransaction => f.write_str("transaction in read-only mode"),
            CoordError::ReadOnlyParameter(p) => {
                write!(f, "parameter {} cannot be changed", p.name().quoted())
            }
            CoordError::SafeModeViolation(feature) => {
                write!(f, "cannot create {} in safe mode", feature)
            }
            CoordError::SqlCatalog(e) => e.fmt(f),
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
            CoordError::Unstructured(e) => write!(f, "{:#}", e),
            CoordError::WriteOnlyTransaction => f.write_str("transaction in write-only mode"),
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

impl Error for CoordError {}

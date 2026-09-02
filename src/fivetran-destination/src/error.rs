// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::borrow::Cow;
use std::fmt;

use crate::fivetran_sdk::Table;

/// Error that gets returned by internal operations of our Fivetran connector.
///
/// Note: We made our own error kind instead of using `anyhow::Error` because we want to internally
/// retry specific failures. Given a lot of the operations run by the connector are SQL queries, we
/// also want to track the context of where these queries were run, hence this relatively complex
/// error type.
#[derive(Debug)]
pub struct OpError {
    kind: OpErrorKind,
    context: Vec<Cow<'static, str>>,
}

impl OpError {
    pub fn new(kind: OpErrorKind) -> Self {
        OpError {
            kind,
            context: Vec::new(),
        }
    }

    pub fn kind(&self) -> &OpErrorKind {
        &self.kind
    }
}

impl<T: Into<OpErrorKind>> From<T> for OpError {
    fn from(value: T) -> Self {
        OpError::new(value.into())
    }
}

impl fmt::Display for OpError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.kind)?;
        for cause in &self.context {
            write!(f, ": {}", cause)?;
        }
        Ok(())
    }
}

#[derive(Debug, thiserror::Error)]
pub enum OpErrorKind {
    #[error("the required field '{0}' is missing")]
    FieldMissing(&'static str),
    #[error("failed to create a temporary resource: {0:?}")]
    TemporaryResource(tokio_postgres::Error),
    #[error("internal invariant was violated: {0}")]
    InvariantViolated(String),
    #[error("error when calling Materialize: {0:?}")]
    MaterializeError(#[from] tokio_postgres::Error),
    #[error("postgres type error: {0:?}")]
    PgTypeError(#[from] mz_pgrepr::TypeFromOidError),
    #[error("user lacks privilege \"{privilege}\" on object \"{object}\"")]
    MissingPrivilege {
        privilege: &'static str,
        object: String,
    },
    #[error("filesystem error: {0:?}")]
    Filesystem(#[from] std::io::Error),
    #[error("cryptography error: {0:?}")]
    Crypto(#[from] openssl::error::ErrorStack),
    #[error("failure when parsing csv: {0:?}")]
    CsvReader(#[from] csv_async::Error),
    #[error("failed to map csv to table: {msg}\ntable: {table:?}\ncsv headers: {headers:?}")]
    CsvMapping {
        headers: csv_async::StringRecord,
        table: Table,
        msg: String,
    },
    #[error("this feature is unsupported: {0}")]
    Unsupported(String),
    #[error("invalid identifier: {0:?}")]
    IdentError(#[from] mz_sql_parser::ast::IdentError),
    #[error("unknown table: {database}.{schema}.{table}")]
    UnknownTable {
        database: String,
        schema: String,
        table: String,
    },
    #[error("unknown request: {0}")]
    UnknownRequest(String),
}

impl OpErrorKind {
    /// Returns whether or not the error should be retried.
    pub fn can_retry(&self) -> bool {
        use OpErrorKind::*;

        match self {
            e @ TemporaryResource(err) | e @ MaterializeError(err) => {
                use tokio_postgres::error::SqlState;

                let Some(db_error) = err.as_db_error() else {
                    return false;
                };

                // Note: This list was chosen fairly conservatively, feel free to add more errors
                // as seen fit.
                match *db_error.code() {
                    SqlState::CONNECTION_EXCEPTION
                    | SqlState::CONNECTION_FAILURE
                    | SqlState::CONNECTION_DOES_NOT_EXIST
                    | SqlState::SQLCLIENT_UNABLE_TO_ESTABLISH_SQLCONNECTION
                    | SqlState::TRANSACTION_RESOLUTION_UNKNOWN => true,
                    SqlState::DUPLICATE_OBJECT | SqlState::DUPLICATE_TABLE
                        if matches!(e, TemporaryResource(_)) =>
                    {
                        true
                    }
                    _ => false,
                }
            }
            e @ Filesystem(_) | e @ CsvReader(_) => {
                use std::io::ErrorKind::*;

                let io_error_kind = match e {
                    Filesystem(err) => err.kind(),
                    CsvReader(err) => match err.kind() {
                        csv_async::ErrorKind::Io(io_err) => io_err.kind(),
                        _ => return false,
                    },
                    e => unreachable!("programming error, {e:?} should have been matched above"),
                };

                // Note: We don't expect to receive the network related errors, but for
                // completeness we include them here.
                match io_error_kind {
                    NotFound | ConnectionReset | ConnectionAborted | BrokenPipe | TimedOut
                    | Interrupted | UnexpectedEof => true,
                    _ => false,
                }
            }
            FieldMissing(_)
            | InvariantViolated(_)
            | PgTypeError(_)
            | MissingPrivilege { .. }
            | Crypto(_)
            | CsvMapping { .. }
            | Unsupported(_)
            | IdentError(_)
            | UnknownTable { .. }
            | UnknownRequest(_) => false,
        }
    }
}

pub trait Context {
    type TransformType;

    fn context(self, context: &'static str) -> Self::TransformType;
    fn with_context(self, f: impl FnOnce() -> String) -> Self::TransformType;
}

impl Context for OpErrorKind {
    type TransformType = OpError;

    fn context(self, context: &'static str) -> Self::TransformType {
        OpError {
            kind: self,
            context: vec![context.into()],
        }
    }

    fn with_context(self, f: impl FnOnce() -> String) -> Self::TransformType {
        OpError {
            kind: self,
            context: vec![f().into()],
        }
    }
}

impl<T> Context for Result<T, OpError> {
    type TransformType = Result<T, OpError>;

    fn context(mut self, context: &'static str) -> Self::TransformType {
        if let Err(err) = &mut self {
            err.context.push(context.into());
        }
        self
    }

    fn with_context(mut self, f: impl FnOnce() -> String) -> Self::TransformType {
        if let Err(err) = &mut self {
            err.context.push(f().into());
        }
        self
    }
}

impl<T, E: Into<OpErrorKind>> Context for Result<T, E> {
    type TransformType = Result<T, OpError>;

    fn context(self, context: &'static str) -> Self::TransformType {
        self.map_err(|err| {
            let kind: OpErrorKind = err.into();
            OpError {
                kind,
                context: vec![context.into()],
            }
        })
    }

    fn with_context(self, f: impl FnOnce() -> String) -> Self::TransformType {
        self.map_err(|err| {
            let kind: OpErrorKind = err.into();
            OpError {
                kind,
                context: vec![f().into()],
            }
        })
    }
}

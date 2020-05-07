// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::fmt;

use backtrace::Backtrace;

#[derive(Debug)]
pub struct Error {
    kind: ErrorKind,
    backtrace: Backtrace,
}

#[derive(Debug)]
pub(crate) enum ErrorKind {
    Corruption { detail: String },
    IdExhaustion,
    UnknownDatabase(String),
    UnknownSchema(String),
    UnknownItem(String),
    DatabaseAlreadyExists(String),
    SchemaAlreadyExists(String),
    ItemAlreadyExists(String),
    UnacceptableSchemaName(String),
    ReadOnlySystemSchema(String),
    UnsatisfiableLoggingDependency { depender_name: String },
    Storage(rusqlite::Error),
}

impl Error {
    pub(crate) fn new(kind: ErrorKind) -> Error {
        Error {
            kind,
            backtrace: Backtrace::new_unresolved(),
        }
    }
}

impl From<rusqlite::Error> for Error {
    fn from(e: rusqlite::Error) -> Error {
        Error::new(ErrorKind::Storage(e))
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match &self.kind {
            ErrorKind::Corruption { .. }
            | ErrorKind::IdExhaustion
            | ErrorKind::UnknownDatabase(_)
            | ErrorKind::UnknownSchema(_)
            | ErrorKind::UnknownItem(_)
            | ErrorKind::DatabaseAlreadyExists(_)
            | ErrorKind::SchemaAlreadyExists(_)
            | ErrorKind::ItemAlreadyExists(_)
            | ErrorKind::UnacceptableSchemaName(_)
            | ErrorKind::ReadOnlySystemSchema(_)
            | ErrorKind::UnsatisfiableLoggingDependency { .. } => None,
            ErrorKind::Storage(e) => Some(e),
        }
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match &self.kind {
            ErrorKind::Corruption { detail } => write!(f, "corrupt catalog: {}", detail),
            ErrorKind::IdExhaustion => write!(f, "id counter overflows i64"),
            ErrorKind::UnknownDatabase(name) => write!(f, "unknown database '{}'", name),
            ErrorKind::UnknownSchema(name) => write!(f, "unknown schema '{}'", name),
            ErrorKind::UnknownItem(name) => write!(f, "unknown catalog item '{}'", name),
            ErrorKind::DatabaseAlreadyExists(name) => {
                write!(f, "database '{}' already exists", name)
            }
            ErrorKind::SchemaAlreadyExists(name) => write!(f, "schema '{}' already exists", name),
            ErrorKind::ItemAlreadyExists(name) => {
                write!(f, "catalog item '{}' already exists", name)
            }
            ErrorKind::UnacceptableSchemaName(name) => {
                write!(f, "unacceptable schema name '{}'", name)
            }
            ErrorKind::ReadOnlySystemSchema(name) => {
                write!(f, "system schema '{}' cannot be modified", name)
            }
            ErrorKind::UnsatisfiableLoggingDependency { depender_name } => write!(
                f,
                "catalog item '{}' depends on system logging, but logging is disabled",
                depender_name
            ),
            ErrorKind::Storage(e) => write!(f, "sqlite error: {}", e),
        }
    }
}

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
    pub(crate) kind: ErrorKind,
    pub(crate) backtrace: Backtrace,
}

#[derive(Debug)]
pub(crate) enum ErrorKind {
    Corruption {
        detail: String,
    },
    IdExhaustion,
    UnknownDatabase(String),
    UnknownSchema(String),
    UnknownItem(String),
    DatabaseAlreadyExists(String),
    SchemaAlreadyExists(String),
    ItemAlreadyExists(String),
    UnacceptableSchemaName(String),
    ReadOnlySystemSchema(String),
    SchemaNotEmpty(String),
    InvalidTemporaryDependency(String),
    InvalidTemporarySchema,
    UnsatisfiableLoggingDependency {
        depender_name: String,
    },
    Storage(rusqlite::Error),
    AmbiguousRename {
        depender: String,
        dependee: String,
        message: String,
    },
    ExperimentalModeRequired,
    ExperimentalModeUnavailable,
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
            | ErrorKind::SchemaNotEmpty(_)
            | ErrorKind::InvalidTemporaryDependency(_)
            | ErrorKind::InvalidTemporarySchema
            | ErrorKind::UnsatisfiableLoggingDependency { .. }
            | ErrorKind::AmbiguousRename { .. }
            | ErrorKind::ExperimentalModeRequired
            | ErrorKind::ExperimentalModeUnavailable => None,
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
            ErrorKind::SchemaNotEmpty(name) => write!(f, "cannot drop non-empty schema '{}'", name),
            ErrorKind::InvalidTemporaryDependency(name) => write!(
                f,
                "non-temporary items cannot depend on temporary item '{}'",
                name
            ),
            ErrorKind::InvalidTemporarySchema => {
                write!(f, "cannot create temporary item in non-temporary schema")
            }
            ErrorKind::UnsatisfiableLoggingDependency { depender_name } => write!(
                f,
                "catalog item '{}' depends on system logging, but logging is disabled",
                depender_name
            ),
            ErrorKind::Storage(e) => write!(f, "sqlite error: {}", e),
            ErrorKind::AmbiguousRename {
                depender,
                dependee,
                message,
            } => {
                if depender == dependee {
                    write!(f, "renaming conflict: in {}, {}", dependee, message)
                } else {
                    write!(
                        f,
                        "renaming conflict: in {}, which uses {}, {}",
                        depender, dependee, message
                    )
                }
            }
            ErrorKind::ExperimentalModeRequired => write!(
                f,
                r#"Materialize previously started with --experimental to
enable experimental features, so now must be started in experimental
mode. For more details, see
https://materialize.io/docs/cli#experimental-mode"#
            ),
            ErrorKind::ExperimentalModeUnavailable => write!(
                f,
                r#"Experimental mode is only available on new nodes. For
more details, see https://materialize.io/docs/cli#experimental-mode"#
            ),
        }
    }
}

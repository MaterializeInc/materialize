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

use ore::str::StrExt;
use sql::catalog::CatalogError as SqlCatalogError;

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
    OidExhaustion,
    Sql(SqlCatalogError),
    DatabaseAlreadyExists(String),
    SchemaAlreadyExists(String),
    RoleAlreadyExists(String),
    ItemAlreadyExists(String),
    ReservedSchemaName(String),
    ReservedRoleName(String),
    ReadOnlySystemSchema(String),
    ReadOnlyItem(String),
    SchemaNotEmpty(String),
    InvalidTemporaryDependency(String),
    InvalidTemporarySchema,
    MandatoryTableIndex(String),
    UnsatisfiableLoggingDependency {
        depender_name: String,
    },
    Storage(rusqlite::Error),
    AmbiguousRename {
        depender: String,
        dependee: String,
        message: String,
    },
    TypeRename(String),
    ExperimentalModeRequired,
    ExperimentalModeUnavailable,
    FailedMigration {
        last_version: usize,
        cause: String,
    },
}

impl Error {
    pub(crate) fn new(kind: ErrorKind) -> Error {
        Error {
            kind,
            backtrace: Backtrace::new_unresolved(),
        }
    }

    /// Reports additional details about the error, if any are available.
    pub fn detail(&self) -> Option<String> {
        match &self.kind {
            ErrorKind::ReservedSchemaName(_) => {
                Some("The prefixes \"mz_\" and \"pg_\" are reserved for system schemas.".into())
            }
            ErrorKind::ReservedRoleName(_) => {
                Some("The prefixes \"mz_\" and \"pg_\" are reserved for system roles.".into())
            }
            _ => None,
        }
    }

    /// Reports a hint for the user about how the error could be fixed.
    pub fn hint(&self) -> Option<String> {
        None
    }
}

impl From<rusqlite::Error> for Error {
    fn from(e: rusqlite::Error) -> Error {
        Error::new(ErrorKind::Storage(e))
    }
}

impl From<SqlCatalogError> for Error {
    fn from(e: SqlCatalogError) -> Error {
        Error::new(ErrorKind::Sql(e))
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match &self.kind {
            ErrorKind::Corruption { .. }
            | ErrorKind::IdExhaustion
            | ErrorKind::OidExhaustion
            | ErrorKind::DatabaseAlreadyExists(_)
            | ErrorKind::SchemaAlreadyExists(_)
            | ErrorKind::RoleAlreadyExists(_)
            | ErrorKind::ItemAlreadyExists(_)
            | ErrorKind::ReservedSchemaName(_)
            | ErrorKind::ReservedRoleName(_)
            | ErrorKind::ReadOnlySystemSchema(_)
            | ErrorKind::ReadOnlyItem(_)
            | ErrorKind::SchemaNotEmpty(_)
            | ErrorKind::InvalidTemporaryDependency(_)
            | ErrorKind::InvalidTemporarySchema
            | ErrorKind::MandatoryTableIndex(_)
            | ErrorKind::UnsatisfiableLoggingDependency { .. }
            | ErrorKind::AmbiguousRename { .. }
            | ErrorKind::TypeRename(_)
            | ErrorKind::ExperimentalModeRequired
            | ErrorKind::ExperimentalModeUnavailable
            | ErrorKind::FailedMigration { .. } => None,
            ErrorKind::Sql(e) => Some(e),
            ErrorKind::Storage(e) => Some(e),
        }
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match &self.kind {
            ErrorKind::Corruption { detail } => write!(f, "corrupt catalog: {}", detail),
            ErrorKind::IdExhaustion => write!(f, "id counter overflows i64"),
            ErrorKind::OidExhaustion => write!(f, "oid counter overflows u32"),
            ErrorKind::Sql(e) => write!(f, "{}", e),
            ErrorKind::DatabaseAlreadyExists(name) => {
                write!(f, "database '{}' already exists", name)
            }
            ErrorKind::SchemaAlreadyExists(name) => write!(f, "schema '{}' already exists", name),
            ErrorKind::RoleAlreadyExists(name) => {
                write!(f, "role '{}' already exists", name)
            }
            ErrorKind::ItemAlreadyExists(name) => {
                write!(f, "catalog item '{}' already exists", name)
            }
            ErrorKind::ReservedSchemaName(name) => {
                write!(f, "unacceptable schema name '{}'", name)
            }
            ErrorKind::ReservedRoleName(name) => {
                write!(f, "role name {} is reserved", name.quoted())
            }
            ErrorKind::ReadOnlySystemSchema(name) => {
                write!(f, "system schema '{}' cannot be modified", name)
            }
            ErrorKind::ReadOnlyItem(name) => write!(f, "system item '{}' cannot be modified", name),
            ErrorKind::SchemaNotEmpty(name) => write!(f, "cannot drop non-empty schema '{}'", name),
            ErrorKind::InvalidTemporaryDependency(name) => write!(
                f,
                "non-temporary items cannot depend on temporary item '{}'",
                name
            ),
            ErrorKind::InvalidTemporarySchema => {
                write!(f, "cannot create temporary item in non-temporary schema")
            }
            ErrorKind::MandatoryTableIndex(index_name) => write!(
                f,
                "cannot drop '{}' as it is the default index for a table",
                index_name
            ),
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
            ErrorKind::TypeRename(typ) => write!(f, "cannot rename type: {}", typ),
            ErrorKind::ExperimentalModeRequired => write!(
                f,
                r#"Materialize previously started with --experimental to
enable experimental features, so now must be started in experimental
mode. For more details, see
https://materialize.com/docs/cli#experimental-mode"#
            ),
            ErrorKind::ExperimentalModeUnavailable => write!(
                f,
                r#"Experimental mode is only available on new nodes. For
more details, see https://materialize.com/docs/cli#experimental-mode"#
            ),
            ErrorKind::FailedMigration {
                last_version,
                cause,
            } => write!(
                f,
                "migration from catalog content version {} failed: {}",
                last_version, cause,
            ),
        }
    }
}

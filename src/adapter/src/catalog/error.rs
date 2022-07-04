// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::fmt;

use mz_ore::str::StrExt;
use mz_sql::catalog::CatalogError as SqlCatalogError;

#[derive(Debug, thiserror::Error)]
#[error(transparent)]
pub struct Error {
    #[from]
    pub(crate) kind: ErrorKind,
}

#[derive(Debug, thiserror::Error)]
pub enum ErrorKind {
    #[error("corrupt catalog: {detail}")]
    Corruption { detail: String },
    #[error("id counter overflows i64")]
    IdExhaustion,
    #[error("oid counter overflows i64")]
    OidExhaustion,
    #[error(transparent)]
    Sql(#[from] SqlCatalogError),
    #[error("database '{0}' already exists")]
    DatabaseAlreadyExists(String),
    #[error("schema '{0}' already exists")]
    SchemaAlreadyExists(String),
    #[error("role '{0}' already exists")]
    RoleAlreadyExists(String),
    #[error("cluster '{0}' already exists")]
    ClusterAlreadyExists(String),
    #[error("cannot create multiple replicas named '{0}' on cluster '{1}'")]
    DuplicateReplica(String, String),
    #[error("catalog item '{0}' already exists")]
    ItemAlreadyExists(String),
    #[error("unacceptable schema name '{0}'")]
    ReservedSchemaName(String),
    #[error("role name {} is reserved", .0.quoted())]
    ReservedRoleName(String),
    #[error("cluster name {} is reserved", .0.quoted())]
    ReservedClusterName(String),
    #[error("replica name {} is reserved", .0.quoted())]
    ReservedReplicaName(String),
    #[error("system schema '{0}' cannot be modified")]
    ReadOnlySystemSchema(String),
    #[error("system item '{0}' cannot be modified")]
    ReadOnlyItem(String),
    #[error("cannot drop non-empty schema '{0}'")]
    SchemaNotEmpty(String),
    #[error("non-temporary items cannot depend on temporary item '{0}'")]
    InvalidTemporaryDependency(String),
    #[error("cannot create temporary item in non-temporary schema")]
    InvalidTemporarySchema,
    #[error("catalog item '{depender_name}' depends on system logging, but logging is disabled")]
    UnsatisfiableLoggingDependency { depender_name: String },
    #[error("sqlite error: {0}")]
    Storage(#[from] rusqlite::Error),
    #[error(transparent)]
    AmbiguousRename(#[from] AmbiguousRename),
    #[error("cannot rename type: {0}")]
    TypeRename(String),
    #[error("cannot migrate from catalog version {last_seen_version} to version {this_version} (earlier versions might still work): {cause}")]
    FailedMigration {
        last_seen_version: String,
        this_version: &'static str,
        cause: String,
    },
    #[error("failpoint {0} reached)")]
    FailpointReached(String),
    #[error("{0}")]
    Unstructured(String),
    #[error(transparent)]
    Stash(#[from] mz_stash::StashError),
    #[error("stash in unexpected state")]
    UnexpectedStashState,
    #[error(transparent)]
    Uuid(#[from] uuid::Error),
}

impl Error {
    pub(crate) fn new(kind: ErrorKind) -> Error {
        Error { kind }
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
            ErrorKind::ReservedClusterName(_) => {
                Some("The prefixes \"mz_\" and \"pg_\" are reserved for system clusters.".into())
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
        Error::new(ErrorKind::from(e))
    }
}

impl From<SqlCatalogError> for Error {
    fn from(e: SqlCatalogError) -> Error {
        Error::new(ErrorKind::from(e))
    }
}

impl From<mz_stash::StashError> for Error {
    fn from(e: mz_stash::StashError) -> Error {
        Error::new(ErrorKind::from(e))
    }
}

impl From<uuid::Error> for Error {
    fn from(e: uuid::Error) -> Error {
        Error::new(ErrorKind::from(e))
    }
}

#[derive(Debug)]
pub struct AmbiguousRename {
    pub depender: String,
    pub dependee: String,
    pub message: String,
}

// Implement `Display` for `MinMax`.
impl fmt::Display for AmbiguousRename {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        if self.depender == self.dependee {
            write!(
                f,
                "renaming conflict: in {}, {}",
                self.dependee, self.message
            )
        } else {
            write!(
                f,
                "renaming conflict: in {}, which uses {}, {}",
                self.depender, self.dependee, self.message
            )
        }
    }
}

impl std::error::Error for AmbiguousRename {
    // Explicitly no source for this kind of error
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        None
    }
}

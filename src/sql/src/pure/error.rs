// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use mz_repr::adt::system::Oid;
use mz_sql_parser::ast::display::AstDisplay;
use mz_sql_parser::ast::{ReferencedSubsources, UnresolvedItemName};

use crate::names::{FullItemName, PartialItemName};
use crate::pure::Aug;

/// Logical errors detectable during purification for a POSTGRES SOURCE.
#[derive(Debug, Clone, thiserror::Error)]
pub enum PgSourcePurificationError {
    #[error("CREATE SOURCE specifies DETAILS option")]
    UserSpecifiedDetails,
    #[error("PUBLICATION {0} is empty")]
    EmptyPublication(String),
    #[error("database {database} missing referenced schemas")]
    DatabaseMissingFilteredSchemas {
        database: String,
        schemas: Vec<String>,
    },
    #[error("missing TABLES specification")]
    RequiresReferencedSubsources,
    #[error("multiple subsources would be named {name}")]
    SubsourceNameConflict {
        name: UnresolvedItemName,
        upstream_references: Vec<UnresolvedItemName>,
    },
    #[error("multiple subsources refer to table {name}")]
    SubsourceDuplicateReference {
        name: UnresolvedItemName,
        target_names: Vec<UnresolvedItemName>,
    },
    /// This is the ALTER SOURCE version of [`Self::SubsourceDuplicateReference`].
    #[error("another subsource already refers to {name}")]
    SubsourceAlreadyReferredTo { name: UnresolvedItemName },
    #[error("insufficient privileges")]
    UserLacksUsageOnSchemas { user: String, schemas: Vec<String> },
    #[error("insufficient privileges")]
    UserLacksSelectOnTables { user: String, tables: Vec<String> },
    #[error("referenced items not tables with REPLICA IDENTITY FULL")]
    NotTablesWReplicaIdentityFull { items: Vec<String> },
    #[error("TEXT COLUMNS refers to table not currently being added")]
    DanglingTextColumns { items: Vec<PartialItemName> },
    #[error("referenced tables use unsupported types")]
    UnrecognizedTypes { cols: Vec<(String, Oid)> },
    #[error("{0} is not a POSTGRES CONNECTION")]
    NotPgConnection(FullItemName),
    #[error("POSTGRES CONNECTION must specify PUBLICATION")]
    ConnectionMissingPublication,
}

impl PgSourcePurificationError {
    pub fn detail(&self) -> Option<String> {
        match self {
            Self::DanglingTextColumns { items } => Some(format!(
                "the following tables are referenced but not added: {}",
                itertools::join(items, ", ")
            )),
            Self::DatabaseMissingFilteredSchemas {
                database: _,
                schemas,
            } => Some(format!(
                "missing schemas: {}",
                itertools::join(schemas.iter(), ", ")
            )),
            Self::SubsourceNameConflict {
                name: _,
                upstream_references,
            } => Some(format!(
                "referenced tables with duplicate name: {}",
                itertools::join(upstream_references, ", ")
            )),
            Self::SubsourceDuplicateReference {
                name: _,
                target_names,
            } => Some(format!(
                "subsources referencing table: {}",
                itertools::join(target_names, ", ")
            )),
            Self::UserLacksUsageOnSchemas { user, schemas } => Some(format!(
                "user {} lacks USAGE privileges for schemas {}",
                user,
                schemas.join(", ")
            )),
            Self::UserLacksSelectOnTables { user, tables } => Some(format!(
                "user {} lacks SELECT privileges for tables {}",
                user,
                tables.join(", ")
            )),
            Self::NotTablesWReplicaIdentityFull { items } => {
                Some(format!("referenced items: {}", items.join(", ")))
            }
            Self::UnrecognizedTypes { cols } => Some(format!(
                "the following columns contain unsupported types:\n{}",
                itertools::join(
                    cols.into_iter()
                        .map(|(col, Oid(oid))| format!("{} (OID {})", col, oid)),
                    "\n"
                )
            )),
            _ => None,
        }
    }

    pub fn hint(&self) -> Option<String> {
        match self {
            Self::UserSpecifiedDetails => Some(
                "If trying to use the output of SHOW CREATE SOURCE, remove the DETAILS option."
                    .into(),
            ),
            Self::RequiresReferencedSubsources => {
                Some("provide a FOR TABLES (..), FOR SCHEMAS (..), or FOR ALL TABLES clause".into())
            }
            Self::SubsourceNameConflict { .. } | Self::SubsourceAlreadyReferredTo { .. } => {
                Some("Specify target table names using FOR TABLES (foo AS bar), or limit the upstream tables using FOR SCHEMAS (foo)".into())
            }
            Self::UnrecognizedTypes {
                cols: _,
            } => Some(
                "Use the TEXT COLUMNS option naming the listed columns, and Materialize can ingest their values \
                as text."
                    .into(),
            ),
            _ => None,
        }
    }
}

/// Logical errors detectable during purification for a KAFKA SOURCE.
#[derive(Debug, Clone, thiserror::Error)]
pub enum KafkaSourcePurificationError {
    #[error("{} is only valid for multi-output sources", .0.to_ast_string())]
    ReferencedSubsources(ReferencedSubsources<Aug>),
    #[error("KAFKA CONNECTION without TOPIC")]
    ConnectionMissingTopic,
    #[error("{0} is not a KAFKA CONNECTION")]
    NotKafkaConnection(FullItemName),
    #[error("failed to create and connect Kafka consumer")]
    KafkaConsumerError(String),
}

impl KafkaSourcePurificationError {
    pub fn detail(&self) -> Option<String> {
        match self {
            Self::KafkaConsumerError(e) => Some(e.clone()),
            _ => None,
        }
    }

    pub fn hint(&self) -> Option<String> {
        None
    }
}

/// Logical errors detectable during purification for a TEST SCRIPT SOURCE.
#[derive(Debug, Clone, thiserror::Error)]
pub enum TestScriptSourcePurificationError {
    #[error("{} is only valid for multi-output sources", .0.to_ast_string())]
    ReferencedSubsources(ReferencedSubsources<Aug>),
}

impl TestScriptSourcePurificationError {
    pub fn detail(&self) -> Option<String> {
        None
    }

    pub fn hint(&self) -> Option<String> {
        None
    }
}

/// Logical errors detectable during purification for a LOAD GENERATOR SOURCE.
#[derive(Debug, Clone, thiserror::Error)]
pub enum LoadGeneratorSourcePurificationError {
    #[error("FOR ALL TABLES is only valid for multi-output sources")]
    ForAllTables,
    #[error("FOR SCHEMAS (..) unsupported")]
    ForSchemas,
    #[error("FOR TABLES (..) unsupported")]
    ForTables,
    #[error("multi-output sources require a FOR TABLES (..) or FOR ALL TABLES statement")]
    MultiOutputRequiresForAllTables,
}

impl LoadGeneratorSourcePurificationError {
    pub fn detail(&self) -> Option<String> {
        match self {
            _ => None,
        }
    }

    pub fn hint(&self) -> Option<String> {
        match self {
            _ => None,
        }
    }
}

/// Logical errors detectable during purification for a KAFKA SOURCE.
#[derive(Debug, Clone, thiserror::Error)]
pub enum KafkaSinkPurificationError {
    #[error("{0} is not a KAFKA CONNECTION")]
    NotKafkaConnection(FullItemName),
    #[error("admin client errored")]
    AdminClientError(String),
    #[error("zero brokers discovered in metadata request")]
    ZeroBrokers,
}

impl KafkaSinkPurificationError {
    pub fn detail(&self) -> Option<String> {
        match self {
            Self::AdminClientError(e) => Some(e.clone()),
            _ => None,
        }
    }

    pub fn hint(&self) -> Option<String> {
        None
    }
}

/// Logical errors detectable during purification for a KAFKA SOURCE.
#[derive(Debug, Clone, thiserror::Error)]
pub enum CsrPurificationError {
    #[error("{0} is not a CONFLUENT SCHEMA REGISTRY CONNECTION")]
    NotCsrConnection(FullItemName),
    // `mz_storage_types::Errors::CsrConnectError` has an `anyhow::Error` that is not cloneable.
    #[error("client errored")]
    ClientError(String),
    // `mz_ccsr::ListError` has a `reqwest::Error` that is not cloneable.
    #[error("list subjects failed")]
    ListSubjectsError(String),
}

impl CsrPurificationError {
    pub fn detail(&self) -> Option<String> {
        match self {
            Self::ClientError(e) => Some(e.clone()),
            _ => None,
        }
    }

    pub fn hint(&self) -> Option<String> {
        None
    }
}

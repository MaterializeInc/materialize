// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use mz_ccsr::ListError;
use mz_repr::adt::system::Oid;
use mz_sql_parser::ast::display::AstDisplay;
use mz_sql_parser::ast::{ExternalReferences, UnresolvedItemName};
use mz_storage_types::errors::{ContextCreationError, CsrConnectError};

use crate::names::{FullItemName, PartialItemName};

/// Logical errors detectable during purification for a POSTGRES SOURCE.
#[derive(Debug, Clone, thiserror::Error)]
pub enum PgSourcePurificationError {
    #[error("CREATE SOURCE specifies DETAILS option")]
    UserSpecifiedDetails,
    #[error("{0} option is unnecessary when no tables are added")]
    UnnecessaryOptionsWithoutReferences(String),
    #[error("PUBLICATION {0} is empty")]
    EmptyPublication(String),
    #[error("database {database} missing referenced schemas")]
    DatabaseMissingFilteredSchemas {
        database: String,
        schemas: Vec<String>,
    },
    #[error("missing TABLES specification")]
    RequiresExternalReferences,
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
    #[error("{0} is not a YUGABYTE CONNECTION")]
    NotYugabyteConnection(FullItemName),
    #[error("CONNECTION must specify PUBLICATION")]
    ConnectionMissingPublication,
    #[error("PostgreSQL server has insufficient number of replication slots available")]
    InsufficientReplicationSlotsAvailable { count: usize },
    #[error("server must have wal_level >= logical, but has {wal_level}")]
    InsufficientWalLevel {
        wal_level: mz_postgres_util::replication::WalLevel,
    },
    #[error("replication disabled on server")]
    ReplicationDisabled,
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
            Self::InsufficientReplicationSlotsAvailable { count } => Some(format!(
                "executing this statement requires {} replication slot{}",
                count,
                if *count == 1 { "" } else { "s" }
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
            Self::RequiresExternalReferences => {
                Some("provide a FOR TABLES (..), FOR SCHEMAS (..), or FOR ALL TABLES clause".into())
            }
            Self::UnrecognizedTypes {
                cols: _,
            } => Some(
                "Use the TEXT COLUMNS option naming the listed columns, and Materialize can ingest their values \
                as text."
                    .into(),
            ),
            Self::InsufficientReplicationSlotsAvailable { .. } => Some(
                "you might be able to wait for other sources to finish snapshotting and try again".into()
            ),
            Self::ReplicationDisabled => Some("set max_wal_senders to a value > 0".into()),
            Self::UnnecessaryOptionsWithoutReferences(option) => Some(format!(
                "Remove the {} option, as no tables are being added.",
                option
            )),
            _ => None,
        }
    }
}

/// Logical errors detectable during purification for a KAFKA SOURCE.
#[derive(Debug, Clone, thiserror::Error)]
pub enum KafkaSourcePurificationError {
    #[error("{} is only valid for multi-output sources", .0.to_ast_string_simple())]
    ReferencedSubsources(ExternalReferences),
    #[error("KAFKA CONNECTION without TOPIC")]
    ConnectionMissingTopic,
    #[error("{0} is not a KAFKA CONNECTION")]
    NotKafkaConnection(FullItemName),
    #[error("failed to create and connect Kafka consumer")]
    KafkaConsumerError(String),
    #[error("Referenced kafka connection uses a different topic '{0}' than specified: '{1}'")]
    WrongKafkaTopic(String, UnresolvedItemName),
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
    #[error("multi-output sources require an external reference")]
    MultiOutputRequiresExternalReference,
    #[error("Referenced load generator is different '{0}' than specified: '{1}'")]
    WrongLoadGenerator(String, UnresolvedItemName),
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

/// Logical errors detectable during purification for a KAFKA SINK.
#[derive(Debug, Clone, thiserror::Error)]
pub enum KafkaSinkPurificationError {
    #[error("{0} is not a KAFKA CONNECTION")]
    NotKafkaConnection(FullItemName),
    #[error("admin client errored")]
    AdminClientError(Arc<ContextCreationError>),
    #[error("zero brokers discovered in metadata request")]
    ZeroBrokers,
}

impl KafkaSinkPurificationError {
    pub fn detail(&self) -> Option<String> {
        match self {
            Self::AdminClientError(e) => Some(e.to_string_with_causes()),
            _ => None,
        }
    }

    pub fn hint(&self) -> Option<String> {
        None
    }
}

use mz_ore::error::ErrorExt;

/// Logical errors detectable during purification for Confluent Schema Registry.
#[derive(Debug, Clone, thiserror::Error)]
pub enum CsrPurificationError {
    #[error("{0} is not a CONFLUENT SCHEMA REGISTRY CONNECTION")]
    NotCsrConnection(FullItemName),
    #[error("client errored")]
    ClientError(Arc<CsrConnectError>),
    #[error("list subjects failed")]
    ListSubjectsError(Arc<ListError>),
}

impl CsrPurificationError {
    pub fn detail(&self) -> Option<String> {
        match self {
            Self::ClientError(e) => Some(e.to_string_with_causes()),
            Self::ListSubjectsError(e) => Some(e.to_string_with_causes()),
            _ => None,
        }
    }

    pub fn hint(&self) -> Option<String> {
        None
    }
}

/// Logical errors detectable during purification for a MySQL SOURCE.
#[derive(Debug, Clone, thiserror::Error)]
pub enum MySqlSourcePurificationError {
    #[error("User lacks required MySQL privileges")]
    UserLacksPrivileges(Vec<(String, String)>),
    #[error("CREATE SOURCE specifies DETAILS option")]
    UserSpecifiedDetails,
    #[error("{0} option is unnecessary when no tables are added")]
    UnnecessaryOptionsWithoutReferences(String),
    #[error("{0} is not a MYSQL CONNECTION")]
    NotMySqlConnection(FullItemName),
    #[error("Invalid MySQL system replication settings")]
    ReplicationSettingsError(Vec<(String, String, String)>),
    #[error("referenced tables use unsupported types")]
    UnrecognizedTypes { cols: Vec<(String, String, String)> },
    #[error("duplicated column name references in table {0}: {1:?}")]
    DuplicatedColumnNames(String, Vec<String>),
    #[error("{option_name} refers to table not currently being added")]
    DanglingColumns {
        option_name: String,
        items: Vec<UnresolvedItemName>,
    },
    #[error("Invalid MySQL table reference: {0}")]
    InvalidTableReference(String),
    #[error("No tables found for provided reference")]
    EmptyDatabase,
    #[error("missing TABLES specification")]
    RequiresExternalReferences,
    #[error("No tables found in referenced schemas")]
    NoTablesFoundForSchemas(Vec<String>),
}

impl MySqlSourcePurificationError {
    pub fn detail(&self) -> Option<String> {
        match self {
            Self::UserLacksPrivileges(missing) => Some(format!(
                "Missing MySQL privileges: {}",
                itertools::join(
                    missing
                        .iter()
                        .map(|(privilege, table)| format!("'{}' on '{}'", privilege, table)),
                    ", "
                )
            )),
            Self::DanglingColumns {
                option_name: _,
                items,
            } => Some(format!(
                "the following columns are referenced but not added: {}",
                itertools::join(items, ", ")
            )),
            Self::ReplicationSettingsError(settings) => Some(format!(
                "Invalid MySQL system replication settings: {}",
                itertools::join(
                    settings.iter().map(|(setting, expected, actual)| format!(
                        "{}: expected {}, got {}",
                        setting, expected, actual
                    )),
                    "; "
                )
            )),
            Self::UnrecognizedTypes { cols } => Some(format!(
                "the following columns contain unsupported types:\n{}",
                itertools::join(
                    cols.into_iter().map(|(table, column, data_type)| format!(
                        "'{}' for {}.{}",
                        data_type, column, table
                    )),
                    "\n"
                )
            )),
            Self::NoTablesFoundForSchemas(schemas) => Some(format!(
                "missing schemas: {}",
                itertools::join(schemas.iter(), ", ")
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
            Self::ReplicationSettingsError(_) => {
                Some("Set the necessary MySQL database system settings.".into())
            }
            Self::RequiresExternalReferences => {
                Some("provide a FOR TABLES (..), FOR SCHEMAS (..), or FOR ALL TABLES clause".into())
            }
            Self::InvalidTableReference(_) => Some(
                "Specify tables names as SCHEMA_NAME.TABLE_NAME in a FOR TABLES (..) clause".into(),
            ),
            Self::UnrecognizedTypes { cols: _ } => Some(
                "Check the docs -- some types can be supported using the TEXT COLUMNS option to \
                ingest their values as text, or ignored using EXCLUDE COLUMNS."
                    .into(),
            ),
            Self::EmptyDatabase => Some(
                "No tables were found to replicate. This could be because \
                the user does not have privileges on the intended tables."
                    .into(),
            ),
            Self::UnnecessaryOptionsWithoutReferences(option) => Some(format!(
                "Remove the {} option, as no tables are being added.",
                option
            )),
            _ => None,
        }
    }
}

/// Logical errors detectable during purification for a SQL Server SOURCE.
#[derive(Debug, Clone, thiserror::Error)]
pub enum SqlServerSourcePurificationError {
    #[error("{0} is not a SQL SERVER CONNECTION")]
    NotSqlServerConnection(FullItemName),
    #[error("CREATE SOURCE specifies DETAILS option")]
    UserSpecifiedDetails,
    #[error("{0} option is unnecessary when no tables are added")]
    UnnecessaryOptionsWithoutReferences(String),
    #[error("Invalid SQL Server system replication settings")]
    ReplicationSettingsError(Vec<(String, String, String)>),
    #[error("missing TABLES specification")]
    RequiresExternalReferences,
    #[error("{option_name} refers to table not currently being added")]
    DanglingColumns {
        option_name: String,
        items: Vec<UnresolvedItemName>,
    },
    #[error("found multiple primary keys for a table. constraints {constraint_names:?}")]
    MultiplePrimaryKeys { constraint_names: Vec<Arc<str>> },
    #[error("column {schema_name}.{tbl_name}.{col_name} of type {col_type} is not supported")]
    UnsupportedColumn {
        schema_name: Arc<str>,
        tbl_name: Arc<str>,
        col_name: Arc<str>,
        col_type: Arc<str>,
    },
    #[error("Table {tbl_name} had all columns excluded")]
    NoColumnsForTable { tbl_name: Arc<str> },
    #[error("No tables found for provided reference")]
    NoTables,
    #[error("programming error: {0}")]
    ProgrammingError(String),
}

impl SqlServerSourcePurificationError {
    pub fn detail(&self) -> Option<String> {
        match self {
            Self::ReplicationSettingsError(settings) => Some(format!(
                "Invalid SQL Server system replication settings: {}",
                itertools::join(
                    settings.iter().map(|(setting, expected, actual)| format!(
                        "{}: expected {}, got {}",
                        setting, expected, actual
                    )),
                    "; "
                )
            )),
            Self::DanglingColumns {
                option_name: _,
                items,
            } => Some(format!(
                "the following columns are referenced but not added: {}",
                itertools::join(items, ", ")
            )),
            _ => None,
        }
    }

    pub fn hint(&self) -> Option<String> {
        match self {
            Self::RequiresExternalReferences => {
                Some("provide a FOR TABLES (..), FOR SCHEMAS (..), or FOR ALL TABLES clause".into())
            }
            Self::UnnecessaryOptionsWithoutReferences(option) => Some(format!(
                "Remove the {} option, as no tables are being added.",
                option
            )),
            Self::NoTables => Some(
                "No tables were found to replicate. This could be because \
                the user does not have privileges on the intended tables."
                    .into(),
            ),
            Self::UnsupportedColumn { .. } => {
                Some("Use EXCLUDE COLUMNS (...) to exclude a column from this source".into())
            }
            _ => None,
        }
    }
}

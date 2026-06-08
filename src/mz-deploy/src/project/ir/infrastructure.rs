// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Infrastructure property extraction from AST statements.
//!
//! Extracts structured metadata from connections, sources, and
//! tables-from-source statements. Used by both the compiler (to persist
//! to SQLite) and the LSP catalog (to build the explore page).
//!
//! ## Extraction Rules
//!
//! | Statement Kind         | Produces                       |
//! |------------------------|--------------------------------|
//! | `CREATE CONNECTION`    | `Infrastructure::Connection`   |
//! | `CREATE SOURCE`        | `Infrastructure::Source`        |
//! | `CREATE TABLE .. FROM` | `Infrastructure::TableFromSource` |
//! | Everything else        | `None`                         |
//!
//! For connections, `PublicKey1` and `PublicKey2` options are filtered out
//! (they are auto-generated SSH key material, not user-configurable).
//!
//! Property values that reference secrets or objects carry the reference
//! name in `secret_ref` / `object_ref` for downstream linking.

use mz_sql_parser::ast::{
    ConnectionOptionName, CreateConnectionType, CreateSourceConnection, Raw, RawItemName,
    WithOptionValue,
};

/// Extracted infrastructure metadata for an object.
#[derive(Debug, Clone)]
pub(crate) enum Infrastructure {
    /// Connection properties (HOST, PORT, USER, PASSWORD, etc.)
    Connection {
        /// Connection type (e.g., "Postgres", "Kafka", "MySQL").
        connector_type: String,
        /// Key-value properties extracted from ConnectionOption values.
        properties: Vec<Property>,
    },
    /// Source configuration (connection ref, cluster, publication/topic, etc.)
    Source {
        /// Source type (e.g., "Postgres", "Kafka", "Load Generator").
        connector_type: String,
        /// The connection object this source uses (for linking).
        connection_ref: Option<String>,
        /// Key-value properties extracted from source options.
        properties: Vec<Property>,
    },
    /// Table-from-source metadata.
    TableFromSource {
        /// The source object this table reads from (for linking).
        source_ref: String,
        /// External reference (e.g., "public.orders" in the upstream DB).
        external_reference: Option<String>,
    },
}

/// A key-value property extracted from an infrastructure object's AST.
#[derive(Debug, Clone)]
pub(crate) struct Property {
    /// Property name (e.g., "HOST", "DATABASE", "PUBLICATION").
    pub key: String,
    /// Display value. For secrets: the secret's fully-qualified name.
    pub value: String,
    /// If this property references a secret, the secret's object ID (for linking).
    pub secret_ref: Option<String>,
    /// If this property references another object, its object ID (for linking).
    pub object_ref: Option<String>,
}

/// Extract infrastructure properties from a statement, if applicable.
///
/// Returns `Some` for connections, sources, and tables-from-source;
/// `None` for all other statement types.
pub(crate) fn extract(stmt: &crate::project::ast::Statement) -> Option<Infrastructure> {
    match stmt {
        crate::project::ast::Statement::CreateConnection(s) => Some(extract_connection(s)),
        crate::project::ast::Statement::CreateSource(s) => Some(extract_source(s)),
        crate::project::ast::Statement::CreateTableFromSource(s) => {
            Some(extract_table_from_source(s))
        }
        _ => None,
    }
}

/// Format a `CreateConnectionType` as a human-readable string.
fn connection_type_name(ct: &CreateConnectionType) -> &'static str {
    match ct {
        CreateConnectionType::Postgres => "Postgres",
        CreateConnectionType::Kafka => "Kafka",
        CreateConnectionType::MySql => "MySQL",
        CreateConnectionType::Ssh => "SSH Tunnel",
        CreateConnectionType::Aws => "AWS",
        CreateConnectionType::AwsPrivatelink => "AWS PrivateLink",
        CreateConnectionType::Csr => "Confluent Schema Registry",
        CreateConnectionType::GlueSchemaRegistry => "Glue Schema Registry",
        CreateConnectionType::SqlServer => "SQL Server",
        CreateConnectionType::IcebergCatalog => "Iceberg Catalog",
    }
}

/// Format a `WithOptionValue<Raw>` into a display string and optional ref IDs.
fn format_option_value(value: &WithOptionValue<Raw>) -> (String, Option<String>, Option<String>) {
    match value {
        WithOptionValue::Secret(name) => {
            let name_str = raw_item_name_to_string(name);
            (name_str.clone(), Some(name_str), None)
        }
        WithOptionValue::Item(name) => {
            let name_str = raw_item_name_to_string(name);
            (name_str.clone(), None, Some(name_str))
        }
        other => (format!("{}", other), None, None),
    }
}

/// Extract the unqualified string from a `RawItemName`.
fn raw_item_name_to_string(name: &RawItemName) -> String {
    match name {
        RawItemName::Name(n) => n.to_string(),
        RawItemName::Id(_, n, _) => n.to_string(),
    }
}

/// Convert a slice of source/connection options into [`Property`] entries.
///
/// Each option must have a displayable `.name` and an `Option<WithOptionValue<Raw>>`
/// `.value`. Options with no value are skipped.
macro_rules! options_to_properties {
    ($options:expr) => {
        $options
            .iter()
            .filter_map(|opt| {
                let value = opt.value.as_ref()?;
                let (display, secret_ref, object_ref) = format_option_value(value);
                Some(Property {
                    key: format!("{}", opt.name),
                    value: display,
                    secret_ref,
                    object_ref,
                })
            })
            .collect()
    };
}

/// Extract structured properties from a CREATE CONNECTION statement.
fn extract_connection(stmt: &mz_sql_parser::ast::CreateConnectionStatement<Raw>) -> Infrastructure {
    let connector_type = connection_type_name(&stmt.connection_type).to_string();
    let properties = stmt
        .values
        .iter()
        .filter_map(|opt| {
            if matches!(
                opt.name,
                ConnectionOptionName::PublicKey1 | ConnectionOptionName::PublicKey2
            ) {
                return None;
            }
            let value = opt.value.as_ref()?;
            let (display, secret_ref, object_ref) = format_option_value(value);
            Some(Property {
                key: format!("{}", opt.name),
                value: display,
                secret_ref,
                object_ref,
            })
        })
        .collect();
    Infrastructure::Connection {
        connector_type,
        properties,
    }
}

/// Extract structured properties from a CREATE SOURCE statement.
fn extract_source(stmt: &mz_sql_parser::ast::CreateSourceStatement<Raw>) -> Infrastructure {
    let (connector_type, connection_ref, properties) = match &stmt.connection {
        CreateSourceConnection::Postgres {
            connection,
            options,
        } => (
            "Postgres".to_string(),
            Some(raw_item_name_to_string(connection)),
            options_to_properties!(options),
        ),
        CreateSourceConnection::Kafka {
            connection,
            options,
        } => (
            "Kafka".to_string(),
            Some(raw_item_name_to_string(connection)),
            options_to_properties!(options),
        ),
        CreateSourceConnection::MySql {
            connection,
            options,
        } => (
            "MySQL".to_string(),
            Some(raw_item_name_to_string(connection)),
            options_to_properties!(options),
        ),
        CreateSourceConnection::SqlServer {
            connection,
            options,
        } => (
            "SQL Server".to_string(),
            Some(raw_item_name_to_string(connection)),
            options_to_properties!(options),
        ),
        CreateSourceConnection::LoadGenerator { generator, options } => (
            format!("Load Generator ({})", generator),
            None,
            options_to_properties!(options),
        ),
    };
    Infrastructure::Source {
        connector_type,
        connection_ref,
        properties,
    }
}

/// Extract structured properties from a CREATE TABLE FROM SOURCE statement.
fn extract_table_from_source(
    stmt: &mz_sql_parser::ast::CreateTableFromSourceStatement<Raw>,
) -> Infrastructure {
    let source_ref = raw_item_name_to_string(&stmt.source);
    let external_reference = stmt.external_reference.as_ref().map(|n| n.to_string());
    Infrastructure::TableFromSource {
        source_ref,
        external_reference,
    }
}

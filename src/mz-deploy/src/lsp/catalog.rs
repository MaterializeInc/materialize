//! Catalog response builder for the `mz-deploy/catalog` custom LSP endpoint.
//!
//! Builds a JSON representation of the project's data catalog for rendering in
//! the VS Code workspace webview. The response contains two collections:
//!
//! - **`databases`** — A tree of [`CatalogDatabase`] → [`CatalogSchema`] entries
//!   providing the sidebar navigation structure with object IDs grouped by
//!   schema.
//!
//! - **`objects`** — One [`CatalogObject`] per project object plus one per
//!   external dependency. Each object carries full metadata for the detail panel:
//!   type, cluster, file path, description, columns, dependencies, dependents,
//!   indexes, constraints, grants, and infrastructure properties.
//!
//! ## Infrastructure Properties
//!
//! Connections, sources, and tables-from-source carry structured metadata
//! extracted from their AST statements. The [`CatalogInfrastructure`] enum
//! holds connector type, key-value properties (with optional secret/object
//! refs for linking), and upstream references. The webview uses these to
//! render type-specific detail layouts.
//!
//! ## Differences from `DagResponse`
//!
//! The DAG endpoint (`dag.rs`) returns only graph topology and minimal node
//! metadata for visualization. This endpoint returns the full object metadata
//! needed for a data catalog experience — columns, constraints, grants, indexes,
//! and descriptions — but omits SQL text and edges (which are only needed for
//! graph rendering).
//!
//! ## Differences from `DocsManifest`
//!
//! The explore manifest (`manifest.rs`) includes SQL text, test results, cluster
//! definitions, stats, and companion SQL for constraints. This endpoint omits
//! all of those since they are not needed in the VS Code catalog view. It also
//! adds `file_path` (relative to project root) which the manifest does not
//! include. Both endpoints extract infrastructure properties from the AST using
//! independent but structurally equivalent helpers.

use crate::project::object_id::ObjectId;
use crate::project::planned;
use crate::types::Types;
use mz_sql_parser::ast::{
    ConnectionOptionName, CreateConnectionType, CreateSourceConnection, Raw, RawItemName,
    WithOptionValue,
};
use serde::Serialize;
use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;

/// Complete catalog response returned by the `mz-deploy/catalog` endpoint.
#[derive(Debug, Serialize)]
pub struct CatalogResponse {
    /// Database → schema tree for sidebar navigation.
    pub databases: Vec<CatalogDatabase>,
    /// All objects with full metadata for the detail panel.
    pub objects: Vec<CatalogObject>,
}

/// A database in the catalog sidebar tree.
#[derive(Debug, Serialize)]
pub struct CatalogDatabase {
    /// Database name.
    pub name: String,
    /// Schemas within this database.
    pub schemas: Vec<CatalogSchema>,
}

/// A schema in the catalog sidebar tree.
#[derive(Debug, Serialize)]
pub struct CatalogSchema {
    /// Schema name.
    pub name: String,
    /// Fully-qualified IDs of objects in this schema (ordering matches project).
    pub object_ids: Vec<String>,
}

/// A single object in the catalog with full metadata.
#[derive(Debug, Serialize)]
pub struct CatalogObject {
    /// Fully-qualified object ID (e.g., `"db.schema.name"`).
    pub id: String,
    /// Database name.
    pub database: String,
    /// Schema name.
    pub schema: String,
    /// Unqualified object name.
    pub name: String,
    /// Object type (e.g., `"view"`, `"materialized-view"`, `"table"`).
    pub object_type: String,
    /// Cluster the object is materialized on, if any.
    pub cluster: Option<String>,
    /// Relative file path to the `.sql` source file, or `null` for external deps.
    pub file_path: Option<String>,
    /// COMMENT ON description text.
    pub description: Option<String>,
    /// Whether this is an external dependency (not defined in the project).
    pub is_external: bool,
    /// Column schemas (from types cache), if available.
    pub columns: Option<Vec<CatalogColumn>>,
    /// Fully-qualified IDs of objects this depends on.
    pub dependencies: Vec<String>,
    /// Fully-qualified IDs of objects that depend on this.
    pub dependents: Vec<String>,
    /// Indexes defined on this object.
    pub indexes: Vec<CatalogIndex>,
    /// Constraints defined on this object.
    pub constraints: Vec<CatalogConstraint>,
    /// Grants on this object.
    pub grants: Vec<CatalogGrant>,
    /// Infrastructure properties for connections, sources, and tables-from-source.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub infrastructure: Option<CatalogInfrastructure>,
}

/// A column in an object's schema.
#[derive(Debug, Serialize)]
pub struct CatalogColumn {
    /// Column name.
    pub name: String,
    /// SQL type name.
    pub type_name: String,
    /// Whether the column is nullable.
    pub nullable: bool,
    /// COMMENT ON COLUMN description, if any.
    pub comment: Option<String>,
}

/// An index on an object.
#[derive(Debug, Serialize)]
pub struct CatalogIndex {
    /// Index name.
    pub name: String,
    /// Cluster the index is on, if specified.
    pub cluster: Option<String>,
    /// Indexed column expressions.
    pub columns: Vec<String>,
}

/// A constraint on an object.
#[derive(Debug, Serialize)]
pub struct CatalogConstraint {
    /// Constraint kind (e.g., `"PRIMARY KEY"`, `"UNIQUE"`, `"FOREIGN KEY"`).
    pub kind: String,
    /// Constraint name.
    pub name: String,
    /// Columns involved.
    pub columns: Vec<String>,
    /// Whether the constraint is enforced.
    pub enforced: bool,
    /// Referenced object for foreign keys.
    pub references: Option<String>,
    /// Referenced columns for foreign keys.
    pub reference_columns: Option<Vec<String>>,
}

/// A grant on an object.
#[derive(Debug, Serialize)]
pub struct CatalogGrant {
    /// Privilege name (e.g., `"SELECT"`, `"INSERT"`, `"ALL"`).
    pub privilege: String,
    /// Role the privilege is granted to.
    pub role: String,
}

/// Structured metadata for infrastructure objects, extracted from the AST.
///
/// Connections, sources, and tables-from-source carry type-specific metadata
/// (connector type, configuration properties, upstream references) that the
/// webview uses to render dedicated detail layouts.
#[derive(Debug, Serialize)]
#[serde(tag = "type")]
pub enum CatalogInfrastructure {
    /// Connection properties (HOST, PORT, USER, PASSWORD, etc.)
    #[serde(rename = "connection")]
    Connection {
        /// Connection type (e.g., "Postgres", "Kafka", "MySQL").
        connector_type: String,
        /// Key-value properties extracted from ConnectionOption values.
        properties: Vec<CatalogProperty>,
    },
    /// Source configuration (connection ref, cluster, publication/topic, etc.)
    #[serde(rename = "source")]
    Source {
        /// Source type (e.g., "Postgres", "Kafka", "Load Generator").
        connector_type: String,
        /// The connection object this source uses (for linking).
        #[serde(skip_serializing_if = "Option::is_none")]
        connection_ref: Option<String>,
        /// Key-value properties extracted from source options.
        properties: Vec<CatalogProperty>,
    },
    /// Table-from-source metadata.
    #[serde(rename = "table_from_source")]
    TableFromSource {
        /// The source object this table reads from (for linking).
        source_ref: String,
        /// External reference (e.g., "public.orders" in the upstream DB).
        #[serde(skip_serializing_if = "Option::is_none")]
        external_reference: Option<String>,
    },
}

/// A key-value property extracted from an infrastructure object's AST.
#[derive(Debug, Serialize)]
pub struct CatalogProperty {
    /// Property name (e.g., "HOST", "DATABASE", "PUBLICATION").
    pub key: String,
    /// Display value. For secrets: the secret's fully-qualified name.
    pub value: String,
    /// If this property references a secret, the secret's object ID (for linking).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub secret_ref: Option<String>,
    /// If this property references another object, its object ID (for linking).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub object_ref: Option<String>,
}

/// Map an AST statement to its display type name.
fn object_type_name(stmt: &crate::project::ast::Statement) -> &'static str {
    match stmt {
        crate::project::ast::Statement::CreateView(_) => "view",
        crate::project::ast::Statement::CreateMaterializedView(_) => "materialized-view",
        crate::project::ast::Statement::CreateTable(_) => "table",
        crate::project::ast::Statement::CreateTableFromSource(_) => "table-from-source",
        crate::project::ast::Statement::CreateSource(_) => "source",
        crate::project::ast::Statement::CreateSink(_) => "sink",
        crate::project::ast::Statement::CreateSecret(_) => "secret",
        crate::project::ast::Statement::CreateConnection(_) => "connection",
    }
}

/// Extract the COMMENT ON object-level description (not column comments).
fn extract_description(
    comments: &[mz_sql_parser::ast::CommentStatement<mz_sql_parser::ast::Raw>],
) -> Option<String> {
    for c in comments {
        if !matches!(
            c.object,
            mz_sql_parser::ast::CommentObjectType::Column { .. }
        ) {
            return c.comment.clone();
        }
    }
    None
}

/// Extract index metadata from a CREATE INDEX statement.
fn index_to_catalog(
    idx: &mz_sql_parser::ast::CreateIndexStatement<mz_sql_parser::ast::Raw>,
) -> CatalogIndex {
    let name = idx.name.as_ref().map(|n| n.to_string()).unwrap_or_default();
    let cluster = idx.in_cluster.as_ref().and_then(|c| match c {
        mz_sql_parser::ast::RawClusterName::Unresolved(ident) => Some(ident.to_string()),
        _ => None,
    });
    let columns: Vec<String> = idx
        .key_parts
        .as_ref()
        .map(|parts| parts.iter().map(|expr| format!("{}", expr)).collect())
        .unwrap_or_default();
    CatalogIndex {
        name,
        cluster,
        columns,
    }
}

/// Extract constraint metadata from a CREATE CONSTRAINT statement.
fn constraint_to_catalog(
    c: &mz_sql_parser::ast::CreateConstraintStatement<mz_sql_parser::ast::Raw>,
) -> CatalogConstraint {
    let kind = format!("{}", c.kind);
    let name = c.name.as_ref().map(|n| n.to_string()).unwrap_or_else(|| {
        let parent_name = c
            .on_name
            .name()
            .0
            .last()
            .map(|i| i.to_string())
            .unwrap_or_default();
        crate::project::constraint::default_constraint_name(&parent_name, &c.columns, &c.kind)
    });
    let columns: Vec<String> = c.columns.iter().map(|col| col.to_string()).collect();
    let references = c.references.as_ref().map(|r| format!("{}", r.object));
    let reference_columns = c
        .references
        .as_ref()
        .map(|r| r.columns.iter().map(|col| col.to_string()).collect());

    CatalogConstraint {
        kind,
        name,
        columns,
        enforced: c.enforced,
        references,
        reference_columns,
    }
}

/// Convert grant statements into structured [`CatalogGrant`] entries.
///
/// Produces one entry per (privilege, role) pair. A single GRANT statement with
/// multiple privileges and multiple roles is expanded into the Cartesian product.
fn grants_to_catalog(
    grants: &[mz_sql_parser::ast::GrantPrivilegesStatement<mz_sql_parser::ast::Raw>],
) -> Vec<CatalogGrant> {
    use mz_sql_parser::ast::PrivilegeSpecification;

    let mut out = Vec::new();
    for g in grants {
        let privilege_names: Vec<String> = match &g.privileges {
            PrivilegeSpecification::All => vec!["ALL".to_string()],
            PrivilegeSpecification::Privileges(privs) => {
                privs.iter().map(|p| format!("{}", p)).collect()
            }
        };
        for role in &g.roles {
            let role_name = format!("{}", role);
            for priv_name in &privilege_names {
                out.push(CatalogGrant {
                    privilege: priv_name.clone(),
                    role: role_name.clone(),
                });
            }
        }
    }
    out
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
        CreateConnectionType::SqlServer => "SQL Server",
        CreateConnectionType::IcebergCatalog => "Iceberg Catalog",
    }
}

/// Extract structured properties from a CREATE CONNECTION statement.
fn extract_connection_properties(
    stmt: &mz_sql_parser::ast::CreateConnectionStatement<Raw>,
) -> CatalogInfrastructure {
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
            Some(CatalogProperty {
                key: format!("{}", opt.name),
                value: display,
                secret_ref,
                object_ref,
            })
        })
        .collect();
    CatalogInfrastructure::Connection {
        connector_type,
        properties,
    }
}

/// Convert a slice of source/connection options into [`CatalogProperty`] entries.
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
                Some(CatalogProperty {
                    key: format!("{}", opt.name),
                    value: display,
                    secret_ref,
                    object_ref,
                })
            })
            .collect()
    };
}

/// Extract structured properties from a CREATE SOURCE statement.
fn extract_source_properties(
    stmt: &mz_sql_parser::ast::CreateSourceStatement<Raw>,
) -> CatalogInfrastructure {
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
    CatalogInfrastructure::Source {
        connector_type,
        connection_ref,
        properties,
    }
}

/// Extract structured properties from a CREATE TABLE FROM SOURCE statement.
fn extract_table_from_source_properties(
    stmt: &mz_sql_parser::ast::CreateTableFromSourceStatement<Raw>,
) -> CatalogInfrastructure {
    let source_ref = raw_item_name_to_string(&stmt.source);
    let external_reference = stmt.external_reference.as_ref().map(|n| n.to_string());
    CatalogInfrastructure::TableFromSource {
        source_ref,
        external_reference,
    }
}

/// Extract infrastructure properties from an AST statement, if applicable.
///
/// Returns `Some` for connections, sources, and tables-from-source; `None` for
/// all other statement types.
fn extract_infrastructure(stmt: &crate::project::ast::Statement) -> Option<CatalogInfrastructure> {
    match stmt {
        crate::project::ast::Statement::CreateConnection(s) => {
            Some(extract_connection_properties(s))
        }
        crate::project::ast::Statement::CreateSource(s) => Some(extract_source_properties(s)),
        crate::project::ast::Statement::CreateTableFromSource(s) => {
            Some(extract_table_from_source_properties(s))
        }
        _ => None,
    }
}

/// Build columns with optional comments from a types cache lookup.
fn build_columns(
    id_str: &str,
    types: Option<&Types>,
    column_comments: &BTreeMap<String, String>,
) -> Option<Vec<CatalogColumn>> {
    types.and_then(|t| {
        t.get_table(id_str).map(|cols| {
            cols.iter()
                .map(|(name, ct)| CatalogColumn {
                    name: name.clone(),
                    type_name: ct.r#type.clone(),
                    nullable: ct.nullable,
                    comment: column_comments.get(name).cloned(),
                })
                .collect()
        })
    })
}

/// Compute dependents for an object, excluding constraint MVs.
fn dependents_for(
    id: &ObjectId,
    reverse_deps: &BTreeMap<ObjectId, BTreeSet<ObjectId>>,
    constraint_mv_ids: &BTreeSet<String>,
) -> Vec<String> {
    reverse_deps
        .get(id)
        .map(|s| {
            s.iter()
                .map(|d| d.to_string())
                .filter(|d| !constraint_mv_ids.contains(d))
                .collect()
        })
        .unwrap_or_default()
}

/// Build a [`CatalogObject`] from a planned database object.
///
/// Pure data transformation: extracts file path, description, columns,
/// dependencies, indexes, constraints, grants, and infrastructure from the
/// planned object and its associated type information.
fn build_catalog_object(
    obj: &planned::DatabaseObject,
    types: Option<&Types>,
    root: &Path,
    reverse_deps: &BTreeMap<ObjectId, BTreeSet<ObjectId>>,
    constraint_mv_ids: &BTreeSet<String>,
) -> CatalogObject {
    let id_str = obj.id.to_string();
    let typed = &obj.typed_object;

    let file_path = typed
        .path
        .strip_prefix(root)
        .ok()
        .map(|p| p.to_string_lossy().to_string());

    let column_comments: BTreeMap<String, String> = typed
        .comments
        .iter()
        .filter_map(|c| {
            if let mz_sql_parser::ast::CommentObjectType::Column { name } = &c.object {
                c.comment
                    .as_ref()
                    .map(|text| (name.column.to_string(), text.clone()))
            } else {
                None
            }
        })
        .collect();

    CatalogObject {
        database: obj.id.database.clone(),
        schema: obj.id.schema.clone(),
        name: obj.id.object.clone(),
        object_type: object_type_name(&typed.stmt).to_string(),
        cluster: typed.clusters().into_iter().next(),
        file_path,
        description: extract_description(&typed.comments),
        is_external: false,
        columns: build_columns(&id_str, types, &column_comments),
        dependencies: obj.dependencies.iter().map(|d| d.to_string()).collect(),
        dependents: dependents_for(&obj.id, reverse_deps, constraint_mv_ids),
        indexes: typed.indexes.iter().map(index_to_catalog).collect(),
        constraints: typed
            .constraints
            .iter()
            .map(constraint_to_catalog)
            .collect(),
        grants: grants_to_catalog(&typed.grants),
        infrastructure: extract_infrastructure(&typed.stmt),
        id: id_str,
    }
}

/// Build a stub [`CatalogObject`] for an external dependency.
///
/// External dependencies are objects referenced but not defined in the project.
/// They have no file path, no infrastructure, and columns come solely from the
/// types cache.
fn build_external_object(
    ext_id: &ObjectId,
    types: Option<&Types>,
    reverse_deps: &BTreeMap<ObjectId, BTreeSet<ObjectId>>,
    constraint_mv_ids: &BTreeSet<String>,
) -> CatalogObject {
    let id_str = ext_id.to_string();
    CatalogObject {
        id: id_str.clone(),
        database: ext_id.database.clone(),
        schema: ext_id.schema.clone(),
        name: ext_id.object.clone(),
        object_type: "external".to_string(),
        cluster: None,
        file_path: None,
        description: None,
        is_external: true,
        columns: build_columns(&id_str, types, &BTreeMap::new()),
        dependencies: Vec::new(),
        dependents: dependents_for(ext_id, reverse_deps, constraint_mv_ids),
        indexes: Vec::new(),
        constraints: Vec::new(),
        grants: Vec::new(),
        infrastructure: None,
    }
}

/// Build the catalog response from a planned project and optional type information.
///
/// Walks all project objects to create the database/schema tree and object
/// metadata via [`build_catalog_object`]. External dependencies are included as
/// stub objects via [`build_external_object`]. Constraint MVs are excluded from
/// both the tree and the dependents lists.
pub fn build_catalog_response(
    project: &planned::Project,
    types: Option<&Types>,
    root: &Path,
) -> CatalogResponse {
    let reverse_deps = project.build_reverse_dependency_graph();

    let constraint_mv_ids: BTreeSet<String> = project
        .databases
        .iter()
        .flat_map(|db| &db.schemas)
        .flat_map(|s| &s.objects)
        .filter(|obj| obj.is_constraint_mv)
        .map(|obj| obj.id.to_string())
        .collect();

    let mut objects = Vec::new();
    let mut databases = Vec::new();

    for db in &project.databases {
        let mut catalog_schemas = Vec::new();

        for schema in &db.schemas {
            let mut schema_object_ids = Vec::new();

            for obj in &schema.objects {
                if obj.is_constraint_mv {
                    continue;
                }
                schema_object_ids.push(obj.id.to_string());
                objects.push(build_catalog_object(
                    obj,
                    types,
                    root,
                    &reverse_deps,
                    &constraint_mv_ids,
                ));
            }

            catalog_schemas.push(CatalogSchema {
                name: schema.name.clone(),
                object_ids: schema_object_ids,
            });
        }

        databases.push(CatalogDatabase {
            name: db.name.clone(),
            schemas: catalog_schemas,
        });
    }

    for ext_id in &project.external_dependencies {
        objects.push(build_external_object(
            ext_id,
            types,
            &reverse_deps,
            &constraint_mv_ids,
        ));
    }

    CatalogResponse { databases, objects }
}

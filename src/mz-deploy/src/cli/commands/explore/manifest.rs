//! Data model and builder for the documentation manifest.
//!
//! The [`DocsManifest`] is a serializable snapshot of the entire project that
//! the frontend renders. [`build_manifest`] walks a [`planned::Project`] and
//! optional [`Types`] to produce this manifest.

use crate::project::clusters::{self, ClusterDefinition};
use crate::project::object_id::ObjectId;
use crate::project::planned;
use crate::project::planned::SchemaType;
use crate::types::Types;
use mz_sql_parser::ast::{
    CommentObjectType, ConnectionOptionName, CreateConnectionType, CreateSourceConnection,
    PrivilegeSpecification, Raw, RawItemName, Statement, WithOptionValue,
};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet, VecDeque};

/// Top-level manifest embedded as JSON in the generated HTML.
#[derive(Debug, Serialize)]
pub struct DocsManifest {
    /// Human-readable project name, typically derived from the directory name.
    pub project_name: String,
    /// ISO-like timestamp of when the manifest was generated.
    pub generated_at: String,
    /// Aggregate statistics for the overview page.
    pub stats: DocsStats,
    /// All databases in the project.
    pub databases: Vec<DocsDatabase>,
    /// Every object (including external dependencies) in a flat list.
    pub objects: Vec<DocsObject>,
    /// Dependency edges between objects.
    pub edges: Vec<DocsEdge>,
    /// Cluster definitions referenced by objects.
    pub clusters: Vec<DocsCluster>,
    /// Test results loaded from `target/test-results.json`, if available.
    pub test_results: Option<DocsTestResults>,
}

/// Aggregate project statistics for the overview page.
#[derive(Debug, Serialize)]
pub struct DocsStats {
    pub total_objects: usize,
    pub views: usize,
    pub materialized_views: usize,
    pub tables: usize,
    pub sources: usize,
    pub sinks: usize,
    pub secrets: usize,
    pub connections: usize,
    pub schemas: usize,
    pub clusters: usize,
    pub external_deps: usize,
    pub tests: usize,
    pub indexes: usize,
}

/// A database in the project.
#[derive(Debug, Serialize)]
pub struct DocsDatabase {
    pub name: String,
    pub schemas: Vec<DocsSchema>,
}

/// A schema within a database.
#[derive(Debug, Serialize)]
pub struct DocsSchema {
    pub name: String,
    pub database: String,
    pub schema_type: String,
    pub is_replacement: bool,
    pub object_ids: Vec<String>,
    /// Schema description extracted from `COMMENT ON SCHEMA`.
    pub description: Option<String>,
    /// File path hint where users can add a `COMMENT ON SCHEMA` statement.
    pub file_hint: Option<String>,
}

/// A single database object (view, table, source, etc.).
#[derive(Debug, Serialize)]
pub struct DocsObject {
    /// Fully-qualified identifier (`database.schema.name`).
    pub id: String,
    /// Database this object belongs to.
    pub database: String,
    /// Schema this object belongs to.
    pub schema: String,
    /// Unqualified object name.
    pub name: String,
    /// Object kind (e.g. `"view"`, `"materialized-view"`, `"source"`).
    pub object_type: String,
    /// Schema deployment type (`"Storage"`, `"Compute"`, `"Empty"`).
    pub schema_type: String,
    /// Cluster the object is deployed on, if any.
    pub cluster: Option<String>,
    /// The SQL statement that creates this object.
    pub sql: String,
    /// Relative file path within the project, if known.
    pub file_path: Option<String>,
    /// Object-level description from a COMMENT ON statement.
    pub description: Option<String>,
    /// Indexes defined on this object.
    pub indexes: Vec<DocsIndex>,
    /// Constraints defined on this object.
    pub constraints: Vec<DocsConstraint>,
    /// Privilege grants on this object.
    pub grants: Vec<DocsGrant>,
    /// Raw COMMENT ON statements as SQL strings.
    pub comments: Vec<String>,
    /// Column schema from `types.lock` / `types.cache`, if available.
    pub columns: Option<Vec<DocsColumn>>,
    /// Test SQL statements associated with this object.
    pub tests: Vec<String>,
    /// Structured infrastructure properties, if this is an infra object.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub infrastructure: Option<DocsInfraProperties>,
    /// Whether this object is an external dependency (not defined in the project).
    pub is_external: bool,
    /// Whether this object's schema uses replacement materialized views.
    pub is_replacement_schema: bool,
    /// IDs of objects this object depends on.
    pub dependencies: Vec<String>,
    /// IDs of objects that depend on this object.
    pub dependents: Vec<String>,
    /// Topological depth in the dependency DAG (0 = no dependencies).
    pub dag_depth: usize,
}

/// An index on an object.
#[derive(Debug, Serialize)]
pub struct DocsIndex {
    pub name: String,
    pub cluster: Option<String>,
    pub columns: Vec<String>,
}

/// A constraint on an object.
#[derive(Debug, Serialize)]
pub struct DocsConstraint {
    pub kind: String,
    pub name: String,
    pub columns: Vec<String>,
    pub enforced: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub references: Option<String>,
    pub cluster: Option<String>,
}

/// A column in an object's schema (from types.lock or types.cache).
#[derive(Debug, Serialize)]
pub struct DocsColumn {
    pub name: String,
    pub type_name: String,
    pub nullable: bool,
    pub comment: Option<String>,
}

/// A key-value property extracted from an infrastructure object's AST.
#[derive(Debug, Serialize)]
pub struct DocsProperty {
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

/// Structured metadata for infrastructure objects, extracted from the AST.
#[derive(Debug, Serialize)]
#[serde(tag = "type")]
pub enum DocsInfraProperties {
    /// Connection properties (HOST, PORT, USER, PASSWORD, etc.)
    #[serde(rename = "connection")]
    Connection {
        /// Connection type (e.g., "Postgres", "Kafka", "MySQL").
        connector_type: String,
        /// Key-value properties extracted from ConnectionOption values.
        properties: Vec<DocsProperty>,
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
        properties: Vec<DocsProperty>,
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

/// The semantic kind of a dependency edge.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum EdgeKind {
    /// Secret consumed by a connection.
    UsesCredential,
    /// Connection consumed by a source.
    UsesConnection,
    /// Source materialized into a table.
    MaterializesFrom,
    /// Data dependency via SQL query (views, materialized views, sinks).
    TransformsFrom,
    /// Dependency on an object not defined in this project.
    External,
}

/// A dependency edge between two objects.
#[derive(Debug, Serialize)]
pub struct DocsEdge {
    /// Fully-qualified ID of the upstream object.
    pub source: String,
    /// Fully-qualified ID of the downstream object.
    pub target: String,
    /// Semantic kind of this dependency relationship.
    pub kind: EdgeKind,
}

/// A cluster with its associated objects.
#[derive(Debug, Serialize)]
pub struct DocsCluster {
    /// Cluster name.
    pub name: String,
    /// Cluster size (e.g. `"25cc"`), if specified.
    pub size: Option<String>,
    /// Replication factor, if specified.
    pub replication_factor: Option<u32>,
    /// IDs of objects deployed on this cluster.
    pub object_ids: Vec<String>,
    /// Number of indexes on this cluster.
    pub index_count: usize,
}

/// A structured privilege grant on an object.
#[derive(Debug, Serialize)]
pub struct DocsGrant {
    /// The privilege name (e.g. `"SELECT"`, `"INSERT"`, `"ALL"`).
    pub privilege: String,
    /// The role the privilege is granted to.
    pub role: String,
}

/// Test results loaded from `target/test-results.json`.
#[derive(Debug, Serialize, Deserialize)]
pub struct DocsTestResults {
    /// Individual test entries.
    pub results: Vec<DocsTestEntry>,
    /// Aggregate pass/fail summary.
    pub summary: DocsTestSummary,
}

/// A single test result entry.
#[derive(Debug, Serialize, Deserialize)]
pub struct DocsTestEntry {
    /// Test name.
    pub name: String,
    /// Fully-qualified ID of the object under test.
    pub object_id: String,
    /// File path of the test.
    pub file_path: String,
    /// Status string (e.g. `"passed"`, `"failed"`).
    pub status: String,
    /// Elapsed time in milliseconds.
    pub elapsed_ms: u64,
    /// Failure details, if any. Kept as raw JSON for the frontend to interpret.
    pub failure: Option<serde_json::Value>,
}

/// Aggregate test summary.
#[derive(Debug, Serialize, Deserialize)]
pub struct DocsTestSummary {
    /// Number of passing tests.
    pub passed: usize,
    /// Number of failing tests.
    pub failed: usize,
    /// Number of tests that failed validation.
    pub validation_failed: usize,
}

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

/// Infer the semantic edge kind from the target object's type.
fn infer_edge_kind(target_type: &str) -> EdgeKind {
    match target_type {
        "connection" => EdgeKind::UsesCredential,
        "source" => EdgeKind::UsesConnection,
        "table-from-source" => EdgeKind::MaterializesFrom,
        _ => EdgeKind::TransformsFrom,
    }
}

fn schema_type_name(st: SchemaType) -> &'static str {
    match st {
        SchemaType::Storage => "Storage",
        SchemaType::Compute => "Compute",
        SchemaType::Empty => "Empty",
    }
}

/// Extract the COMMENT ON <object_type> description (not column comments).
fn extract_description(
    comments: &[mz_sql_parser::ast::CommentStatement<mz_sql_parser::ast::Raw>],
) -> Option<String> {
    for c in comments {
        // Only take object-level comments (not COMMENT ON COLUMN).
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
fn index_to_docs(
    idx: &mz_sql_parser::ast::CreateIndexStatement<mz_sql_parser::ast::Raw>,
) -> DocsIndex {
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
    DocsIndex {
        name,
        cluster,
        columns,
    }
}

/// Extract constraint metadata from a CREATE CONSTRAINT statement.
fn constraint_to_docs(
    c: &mz_sql_parser::ast::CreateConstraintStatement<mz_sql_parser::ast::Raw>,
) -> DocsConstraint {
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
    let cluster = c.in_cluster.as_ref().and_then(|cl| match cl {
        mz_sql_parser::ast::RawClusterName::Unresolved(ident) => Some(ident.to_string()),
        _ => None,
    });
    let references = c.references.as_ref().map(|r| format!("{}", r.object));
    DocsConstraint {
        kind,
        name,
        columns,
        enforced: c.enforced,
        references,
        cluster,
    }
}

/// Convert grant statements into structured [`DocsGrant`] entries.
///
/// Produces one entry per (privilege, role) pair. A single GRANT statement with
/// multiple privileges and multiple roles is expanded into the Cartesian product.
fn grants_to_docs(grants: &[mz_sql_parser::ast::GrantPrivilegesStatement<Raw>]) -> Vec<DocsGrant> {
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
                out.push(DocsGrant {
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
) -> DocsInfraProperties {
    let connector_type = connection_type_name(&stmt.connection_type).to_string();
    let properties = stmt
        .values
        .iter()
        .filter_map(|opt| {
            // Skip internal-only options that aren't useful for display.
            if matches!(
                opt.name,
                ConnectionOptionName::PublicKey1 | ConnectionOptionName::PublicKey2
            ) {
                return None;
            }
            let value = opt.value.as_ref()?;
            let (display, secret_ref, object_ref) = format_option_value(value);
            Some(DocsProperty {
                key: format!("{}", opt.name),
                value: display,
                secret_ref,
                object_ref,
            })
        })
        .collect();
    DocsInfraProperties::Connection {
        connector_type,
        properties,
    }
}

/// Extract structured properties from a CREATE SOURCE statement.
fn extract_source_properties(
    stmt: &mz_sql_parser::ast::CreateSourceStatement<Raw>,
) -> DocsInfraProperties {
    let (connector_type, connection_ref, properties) = match &stmt.connection {
        CreateSourceConnection::Postgres {
            connection,
            options,
        } => {
            let conn = raw_item_name_to_string(connection);
            let props: Vec<DocsProperty> = options
                .iter()
                .filter_map(|opt| {
                    let value = opt.value.as_ref()?;
                    let (display, secret_ref, object_ref) = format_option_value(value);
                    Some(DocsProperty {
                        key: format!("{}", opt.name),
                        value: display,
                        secret_ref,
                        object_ref,
                    })
                })
                .collect();
            ("Postgres".to_string(), Some(conn), props)
        }
        CreateSourceConnection::Kafka {
            connection,
            options,
        } => {
            let conn = raw_item_name_to_string(connection);
            let props: Vec<DocsProperty> = options
                .iter()
                .filter_map(|opt| {
                    let value = opt.value.as_ref()?;
                    let (display, secret_ref, object_ref) = format_option_value(value);
                    Some(DocsProperty {
                        key: format!("{}", opt.name),
                        value: display,
                        secret_ref,
                        object_ref,
                    })
                })
                .collect();
            ("Kafka".to_string(), Some(conn), props)
        }
        CreateSourceConnection::MySql {
            connection,
            options,
        } => {
            let conn = raw_item_name_to_string(connection);
            let props: Vec<DocsProperty> = options
                .iter()
                .filter_map(|opt| {
                    let value = opt.value.as_ref()?;
                    let (display, secret_ref, object_ref) = format_option_value(value);
                    Some(DocsProperty {
                        key: format!("{}", opt.name),
                        value: display,
                        secret_ref,
                        object_ref,
                    })
                })
                .collect();
            ("MySQL".to_string(), Some(conn), props)
        }
        CreateSourceConnection::SqlServer {
            connection,
            options,
        } => {
            let conn = raw_item_name_to_string(connection);
            let props: Vec<DocsProperty> = options
                .iter()
                .filter_map(|opt| {
                    let value = opt.value.as_ref()?;
                    let (display, secret_ref, object_ref) = format_option_value(value);
                    Some(DocsProperty {
                        key: format!("{}", opt.name),
                        value: display,
                        secret_ref,
                        object_ref,
                    })
                })
                .collect();
            ("SQL Server".to_string(), Some(conn), props)
        }
        CreateSourceConnection::LoadGenerator { generator, options } => {
            let props: Vec<DocsProperty> = options
                .iter()
                .filter_map(|opt| {
                    let value = opt.value.as_ref()?;
                    let (display, secret_ref, object_ref) = format_option_value(value);
                    Some(DocsProperty {
                        key: format!("{}", opt.name),
                        value: display,
                        secret_ref,
                        object_ref,
                    })
                })
                .collect();
            (format!("Load Generator ({})", generator), None, props)
        }
    };
    DocsInfraProperties::Source {
        connector_type,
        connection_ref,
        properties,
    }
}

/// Extract structured properties from a CREATE TABLE FROM SOURCE statement.
fn extract_table_from_source_properties(
    stmt: &mz_sql_parser::ast::CreateTableFromSourceStatement<Raw>,
) -> DocsInfraProperties {
    let source_ref = raw_item_name_to_string(&stmt.source);
    let external_reference = stmt.external_reference.as_ref().map(|n| n.to_string());
    DocsInfraProperties::TableFromSource {
        source_ref,
        external_reference,
    }
}

/// Extract infrastructure properties from an AST statement, if applicable.
fn extract_infrastructure(stmt: &crate::project::ast::Statement) -> Option<DocsInfraProperties> {
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

/// Compute topological depth for every object in the dependency graph.
///
/// Objects with no dependencies get depth 0. For each other object,
/// `depth = max(depth of dependencies) + 1`. External dependencies that appear
/// only as targets (not keys) in the graph also get depth 0.
fn compute_dag_depths(
    dependency_graph: &BTreeMap<ObjectId, BTreeSet<ObjectId>>,
) -> BTreeMap<String, usize> {
    // Build forward adjacency (dependency -> set of dependents)
    let mut forward: BTreeMap<&ObjectId, Vec<&ObjectId>> = BTreeMap::new();
    let mut in_degree: BTreeMap<&ObjectId, usize> = BTreeMap::new();

    // Collect all nodes
    for (obj, deps) in dependency_graph {
        in_degree.entry(obj).or_insert(0);
        for dep in deps {
            forward.entry(dep).or_default().push(obj);
            // Ensure dep is in in_degree even if it's not a key in the graph
            in_degree.entry(dep).or_insert(0);
        }
        // in_degree for obj = number of deps
        *in_degree.entry(obj).or_insert(0) = deps.len();
    }

    let mut depths: BTreeMap<&ObjectId, usize> = BTreeMap::new();
    let mut queue: VecDeque<&ObjectId> = VecDeque::new();

    // Seed with nodes that have no dependencies (in_degree == 0 or not in graph keys)
    for (node, &deg) in &in_degree {
        if deg == 0 {
            depths.insert(node, 0);
            queue.push_back(node);
        }
    }

    // BFS / Kahn's-style traversal
    while let Some(node) = queue.pop_front() {
        let current_depth = depths[node];
        if let Some(dependents) = forward.get(node) {
            for dependent in dependents {
                let new_depth = current_depth + 1;
                let entry = depths.entry(dependent).or_insert(0);
                if new_depth > *entry {
                    *entry = new_depth;
                }
                // Decrement in_degree; when it hits 0, the node is ready
                let deg = in_degree.get_mut(dependent).unwrap();
                *deg -= 1;
                if *deg == 0 {
                    queue.push_back(dependent);
                }
            }
        }
    }

    depths
        .into_iter()
        .map(|(id, d)| (id.to_string(), d))
        .collect()
}

/// Build a [`DocsManifest`] from a compiled project and optional type information.
pub fn build_manifest(
    project: &planned::Project,
    types: Option<&Types>,
    project_name: &str,
    cluster_defs: &[ClusterDefinition],
    test_results: Option<DocsTestResults>,
) -> DocsManifest {
    let reverse_deps = project.build_reverse_dependency_graph();
    let dag_depths = compute_dag_depths(&project.dependency_graph);

    // Collect constraint MV IDs so we can exclude them from dependents and edges.
    let constraint_mv_ids: BTreeSet<String> = project
        .databases
        .iter()
        .flat_map(|db| &db.schemas)
        .flat_map(|s| &s.objects)
        .filter(|obj| obj.is_constraint_mv)
        .map(|obj| obj.id.to_string())
        .collect();

    let mut objects = Vec::new();
    let mut edges = Vec::new();
    let mut databases = Vec::new();

    // Track stats
    let mut stats = DocsStats {
        total_objects: 0,
        views: 0,
        materialized_views: 0,
        tables: 0,
        sources: 0,
        sinks: 0,
        secrets: 0,
        connections: 0,
        schemas: 0,
        clusters: 0,
        external_deps: project.external_dependencies.len(),
        tests: project.tests.len(),
        indexes: 0,
    };

    // Track clusters → objects
    let mut cluster_objects: BTreeMap<String, Vec<String>> = BTreeMap::new();
    let mut cluster_index_count: BTreeMap<String, usize> = BTreeMap::new();

    // Walk databases → schemas → objects
    for db in &project.databases {
        let mut doc_schemas = Vec::new();

        for schema in &db.schemas {
            stats.schemas += 1;
            let mut schema_object_ids = Vec::new();

            let is_replacement =
                project
                    .replacement_schemas
                    .contains(&crate::project::SchemaQualifier::new(
                        db.name.clone(),
                        schema.name.clone(),
                    ));

            for obj in &schema.objects {
                if obj.is_constraint_mv {
                    continue;
                }
                let id_str = obj.id.to_string();
                schema_object_ids.push(id_str.clone());
                stats.total_objects += 1;

                let typed = &obj.typed_object;
                let obj_type = object_type_name(&typed.stmt);

                // Count by type
                match obj_type {
                    "view" => stats.views += 1,
                    "materialized-view" => stats.materialized_views += 1,
                    "table" | "table-from-source" => stats.tables += 1,
                    "source" => stats.sources += 1,
                    "sink" => stats.sinks += 1,
                    "secret" => stats.secrets += 1,
                    "connection" => stats.connections += 1,
                    _ => {}
                }

                stats.indexes += typed.indexes.len();

                // Clusters
                for cluster_name in typed.clusters() {
                    cluster_objects
                        .entry(cluster_name.clone())
                        .or_default()
                        .push(id_str.clone());
                }

                for idx in &typed.indexes {
                    if let Some(mz_sql_parser::ast::RawClusterName::Unresolved(ident)) =
                        &idx.in_cluster
                    {
                        let cn = ident.to_string();
                        *cluster_index_count.entry(cn).or_insert(0) += 1;
                    }
                }

                // Dependencies & edges
                let deps: Vec<String> = obj.dependencies.iter().map(|d| d.to_string()).collect();
                for dep_id in &obj.dependencies {
                    let kind = if project.external_dependencies.contains(dep_id) {
                        EdgeKind::External
                    } else {
                        infer_edge_kind(obj_type)
                    };
                    edges.push(DocsEdge {
                        source: dep_id.to_string(),
                        target: id_str.clone(),
                        kind,
                    });
                }

                let dependents: Vec<String> = reverse_deps
                    .get(&obj.id)
                    .map(|s| {
                        s.iter()
                            .map(|d| d.to_string())
                            .filter(|d| !constraint_mv_ids.contains(d))
                            .collect()
                    })
                    .unwrap_or_default();

                // Build column comment map from COMMENT ON COLUMN statements
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

                // Columns from types
                let columns = types.and_then(|t| {
                    t.get_table(&id_str).map(|cols| {
                        cols.iter()
                            .map(|(name, ct)| DocsColumn {
                                name: name.clone(),
                                type_name: ct.r#type.clone(),
                                nullable: ct.nullable,
                                comment: column_comments.get(name).cloned(),
                            })
                            .collect()
                    })
                });

                let description = extract_description(&typed.comments);

                let doc_indexes: Vec<DocsIndex> = typed.indexes.iter().map(index_to_docs).collect();
                let doc_constraints: Vec<DocsConstraint> =
                    typed.constraints.iter().map(constraint_to_docs).collect();

                let grants = grants_to_docs(&typed.grants);
                let comments: Vec<String> =
                    typed.comments.iter().map(|c| format!("{}", c)).collect();
                let tests: Vec<String> = typed.tests.iter().map(|t| format!("{}", t)).collect();

                let file_path = None; // File path not available in planned representation

                let dag_depth = dag_depths.get(&id_str).copied().unwrap_or(0);

                let infrastructure = extract_infrastructure(&typed.stmt);

                objects.push(DocsObject {
                    id: id_str,
                    database: obj.id.database.clone(),
                    schema: obj.id.schema.clone(),
                    name: obj.id.object.clone(),
                    object_type: obj_type.to_string(),
                    schema_type: schema_type_name(schema.schema_type).to_string(),
                    cluster: typed.clusters().into_iter().next(),
                    sql: if obj_type == "secret" {
                        format!("CREATE SECRET {} AS '***'", obj.id)
                    } else {
                        format!("{}", typed.stmt)
                    },
                    file_path,
                    description,
                    indexes: doc_indexes,
                    constraints: doc_constraints,
                    grants,
                    comments,
                    columns,
                    tests,
                    infrastructure,
                    is_external: false,
                    is_replacement_schema: is_replacement,
                    dependencies: deps,
                    dependents,
                    dag_depth,
                });
            }

            let description = schema.mod_statements.as_ref().and_then(|stmts| {
                stmts.iter().find_map(|s| {
                    if let Statement::Comment(c) = s {
                        if matches!(c.object, CommentObjectType::Schema { .. }) {
                            return c.comment.clone();
                        }
                    }
                    None
                })
            });
            let file_hint = Some(format!("models/{}/{}.sql", db.name, schema.name));

            doc_schemas.push(DocsSchema {
                name: schema.name.clone(),
                database: db.name.clone(),
                schema_type: schema_type_name(schema.schema_type).to_string(),
                is_replacement,
                object_ids: schema_object_ids,
                description,
                file_hint,
            });
        }

        databases.push(DocsDatabase {
            name: db.name.clone(),
            schemas: doc_schemas,
        });
    }

    // Add external dependency stubs
    for ext_id in &project.external_dependencies {
        let id_str = ext_id.to_string();
        let columns = types.and_then(|t| {
            t.get_table(&id_str).map(|cols| {
                cols.iter()
                    .map(|(name, ct)| DocsColumn {
                        name: name.clone(),
                        type_name: ct.r#type.clone(),
                        nullable: ct.nullable,
                        comment: None,
                    })
                    .collect()
            })
        });

        let dependents: Vec<String> = reverse_deps
            .get(ext_id)
            .map(|s| {
                s.iter()
                    .map(|d| d.to_string())
                    .filter(|d| !constraint_mv_ids.contains(d))
                    .collect()
            })
            .unwrap_or_default();

        objects.push(DocsObject {
            id: id_str.clone(),
            database: ext_id.database.clone(),
            schema: ext_id.schema.clone(),
            name: ext_id.object.clone(),
            object_type: "external".to_string(),
            schema_type: "External".to_string(),
            cluster: None,
            sql: String::new(),
            file_path: None,
            description: None,
            indexes: Vec::new(),
            constraints: Vec::new(),
            grants: Vec::new(),
            comments: Vec::new(),
            columns,
            tests: Vec::new(),
            infrastructure: None,
            is_external: true,
            is_replacement_schema: false,
            dependencies: Vec::new(),
            dependents,
            dag_depth: 0,
        });
    }

    // Build clusters list
    let cluster_def_map: BTreeMap<String, &ClusterDefinition> =
        cluster_defs.iter().map(|d| (d.name.clone(), d)).collect();

    let all_cluster_names: BTreeSet<&String> = cluster_objects
        .keys()
        .chain(cluster_index_count.keys())
        .collect();
    stats.clusters = all_cluster_names.len();

    let clusters: Vec<DocsCluster> = all_cluster_names
        .into_iter()
        .map(|name| {
            let (size, replication_factor) = cluster_def_map
                .get(name)
                .map(|d| {
                    (
                        clusters::extract_size(&d.create_stmt),
                        clusters::extract_replication_factor(&d.create_stmt),
                    )
                })
                .unwrap_or((None, None));
            DocsCluster {
                name: name.clone(),
                size,
                replication_factor,
                object_ids: cluster_objects.get(name).cloned().unwrap_or_default(),
                index_count: cluster_index_count.get(name).copied().unwrap_or(0),
            }
        })
        .collect();

    DocsManifest {
        project_name: project_name.to_string(),
        generated_at: chrono::Local::now().format("%Y-%m-%d %H:%M:%S").to_string(),
        stats,
        databases,
        objects,
        edges,
        clusters,
        test_results,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::project::object_id::ObjectId;
    use std::collections::{BTreeMap, BTreeSet};

    /// Build a minimal planned project for testing.
    fn make_test_project() -> planned::Project {
        planned::Project {
            databases: Vec::new(),
            dependency_graph: BTreeMap::new(),
            external_dependencies: BTreeSet::new(),
            cluster_dependencies: BTreeSet::new(),
            tests: Vec::new(),
            replacement_schemas: BTreeSet::new(),
        }
    }

    #[test]
    fn empty_project_produces_empty_manifest() {
        let project = make_test_project();
        let manifest = build_manifest(&project, None, "test-project", &[], None);

        assert_eq!(manifest.project_name, "test-project");
        assert_eq!(manifest.stats.total_objects, 0);
        assert!(manifest.objects.is_empty());
        assert!(manifest.edges.is_empty());
        assert!(manifest.clusters.is_empty());
        assert!(manifest.databases.is_empty());
    }

    #[test]
    fn external_deps_appear_in_manifest() {
        let mut project = make_test_project();
        project.external_dependencies.insert(ObjectId::new(
            "app".to_string(),
            "ingest".to_string(),
            "orders".to_string(),
        ));

        let manifest = build_manifest(&project, None, "test-project", &[], None);

        assert_eq!(manifest.stats.external_deps, 1);
        assert_eq!(manifest.objects.len(), 1);
        assert!(manifest.objects[0].is_external);
        assert_eq!(manifest.objects[0].id, "app.ingest.orders");
    }

    #[test]
    fn external_deps_with_types_have_columns() {
        let mut project = make_test_project();
        project.external_dependencies.insert(ObjectId::new(
            "app".to_string(),
            "ingest".to_string(),
            "orders".to_string(),
        ));

        let mut tables = BTreeMap::new();
        let mut cols = BTreeMap::new();
        cols.insert(
            "id".to_string(),
            crate::types::ColumnType {
                r#type: "integer".to_string(),
                nullable: false,
            },
        );
        tables.insert("app.ingest.orders".to_string(), cols);
        let types = Types {
            version: 1,
            tables,
            kinds: std::collections::BTreeMap::new(),
        };

        let manifest = build_manifest(&project, Some(&types), "test-project", &[], None);

        let obj = &manifest.objects[0];
        assert!(obj.columns.is_some());
        let columns = obj.columns.as_ref().unwrap();
        assert_eq!(columns.len(), 1);
        assert_eq!(columns[0].name, "id");
    }
}

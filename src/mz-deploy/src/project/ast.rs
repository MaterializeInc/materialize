//! Abstract Syntax Tree (AST) types shared across different project representations.
//!
//! This module contains core AST types that are used by multiple layers (HIR, MIR, etc.)
//! without creating circular dependencies between those modules.

use mz_sql_parser::ast::*;

/// A structured identifier for database objects supporting partial qualification.
///
/// Represents a database object identifier that may be partially or fully qualified:
/// - Unqualified: `object`
/// - Schema-qualified: `schema.object`
/// - Fully-qualified: `database.schema.object`
///
/// This type is used internally for matching and validating object references across
/// SQL statements where references may have different levels of qualification.
#[derive(Debug)]
pub struct DatabaseIdent {
    pub database: Option<String>,
    pub schema: Option<String>,
    pub object: String,
}

impl From<UnresolvedItemName> for DatabaseIdent {
    fn from(value: UnresolvedItemName) -> Self {
        match value.0.as_slice() {
            [object] => Self {
                database: None,
                schema: None,
                object: object.to_string(),
            },
            [schema, object] => Self {
                database: None,
                schema: Some(schema.to_string()),
                object: object.to_string(),
            },
            [database, schema, object] => Self {
                database: Some(database.to_string()),
                schema: Some(schema.to_string()),
                object: object.to_string(),
            },
            _ => unreachable!(),
        }
    }
}

impl DatabaseIdent {
    /// Checks if this identifier matches another identifier with flexible qualification matching.
    ///
    /// This method performs a partial match where an identifier with fewer qualification
    /// levels can match an identifier with more levels, as long as the specified parts match.
    ///
    /// # Matching Rules
    ///
    /// - Object names must always match exactly
    /// - If this ident has a schema, it must match the other's schema (if present)
    /// - If this ident has a database, it must match the other's database (if present)
    /// - Missing qualifiers in either ident are treated as wildcards
    ///
    /// # Examples
    ///
    /// ```text
    /// "table"              matches "schema.table"           ✓
    /// "schema.table"       matches "db.schema.table"        ✓
    /// "schema.table"       matches "table"                  ✗ (schema specified but not in other)
    /// "schema1.table"      matches "schema2.table"          ✗ (schema mismatch)
    /// "db.schema.table"    matches "db.schema.table"        ✓
    /// ```
    pub(crate) fn matches(&self, other: &DatabaseIdent) -> bool {
        if self.object != other.object {
            return false;
        }

        // If we have a schema specified, it must match
        if let Some(ref our_schema) = self.schema
            && let Some(ref their_schema) = other.schema
            && our_schema != their_schema
        {
            return false;
        }

        // If we have a database specified, it must match
        if let Some(ref our_db) = self.database
            && let Some(ref their_db) = other.database
            && our_db != their_db
        {
            return false;
        }

        true
    }
}

/// A Materialize cluster reference.
///
/// Clusters in Materialize are non-namespaced objects that can be referenced
/// by indexes and materialized views via `IN CLUSTER <name>` clauses.
///
/// This struct provides type safety for cluster references and allows for
/// future extensibility (e.g., tracking cluster size, replicas, etc.).
#[derive(Debug, Clone, Hash, Eq, PartialEq)]
pub struct Cluster {
    pub name: String,
}

impl Cluster {
    /// Creates a new cluster reference with the given name.
    pub fn new(name: String) -> Self {
        Self { name }
    }
}

impl std::fmt::Display for Cluster {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.name)
    }
}

/// A validated SQL statement representing a database object.
///
/// This enum wraps all supported CREATE statements from Materialize's SQL dialect.
/// Each variant contains the parsed AST node with the `Raw` resolution state.
#[derive(Debug, Clone)]
pub enum Statement {
    /// CREATE CONNECTION statement
    CreateConnection(CreateConnectionStatement<Raw>),
    /// CREATE SOURCE ... IN CLUSTER ... FROM WEBHOOK statement
    CreateWebhookSource(CreateWebhookSourceStatement<Raw>),
    /// CREATE SOURCE statement (Kafka, Postgres, etc.)
    CreateSource(CreateSourceStatement<Raw>),
    /// CREATE SOURCE ... FROM ... SUBSOURCE statement
    CreateSubsource(CreateSubsourceStatement<Raw>),
    /// CREATE SINK statement
    CreateSink(CreateSinkStatement<Raw>),
    /// CREATE VIEW statement
    CreateView(CreateViewStatement<Raw>),
    /// CREATE MATERIALIZED VIEW statement
    CreateMaterializedView(CreateMaterializedViewStatement<Raw>),
    /// CREATE TABLE statement
    CreateTable(CreateTableStatement<Raw>),
    /// CREATE TABLE ... FROM SOURCE statement
    CreateTableFromSource(CreateTableFromSourceStatement<Raw>),
    /// CREATE SECRET statement
    CreateSecret(CreateSecretStatement<Raw>),
}

impl Statement {
    /// Extracts the database identifier from the statement.
    ///
    /// Returns the object name (potentially qualified with schema/database)
    /// declared in the CREATE statement.
    pub fn ident(&self) -> DatabaseIdent {
        match self {
            Statement::CreateConnection(c) => c.name.clone().into(),
            Statement::CreateWebhookSource(w) => w.name.clone().into(),
            Statement::CreateSource(s) => s.name.clone().into(),
            Statement::CreateSubsource(s) => s.name.clone().into(),
            Statement::CreateSink(s) => s.name.clone().unwrap().into(),
            Statement::CreateView(v) => v.definition.name.clone().into(),
            Statement::CreateMaterializedView(m) => m.name.clone().into(),
            Statement::CreateTable(t) => t.name.clone().into(),
            Statement::CreateTableFromSource(t) => t.name.clone().into(),
            Statement::CreateSecret(s) => s.name.clone().into(),
        }
    }
}

impl std::fmt::Display for Statement {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Statement::CreateConnection(s) => write!(f, "{}", s),
            Statement::CreateWebhookSource(s) => write!(f, "{}", s),
            Statement::CreateSource(s) => write!(f, "{}", s),
            Statement::CreateSubsource(s) => write!(f, "{}", s),
            Statement::CreateSink(s) => write!(f, "{}", s),
            Statement::CreateView(s) => write!(f, "{}", s),
            Statement::CreateMaterializedView(s) => write!(f, "{}", s),
            Statement::CreateTable(s) => write!(f, "{}", s),
            Statement::CreateTableFromSource(s) => write!(f, "{}", s),
            Statement::CreateSecret(s) => write!(f, "{}", s),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;

    #[test]
    fn test_cluster_creation() {
        let cluster = Cluster::new("quickstart".to_string());
        assert_eq!(cluster.name, "quickstart");
    }

    #[test]
    fn test_cluster_equality() {
        let c1 = Cluster::new("quickstart".to_string());
        let c2 = Cluster::new("quickstart".to_string());
        let c3 = Cluster::new("prod".to_string());

        assert_eq!(c1, c2);
        assert_ne!(c1, c3);
        assert_ne!(c2, c3);
    }

    #[test]
    fn test_cluster_clone() {
        let c1 = Cluster::new("quickstart".to_string());
        let c2 = c1.clone();

        assert_eq!(c1, c2);
        assert_eq!(c1.name, c2.name);
    }

    #[test]
    fn test_cluster_hash_consistency() {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};

        let c1 = Cluster::new("quickstart".to_string());
        let c2 = Cluster::new("quickstart".to_string());

        let mut hasher1 = DefaultHasher::new();
        c1.hash(&mut hasher1);
        let hash1 = hasher1.finish();

        let mut hasher2 = DefaultHasher::new();
        c2.hash(&mut hasher2);
        let hash2 = hasher2.finish();

        assert_eq!(hash1, hash2, "Equal clusters should have equal hashes");
    }

    #[test]
    fn test_cluster_in_hashset() {
        let mut clusters = HashSet::new();

        assert!(clusters.insert(Cluster::new("quickstart".to_string())));
        assert!(!clusters.insert(Cluster::new("quickstart".to_string()))); // duplicate
        assert!(clusters.insert(Cluster::new("prod".to_string())));

        assert_eq!(clusters.len(), 2);
        assert!(clusters.contains(&Cluster::new("quickstart".to_string())));
        assert!(clusters.contains(&Cluster::new("prod".to_string())));
        assert!(!clusters.contains(&Cluster::new("staging".to_string())));
    }

    #[test]
    fn test_database_ident_matches() {
        // Object name only
        let ident1 = DatabaseIdent {
            database: None,
            schema: None,
            object: "table".to_string(),
        };

        let ident2 = DatabaseIdent {
            database: Some("db".to_string()),
            schema: Some("public".to_string()),
            object: "table".to_string(),
        };

        assert!(ident1.matches(&ident2));

        // Schema qualified
        let ident3 = DatabaseIdent {
            database: None,
            schema: Some("public".to_string()),
            object: "table".to_string(),
        };

        assert!(ident3.matches(&ident2));

        // Schema mismatch
        let ident4 = DatabaseIdent {
            database: None,
            schema: Some("private".to_string()),
            object: "table".to_string(),
        };

        assert!(!ident4.matches(&ident2));
    }
}

//! Raw representation of Materialize project files.
//!
//! This module provides functionality for loading and parsing Materialize project files
//! from a directory structure into an unvalidated "raw" representation. The raw types
//! mirror the file system structure and contain parsed SQL AST nodes, but do not enforce
//! semantic validation rules.
//!
//! # Raw vs Typed
//!
//! The raw representation is the first stage in a multi-stage parsing pipeline:
//!
//! ```text
//! File System → raw::Project (parsed but unvalidated)
//!                    ↓
//!               typed::Project (validated)
//! ```
//!
//! **Raw stage:**
//! - Reads files from disk
//! - Parses SQL into AST nodes
//! - Organizes by directory structure
//! - No semantic validation
//!
//! **Typed stage:**
//! - Validates object names match file names
//! - Validates qualified names match directory structure
//! - Validates cross-references between statements
//! - Enforces type consistency
//!
//! # Directory Structure
//!
//! The expected directory structure is:
//! ```text
//! project_root/
//!   database_name/              ← Database directory
//!   database_name.sql           ← Optional database-level statements (sibling)
//!     schema_name/              ← Schema directory
//!     schema_name.sql           ← Optional schema-level statements (sibling)
//!       object_name.sql         ← DatabaseObject
//!       another_object.sql      ← DatabaseObject
//! ```
//!
//! # Database and Schema .sql Files
//!
//! Special `.sql` files can appear as siblings to database and schema directories.
//! These typically contain setup statements like:
//! - `GRANT` statements for database/schema permissions
//! - `COMMENT` statements for database/schema documentation
//! - Other administrative SQL
//!
//! These files are parsed and stored in the raw representation but are not carried
//! forward to the HIR, as validation focuses on individual object files.

use super::error::{LoadError, ProjectError};
use super::parser::parse_statements_with_context;
use mz_sql_parser::ast::{Raw, Statement};
use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};

/// A database object loaded from a single `.sql` file, containing parsed but unvalidated SQL.
///
/// Represents a single file in a schema directory. The file is parsed into SQL AST nodes
/// but no semantic validation is performed at this stage.
///
/// # Contents
///
/// A typical object file contains:
/// - One primary CREATE statement (table, view, source, etc.)
/// - Zero or more supporting statements (indexes, grants, comments)
///
/// Example `users.sql`:
/// ```sql
/// CREATE TABLE users (
///     id INT,
///     name TEXT
/// );
///
/// CREATE INDEX users_id_idx ON users (id);
/// GRANT SELECT ON users TO analyst_role;
/// COMMENT ON TABLE users IS 'User data';
/// ```
///
/// All statements are parsed into `statements` field without validation of their
/// relationships or correctness.
#[derive(Debug, Clone)]
pub struct DatabaseObject {
    /// The name of the file (without extension)
    pub name: String,
    /// The full path to the file
    pub path: PathBuf,
    /// The parsed SQL statements from the file
    ///
    /// All statements in the file are parsed in order. At this stage, no validation
    /// is performed on statement types, relationships, or consistency.
    pub statements: Vec<Statement<Raw>>,
}

/// A schema directory containing multiple database objects and optional setup statements.
///
/// Represents a schema directory within a database. Each schema contains multiple
/// `.sql` files (one per database object) and an optional sibling `.sql` file for schema-level
/// setup (e.g., `public.sql` next to `public/` directory).
///
/// # Directory Mapping
///
/// ```text
/// database_name/
///   schema_name/           ← Schema directory
///   schema_name.sql        ← Optional: mod_statements (sibling to directory)
///     users.sql            ← DatabaseObject
///     orders.sql           ← DatabaseObject
/// ```
///
/// # Schema .sql Usage
///
/// The optional `schema_name.sql` file (sibling to the schema directory) might contain:
/// ```sql
/// -- Schema grants
/// GRANT USAGE ON SCHEMA schema_name TO analyst_role;
///
/// -- Schema comments
/// COMMENT ON SCHEMA schema_name IS 'Analytics data';
/// ```
#[derive(Debug, Clone)]
pub struct Schema {
    /// The name of the schema (directory name)
    pub name: String,
    /// Optional statements from sibling .sql file for the schema
    ///
    /// If a sibling `.sql` file exists (e.g., `public.sql` next to `public/`),
    /// it is parsed and stored here. These statements are typically schema-level
    /// setup code like GRANT or COMMENT statements.
    pub mod_statements: Option<Vec<Statement<Raw>>>,
    /// All database objects in this schema
    ///
    /// Each object corresponds to one `.sql` file in the schema directory.
    pub objects: Vec<DatabaseObject>,
}

/// A database directory containing multiple schemas and optional setup statements.
///
/// Represents a database directory within the project. Each database contains multiple
/// schema directories and an optional sibling `.sql` file for database-level setup
/// (e.g., `materialize.sql` next to `materialize/` directory).
///
/// # Directory Mapping
///
/// ```text
/// project_root/
///   database_name/         ← Database directory
///   database_name.sql      ← Optional: mod_statements (sibling to directory)
///     public/              ← Schema
///       users.sql
///     analytics/           ← Schema
///       reports.sql
/// ```
///
/// # Database .sql Usage
///
/// The optional `database_name.sql` file (sibling to the database directory) might contain:
/// ```sql
/// -- Database grants
/// GRANT CREATE ON DATABASE database_name TO admin_role;
///
/// -- Database comments
/// COMMENT ON DATABASE database_name IS 'Production database';
/// ```
#[derive(Debug, Clone)]
pub struct Database {
    /// The name of the database (directory name)
    pub name: String,
    /// Optional statements from sibling .sql file for the database
    ///
    /// If a sibling `.sql` file exists (e.g., `materialize.sql` next to `materialize/`),
    /// it is parsed and stored here. These statements are typically database-level
    /// setup code like GRANT or COMMENT statements.
    pub mod_statements: Option<Vec<Statement<Raw>>>,
    /// All schemas in this database, keyed by schema name
    ///
    /// Each schema corresponds to one subdirectory in the database directory.
    /// Hidden directories (starting with `.`) are excluded.
    pub schemas: HashMap<String, Schema>,
}

/// The complete unvalidated project structure loaded from the file system.
///
/// Represents the entire project directory tree with all databases, schemas, and
/// objects loaded and parsed but not yet validated. This is the top-level raw type
/// returned by [`load_project`].
///
/// # Purpose
///
/// The `Project` type serves as the entry point for working with Materialize project
/// files. After loading with `load_project`, you typically convert it to the validated
/// HIR representation.
#[derive(Debug, Clone)]
pub struct Project {
    /// The root directory of the project
    ///
    /// This is the absolute path to the directory that was passed to `load_project`.
    /// All other paths in the project are relative to or beneath this root.
    pub root: PathBuf,
    /// All databases in this project, keyed by database name
    ///
    /// Each database corresponds to one subdirectory in the project root.
    /// Hidden directories (starting with `.`) are excluded.
    pub databases: HashMap<String, Database>,
}

/// Loads and parses a Materialize project from a directory structure.
pub fn load_project<P: AsRef<Path>>(root: P) -> Result<Project, ProjectError> {
    let root = root.as_ref();

    if !root.exists() {
        return Err(LoadError::RootNotFound {
            path: root.to_path_buf(),
        }
        .into());
    }

    if !root.is_dir() {
        return Err(LoadError::RootNotDirectory {
            path: root.to_path_buf(),
        }
        .into());
    }

    let mut databases = HashMap::new();

    // Iterate over database directories (first level)
    for db_entry in fs::read_dir(root).map_err(|source| LoadError::DirectoryReadFailed {
        path: root.to_path_buf(),
        source,
    })? {
        let db_entry = db_entry.map_err(|source| LoadError::EntryReadFailed {
            directory: root.to_path_buf(),
            source,
        })?;
        let db_path = db_entry.path();

        // Skip non-directories and hidden directories
        if !db_path.is_dir() || db_entry.file_name().to_string_lossy().starts_with('.') {
            continue;
        }

        let db_name = db_entry.file_name().to_string_lossy().to_string();
        let mut schemas = HashMap::new();

        // Check for database-level sibling .sql file (e.g., materialize.sql next to materialize/)
        let db_mod_path = root.join(format!("{}.sql", db_name));
        let db_mod_statements = if db_mod_path.exists() {
            let sql_content =
                fs::read_to_string(&db_mod_path).map_err(|source| LoadError::FileReadFailed {
                    path: db_mod_path.clone(),
                    source,
                })?;
            Some(parse_statements_with_context(
                &sql_content,
                db_mod_path.clone(),
            )?)
        } else {
            None
        };

        // Iterate over schema directories (second level)
        for schema_entry in
            fs::read_dir(&db_path).map_err(|source| LoadError::DirectoryReadFailed {
                path: db_path.clone(),
                source,
            })?
        {
            let schema_entry = schema_entry.map_err(|source| LoadError::EntryReadFailed {
                directory: db_path.clone(),
                source,
            })?;
            let schema_path = schema_entry.path();

            // Skip non-directories, hidden directories, and .sql files (schema-level .sql files are handled separately)
            if !schema_path.is_dir() || schema_entry.file_name().to_string_lossy().starts_with('.')
            {
                continue;
            }

            let schema_name = schema_entry.file_name().to_string_lossy().to_string();
            let mut objects = Vec::new();

            // Check for schema-level sibling .sql file (e.g., public.sql next to public/)
            let schema_mod_path = db_path.join(format!("{}.sql", schema_name));
            let schema_mod_statements = if schema_mod_path.exists() {
                let sql_content = fs::read_to_string(&schema_mod_path).map_err(|source| {
                    LoadError::FileReadFailed {
                        path: schema_mod_path.clone(),
                        source,
                    }
                })?;
                Some(parse_statements_with_context(
                    &sql_content,
                    schema_mod_path.clone(),
                )?)
            } else {
                None
            };

            // Iterate over SQL files (third level)
            for object_entry in
                fs::read_dir(&schema_path).map_err(|source| LoadError::DirectoryReadFailed {
                    path: schema_path.clone(),
                    source,
                })?
            {
                let object_entry = object_entry.map_err(|source| LoadError::EntryReadFailed {
                    directory: schema_path.clone(),
                    source,
                })?;
                let object_path = object_entry.path();

                // Only process .sql files
                if !object_path.is_file()
                    || object_path.extension().and_then(|s| s.to_str()) != Some("sql")
                {
                    continue;
                }

                let object_name = object_path
                    .file_stem()
                    .and_then(|s| s.to_str())
                    .ok_or_else(|| LoadError::InvalidFileName {
                        path: object_path.clone(),
                    })?
                    .to_string();

                // Read and parse the SQL file
                let sql_content = fs::read_to_string(&object_path).map_err(|source| {
                    LoadError::FileReadFailed {
                        path: object_path.clone(),
                        source,
                    }
                })?;

                let statements = parse_statements_with_context(&sql_content, object_path.clone())?;

                objects.push(DatabaseObject {
                    name: object_name,
                    path: object_path,
                    statements,
                });
            }

            // Only add schema if it has objects or mod statements
            if !objects.is_empty() || schema_mod_statements.is_some() {
                schemas.insert(
                    schema_name.clone(),
                    Schema {
                        name: schema_name,
                        mod_statements: schema_mod_statements,
                        objects,
                    },
                );
            }
        }

        // Only add database if it has schemas or mod statements
        if !schemas.is_empty() || db_mod_statements.is_some() {
            databases.insert(
                db_name.clone(),
                Database {
                    name: db_name,
                    mod_statements: db_mod_statements,
                    schemas,
                },
            );
        }
    }

    Ok(Project {
        root: root.to_path_buf(),
        databases,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use tempfile::TempDir;

    #[test]
    fn test_load_project_basic_structure() {
        // Create a temporary directory structure
        let temp_dir = TempDir::new().unwrap();
        let root = temp_dir.path();

        // Create database/schema/object structure
        let db_path = root.join("my_database");
        let schema_path = db_path.join("my_schema");
        fs::create_dir_all(&schema_path).unwrap();

        // Write a simple SQL file
        let sql_file = schema_path.join("my_table.sql");
        fs::write(&sql_file, "CREATE TABLE t (id INT);").unwrap();

        // Load the project
        let project = load_project(root).unwrap();

        // Verify structure
        assert_eq!(project.databases.len(), 1);
        assert!(project.databases.contains_key("my_database"));

        let database = &project.databases["my_database"];
        assert_eq!(database.schemas.len(), 1);
        assert!(database.schemas.contains_key("my_schema"));

        let schema = &database.schemas["my_schema"];
        assert_eq!(schema.objects.len(), 1);
        assert_eq!(schema.objects[0].name, "my_table");
        assert_eq!(schema.objects[0].statements.len(), 1);
    }

    #[test]
    fn test_load_project_multiple_databases_and_schemas() {
        let temp_dir = TempDir::new().unwrap();
        let root = temp_dir.path();

        // Create multiple databases with multiple schemas
        for db in ["db1", "db2"] {
            for schema in ["schema1", "schema2"] {
                let schema_path = root.join(db).join(schema);
                fs::create_dir_all(&schema_path).unwrap();

                // Create a SQL file in each schema
                let sql_file = schema_path.join("object.sql");
                fs::write(&sql_file, "CREATE TABLE t (id INT);").unwrap();
            }
        }

        let project = load_project(root).unwrap();

        assert_eq!(project.databases.len(), 2);
        for db_name in ["db1", "db2"] {
            let database = &project.databases[db_name];
            assert_eq!(database.schemas.len(), 2);
        }
    }

    #[test]
    fn test_load_project_ignores_hidden_directories() {
        let temp_dir = TempDir::new().unwrap();
        let root = temp_dir.path();

        // Create a hidden directory
        let hidden_path = root.join(".hidden").join("schema");
        fs::create_dir_all(&hidden_path).unwrap();
        fs::write(hidden_path.join("object.sql"), "CREATE TABLE t (id INT);").unwrap();

        // Create a normal directory
        let normal_path = root.join("normal").join("schema");
        fs::create_dir_all(&normal_path).unwrap();
        fs::write(normal_path.join("object.sql"), "CREATE TABLE t (id INT);").unwrap();

        let project = load_project(root).unwrap();

        assert_eq!(project.databases.len(), 1);
        assert!(project.databases.contains_key("normal"));
        assert!(!project.databases.contains_key(".hidden"));
    }

    #[test]
    fn test_load_project_with_database_level_sql() {
        let temp_dir = TempDir::new().unwrap();
        let root = temp_dir.path();

        // Create database with sibling .sql file
        let db_path = root.join("my_database");
        let schema_path = db_path.join("my_schema");
        fs::create_dir_all(&schema_path).unwrap();

        // Write database-level sibling .sql file (my_database.sql next to my_database/)
        fs::write(
            root.join("my_database.sql"),
            "GRANT CREATE ON DATABASE my_database TO admin;",
        )
        .unwrap();

        // Write a regular object
        fs::write(schema_path.join("object.sql"), "CREATE TABLE t (id INT);").unwrap();

        let project = load_project(root).unwrap();

        let database = &project.databases["my_database"];
        assert!(database.mod_statements.is_some());
        let mod_stmts = database.mod_statements.as_ref().unwrap();
        assert_eq!(mod_stmts.len(), 1);
    }

    #[test]
    fn test_load_project_with_schema_level_sql() {
        let temp_dir = TempDir::new().unwrap();
        let root = temp_dir.path();

        // Create schema with sibling .sql file
        let db_path = root.join("my_database");
        let schema_path = db_path.join("my_schema");
        fs::create_dir_all(&schema_path).unwrap();

        // Write schema-level sibling .sql file (my_schema.sql next to my_schema/)
        fs::write(
            db_path.join("my_schema.sql"),
            "GRANT USAGE ON SCHEMA my_schema TO analyst;",
        )
        .unwrap();

        // Write a regular object
        fs::write(schema_path.join("object.sql"), "CREATE TABLE t (id INT);").unwrap();

        let project = load_project(root).unwrap();

        let schema = &project.databases["my_database"].schemas["my_schema"];
        assert!(schema.mod_statements.is_some());
        let mod_stmts = schema.mod_statements.as_ref().unwrap();
        assert_eq!(mod_stmts.len(), 1);
    }

    #[test]
    fn test_load_project_schema_sql_not_treated_as_object() {
        let temp_dir = TempDir::new().unwrap();
        let root = temp_dir.path();

        let db_path = root.join("my_database");
        let schema_path = db_path.join("my_schema");
        fs::create_dir_all(&schema_path).unwrap();

        // Write both schema-level .sql (sibling) and regular objects
        fs::write(
            db_path.join("my_schema.sql"),
            "GRANT USAGE ON SCHEMA my_schema TO analyst;",
        )
        .unwrap();
        fs::write(schema_path.join("table1.sql"), "CREATE TABLE t1 (id INT);").unwrap();
        fs::write(schema_path.join("table2.sql"), "CREATE TABLE t2 (id INT);").unwrap();

        let project = load_project(root).unwrap();

        let schema = &project.databases["my_database"].schemas["my_schema"];

        // Schema-level .sql should be in mod_statements, not objects
        assert!(schema.mod_statements.is_some());
        assert_eq!(schema.objects.len(), 2);

        // Verify that objects only include table1 and table2
        let object_names: Vec<_> = schema.objects.iter().map(|o| o.name.as_str()).collect();
        assert!(object_names.contains(&"table1"));
        assert!(object_names.contains(&"table2"));
    }

    #[test]
    fn test_load_project_schema_with_only_schema_sql() {
        let temp_dir = TempDir::new().unwrap();
        let root = temp_dir.path();

        let db_path = root.join("my_database");
        let schema_path = db_path.join("my_schema");
        fs::create_dir_all(&schema_path).unwrap();

        // Write only schema-level .sql (sibling), no other objects
        fs::write(
            db_path.join("my_schema.sql"),
            "GRANT USAGE ON SCHEMA my_schema TO analyst;",
        )
        .unwrap();

        let project = load_project(root).unwrap();

        // Schema should still be loaded even with only schema-level .sql
        assert!(project.databases.contains_key("my_database"));
        let database = &project.databases["my_database"];
        assert!(database.schemas.contains_key("my_schema"));

        let schema = &database.schemas["my_schema"];
        assert!(schema.mod_statements.is_some());
        assert_eq!(schema.objects.len(), 0);
    }

    #[test]
    fn test_cluster_dependencies_through_full_pipeline() {
        use crate::project::ast::Cluster;
        use crate::project::planned;
        use crate::project::typed;

        let temp_dir = TempDir::new().unwrap();
        let root = temp_dir.path();

        // Create database/schema structure with separate schemas for tables and views
        let db_path = root.join("test_db");
        let tables_schema_path = db_path.join("tables");
        let views_schema_path = db_path.join("views");
        fs::create_dir_all(&tables_schema_path).unwrap();
        fs::create_dir_all(&views_schema_path).unwrap();

        // Create a base table in tables schema
        fs::write(
            tables_schema_path.join("users.sql"),
            "CREATE TABLE users (id INT, name TEXT);",
        )
        .unwrap();

        // Create materialized views with different clusters in views schema
        fs::write(
            views_schema_path.join("mv1.sql"),
            "CREATE MATERIALIZED VIEW mv1 IN CLUSTER quickstart AS SELECT * FROM tables.users;",
        )
        .unwrap();

        fs::write(
            views_schema_path.join("mv2.sql"),
            "CREATE MATERIALIZED VIEW mv2 IN CLUSTER prod AS SELECT id FROM tables.users;",
        )
        .unwrap();

        fs::write(
            views_schema_path.join("mv3.sql"),
            "CREATE MATERIALIZED VIEW mv3 IN CLUSTER quickstart AS SELECT name FROM tables.users;",
        )
        .unwrap();

        // Create a regular view (no cluster) in views schema
        fs::write(
            views_schema_path.join("view1.sql"),
            "CREATE VIEW view1 AS SELECT * FROM tables.users;",
        )
        .unwrap();

        // Load raw project
        let raw_project = load_project(root).unwrap();
        assert_eq!(raw_project.databases.len(), 1);

        // Convert to typed
        let typed_project = typed::Project::try_from(raw_project).unwrap();
        assert_eq!(typed_project.databases.len(), 1);

        // Convert to planned
        let planned_project = planned::Project::from(typed_project);

        // Verify cluster dependencies
        assert_eq!(planned_project.cluster_dependencies.len(), 2);
        assert!(
            planned_project
                .cluster_dependencies
                .contains(&Cluster::new("quickstart".to_string()))
        );
        assert!(
            planned_project
                .cluster_dependencies
                .contains(&Cluster::new("prod".to_string()))
        );

        // Verify objects exist
        assert_eq!(planned_project.databases.len(), 1);
        let database = &planned_project.databases[0];
        assert_eq!(database.name, "test_db");
        assert_eq!(database.schemas.len(), 2); // tables and views schemas

        // Find the schemas
        let tables_schema = database
            .schemas
            .iter()
            .find(|s| s.name == "tables")
            .unwrap();
        let views_schema = database.schemas.iter().find(|s| s.name == "views").unwrap();

        assert_eq!(tables_schema.objects.len(), 1); // 1 table
        assert_eq!(views_schema.objects.len(), 4); // 3 MVs + 1 view
    }
}

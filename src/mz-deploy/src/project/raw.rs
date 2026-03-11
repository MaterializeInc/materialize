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
//!   models/                       ← Models directory (contains all databases)
//!     database_name/              ← Database directory
//!     database_name.sql           ← Optional database-level statements (sibling)
//!       schema_name/              ← Schema directory
//!       schema_name.sql           ← Optional schema-level statements (sibling)
//!         object_name.sql         ← DatabaseObject
//!         another_object.sql      ← DatabaseObject
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
use super::profile_files::collect_all_sql_files;
use mz_sql_parser::ast::{Raw, Statement};
use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};

/// A single file variant of a database object (default or profile-specific).
#[derive(Debug, Clone)]
pub struct ObjectVariant {
    /// The full path to the file
    pub path: PathBuf,
    /// The profile name, or `None` for the default variant
    pub profile: Option<String>,
    /// The parsed SQL statements from the file
    pub statements: Vec<Statement<Raw>>,
}

/// A database object that may have multiple profile variants.
///
/// Represents one logical object name in a schema directory. The object may have
/// a default file and/or one or more profile-specific override files. All variants
/// are loaded and parsed; cross-variant validation and active-variant resolution
/// happen during typed conversion.
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
    /// The name of the object (without extension or profile suffix)
    pub name: String,
    /// The suffixed database name (same as directory name when no suffix is active)
    pub database: String,
    /// The schema name (directory name)
    pub schema: String,
    /// All profile variants for this object (at least one)
    pub variants: Vec<ObjectVariant>,
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
    pub schemas: BTreeMap<String, Schema>,
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
    /// The active profile name
    pub profile: String,
    /// All databases in this project, keyed by (possibly suffixed) database name
    ///
    /// Each database corresponds to one subdirectory in the project root.
    /// Hidden directories (starting with `.`) are excluded.
    pub databases: BTreeMap<String, Database>,
    /// Mapping from original database directory names to suffixed names.
    /// Empty when no suffix is active.
    pub database_name_map: BTreeMap<String, String>,
}

/// Loads and parses a Materialize project from a directory structure.
pub fn load_project<P: AsRef<Path>>(
    root: P,
    profile: &str,
    suffix: Option<&str>,
    variables: &BTreeMap<String, String>,
) -> Result<Project, ProjectError> {
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

    let models_dir = root.join("models");
    if !models_dir.is_dir() {
        return Err(LoadError::ModelsNotFound { path: models_dir }.into());
    }

    let mut databases = BTreeMap::new();
    let mut database_name_map = BTreeMap::new();

    // Iterate over database directories (first level inside models/)
    for db_entry in fs::read_dir(&models_dir).map_err(|source| LoadError::DirectoryReadFailed {
        path: models_dir.to_path_buf(),
        source,
    })? {
        let db_entry = db_entry.map_err(|source| LoadError::EntryReadFailed {
            directory: models_dir.to_path_buf(),
            source,
        })?;
        let db_path = db_entry.path();

        // Skip non-directories and hidden directories
        if !db_path.is_dir() || db_entry.file_name().to_string_lossy().starts_with('.') {
            continue;
        }

        let original_db_name = db_entry.file_name().to_string_lossy().to_string();
        let db_name = match suffix {
            Some(s) => format!("{}{}", original_db_name, s),
            None => original_db_name.clone(),
        };

        if suffix.is_some() {
            database_name_map.insert(original_db_name.clone(), db_name.clone());
        }

        let mut schemas = BTreeMap::new();

        // Check for database-level sibling .sql file (e.g., materialize.sql next to materialize/)
        let db_mod_path = models_dir.join(format!("{}.sql", original_db_name));
        let db_mod_statements = if db_mod_path.exists() {
            let mut sql_content =
                fs::read_to_string(&db_mod_path).map_err(|source| LoadError::FileReadFailed {
                    path: db_mod_path.clone(),
                    source,
                })?;
            if suffix.is_some() {
                sql_content = sql_content.replace(&original_db_name, &db_name);
            }
            Some(parse_statements_with_context(
                &sql_content,
                db_mod_path.clone(),
                variables,
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
                let mut sql_content = fs::read_to_string(&schema_mod_path).map_err(|source| {
                    LoadError::FileReadFailed {
                        path: schema_mod_path.clone(),
                        source,
                    }
                })?;
                if suffix.is_some() {
                    sql_content = sql_content.replace(&original_db_name, &db_name);
                }
                Some(parse_statements_with_context(
                    &sql_content,
                    schema_mod_path.clone(),
                    variables,
                )?)
            } else {
                None
            };

            // Collect all SQL files grouped by object name (all profile variants)
            let all_files = collect_all_sql_files(&schema_path)?;

            for object_files in all_files {
                let mut variants = Vec::new();

                // Parse the default file if it exists
                if let Some(ref default_path) = object_files.default {
                    let sql_content = fs::read_to_string(default_path).map_err(|source| {
                        LoadError::FileReadFailed {
                            path: default_path.clone(),
                            source,
                        }
                    })?;
                    let statements = parse_statements_with_context(
                        &sql_content,
                        default_path.clone(),
                        variables,
                    )?;
                    variants.push(ObjectVariant {
                        path: default_path.clone(),
                        profile: None,
                        statements,
                    });
                }

                // Parse all profile override files
                for (prof, override_path) in &object_files.overrides {
                    let sql_content = fs::read_to_string(override_path).map_err(|source| {
                        LoadError::FileReadFailed {
                            path: override_path.clone(),
                            source,
                        }
                    })?;
                    let statements = parse_statements_with_context(
                        &sql_content,
                        override_path.clone(),
                        variables,
                    )?;
                    variants.push(ObjectVariant {
                        path: override_path.clone(),
                        profile: Some(prof.clone()),
                        statements,
                    });
                }

                objects.push(DatabaseObject {
                    name: object_files.name,
                    database: db_name.clone(),
                    schema: schema_name.clone(),
                    variants,
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
        profile: profile.to_string(),
        databases,
        database_name_map,
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

        // Create models/database/schema/object structure
        let db_path = root.join("models").join("my_database");
        let schema_path = db_path.join("my_schema");
        fs::create_dir_all(&schema_path).unwrap();

        // Write a simple SQL file
        let sql_file = schema_path.join("my_table.sql");
        fs::write(&sql_file, "CREATE TABLE t (id INT);").unwrap();

        // Load the project
        let project = load_project(root, "default", None, &BTreeMap::new()).unwrap();

        // Verify structure
        assert_eq!(project.databases.len(), 1);
        assert!(project.databases.contains_key("my_database"));

        let database = &project.databases["my_database"];
        assert_eq!(database.schemas.len(), 1);
        assert!(database.schemas.contains_key("my_schema"));

        let schema = &database.schemas["my_schema"];
        assert_eq!(schema.objects.len(), 1);
        assert_eq!(schema.objects[0].name, "my_table");
        assert_eq!(schema.objects[0].variants.len(), 1);
        assert_eq!(schema.objects[0].variants[0].statements.len(), 1);
    }

    #[test]
    fn test_load_project_multiple_databases_and_schemas() {
        let temp_dir = TempDir::new().unwrap();
        let root = temp_dir.path();

        // Create multiple databases with multiple schemas
        for db in ["db1", "db2"] {
            for schema in ["schema1", "schema2"] {
                let schema_path = root.join("models").join(db).join(schema);
                fs::create_dir_all(&schema_path).unwrap();

                // Create a SQL file in each schema
                let sql_file = schema_path.join("object.sql");
                fs::write(&sql_file, "CREATE TABLE t (id INT);").unwrap();
            }
        }

        let project = load_project(root, "default", None, &BTreeMap::new()).unwrap();

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
        let hidden_path = root.join("models").join(".hidden").join("schema");
        fs::create_dir_all(&hidden_path).unwrap();
        fs::write(hidden_path.join("object.sql"), "CREATE TABLE t (id INT);").unwrap();

        // Create a normal directory
        let normal_path = root.join("models").join("normal").join("schema");
        fs::create_dir_all(&normal_path).unwrap();
        fs::write(normal_path.join("object.sql"), "CREATE TABLE t (id INT);").unwrap();

        let project = load_project(root, "default", None, &BTreeMap::new()).unwrap();

        assert_eq!(project.databases.len(), 1);
        assert!(project.databases.contains_key("normal"));
        assert!(!project.databases.contains_key(".hidden"));
    }

    #[test]
    fn test_load_project_with_database_level_sql() {
        let temp_dir = TempDir::new().unwrap();
        let root = temp_dir.path();

        // Create database with sibling .sql file
        let models_dir = root.join("models");
        let db_path = models_dir.join("my_database");
        let schema_path = db_path.join("my_schema");
        fs::create_dir_all(&schema_path).unwrap();

        // Write database-level sibling .sql file (my_database.sql next to my_database/)
        fs::write(
            models_dir.join("my_database.sql"),
            "GRANT CREATE ON DATABASE my_database TO admin;",
        )
        .unwrap();

        // Write a regular object
        fs::write(schema_path.join("object.sql"), "CREATE TABLE t (id INT);").unwrap();

        let project = load_project(root, "default", None, &BTreeMap::new()).unwrap();

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
        let db_path = root.join("models").join("my_database");
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

        let project = load_project(root, "default", None, &BTreeMap::new()).unwrap();

        let schema = &project.databases["my_database"].schemas["my_schema"];
        assert!(schema.mod_statements.is_some());
        let mod_stmts = schema.mod_statements.as_ref().unwrap();
        assert_eq!(mod_stmts.len(), 1);
    }

    #[test]
    fn test_load_project_schema_sql_not_treated_as_object() {
        let temp_dir = TempDir::new().unwrap();
        let root = temp_dir.path();

        let db_path = root.join("models").join("my_database");
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

        let project = load_project(root, "default", None, &BTreeMap::new()).unwrap();

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

        let db_path = root.join("models").join("my_database");
        let schema_path = db_path.join("my_schema");
        fs::create_dir_all(&schema_path).unwrap();

        // Write only schema-level .sql (sibling), no other objects
        fs::write(
            db_path.join("my_schema.sql"),
            "GRANT USAGE ON SCHEMA my_schema TO analyst;",
        )
        .unwrap();

        let project = load_project(root, "default", None, &BTreeMap::new()).unwrap();

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
        let db_path = root.join("models").join("test_db");
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
        let raw_project = load_project(root, "default", None, &BTreeMap::new()).unwrap();
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

    #[test]
    fn test_load_project_with_suffix() {
        let temp_dir = TempDir::new().unwrap();
        let root = temp_dir.path();

        let schema_path = root.join("models").join("mydb").join("public");
        fs::create_dir_all(&schema_path).unwrap();
        fs::write(
            schema_path.join("my_view.sql"),
            "CREATE VIEW my_view AS SELECT 1;",
        )
        .unwrap();

        let project = load_project(root, "default", Some("_stg"), &BTreeMap::new()).unwrap();

        assert_eq!(project.databases.len(), 1);
        assert!(project.databases.contains_key("mydb_stg"));

        let database = &project.databases["mydb_stg"];
        assert_eq!(database.name, "mydb_stg");

        let obj = &database.schemas["public"].objects[0];
        assert_eq!(obj.database, "mydb_stg");
        assert_eq!(obj.schema, "public");
    }

    #[test]
    fn test_load_project_with_suffix_multiple_databases() {
        let temp_dir = TempDir::new().unwrap();
        let root = temp_dir.path();

        for db in ["db1", "db2"] {
            let schema_path = root.join("models").join(db).join("public");
            fs::create_dir_all(&schema_path).unwrap();
            fs::write(schema_path.join("t.sql"), "CREATE TABLE t (id INT);").unwrap();
        }

        let project = load_project(root, "default", Some("_stg"), &BTreeMap::new()).unwrap();

        assert_eq!(project.databases.len(), 2);
        assert!(project.databases.contains_key("db1_stg"));
        assert!(project.databases.contains_key("db2_stg"));
        assert_eq!(project.database_name_map.len(), 2);
        assert_eq!(project.database_name_map["db1"], "db1_stg");
        assert_eq!(project.database_name_map["db2"], "db2_stg");
    }

    #[test]
    fn test_load_project_no_suffix_unchanged() {
        let temp_dir = TempDir::new().unwrap();
        let root = temp_dir.path();

        let schema_path = root.join("models").join("mydb").join("public");
        fs::create_dir_all(&schema_path).unwrap();
        fs::write(schema_path.join("t.sql"), "CREATE TABLE t (id INT);").unwrap();

        let project = load_project(root, "default", None, &BTreeMap::new()).unwrap();

        assert!(project.databases.contains_key("mydb"));
        assert!(project.database_name_map.is_empty());

        let obj = &project.databases["mydb"].schemas["public"].objects[0];
        assert_eq!(obj.database, "mydb");
        assert_eq!(obj.schema, "public");
    }

    #[test]
    fn test_load_project_suffix_preserves_schema_names() {
        let temp_dir = TempDir::new().unwrap();
        let root = temp_dir.path();

        let schema_path = root.join("models").join("mydb").join("analytics");
        fs::create_dir_all(&schema_path).unwrap();
        fs::write(schema_path.join("t.sql"), "CREATE TABLE t (id INT);").unwrap();

        let project = load_project(root, "default", Some("_stg"), &BTreeMap::new()).unwrap();

        let db = &project.databases["mydb_stg"];
        assert!(db.schemas.contains_key("analytics"));
        assert_eq!(db.schemas["analytics"].name, "analytics");
    }

    #[test]
    fn test_load_project_suffix_database_mod_statements_rewritten() {
        let temp_dir = TempDir::new().unwrap();
        let root = temp_dir.path();

        let models_dir = root.join("models");
        let db_path = models_dir.join("mydb");
        let schema_path = db_path.join("public");
        fs::create_dir_all(&schema_path).unwrap();
        fs::write(schema_path.join("t.sql"), "CREATE TABLE t (id INT);").unwrap();

        // Database-level mod file referencing original db name
        fs::write(
            models_dir.join("mydb.sql"),
            "GRANT CREATE ON DATABASE mydb TO admin;",
        )
        .unwrap();

        let project = load_project(root, "default", Some("_stg"), &BTreeMap::new()).unwrap();

        let db = &project.databases["mydb_stg"];
        let mod_stmts = db.mod_statements.as_ref().unwrap();
        // The SQL should reference mydb_stg, not mydb
        let sql = format!("{}", mod_stmts[0]);
        assert!(sql.contains("mydb_stg"), "Expected 'mydb_stg' in: {}", sql);
    }

    #[test]
    fn test_database_name_map_only_populated_with_suffix() {
        let temp_dir = TempDir::new().unwrap();
        let root = temp_dir.path();

        let schema_path = root.join("models").join("mydb").join("public");
        fs::create_dir_all(&schema_path).unwrap();
        fs::write(schema_path.join("t.sql"), "CREATE TABLE t (id INT);").unwrap();

        // Without suffix
        let project = load_project(root, "default", None, &BTreeMap::new()).unwrap();
        assert!(project.database_name_map.is_empty());

        // With suffix
        let project = load_project(root, "default", Some("_stg"), &BTreeMap::new()).unwrap();
        assert_eq!(project.database_name_map.len(), 1);
    }

    #[test]
    fn test_plan_single_db_with_suffix() {
        use crate::project;

        let temp_dir = TempDir::new().unwrap();
        let root = temp_dir.path();

        let schema_path = root.join("models").join("testdb").join("public");
        fs::create_dir_all(&schema_path).unwrap();
        fs::write(
            schema_path.join("my_view.sql"),
            "CREATE VIEW my_view AS SELECT 1;",
        )
        .unwrap();

        // Need a project.toml for the project to load
        fs::write(root.join("project.toml"), "profile = \"default\"").unwrap();

        let planned =
            project::plan(root, "default", Some("_staging"), None, &BTreeMap::new()).unwrap();

        assert_eq!(planned.databases.len(), 1);
        assert_eq!(planned.databases[0].name, "testdb_staging");

        let obj = &planned.databases[0].schemas[0].objects[0];
        assert_eq!(obj.id.database, "testdb_staging");
    }

    #[test]
    fn test_plan_cross_db_references_rewritten() {
        use crate::project;

        let temp_dir = TempDir::new().unwrap();
        let root = temp_dir.path();

        // db1 has a table
        let db1_schema = root.join("models").join("db1").join("public");
        fs::create_dir_all(&db1_schema).unwrap();
        fs::write(db1_schema.join("tbl.sql"), "CREATE TABLE tbl (id INT);").unwrap();

        // db2 has a view that references db1.public.tbl
        let db2_schema = root.join("models").join("db2").join("public");
        fs::create_dir_all(&db2_schema).unwrap();
        fs::write(
            db2_schema.join("v.sql"),
            "CREATE VIEW v AS SELECT * FROM db1.public.tbl;",
        )
        .unwrap();

        fs::write(root.join("project.toml"), "profile = \"default\"").unwrap();

        let planned = project::plan(root, "default", Some("_stg"), None, &BTreeMap::new()).unwrap();

        // Find the view in db2_stg
        let db2 = planned
            .databases
            .iter()
            .find(|d| d.name == "db2_stg")
            .expect("db2_stg should exist");
        let view = &db2.schemas[0].objects[0];
        let sql = format!("{}", view.typed_object.stmt);
        assert!(
            sql.contains("db1_stg"),
            "Expected cross-db reference to be rewritten to db1_stg in: {}",
            sql
        );
    }

    #[test]
    fn test_plan_external_db_references_preserved() {
        use crate::project;

        let temp_dir = TempDir::new().unwrap();
        let root = temp_dir.path();

        let schema_path = root.join("models").join("mydb").join("public");
        fs::create_dir_all(&schema_path).unwrap();
        fs::write(
            schema_path.join("v.sql"),
            "CREATE VIEW v AS SELECT * FROM external.someschema.tbl;",
        )
        .unwrap();

        fs::write(root.join("project.toml"), "profile = \"default\"").unwrap();

        let planned = project::plan(root, "default", Some("_stg"), None, &BTreeMap::new()).unwrap();

        let view = &planned.databases[0].schemas[0].objects[0];
        let sql = format!("{}", view.typed_object.stmt);
        // external is not a project DB, so it should NOT be rewritten
        assert!(
            sql.contains("external"),
            "Expected external reference to be preserved in: {}",
            sql
        );
        assert!(
            !sql.contains("external_stg"),
            "External reference should NOT be suffixed in: {}",
            sql
        );
    }

    #[test]
    fn test_plan_no_suffix_unchanged() {
        use crate::project;

        let temp_dir = TempDir::new().unwrap();
        let root = temp_dir.path();

        let schema_path = root.join("models").join("mydb").join("public");
        fs::create_dir_all(&schema_path).unwrap();
        fs::write(schema_path.join("t.sql"), "CREATE TABLE t (id INT);").unwrap();

        fs::write(root.join("project.toml"), "profile = \"default\"").unwrap();

        let planned = project::plan(root, "default", None, None, &BTreeMap::new()).unwrap();

        assert_eq!(planned.databases[0].name, "mydb");
        assert_eq!(
            planned.databases[0].schemas[0].objects[0].id.database,
            "mydb"
        );
    }

    #[test]
    fn test_load_project_loads_all_variants() {
        let temp_dir = TempDir::new().unwrap();
        let root = temp_dir.path();

        let schema_path = root.join("models").join("mydb").join("public");
        fs::create_dir_all(&schema_path).unwrap();

        fs::write(
            schema_path.join("my_secret.sql"),
            "CREATE SECRET my_secret AS 'default_val';",
        )
        .unwrap();
        fs::write(
            schema_path.join("my_secret__staging.sql"),
            "CREATE SECRET my_secret AS 'staging_val';",
        )
        .unwrap();
        fs::write(
            schema_path.join("my_secret__prod.sql"),
            "CREATE SECRET my_secret AS 'prod_val';",
        )
        .unwrap();

        let project = load_project(root, "default", None, &BTreeMap::new()).unwrap();

        let schema = &project.databases["mydb"].schemas["public"];
        assert_eq!(schema.objects.len(), 1);
        assert_eq!(schema.objects[0].name, "my_secret");
        // Should have 3 variants: default + staging + prod
        assert_eq!(schema.objects[0].variants.len(), 3);

        let default_variant = schema.objects[0]
            .variants
            .iter()
            .find(|v| v.profile.is_none());
        assert!(default_variant.is_some(), "should have default variant");

        let staging_variant = schema.objects[0]
            .variants
            .iter()
            .find(|v| v.profile.as_deref() == Some("staging"));
        assert!(staging_variant.is_some(), "should have staging variant");

        let prod_variant = schema.objects[0]
            .variants
            .iter()
            .find(|v| v.profile.as_deref() == Some("prod"));
        assert!(prod_variant.is_some(), "should have prod variant");
    }

    #[test]
    fn test_plan_type_mismatch_across_profiles_errors() {
        use crate::project;

        let temp_dir = TempDir::new().unwrap();
        let root = temp_dir.path();

        let schema_path = root.join("models").join("mydb").join("public");
        fs::create_dir_all(&schema_path).unwrap();

        // Default is a secret, staging override is a view → type mismatch
        fs::write(schema_path.join("foo.sql"), "CREATE SECRET foo AS 'val';").unwrap();
        fs::write(
            schema_path.join("foo__staging.sql"),
            "CREATE VIEW foo AS SELECT 1;",
        )
        .unwrap();

        fs::write(root.join("project.toml"), "profile = \"default\"").unwrap();

        let result = project::plan(root, "default", None, None, &BTreeMap::new());
        assert!(
            result.is_err(),
            "type mismatch between profiles should error"
        );
        let err_str = result.unwrap_err().to_string();
        assert!(
            err_str.contains("profile variant type mismatch"),
            "error should mention type mismatch: {}",
            err_str
        );
    }

    #[test]
    fn test_plan_view_override_not_allowed() {
        use crate::project;

        let temp_dir = TempDir::new().unwrap();
        let root = temp_dir.path();

        let schema_path = root.join("models").join("mydb").join("public");
        fs::create_dir_all(&schema_path).unwrap();

        fs::write(schema_path.join("bar.sql"), "CREATE VIEW bar AS SELECT 1;").unwrap();
        fs::write(
            schema_path.join("bar__staging.sql"),
            "CREATE VIEW bar AS SELECT 2;",
        )
        .unwrap();

        fs::write(root.join("project.toml"), "profile = \"default\"").unwrap();

        let result = project::plan(root, "default", None, None, &BTreeMap::new());
        assert!(result.is_err(), "views cannot have profile overrides");
        let err_str = result.unwrap_err().to_string();
        assert!(
            err_str.contains("cannot have profile-specific overrides"),
            "error should mention overrides not allowed: {}",
            err_str
        );
    }

    #[test]
    fn test_plan_consistent_secret_profiles_succeeds() {
        use crate::project;

        let temp_dir = TempDir::new().unwrap();
        let root = temp_dir.path();

        let schema_path = root.join("models").join("mydb").join("public");
        fs::create_dir_all(&schema_path).unwrap();

        fs::write(
            schema_path.join("my_secret.sql"),
            "CREATE SECRET my_secret AS 'default_val';",
        )
        .unwrap();
        fs::write(
            schema_path.join("my_secret__staging.sql"),
            "CREATE SECRET my_secret AS 'staging_val';",
        )
        .unwrap();

        fs::write(root.join("project.toml"), "profile = \"default\"").unwrap();

        let result = project::plan(root, "default", None, None, &BTreeMap::new());
        assert!(
            result.is_ok(),
            "consistent secret profiles should work: {:?}",
            result.err()
        );
    }
}

//! Type definitions for the planned representation.

use super::super::ast::Cluster;
use super::super::typed;
use crate::project::object_id::ObjectId;
use mz_sql_parser::ast::*;
use std::collections::{HashMap, HashSet};

/// A database object with its dependencies.
#[derive(Debug)]
pub struct DatabaseObject {
    /// The object identifier
    pub id: ObjectId,
    /// The validated typed statement
    pub typed_object: typed::DatabaseObject,
    /// Set of objects this object depends on
    pub dependencies: HashSet<ObjectId>,
}

/// A module-level statement with context about where it should be executed.
///
/// Module statements are executed before object statements and come from
/// database.sql or schema.sql files. They're used for setup like grants,
/// comments, and other database/schema-level configuration.
#[derive(Debug)]
pub enum ModStatement<'a> {
    /// Database-level statement (from database.sql file)
    Database {
        /// The database name
        database: &'a str,
        /// The statement to execute
        statement: &'a mz_sql_parser::ast::Statement<Raw>,
    },
    /// Schema-level statement (from schema.sql file)
    Schema {
        /// The database name
        database: &'a str,
        /// The schema name
        schema: &'a str,
        /// The statement to execute
        statement: &'a mz_sql_parser::ast::Statement<Raw>,
    },
}

/// The type of objects contained in a schema.
///
/// Schemas are segregated by object type to prevent accidental recreation:
/// - Storage schemas contain tables, sinks, and tables from sources
/// - Compute schemas contain views and materialized views
/// - Empty schemas contain no objects
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SchemaType {
    /// Schema contains storage objects (tables, sinks)
    Storage,
    /// Schema contains computation objects (views, materialized views)
    Compute,
    /// Schema contains no objects
    Empty,
}

/// A schema containing objects with dependency information.
#[derive(Debug)]
pub struct Schema {
    pub name: String,
    pub objects: Vec<DatabaseObject>,
    /// Optional module-level statements (from schema.sql file)
    pub mod_statements: Option<Vec<mz_sql_parser::ast::Statement<Raw>>>,
    /// The type of objects in this schema (Storage, Compute, or Empty)
    pub schema_type: SchemaType,
}

/// A database containing schemas with dependency information.
#[derive(Debug)]
pub struct Database {
    pub name: String,
    pub schemas: Vec<Schema>,
    /// Optional module-level statements (from database.sql file)
    pub mod_statements: Option<Vec<mz_sql_parser::ast::Statement<Raw>>>,
}

/// A project with full dependency tracking.
#[derive(Debug)]
pub struct Project {
    pub databases: Vec<Database>,
    /// Global dependency graph: object_id -> set of dependencies
    pub dependency_graph: HashMap<ObjectId, HashSet<ObjectId>>,
    /// External dependencies: objects referenced but not defined in this project
    pub external_dependencies: HashSet<ObjectId>,
    /// Cluster dependencies: clusters referenced by indexes and materialized views
    pub cluster_dependencies: HashSet<Cluster>,
    /// Unit tests defined in the project, organized by the object they test
    pub tests: Vec<(ObjectId, crate::unit_test::UnitTest)>,
}

// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Dependency-aware project graph.
//!
//! [`Project`] is the final output of compilation — the result type of
//! [`compile_sync`](crate::project::compiler::compile_sync). It combines a
//! hierarchical view of the project with a flat dependency graph:
//!
//! - **`databases`** — the `database > schema > object` hierarchy used for
//!   iteration, deployment ordering, and module-statement execution.
//! - **`dependency_graph`** — a flat adjacency list (`ObjectId → {ObjectId}`)
//!   used for topological sort, change propagation, and reverse-dependency
//!   lookups.
//! - **`external_dependencies`** — objects referenced by the project but not
//!   defined in it (e.g., pre-existing sources). These appear as values in
//!   `dependency_graph` entries but are excluded from topological sorts and
//!   deployment.
//! - **`cluster_dependencies`** and **`replacement_schemas`** — deployment
//!   metadata extracted during graph assembly.
//!
//! **Invariant:** every `ObjectId` reachable through `databases` has an entry
//! in `dependency_graph`. External dependencies appear only in dependency-set
//! values and in the `external_dependencies` set.

use super::super::ast::Cluster;
use crate::project::SchemaQualifier;
use crate::project::ir::compiled;
use crate::project::ir::object_id::ObjectId;
use mz_sql_parser::ast::*;
use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Display;
use std::str::FromStr;

/// A database object with its dependencies.
#[derive(Debug)]
pub struct DatabaseObject {
    /// The object identifier
    pub id: ObjectId,
    /// The validated compiled object
    pub typed_object: compiled::DatabaseObject,
    /// Set of objects this object depends on
    pub dependencies: BTreeSet<ObjectId>,
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
        statement: &'a Statement<Raw>,
    },
    /// Schema-level statement (from schema.sql file)
    Schema {
        /// The database name
        database: &'a str,
        /// The schema name
        schema: &'a str,
        /// The statement to execute
        statement: &'a Statement<Raw>,
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

impl Display for SchemaType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SchemaType::Storage => write!(f, "storage"),
            SchemaType::Compute => write!(f, "compute"),
            SchemaType::Empty => write!(f, "empty"),
        }
    }
}

impl FromStr for SchemaType {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "storage" => Ok(SchemaType::Storage),
            "compute" => Ok(SchemaType::Compute),
            "empty" => Ok(SchemaType::Empty),
            _ => Err(format!("unknown schema type {}", s)),
        }
    }
}

/// A schema containing objects with dependency information.
#[derive(Debug)]
pub struct Schema {
    pub name: String,
    pub objects: Vec<DatabaseObject>,
    /// Optional module-level statements (from schema.sql file)
    pub mod_statements: Option<Vec<Statement<Raw>>>,
    /// The type of objects in this schema (Storage, Compute, or Empty)
    pub schema_type: SchemaType,
}

/// A database containing schemas with dependency information.
#[derive(Debug)]
pub struct Database {
    pub name: String,
    pub schemas: Vec<Schema>,
    /// Optional module-level statements (from database.sql file)
    pub mod_statements: Option<Vec<Statement<Raw>>>,
}

/// A project graph with full dependency tracking.
#[derive(Debug)]
pub struct Project {
    pub databases: Vec<Database>,
    /// Global dependency graph: object_id -> set of dependencies
    pub dependency_graph: BTreeMap<ObjectId, BTreeSet<ObjectId>>,
    /// External dependencies: objects referenced but not defined in this project
    pub external_dependencies: BTreeSet<ObjectId>,
    /// Cluster dependencies: clusters referenced by indexes and materialized views
    pub cluster_dependencies: BTreeSet<Cluster>,
    /// Unit tests defined in the project, organized by the object they test
    pub tests: Vec<(ObjectId, crate::project::ir::unit_test::UnitTest)>,
    /// Schemas that use replacement materialized views, derived from
    /// `SET api = stable` statements.
    pub replacement_schemas: BTreeSet<SchemaQualifier>,
    /// Objects whose compiled artifact was a cache miss in this run.
    /// Drives incremental typechecking: a miss means the file content
    /// changed (or never compiled before), so the object's typecheck
    /// result must be recomputed.
    pub compile_dirty: BTreeSet<ObjectId>,
}

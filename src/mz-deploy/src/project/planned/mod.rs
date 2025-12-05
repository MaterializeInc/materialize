//! Planned representation for Materialize projects.
//!
//! This module provides a dependency-aware representation of a Materialize project.
//! It builds on top of the validated typed representation and adds dependency tracking between objects,
//! enabling topological sorting for deployment order.
//!
//! # Transformation Flow
//!
//! ```text
//! raw::Project → typed::Project → planned::Project
//!                    ↓              ↓
//!              (validated)    (with dependencies)
//! ```
//!
//! # Dependency Extraction
//!
//! Dependencies are extracted from:
//! - View and materialized view queries (FROM clauses, JOINs, subqueries, CTEs)
//! - Tables created from sources
//! - Indexes (the table/view they're created on)
//! - Sinks (the object they read from)
//!
//! # Example
//!
//! ```text
//! CREATE TABLE users (...);
//! CREATE VIEW active_users AS SELECT * FROM users WHERE active = true;
//! CREATE INDEX idx ON active_users (id);
//!
//! Dependencies:
//! - active_users depends on: users
//! - idx depends on: active_users
//! ```
//!
//! # Module Structure
//!
//! - [`types`]: Type definitions (DatabaseObject, ModStatement, Schema, Database, Project)
//! - [`project`]: Project implementation methods (topological_sort, iter_objects, etc.)
//! - [`dependency`]: Dependency extraction from typed representation

mod dependency;
mod project;
mod types;

// Re-export all public types and functions
pub use dependency::{extract_dependencies, extract_external_indexes};
pub use types::{Database, DatabaseObject, ModStatement, Project, Schema, SchemaType};

#[cfg(test)]
mod tests;

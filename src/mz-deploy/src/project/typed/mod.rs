//! Typed representation for Materialize projects.
//!
//! This module provides a validated, type-safe representation of a Materialize project
//! structure. It transforms the raw parsed AST from the `raw` module into a semantically
//! validated typed representation that enforces structural constraints and relationships.
//!
//! # Transformation Flow
//!
//! ```text
//! File System -> raw::Project -> typed::Project (validated)
//!                  |              |
//!              raw::Database -> typed::Database
//!                  |              |
//!              raw::Schema   -> typed::Schema
//!                  |              |
//!          raw::DatabaseObject -> typed::DatabaseObject
//! ```
//!
//! # Validation Rules
//!
//! During transformation from raw to typed, the following validations are performed:
//!
//! - **Object Identity**: Each file must contain exactly one primary CREATE statement
//!   (table, view, source, etc.), and the object name must match the file name.
//!
//! - **Path Consistency**: Qualified names in CREATE statements must match the directory
//!   structure (e.g., `CREATE TABLE db.schema.table` in `db/schema/table.sql`).
//!
//! - **Reference Validation**: Supporting statements (indexes, grants, comments) must
//!   reference the primary object defined in the same file.
//!
//! - **Type Consistency**: GRANT and COMMENT statements must use the correct object type
//!   for the primary object.
//!
//! # Module Structure
//!
//! - [`types`]: Core type definitions (FullyQualifiedName, DatabaseObject, Schema, Database, Project)
//! - [`validation`]: Validation helper functions used during conversion
//! - [`conversion`]: TryFrom implementations for raw to typed conversion

mod conversion;
mod types;
mod validation;

// Re-export all public types from types module
pub use types::{Database, DatabaseObject, FullyQualifiedName, Project, Schema};

#[cfg(test)]
mod tests;

//! Validation functions for typed representation.
//!
//! This module contains helper functions used during the conversion from
//! raw to typed representation to validate various constraints.
//!
//! # Submodules
//!
//! - [`identifiers`]: Identifier format validation (naming rules)
//! - [`clusters`]: Cluster specification validation for indexes, MVs, sinks, sources
//! - [`references`]: Reference validation for indexes, grants, and comments
//! - [`mod_statements`]: Database and schema mod file statement validation
//! - [`schema_constraints`]: Schema-level structural constraint validation

mod clusters;
mod constraints;
mod identifiers;
mod mod_statements;
mod references;
mod schema_constraints;

// Re-export all public items so existing `use super::validation::{...}` paths continue to work.
pub(super) use clusters::{
    validate_constraint_clusters, validate_index_clusters, validate_mv_cluster,
    validate_sink_cluster, validate_source_cluster,
};
pub(super) use constraints::validate_constraint_enforcement;
pub use constraints::{validate_constraint_columns, validate_constraint_fk_targets};
#[cfg(test)]
pub(super) use identifiers::{IdentifierKind, validate_identifier_format};
pub(super) use identifiers::{validate_fqn_identifiers, validate_ident};
pub(super) use mod_statements::{validate_database_mod_statements, validate_schema_mod_statements};
pub(super) use references::{
    validate_comment_references, validate_constraint_references, validate_grant_references,
    validate_index_references,
};
pub(super) use schema_constraints::validate_no_storage_and_computation_in_schema;

//! Project loading, validation, and deployment planning.
//!
//! This module implements two conceptual layers for preparing a Materialize
//! project for deployment:
//!
//! ## Layer 1 — Pipeline (`raw` → `typed` → `planned`)
//!
//! Project definitions on disk go through three stages:
//!
//! 1. **`raw`** — Loads `.sql` files from the project directory and parses them
//!    into AST statements. No semantic validation.
//! 2. **`typed`** — Validates the raw AST: checks naming conventions, ensures
//!    one main statement per file, validates cross-references (indexes, grants,
//!    comments), and builds a typed intermediate representation.
//! 3. **`planned`** — Extracts dependency graphs from the typed representation,
//!    computes topological sort order, and produces the final deployment plan.
//!
//! The top-level [`plan()`] function runs the full pipeline.
//!
//! ## Layer 2 — Deployment Analysis
//!
//! Once a planned project exists, these sibling modules analyze what needs
//! deploying:
//!
//! - **`changeset`** — Dirty propagation algorithm: compares the current project
//!   against a prior deployment snapshot and computes which objects, schemas, and
//!   clusters must be redeployed (fixed-point Datalog-style iteration).
//! - **`deployment_snapshot`** — Hash-based change detection: captures the
//!   normalized typed representation of every object so that formatting or
//!   comment-only changes don't trigger redeployments.
//! - **`normalize`** — AST name transformation: rewrites identifiers for staging
//!   (e.g., prefixing schema names) so that blue/green deployments don't collide.
//!
//! These modules live in `project/` because they are tightly coupled to project
//! internals (`planned::Project`, `ObjectId`, `ast::Statement`, etc.).
//!
//! ## Supporting Modules
//!
//! - **`clusters`** and **`roles`** — Load cluster and role definitions that
//!   exist alongside the pipeline stages.
//! - **`ast`** — Project-specific AST wrapper types (`Statement`, `Cluster`,
//!   `DatabaseIdent`).
//! - **`object_id`** — Fully qualified object identifier used throughout the
//!   pipeline.
//! - **`error`** — Structured error types for every stage.

use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;

pub mod ast;
pub mod changeset;
pub mod clusters;
pub mod deployment_snapshot;
pub mod error;
pub mod network_policies;
pub mod normalize;
pub mod object_id;
mod parser;
pub mod planned;
pub mod profile_files;
pub mod raw;
pub mod roles;
pub mod typed;
mod variables;

// Re-export commonly used types
pub use planned::ModStatement;

/// A `(database_name, schema_name)` pair identifying a schema within a project.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct SchemaQualifier {
    pub database: String,
    pub schema: String,
}

impl SchemaQualifier {
    pub fn new(database: String, schema: String) -> Self {
        Self { database, schema }
    }

    /// Collect the distinct `(database, schema)` pairs from a slice of objects.
    pub fn collect_from(objs: &[&planned::DatabaseObject]) -> BTreeSet<Self> {
        objs.iter()
            .map(|obj| Self::new(obj.id.database.clone(), obj.id.schema.clone()))
            .collect()
    }
}

/// Load, validate, and convert a project to a planned deployment representation.
pub fn plan<P: AsRef<Path>>(
    root: P,
    profile: &str,
    suffix: Option<&str>,
    cluster_suffix: Option<&str>,
    variables: &BTreeMap<String, String>,
) -> Result<planned::Project, error::ProjectError> {
    let raw_project = raw::load_project(root, profile, suffix, variables)?;
    let db_name_map = raw_project.database_name_map.clone();
    let mut typed_project = typed::Project::try_from(raw_project)?;
    if !db_name_map.is_empty() {
        typed_project.rewrite_database_references(&db_name_map);
    }
    if let Some(cs) = cluster_suffix {
        let cluster_name_map = build_cluster_name_map(&typed_project, cs);
        if !cluster_name_map.is_empty() {
            typed_project.rewrite_cluster_references(&cluster_name_map);
        }
    }
    let planned_project = planned::Project::from(typed_project);
    Ok(planned_project)
}

/// Build a map from original cluster name → suffixed cluster name for all
/// clusters referenced across the typed project.
fn build_cluster_name_map(
    project: &typed::Project,
    cluster_suffix: &str,
) -> BTreeMap<String, String> {
    let mut names = BTreeSet::new();
    for db in &project.databases {
        for schema in &db.schemas {
            for obj in &schema.objects {
                names.extend(obj.clusters());
            }
        }
    }
    names
        .into_iter()
        .map(|name| {
            let suffixed = format!("{}{}", name, cluster_suffix);
            (name, suffixed)
        })
        .collect()
}

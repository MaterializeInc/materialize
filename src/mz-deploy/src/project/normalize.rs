//! Name normalization for SQL statements using the visitor pattern.
//!
//! This module provides a flexible framework for transforming object names in SQL
//! statements. It uses a trait-based visitor pattern to support different normalization
//! strategies while sharing the same traversal logic.
//!
//! # Normalization Strategies
//!
//! - **Fully Qualifying**: Transforms names to `database.schema.object` format
//! - **Flattening**: Transforms names to `database_schema_object` format (single identifier)
//!
//! # Usage
//!
//! ```rust,ignore
//! use mz_deploy::project::normalize::NormalizingVisitor;
//!
//! // Create a fully qualifying visitor
//! let visitor = NormalizingVisitor::fully_qualifying(&fqn);
//!
//! // Or create a flattening visitor
//! let visitor = NormalizingVisitor::flattening(&fqn);
//! ```
//!
//! # Module Structure
//!
//! - [`transformers`]: Name transformation strategies (FullyQualifying, Flattening, Staging)
//! - [`visitor`]: The NormalizingVisitor that traverses SQL AST and applies transformations

mod transformers;
mod visitor;

// Re-export all public types and functions
pub use transformers::{
    ClusterTransformer, FlatteningTransformer, FullyQualifyingTransformer, NameTransformer,
    StagingTransformer,
};
pub use visitor::NormalizingVisitor;

use mz_sql_parser::ast::{CreateIndexStatement, Ident, Raw, RawClusterName};

/// Transform cluster names in index statements for staging environments.
///
/// This is a standalone function that transforms cluster references without
/// needing a full `NormalizingVisitor`. Use this when you only need to rename
/// clusters (e.g., `quickstart` -> `quickstart_staging`) without transforming
/// object names.
///
/// # Arguments
/// * `indexes` - Slice of index statements to transform in place
/// * `staging_suffix` - The suffix to append to cluster names (e.g., "_staging")
///
/// # Example
/// ```rust,ignore
/// transform_cluster_names_for_staging(&mut indexes, "_staging");
/// // Transforms: IN CLUSTER quickstart -> IN CLUSTER quickstart_staging
/// ```
pub fn transform_cluster_names_for_staging(
    indexes: &mut [CreateIndexStatement<Raw>],
    staging_suffix: &str,
) {
    for index in indexes {
        if let Some(ref mut cluster_name) = index.in_cluster {
            if let RawClusterName::Unresolved(ident) = cluster_name {
                let new_name = format!("{}{}", ident, staging_suffix);
                *cluster_name =
                    RawClusterName::Unresolved(Ident::new(&new_name).expect("valid cluster name"));
            }
        }
    }
}

#[cfg(test)]
mod tests;

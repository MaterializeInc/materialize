//! Name transformation strategies for SQL AST normalization.
//!
//! This module provides different strategies for transforming object names in SQL statements.
//! Each transformer implements the `NameTransformer` trait, allowing the `NormalizingVisitor`
//! to apply different transformation strategies using the same traversal logic.

use super::super::typed::FullyQualifiedName;
use crate::project::object_id::ObjectId;
use mz_sql_parser::ast::*;

/// Trait for transforming object names in SQL AST nodes.
///
/// Implementations of this trait define how names should be transformed
/// (e.g., fully qualified, flattened, etc.).
pub trait NameTransformer {
    /// Transform a name using the implementing strategy.
    ///
    /// Takes an `UnresolvedItemName` and returns a transformed version according
    /// to the strategy. The input may be partially qualified (1, 2, or 3 parts).
    fn transform_name(&self, name: &UnresolvedItemName) -> UnresolvedItemName;

    /// Get the database name from the transformer's FQN context.
    fn database_name(&self) -> &str;
}

/// Transforms names to be fully qualified (`database.schema.object`).
///
/// This is the default normalization strategy that ensures all object references
/// use the 3-part qualified format.
pub struct FullyQualifyingTransformer<'a> {
    pub(crate) fqn: &'a FullyQualifiedName,
}

impl<'a> NameTransformer for FullyQualifyingTransformer<'a> {
    fn transform_name(&self, name: &UnresolvedItemName) -> UnresolvedItemName {
        match name.0.len() {
            1 => {
                // Unqualified: object only
                // Convert to database.schema.object
                let object = name.0[0].clone();
                let database = Ident::new(self.fqn.database()).expect("valid database identifier");
                let schema = Ident::new(self.fqn.schema()).expect("valid schema identifier");
                UnresolvedItemName(vec![database, schema, object])
            }
            2 => {
                // Schema-qualified: schema.object
                // Prepend database to make database.schema.object
                let schema = name.0[0].clone();
                let object = name.0[1].clone();
                let database = Ident::new(self.fqn.database()).expect("valid database identifier");
                UnresolvedItemName(vec![database, schema, object])
            }
            _ => {
                // Already fully qualified or invalid - return as-is
                name.clone()
            }
        }
    }

    fn database_name(&self) -> &str {
        self.fqn.database()
    }
}

/// Transforms names to be flattened (`database_schema_object`).
///
/// This strategy creates a single unqualified identifier by concatenating
/// the database, schema, and object names with underscores. Useful for
/// temporary objects that need unqualified names.
pub struct FlatteningTransformer<'a> {
    pub(crate) fqn: &'a FullyQualifiedName,
}

impl<'a> NameTransformer for FlatteningTransformer<'a> {
    fn transform_name(&self, name: &UnresolvedItemName) -> UnresolvedItemName {
        // First, fully qualify the name to ensure we have all parts
        let fully_qualified = match name.0.len() {
            1 => {
                // Unqualified: object only - use FQN context
                vec![
                    self.fqn.database().to_string(),
                    self.fqn.schema().to_string(),
                    name.0[0].to_string(),
                ]
            }
            2 => {
                // Schema-qualified: schema.object - use FQN database
                vec![
                    self.fqn.database().to_string(),
                    name.0[0].to_string(),
                    name.0[1].to_string(),
                ]
            }
            3 => {
                // Already fully qualified
                vec![
                    name.0[0].to_string(),
                    name.0[1].to_string(),
                    name.0[2].to_string(),
                ]
            }
            _ => {
                // Invalid - return as-is
                return name.clone();
            }
        };

        // Flatten to single identifier: database_schema_object
        let flattened = fully_qualified.join("_");
        let flattened_ident = Ident::new(&flattened).expect("valid flattened identifier");
        UnresolvedItemName(vec![flattened_ident])
    }

    fn database_name(&self) -> &str {
        self.fqn.database()
    }
}

/// Transforms names for staging environments by appending a suffix to schema names.
///
/// This strategy is used to create isolated staging environments where all objects
/// are deployed to schema names with a suffix (e.g., `public_staging`), and all
/// clusters are renamed with the same suffix (e.g., `quickstart_staging`).
///
/// External dependencies (objects not defined in the project) are NOT transformed.
/// Objects not being deployed in this staging run are also treated as external.
pub struct StagingTransformer<'a> {
    fqn: &'a FullyQualifiedName,
    staging_suffix: String,
    external_dependencies: &'a std::collections::HashSet<ObjectId>,
    objects_to_deploy: Option<&'a std::collections::HashSet<ObjectId>>,
}

impl<'a> StagingTransformer<'a> {
    /// Create a new staging transformer with the given suffix.
    ///
    /// # Arguments
    /// * `fqn` - The fully qualified name context
    /// * `staging_suffix` - The suffix to append (e.g., "_staging")
    /// * `external_dependencies` - Set of external dependencies that should NOT be transformed
    /// * `objects_to_deploy` - Optional set of objects being deployed; objects not in this set are treated as external
    pub fn new(
        fqn: &'a FullyQualifiedName,
        staging_suffix: String,
        external_dependencies: &'a std::collections::HashSet<ObjectId>,
        objects_to_deploy: Option<&'a std::collections::HashSet<ObjectId>>,
    ) -> Self {
        Self {
            fqn,
            staging_suffix,
            external_dependencies,
            objects_to_deploy,
        }
    }

    /// Check if a name refers to an external dependency or an object not being deployed
    pub(crate) fn is_external(&self, name: &UnresolvedItemName) -> bool {
        use ObjectId;

        // Try to construct an ObjectId from the name
        let object_id = match name.0.len() {
            1 => {
                // Unqualified: use default database and schema
                ObjectId {
                    database: self.fqn.database().to_string(),
                    schema: self.fqn.schema().to_string(),
                    object: name.0[0].to_string(),
                }
            }
            2 => {
                // Schema-qualified: use default database
                ObjectId {
                    database: self.fqn.database().to_string(),
                    schema: name.0[0].to_string(),
                    object: name.0[1].to_string(),
                }
            }
            3 => {
                // Fully qualified
                ObjectId {
                    database: name.0[0].to_string(),
                    schema: name.0[1].to_string(),
                    object: name.0[2].to_string(),
                }
            }
            _ => return false, // Invalid name, not external
        };

        // Check if it's in the external dependencies
        if self.external_dependencies.contains(&object_id) {
            return true;
        }

        // If objects_to_deploy is specified, check if this object is NOT in that set
        // If not being deployed, treat as external
        if let Some(objects_to_deploy) = self.objects_to_deploy
            && !objects_to_deploy.contains(&object_id)
        {
            return true;
        }

        false
    }
}

impl<'a> NameTransformer for StagingTransformer<'a> {
    fn transform_name(&self, name: &UnresolvedItemName) -> UnresolvedItemName {
        // Check if this is an external dependency - if so, don't transform it
        if self.is_external(name) {
            return name.clone();
        }

        match name.0.len() {
            1 => {
                // Unqualified: object only
                // Add staging suffix to schema: database.schema_staging.object
                let object = name.0[0].clone();
                let database = Ident::new(self.fqn.database()).expect("valid database identifier");
                let staging_schema = format!("{}{}", self.fqn.schema(), self.staging_suffix);
                let schema = Ident::new(&staging_schema).expect("valid schema identifier");
                UnresolvedItemName(vec![database, schema, object])
            }
            2 => {
                // Schema-qualified: schema.object
                // Add staging suffix to schema: database.schema_staging.object
                let schema_name = format!("{}{}", name.0[0], self.staging_suffix);
                let schema = Ident::new(&schema_name).expect("valid schema identifier");
                let object = name.0[1].clone();
                let database = Ident::new(self.fqn.database()).expect("valid database identifier");
                UnresolvedItemName(vec![database, schema, object])
            }
            3 => {
                // Fully qualified: database.schema.object
                // Add staging suffix to schema: database.schema_staging.object
                let database = name.0[0].clone();
                let schema_name = format!("{}{}", name.0[1], self.staging_suffix);
                let schema = Ident::new(&schema_name).expect("valid schema identifier");
                let object = name.0[2].clone();
                UnresolvedItemName(vec![database, schema, object])
            }
            _ => {
                // Invalid - return as-is
                name.clone()
            }
        }
    }

    fn database_name(&self) -> &str {
        self.fqn.database()
    }
}

/// Extension trait for transformers that also transform cluster names.
///
/// This trait allows transformers to modify cluster references in addition to
/// object names. It's used by the StagingTransformer to rename clusters for
/// staging environments.
pub trait ClusterTransformer: NameTransformer {
    /// Transform a cluster name according to the strategy.
    fn transform_cluster(&self, cluster_name: &Ident) -> Ident;

    /// Get the original cluster name from a transformed name.
    ///
    /// This is used to look up production cluster configurations when creating
    /// staging clusters.
    fn get_original_cluster_name(&self, staged_name: &str) -> String;
}

impl<'a> ClusterTransformer for StagingTransformer<'a> {
    fn transform_cluster(&self, cluster_name: &Ident) -> Ident {
        // Transform: quickstart → quickstart_staging
        let staging_name = format!("{}{}", cluster_name, self.staging_suffix);
        Ident::new(&staging_name).expect("valid cluster identifier")
    }

    fn get_original_cluster_name(&self, staged_name: &str) -> String {
        // Reverse transform: quickstart_staging → quickstart
        staged_name
            .strip_suffix(&self.staging_suffix)
            .unwrap_or(staged_name)
            .to_string()
    }
}

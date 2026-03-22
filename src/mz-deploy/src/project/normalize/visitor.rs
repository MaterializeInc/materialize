//! The NormalizingVisitor for traversing SQL AST and applying name transformations.
//!
//! This module contains the `NormalizingVisitor` struct which transforms object
//! names in SQL statements using a configurable strategy (via the `NameTransformer`
//! trait). Query-level traversal is delegated to mz-sql-parser's auto-generated
//! [`VisitMut`] trait — the visitor overrides `visit_query_mut` (for CTE scope
//! management) and `visit_table_factor_mut` (for name transformation and implicit
//! aliasing). All other AST nodes (expressions, set operations, etc.) are handled
//! by the default traversal.
//!
//! ## CTE Scoping
//!
//! Common Table Expressions (CTEs) introduce names that shadow real database
//! objects. The visitor uses [`CteScope`](crate::project::cte_scope::CteScope)
//! to track which names are currently in scope:
//!
//! - When entering a `WITH` clause, all CTE names from that clause are pushed
//!   as a new scope level.
//! - Unqualified single-identifier references are checked against the scope
//!   stack — if a match is found, the reference is **not transformed** (it
//!   refers to a CTE, not a database object).
//! - When leaving a `WITH` clause, the scope is popped.
//!
//! **Key Insight:** CTE names can only be referenced by their unqualified
//! name. Any multi-part reference (e.g., `schema.name`) is always a database
//! object reference and is always transformed.
//!
//! ## Implicit Aliasing
//!
//! When a table reference in a FROM clause is transformed (e.g., `sales` →
//! `materialize.public.sales`), an implicit alias preserving the original
//! table name is attached. This ensures that column references like
//! `sales.column` continue to resolve correctly after transformation.

use super::super::cte_scope::CteScope;
use super::super::typed::FullyQualifiedName;
use super::transformers::{
    ClusterTransformer, FlatteningTransformer, FullyQualifyingTransformer, NameTransformer,
    StagingTransformer,
};
use crate::project::object_id::ObjectId;
use mz_sql_parser::ast::visit_mut::{self, VisitMut};
use mz_sql_parser::ast::*;

/// Visitor that traverses SQL AST and transforms names using a given strategy.
///
/// This struct is generic over the `NameTransformer` trait, allowing different
/// transformation strategies to reuse the same traversal logic.
///
/// Implements [`VisitMut`] to delegate query-level traversal to mz-sql-parser's
/// auto-generated visitor, overriding only `visit_query_mut` (CTE scope) and
/// `visit_table_factor_mut` (name transformation + implicit aliasing).
pub struct NormalizingVisitor<T: NameTransformer> {
    transformer: T,
    cte_scope: CteScope,
}

impl<T: NameTransformer> NormalizingVisitor<T> {
    /// Create a new visitor with the given transformer.
    pub fn new(transformer: T) -> Self {
        Self {
            transformer,
            cte_scope: CteScope::new(),
        }
    }

    /// Get a reference to the transformer.
    pub fn transformer(&self) -> &T {
        &self.transformer
    }

    /// Normalize a RawItemName to be transformed according to the strategy.
    ///
    /// Converts partially qualified or unqualified object references using
    /// the current file's FQN context.
    ///
    /// CTEs (Common Table Expressions) are not transformed - they remain as-is.
    pub fn normalize_raw_item_name(&self, name: &mut RawItemName) {
        let unresolved = name.name_mut();

        // Check if this is a CTE reference (unqualified single identifier)
        // CTEs can only be referenced by their unqualified name
        if unresolved.0.len() == 1 {
            let name_str = unresolved.0[0].to_string();
            if self.cte_scope.is_cte(&name_str) {
                // This is a CTE reference - don't transform it
                crate::verbose!("Skipping transform of CTE reference: {}", name_str);
                return;
            }
            crate::verbose!("Transforming non-CTE reference: {}", name_str);
        }

        *unresolved = self.transformer.transform_name(unresolved);
    }

    /// Normalize an UnresolvedItemName to be transformed according to the strategy.
    ///
    /// Similar to normalize_raw_item_name, but works directly with UnresolvedItemName.
    pub fn normalize_unresolved_item_name(&self, name: &mut UnresolvedItemName) {
        *name = self.transformer.transform_name(name);
    }

    /// Normalize an UnresolvedSchemaName to be fully qualified (`database.schema`).
    ///
    /// Converts unqualified schema names (e.g., `public`) to fully qualified
    /// names (e.g., `materialize.public`) using the current file's FQN context.
    pub fn normalize_unresolved_schema_name(&self, name: &mut UnresolvedSchemaName) {
        match name.0.len() {
            1 => {
                // Unqualified: schema only (e.g., "public")
                // Prepend database to make database.schema
                let schema = name.0[0].clone();
                let database = Ident::new(self.transformer.database_name())
                    .expect("valid database identifier");
                name.0 = vec![database, schema];
            }
            _ => {
                // Already qualified or invalid - leave as-is
            }
        }
    }

    /// Normalize connection references in CREATE SINK statements.
    ///
    /// Handles both Kafka and Iceberg sink types, ensuring their connection
    /// references are normalized.
    pub fn normalize_sink_connection(&self, connection: &mut CreateSinkConnection<Raw>) {
        match connection {
            CreateSinkConnection::Kafka { connection, .. } => {
                self.normalize_raw_item_name(connection);
            }
            CreateSinkConnection::Iceberg { connection, .. } => {
                self.normalize_raw_item_name(connection);
            }
        }
    }

    /// Normalize the connection reference in CREATE SOURCE statements.
    ///
    /// Sources reference a connection (Kafka, Postgres, etc.) that needs to be
    /// normalized to a fully qualified name.
    pub fn normalize_source_connection(&self, connection: &mut CreateSourceConnection<Raw>) {
        match connection {
            CreateSourceConnection::Kafka { connection, .. }
            | CreateSourceConnection::Postgres { connection, .. }
            | CreateSourceConnection::SqlServer { connection, .. }
            | CreateSourceConnection::MySql { connection, .. } => {
                self.normalize_raw_item_name(connection);
            }
            CreateSourceConnection::LoadGenerator { .. } => {}
        }
    }

    /// Normalize connection option references in CREATE CONNECTION statements.
    ///
    /// Handles secret references, item references, AWS PrivateLink connections,
    /// and Kafka broker tunnels within connection options.
    pub fn normalize_connection_options(&self, options: &mut [ConnectionOption<Raw>]) {
        for option in options {
            if let Some(ref mut value) = option.value {
                self.normalize_with_option_value(value);
            }
        }
    }

    /// Normalize a single WithOptionValue, recursing into nested structures.
    fn normalize_with_option_value(&self, value: &mut WithOptionValue<Raw>) {
        match value {
            WithOptionValue::Secret(name) | WithOptionValue::Item(name) => {
                self.normalize_raw_item_name(name);
            }
            WithOptionValue::ConnectionAwsPrivatelink(pl) => {
                self.normalize_raw_item_name(&mut pl.connection);
            }
            WithOptionValue::ConnectionKafkaBroker(broker) => match &mut broker.tunnel {
                KafkaBrokerTunnel::SshTunnel(name) => self.normalize_raw_item_name(name),
                KafkaBrokerTunnel::AwsPrivatelink(aws) => {
                    self.normalize_raw_item_name(&mut aws.connection)
                }
                KafkaBrokerTunnel::Direct => {}
            },
            WithOptionValue::Sequence(items) => {
                for item in items {
                    self.normalize_with_option_value(item);
                }
            }
            _ => {}
        }
    }

    /// Normalize all table references in a query (used for views and materialized views).
    ///
    /// Delegates to the [`VisitMut`] implementation which handles CTE scoping
    /// and recursive traversal automatically.
    pub fn normalize_query(&mut self, query: &mut Query<Raw>) {
        self.visit_query_mut(query);
    }

    /// Normalize index references.
    ///
    /// Indexes reference the table/view they're created on, and this reference
    /// needs to be normalized.
    pub fn normalize_index_references(&self, indexes: &mut [CreateIndexStatement<Raw>]) {
        for index in indexes {
            self.normalize_raw_item_name(&mut index.on_name);
        }
    }

    /// Normalize cluster references in indexes.
    ///
    /// Indexes can specify an IN CLUSTER clause, and these cluster references
    /// need to be normalized for staging environments.
    pub fn normalize_index_clusters(&self, indexes: &mut [CreateIndexStatement<Raw>])
    where
        T: ClusterTransformer,
    {
        for index in indexes {
            self.normalize_cluster_name(&mut index.in_cluster);
        }
    }

    /// Normalize constraint references.
    ///
    /// Constraints reference the table/view they're created on via `on_name`,
    /// and foreign keys reference another object via `references.object`.
    /// Both need to be normalized.
    pub fn normalize_constraint_references(
        &self,
        constraints: &mut [CreateConstraintStatement<Raw>],
    ) {
        for constraint in constraints {
            self.normalize_raw_item_name(&mut constraint.on_name);
            if let Some(ref mut refs) = constraint.references {
                self.normalize_raw_item_name(&mut refs.object);
            }
        }
    }

    /// Normalize cluster references in constraints.
    ///
    /// Constraints can specify an IN CLUSTER clause, and these cluster references
    /// need to be normalized for staging environments.
    pub fn normalize_constraint_clusters(&self, constraints: &mut [CreateConstraintStatement<Raw>])
    where
        T: ClusterTransformer,
    {
        for constraint in constraints {
            self.normalize_cluster_name(&mut constraint.in_cluster);
        }
    }

    /// Normalize grant target references.
    ///
    /// GRANT statements reference the object they grant permissions on, and these
    /// references need to be normalized.
    pub fn normalize_grant_references(&self, grants: &mut [GrantPrivilegesStatement<Raw>]) {
        for grant in grants {
            if let GrantTargetSpecification::Object {
                object_spec_inner, ..
            } = &mut grant.target
                && let GrantTargetSpecificationInner::Objects { names } = object_spec_inner
            {
                for obj in names {
                    if let UnresolvedObjectName::Item(item_name) = obj {
                        self.normalize_unresolved_item_name(item_name);
                    }
                }
            }
        }
    }

    /// Normalize comment object references.
    ///
    /// COMMENT statements reference the object they comment on, and these
    /// references need to be normalized.
    pub fn normalize_comment_references(&self, comments: &mut [CommentStatement<Raw>]) {
        for comment in comments {
            match &mut comment.object {
                CommentObjectType::Table { name }
                | CommentObjectType::View { name }
                | CommentObjectType::MaterializedView { name }
                | CommentObjectType::Source { name }
                | CommentObjectType::Sink { name }
                | CommentObjectType::Connection { name }
                | CommentObjectType::Secret { name } => {
                    self.normalize_raw_item_name(name);
                }
                CommentObjectType::Column { name } => {
                    // For columns, normalize the table/view reference (the relation)
                    self.normalize_raw_item_name(&mut name.relation);
                }
                _ => {
                    // Other comment types don't need normalization
                }
            }
        }
    }

    /// Normalize a cluster name using a ClusterTransformer.
    ///
    /// This method transforms cluster references in statements that support
    /// the `IN CLUSTER` clause. It's primarily used by the StagingTransformer
    /// to rename clusters for staging environments.
    ///
    /// # Type Parameter
    /// `T` must implement `ClusterTransformer` for this method to be callable.
    pub fn normalize_cluster_name(&self, cluster: &mut Option<RawClusterName>)
    where
        T: ClusterTransformer,
    {
        if let Some(cluster_name) = cluster {
            match cluster_name {
                RawClusterName::Unresolved(ident) => {
                    let transformed = self.transformer.transform_cluster(ident);
                    *cluster_name = RawClusterName::Unresolved(transformed);
                }
                RawClusterName::Resolved(_) => {
                    // Already resolved, leave as-is
                }
            }
        }
    }
}

impl<T: NameTransformer> VisitMut<'_, Raw> for NormalizingVisitor<T> {
    fn visit_query_mut(&mut self, node: &mut Query<Raw>) {
        let names = CteScope::collect_cte_names(&node.ctes);
        self.cte_scope.push(names);
        visit_mut::visit_query_mut(self, node);
        self.cte_scope.pop();
    }

    fn visit_table_factor_mut(&mut self, node: &mut TableFactor<Raw>) {
        match node {
            TableFactor::Table { name, alias } => {
                // Save the original table name (the last part) before transformation.
                // This will be used as an implicit alias if one doesn't exist.
                let original_table_name = match name.name().0.len() {
                    1 => {
                        let name_str = name.name().0[0].to_string();
                        // Don't create an alias if this is a CTE reference (it won't be transformed)
                        if !self.cte_scope.is_cte(&name_str) {
                            Some(name.name().0[0].clone())
                        } else {
                            None
                        }
                    }
                    2 | 3 => Some(name.name().0.last().unwrap().clone()),
                    _ => None,
                };

                // Normalize the table name (e.g., "sales" -> "materialize.public.sales")
                self.normalize_raw_item_name(name);

                // If there's no explicit alias and we have an original table name, create
                // an implicit alias so that qualified column references like "sales.column"
                // continue to work after transformation.
                if alias.is_none() {
                    if let Some(original) = original_table_name {
                        *alias = Some(TableAlias {
                            name: original,
                            columns: vec![],
                            strict: false,
                        });
                    }
                }
            }
            _ => visit_mut::visit_table_factor_mut(self, node),
        }
    }
}

// Convenience constructors for common use cases
impl<'a> NormalizingVisitor<FullyQualifyingTransformer<'a>> {
    /// Create a visitor that fully qualifies names (`database.schema.object`).
    pub fn fully_qualifying(fqn: &'a FullyQualifiedName) -> Self {
        Self::new(FullyQualifyingTransformer {
            fqn,
            database_name_map: None,
        })
    }

    /// Create a visitor that fully qualifies names and optionally rewrites
    /// cross-database references using a database name map.
    pub fn fully_qualifying_with_db_map(
        fqn: &'a FullyQualifiedName,
        database_name_map: Option<&'a std::collections::BTreeMap<String, String>>,
    ) -> Self {
        Self::new(FullyQualifyingTransformer {
            fqn,
            database_name_map,
        })
    }
}

impl<'a> NormalizingVisitor<FlatteningTransformer<'a>> {
    /// Create a visitor that flattens names (`database_schema_object`).
    pub fn flattening(fqn: &'a FullyQualifiedName) -> Self {
        Self::new(FlatteningTransformer { fqn })
    }
}

impl<'a> NormalizingVisitor<StagingTransformer<'a>> {
    /// Create a visitor that transforms names for staging environments.
    ///
    /// This visitor appends a suffix to schema and cluster names to create
    /// isolated staging environments. External dependencies and objects not
    /// being deployed are NOT transformed.
    ///
    /// # Arguments
    /// * `fqn` - The fully qualified name context
    /// * `suffix` - The suffix to append (e.g., "_staging")
    /// * `external_dependencies` - Set of external dependencies that should NOT be transformed
    /// * `objects_to_deploy` - Optional set of objects being deployed; objects not in this set are treated as external
    ///
    /// # Example
    /// ```rust,ignore
    /// let visitor = NormalizingVisitor::staging(&fqn, "_staging".to_string(), &external_deps, Some(&objects));
    /// // Transforms: public → public_staging, quickstart → quickstart_staging
    /// // But leaves external dependencies and non-deployed objects unchanged
    /// ```
    pub fn staging(
        fqn: &'a FullyQualifiedName,
        suffix: String,
        external_dependencies: &'a std::collections::BTreeSet<ObjectId>,
        objects_to_deploy: Option<&'a std::collections::BTreeSet<ObjectId>>,
        replacement_objects: &'a std::collections::BTreeSet<ObjectId>,
    ) -> Self {
        Self::new(StagingTransformer::new(
            fqn,
            suffix,
            external_dependencies,
            objects_to_deploy,
            replacement_objects,
        ))
    }
}

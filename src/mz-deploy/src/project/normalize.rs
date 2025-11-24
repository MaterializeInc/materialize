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

use super::hir::FullyQualifiedName;
use mz_sql_parser::ast::*;
use crate::project::object_id::ObjectId;

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
    fqn: &'a FullyQualifiedName,
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
    fqn: &'a FullyQualifiedName,
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

impl<'a> StagingTransformer<'a> {
    /// Check if a name refers to an external dependency or an object not being deployed
    fn is_external(&self, name: &UnresolvedItemName) -> bool {
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

/// Visitor that traverses SQL AST and transforms names using a given strategy.
///
/// This struct is generic over the `NameTransformer` trait, allowing different
/// transformation strategies to reuse the same traversal logic.
pub struct NormalizingVisitor<T: NameTransformer> {
    transformer: T,
}

impl<T: NameTransformer> NormalizingVisitor<T> {
    /// Create a new visitor with the given transformer.
    pub fn new(transformer: T) -> Self {
        Self { transformer }
    }

    /// Get a reference to the transformer.
    pub fn transformer(&self) -> &T {
        &self.transformer
    }

    /// Normalize a RawItemName to be transformed according to the strategy.
    ///
    /// Converts partially qualified or unqualified object references using
    /// the current file's FQN context.
    pub fn normalize_raw_item_name(&self, name: &mut RawItemName) {
        let unresolved = name.name_mut();
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

    /// Normalize all table references in a query (used for views and materialized views).
    ///
    /// Recursively traverses the query AST to find and normalize all object references
    /// in FROM clauses, JOINs, subqueries, and CTEs.
    pub fn normalize_query(&self, query: &mut Query<Raw>) {
        // Normalize CTEs (WITH clause)
        if let CteBlock::Simple(ref mut ctes) = query.ctes {
            for cte in ctes {
                self.normalize_query(&mut cte.query);
            }
        }

        // Normalize main query body
        self.normalize_set_expr(&mut query.body);
    }

    /// Normalize a set expression (SELECT, UNION, INTERSECT, EXCEPT, etc.).
    pub fn normalize_set_expr(&self, set_expr: &mut SetExpr<Raw>) {
        match set_expr {
            SetExpr::Select(select) => {
                self.normalize_select(select);
            }
            SetExpr::Query(query) => {
                self.normalize_query(query);
            }
            SetExpr::SetOperation { left, right, .. } => {
                self.normalize_set_expr(left);
                self.normalize_set_expr(right);
            }
            SetExpr::Values(_) | SetExpr::Show(_) | SetExpr::Table(_) => {
                // These don't contain table references
            }
        }
    }

    /// Normalize a SELECT statement.
    ///
    /// Handles table references in FROM, JOIN, WHERE (subqueries), and SELECT items (subqueries).
    pub fn normalize_select(&self, select: &mut Select<Raw>) {
        // Normalize FROM clause
        for table_with_joins in &mut select.from {
            self.normalize_table_factor(&mut table_with_joins.relation);

            // Normalize JOINs
            for join in &mut table_with_joins.joins {
                self.normalize_table_factor(&mut join.relation);
            }
        }

        // Normalize WHERE clause (may contain subqueries)
        if let Some(ref mut selection) = select.selection {
            self.normalize_expr(selection);
        }

        // Normalize SELECT items (may contain subqueries in expressions)
        for item in &mut select.projection {
            if let SelectItem::Expr { expr, .. } = item {
                self.normalize_expr(expr);
            }
        }
    }

    /// Normalize a table factor (table reference, subquery, or nested join).
    ///
    /// This is the key function where actual table names are normalized.
    pub fn normalize_table_factor(&self, table_factor: &mut TableFactor<Raw>) {
        match table_factor {
            TableFactor::Table { name, .. } => {
                // This is where table references are normalized!
                self.normalize_raw_item_name(name);
            }
            TableFactor::Derived { subquery, .. } => {
                self.normalize_query(subquery);
            }
            TableFactor::NestedJoin { join, .. } => {
                self.normalize_table_factor(&mut join.relation);
                for nested_join in &mut join.joins {
                    self.normalize_table_factor(&mut nested_join.relation);
                }
            }
            TableFactor::Function { .. } | TableFactor::RowsFrom { .. } => {
                // Table functions might reference tables, but the structure is complex
                // For now, we don't normalize these
            }
        }
    }

    /// Normalize expressions (handles subqueries in WHERE, CASE, etc.).
    pub fn normalize_expr(&self, expr: &mut Expr<Raw>) {
        match expr {
            Expr::Subquery(query) | Expr::Exists(query) => {
                self.normalize_query(query);
            }
            Expr::InSubquery { expr, subquery, .. } => {
                self.normalize_expr(expr);
                self.normalize_query(subquery);
            }
            Expr::Between {
                expr, low, high, ..
            } => {
                self.normalize_expr(expr);
                self.normalize_expr(low);
                self.normalize_expr(high);
            }
            Expr::Cast { expr, .. } => {
                self.normalize_expr(expr);
            }
            Expr::Case {
                operand,
                conditions,
                results,
                else_result,
            } => {
                if let Some(operand) = operand {
                    self.normalize_expr(operand);
                }
                for cond in conditions {
                    self.normalize_expr(cond);
                }
                for result in results {
                    self.normalize_expr(result);
                }
                if let Some(else_result) = else_result {
                    self.normalize_expr(else_result);
                }
            }
            Expr::Function(func) => {
                if let FunctionArgs::Args { args, order_by, .. } = &mut func.args {
                    for arg in args {
                        self.normalize_expr(arg);
                    }
                    for order in order_by {
                        self.normalize_expr(&mut order.expr);
                    }
                }
            }
            Expr::Array(exprs) | Expr::List(exprs) => {
                for expr in exprs {
                    self.normalize_expr(expr);
                }
            }
            Expr::Row { exprs } => {
                for expr in exprs {
                    self.normalize_expr(expr);
                }
            }
            Expr::Collate { expr, .. } => {
                self.normalize_expr(expr);
            }
            Expr::IsExpr {
                expr, construct, ..
            } => {
                self.normalize_expr(expr);
                if let IsExprConstruct::DistinctFrom(other_expr) = construct {
                    self.normalize_expr(other_expr);
                }
            }
            // These don't contain subqueries or table references
            _ => {}
        }
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

// Convenience constructors for common use cases
impl<'a> NormalizingVisitor<FullyQualifyingTransformer<'a>> {
    /// Create a visitor that fully qualifies names (`database.schema.object`).
    pub fn fully_qualifying(fqn: &'a FullyQualifiedName) -> Self {
        Self::new(FullyQualifyingTransformer { fqn })
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
        external_dependencies: &'a std::collections::HashSet<ObjectId>,
        objects_to_deploy: Option<&'a std::collections::HashSet<ObjectId>>,
    ) -> Self {
        Self::new(StagingTransformer::new(
            fqn,
            suffix,
            external_dependencies,
            objects_to_deploy,
        ))
    }
}

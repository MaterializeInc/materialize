//! The NormalizingVisitor for traversing SQL AST and applying name transformations.
//!
//! This module contains the `NormalizingVisitor` struct which traverses SQL statements
//! and applies name transformations using a configurable strategy (via the `NameTransformer` trait).

use super::super::typed::FullyQualifiedName;
use super::transformers::{
    ClusterTransformer, FlatteningTransformer, FullyQualifyingTransformer, NameTransformer,
    StagingTransformer,
};
use crate::project::object_id::ObjectId;
use mz_sql_parser::ast::*;

/// Visitor that traverses SQL AST and transforms names using a given strategy.
///
/// This struct is generic over the `NameTransformer` trait, allowing different
/// transformation strategies to reuse the same traversal logic.
pub struct NormalizingVisitor<T: NameTransformer> {
    transformer: T,
    cte_scope: std::cell::RefCell<Vec<std::collections::HashSet<String>>>,
}

impl<T: NameTransformer> NormalizingVisitor<T> {
    /// Create a new visitor with the given transformer.
    pub fn new(transformer: T) -> Self {
        Self {
            transformer,
            cte_scope: std::cell::RefCell::new(Vec::new()),
        }
    }

    /// Check if a name is a CTE currently in scope.
    fn is_cte_in_scope(&self, name: &str) -> bool {
        self.cte_scope
            .borrow()
            .iter()
            .any(|scope| scope.contains(name))
    }

    /// Push a new CTE scope onto the stack.
    fn push_cte_scope(&self, cte_names: std::collections::HashSet<String>) {
        self.cte_scope.borrow_mut().push(cte_names);
    }

    /// Pop the current CTE scope from the stack.
    fn pop_cte_scope(&self) {
        self.cte_scope.borrow_mut().pop();
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
            if self.is_cte_in_scope(&name_str) {
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

    /// Normalize all table references in a query (used for views and materialized views).
    ///
    /// Recursively traverses the query AST to find and normalize all object references
    /// in FROM clauses, JOINs, subqueries, and CTEs.
    pub fn normalize_query(&self, query: &mut Query<Raw>) {
        // Collect CTE names from this query to track them in scope
        let cte_names = match &query.ctes {
            CteBlock::Simple(ctes) => ctes
                .iter()
                .map(|cte| cte.alias.name.to_string())
                .collect::<std::collections::HashSet<String>>(),
            CteBlock::MutuallyRecursive(mut_rec_block) => mut_rec_block
                .ctes
                .iter()
                .map(|cte| cte.name.to_string())
                .collect::<std::collections::HashSet<String>>(),
        };

        // Push CTE names onto scope stack
        self.push_cte_scope(cte_names);

        // Normalize CTEs (WITH clause)
        // Note: CTE definitions themselves can reference earlier CTEs in the same WITH clause
        match &mut query.ctes {
            CteBlock::Simple(ctes) => {
                for cte in ctes {
                    self.normalize_query(&mut cte.query);
                }
            }
            CteBlock::MutuallyRecursive(mut_rec_block) => {
                for cte in &mut mut_rec_block.ctes {
                    self.normalize_query(&mut cte.query);
                }
            }
        }

        // Normalize main query body (can reference all CTEs from this query)
        self.normalize_set_expr(&mut query.body);

        // Pop CTE scope after processing this query
        self.pop_cte_scope();
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

        // Normalize HAVING clause (may contain subqueries)
        if let Some(ref mut having) = select.having {
            self.normalize_expr(having);
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
            TableFactor::Table { name, alias } => {
                // Save the original table name (the last part) before transformation
                // This will be used as an implicit alias if one doesn't exist
                let original_table_name = match name.name().0.len() {
                    1 => {
                        // Unqualified: "sales"
                        let name_str = name.name().0[0].to_string();
                        // Don't create an alias if this is a CTE reference (it won't be transformed)
                        if !self.is_cte_in_scope(&name_str) {
                            Some(name.name().0[0].clone())
                        } else {
                            None
                        }
                    }
                    2 | 3 => {
                        // Schema-qualified: "schema.sales" or fully qualified: "db.schema.sales"
                        // Extract the table name (last part) to use as implicit alias
                        Some(name.name().0.last().unwrap().clone())
                    }
                    _ => None,
                };

                // Normalize the table name (e.g., "sales" -> "materialize.public.sales")
                self.normalize_raw_item_name(name);

                // If there's no explicit alias and we have an original table name, create an implicit alias
                // This ensures qualified column references like "sales.column" continue to work
                // after the table name is transformed to "materialize.public.sales" or "materialize_public_sales"
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
            Expr::Op { expr1, expr2, .. } => {
                // Recursively normalize operands of binary/unary operations (e.g., AND, OR, =, >, <, +, etc.)
                // This ensures subqueries in comparisons like "COUNT(*) > (SELECT ...)" are normalized
                self.normalize_expr(expr1);
                if let Some(expr2) = expr2 {
                    self.normalize_expr(expr2);
                }
            }
            // Note: We intentionally don't transform Expr::Identifier or Expr::QualifiedWildcard
            // because qualified column references (like `alias.column`) should reference table
            // aliases from the FROM clause, not fully qualified table names. The aliases themselves
            // are already attached to transformed table names in the FROM clause.
            //
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

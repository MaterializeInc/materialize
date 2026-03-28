//! CTE scope tracking for SQL AST visitors.
//!
//! Common Table Expressions (CTEs) introduce names that shadow database objects.
//! When traversing a SQL AST to collect dependencies, extract aliases, or
//! transform names, CTE references must be distinguished from real object
//! references. This module provides [`CteScope`], a stack-based tracker that
//! manages CTE name visibility across nested queries.
//!
//! ## Scoping Rules
//!
//! - **Simple CTEs** (`WITH a AS (...), b AS (...) SELECT ...`): All CTE names
//!   are visible to the main query body and to each other's definitions. While
//!   SQL semantics allow each simple CTE to reference only earlier siblings (not
//!   itself), pushing all names at once is safe because self-references in simple
//!   CTEs are SQL errors that Materialize rejects.
//!
//! - **Mutually Recursive CTEs** (`WITH MUTUALLY RECURSIVE a AS (...), b AS (...)
//!   SELECT ...`): All CTE names are visible to all CTE definitions and the main
//!   query body. Self-references and forward references are valid.
//!
//! - **Nested queries**: Each subquery can introduce its own CTEs. The scope
//!   stack ensures inner CTE names shadow outer ones, and are properly removed
//!   when the subquery scope ends.
//!
//! ## Usage Pattern
//!
//! Used with mz-sql-parser's `Visit` / `VisitMut` traits by overriding
//! `visit_query`:
//!
//! ```ignore
//! fn visit_query(&mut self, node: &'ast Query<Raw>) {
//!     let names = CteScope::collect_cte_names(&node.ctes);
//!     self.cte_scope.push(names);
//!     visit::visit_query(self, node);  // default traversal
//!     self.cte_scope.pop();
//! }
//! ```
//!
//! Then in `visit_table_factor`, check `self.cte_scope.is_cte(name)` before
//! treating a single-ident reference as a database object.

use std::collections::BTreeSet;

use mz_sql_parser::ast::{CteBlock, Raw};

/// Stack-based tracker for CTE names currently in scope.
///
/// Each level in the stack corresponds to one `WITH` clause. Names from all
/// levels are visible (inner scopes shadow outer ones, though in practice
/// CTE names don't conflict with each other — they conflict with table names).
pub struct CteScope {
    stack: Vec<BTreeSet<String>>,
}

impl CteScope {
    /// Create an empty scope with no CTE names.
    pub fn new() -> Self {
        Self { stack: Vec::new() }
    }

    /// Push a new set of CTE names onto the scope stack.
    ///
    /// Call this when entering a `WITH` clause. The names remain visible
    /// until [`pop`](Self::pop) is called.
    pub fn push(&mut self, names: BTreeSet<String>) {
        self.stack.push(names);
    }

    /// Pop the most recent CTE scope.
    ///
    /// Call this when leaving a `WITH` clause.
    pub fn pop(&mut self) {
        self.stack.pop();
    }

    /// Check whether `name` is a CTE in any active scope level.
    ///
    /// Returns `true` if the name matches a CTE at any depth in the stack.
    /// Only unqualified single-identifier references should be checked —
    /// multi-part names (e.g., `schema.name`) are always database object
    /// references.
    pub fn is_cte(&self, name: &str) -> bool {
        self.stack.iter().any(|scope| scope.contains(name))
    }

    /// Extract CTE names from a [`CteBlock`].
    ///
    /// For both `Simple` and `MutuallyRecursive` blocks, collects all CTE
    /// names into a single set. See the module docs for why pushing all names
    /// at once is correct for both block types.
    pub fn collect_cte_names(ctes: &CteBlock<Raw>) -> BTreeSet<String> {
        match ctes {
            CteBlock::Simple(ctes) => ctes.iter().map(|cte| cte.alias.name.to_string()).collect(),
            CteBlock::MutuallyRecursive(block) => {
                block.ctes.iter().map(|cte| cte.name.to_string()).collect()
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_empty_scope() {
        let scope = CteScope::new();
        assert!(!scope.is_cte("anything"));
    }

    #[test]
    fn test_single_scope() {
        let mut scope = CteScope::new();
        scope.push(BTreeSet::from(["cte_a".to_string(), "cte_b".to_string()]));

        assert!(scope.is_cte("cte_a"));
        assert!(scope.is_cte("cte_b"));
        assert!(!scope.is_cte("not_a_cte"));

        scope.pop();
        assert!(!scope.is_cte("cte_a"));
    }

    #[test]
    fn test_nested_scopes() {
        let mut scope = CteScope::new();

        // Outer query WITH clause
        scope.push(BTreeSet::from(["outer_cte".to_string()]));
        assert!(scope.is_cte("outer_cte"));

        // Inner subquery WITH clause
        scope.push(BTreeSet::from(["inner_cte".to_string()]));
        assert!(scope.is_cte("outer_cte")); // still visible
        assert!(scope.is_cte("inner_cte"));

        // Leave inner scope
        scope.pop();
        assert!(scope.is_cte("outer_cte"));
        assert!(!scope.is_cte("inner_cte")); // no longer visible

        // Leave outer scope
        scope.pop();
        assert!(!scope.is_cte("outer_cte"));
    }

    #[test]
    fn test_empty_push() {
        let mut scope = CteScope::new();
        scope.push(BTreeSet::new());
        assert!(!scope.is_cte("anything"));
        scope.pop();
    }
}

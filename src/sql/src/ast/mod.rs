// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! SQL abstract syntax tree.

use mz_sql_parser::ast::visit::Visit;
pub use mz_sql_parser::ast::*;
pub mod transform;

/// A visitor that determines if object references are present.
#[derive(Debug)]
pub struct ObjectReferences {
    pub references: bool,
}

impl ObjectReferences {
    /// Returns the referenced objects.
    pub fn insert_source<T: AstInfo>(node: &InsertSource<T>) -> bool {
        let mut visitor = Self { references: false };
        visitor.visit_insert_source(node);
        visitor.references
    }
}

impl<'ast, T: AstInfo> Visit<'ast, T> for ObjectReferences {
    fn visit_set_expr(&mut self, node: &'ast SetExpr<T>) {
        if matches!(node, SetExpr::Show(_) | SetExpr::Table(_)) {
            self.references = true;
        }
        visit::visit_set_expr(self, node);
    }

    fn visit_table_factor(&mut self, node: &'ast TableFactor<T>) {
        // It's possible a table reference is referencing some constant table defined within the
        // query, but we don't want the AST to have to figure that out yet. If that's required, the
        // statement must be planned instead.
        if matches!(node, TableFactor::Table { .. }) {
            self.references = true;
        }
    }
}

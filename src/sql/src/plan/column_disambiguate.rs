// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Transforms the SQL AST to protect against ambiguous column references.
//!
//! There are certain expression that can cause column references to
//! become ambiguous if a new column is added to some relation. This
//! module is responsible for transforming SQL ASTs to tranform these
//! types of expressions so they cannot be ambiguous.
//!
//! As an example imagine the following two tables:
//!     `CREATE TABLE t1 (a INT);`
//!     `CREATE TABLE t2 (b INT);`
//!     `SELECT a FROM t1, t2;`
//! `a` is not ambiguous because only table `t1` contains a column
//! named `a`. However, if we add a column named `a` to `t2`, then `a`
//! would be ambiguous. Therefore, view definitions should fully
//! qualify all column names in case a similar column is added to
//! another table in the future.
//!
//! Currently, the only expression that is rewritten are column
//! references to be fully qualified. This is not the full set of
//! potentially ambiguous column references. For example, it doesn't
//! contain `NATURAL JOIN` or `*`. Instead we don't allow users to add
//! columns to user objects and we disallow `NATURAL JOIN` and `*` from
//! views with system objects.
//! <https://github.com/MaterializeInc/materialize/issues/16650> tracks
//! adding the rest of the expressions to this module.

use crate::names::{Aug, PartialObjectName};
use crate::plan::StatementContext;
use mz_repr::ColumnName;
use mz_sql_parser::ast::visit::{self, Visit};
use mz_sql_parser::ast::visit_mut::{self, VisitMut};
use mz_sql_parser::ast::{Expr, Ident, Query};
use std::collections::BTreeMap;

/// Responsible for generating unique IDs for AST nodes.
#[derive(Debug, Clone)]
pub struct StatementTagger {
    next_node_id: u64,
}

impl StatementTagger {
    pub fn new() -> Self {
        Self { next_node_id: 0 }
    }
    /// Generate unique ID.
    pub fn node_id(&mut self) -> u64 {
        let id = self.next_node_id;
        self.next_node_id += 1;
        id
    }
}

/// Stores the metadata during planning that is needed to remove ambiguous column references.
#[derive(Debug, Clone)]
pub struct ColumnDisambiguationMetadata {
    /// Whether the statement contains an expression that can make the exact column list
    /// ambiguous. For example `NATURAL JOIN` or `SELECT *`. This is filled in as planning occurs.
    pub(crate) ambiguous_columns: bool,
    /// Assigns new AST nodes a unique node id.
    pub(crate) statement_tagger: StatementTagger,
    /// Maps a node id to a fully qualified column reference.
    pub(crate) column_references: BTreeMap<u64, Vec<Ident>>,
}

impl ColumnDisambiguationMetadata {
    pub fn new(statement_tagger: StatementTagger) -> Self {
        Self {
            ambiguous_columns: false,
            statement_tagger,
            column_references: BTreeMap::new(),
            // redundant_expressions: BTreeMap::new(),
        }
    }

    pub fn mark_ambiguous_column_ref(&mut self) {
        self.ambiguous_columns = true;
    }

    pub fn ambiguous_column_ref(&self) -> bool {
        self.ambiguous_columns
    }

    pub fn node_id(&mut self) -> u64 {
        self.statement_tagger.node_id()
    }

    pub fn insert_column_reference(
        &mut self,
        expr: Expr<Aug>,
        id: &Option<u64>,
        table_name: &Option<PartialObjectName>,
        col_name: &ColumnName,
        redundant_expressions: &mut BTreeMap<Expr<Aug>, u64>,
    ) {
        match (id, table_name) {
            (Some(id), Some(table_name)) => {
                redundant_expressions.insert(expr, *id);
                let mut table_name = table_name.into_idents();
                table_name.push(Ident::new(col_name.as_str()));
                self.column_references.insert(*id, table_name);
            }
            _ => {}
        }
    }
}

impl Default for ColumnDisambiguationMetadata {
    fn default() -> Self {
        Self {
            ambiguous_columns: false,
            statement_tagger: StatementTagger::new(),
            column_references: BTreeMap::new(),
            // redundant_expressions: BTreeMap::new(),
        }
    }
}

/// Walks the AST of an expression and collect metadata for any expression that's already been
/// planned.
pub fn collect_redundant_exprs<'a>(
    column_disambiguation_metadata: &mut ColumnDisambiguationMetadata,
    redundant_expressions: &'a BTreeMap<Expr<Aug>, u64>,
    expr: &'a Expr<Aug>,
) {
    let mut redundant_expr_collector =
        RedundantExprCollector::new(column_disambiguation_metadata, redundant_expressions);
    redundant_expr_collector.visit_expr(expr);
}

/// Walks the AST of an expression and collect metadata for any expression that's already been
/// planned.
struct RedundantExprCollector<'a> {
    column_disambiguation_metadata: &'a mut ColumnDisambiguationMetadata,
    redundant_expressions: &'a BTreeMap<Expr<Aug>, u64>,
}

impl<'a> RedundantExprCollector<'a> {
    fn new(
        column_disambiguation_metadata: &'a mut ColumnDisambiguationMetadata,
        redundant_expressions: &'a BTreeMap<Expr<Aug>, u64>,
    ) -> RedundantExprCollector<'a> {
        RedundantExprCollector {
            column_disambiguation_metadata,
            redundant_expressions,
        }
    }
}

impl<'ast> Visit<'ast, Aug> for RedundantExprCollector<'_> {
    fn visit_expr(&mut self, expr: &'ast Expr<Aug>) {
        visit::visit_expr(self, expr);

        if let Some(matching_id) = self.redundant_expressions.get(expr) {
            let matching_id = *matching_id;
            match expr {
                Expr::Identifier { id, .. } => {
                    if let Some(id) = id {
                        let idents = self
                            .column_disambiguation_metadata
                            .column_references
                            .get(&matching_id)
                            .expect("column disambiguation metadata is out of sync")
                            .clone();
                        self.column_disambiguation_metadata
                            .column_references
                            .insert(*id, idents);
                    }
                }
                _ => {}
            }
        }
    }
}

/// Transforms an AST so that it has no potential ambiguous column references.
pub fn disambiguate_columns<'a>(scx: &StatementContext, query: &'a mut Query<Aug>) {
    let mut column_disambiguator = ColumnDisambiguator::new(scx);
    column_disambiguator.visit_query_mut(query);
}

/// Transforms an AST so that it has no potential ambiguous column references.
struct ColumnDisambiguator<'a> {
    scx: &'a StatementContext<'a>,
}

impl<'a> ColumnDisambiguator<'a> {
    fn new(scx: &'a StatementContext<'a>) -> ColumnDisambiguator<'a> {
        ColumnDisambiguator { scx }
    }
}

impl<'ast> VisitMut<'ast, Aug> for ColumnDisambiguator<'_> {
    fn visit_expr_mut(&mut self, expr: &'ast mut Expr<Aug>) {
        visit_mut::visit_expr_mut(self, expr);
        match expr {
            Expr::Identifier { names, id } => {
                if let Some(id) = id {
                    if let Some(qualified_names) = self
                        .scx
                        .column_disambiguation_metadata
                        .borrow_mut()
                        .column_references
                        .get(id)
                    {
                        *names = qualified_names.clone();
                    }
                }
            }
            _ => {}
        }
    }
}

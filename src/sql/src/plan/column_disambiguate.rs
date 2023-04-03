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
//! module is responsible for transforming SQL ASTs to transform these
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
//! Other examples include views with `*` or `NATURAL JOIN`s in their
//! definition. Adding a column to underlying tables will implicitly
//! change the definition of the view.
//!
//! Currently this modules tries to apply the following rules:
//!   - Fully qualify all column references.
//!   - Expand all `*` expressions to a list of unambiguous column
//!   references.
//!   - Expand all `NATURAL JOIN` join constraints to `USING` join
//!   constraints.
//!
//! However, the implementation currently doesn't work properly, i.e.
//! it's broken. We've kept the broken implementation here and hidden
//! it behind a feature flag to avoid losing all the work that's
//! already gone into it.
//!
//! Additionally, this module helps ban views that contain `*` or
//! `NATURAL JOIN` expressions AND system objects to protect against
//! breaking views when we add columns to system objects.
//!
//! For more information look at
//! <https://github.com/MaterializeInc/materialize/issues/16650>.

use std::collections::{BTreeMap, BTreeSet};

use mz_repr::ColumnName;
use mz_sql_parser::ast::visit_mut::{self, VisitMut};
use mz_sql_parser::ast::{
    Expr, Ident, JoinConstraint, Query, Select, SelectItem, TableAlias, TableFactor, Value,
};

use crate::names::{Aug, PartialObjectName};
use crate::plan::scope::{Scope, ScopeItem};
use crate::plan::StatementContext;

/// Responsible for generating unique IDs for AST nodes.
#[derive(Debug, Clone)]
pub struct StatementTagger {
    next_node_id: u64,
    enabled: bool,
}

impl StatementTagger {
    pub fn new(enabled: bool) -> Self {
        Self {
            next_node_id: 0,
            enabled,
        }
    }
    /// Generate unique ID.
    pub fn node_id(&mut self) -> Option<u64> {
        if self.enabled {
            let id = self.next_node_id;
            self.next_node_id += 1;
            Some(id)
        } else {
            None
        }
    }
}

/// A single column expanded from a wildcard.
#[derive(Debug, Clone)]
pub struct TableFactorExpansion {
    pub table_alias: Option<Ident>,
    pub derived_column_names: Vec<ColumnName>,
}

/// A single column expanded from a wildcard.
#[derive(Debug, Clone)]
pub struct ColumnExpansion {
    /// Fully qualified table name.
    pub table_name: Option<PartialObjectName>,
    /// Visible name of column.
    pub column_name: ColumnName,
    /// Alias to set on column.
    pub column_alias: Option<ColumnName>,
}

impl ColumnExpansion {
    pub fn new(
        table_name: Option<PartialObjectName>,
        column_name: ColumnName,
        column_alias: Option<ColumnName>,
    ) -> ColumnExpansion {
        ColumnExpansion {
            table_name,
            column_name,
            column_alias,
        }
    }
}

/// Stores the metadata during planning that is needed to remove ambiguous column references.
#[derive(Debug, Clone)]
pub struct ColumnDisambiguationMetadata {
    /// Whether the statement contains an expression that can make the exact column list
    /// ambiguous. For example `NATURAL JOIN` or `SELECT *`. This is filled in as planning occurs.
    ambiguous_columns: bool,
    /// Assigns new AST nodes a unique node id.
    statement_tagger: StatementTagger,
    /// Maps an identifier node id to a fully qualified column reference.
    column_references: BTreeMap<u64, Vec<Ident>>,
    /// Maps a table factor node id to an expanded alias and derived column list.
    table_factor_expansions: BTreeMap<u64, TableFactorExpansion>,
    /// Maps a wildcard node id to an expanded column list.
    wildcard_expansions: BTreeMap<u64, Vec<ColumnExpansion>>,
    /// Maps a natural join node id to an expanded using column list.
    natural_join_expansions: BTreeMap<u64, Vec<ColumnName>>,
}

impl ColumnDisambiguationMetadata {
    pub fn new(statement_tagger: StatementTagger) -> Self {
        Self {
            ambiguous_columns: false,
            statement_tagger,
            column_references: BTreeMap::new(),
            table_factor_expansions: BTreeMap::new(),
            wildcard_expansions: BTreeMap::new(),
            natural_join_expansions: BTreeMap::new(),
        }
    }

    pub fn statement_tagger_mut(&mut self) -> &mut StatementTagger {
        &mut self.statement_tagger
    }

    pub fn mark_ambiguous_column_ref(&mut self) {
        self.ambiguous_columns = true;
    }

    pub fn ambiguous_column_ref(&self) -> bool {
        self.ambiguous_columns
    }

    pub fn node_id(&mut self) -> Option<u64> {
        self.statement_tagger.node_id()
    }

    pub fn insert_column_reference(
        &mut self,
        id: &Option<u64>,
        // TODO(jkosh44) This is currently wrong, we should be passing the expanded table alias
        //  here.
        table_name: &Option<PartialObjectName>,
        // TODO(jkosh44) This is currently wrong, we should be passing the unique derived column
        //  name here, and not the original column name.
        col_name: &ColumnName,
    ) {
        match (id, table_name) {
            (Some(id), Some(table_name)) => {
                let mut table_name = table_name.into_idents();
                table_name.push(Ident::new(col_name.as_str()));
                self.column_references.insert(*id, table_name);
            }
            _ => {}
        }
    }

    pub fn insert_table_factor_alias(
        &mut self,
        id: u64,
        table_factor_expansion: TableFactorExpansion,
    ) {
        self.table_factor_expansions
            .insert(id, table_factor_expansion);
    }

    pub fn insert_wildcard_expansion(&mut self, id: u64, column_expansions: Vec<ColumnExpansion>) {
        self.wildcard_expansions.insert(id, column_expansions);
    }

    pub fn insert_natural_join_expansion(&mut self, id: u64, column_names: Vec<ColumnName>) {
        self.natural_join_expansions.insert(id, column_names);
    }

    pub fn get_table_factor_expansion(&self, id: &u64) -> Option<&TableFactorExpansion> {
        self.table_factor_expansions.get(id)
    }

    pub fn get_wildcard_expansion(&self, id: &u64) -> Option<&Vec<ColumnExpansion>> {
        self.wildcard_expansions.get(id)
    }

    pub fn get_natural_join_expansion(&self, id: &u64) -> Option<&Vec<ColumnName>> {
        self.natural_join_expansions.get(id)
    }
}

// Planning helper functions.

pub fn collect_table_factor_expansions(
    column_disambiguation_metadata: &mut ColumnDisambiguationMetadata,
    using_columns_in_scope: &mut BTreeSet<ColumnName>,
    scope: &mut Scope,
    id: &Option<u64>,
) {
    if let Some(id) = id {
        let mut derived_column_names = Vec::new();
        let mut table_alias = None;

        for item in scope.items.iter_mut() {
            let column_name = &item.column_name;
            let mut derived_column_name = column_name.clone();

            // TODO(jkosh44) This is not the correct way to generate table alias names. We'll need a
            //  smarter way of doing this, to ensure that the aliases are unique. For example two fully
            //  qualified tables with the same name from different schemas will end up with the same
            //  alias. Also this doesn't really make sense for nested joins.
            if table_alias.is_none() {
                if let Some(cur_table_name) = &item.table_name {
                    table_alias = Some(Ident::new(cur_table_name.item.clone()));
                }
            }

            let mut i = 1;
            while derived_column_names.contains(&derived_column_name)
                || using_columns_in_scope.contains(&derived_column_name)
            {
                derived_column_name = format!("{column_name}_{i}").into();
                i += 1;
            }

            item.unique_derived_column_name = Some(derived_column_name.clone());
            if item.using_column {
                using_columns_in_scope.insert(derived_column_name.clone());
            }
            derived_column_names.push(derived_column_name);
        }
        column_disambiguation_metadata.insert_table_factor_alias(
            *id,
            TableFactorExpansion {
                table_alias,
                derived_column_names,
            },
        );
    } else {
        let using_columns = scope
            .items
            .iter()
            .filter(|item| item.using_column)
            .map(|item| {
                item.unique_derived_column_name
                    .clone()
                    .unwrap_or_else(|| item.column_name.clone())
            });
        using_columns_in_scope.extend(using_columns);
    }
}

pub fn collect_wildcard_expansions<'a>(
    column_disambiguation_metadata: &mut ColumnDisambiguationMetadata,
    scope_items: impl Iterator<Item = &'a ScopeItem>,
    id: &Option<u64>,
) {
    column_disambiguation_metadata.mark_ambiguous_column_ref();
    if let Some(id) = id {
        let column_expansions = scope_items
            .map(|item| {
                let (column_name, column_alias) = match &item.unique_derived_column_name {
                    Some(derived_column_name) => {
                        (derived_column_name.clone(), Some(item.column_name.clone()))
                    }
                    None => (item.column_name.clone(), None),
                };
                ColumnExpansion::new(item.table_name.clone(), column_name, column_alias)
            })
            .collect();
        column_disambiguation_metadata.insert_wildcard_expansion(*id, column_expansions);
    }
}

pub fn collect_natural_join_expansions(
    column_disambiguation_metadata: &mut ColumnDisambiguationMetadata,
    column_names: &Vec<ColumnName>,
    id: &Option<u64>,
) {
    column_disambiguation_metadata.mark_ambiguous_column_ref();
    if let Some(id) = id {
        column_disambiguation_metadata.insert_natural_join_expansion(*id, column_names.clone());
    }
}

// Post-planning transformations.

// TODO(jkosh44) Most of the clones below can probably be removed if we switch the Map get
//  methods to Map remove methods.

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
    // Fully qualify column names.
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

    // Assign unique table alias to all table factors.
    fn visit_table_factor_mut(&mut self, table_factor: &'ast mut TableFactor<Aug>) {
        visit_mut::visit_table_factor_mut(self, table_factor);

        let id = table_factor.id();
        if let Some(id) = id {
            let id = id.clone();
            let is_nested_join = matches!(table_factor, TableFactor::NestedJoin { .. });
            let alias = table_factor.alias_mut();

            let binding = self.scx.column_disambiguation_metadata.borrow();
            let TableFactorExpansion {
                table_alias,
                derived_column_names,
            } = binding
                .get_table_factor_expansion(&id)
                .expect("table factor never planned");
            let derived_column_names = derived_column_names
                .iter()
                .map(|col| Ident::new(col.as_str()))
                .collect();
            if let Some(alias) = alias {
                alias.columns = derived_column_names;
            } else if let Some(table_alias) = table_alias {
                // TODO(jkosh44) It's unclear what to do for nested join. It will require some more
                //  investigation.
                if !is_nested_join {
                    *alias = Some(TableAlias {
                        name: table_alias.clone(),
                        columns: derived_column_names,
                        strict: false,
                    });
                }
            }
        }
    }

    // Expand wildcard expressions.
    fn visit_select_mut(&mut self, select: &'ast mut Select<Aug>) {
        visit_mut::visit_select_mut(self, select);
        let mut projection = Vec::new();
        for select_item in select.projection.drain(..) {
            match select_item {
                SelectItem::Expr {
                    expr: Expr::QualifiedWildcard { id, .. },
                    ..
                }
                | SelectItem::Wildcard { id } => {
                    if let Some(id) = id {
                        let expansion = self
                            .scx
                            .column_disambiguation_metadata
                            .borrow()
                            .get_wildcard_expansion(&id)
                            .cloned()
                            .expect("wildcard not planned");
                        for ColumnExpansion {
                            table_name,
                            column_name,
                            column_alias,
                        } in expansion
                        {
                            let mut ident = Vec::new();
                            if let Some(table_name) = table_name {
                                // PostgreSQL only uses the table name to qualify the column and
                                // not the database or schema. In case there's naming conflicts
                                // across schemas, PostreSQL will invent a unique alias for one of
                                // the tables. We just fully qualify the columns with their
                                // database and schema to avoid inventing unique aliases when possible.
                                if let Some(database) = table_name.database {
                                    ident.push(Ident::new(database));
                                }
                                if let Some(schema) = table_name.schema {
                                    ident.push(Ident::new(schema));
                                }
                                ident.push(Ident::new(table_name.item));
                            }
                            ident.push(Ident::new(column_name.as_str()));

                            let column_alias =
                                column_alias.map(|column_alias| Ident::new(column_alias.as_str()));
                            let select_item = SelectItem::Expr {
                                expr: Expr::Identifier {
                                    names: ident,
                                    id: None,
                                },
                                alias: column_alias,
                            };
                            projection.push(select_item);
                        }
                    }
                }
                SelectItem::Expr {
                    expr: Expr::WildcardAccess { expr, id },
                    ..
                } => {
                    if let Some(id) = id {
                        let expansion = self
                            .scx
                            .column_disambiguation_metadata
                            .borrow()
                            .get_wildcard_expansion(&id)
                            .cloned()
                            .expect("wildcard not planned");
                        for ColumnExpansion {
                            table_name: _,
                            column_name,
                            column_alias,
                        } in expansion
                        {
                            let alias = column_alias.map(|alias| Ident::new(alias.as_str()));
                            let select_item = SelectItem::Expr {
                                expr: Expr::FieldAccess {
                                    expr: expr.clone(),
                                    field: Ident::new(column_name.as_str()),
                                },
                                alias,
                            };
                            projection.push(select_item);
                        }
                    }
                }
                item @ SelectItem::Expr { .. } => projection.push(item),
            }
        }
        select.projection = projection;
    }

    // Expand NATURAL JOINs.
    fn visit_join_constraint_mut(&mut self, join_constraint: &'ast mut JoinConstraint<Aug>) {
        visit_mut::visit_join_constraint_mut(self, join_constraint);
        if let JoinConstraint::Natural { id } = join_constraint {
            if let Some(id) = id {
                let expansion = self
                    .scx
                    .column_disambiguation_metadata
                    .borrow()
                    .get_natural_join_expansion(id)
                    .cloned()
                    .expect("join constraint not planned");
                *join_constraint = if expansion.is_empty() {
                    JoinConstraint::On(Expr::Value(Value::Boolean(true)))
                } else {
                    JoinConstraint::Using(
                        expansion
                            .into_iter()
                            .map(|col| Ident::new(col.as_str()))
                            .collect(),
                    )
                };
            }
        }
    }
}

// Copyright 2019 Materialize, Inc. All rights reserved.
//
// This file is part of Materialize. Materialize may not be used or
// distributed without the express permission of Materialize, Inc.

//! Handles SQLs scoping rules.

//! A scope spans a single SQL `Query`.
//! Nested subqueries create new scopes.
//! Names are resolved against the innermost scope first.
//! * If a match is found, it is returned
//! * If no matches are found, the name is resolved against the parent scope
//! * If multiple matches are found, the name is ambigious and we return an error to the user

//! Matching rules:
//! * `bar` will match any column in the scope named `bar`
//! * `foo.bar` will match any column in the scope named `bar` that originated from a table named `foo`
//! * Table aliases such as `foo as quux` replace the old table name.
//! * Functions create unnamed columns, which can be named with columns aliases `(bar + 1) as more_bar`

//! Additionally, most databases fold some form of CSE into name resolution so that eg `SELECT sum(x) FROM foo GROUP BY sum(x)` would be treated something like `SELECT "sum(x)" FROM foo GROUP BY sum(x) AS "sum(x)"` rather than failing to resolve `x`. We handle this by including the underlying `sqlparser::ast::Expr` in cases where this is possible.

//! Many sql expressions do strange and arbitrary things to scopes. Rather than try to capture them all here, we just expose the internals of `Scope` and handle it in the appropriate place in `super::query`.

use failure::bail;

use super::expr::ColumnRef;
use repr::{ColumnName, QualName};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ScopeItemName {
    pub table_name: Option<QualName>,
    pub column_name: Option<ColumnName>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ScopeItem {
    // The canonical name should appear first in the list (e.g., the name
    // assigned by an alias.)
    pub names: Vec<ScopeItemName>,
    pub expr: Option<sqlparser::ast::Expr>,
    // Whether this item is actually resolveable by its name. Non-nameable scope
    // items are used e.g. in the scope created by an inner join, so that the
    // duplicated key columns from the right relation do not cause ambiguous
    // column names. Omitting the name entirely is not an option, since the name
    // is used to label the column in the result set.
    pub nameable: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OuterScopeItem {
    /// The actual scope item.
    scope_item: ScopeItem,
    /// The "outerness" of this scope item. An item from the parent scope is
    /// level 0. An item from the parent's parent scope is level 1. And so on.
    level: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ScopeLevel {
    /// The inner scope.
    Inner,
    /// The outer scope with the specified level.
    Outer(usize),
}

#[derive(Debug, Clone)]
pub struct Scope {
    // items in this query
    pub items: Vec<ScopeItem>,
    // items inherited from an enclosing query
    pub outer_items: Vec<OuterScopeItem>,
}

impl ScopeItem {
    pub fn from_column_name(column_name: Option<ColumnName>) -> Self {
        ScopeItem {
            names: vec![ScopeItemName {
                table_name: None,
                column_name,
            }],
            expr: None,
            nameable: true,
        }
    }
}

impl Scope {
    pub fn empty(outer_scope: Option<Scope>) -> Self {
        Scope {
            items: vec![],
            outer_items: if let Some(mut outer_scope) = outer_scope {
                for mut item in &mut outer_scope.outer_items {
                    item.level += 1;
                }
                outer_scope
                    .outer_items
                    .into_iter()
                    .chain(outer_scope.items.into_iter().map(|item| OuterScopeItem {
                        scope_item: item,
                        level: 0,
                    }))
                    .collect()
            } else {
                vec![]
            },
        }
    }

    pub fn from_source<T, I, N>(
        table_name: Option<T>,
        column_names: I,
        outer_scope: Option<Scope>,
    ) -> Self
    where
        T: Into<QualName>,
        I: IntoIterator<Item = Option<N>>,
        N: Into<ColumnName>,
    {
        let mut scope = Scope::empty(outer_scope);
        let table_name = table_name.map(|n| n.into());
        scope.items = column_names
            .into_iter()
            .map(|column_name| ScopeItem {
                names: vec![ScopeItemName {
                    table_name: table_name.clone(),
                    column_name: column_name.map(|n| n.into()),
                }],
                expr: None,
                nameable: true,
            })
            .collect();
        scope
    }

    /// Constructs an iterator over the canonical name for each column.
    pub fn column_names(&self) -> impl Iterator<Item = Option<&ColumnName>> {
        self.items.iter().map(|item| {
            item.names
                .iter()
                .filter_map(|n| n.column_name.as_ref())
                .next()
        })
    }

    pub fn len(&self) -> usize {
        self.items.len()
    }

    fn iter_items(&self) -> impl Iterator<Item = (usize, &ScopeItem, ScopeLevel)> {
        self.items
            .iter()
            .enumerate()
            .map(|(pos, item)| (pos, item, ScopeLevel::Inner))
    }

    fn iter_outer_items(
        &self,
    ) -> impl Iterator<Item = (usize, &ScopeItem, ScopeLevel)> + DoubleEndedIterator {
        self.outer_items
            .iter()
            .enumerate()
            .map(|(pos, oitem)| (pos, &oitem.scope_item, ScopeLevel::Outer(oitem.level)))
    }

    fn resolve<'a, Matches>(
        &'a self,
        matches: Matches,
        name_in_error: &str,
    ) -> Result<(ColumnRef, &'a ScopeItemName), failure::Error>
    where
        Matches: Fn(&ScopeItemName) -> bool,
    {
        let mut results = self
            .iter_items()
            // We reverse the outer items so that we prefer closer scopes.
            // E.g., given A(B(C)), items from C should be preferred to items
            // from B to items from A.
            .chain(self.iter_outer_items().rev())
            .map(|(pos, item, level)| item.names.iter().map(move |name| (pos, item, level, name)))
            .flatten()
            .filter(|(_pos, item, _level, name)| (matches)(name) && item.nameable);
        match results.next() {
            None => bail!("column \"{}\" does not exist", name_in_error),
            Some((pos, _item, level, name)) => {
                if results
                    .find(|(pos2, item, level2, _name)| {
                        pos != *pos2 && level == *level2 && item.nameable
                    })
                    .is_none()
                {
                    match level {
                        ScopeLevel::Inner => Ok((ColumnRef::Inner(pos), name)),
                        ScopeLevel::Outer(_) => Ok((ColumnRef::Outer(pos), name)),
                    }
                } else {
                    bail!("Column name {} is ambiguous", name_in_error)
                }
            }
        }
    }

    pub fn resolve_column<'a>(
        &'a self,
        column_name: &ColumnName,
    ) -> Result<(ColumnRef, &'a ScopeItemName), failure::Error> {
        self.resolve(
            |item: &ScopeItemName| item.column_name.as_ref() == Some(column_name),
            column_name.as_str(),
        )
    }

    pub fn resolve_table_column<'a>(
        &'a self,
        table_name: &QualName,
        column_name: &ColumnName,
    ) -> Result<(ColumnRef, &'a ScopeItemName), failure::Error> {
        self.resolve(
            |item: &ScopeItemName| {
                item.table_name.as_ref() == Some(table_name)
                    && item.column_name.as_ref() == Some(column_name)
            },
            &format!("{}.{}", table_name, column_name),
        )
    }

    /// Look to see if there is an already-calculated instance of this expr.
    /// Failing to find one is not an error, so this just returns Option
    pub fn resolve_expr<'a>(
        &'a self,
        expr: &sqlparser::ast::Expr,
    ) -> Option<(ColumnRef, Option<&'a ScopeItemName>)> {
        self.items
            .iter()
            .enumerate()
            .find(|(_, item)| item.expr.as_ref() == Some(expr))
            .map(|(i, item)| (ColumnRef::Inner(i), item.names.first()))
    }

    pub fn product(self, right: Self) -> Self {
        assert!(self.outer_items == right.outer_items);
        Scope {
            items: self
                .items
                .into_iter()
                .chain(right.items.into_iter())
                .collect(),
            outer_items: self.outer_items,
        }
    }

    pub fn project(&self, columns: &[usize]) -> Self {
        Scope {
            items: columns.iter().map(|&i| self.items[i].clone()).collect(),
            outer_items: self.outer_items.clone(),
        }
    }
}

// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Handles SQL's scoping rules.
//!
//! A scope spans a single SQL `Query`. Nested subqueries create new scopes.
//! Names are resolved against the innermost scope first.
//! * If a match is found, it is returned.
//! * If no matches are found, the name is resolved against the parent scope.
//! * If multiple matches are found, the name is ambigious and we return an
//!   error to the user.
//!
//! Matching rules:
//! * `bar` will match any column in the scope named `bar`
//! * `foo.bar` will match any column in the scope named `bar` that originated
//!    from a table named `foo`.
//! * Table aliases such as `foo AS quux` replace the old table name.
//! * Functions create unnamed columns, which can be named with columns aliases
//!   `(bar + 1) as more_bar`.
//!
//! Additionally, most databases fold some form of CSE into name resolution so
//! that eg `SELECT sum(x) FROM foo GROUP BY sum(x)` would be treated something
//! like `SELECT "sum(x)" FROM foo GROUP BY sum(x) AS "sum(x)"` rather than
//! failing to resolve `x`. We handle this by including the underlying
//! `sql_parser::ast::Expr` in cases where this is possible.
//!
//! Many SQL expressions do strange and arbitrary things to scopes. Rather than
//! try to capture them all here, we just expose the internals of `Scope` and
//! handle it in the appropriate place in `super::query`.
//!
//! NOTE(benesch): The above approach of exposing scope's internals to the
//! entire planner has not aged well. SQL scopes are now full of undocumented
//! assumptions and requirements, since various subcomponents of the planner
//! shove data into scope items to communicate with subcomponents a mile away.
//! I've tried to refactor this code several times to no avail. It works better
//! than you might expect. But you have been warned. Tread carefully!

use itertools::Itertools;

use repr::ColumnName;

use crate::names::PartialName;
use crate::plan::error::PlanError;
use crate::plan::expr::ColumnRef;
use crate::plan::query::Aug;

#[derive(Debug, Clone)]
pub struct ScopeItemName {
    pub table_name: Option<PartialName>,
    pub column_name: Option<ColumnName>,
    /// Whether this name is in the "priority" class or not.
    ///
    /// Names are divided into two classes: non-priority and priority. When
    /// resolving a name, if the name appears as both a priority and
    /// non-priority name, the priority name will be chosen. But if the same
    /// name appears in the same class more than once (i.e., it appears as a
    /// priority name twice), resolving that name will trigger an "ambiguous
    /// name" error.
    ///
    /// This exists to support the special behavior of scoping in intrusive
    /// `ORDER BY` clauses. For example, in the following `SELECT` statement
    ///
    ///   CREATE TABLE t (a int, b)
    ///   SELECT 'outer' AS a, b FROM t ORDER BY a
    ///
    /// even though there are two columns named `a` in scope in the ORDER BY
    /// (one from `t` and one declared in the select list), the column declared
    /// in the select list has priority. Remember that if there are two columns
    /// named `a` that are both in the priority class, as in
    ///
    ///   SELECT 'outer' AS a, a FROM t ORDER BY a
    ///
    /// this still needs to generate an "ambiguous name" error.
    pub priority: bool,
}

#[derive(Debug, Clone)]
pub struct ScopeItem {
    // The canonical name should appear first in the list (e.g., the name
    // assigned by an alias.) Similarly, the name that specifies the canonical
    // table must appear before names that specify non-canonical tables.
    // This impacts the behavior of the `is_from_table` test.
    pub names: Vec<ScopeItemName>,
    pub expr: Option<sql_parser::ast::Expr<Aug>>,
    // Whether this item is actually resolveable by its name. Non-nameable scope
    // items are used e.g. in the scope created by an inner join, so that the
    // duplicated key columns from the right relation do not cause ambiguous
    // column names. Omitting the name entirely is not an option, since the name
    // is used to label the column in the result set.
    pub nameable: bool,
}

#[derive(Debug, Clone)]
pub struct Scope {
    // items in this query
    pub items: Vec<ScopeItem>,
    // items inherited from an enclosing query
    pub outer_scope: Option<Box<Scope>>,
}

impl ScopeItem {
    pub fn from_column_name(column_name: Option<ColumnName>) -> Self {
        ScopeItem {
            names: vec![ScopeItemName {
                table_name: None,
                column_name,
                priority: false,
            }],
            expr: None,
            nameable: true,
        }
    }

    pub fn is_from_table(&self, table_name: &PartialName) -> bool {
        // Only consider the first name that specifies a table component.
        // Even though there may be a later name that matches the table, the
        // column is not actually considered to come from that table; it is just
        // known to be equivalent to the *real* column from that table.
        self.names
            .iter()
            .find_map(|n| n.table_name.as_ref())
            .map(|n| n.matches(table_name))
            .unwrap_or(false)
    }

    pub fn short_display_name(&self) -> String {
        match self.names.get(0) {
            None => "?".into(),
            Some(name) => {
                let column_name = match &name.column_name {
                    None => "?column?",
                    Some(column_name) => column_name.as_str(),
                };
                match &name.table_name {
                    None => column_name.into(),
                    Some(table_name) => format!("{}.{}", table_name.item, column_name),
                }
            }
        }
    }
}

impl Scope {
    pub fn empty(outer_scope: Option<Scope>) -> Self {
        Scope {
            items: vec![],
            outer_scope: outer_scope.map(Box::new),
        }
    }

    pub fn from_source<I, N>(
        table_name: Option<PartialName>,
        column_names: I,
        outer_scope: Option<Scope>,
    ) -> Self
    where
        I: IntoIterator<Item = Option<N>>,
        N: Into<ColumnName>,
    {
        let mut scope = Scope::empty(outer_scope);
        scope.items = column_names
            .into_iter()
            .map(|column_name| ScopeItem {
                names: vec![ScopeItemName {
                    table_name: table_name.clone(),
                    column_name: column_name.map(|n| n.into()),
                    priority: false,
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

    pub fn all_items(&self) -> Vec<(usize, usize, &ScopeItem)> {
        // These are in order of preference eg
        // given scopes A(B(C))
        // items from C should be preferred to items from B to items from A
        let mut items = vec![];
        let mut level = 0;
        let mut scope = self;
        loop {
            for (column, item) in scope.items.iter().enumerate() {
                items.push((level, column, item));
            }
            if let Some(outer_scope) = &scope.outer_scope {
                scope = outer_scope;
                level += 1;
            } else {
                break;
            }
        }
        items
    }

    fn resolve<'a, M>(
        &'a self,
        mut matches: M,
        name_in_error: &str,
    ) -> Result<(ColumnRef, &'a ScopeItemName), PlanError>
    where
        M: FnMut(usize, &ScopeItemName) -> bool,
    {
        let mut results = self
            .all_items()
            .into_iter()
            .flat_map(|(level, column, item)| {
                item.names
                    .iter()
                    .map(move |name| (level, column, item, name))
            })
            .filter(|(level, _column, item, name)| (matches)(*level, name) && item.nameable)
            .sorted_by_key(|(level, _column, _item, name)| (*level, !name.priority));
        match results.next() {
            None => Err(PlanError::UnknownColumn(name_in_error.to_owned())),
            Some((level, column, _item, name)) => {
                if results
                    .find(|(level2, column2, item, name2)| {
                        column != *column2
                            && level == *level2
                            && item.nameable
                            && name.priority == name2.priority
                    })
                    .is_none()
                {
                    Ok((ColumnRef { level, column }, name))
                } else {
                    Err(PlanError::AmbiguousColumn(name_in_error.to_owned()))
                }
            }
        }
    }

    pub fn resolve_column<'a>(
        &'a self,
        column_name: &ColumnName,
    ) -> Result<(ColumnRef, &'a ScopeItemName), PlanError> {
        self.resolve(
            |_level, item| item.column_name.as_ref() == Some(column_name),
            column_name.as_str(),
        )
    }

    pub fn resolve_table_column<'a>(
        &'a self,
        table_name: &PartialName,
        column_name: &ColumnName,
    ) -> Result<(ColumnRef, &'a ScopeItemName), PlanError> {
        let mut seen_at_level = None;
        self.resolve(
            |level, item| {
                // Once we've matched a table name at a level, even if the
                // column name did not match, we can never match an item from
                // another level.
                if let Some(seen_at_level) = seen_at_level {
                    if seen_at_level != level {
                        return false;
                    }
                }
                if item.table_name.as_ref().map(|n| n.matches(table_name)) == Some(true) {
                    seen_at_level = Some(level);
                    item.column_name.as_ref() == Some(column_name)
                } else {
                    false
                }
            },
            &format!("{}.{}", table_name, column_name),
        )
    }

    /// Look to see if there is an already-calculated instance of this expr.
    /// Failing to find one is not an error, so this just returns Option
    pub fn resolve_expr<'a>(&'a self, expr: &sql_parser::ast::Expr<Aug>) -> Option<ColumnRef> {
        self.items
            .iter()
            .enumerate()
            .find(|(_, item)| item.expr.as_ref() == Some(expr))
            .map(|(i, _)| ColumnRef {
                level: 0,
                column: i,
            })
    }

    pub fn product(self, right: Self) -> Self {
        Scope {
            items: self
                .items
                .into_iter()
                .chain(right.items.into_iter())
                .collect(),
            outer_scope: self.outer_scope,
        }
    }

    pub fn project(&self, columns: &[usize]) -> Self {
        Scope {
            items: columns.iter().map(|&i| self.items[i].clone()).collect(),
            outer_scope: self.outer_scope.clone(),
        }
    }
}

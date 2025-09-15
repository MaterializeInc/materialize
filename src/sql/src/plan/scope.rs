// Copyright Materialize, Inc. and contributors. All rights reserved.
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
//! * If multiple matches are found, the name is ambiguous and we return an
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

use std::collections::BTreeSet;
use std::iter;
use std::sync::Arc;

use mz_ore::iter::IteratorExt;
use mz_repr::{ColumnName, UNKNOWN_COLUMN_NAME};

use crate::ast::Expr;
use crate::names::{Aug, PartialItemName};
use crate::plan::error::PlanError;
use crate::plan::hir::ColumnRef;
use crate::plan::plan_utils::JoinSide;
use crate::plan::query::NameManager;

#[derive(Debug, Clone)]
pub struct ScopeItem {
    /// The name of the table that produced this scope item, if any.
    pub table_name: Option<PartialItemName>,
    /// The name of the column.
    pub column_name: ColumnName,
    /// The expressions from which this scope item is derived. Used by `GROUP
    /// BY` and window functions.
    pub exprs: BTreeSet<Expr<Aug>>,
    /// Whether the column is the return value of a function that produces only
    /// a single column. This accounts for a strange PostgreSQL special case
    /// around whole-row expansion.
    pub from_single_column_function: bool,
    /// Controls whether the column is only accessible via a table-qualified
    /// reference. When false, the scope item is also excluded from `SELECT *`.
    ///
    /// This should be true for almost all scope items. It is set to false for
    /// join columns in USING constraints. For example, in `t1 FULL JOIN t2
    /// USING a`, `t1.a` and `t2.a` are still available by fully-qualified
    /// reference, but a bare `a` refers to a new column whose value is
    /// `coalesce(t1.a, t2.a)`. This is a big special case because normally
    /// having three columns in scope named `a` would result in "ambiguous
    /// column reference" errors.
    pub allow_unqualified_references: bool,
    /// If set, any attempt to reference this item will return the error
    /// produced by this function.
    ///
    /// The function is provided with the table and column name in the
    /// reference. It should return a `PlanError` describing why the reference
    /// is invalid.
    ///
    /// This is useful for preventing access to certain columns in specific
    /// contexts, like columns that are on the wrong side of a `LATERAL` join.
    pub error_if_referenced: Option<fn(Option<&PartialItemName>, &ColumnName) -> PlanError>,
    /// For table functions in scalar positions, this flag is true for the
    /// ordinality column. If true, then this column represents an "exists" flag
    /// for the entire row of the table function. In that case, this column must
    /// be excluded from `*` expansion. If the corresponding datum is `NULL`, then
    /// `*` expansion should yield a single `NULL` instead of a record with various
    /// datums.
    pub is_exists_column_for_a_table_function_that_was_in_the_target_list: bool,
    // Force use of the constructor methods.
    _private: (),
}

/// An ungrouped column in a scope.
///
/// We can't simply drop these items from scope. These items need to *exist*
/// because they might shadow variables in outer scopes that would otherwise be
/// valid to reference, but accessing them needs to produce an error.
#[derive(Debug, Clone)]
pub struct ScopeUngroupedColumn {
    /// The name of the table that produced this ungrouped column, if any.
    pub table_name: Option<PartialItemName>,
    /// The name of the ungrouped column.
    pub column_name: ColumnName,
    /// Whether the original scope item allowed unqualified references.
    pub allow_unqualified_references: bool,
}

#[derive(Debug, Clone)]
pub struct Scope {
    /// The items in this scope.
    pub items: Vec<ScopeItem>,
    /// The ungrouped columns in the scope.
    pub ungrouped_columns: Vec<ScopeUngroupedColumn>,
    // Whether this scope starts a new chain of lateral outer scopes.
    //
    // It's easiest to understand with an example. Consider this query:
    //
    //     SELECT (SELECT * FROM tab1, tab2, (SELECT tab1.a, tab3.a)) FROM tab3, tab4
    //     Scope 1:                          ------------------------
    //     Scope 2:                    ----
    //     Scope 3:              ----
    //     Scope 4:                                                              ----
    //     Scope 5:                                                        ----
    //
    // Note that the because the derived table is not marked `LATERAL`, its
    // reference to `tab3.a` is valid but its reference to `tab1.a` is not.
    //
    // Scope 5 is the parent of scope 4, scope 4 is the parent of scope 3, and
    // so on. The non-lateral derived table is not allowed to access scopes 2
    // and 3, because they are part of the same lateral chain, but it *is*
    // allowed to access scope 4 and 5. So, to capture this information, we set
    // `lateral_barrier: true` for scope 4.
    pub lateral_barrier: bool,
}

impl ScopeItem {
    pub fn empty() -> ScopeItem {
        ScopeItem {
            table_name: None,
            column_name: UNKNOWN_COLUMN_NAME.into(),
            exprs: BTreeSet::new(),
            from_single_column_function: false,
            allow_unqualified_references: true,
            error_if_referenced: None,
            is_exists_column_for_a_table_function_that_was_in_the_target_list: false,
            _private: (),
        }
    }

    /// Constructs a new scope item from an unqualified column name.
    pub fn from_column_name<N>(column_name: N) -> ScopeItem
    where
        N: Into<ColumnName>,
    {
        ScopeItem::from_name(None, column_name.into())
    }

    /// Constructs a new scope item from a name.
    pub fn from_name<N>(table_name: Option<PartialItemName>, column_name: N) -> ScopeItem
    where
        N: Into<ColumnName>,
    {
        let mut item = ScopeItem::empty();
        item.table_name = table_name;
        item.column_name = column_name.into();
        item
    }

    /// Constructs a new scope item with no name from an expression.
    pub fn from_expr(expr: impl Into<Option<Expr<Aug>>>) -> ScopeItem {
        let mut item = ScopeItem::empty();
        if let Some(expr) = expr.into() {
            item.exprs.insert(expr);
        }
        item
    }

    pub fn is_from_table(&self, table_name: &PartialItemName) -> bool {
        match &self.table_name {
            None => false,
            Some(n) => n.matches(table_name),
        }
    }
}

impl Scope {
    pub fn empty() -> Self {
        Scope {
            items: vec![],
            ungrouped_columns: vec![],
            lateral_barrier: false,
        }
    }

    pub fn from_source<I, N>(table_name: Option<PartialItemName>, column_names: I) -> Self
    where
        I: IntoIterator<Item = N>,
        N: Into<ColumnName>,
    {
        let mut scope = Scope::empty();
        scope.items = column_names
            .into_iter()
            .map(|column_name| ScopeItem::from_name(table_name.clone(), column_name.into()))
            .collect();
        scope
    }

    /// Constructs an iterator over the canonical name for each column.
    pub fn column_names(&self) -> impl Iterator<Item = &ColumnName> {
        self.items.iter().map(|item| &item.column_name)
    }

    pub fn len(&self) -> usize {
        self.items.len()
    }

    /// Iterates over all items in the scope.
    ///
    /// Items are returned in order of preference, where the innermost scope has
    /// the highest preference. For example, given scopes `A(B(C))`, items are
    /// presented in the order `C`, `B`, `A`.
    ///
    /// Items are returned alongside the column reference that targets that item
    /// and the item's "lateral level". The latter bears explaining. The lateral
    /// level is the number of lateral barriers between this scope and the item.
    /// See `Scope::lateral_barrier` for a diagram. Roughly speaking, items from
    /// different levels but the same lateral level are items from different
    /// joins in the same subquery, while items in different lateral levels are
    /// items from different queries entirely. Rules about ambiguity apply
    /// within an entire lateral level, not just within a single scope level.
    ///
    /// NOTE(benesch): Scope` really shows its weaknesses here. Ideally we'd
    /// have separate types like `QueryScope` and `JoinScope` that more
    /// naturally encode the concept of a "lateral level", or at least something
    /// along those lines.
    fn all_items<'a>(
        &'a self,
        outer_scopes: &'a [Scope],
    ) -> impl Iterator<Item = ScopeCursor<'a>> + 'a {
        let mut lat_level = 0;
        iter::once(self)
            .chain(outer_scopes)
            .enumerate()
            .flat_map(move |(level, scope)| {
                if scope.lateral_barrier {
                    lat_level += 1;
                }
                let items = scope
                    .items
                    .iter()
                    .enumerate()
                    .map(move |(column, item)| ScopeCursor {
                        lat_level,
                        inner: ScopeCursorInner::Item {
                            column: ColumnRef { level, column },
                            item,
                        },
                    });
                let ungrouped_columns = scope.ungrouped_columns.iter().map(move |uc| ScopeCursor {
                    lat_level,
                    inner: ScopeCursorInner::UngroupedColumn(uc),
                });
                items.chain(ungrouped_columns)
            })
    }

    /// Returns all items from the given table name in the closest scope.
    ///
    /// If no tables with the given name are in scope, returns an empty
    /// iterator.
    ///
    /// NOTE(benesch): This is wrong for zero-arity relations, because we can't
    /// distinguish between "no such table" and a table that exists but has no
    /// columns. The current design of scope makes this difficult to fix,
    /// unfortunately.
    pub fn items_from_table<'a>(
        &'a self,
        outer_scopes: &'a [Scope],
        table: &PartialItemName,
    ) -> Result<Vec<(ColumnRef, &'a ScopeItem)>, PlanError> {
        let mut seen_level = None;
        let items: Vec<_> = self
            .all_items(outer_scopes)
            .filter_map(move |c| match c.inner {
                ScopeCursorInner::Item { column, item }
                    if item.is_from_table(table)
                        && *seen_level.get_or_insert(c.lat_level) == c.lat_level =>
                {
                    Some((column, item))
                }
                _ => None,
            })
            .collect();
        if !items.iter().map(|(column, _)| column.level).all_equal() {
            return Err(PlanError::AmbiguousTable(table.clone()));
        }
        Ok(items)
    }

    /// Returns a matching [`ColumnRef`] and interned name, if one exists.
    ///
    /// Filters all visible items against the provided `matches` closure, and then matches this
    /// filtered set against the provided `column_name`.
    fn resolve_internal<'a, M>(
        &'a self,
        outer_scopes: &[Scope],
        mut matches: M,
        table_name: Option<&PartialItemName>,
        column_name: &ColumnName,
        name_manager: &mut NameManager,
    ) -> Result<(ColumnRef, Arc<str>), PlanError>
    where
        M: FnMut(&ScopeCursor) -> bool,
    {
        let mut results = self
            .all_items(outer_scopes)
            .filter(|c| (matches)(c) && c.column_name() == column_name);
        match results.next() {
            None => {
                let similar = self
                    .all_items(outer_scopes)
                    .filter(|c| (matches)(c))
                    .filter_map(|c| {
                        c.column_name()
                            .is_similar(column_name)
                            .then(|| c.column_name().clone())
                    })
                    .collect();
                Err(PlanError::UnknownColumn {
                    table: table_name.cloned(),
                    column: column_name.clone(),
                    similar,
                })
            }
            Some(c) => {
                if let Some(ambiguous) = results.find(|c2| c.lat_level == c2.lat_level) {
                    if let Some(table_name) = table_name {
                        if let (
                            ScopeCursorInner::Item {
                                column: ColumnRef { level: c_level, .. },
                                ..
                            },
                            ScopeCursorInner::Item {
                                column:
                                    ColumnRef {
                                        level: ambiguous_level,
                                        ..
                                    },
                                ..
                            },
                        ) = (c.inner, ambiguous.inner)
                        {
                            // ColumnRefs with identical levels indicate multiple columns of the
                            // same name in relation. If the levels differ then it is instead two
                            // tables with the same name, both having a column with this name.
                            if c_level == ambiguous_level {
                                return Err(PlanError::AmbiguousColumn(column_name.clone()));
                            }
                        }
                        return Err(PlanError::AmbiguousTable(table_name.clone()));
                    } else {
                        return Err(PlanError::AmbiguousColumn(column_name.clone()));
                    }
                }

                match c.inner {
                    ScopeCursorInner::UngroupedColumn(uc) => Err(PlanError::UngroupedColumn {
                        table: uc.table_name.clone(),
                        column: uc.column_name.clone(),
                    }),
                    ScopeCursorInner::Item { column, item } => {
                        if let Some(error_if_referenced) = item.error_if_referenced {
                            return Err(error_if_referenced(table_name, column_name));
                        }
                        Ok((column, name_manager.intern_scope_item(item)))
                    }
                }
            }
        }
    }

    /// Resolves references to a column name to a single column, or errors if
    /// multiple columns are equally valid references.
    pub fn resolve_column<'a>(
        &'a self,
        outer_scopes: &[Scope],
        column_name: &ColumnName,
        name_manager: &mut NameManager,
    ) -> Result<(ColumnRef, Arc<str>), PlanError> {
        let table_name = None;
        self.resolve_internal(
            outer_scopes,
            |c| c.allow_unqualified_references(),
            table_name,
            column_name,
            name_manager,
        )
    }

    /// Resolves a column name in a `USING` clause.
    pub fn resolve_using_column(
        &self,
        column_name: &ColumnName,
        join_side: JoinSide,
        name_manager: &mut NameManager,
    ) -> Result<(ColumnRef, Arc<str>), PlanError> {
        self.resolve_column(&[], column_name, name_manager)
            .map_err(|e| match e {
                // Attach a bit more context to unknown and ambiguous column
                // errors to match PostgreSQL.
                PlanError::AmbiguousColumn(column) => {
                    PlanError::AmbiguousColumnInUsingClause { column, join_side }
                }
                PlanError::UnknownColumn { column, .. } => {
                    PlanError::UnknownColumnInUsingClause { column, join_side }
                }
                _ => e,
            })
    }

    /// Resolves a named column reference in a given scope.
    /// Returns the interned name for tracking purposes.
    pub fn resolve_table_column<'a>(
        &'a self,
        outer_scopes: &[Scope],
        table_name: &PartialItemName,
        column_name: &ColumnName,
        name_manager: &mut NameManager,
    ) -> Result<(ColumnRef, Arc<str>), PlanError> {
        let mut seen_at_level = None;
        self.resolve_internal(
            outer_scopes,
            |c| {
                // Once we've matched a table name at a lateral level, even if
                // the column name did not match, we can never match an item
                // from another lateral level.
                if let Some(seen_at_level) = seen_at_level {
                    if seen_at_level != c.lat_level {
                        return false;
                    }
                }
                if c.table_name().as_ref().map(|n| n.matches(table_name)) == Some(true) {
                    seen_at_level = Some(c.lat_level);
                    true
                } else {
                    false
                }
            },
            Some(table_name),
            column_name,
            name_manager,
        )
    }

    /// Look to see if there is an already-calculated instance of this expr.
    /// Failing to find one is not an error, so this just returns an Option.
    ///
    /// We do, however, return the `ScopeItem`, so that we can resolve and
    /// intern the column name.
    pub fn resolve_expr<'a>(&'a self, expr: &Expr<Aug>) -> Option<(ColumnRef, &'a ScopeItem)> {
        // Literal values should not be treated as "cached" because their types
        // in scope will have already been determined, but the type of the
        // reoccurence of the expr might want to have a different type.
        //
        // This is most evident in the case of literal `NULL` values. The first
        // occurrence is likely to be cast as `ScalarType::String`, but
        // subsequent `NULL` values should be untyped.
        if matches!(expr, Expr::Value(_)) {
            return None;
        }

        self.items
            .iter()
            .enumerate()
            .find(|(_, item)| item.exprs.contains(expr))
            .map(|(i, item)| {
                (
                    ColumnRef {
                        level: 0,
                        column: i,
                    },
                    item,
                )
            })
    }

    pub fn product(self, right: Self) -> Result<Self, PlanError> {
        let mut l_tables = self.table_names().into_iter().collect::<Vec<_>>();
        // Make ordering deterministic for testing
        l_tables.sort_by(|l, r| l.item.cmp(&r.item));
        let r_tables = right.table_names();
        for l in l_tables {
            for r in &r_tables {
                if l.matches(r) {
                    sql_bail!("table name \"{}\" specified more than once", l.item)
                }
            }
        }
        Ok(Scope {
            items: self.items.into_iter().chain(right.items).collect(),
            ungrouped_columns: vec![],
            lateral_barrier: false,
        })
    }

    pub fn project(&self, columns: &[usize]) -> Self {
        Scope {
            items: columns.iter().map(|&i| self.items[i].clone()).collect(),
            ungrouped_columns: vec![],
            lateral_barrier: false,
        }
    }

    pub fn table_names(&self) -> BTreeSet<&PartialItemName> {
        self.items
            .iter()
            .filter_map(|name| name.table_name.as_ref())
            .collect::<BTreeSet<&PartialItemName>>()
    }
}

/// A pointer to a scope item or an ungrouped column along side its lateral
/// level. Used internally while iterating.
#[derive(Debug, Clone)]
struct ScopeCursor<'a> {
    lat_level: usize,
    inner: ScopeCursorInner<'a>,
}

#[derive(Debug, Clone)]
enum ScopeCursorInner<'a> {
    Item {
        column: ColumnRef,
        item: &'a ScopeItem,
    },
    UngroupedColumn(&'a ScopeUngroupedColumn),
}

impl ScopeCursor<'_> {
    fn table_name(&self) -> Option<&PartialItemName> {
        match &self.inner {
            ScopeCursorInner::Item { item, .. } => item.table_name.as_ref(),
            ScopeCursorInner::UngroupedColumn(uc) => uc.table_name.as_ref(),
        }
    }

    fn column_name(&self) -> &ColumnName {
        match &self.inner {
            ScopeCursorInner::Item { item, .. } => &item.column_name,
            ScopeCursorInner::UngroupedColumn(uc) => &uc.column_name,
        }
    }

    fn allow_unqualified_references(&self) -> bool {
        match &self.inner {
            ScopeCursorInner::Item { item, .. } => item.allow_unqualified_references,
            ScopeCursorInner::UngroupedColumn(uc) => uc.allow_unqualified_references,
        }
    }
}

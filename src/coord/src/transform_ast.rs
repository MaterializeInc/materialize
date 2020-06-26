// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::{HashMap, HashSet};

use sql::names::FullName;
use sql_parser::ast::visit::{self, Visit};
use sql_parser::ast::visit_mut::{self, VisitMut};
use sql_parser::ast::{Expr, Ident, ObjectName, Query, Statement};

/// Updates all instances of `from_name` in `source` to `to_name` or errors if
/// request is ambiguous.
///
/// Requests are considered ambiguous if `create_stmt` is a `Statement::CreateView`,
/// and any of the following apply to its `query`:
/// - `to_name.item` is used as an [`Ident`] in `query`.
/// - `from_name.item` does not unambiguously refer to an item in the query,
///   e.g. it is also used as a schema.
/// - `to_name.item` does not unambiguously refer to an item in the query after
///   the rename. Right now, given the first condition, this is just a coherence
///   check, but will be more meaningful once the first restriction is lifted.
pub fn rename_in_create_stmt(
    create_stmt: &mut Statement,
    from_name: &FullName,
    to_name: &FullName,
) -> Result<(), String> {
    let from_object = ObjectName::from(from_name).to_string();
    let maybe_update_object_name = |object_name: &mut ObjectName| {
        // `ObjectName` sensibly doesn't `impl PartialEq`, so we have to cheat
        // it.
        if object_name.to_string() == from_object {
            object_name.0[2] = Ident::new(to_name.item.clone());
        }
    };

    match create_stmt {
        Statement::CreateView { name, query, .. } => {
            maybe_update_object_name(name);
            rewrite_query(from_name, to_name, query)?;
        }
        Statement::CreateSource { name, .. } => {
            maybe_update_object_name(name);
        }
        Statement::CreateSink { name, from, .. } => {
            maybe_update_object_name(name);
            maybe_update_object_name(from);
        }
        Statement::CreateIndex { name, on_name, .. } => {
            let idents = &on_name.0;
            // Determine if the database and schema match. This is
            // future-proofing instances where a view might depend on another
            // view's or source's index, e.g. though index hints.
            let db_schema_match = idents[0].to_string() == from_name.database.to_string()
                && idents[1].to_string() == from_name.schema;

            // Maybe rename the index itself...
            if db_schema_match && name.as_ref().unwrap().to_string() == from_name.item {
                *name = Some(Ident::new(to_name.item.clone()));
            // ...or its parent item.
            } else if db_schema_match && idents[2].to_string() == from_name.item {
                on_name.0[2] = Ident::new(to_name.item.clone());
            }
        }
        _ => unreachable!(),
    }

    Ok(())
}

/// Rewrites query's references of `from` to `to` or errors if too ambiguous.
fn rewrite_query(from: &FullName, to: &FullName, query: &mut Query) -> Result<(), String> {
    let from_ident = Ident::new(from.item.clone());
    let to_ident = Ident::new(to.item.clone());
    let qual_depth =
        QueryIdentAgg::determine_qual_depth(&from_ident, Some(to_ident.clone()), query)?;
    CreateSqlRewriter::rewrite_query_with_qual_depth(from, to, qual_depth, query);
    // Ensure that our rewrite didn't didn't introduce ambiguous
    // references to `to_name`.
    match QueryIdentAgg::determine_qual_depth(&to_ident, None, query) {
        Ok(_) => Ok(()),
        Err(e) => Err(e),
    }
}

fn ambiguous_err(n: &Ident, t: &str) -> String {
    format!("{} potentially used ambiguously as item and {}", n, t)
}

/// Visits a [`Query`], assessing catalog item [`Ident`]s' use of a specified `Ident`.
struct QueryIdentAgg<'a> {
    /// The name whose usage you want to assess.
    name: &'a Ident,
    /// Tracks all second-level qualifiers used on `name` in a `HashMap`, as
    /// well as any third-level qualifiers used on those second-level qualifiers
    /// in a `HashSet`.
    qualifiers: HashMap<Ident, HashSet<Ident>>,
    /// Tracks all potential item names we encounter.
    items: HashSet<Ident>,
    /// Tracks the least qualified instance of `name` seen.
    min_qual_depth: usize,
    /// Provides an option to fail the visit if encounters a specified `Ident`.
    fail_on: Option<Ident>,
    err: Option<String>,
}

impl<'a> QueryIdentAgg<'a> {
    /// Determines the depth of qualification needed to unambiguously reference
    /// catalog items in a [`Query`].
    ///
    /// Includes an option to fail if a given `Ident` is encountered.
    ///
    /// `Result`s of `Ok(usize)` indicate that `name` can be unambiguously
    /// referred to with `usize` parts, e.g. 2 requires schema and item name
    /// qualification.
    ///
    /// `Result`s of `Err` indicate that we cannot unambiguously reference
    /// `name` or encountered `fail_on`, if it's provided.
    fn determine_qual_depth(
        name: &Ident,
        fail_on: Option<Ident>,
        query: &Query,
    ) -> Result<usize, String> {
        let mut v = QueryIdentAgg {
            qualifiers: HashMap::new(),
            items: HashSet::new(),
            min_qual_depth: usize::MAX,
            err: None,
            name,
            fail_on,
        };

        // Aggregate identities in `v`.
        v.visit_query(query);

        if let Some(e) = v.err {
            return Err(e);
        }

        // If any `Ident` is used to qualify `name` and is an item, then `name`
        // might be a column.
        if v.items.iter().any(|i| v.qualifiers.contains_key(i)) {
            return Err(ambiguous_err(name, "column"));
        }

        // If we see `<qual>.<item>`, but never see `<other>.<qual>.<item>`,
        // then `<item>` is used as a schema.
        if v.qualifiers.values().any(|v| v.is_empty()) {
            return Err(ambiguous_err(name, "schema"));
        }

        // We cannot disambiguate items where `name` is used to qualify itself.
        // e.g. if we encounter `a.b.a` we do not determine which level of
        // qualification `a` applies to.
        if v.qualifiers.values().any(|t| t.contains(&name)) || v.qualifiers.contains_key(&name) {
            return Err(ambiguous_err(name, "database or schema"));
        }
        // Check if there was more than one 3rd-level (e.g.
        // database) qualification used for any reference to `name`.
        let req_depth = if v.qualifiers.values().any(|v| v.len() > 1) {
            3
        // Check if there was more than one 2nd-level (e.g. schema)
        // qualification used for any reference to `name`.
        } else if v.qualifiers.len() > 1 {
            2
        } else {
            return Ok(1);
        };

        if v.min_qual_depth < req_depth {
            Err(format!(
                "{} is not sufficiently qualified to support renaming",
                name
            ))
        } else {
            Ok(req_depth)
        }
    }

    /// Assesses `v` for uses of `self.name` and `self.fail_on`.
    fn aggregate_names(&mut self, v: &[Ident]) {
        // Fail if we encounter `self.fail_on`.
        if let Some(f) = &self.fail_on {
            if v.iter().any(|i| i == f) {
                self.err = Some(format!(
                    "found reference to {}; cannot rename {} to any identity used in an \
                    existing view definition",
                    f, self.name
                ));
                return;
            }
        }

        // All items are referred to at least once with a `<db>.<scm>.<item>`
        // reference, so processing values here ensures we see all item names.
        if v.len() == 3 {
            self.items.insert(v[2].clone());
        }

        if let Some(p) = v.iter().rposition(|i| i == self.name) {
            // Ensures that the match is never in the database/schema
            // qualification position. e.g. `[<match>, <miss>, <miss>]`
            // indicates that the match is in either the database or schema
            // qualification position, which we disallow.
            if v.len() - p > 2 {
                self.err = Some(ambiguous_err(self.name, "database or schema"));
            }

            // Get elements of `v` leading up to and including `p`.
            let i = v[..p + 1].to_vec();
            match i.len() {
                1 => {
                    // Indicates that this is an unqualified reference to
                    // `name`, i.e. a column reference, which we disallow.
                    // However, this is only the case when `v.len() == 1`; in
                    // other cases, it indicates an unqualified use of
                    // `self.name`.
                    if v.len() == 1 {
                        self.err = Some(ambiguous_err(self.name, "column"));
                    }
                }
                2 => {
                    //  This is a column name, i.e `<item>.<col>`.
                    if v.len() == 2 {
                        self.err = Some(ambiguous_err(self.name, "column"));
                    }

                    // 2nd-level qualification on `self.name`.
                    self.qualifiers.entry(i[0].clone()).or_default();
                }
                3 => {
                    // 3rd-level qualification on `self.name`.
                    self.qualifiers
                        .entry(i[1].clone())
                        .or_default()
                        .insert(i[0].clone());
                }
                4 => {
                    // `self.name` used as a column name, i.e. `<db>.<scm>.<item>.<col>`.
                    self.err = Some(ambiguous_err(self.name, "column"));
                }
                _ => unreachable!(),
            }
            self.min_qual_depth = std::cmp::min(i.len(), self.min_qual_depth);
        }
    }
}

impl<'a, 'ast> Visit<'ast> for QueryIdentAgg<'a> {
    fn visit_query(&mut self, query: &'ast Query) {
        visit::visit_query(self, query);
    }
    fn visit_expr(&mut self, e: &'ast Expr) {
        match e {
            Expr::Identifier(i) | Expr::QualifiedWildcard(i) => {
                self.aggregate_names(i);
            }
            _ => visit::visit_expr(self, e),
        }
    }
    fn visit_ident(&mut self, ident: &'ast Ident) {
        // This is an unqualified item using `self.name`, e.g. an alias, which
        // we cannot unambiguously resolve.
        if ident == self.name {
            self.err = Some(ambiguous_err(self.name, "alias or column"));
        }
    }
    fn visit_object_name(&mut self, object_name: &'ast ObjectName) {
        self.aggregate_names(&object_name.0);
    }
}

struct CreateSqlRewriter {
    from: Vec<Ident>,
    to: Vec<Ident>,
}

impl CreateSqlRewriter {
    fn rewrite_query_with_qual_depth(
        from_name: &FullName,
        to_name: &FullName,
        qual_depth: usize,
        query: &mut Query,
    ) {
        let (from, to) = match qual_depth {
            1 => (
                vec![Ident::new(from_name.item.clone())],
                vec![Ident::new(to_name.item.clone())],
            ),
            2 => (
                vec![
                    Ident::new(from_name.schema.clone()),
                    Ident::new(from_name.item.clone()),
                ],
                vec![
                    Ident::new(to_name.schema.clone()),
                    Ident::new(to_name.item.clone()),
                ],
            ),
            3 => (
                vec![
                    Ident::new(from_name.database.to_string()),
                    Ident::new(from_name.schema.clone()),
                    Ident::new(from_name.item.clone()),
                ],
                vec![
                    Ident::new(to_name.database.to_string()),
                    Ident::new(to_name.schema.clone()),
                    Ident::new(to_name.item.clone()),
                ],
            ),
            _ => unreachable!(),
        };
        let mut v = CreateSqlRewriter { from, to };
        v.visit_query_mut(query);
    }

    fn maybe_rewrite_idents(&mut self, h: &mut Vec<Ident>) {
        // We don't want to rewrite if the item we're rewriting is shorter than
        // the values we want to replace them with.
        if h.len() < self.from.len() {
            return;
        }
        let n = &self.from;
        for i in 0..h.len() - n.len() + 1 {
            // If subset of `h` matches `self.from`...
            if h[i..i + n.len()] == n[..] {
                // ...splice `self.to` into `h` in that subset's location.
                h.splice(i..i + n.len(), self.to.iter().cloned());
                return;
            }
        }
    }
}

impl<'ast> VisitMut<'ast> for CreateSqlRewriter {
    fn visit_query_mut(&mut self, query: &'ast mut Query) {
        visit_mut::visit_query_mut(self, query);
    }
    fn visit_expr_mut(&mut self, e: &'ast mut Expr) {
        match e {
            Expr::Identifier(ref mut i) | Expr::QualifiedWildcard(ref mut i) => {
                self.maybe_rewrite_idents(i);
            }
            _ => visit_mut::visit_expr_mut(self, e),
        }
    }
    fn visit_object_name_mut(&mut self, object_name: &'ast mut ObjectName) {
        self.maybe_rewrite_idents(&mut object_name.0);
    }
}

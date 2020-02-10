// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use failure::bail;

use catalog::names::{DatabaseSpecifier, FullName, PartialName};
use ore::collections::CollectionExt;
use repr::ColumnName;
use sql_parser::ast::visit_mut::VisitMut;
use sql_parser::ast::{Expr, Function, Ident, ObjectName, Statement, TableAlias};

use crate::statement::StatementContext;

pub fn ident(ident: Ident) -> String {
    if ident.quote_style.is_some() {
        ident.value
    } else {
        ident.value.to_lowercase()
    }
}

pub fn function_name(name: ObjectName) -> Result<String, failure::Error> {
    if name.0.len() != 1 {
        bail!("qualified function names are not supported");
    }
    Ok(ident(name.0.into_element()))
}

pub fn column_name(id: Ident) -> ColumnName {
    ColumnName::from(ident(id))
}

pub fn object_name(mut name: ObjectName) -> Result<PartialName, failure::Error> {
    if name.0.len() < 1 || name.0.len() > 3 {
        bail!(
            "qualified names must have between 1 and 3 components, got {}: {}",
            name.0.len(),
            name
        );
    }
    let out = PartialName {
        item: ident(
            name.0
                .pop()
                .expect("name checked to have at least one component"),
        ),
        schema: name.0.pop().map(ident),
        database: name.0.pop().map(ident),
    };
    assert!(name.0.is_empty());
    Ok(out)
}

pub fn unresolve(name: FullName) -> ObjectName {
    let mut out = vec![];
    if let DatabaseSpecifier::Name(n) = name.database {
        out.push(Ident::with_quote('"', n));
    }
    out.push(Ident::with_quote('"', name.schema));
    out.push(Ident::with_quote('"', name.item));
    ObjectName(out)
}

/// Normalizes a `CREATE { SOURCE | VIEW | INDEX | SINK }` statement so that the
/// statement does not depend upon any session parameters, nor specify any
/// non-default options (like `MATERIALIZED`, `IF NOT EXISTS`, etc).
///
/// The goal is to construct a backwards-compatible description of the object.
/// SQL is the most stable part of Materialize, so SQL is used to describe the
/// objects that are persisted in the catalog.
pub fn create_statement(
    scx: &StatementContext,
    mut stmt: Statement,
) -> Result<String, failure::Error> {
    fn norm_ident(ident: &mut Ident) {
        if ident.quote_style.is_none() {
            ident.value = ident.value.to_lowercase();
        }
        ident.quote_style = Some('"');
    };

    let allocate_name = |name: &ObjectName| -> Result<_, failure::Error> {
        Ok(unresolve(scx.allocate_name(object_name(name.clone())?)))
    };

    let resolve_name = |name: &ObjectName| -> Result<_, failure::Error> {
        Ok(unresolve(scx.resolve_name(name.clone())?))
    };

    struct QueryNormalizer<'a> {
        scx: &'a StatementContext<'a>,
        err: Option<failure::Error>,
    }

    impl<'a, 'ast> VisitMut<'ast> for QueryNormalizer<'a> {
        fn visit_function(&mut self, func: &'ast mut Function) {
            // Don't visit the function name, because function names are not
            // (yet) object names we can resolve.
            for arg in &mut func.args {
                self.visit_expr(arg);
            }
            if let Some(over) = &mut func.over {
                self.visit_window_spec(over);
            }
        }

        fn visit_table_table_factor(
            &mut self,
            name: &'ast mut ObjectName,
            alias: Option<&'ast mut TableAlias>,
            args: &'ast mut [Expr],
            with_hints: &'ast mut [Expr],
        ) {
            // Only attempt to resolve the name if it is not a table function
            // (i.e., there are no arguments).
            if args.is_empty() {
                self.visit_object_name(name)
            }
            for expr in args {
                self.visit_expr(expr);
            }
            if let Some(alias) = alias {
                self.visit_table_alias(alias);
            }
            for expr in with_hints {
                self.visit_expr(expr);
            }
        }

        fn visit_object_name(&mut self, object_name: &'ast mut ObjectName) {
            match self.scx.resolve_name(object_name.clone()) {
                Ok(full_name) => *object_name = unresolve(full_name),
                Err(e) => self.err = Some(e),
            };
        }

        fn visit_ident(&mut self, ident: &'ast mut Ident) {
            norm_ident(ident);
        }
    }

    // Think very hard before changing any of the branches in this match
    // statement. All identifiers must be quoted. All object names must be
    // allocated or resolved, depending on whether they are the object created
    // by the statement or an object depended upon by the statement. All
    // non-default options must be disabled.
    //
    // Wildcard matches are explicitly avoided so that future additions to the
    // syntax cause compile errors here. Before you ignore a new field, triple
    // check that it does not need to be normalized according to the rules
    // above.
    match &mut stmt {
        Statement::CreateSource {
            name,
            connector: _,
            format: _,
            envelope: _,
            if_not_exists,
        } => {
            *name = allocate_name(name)?;
            *if_not_exists = false;
        }

        Statement::CreateSink {
            name,
            from,
            connector: _,
            format: _,
            if_not_exists,
        } => {
            *name = allocate_name(name)?;
            *from = resolve_name(from)?;
            *if_not_exists = false;
        }

        Statement::CreateView {
            name,
            columns,
            query,
            materialized,
            replace,
            with_options: _,
        } => {
            *name = allocate_name(name)?;
            for c in columns {
                norm_ident(c);
            }
            {
                let mut normalizer = QueryNormalizer { scx, err: None };
                normalizer.visit_query(query);
                if let Some(err) = normalizer.err {
                    return Err(err);
                }
            }
            *materialized = false;
            *replace = false;
        }

        Statement::CreateIndex {
            name,
            on_name,
            key_parts,
            if_not_exists,
        } => {
            norm_ident(name);
            *on_name = resolve_name(on_name)?;
            let mut normalizer = QueryNormalizer { scx, err: None };
            for key_part in key_parts {
                normalizer.visit_expr(key_part);
                if let Some(err) = normalizer.err {
                    return Err(err);
                }
            }
            *if_not_exists = false;
        }

        _ => unreachable!(),
    }

    Ok(stmt.to_string())
}

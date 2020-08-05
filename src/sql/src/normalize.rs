// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::HashMap;

use ore::collections::CollectionExt;
use repr::ColumnName;
use sql_parser::ast::display::AstDisplay;
use sql_parser::ast::visit_mut::{self, VisitMut};
use sql_parser::ast::{
    CreateIndexStatement, CreateSinkStatement, CreateSourceStatement, CreateTableStatement,
    CreateViewStatement, Function, FunctionArgs, Ident, IfExistsBehavior, ObjectName, SqlOption,
    Statement, TableFactor, Value,
};

use crate::names::{DatabaseSpecifier, FullName, PartialName};
use crate::plan::error::PlanError;
use crate::plan::statement::StatementContext;

pub fn ident(ident: Ident) -> String {
    ident.as_str().into()
}

pub fn function_name(name: ObjectName) -> Result<String, PlanError> {
    if name.0.len() != 1 {
        unsupported!("qualified function names");
    }
    Ok(ident(name.0.into_element()))
}

pub fn column_name(id: Ident) -> ColumnName {
    ColumnName::from(ident(id))
}

pub fn object_name(mut name: ObjectName) -> Result<PartialName, PlanError> {
    if name.0.len() < 1 || name.0.len() > 3 {
        return Err(PlanError::MisqualifiedName(name.to_string()));
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

pub fn with_options(options: &[SqlOption]) -> HashMap<String, Value> {
    options
        .iter()
        .map(|o| (ident(o.name.clone()), o.value.clone()))
        .collect()
}

pub fn unresolve(name: FullName) -> ObjectName {
    let mut out = vec![];
    if let DatabaseSpecifier::Name(n) = name.database {
        out.push(Ident::new(n));
    }
    out.push(Ident::new(name.schema));
    out.push(Ident::new(name.item));
    ObjectName(out)
}

/// Normalizes a `CREATE { SOURCE | VIEW | INDEX | SINK }` statement so that the
/// statement does not depend upon any session parameters, nor specify any
/// non-default options (like `MATERIALIZED`, `IF NOT EXISTS`, etc).
///
/// The goal is to construct a backwards-compatible description of the object.
/// SQL is the most stable part of Materialize, so SQL is used to describe the
/// objects that are persisted in the catalog.
pub fn create_statement(scx: &StatementContext, mut stmt: Statement) -> Result<String, PlanError> {
    let allocate_name = |name: &ObjectName| -> Result<_, PlanError> {
        Ok(unresolve(scx.allocate_name(object_name(name.clone())?)))
    };

    let allocate_temporary_name = |name: &ObjectName| -> Result<_, PlanError> {
        Ok(unresolve(
            scx.allocate_temporary_name(object_name(name.clone())?),
        ))
    };

    let resolve_item = |name: &ObjectName| -> Result<_, PlanError> {
        Ok(unresolve(scx.resolve_item(name.clone())?))
    };

    struct QueryNormalizer<'a> {
        scx: &'a StatementContext<'a>,
        err: Option<PlanError>,
    }

    impl<'a, 'ast> VisitMut<'ast> for QueryNormalizer<'a> {
        fn visit_function_mut(&mut self, func: &'ast mut Function) {
            // Don't visit the function name, because function names are not
            // (yet) object names we can resolve.
            match &mut func.args {
                FunctionArgs::Star => (),
                FunctionArgs::Args(args) => {
                    for arg in args {
                        self.visit_expr_mut(arg);
                    }
                }
            }
            if let Some(over) = &mut func.over {
                self.visit_window_spec_mut(over);
            }
        }

        fn visit_table_factor_mut(&mut self, table_factor: &'ast mut TableFactor) {
            match table_factor {
                TableFactor::Table { name, alias } => {
                    self.visit_object_name_mut(name);
                    if let Some(alias) = alias {
                        self.visit_table_alias_mut(alias);
                    }
                }
                TableFactor::Function {
                    name: _,
                    args,
                    alias,
                } => {
                    match args {
                        FunctionArgs::Star => (),
                        FunctionArgs::Args(args) => {
                            for expr in args {
                                self.visit_expr_mut(expr);
                            }
                        }
                    }
                    if let Some(alias) = alias {
                        self.visit_table_alias_mut(alias);
                    }
                }
                // We only need special behavior for `TableFactor::Table` and
                // `TableFactor::Function`. Just visit the other types of table
                // factors like normal.
                _ => visit_mut::visit_table_factor_mut(self, table_factor),
            }
        }

        fn visit_object_name_mut(&mut self, object_name: &'ast mut ObjectName) {
            match self.scx.resolve_item(object_name.clone()) {
                Ok(full_name) => *object_name = unresolve(full_name),
                Err(e) => self.err = Some(e),
            };
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
        Statement::CreateSource(CreateSourceStatement {
            name,
            col_names: _,
            connector: _,
            with_options: _,
            format: _,
            envelope: _,
            if_not_exists,
            materialized,
        }) => {
            *name = allocate_name(name)?;
            *if_not_exists = false;
            *materialized = false;
        }

        Statement::CreateTable(CreateTableStatement {
            name,
            columns: _,
            constraints: _,
            with_options: _,
            if_not_exists,
        }) => {
            *name = allocate_name(name)?;
            *if_not_exists = false;
        }

        Statement::CreateSink(CreateSinkStatement {
            name,
            from,
            connector: _,
            with_options: _,
            format: _,
            with_snapshot: _,
            as_of: _,
            if_not_exists,
        }) => {
            *name = allocate_name(name)?;
            *from = resolve_item(from)?;
            *if_not_exists = false;
        }

        Statement::CreateView(CreateViewStatement {
            name,
            columns: _,
            query,
            temporary,
            materialized,
            if_exists,
            with_options: _,
        }) => {
            *name = if *temporary {
                allocate_temporary_name(name)?
            } else {
                allocate_name(name)?
            };
            {
                let mut normalizer = QueryNormalizer { scx, err: None };
                normalizer.visit_query_mut(query);
                if let Some(err) = normalizer.err {
                    return Err(err);
                }
            }
            *materialized = false;
            *if_exists = IfExistsBehavior::Error;
        }

        Statement::CreateIndex(CreateIndexStatement {
            name: _,
            on_name,
            key_parts,
            if_not_exists,
        }) => {
            *on_name = resolve_item(on_name)?;
            let mut normalizer = QueryNormalizer { scx, err: None };
            if let Some(key_parts) = key_parts {
                for key_part in key_parts {
                    normalizer.visit_expr_mut(key_part);
                    if let Some(err) = normalizer.err {
                        return Err(err);
                    }
                }
            }
            *if_not_exists = false;
        }

        _ => unreachable!(),
    }

    Ok(stmt.to_ast_string_stable())
}

#[cfg(test)]
mod tests {
    use std::cell::RefCell;
    use std::collections::BTreeMap;
    use std::error::Error;
    use std::rc::Rc;

    use super::*;
    use crate::catalog::DummyCatalog;
    use crate::plan::PlanContext;

    #[test]
    fn normalized_create() -> Result<(), Box<dyn Error>> {
        let scx = &StatementContext {
            pcx: &PlanContext::default(),
            catalog: &DummyCatalog,
            param_types: Rc::new(RefCell::new(BTreeMap::new())),
        };

        let parsed = sql_parser::parser::parse_statements(
            "create materialized view foo as select 1 as bar".into(),
        )?
        .into_element();

        // Ensure that all identifiers are quoted.
        assert_eq!(
            r#"CREATE VIEW "dummy"."public"."foo" AS SELECT 1 AS "bar""#,
            create_statement(scx, parsed)?,
        );

        Ok(())
    }
}

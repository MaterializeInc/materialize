// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use anyhow::bail;

use ore::collections::CollectionExt;
use sql::ast::display::AstDisplay;
use sql::ast::visit_mut::{self, VisitMut};
use sql::ast::{
    CreateIndexStatement, CreateSinkStatement, CreateTableStatement, CreateTypeStatement,
    CreateViewStatement, DataType, Function, Ident, Raw, RawName, Statement, TableFactor,
    UnresolvedObjectName, ViewDefinition,
};
use sql::plan::resolve_names_stmt;

use crate::catalog::storage::Transaction;
use crate::catalog::{Catalog, ConnCatalog, SerializedCatalogItem};
use crate::catalog::{MZ_CATALOG_SCHEMA, MZ_INTERNAL_SCHEMA, PG_CATALOG_SCHEMA};

fn rewrite_items<F>(tx: &Transaction, mut f: F) -> Result<(), anyhow::Error>
where
    F: FnMut(&mut sql::ast::Statement<Raw>) -> Result<(), anyhow::Error>,
{
    let items = tx.load_items()?;
    for (id, name, def) in items {
        let SerializedCatalogItem::V1 {
            create_sql,
            eval_env,
        } = serde_json::from_slice(&def)?;
        let mut stmt = sql::parse::parse(&create_sql)?.into_element();

        f(&mut stmt)?;

        let serialized_item = SerializedCatalogItem::V1 {
            create_sql: stmt.to_ast_string_stable(),
            eval_env,
        };

        let serialized_item =
            serde_json::to_vec(&serialized_item).expect("catalog serialization cannot fail");
        tx.update_item(id, &name.item, &serialized_item)?;
    }
    Ok(())
}

pub(crate) fn migrate(catalog: &mut Catalog) -> Result<(), anyhow::Error> {
    let mut storage = catalog.storage();
    let mut tx = storage.transaction()?;
    // First, do basic AST -> AST transformations.
    rewrite_items(&tx, |stmt| {
        ast_rewrite_type_references_0_6_1(stmt)?;
        ast_use_pg_catalog_0_7_1(stmt)?;
        Ok(())
    })?;
    // Then, load up a temporary catalog with the rewritten items, and perform
    // some transformations that require introspecting the catalog. These
    // migrations are *weird*: they're rewriting the catalog while looking at
    // it. You probably should be adding a basic AST migration above, unless
    // you are really certain you want one of these crazy migrations.
    let cat = Catalog::load_catalog_items(&mut tx, &catalog)?;
    let conn_cat = cat.for_system_session();
    rewrite_items(&tx, |item| {
        semantic_use_id_for_table_format_0_7_1(&conn_cat, item)?;
        Ok(())
    })?;
    tx.commit().map_err(|e| e.into())
}

// Add new migrations below their appropriate heading, and precede them with a
// short summary of the migration's purpose and optional additional commentary
// about safety or approach.
//
// The convention is to name the migration function using snake case:
// > <category>_<description>_<version>
//
// Note that:
// - Migration functions must be idempotent because all migrations run every
//   time the catalog opens, unless migrations are explicitly disabled. This
//   might mean changing code outside the migration itself.
// - Migrations must preserve backwards compatibility with all past releases of
//   materialized.
//
// Please include @benesch on any code reviews that add or edit migrations.

// ****************************************************************************
// AST migrations -- Basic AST -> AST transformations
// ****************************************************************************

// Rewrites all function references to have `pg_catalog` qualification; this
// is necessary to support resolving all built-in functions to the catalog.
// (At the time of writing Materialize did not support user-defined
// functions.)
//
// The approach is to prepend `pg_catalog` to all `UnresolvedObjectName`
// names that could refer to functions.
fn ast_use_pg_catalog_0_7_1(stmt: &mut sql::ast::Statement<Raw>) -> Result<(), anyhow::Error> {
    fn normalize_function_name(name: &mut UnresolvedObjectName) {
        if name.0.len() == 1 {
            let func_name = name.to_string();
            for (schema, funcs) in &[
                (PG_CATALOG_SCHEMA, &*sql::func::PG_CATALOG_BUILTINS),
                (MZ_CATALOG_SCHEMA, &*sql::func::MZ_CATALOG_BUILTINS),
                (MZ_INTERNAL_SCHEMA, &*sql::func::MZ_INTERNAL_BUILTINS),
            ] {
                if funcs.contains_key(func_name.as_str()) {
                    *name = UnresolvedObjectName(vec![Ident::new(*schema), name.0.remove(0)]);
                    break;
                }
            }
        }
    }

    struct FuncNormalizer;

    impl<'ast> VisitMut<'ast, Raw> for FuncNormalizer {
        fn visit_function_mut(&mut self, func: &'ast mut Function<Raw>) {
            normalize_function_name(&mut func.name);
            // Function args can be functions themselves, so let the visitor
            // find them.
            visit_mut::visit_function_mut(self, func)
        }
        fn visit_table_factor_mut(&mut self, table_factor: &'ast mut TableFactor<Raw>) {
            if let TableFactor::Function { ref mut name, .. } = table_factor {
                normalize_function_name(name);
            }
            // Function args can be functions themselves, so let the visitor
            // find them.
            visit_mut::visit_table_factor_mut(self, table_factor)
        }
    }

    match stmt {
        Statement::CreateView(CreateViewStatement {
            temporary: _,
            materialized: _,
            if_exists: _,
            definition:
                ViewDefinition {
                    name: _,
                    columns: _,
                    query,
                    with_options: _,
                },
        }) => FuncNormalizer.visit_query_mut(query),

        Statement::CreateIndex(CreateIndexStatement {
            name: _,
            on_name: _,
            key_parts,
            with_options: _,
            if_not_exists: _,
        }) => {
            if let Some(key_parts) = key_parts {
                for key_part in key_parts {
                    FuncNormalizer.visit_expr_mut(key_part);
                }
            }
        }

        Statement::CreateSink(CreateSinkStatement {
            name: _,
            from: _,
            connector: _,
            with_options: _,
            format: _,
            envelope: _,
            with_snapshot: _,
            as_of,
            if_not_exists: _,
        }) => {
            if let Some(expr) = as_of {
                FuncNormalizer.visit_expr_mut(expr);
            }
        }

        // At the time the migration was written, tables, sources, and
        // types could not contain references to functions.
        Statement::CreateTable(_) | Statement::CreateSource(_) | Statement::CreateType(_) => {}

        _ => bail!("catalog item contained inappropriate statement: {}", stmt),
    };

    Ok(())
}

/// Rewrites all built-in type references to have `pg_catalog` qualification;
/// this is necessary to support resolving all type names to the catalog.
///
/// The approach is to prepend `pg_catalog` to all `DataType::Other` names
/// that only contain a single element. We do this in the AST and without
/// replanning the `CREATE` statement because the catalog still contains no
/// items at this point, e.g. attempting to plan any item with a dependency
/// will fail.
fn ast_rewrite_type_references_0_6_1(
    stmt: &mut sql::ast::Statement<Raw>,
) -> Result<(), anyhow::Error> {
    struct TypeNormalizer;

    impl<'ast> VisitMut<'ast, Raw> for TypeNormalizer {
        fn visit_data_type_mut(&mut self, data_type: &'ast mut DataType<Raw>) {
            if let DataType::Other { name, .. } = data_type {
                let mut unresolved_name = name.name().clone();
                if unresolved_name.0.len() == 1 {
                    unresolved_name = UnresolvedObjectName(vec![
                        Ident::new(PG_CATALOG_SCHEMA),
                        unresolved_name.0.remove(0),
                    ]);
                }
                *name = match name {
                    RawName::Name(_) => RawName::Name(unresolved_name),
                    RawName::Id(id, _) => RawName::Id(id.clone(), unresolved_name),
                }
            }
        }
    }

    match stmt {
        Statement::CreateTable(CreateTableStatement {
            name: _,
            columns,
            constraints: _,
            with_options: _,
            if_not_exists: _,
            temporary: _,
        }) => {
            for c in columns {
                TypeNormalizer.visit_column_def_mut(c);
            }
        }

        Statement::CreateView(CreateViewStatement {
            temporary: _,
            materialized: _,
            if_exists: _,
            definition:
                ViewDefinition {
                    name: _,
                    columns: _,
                    query,
                    with_options: _,
                },
        }) => TypeNormalizer.visit_query_mut(query),

        Statement::CreateIndex(CreateIndexStatement {
            name: _,
            on_name: _,
            key_parts,
            with_options,
            if_not_exists: _,
        }) => {
            if let Some(key_parts) = key_parts {
                for key_part in key_parts {
                    TypeNormalizer.visit_expr_mut(key_part);
                }
            }
            for with_option in with_options {
                TypeNormalizer.visit_with_option_mut(with_option);
            }
        }

        Statement::CreateType(CreateTypeStatement {
            name: _,
            as_type: _,
            with_options,
        }) => {
            for option in with_options {
                TypeNormalizer.visit_sql_option_mut(option);
            }
        }

        // At the time the migration was written, sinks and sources
        // could not contain references to types.
        Statement::CreateSource(_) | Statement::CreateSink(_) => {}

        _ => bail!("catalog item contained inappropriate statement: {}", stmt),
    };

    Ok(())
}

// ****************************************************************************
// Semantic migrations -- Weird migrations that require access to the catalog
// ****************************************************************************

// Rewrites all table references to use their id as reference rather than
// their name. This allows us to safely rename tables without having to
// rewrite their dependents.
fn semantic_use_id_for_table_format_0_7_1(
    cat: &ConnCatalog,
    stmt: &mut sql::ast::Statement<Raw>,
) -> Result<(), anyhow::Error> {
    // Resolve Statement<Raw> to Statement<Aug>
    let resolved = resolve_names_stmt(cat, stmt.clone()).unwrap();
    // Use consistent intermediary format between Aug and Raw.
    let create_sql = resolved.to_ast_string_stable();
    // Convert Statement<Aug> to Statement<Raw> (Aug is a subset of Raw's
    // semantics) and reassign to `stmt`.
    *stmt = sql::parse::parse(&create_sql)?.into_element();
    Ok(())
}

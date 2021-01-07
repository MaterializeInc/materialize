// Copyright Materialize, Inc. All rights reserved.
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
use sql::ast::visit_mut::VisitMut;
use sql::ast::{
    CreateIndexStatement, CreateTableStatement, CreateTypeStatement, CreateViewStatement, DataType,
    Ident, ObjectName, Statement,
};

use crate::catalog::PG_CATALOG_SCHEMA;
use crate::catalog::{Catalog, SerializedCatalogItem};

pub const CONTENT_MIGRATIONS: &[fn(&mut Catalog) -> Result<(), anyhow::Error>] = &[
    // Rewrites all built-in type references to have `pg_catalog` qualification;
    // this is necessary to support resolving all type names to the catalog.
    //
    // The approach is to prepend `pg_catalog` to all `DataType::Other` names
    // that only contain a single element. We do this in the AST and without
    // replanning the `CREATE` statement because the catalog still contains no
    // items at this point, e.g. attempting to plan any item with a dependency
    // will fail.
    //
    // Introduced for v0.6.1
    |catalog: &mut Catalog| {
        struct TypeNormalizer;

        impl<'ast> VisitMut<'ast> for TypeNormalizer {
            fn visit_data_type_mut(&mut self, data_type: &'ast mut DataType) {
                if let DataType::Other { name, .. } = data_type {
                    if name.0.len() == 1 {
                        *name = ObjectName(vec![Ident::new(PG_CATALOG_SCHEMA), name.0.remove(0)]);
                    }
                }
            }
        }

        let mut storage = catalog.storage();
        let items = storage.load_items()?;
        let tx = storage.transaction()?;

        for (id, name, def) in items {
            let SerializedCatalogItem::V1 {
                create_sql,
                eval_env,
            } = serde_json::from_slice(&def)?;

            let mut stmt = sql::parse::parse(&create_sql)?.into_element();
            match &mut stmt {
                Statement::CreateTable(CreateTableStatement {
                    name: _,
                    columns,
                    constraints: _,
                    with_options: _,
                    if_not_exists: _,
                }) => {
                    for c in columns {
                        TypeNormalizer.visit_column_def_mut(c);
                    }
                }

                Statement::CreateView(CreateViewStatement {
                    name: _,
                    columns: _,
                    query,
                    temporary: _,
                    materialized: _,
                    if_exists: _,
                    with_options: _,
                }) => TypeNormalizer.visit_query_mut(query),

                Statement::CreateIndex(CreateIndexStatement {
                    name: _,
                    on_name: _,
                    key_parts,
                    if_not_exists: _,
                }) => {
                    if let Some(key_parts) = key_parts {
                        for key_part in key_parts {
                            TypeNormalizer.visit_expr_mut(key_part);
                        }
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
                Statement::CreateSource(_) | Statement::CreateSink(_) => continue,

                _ => bail!("catalog item contained inappropriate statement: {}", stmt),
            }

            let serialized_item = SerializedCatalogItem::V1 {
                create_sql: stmt.to_ast_string_stable(),
                eval_env,
            };

            let serialized_item =
                serde_json::to_vec(&serialized_item).expect("catalog serialization cannot fail");
            tx.update_item(id, &name.item, &serialized_item)?;
        }
        tx.commit()?;
        Ok(())
    },
    // Add new migrations here.
    //
    // Migrations should be preceded with a comment of the following form:
    //
    //     > Short summary of migration's purpose.
    //     >
    //     > Introduced in <VERSION>.
    //     >
    //     > Optional additional commentary about safety or approach.
    //
    // Please include @benesch on any code reviews that add or edit migrations.
    // Migrations must preserve backwards compatibility with all past releases
    // of materialized. Migrations can be edited up until they ship in a
    // release, after which they must never be removed, only patched by future
    // migrations.
];

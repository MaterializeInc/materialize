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
use sql::ast::visit_mut::{self, VisitMut};
use sql::ast::{
    AvroSchema, CreateIndexStatement, CreateSinkStatement, CreateSourceStatement,
    CreateTableStatement, CreateTypeStatement, CreateViewStatement, DataType, Format, Function,
    Ident, Raw, RawName, Statement, TableFactor, UnresolvedObjectName, Value, WithOption,
    WithOptionValue,
};
use sql::plan::resolve_names_stmt;

use crate::catalog::{Catalog, SerializedCatalogItem};
use crate::catalog::{MZ_CATALOG_SCHEMA, MZ_INTERNAL_SCHEMA, PG_CATALOG_SCHEMA};

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
                    temporary: _,
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
    // This was previously the place where the function name migration occurred;
    // however #5802 showed that the implementation was insufficient.
    //
    // Introduced for v0.7.0
    |_: &mut Catalog| Ok(()),
    // Rewrites all function references to have `pg_catalog` qualification; this
    // is necessary to support resolving all built-in functions to the catalog.
    // (At the time of writing Materialize did not support user-defined
    // functions.)
    //
    // The approach is to prepend `pg_catalog` to all `UnresolvedObjectName`
    // names that could refer to functions.
    //
    // Introduced for v0.7.1
    |catalog: &mut Catalog| {
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
                Statement::CreateView(CreateViewStatement {
                    name: _,
                    columns: _,
                    query,
                    temporary: _,
                    materialized: _,
                    if_exists: _,
                    with_options: _,
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
                Statement::CreateTable(_)
                | Statement::CreateSource(_)
                | Statement::CreateType(_) => continue,

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
    // Insert default value for confluent_wire_format
    //
    // This PR introduced a new with options block attached specifically to the
    // inline schema clause. Previously-created versions of this object must
    // have no options specified, so we explicitly set them to their existing
    // behavior.
    //
    // The existing behavior is that we expected all Avro-encoded messages over
    // Kafka to come prepended with a magic byte and a schema registry ID (the
    // "confluent wire format"), and so some folks modified their input stream
    // to include those magic bytes. In case we change the default to a
    // possibly more reasonable one in the future, we ensure that the current
    // default is encoded in the on-disk catalog.
    //
    // Introduced for v0.7.1
    |catalog: &mut Catalog| {
        let mut storage = catalog.storage();
        let items = storage.load_items()?;
        let tx = storage.transaction()?;

        for (id, name, def) in items {
            let SerializedCatalogItem::V1 {
                create_sql,
                eval_env,
            } = serde_json::from_slice(&def)?;

            let mut stmt = sql::parse::parse(&create_sql)?.into_element();

            // the match arm is long enough that this is easier to understand
            #[allow(clippy::single_match)]
            match stmt {
                Statement::CreateSource(CreateSourceStatement {
                    format:
                        Some(Format::Avro(AvroSchema::Schema {
                            ref mut with_options,
                            ..
                        })),
                    ..
                }) => {
                    if with_options.is_empty() {
                        with_options.push(WithOption {
                            key: Ident::new("confluent_wire_format"),
                            value: Some(WithOptionValue::Value(Value::Boolean(true))),
                        })
                    }
                }
                _ => {}
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
    // Rewrites all table references to use their id as reference rather than
    // their name. This allows us to safely rename tables without having to
    // rewrite their dependents.
    //
    // Introduced for v0.7.1
    |catalog: &mut Catalog| {
        let cat = Catalog::load_catalog_items(catalog.clone())?;
        let cat = cat.for_system_session();

        let items = catalog.storage().load_items()?;
        let mut storage = catalog.storage();
        let tx = storage.transaction()?;

        for (id, name, def) in items {
            let SerializedCatalogItem::V1 {
                create_sql,
                eval_env,
            } = serde_json::from_slice(&def)?;

            let stmt = sql::parse::parse(&create_sql)?.into_element();

            let resolved = resolve_names_stmt(&cat, stmt.clone()).unwrap();

            let serialized_item = SerializedCatalogItem::V1 {
                create_sql: resolved.to_ast_string_stable(),
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

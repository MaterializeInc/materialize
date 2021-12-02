// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::fs;

use anyhow::{anyhow, bail};
use futures::executor::block_on;
use lazy_static::lazy_static;
use protobuf::Message;
use regex::Regex;
use repr::strconv;
use semver::Version;
use tempfile;
use tokio::fs::File;

use mz_protoc::Protoc;
use ore::collections::CollectionExt;
use sql::ast::display::AstDisplay;
use sql::ast::visit_mut::{self, VisitMut};
use sql::ast::{
    AvroSchema, CreateIndexStatement, CreateSinkStatement, CreateSourceConnector,
    CreateSourceFormat, CreateSourceStatement, CreateTableStatement, CreateTypeStatement,
    CreateViewStatement, CsrConnectorAvro, CsrConnectorProto, CsrSeed, CsrSeedCompiled,
    CsrSeedCompiledEncoding, CsrSeedCompiledOrLegacy, CsvColumns, DataType, Format, Function,
    Ident, ProtobufSchema, Raw, RawName, SqlOption, Statement, TableFactor, TableFunction,
    UnresolvedObjectName, Value, ViewDefinition, WithOption, WithOptionValue,
};
use sql::plan::resolve_names_stmt;
use uuid::Uuid;

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
            persist_name,
            persist_details,
        } = serde_json::from_slice(&def)?;
        let mut stmt = sql::parse::parse(&create_sql)?.into_element();

        f(&mut stmt)?;

        let serialized_item = SerializedCatalogItem::V1 {
            create_sql: stmt.to_ast_string_stable(),
            eval_env,
            persist_name,
            persist_details,
        };

        let serialized_item =
            serde_json::to_vec(&serialized_item).expect("catalog serialization cannot fail");
        tx.update_item(id, &name.item, &serialized_item)?;
    }
    Ok(())
}

lazy_static! {
    static ref VER_0_9_1: Version = Version::new(0, 9, 1);
    static ref VER_0_9_2: Version = Version::new(0, 9, 2);
    static ref VER_0_9_13: Version = Version::new(0, 9, 13);
}

pub(crate) fn migrate(catalog: &mut Catalog) -> Result<(), anyhow::Error> {
    let mut storage = catalog.storage();
    let catalog_version = storage.get_catalog_content_version()?;
    let catalog_version = match Version::parse(&catalog_version) {
        Ok(v) => v,
        // Catalog content versions changed to semver after 0.8.3, so all
        // non-semver versions are less than that.
        Err(_) => Version::new(0, 0, 0),
    };

    let mut tx = storage.transaction()?;
    // First, do basic AST -> AST transformations.
    rewrite_items(&tx, |stmt| {
        ast_rewrite_type_references_0_6_1(stmt)?;
        ast_use_pg_catalog_0_7_1(stmt)?;
        ast_insert_default_confluent_wire_format_0_7_1(stmt)?;
        if catalog_version < *VER_0_9_1 {
            ast_rewrite_pg_catalog_char_to_text_0_9_1(stmt)?;
        }
        if catalog_version < *VER_0_9_2 {
            ast_rewrite_csv_column_aliases_0_9_2(stmt)?;
        }
        if catalog_version < *VER_0_9_13 {
            ast_rewrite_kafka_protobuf_source_text_to_compiled_0_9_13(stmt)?;
        }
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
// - The sum of all migrations must be idempotent because all migrations run
//   every time the catalog opens, unless migrations are explicitly disabled.
//   This might mean changing code outside the migration itself, or only
//   executing some migrations when encountering certain versions.
// - Migrations must preserve backwards compatibility with all past releases of
//   materialized.
//
// Please include @benesch on any code reviews that add or edit migrations.

// ****************************************************************************
// AST migrations -- Basic AST -> AST transformations
// ****************************************************************************

/// Rewrites Protobuf sources to store the compiled bytes rather than the text
/// of the schema.
fn ast_rewrite_kafka_protobuf_source_text_to_compiled_0_9_13(
    stmt: &mut sql::ast::Statement<Raw>,
) -> Result<(), anyhow::Error> {
    fn compile_proto(schema: &str) -> Result<CsrSeedCompiledEncoding, anyhow::Error> {
        let temp_schema_name: String = Uuid::new_v4().to_simple().to_string();
        let include_dir = tempfile::tempdir()?;
        let schema_path = include_dir.path().join(&temp_schema_name);
        let schema_bytes = strconv::parse_bytes(schema)?;
        fs::write(&schema_path, &schema_bytes)?;

        match Protoc::new()
            .include(include_dir.path())
            .input(schema_path)
            .parse()
        {
            Ok(fds) => {
                let message_name = fds
                    .file
                    .iter()
                    .find(|f| f.get_name() == temp_schema_name)
                    .map(|file| file.message_type.first())
                    .flatten()
                    .map(|message| format!(".{}", message.get_name()))
                    .ok_or_else(|| anyhow!("unable to compile temporary schema"))?;
                let mut schema = String::new();
                strconv::format_bytes(&mut schema, &fds.write_to_bytes()?);
                Ok(CsrSeedCompiledEncoding {
                    schema,
                    message_name,
                })
            }
            Err(e) => {
                lazy_static! {
                    static ref MISSING_IMPORT_ERROR: Regex = Regex::new(
                        r#"protobuf path \\"(?P<reference>.*)\\" is not found in import path"#
                    )
                    .unwrap();
                }

                // Make protobuf import errors more user-friendly.
                if let Some(captures) = MISSING_IMPORT_ERROR.captures(&e.to_string()) {
                    bail!(
                        "unsupported protobuf schema reference {}",
                        &captures["reference"]
                    )
                } else {
                    Err(e)
                }
            }
        }
    }

    fn do_upgrade(seed: &mut CsrSeedCompiledOrLegacy) -> Result<(), anyhow::Error> {
        match seed {
            CsrSeedCompiledOrLegacy::Legacy(CsrSeed {
                key_schema,
                value_schema,
            }) => {
                let key = match key_schema {
                    Some(k) => Some(compile_proto(k)?),
                    None => None,
                };
                *seed = CsrSeedCompiledOrLegacy::Compiled(CsrSeedCompiled {
                    value: compile_proto(value_schema)?,
                    key,
                });
            }
            CsrSeedCompiledOrLegacy::Compiled(_) => (),
        }
        Ok(())
    }

    if let Statement::CreateSource(CreateSourceStatement { format, .. }) = stmt {
        match format {
            CreateSourceFormat::Bare(value) => {
                if let Format::Protobuf(ProtobufSchema::Csr {
                    csr_connector: CsrConnectorProto { seed: Some(s), .. },
                }) = value
                {
                    do_upgrade(s)?;
                }
            }
            CreateSourceFormat::KeyValue { key, value } => {
                if let Format::Protobuf(ProtobufSchema::Csr {
                    csr_connector: CsrConnectorProto { seed: Some(s), .. },
                }) = key
                {
                    do_upgrade(s)?;
                }
                if let Format::Protobuf(ProtobufSchema::Csr {
                    csr_connector: CsrConnectorProto { seed: Some(s), .. },
                }) = value
                {
                    do_upgrade(s)?;
                }
            }
            CreateSourceFormat::None => {}
        }
    }
    Ok(())
}

/// Rewrites all references of `pg_catalog.char` to `pg_catalog.text`, which
/// matches the previous char implementation's semantics.
///
/// Note that the previous `char` "implementation" was simply an alias to
/// `text`. However, the new `char` semantics mirrors Postgres' `bpchar` type,
/// which `char` is now essentially an alias of.
///
/// This approach is safe because all previous references to `char` were
/// actually `text` references. All `char` references going forward will
/// properly behave as `bpchar` references.
fn ast_rewrite_pg_catalog_char_to_text_0_9_1(
    stmt: &mut sql::ast::Statement<Raw>,
) -> Result<(), anyhow::Error> {
    struct TypeNormalizer;

    lazy_static! {
        static ref CHAR_REFERENCE: UnresolvedObjectName =
            UnresolvedObjectName(vec![Ident::new(PG_CATALOG_SCHEMA), Ident::new("char")]);
        static ref TEXT_REFERENCE: UnresolvedObjectName =
            UnresolvedObjectName(vec![Ident::new(PG_CATALOG_SCHEMA), Ident::new("text")]);
    }

    impl<'ast> VisitMut<'ast, Raw> for TypeNormalizer {
        fn visit_data_type_mut(&mut self, data_type: &'ast mut DataType<Raw>) {
            if let DataType::Other { name, typ_mod } = data_type {
                if name.name() == &*CHAR_REFERENCE {
                    let t = TEXT_REFERENCE.clone();
                    *name = match name {
                        RawName::Name(_) => RawName::Name(t),
                        RawName::Id(id, _) => RawName::Id(id.clone(), t),
                    };
                    *typ_mod = vec![];
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

// Insert default value for confluent_wire_format.
//
// This PR introduced a new `WITH` options block attached to the inline schema
// clause. Previously-created versions of this object must have no options
// specified, so we explicitly set them to their existing behavior, which is
// now described by `confluent_wire_format = true`.
//
// This gives us flexibility to change the default to `false` in the future,
// if desired.
fn ast_insert_default_confluent_wire_format_0_7_1(
    stmt: &mut sql::ast::Statement<Raw>,
) -> Result<(), anyhow::Error> {
    match stmt {
        Statement::CreateSource(CreateSourceStatement {
            format:
                CreateSourceFormat::Bare(Format::Avro(AvroSchema::InlineSchema {
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
        Statement::CreateSource(CreateSourceStatement {
            format:
                CreateSourceFormat::Bare(Format::Avro(AvroSchema::Csr {
                    csr_connector:
                        CsrConnectorAvro {
                            ref mut with_options,
                            ..
                        },
                })),
            ..
        }) => {
            if with_options.is_empty() {
                with_options.push(SqlOption::Value {
                    name: Ident::new("confluent_wire_format"),
                    value: Value::Boolean(true),
                })
            }
        }
        _ => {}
    }
    Ok(())
}

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
            if let TableFactor::Function {
                function: TableFunction { ref mut name, .. },
                ..
            } = table_factor
            {
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

/// Rewrite CSV sources to use the explicit `FORMAT CSV WITH HEADER (name, ...)` syntax
///
/// This provides us an explicit check that we are reading the correct columns, and also allows us
/// to in the future correctly loosen the semantics of our column aliases syntax to not exactly
/// match the number of columns in a source.
fn ast_rewrite_csv_column_aliases_0_9_2(
    stmt: &mut sql::ast::Statement<Raw>,
) -> Result<(), anyhow::Error> {
    let (connector, col_names, columns, delimiter) =
        if let Statement::CreateSource(CreateSourceStatement {
            connector,
            col_names,
            format: CreateSourceFormat::Bare(Format::Csv { columns, delimiter }),
            ..
        }) = stmt
        {
            // only do anything if we have empty header names for a csv source
            if !matches!(columns, CsvColumns::Header { .. }) {
                return Ok(());
            }
            if let CsvColumns::Header { names } = columns {
                if !names.is_empty() {
                    return Ok(());
                }
            }

            (connector, col_names, columns, delimiter)
        } else {
            return Ok(());
        };

    // Try to load actual columns from existing file if we don't have correct data
    let result = (|| -> anyhow::Result<()> {
        if let CreateSourceConnector::File { path, .. } = &connector {
            let file = block_on(async {
                let f = File::open(&path).await?;

                if f.metadata().await?.is_dir() {
                    bail!("expected a regular file, but {} is a directory.", path);
                }
                Ok(Some(f))
            })?;

            block_on(async { sql::pure::purify_csv(file, &connector, *delimiter, columns).await })?;
        }
        Ok(())
    })();

    // if we can't read from the file, or purification fails for some other reason, then we can
    // at least use the names that may have been auto-populated from the file previously. If
    // they match then everything will work out. If they don't match, then at least there isn't
    // a catalog corruption error.
    if let Err(e) = result {
        log::warn!(
            "Error retrieving column names from file ({}) \
                 using previously defined column aliases",
            e
        );
        if let CsvColumns::Header { names } = columns {
            names.extend_from_slice(col_names);
        }
    }

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

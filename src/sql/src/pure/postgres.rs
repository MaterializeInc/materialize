// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Postgres utilities for SQL purification.

use std::collections::{BTreeMap, BTreeSet};

use itertools::Itertools;
use mz_postgres_util::desc::PostgresTableDesc;
use mz_postgres_util::Config;
use mz_repr::adt::system::Oid;
use mz_repr::GlobalId;
use mz_sql_parser::ast::display::AstDisplay;
use mz_sql_parser::ast::{
    ColumnDef, CreateSubsourceOption, CreateSubsourceOptionName, CreateSubsourceStatement,
    DeferredItemName, Ident, Value, WithOptionValue,
};
use mz_sql_parser::ast::{CreateSourceSubsource, UnresolvedItemName};

use crate::catalog::ErsatzCatalog;
use crate::names::Aug;
use crate::normalize;
use crate::plan::{PlanError, StatementContext};

pub(super) fn derive_catalog_from_publication_tables<'a>(
    database: &'a str,
    publication_tables: &'a [PostgresTableDesc],
) -> Result<ErsatzCatalog<'a, PostgresTableDesc>, PlanError> {
    // An index from table name -> schema name -> database name -> PostgresTableDesc
    let mut tables_by_name = BTreeMap::new();
    for table in publication_tables.iter() {
        tables_by_name
            .entry(table.name.clone())
            .or_insert_with(BTreeMap::new)
            .entry(table.namespace.clone())
            .or_insert_with(BTreeMap::new)
            .entry(database.to_string())
            .or_insert(table);
    }

    Ok(ErsatzCatalog(tables_by_name))
}

pub(super) async fn validate_requested_subsources(
    config: &Config,
    requested_subsources: &[(UnresolvedItemName, UnresolvedItemName, &PostgresTableDesc)],
) -> Result<(), PlanError> {
    // This condition would get caught during the catalog transaction, but produces a
    // vague, non-contextual error. Instead, error here so we can suggest to the user
    // how to fix the problem.
    if let Some(name) = requested_subsources
        .iter()
        .map(|(_, subsource_name, _)| subsource_name)
        .duplicates()
        .next()
        .cloned()
    {
        let mut upstream_references: Vec<_> = requested_subsources
            .into_iter()
            .filter_map(|(u, t, _)| if t == &name { Some(u.clone()) } else { None })
            .collect();

        upstream_references.sort();

        return Err(PlanError::SubsourceNameConflict {
            name,
            upstream_references,
        });
    }

    // We technically could allow multiple subsources to ingest the same upstream table, but
    // it is almost certainly an error on the user's end.
    if let Some(name) = requested_subsources
        .iter()
        .map(|(referenced_name, _, _)| referenced_name)
        .duplicates()
        .next()
        .cloned()
    {
        let mut target_names: Vec<_> = requested_subsources
            .into_iter()
            .filter_map(|(u, t, _)| if u == &name { Some(t.clone()) } else { None })
            .collect();

        target_names.sort();

        return Err(PlanError::SubsourceDuplicateReference { name, target_names });
    }

    // Ensure that we have select permissions on all tables; we have to do this before we
    // start snapshotting because if we discover we cannot `COPY` from a table while
    // snapshotting, we break the entire source.
    let tables_to_check_permissions = requested_subsources
        .iter()
        .map(|(UnresolvedItemName(inner), _, _)| [inner[1].as_str(), inner[2].as_str()])
        .collect();

    mz_postgres_util::check_table_privileges(config, tables_to_check_permissions).await?;

    Ok(())
}

pub(super) fn generate_text_columns(
    publication_catalog: &ErsatzCatalog<'_, PostgresTableDesc>,
    text_columns: &mut [UnresolvedItemName],
    option_name: &str,
) -> Result<BTreeMap<u32, BTreeSet<String>>, PlanError> {
    let mut text_cols_dict: BTreeMap<u32, BTreeSet<String>> = BTreeMap::new();

    for name in text_columns {
        let (qual, col) = match name.0.split_last().expect("must have at least one element") {
            (col, qual) if qual.is_empty() => {
                return Err(PlanError::InvalidOptionValue {
                    option_name: option_name.to_string(),
                    err: Box::new(PlanError::UnderqualifiedColumnName(
                        col.as_str().to_string(),
                    )),
                });
            }
            (col, qual) => (qual.to_vec(), col.as_str().to_string()),
        };

        let qual_name = UnresolvedItemName(qual);

        let (mut fully_qualified_name, desc) =
            publication_catalog
                .resolve(qual_name)
                .map_err(|e| PlanError::InvalidOptionValue {
                    option_name: option_name.to_string(),
                    err: Box::new(e),
                })?;

        if !desc.columns.iter().any(|column| column.name == col) {
            return Err(PlanError::InvalidOptionValue {
                option_name: option_name.to_string(),
                err: Box::new(PlanError::UnknownColumn {
                    table: Some(
                        normalize::unresolved_item_name(fully_qualified_name)
                            .expect("known to be of valid len"),
                    ),
                    column: mz_repr::ColumnName::from(col),
                }),
            });
        }

        // Rewrite fully qualified name.
        fully_qualified_name.0.push(col.as_str().to_string().into());
        *name = fully_qualified_name;

        let new = text_cols_dict
            .entry(desc.oid)
            .or_default()
            .insert(col.as_str().to_string());

        if !new {
            return Err(PlanError::InvalidOptionValue {
                option_name: option_name.to_string(),
                err: Box::new(PlanError::UnexpectedDuplicateReference { name: name.clone() }),
            });
        }
    }

    Ok(text_cols_dict)
}

pub(crate) fn generate_targeted_subsources<F>(
    scx: &StatementContext,
    validated_requested_subsources: Vec<(
        UnresolvedItemName,
        UnresolvedItemName,
        &PostgresTableDesc,
    )>,
    text_cols_dict: BTreeMap<u32, BTreeSet<String>>,
    mut get_transient_subsource_id: F,
) -> Result<
    (
        Vec<CreateSourceSubsource<Aug>>,
        Vec<(GlobalId, CreateSubsourceStatement<Aug>)>,
    ),
    PlanError,
>
where
    F: FnMut() -> u64,
{
    let mut targeted_subsources = vec![];
    let mut subsources = vec![];

    // Aggregate all unrecognized types.
    let mut unsupported_cols = vec![];

    // Now that we have an explicit list of validated requested subsources we can create them
    for (upstream_name, subsource_name, table) in validated_requested_subsources.into_iter() {
        // Figure out the schema of the subsource
        let mut columns = vec![];
        for c in table.columns.iter() {
            let name = Ident::new(c.name.clone());
            let ty = match text_cols_dict.get(&table.oid) {
                Some(names) if names.contains(&c.name) => mz_pgrepr::Type::Text,
                _ => match mz_pgrepr::Type::from_oid_and_typmod(c.type_oid, c.type_mod) {
                    Ok(t) => t,
                    Err(_) => {
                        let mut full_name = upstream_name.0.clone();
                        full_name.push(name);
                        unsupported_cols.push((
                            UnresolvedItemName(full_name).to_ast_string(),
                            Oid(c.type_oid),
                        ));
                        continue;
                    }
                },
            };

            let data_type = scx.resolve_type(ty)?;
            let mut options = vec![];

            if !c.nullable {
                options.push(mz_sql_parser::ast::ColumnOptionDef {
                    name: None,
                    option: mz_sql_parser::ast::ColumnOption::NotNull,
                });
            }

            columns.push(ColumnDef {
                name,
                data_type,
                collation: None,
                options,
            });
        }

        let mut constraints = vec![];
        for key in table.keys.clone() {
            let mut key_columns = vec![];

            for col_num in key.cols {
                key_columns.push(Ident::new(
                    table
                        .columns
                        .iter()
                        .find(|col| col.col_num == Some(col_num))
                        .expect("key exists as column")
                        .name
                        .clone(),
                ))
            }

            let constraint = mz_sql_parser::ast::TableConstraint::Unique {
                name: Some(Ident::new(key.name)),
                columns: key_columns,
                is_primary: key.is_primary,
                nulls_not_distinct: key.nulls_not_distinct,
            };

            // We take the first constraint available to be the primary key.
            if key.is_primary {
                constraints.insert(0, constraint);
            } else {
                constraints.push(constraint);
            }
        }

        // Create the targeted AST node for the original CREATE SOURCE statement
        let transient_id = GlobalId::Transient(get_transient_subsource_id());

        let subsource = scx.allocate_resolved_item_name(transient_id, subsource_name.clone())?;

        targeted_subsources.push(CreateSourceSubsource {
            reference: upstream_name,
            subsource: Some(DeferredItemName::Named(subsource)),
        });

        // Create the subsource statement
        let subsource = CreateSubsourceStatement {
            name: subsource_name,
            columns,
            // TODO(petrosagg): nothing stops us from getting the constraints of the
            // upstream tables and mirroring them here which will lead to more optimization
            // opportunities if for example there is a primary key or an index.
            //
            // If we ever do that we must triple check that we will get notified *in the
            // replication stream*, if our assumptions change. Failure to do that could
            // mean that an upstream table that started with an index was then altered to
            // one without and now we're producing garbage data.
            constraints,
            if_not_exists: false,
            with_options: vec![CreateSubsourceOption {
                name: CreateSubsourceOptionName::References,
                value: Some(WithOptionValue::Value(Value::Boolean(true))),
            }],
        };
        subsources.push((transient_id, subsource));
    }

    if !unsupported_cols.is_empty() {
        return Err(PlanError::UnrecognizedTypeInPostgresSource {
            cols: unsupported_cols,
        });
    }

    targeted_subsources.sort();

    Ok((targeted_subsources, subsources))
}

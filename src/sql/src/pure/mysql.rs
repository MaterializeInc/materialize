// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! MySQL utilities for SQL purification.

use std::collections::BTreeMap;

use mz_mysql_util::MySqlTableDesc;
use mz_repr::GlobalId;
use mz_sql_parser::ast::{
    ColumnDef, CreateSubsourceOption, CreateSubsourceOptionName, CreateSubsourceStatement,
    DeferredItemName, Ident, IdentError, Value, WithOptionValue,
};
use mz_sql_parser::ast::{CreateSourceSubsource, UnresolvedItemName};

use crate::catalog::SubsourceCatalog;
use crate::names::Aug;
use crate::plan::{PlanError, StatementContext};

use super::RequestedSubsource;

/// The name of the fake database that we use for MySQL sources
/// to fit our model of a 3-layer catalog. MySQL doesn't have a concept
/// of databases AND schemas, it treats both as the same thing.
static MYSQL_DATABASE_FAKE_NAME: &str = "mysql";

pub(super) fn mysql_upstream_name(
    table: &MySqlTableDesc,
) -> Result<UnresolvedItemName, IdentError> {
    Ok(UnresolvedItemName::qualified(&[
        Ident::new(MYSQL_DATABASE_FAKE_NAME)?,
        Ident::new(&table.schema_name)?,
        Ident::new(&table.name)?,
    ]))
}

pub(super) fn derive_catalog_from_tables<'a>(
    tables: &'a [MySqlTableDesc],
) -> Result<SubsourceCatalog<&'a MySqlTableDesc>, PlanError> {
    // An index from table name -> schema name -> MySqlTableDesc
    let mut tables_by_name = BTreeMap::new();
    for table in tables.iter() {
        tables_by_name
            .entry(table.name.clone())
            .or_insert_with(BTreeMap::new)
            .entry(table.schema_name.clone())
            .or_insert_with(BTreeMap::new)
            .entry(MYSQL_DATABASE_FAKE_NAME.to_string())
            .or_insert(table);
    }

    Ok(SubsourceCatalog(tables_by_name))
}

pub(super) fn generate_targeted_subsources<F>(
    scx: &StatementContext,
    validated_requested_subsources: Vec<RequestedSubsource<MySqlTableDesc>>,
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

    // Now that we have an explicit list of validated requested subsources we can create them
    for RequestedSubsource {
        upstream_name,
        subsource_name,
        table,
    } in validated_requested_subsources.into_iter()
    {
        // Figure out the schema of the subsource
        let mut columns = vec![];
        for c in table.columns.iter() {
            let name = Ident::new(&c.name)?;

            let ty = mz_pgrepr::Type::from(&c.column_type.scalar_type);
            let data_type = scx.resolve_type(ty)?;
            let mut col_options = vec![];

            if !c.column_type.nullable {
                col_options.push(mz_sql_parser::ast::ColumnOptionDef {
                    name: None,
                    option: mz_sql_parser::ast::ColumnOption::NotNull,
                });
            }
            columns.push(ColumnDef {
                name,
                data_type,
                collation: None,
                options: col_options,
            });
        }

        let mut constraints = vec![];
        for key in table.keys.iter() {
            let columns: Result<Vec<Ident>, _> = key.columns.iter().map(Ident::new).collect();

            let constraint = mz_sql_parser::ast::TableConstraint::Unique {
                name: Some(Ident::new(&key.name)?),
                columns: columns?,
                is_primary: key.is_primary,
                // MySQL always permits multiple nulls values in unique indexes.
                nulls_not_distinct: false,
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
            constraints,
            if_not_exists: false,
            with_options: vec![CreateSubsourceOption {
                name: CreateSubsourceOptionName::References,
                value: Some(WithOptionValue::Value(Value::Boolean(true))),
            }],
        };
        subsources.push((transient_id, subsource));
    }

    targeted_subsources.sort();

    Ok((targeted_subsources, subsources))
}

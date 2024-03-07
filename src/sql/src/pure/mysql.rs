// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! MySQL utilities for SQL purification.

use std::collections::{BTreeMap, BTreeSet};

use mz_mysql_util::{validate_source_privileges, MySqlError, MySqlTableDesc, QualifiedTableRef};
use mz_sql_parser::ast::display::AstDisplay;
use mz_sql_parser::ast::UnresolvedItemName;
use mz_sql_parser::ast::{
    ColumnDef, CreateSubsourceOption, CreateSubsourceOptionName, CreateSubsourceStatement, Ident,
    IdentError, Value, WithOptionValue,
};

use crate::catalog::SubsourceCatalog;
use crate::names::Aug;
use crate::plan::{PlanError, StatementContext};
use crate::pure::{MySqlConfigOptionName, MySqlSourcePurificationError};

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

/// Reverses the `mysql_upstream_name` function.
fn upstream_name_to_table(
    name: &UnresolvedItemName,
) -> Result<QualifiedTableRef, MySqlSourcePurificationError> {
    if name.0.len() != 3 {
        Err(MySqlSourcePurificationError::InvalidTableReference(name.to_string()).into())?
    }
    if name.0.get(0).map(|s| s.as_str()) != Some(MYSQL_DATABASE_FAKE_NAME) {
        Err(MySqlSourcePurificationError::InvalidTableReference(name.to_string()).into())?
    }
    Ok(QualifiedTableRef {
        schema_name: name.0[1].as_str(),
        table_name: name.0[2].as_str(),
    })
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

pub(super) fn generate_targeted_subsources(
    scx: &StatementContext,
    validated_requested_subsources: Vec<RequestedSubsource<MySqlTableDesc>>,
) -> Result<Vec<CreateSubsourceStatement<Aug>>, PlanError> {
    let mut subsources = vec![];

    // Now that we have an explicit list of validated requested subsources we can create them
    for RequestedSubsource {
        // TODO: move MySQL sources to new structure
        upstream_name: _,
        subsource_name,
        table,
    } in validated_requested_subsources.into_iter()
    {
        // Figure out the schema of the subsource
        let mut columns = vec![];
        for c in table.columns.iter() {
            match c.column_type {
                // This column is intentionally ignored, so we don't generate a column for it in
                // the subsource.
                None => {}
                Some(ref column_type) => {
                    let name = Ident::new(&c.name)?;

                    let ty = mz_pgrepr::Type::from(&column_type.scalar_type);
                    let data_type = scx.resolve_type(ty)?;
                    let mut col_options = vec![];

                    if !column_type.nullable {
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
            }
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

        // Create the subsource statement
        let subsource = CreateSubsourceStatement {
            name: subsource_name,
            columns,
            of_source: None,
            constraints,
            if_not_exists: false,
            with_options: vec![CreateSubsourceOption {
                name: CreateSubsourceOptionName::References,
                value: Some(WithOptionValue::Value(Value::Boolean(true))),
            }],
        };
        subsources.push(subsource);
    }

    Ok(subsources)
}

/// Map a list of column references to a map of table references to column names.
pub(super) fn map_column_refs<'a>(
    cols: &'a [UnresolvedItemName],
    option_type: MySqlConfigOptionName,
) -> Result<BTreeMap<QualifiedTableRef<'a>, BTreeSet<&'a str>>, PlanError> {
    let mut table_to_cols = BTreeMap::new();
    for name in cols.iter() {
        // We only support fully qualified references for now (e.g. `schema_name.table_name.column_name`)
        if name.0.len() == 3 {
            let key = mz_mysql_util::QualifiedTableRef {
                schema_name: name.0[0].as_str(),
                table_name: name.0[1].as_str(),
            };
            table_to_cols
                .entry(key)
                .or_insert_with(BTreeSet::new)
                .insert(name.0[2].as_str());
        } else {
            return Err(PlanError::InvalidOptionValue {
                option_name: option_type.to_ast_string(),
                err: Box::new(PlanError::UnderqualifiedColumnName(name.to_string())),
            });
        }
    }
    Ok(table_to_cols)
}

/// Normalize column references to a sorted, deduplicated options list of column names.
pub(super) fn normalize_column_refs<'a>(
    cols: Vec<UnresolvedItemName>,
    catalog: &'a SubsourceCatalog<&'a MySqlTableDesc>,
) -> Result<Vec<WithOptionValue<Aug>>, MySqlSourcePurificationError> {
    let (seq, unknown): (Vec<_>, Vec<_>) = cols.into_iter().partition(|name| {
        let (column_name, qual) = name.0.split_last().expect("non-empty");
        match catalog.resolve(UnresolvedItemName::qualified(qual)) {
            Ok((_, desc)) => desc.columns.iter().any(|n| &n.name == column_name.as_str()),
            Err(_) => false,
        }
    });

    if !unknown.is_empty() {
        return Err(MySqlSourcePurificationError::DanglingTextColumns { items: unknown });
    }

    let mut seq: Vec<_> = seq
        .into_iter()
        .map(WithOptionValue::UnresolvedItemName)
        .collect();
    seq.sort();
    seq.dedup();
    Ok(seq)
}

pub(super) async fn validate_requested_subsources_privileges(
    requested_subsources: &[RequestedSubsource<'_, MySqlTableDesc>],
    conn: &mut mz_mysql_util::MySqlConn,
) -> Result<(), PlanError> {
    // Ensure that we have correct privileges on all tables; we have to do this before we
    // start snapshotting because if we discover we cannot `SELECT` from a table while
    // snapshotting, we break the entire source.
    let tables_to_check_permissions = requested_subsources
        .iter()
        .map(|RequestedSubsource { upstream_name, .. }| upstream_name_to_table(upstream_name))
        .collect::<Result<Vec<_>, _>>()?;

    validate_source_privileges(&mut *conn, &tables_to_check_permissions)
        .await
        .map_err(|err| match err {
            MySqlError::MissingPrivileges(missing_privileges) => {
                MySqlSourcePurificationError::UserLacksPrivileges(
                    missing_privileges
                        .into_iter()
                        .map(|mp| (mp.privilege, mp.qualified_table_name))
                        .collect(),
                )
                .into()
            }
            _ => PlanError::MySqlConnectionErr { cause: err.into() },
        })?;

    Ok(())
}

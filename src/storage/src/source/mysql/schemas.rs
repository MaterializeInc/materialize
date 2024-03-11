// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::{BTreeMap, BTreeSet};

use mysql_async::prelude::Queryable;
use mz_mysql_util::{schema_info, MySqlError, MySqlTableDesc, QualifiedTableRef, SchemaRequest};
use mz_storage_types::sources::mysql::MySqlColumnRef;

use super::{DefiniteError, MySqlTableName};

/// Given a list of tables and their expected schemas, retrieve the current schema for each table
/// and verify they are compatible with the expected schema.
///
/// Returns a vec of tables that have incompatible schema changes.
pub(super) async fn verify_schemas<'a, Q>(
    conn: &mut Q,
    expected: &[(&'a MySqlTableName, &MySqlTableDesc)],
    text_columns: &Vec<MySqlColumnRef>,
    ignore_columns: &Vec<MySqlColumnRef>,
) -> Result<Vec<(&'a MySqlTableName, DefiniteError)>, MySqlError>
where
    Q: Queryable,
{
    let text_column_map = map_columns(text_columns);
    let ignore_column_map = map_columns(ignore_columns);

    // Get the current schema for each requested table from mysql
    let cur_schemas: BTreeMap<_, _> = schema_info(
        conn,
        &SchemaRequest::Tables(
            expected
                .iter()
                .map(|(f, _)| (f.0.as_str(), f.1.as_str()))
                .collect(),
        ),
        &text_column_map,
        &ignore_column_map,
    )
    .await?
    .into_iter()
    .map(|schema| {
        (
            MySqlTableName::new(&schema.schema_name, &schema.name),
            schema,
        )
    })
    .collect();

    Ok(expected
        .into_iter()
        .filter_map(|(table, desc)| {
            if let Err(err) = verify_schema(table, desc, &cur_schemas) {
                Some((*table, err))
            } else {
                None
            }
        })
        .collect())
}

/// Ensures that the specified table is still compatible with the current upstream schema
/// and that it has not been dropped.
fn verify_schema(
    table: &MySqlTableName,
    expected_desc: &MySqlTableDesc,
    upstream_info: &BTreeMap<MySqlTableName, MySqlTableDesc>,
) -> Result<(), DefiniteError> {
    let current_desc = upstream_info
        .get(table)
        .ok_or_else(|| DefiniteError::TableDropped(table.to_string()))?;

    match expected_desc.determine_compatibility(current_desc) {
        Ok(()) => Ok(()),
        Err(err) => Err(DefiniteError::IncompatibleSchema(err.to_string())),
    }
}

fn map_columns<'a>(
    columns: &'a [MySqlColumnRef],
) -> BTreeMap<QualifiedTableRef<'a>, BTreeSet<&'a str>> {
    let mut column_map = BTreeMap::new();
    for column in columns {
        column_map
            .entry(QualifiedTableRef {
                schema_name: column.schema_name.as_str(),
                table_name: column.table_name.as_str(),
            })
            .or_insert_with(BTreeSet::new)
            .insert(column.column_name.as_str());
    }
    column_map
}

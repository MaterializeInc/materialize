// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::BTreeMap;

use mysql_async::prelude::Queryable;
use mz_mysql_util::{schema_info, MySqlError, MySqlTableDesc, SchemaRequest};

use super::{DefiniteError, MySqlTableName};

/// Given a list of tables and their expected schemas, retrieve the current schema for each table
/// and verify they are compatible with the expected schema.
///
/// Returns a vec of tables that have incompatible schema changes.
pub(super) async fn verify_schemas<'a, Q>(
    conn: &mut Q,
    expected: &[(&'a MySqlTableName, &MySqlTableDesc)],
) -> Result<Vec<(&'a MySqlTableName, DefiniteError)>, MySqlError>
where
    Q: Queryable,
{
    // Get the current schema for each requested table from mysql
    let cur_schemas: BTreeMap<_, _> = schema_info(
        conn,
        &SchemaRequest::Tables(
            expected
                .iter()
                .map(|(f, _)| (f.0.as_str(), f.1.as_str()))
                .collect(),
        ),
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

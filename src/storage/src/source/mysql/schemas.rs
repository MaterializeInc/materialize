// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::BTreeMap;

use mz_mysql_util::{schema_info, MySqlConn, MySqlError, MySqlTableDesc, SchemaRequest};
use mz_sql_parser::ast::display::AstDisplay;
use mz_sql_parser::ast::UnresolvedItemName;

use super::{table_name, DefiniteError};

/// Given a list of tables and their expected schemas, retrieve the current schema for each table
/// and verify they are compatible with the expected schema.
///
/// Returns a vec of tables that have incompatible schema changes.
pub(super) async fn verify_schemas<'a>(
    conn: &mut MySqlConn,
    expected: &[(&'a UnresolvedItemName, &MySqlTableDesc)],
) -> Result<Vec<(&'a UnresolvedItemName, DefiniteError)>, MySqlError> {
    // Get the current schema for each requested table from mysql
    let cur_schemas: BTreeMap<_, _> = schema_info(
        conn,
        &SchemaRequest::Tables(
            expected
                .iter()
                .map(|(f, _)| (f.0[0].as_str(), f.0[1].as_str()))
                .collect(),
        ),
    )
    .await?
    .drain(..)
    .map(|schema| {
        (
            table_name(&schema.schema_name, &schema.name).unwrap(),
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
///
/// TODO: Implement real compatibility checks and don't error on backwards-compatible
/// schema changes
fn verify_schema(
    table: &UnresolvedItemName,
    expected_desc: &MySqlTableDesc,
    upstream_info: &BTreeMap<UnresolvedItemName, MySqlTableDesc>,
) -> Result<(), DefiniteError> {
    let current_desc = upstream_info
        .get(table)
        .ok_or_else(|| DefiniteError::TableDropped(table.to_ast_string()))?;

    if current_desc != expected_desc {
        return Err(DefiniteError::IncompatibleSchema(format!(
            "expected: {:#?}, actual: {:#?}",
            expected_desc, current_desc
        )));
    }
    Ok(())
}

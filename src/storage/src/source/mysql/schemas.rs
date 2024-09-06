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
use mz_mysql_util::{schema_info, MySqlError, SchemaRequest};

use super::{DefiniteError, MySqlTableName, SourceOutputInfo};

/// Given a map of tables and the expected schemas of their outputs, retrieve the current
/// schema for each and verify they are compatible with the expected schema.
///
/// Returns a vec of outputs that have incompatible schema changes.
pub(super) async fn verify_schemas<'a, Q, I>(
    conn: &mut Q,
    expected: BTreeMap<&'a MySqlTableName, I>,
) -> Result<Vec<(&'a SourceOutputInfo, DefiniteError)>, MySqlError>
where
    Q: Queryable,
    I: IntoIterator<Item = &'a SourceOutputInfo>,
{
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
        .flat_map(|(table, outputs)| {
            // For each output for this upstream table, verify that the existing output
            // desc is compatible with the new desc.
            outputs
                .into_iter()
                .filter_map(|output| match &cur_schemas.get(table) {
                    None => Some((output, DefiniteError::TableDropped(table.to_string()))),
                    Some(schema) => {
                        let new_desc = (*schema).clone().to_desc(
                            Some(&BTreeSet::from_iter(
                                output.text_columns.iter().map(|s| s.as_str()),
                            )),
                            Some(&BTreeSet::from_iter(
                                output.exclude_columns.iter().map(|s| s.as_str()),
                            )),
                        );
                        match new_desc {
                            Ok(desc) => match output.desc.determine_compatibility(&desc) {
                                Ok(()) => None,
                                Err(err) => Some((
                                    output,
                                    DefiniteError::IncompatibleSchema(err.to_string()),
                                )),
                            },
                            Err(err) => {
                                Some((output, DefiniteError::IncompatibleSchema(err.to_string())))
                            }
                        }
                    }
                })
        })
        .collect())
}

// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use anyhow::anyhow;
use postgres_array::{Array, Dimension};

use crate::{Config, PostgresError};

async fn check_schema_privileges(config: &Config, schemas: Vec<&str>) -> Result<(), PostgresError> {
    let client = config.connect("check_schema_privileges").await?;

    let schemas_len = schemas.len();

    let schemas = Array::from_parts(
        schemas,
        vec![Dimension {
            len: i32::try_from(schemas_len).expect("fewer than i32::MAX schemas"),
            lower_bound: 0,
        }],
    );

    let invalid_schema_privileges = client
        .query(
            "
            SELECT
                s, has_schema_privilege($2::text, s, 'usage') AS p
            FROM
                (SELECT unnest($1::text[])) AS o (s);",
            &[
                &schemas,
                &config.get_user().expect("connection specifies user"),
            ],
        )
        .await?
        .into_iter()
        .filter_map(|row| {
            // Only get rows that do not have sufficient privileges.
            let privileges: bool = row.get("p");
            if !privileges {
                Some(row.get("s"))
            } else {
                None
            }
        })
        .collect::<Vec<String>>();

    if invalid_schema_privileges.is_empty() {
        Ok(())
    } else {
        Err(anyhow!(
            "user {} lacks USAGE privileges for schemas {}",
            config.get_user().expect("connection specifies user"),
            invalid_schema_privileges.join(", ")
        )
        .into())
    }
}

/// Ensure that the user specified in `config` has:
///
/// -`SELECT` privileges for the identified `tables`.
///
///  `tables`'s elements should be of the structure `[<schema name>, <table name>]`.
///
/// - `USAGE` privileges on the schemas references in `tables`.
///
/// # Panics
/// If `config` does not specify a user.
pub async fn check_table_privileges(
    config: &Config,
    tables: Vec<[&str; 2]>,
) -> Result<(), PostgresError> {
    let schemas = tables.iter().map(|t| t[0]).collect();
    check_schema_privileges(config, schemas).await?;

    let client = config.connect("check_table_privileges").await?;

    let tables_len = tables.len();

    let tables = Array::from_parts(
        tables.into_iter().map(|i| i.to_vec()).flatten().collect(),
        vec![
            Dimension {
                len: i32::try_from(tables_len).expect("fewer than i32::MAX tables"),
                lower_bound: 1,
            },
            Dimension {
                len: 2,
                lower_bound: 1,
            },
        ],
    );

    let mut invalid_table_privileges = client
        .query(
            "
            WITH
                data AS (SELECT $1::text[] AS arr)
            SELECT
                t, has_table_privilege($2::text, t, 'select') AS p
            FROM
                (
                    SELECT
                        format('%I.%I', arr[i][1], arr[i][2]) AS t
                    FROM
                        data, ROWS FROM (generate_subscripts((SELECT arr FROM data), 1)) AS i
                )
                    AS o (t);",
            &[
                &tables,
                &config.get_user().expect("connection specifies user"),
            ],
        )
        .await?
        .into_iter()
        .filter_map(|row| {
            // Only get rows that do not have sufficient privileges.
            let privileges: bool = row.get("p");
            if !privileges {
                Some(row.get("t"))
            } else {
                None
            }
        })
        .collect::<Vec<String>>();

    invalid_table_privileges.sort();

    if invalid_table_privileges.is_empty() {
        Ok(())
    } else {
        Err(anyhow!(
            "user {} lacks SELECT privileges for tables {}",
            config.get_user().expect("connection specifies user"),
            invalid_table_privileges.join(", ")
        )
        .into())
    }
}

// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use tokio_postgres::types::Oid;
use tokio_postgres::Client;

use crate::desc::{PostgresColumnDesc, PostgresKeyDesc, PostgresSchemaDesc, PostgresTableDesc};
use crate::PostgresError;

pub async fn get_schemas(client: &Client) -> Result<Vec<PostgresSchemaDesc>, PostgresError> {
    Ok(client
        .query("SELECT oid, nspname, nspowner FROM pg_namespace", &[])
        .await?
        .into_iter()
        .map(|row| {
            let oid: Oid = row.get("oid");
            let name: String = row.get("nspname");
            let owner: Oid = row.get("nspowner");
            PostgresSchemaDesc { oid, name, owner }
        })
        .collect::<Vec<_>>())
}

/// Fetches table schema information from an upstream Postgres source for tables
/// that are part of a publication, given a connection string and the
/// publication name.
///
/// # Errors
///
/// - Invalid connection string, user information, or user permissions.
/// - Upstream publication does not exist or contains invalid values.
pub async fn publication_info(
    client: &Client,
    publication: &str,
) -> Result<Vec<PostgresTableDesc>, PostgresError> {
    client
        .query(
            "SELECT oid FROM pg_publication WHERE pubname = $1",
            &[&publication],
        )
        .await
        .map_err(PostgresError::from)?
        .get(0)
        .ok_or_else(|| PostgresError::PublicationMissing(publication.to_string()))?;

    let tables = client
        .query(
            "SELECT
                c.oid, p.schemaname, p.tablename
            FROM
                pg_catalog.pg_class AS c
                JOIN pg_namespace AS n ON c.relnamespace = n.oid
                JOIN pg_publication_tables AS p ON
                        c.relname = p.tablename AND n.nspname = p.schemaname
            WHERE
                p.pubname = $1",
            &[&publication],
        )
        .await
        .map_err(PostgresError::from)?;

    let mut table_infos = vec![];
    for row in tables {
        let oid = row.get("oid");

        // The Postgres replication protocol does not support GENERATED columns
        // so we exclude them from this query. But not all Postgres-like
        // databases have the `pg_attribute.attgenerated` column, so we maintain
        // two different versions of the query.
        let pg_columns_check_generated = "
        SELECT
            a.attname AS name,
            a.atttypid AS typoid,
            a.attnum AS colnum,
            a.atttypmod AS typmod,
            a.attnotnull AS not_null,
            b.oid IS NOT NULL AS primary_key
        FROM pg_catalog.pg_attribute a
        LEFT JOIN pg_catalog.pg_constraint b
            ON a.attrelid = b.conrelid
            AND b.contype = 'p'
            AND a.attnum = ANY (b.conkey)
        WHERE a.attnum > 0::pg_catalog.int2
            AND NOT a.attisdropped
            AND a.attgenerated = ''
            AND a.attrelid = $1
        ORDER BY a.attnum";

        let pg_columns_no_check_generated = "
        SELECT
            a.attname AS name,
            a.atttypid AS typoid,
            a.attnum AS colnum,
            a.atttypmod AS typmod,
            a.attnotnull AS not_null,
            b.oid IS NOT NULL AS primary_key
        FROM pg_catalog.pg_attribute a
        LEFT JOIN pg_catalog.pg_constraint b
            ON a.attrelid = b.conrelid
            AND b.contype = 'p'
            AND a.attnum = ANY (b.conkey)
        WHERE a.attnum > 0::pg_catalog.int2
            AND NOT a.attisdropped
            AND a.attrelid = $1
        ORDER BY a.attnum";

        let columns_result = match client.query(pg_columns_check_generated, &[&oid]).await {
            Ok(result) => Ok(result),
            // Not all Postgres-like databases have the `attgenerated` column
            // so issue a second query without the check if so.
            Err(e)
                if e.to_string()
                    .contains("column a.attgenerated does not exist") =>
            {
                client.query(pg_columns_no_check_generated, &[&oid]).await
            }
            other => other,
        };

        let columns = columns_result
            .map_err(PostgresError::from)?
            .into_iter()
            .map(|row| {
                let name: String = row.get("name");
                let type_oid = row.get("typoid");
                let col_num = row
                    .get::<_, i16>("colnum")
                    .try_into()
                    .expect("non-negative values");
                let type_mod: i32 = row.get("typmod");
                let not_null: bool = row.get("not_null");
                Ok(PostgresColumnDesc {
                    name,
                    col_num,
                    type_oid,
                    type_mod,
                    nullable: !not_null,
                })
            })
            .collect::<Result<Vec<_>, PostgresError>>()?;

        // PG 15 adds UNIQUE NULLS NOT DISTINCT, which would let us use `UNIQUE` constraints over
        // nullable columns as keys; i.e. aligns a PG index's NULL handling with an arrangement's
        // keys. For more info, see https://www.postgresql.org/about/featurematrix/detail/392/
        let pg_15_plus_keys = "
        SELECT
            pg_constraint.oid,
            pg_constraint.conkey,
            pg_constraint.conname,
            pg_constraint.contype = 'p' AS is_primary,
            pg_index.indnullsnotdistinct AS nulls_not_distinct
        FROM
            pg_constraint
                JOIN
                    pg_index
                    ON pg_index.indexrelid = pg_constraint.conindid
        WHERE
            pg_constraint.conrelid = $1
                AND
            pg_constraint.contype =ANY (ARRAY['p', 'u']);";

        // As above but for versions of PG without indnullsnotdistinct.
        let pg_14_minus_keys = "
        SELECT
            pg_constraint.oid,
            pg_constraint.conkey,
            pg_constraint.conname,
            pg_constraint.contype = 'p' AS is_primary,
            false AS nulls_not_distinct
        FROM pg_constraint
        WHERE
            pg_constraint.conrelid = $1
                AND
            pg_constraint.contype =ANY (ARRAY['p', 'u']);";

        let keys = match client.query(pg_15_plus_keys, &[&oid]).await {
            Ok(keys) => keys,
            Err(e)
                // PG versions prior to 15 do not contain this column.
                if e.to_string()
                    == "db error: ERROR: column pg_index.indnullsnotdistinct does not exist" =>
            {
                client.query(pg_14_minus_keys, &[&oid]).await.map_err(PostgresError::from)?
            }
            e => e.map_err(PostgresError::from)?,
        };

        let keys = keys
            .into_iter()
            .map(|row| {
                let oid: u32 = row.get("oid");
                let cols: Vec<i16> = row.get("conkey");
                let name: String = row.get("conname");
                let is_primary: bool = row.get("is_primary");
                let nulls_not_distinct: bool = row.get("nulls_not_distinct");
                let cols = cols
                    .into_iter()
                    .map(|col| u16::try_from(col).expect("non-negative colnums"))
                    .collect();
                PostgresKeyDesc {
                    oid,
                    name,
                    cols,
                    is_primary,
                    nulls_not_distinct,
                }
            })
            .collect();

        table_infos.push(PostgresTableDesc {
            oid,
            namespace: row.get("schemaname"),
            name: row.get("tablename"),
            columns,
            keys,
        });
    }

    Ok(table_infos)
}

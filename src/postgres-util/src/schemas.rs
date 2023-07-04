// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use tokio_postgres::types::Oid;

use crate::desc::{PostgresColumnDesc, PostgresKeyDesc, PostgresSchemaDesc, PostgresTableDesc};
use crate::{Config, PostgresError};

pub async fn get_schemas(config: &Config) -> Result<Vec<PostgresSchemaDesc>, PostgresError> {
    let client = config.connect("postgres_schemas").await?;

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

/// Fetches table schema information from an upstream Postgres source for
/// tables that are part of a publication, given a connection string and the
/// publication name.
///
/// If `oid_filter` is `None`, returns all tables, otherwise returns only the
/// details for the identified oid.
///
/// # Errors
///
/// - Invalid connection string, user information, or user permissions.
/// - Upstream publication does not exist or contains invalid values.
pub async fn publication_info(
    config: &Config,
    publication: &str,
    oid_filter: Option<u32>,
) -> Result<Vec<PostgresTableDesc>, PostgresError> {
    let client = config.connect("postgres_publication_info").await?;

    client
        .query(
            "SELECT oid FROM pg_publication WHERE pubname = $1",
            &[&publication],
        )
        .await?
        .get(0)
        .ok_or_else(|| anyhow::anyhow!("publication {:?} does not exist", publication))?;

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
                p.pubname = $1
                AND ($2::oid IS NULL OR c.oid = $2::oid)",
            &[&publication, &oid_filter],
        )
        .await?;

    let mut table_infos = vec![];
    for row in tables {
        let oid = row.get("oid");

        let columns = client
            .query(
                "SELECT
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
                    ORDER BY a.attnum",
                &[&oid],
            )
            .await?
            .into_iter()
            .map(|row| {
                let name: String = row.get("name");
                let type_oid = row.get("typoid");
                let col_num = Some(
                    row.get::<_, i16>("colnum")
                        .try_into()
                        .expect("non-negative values"),
                );
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
                client.query(pg_14_minus_keys, &[&oid]).await?
            }
            e => e?,
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

// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Utilities to fetch schema information for Postgres sources.

use std::collections::{BTreeMap, BTreeSet};

use anyhow::anyhow;
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
/// The `oids` parameter controls for which tables to fetch schema information. If `None`,
/// schema information for all tables in the publication is fetched. If `Some`, only
/// schema information for the tables with the specified OIDs is fetched.
///
/// # Errors
///
/// - Invalid connection string, user information, or user permissions.
/// - Upstream publication does not exist or contains invalid values.
pub async fn publication_info(
    client: &Client,
    publication: &str,
    oids: Option<&[u32]>,
) -> Result<Vec<PostgresTableDesc>, PostgresError> {
    let server_version_num = client
        .query_one("SHOW server_version_num", &[])
        .await?
        .get::<_, &str>("server_version_num")
        .parse::<i32>()
        .map_err(|e| PostgresError::Generic(anyhow!("unable to parse server_version_num: {e}")))?;

    client
        .query(
            "SELECT oid FROM pg_publication WHERE pubname = $1",
            &[&publication],
        )
        .await
        .map_err(PostgresError::from)?
        .get(0)
        .ok_or_else(|| PostgresError::PublicationMissing(publication.to_string()))?;

    let tables = if let Some(oids) = oids {
        client
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
                    AND c.oid = ANY($2)",
                &[&publication, &oids],
            )
            .await
    } else {
        client
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
    }?;

    // The Postgres replication protocol does not support GENERATED columns
    // so we exclude them from this query. But not all Postgres-like
    // databases have the `pg_attribute.attgenerated` column.
    let attgenerated = if server_version_num >= 120000 {
        "a.attgenerated = ''"
    } else {
        "true"
    };

    let pg_columns = format!(
        "
        SELECT
            a.attrelid AS table_oid,
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
            AND {attgenerated}
            AND a.attrelid =ANY($1)
        ORDER BY a.attnum"
    );

    let table_oids = tables
        .iter()
        .map(|row| row.get("oid"))
        .collect::<Vec<u32>>();

    let mut columns: BTreeMap<u32, Vec<_>> = BTreeMap::new();
    for row in client.query(&pg_columns, &[&table_oids]).await? {
        let table_oid: u32 = row.get("table_oid");
        let name: String = row.get("name");
        let type_oid = row.get("typoid");
        let col_num = row
            .get::<_, i16>("colnum")
            .try_into()
            .expect("non-negative values");
        let type_mod: i32 = row.get("typmod");
        let not_null: bool = row.get("not_null");
        let desc = PostgresColumnDesc {
            name,
            col_num,
            type_oid,
            type_mod,
            nullable: !not_null,
        };
        columns.entry(table_oid).or_default().push(desc);
    }

    // PG 15 adds UNIQUE NULLS NOT DISTINCT, which would let us use `UNIQUE` constraints over
    // nullable columns as keys; i.e. aligns a PG index's NULL handling with an arrangement's
    // keys. For more info, see https://www.postgresql.org/about/featurematrix/detail/392/
    let nulls_not_distinct = if server_version_num >= 150000 {
        "pg_index.indnullsnotdistinct"
    } else {
        "false"
    };
    let pg_keys = format!(
        "
        SELECT
            pg_constraint.conrelid AS table_oid,
            pg_constraint.oid,
            pg_constraint.conkey,
            pg_constraint.conname,
            pg_constraint.contype = 'p' AS is_primary,
            {nulls_not_distinct} AS nulls_not_distinct
        FROM
            pg_constraint
                JOIN
                    pg_index
                    ON pg_index.indexrelid = pg_constraint.conindid
        WHERE
            pg_constraint.conrelid =ANY($1)
                AND
            pg_constraint.contype =ANY (ARRAY['p', 'u']);"
    );

    let mut keys: BTreeMap<u32, BTreeSet<_>> = BTreeMap::new();
    for row in client.query(&pg_keys, &[&table_oids]).await? {
        let table_oid: u32 = row.get("table_oid");
        let oid: u32 = row.get("oid");
        let cols: Vec<i16> = row.get("conkey");
        let name: String = row.get("conname");
        let is_primary: bool = row.get("is_primary");
        let nulls_not_distinct: bool = row.get("nulls_not_distinct");
        let cols = cols
            .into_iter()
            .map(|col| u16::try_from(col).expect("non-negative colnums"))
            .collect();
        let desc = PostgresKeyDesc {
            oid,
            name,
            cols,
            is_primary,
            nulls_not_distinct,
        };
        keys.entry(table_oid).or_default().insert(desc);
    }

    Ok(tables
        .into_iter()
        .map(|row| {
            let oid: u32 = row.get("oid");
            let columns = columns.remove(&oid).unwrap_or_default();
            let keys = keys.remove(&oid).unwrap_or_default();
            PostgresTableDesc {
                oid,
                namespace: row.get("schemaname"),
                name: row.get("tablename"),
                columns,
                keys,
            }
        })
        .collect())
}

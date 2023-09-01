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
use tokio_postgres::types::Oid;

use crate::{Config, PostgresError};

/// Ensures that all provided OIDs are tables with `REPLICA IDENTITY FULL`.
pub async fn check_replica_identity_full(
    config: &Config,
    oids: Vec<Oid>,
) -> Result<(), PostgresError> {
    let client = config.connect("chec_replica_identity_full").await?;

    let oids_len = oids.len();

    let oids = Array::from_parts(
        oids,
        vec![Dimension {
            len: i32::try_from(oids_len).expect("fewer than i32::MAX schemas"),
            lower_bound: 0,
        }],
    );

    let mut invalid_replica_identity = client
        .query(
            "
            SELECT
                input.oid::REGCLASS::TEXT AS name
            FROM
                (SELECT unnest($1::OID[]) AS oid) AS input
                LEFT JOIN pg_class ON input.oid = pg_class.oid
            WHERE
                relreplident != 'f' OR relreplident IS NULL;",
            &[&oids],
        )
        .await?
        .into_iter()
        .map(|row| row.get("name"))
        .collect::<Vec<String>>();

    if invalid_replica_identity.is_empty() {
        Ok(())
    } else {
        invalid_replica_identity.sort();

        Err(anyhow!(
            "the following are not tables with REPLICA IDENTITY FULL: {}",
            invalid_replica_identity.join(", ")
        )
        .into())
    }
}

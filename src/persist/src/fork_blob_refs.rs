// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Reference counts that pin persist blobs against garbage collection.
//!
//! Each row records a single `(blob_key, fork_shard_id, branch_id)` tuple
//! asserting that `fork_shard_id` (a shard manifest belonging to `branch_id`)
//! references `blob_key`. Persist's GC consults [`ForkBlobRefs::exists`]
//! before deleting a blob and skips the delete if any reference remains.
//!
//! The table lives in the same Postgres / CockroachDB instance as the
//! `consensus` table; it is created idempotently the first time
//! [`ForkBlobRefs::open`] is called.

use deadpool_postgres::Object;
use deadpool_postgres::tokio_postgres::Config;
use mz_ore::url::SensitiveUrl;
use mz_postgres_client::{PostgresClient, PostgresClientConfig};
use postgres_protocol::escape::escape_identifier;
use tokio_postgres::error::SqlState;
use tokio_postgres::types::ToSql;
use tracing::{info, warn};
use uuid::Uuid;

use crate::location::ExternalError;

/// SQL schema for the reference-count table. Idempotent: safe to apply on
/// every open.
///
/// `branch_id` is stored as TEXT (a stringified [`Uuid`]) rather than the
/// native UUID column type. The CRUD wrapper takes [`Uuid`] arguments and
/// converts; this avoids dragging the `with-uuid-1` feature into every
/// crate that depends on tokio-postgres transitively, and the textual
/// representation is exactly as cheap on the wire.
const SCHEMA: &str = "
CREATE TABLE IF NOT EXISTS fork_blob_refs (
    blob_key      TEXT NOT NULL,
    fork_shard_id TEXT NOT NULL,
    branch_id     TEXT NOT NULL,
    PRIMARY KEY (blob_key, fork_shard_id)
);
CREATE INDEX IF NOT EXISTS fork_blob_refs_branch_id_idx ON fork_blob_refs (branch_id);
CREATE INDEX IF NOT EXISTS fork_blob_refs_blob_key_idx ON fork_blob_refs (blob_key);
";

// `fork_blob_refs` follows the same operational pattern as `consensus`: many
// short-lived rows turn over per branch lifecycle, so on CockroachDB we want
// the same tight GC window.
const CRDB_SCHEMA_OPTIONS: &str = "WITH (sql_stats_automatic_collection_enabled = false)";
const CRDB_CONFIGURE_ZONE: &str =
    "ALTER TABLE fork_blob_refs CONFIGURE ZONE USING gc.ttlseconds = 600";

/// A single reference row: `fork_shard_id` (owning a manifest under
/// `branch_id`) holds `blob_key` against deletion.
#[derive(Debug, Clone)]
pub struct ForkBlobRef {
    /// Full blob path being pinned.
    pub blob_key: String,
    /// Shard whose manifest references the blob.
    pub fork_shard_id: String,
    /// Branch this reference belongs to, used for bulk deletion at
    /// branch teardown.
    pub branch_id: Uuid,
}

async fn pg_batch_execute(client: &Object, query: &str) -> Result<(), tokio_postgres::Error> {
    #[allow(clippy::disallowed_methods)]
    client.batch_execute(query).await
}

async fn pg_execute_prepared(
    client: &Object,
    statement: &tokio_postgres::Statement,
    params: &[&(dyn ToSql + Sync)],
) -> Result<u64, tokio_postgres::Error> {
    #[allow(clippy::disallowed_methods)]
    client.execute(statement, params).await
}

async fn pg_query_opt_prepared(
    client: &Object,
    statement: &tokio_postgres::Statement,
    params: &[&(dyn ToSql + Sync)],
) -> Result<Option<tokio_postgres::Row>, tokio_postgres::Error> {
    #[allow(clippy::disallowed_methods)]
    client.query_opt(statement, params).await
}

/// Backing store for the `fork_blob_refs` table. Trait so tests can mock
/// the durable store with an in-memory map without dragging in a real
/// Postgres connection.
#[async_trait::async_trait]
pub trait ForkBlobRefsStore: std::fmt::Debug + Send + Sync {
    async fn bulk_insert(&self, rows: &[ForkBlobRef]) -> Result<(), ExternalError>;
    async fn delete_by_branch(&self, branch_id: Uuid) -> Result<u64, ExternalError>;
    async fn exists(&self, blob_key: &str) -> Result<bool, ExternalError>;
}

#[async_trait::async_trait]
impl ForkBlobRefsStore for ForkBlobRefs {
    async fn bulk_insert(&self, rows: &[ForkBlobRef]) -> Result<(), ExternalError> {
        ForkBlobRefs::bulk_insert(self, rows).await
    }
    async fn delete_by_branch(&self, branch_id: Uuid) -> Result<u64, ExternalError> {
        ForkBlobRefs::delete_by_branch(self, branch_id).await
    }
    async fn exists(&self, blob_key: &str) -> Result<bool, ExternalError> {
        ForkBlobRefs::exists(self, blob_key).await
    }
}

/// An in-memory [`ForkBlobRefsStore`] for tests. Mirrors the same row
/// semantics as the Postgres-backed [`ForkBlobRefs`]: `(blob_key,
/// fork_shard_id)` is the primary key, duplicates are silently skipped,
/// and `delete_by_branch` removes every matching row.
#[derive(Default, Debug)]
pub struct InMemoryForkBlobRefs {
    rows: tokio::sync::Mutex<std::collections::BTreeMap<(String, String), (Uuid,)>>,
}

#[async_trait::async_trait]
impl ForkBlobRefsStore for InMemoryForkBlobRefs {
    async fn bulk_insert(&self, rows: &[ForkBlobRef]) -> Result<(), ExternalError> {
        let mut store = self.rows.lock().await;
        for row in rows {
            store
                .entry((row.blob_key.clone(), row.fork_shard_id.clone()))
                .or_insert((row.branch_id,));
        }
        Ok(())
    }
    async fn delete_by_branch(&self, branch_id: Uuid) -> Result<u64, ExternalError> {
        let mut store = self.rows.lock().await;
        let before = store.len();
        store.retain(|_, (bid,)| *bid != branch_id);
        Ok(u64::try_from(before - store.len()).unwrap_or(0))
    }
    async fn exists(&self, blob_key: &str) -> Result<bool, ExternalError> {
        let store = self.rows.lock().await;
        Ok(store.keys().any(|(k, _)| k == blob_key))
    }
}

/// Reference-count table backed by Postgres or CockroachDB.
pub struct ForkBlobRefs {
    postgres_client: PostgresClient,
}

impl std::fmt::Debug for ForkBlobRefs {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ForkBlobRefs").finish_non_exhaustive()
    }
}

impl ForkBlobRefs {
    /// Open a [`ForkBlobRefs`] against the given Postgres / CockroachDB
    /// instance, creating the table if it does not already exist.
    pub async fn open(
        url: &SensitiveUrl,
        config: PostgresClientConfig,
    ) -> Result<Self, ExternalError> {
        let pg_config: Config = url.to_string().parse()?;
        let role = pg_config.get_user().expect("failed to get PostgreSQL user");
        let create_schema = format!(
            "CREATE SCHEMA IF NOT EXISTS consensus AUTHORIZATION {}",
            escape_identifier(role),
        );

        let postgres_client = PostgresClient::open(config)?;
        let client = postgres_client.get_connection().await?;

        // Try the CRDB-flavored migration first; fall back to plain Postgres
        // if CRDB-specific options are rejected.
        match pg_batch_execute(
            &client,
            &format!(
                "{}; {}{}; {};",
                create_schema, SCHEMA, CRDB_SCHEMA_OPTIONS, CRDB_CONFIGURE_ZONE,
            ),
        )
        .await
        {
            Ok(()) => {}
            Err(e) if e.code() == Some(&SqlState::INSUFFICIENT_PRIVILEGE) => {
                warn!(
                    "unable to ALTER TABLE fork_blob_refs, expected and OK when connecting with a read-only user"
                );
            }
            Err(e)
                if e.code() == Some(&SqlState::INVALID_PARAMETER_VALUE)
                    || e.code() == Some(&SqlState::SYNTAX_ERROR) =>
            {
                info!(
                    "unable to apply fork_blob_refs schema with CRDB params, falling back to vanilla Postgres: {:?}",
                    e
                );
                pg_batch_execute(&client, &format!("{}; {};", create_schema, SCHEMA)).await?;
            }
            Err(e) => return Err(e.into()),
        }

        drop(client);

        Ok(ForkBlobRefs { postgres_client })
    }

    /// Insert one row per `(blob_key, fork_shard_id)` pair. Duplicates that
    /// already exist in the table are silently skipped so callers can retry
    /// without losing or double-counting references.
    pub async fn bulk_insert(&self, rows: &[ForkBlobRef]) -> Result<(), ExternalError> {
        if rows.is_empty() {
            return Ok(());
        }
        // The unnest pattern lets us send all three columns as parallel arrays
        // and have the server expand them, which is much faster than building
        // up an N-row VALUES clause for large inserts.
        let blob_keys: Vec<&str> = rows.iter().map(|r| r.blob_key.as_str()).collect();
        let shard_ids: Vec<&str> = rows.iter().map(|r| r.fork_shard_id.as_str()).collect();
        let branch_ids: Vec<String> =
            rows.iter().map(|r| r.branch_id.to_string()).collect();
        let branch_id_refs: Vec<&str> = branch_ids.iter().map(String::as_str).collect();

        let q = "
            INSERT INTO fork_blob_refs (blob_key, fork_shard_id, branch_id)
            SELECT * FROM unnest($1::TEXT[], $2::TEXT[], $3::TEXT[])
            ON CONFLICT (blob_key, fork_shard_id) DO NOTHING
        ";
        let client = self.postgres_client.get_connection().await?;
        let statement = client.prepare_cached(q).await?;
        pg_execute_prepared(
            &client,
            &statement,
            &[&blob_keys, &shard_ids, &branch_id_refs],
        )
        .await?;
        Ok(())
    }

    /// Delete every row tagged with `branch_id`. Returns the number of rows
    /// removed so the caller can sanity-check the cleanup.
    pub async fn delete_by_branch(&self, branch_id: Uuid) -> Result<u64, ExternalError> {
        let q = "DELETE FROM fork_blob_refs WHERE branch_id = $1";
        let branch_id = branch_id.to_string();
        let client = self.postgres_client.get_connection().await?;
        let statement = client.prepare_cached(q).await?;
        let removed = pg_execute_prepared(&client, &statement, &[&branch_id]).await?;
        Ok(removed)
    }

    /// Whether any row in the table references `blob_key`. The GC gate uses
    /// this as a point lookup on every superseded blob; on environments with
    /// no live branches the table is empty and the query returns instantly.
    pub async fn exists(&self, blob_key: &str) -> Result<bool, ExternalError> {
        let q = "SELECT 1 FROM fork_blob_refs WHERE blob_key = $1 LIMIT 1";
        let client = self.postgres_client.get_connection().await?;
        let statement = client.prepare_cached(q).await?;
        let row = pg_query_opt_prepared(&client, &statement, &[&blob_key]).await?;
        Ok(row.is_some())
    }
}

/// Configuration for opening a [`ForkBlobRefs`] in tests. Mirrors
/// [`crate::postgres::PostgresConsensusConfig::new_for_test`] so the same
/// `MZ_PERSIST_EXTERNAL_STORAGE_TEST_POSTGRES_URL` env var activates both.
#[cfg(test)]
pub(crate) struct ForkBlobRefsTestConfig {
    pub url: SensitiveUrl,
    pub client_config: PostgresClientConfig,
}

#[cfg(test)]
impl ForkBlobRefsTestConfig {
    pub fn new_for_test() -> Result<Option<Self>, String> {
        use std::str::FromStr;
        use std::sync::Arc;
        use std::time::Duration;

        use mz_dyncfg::ConfigSet;
        use mz_ore::metrics::MetricsRegistry;
        use mz_postgres_client::PostgresClientKnobs;
        use mz_postgres_client::metrics::PostgresClientMetrics;

        let url = match std::env::var("MZ_PERSIST_EXTERNAL_STORAGE_TEST_POSTGRES_URL") {
            Ok(url) => SensitiveUrl::from_str(&url).map_err(|e| e.to_string())?,
            Err(_) => {
                if mz_ore::env::is_var_truthy("CI") {
                    panic!("CI is supposed to run this test but something has gone wrong!");
                }
                return Ok(None);
            }
        };

        struct TestKnobs;
        impl std::fmt::Debug for TestKnobs {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                f.debug_struct("TestKnobs").finish_non_exhaustive()
            }
        }
        impl PostgresClientKnobs for TestKnobs {
            fn connection_pool_max_size(&self) -> usize {
                2
            }
            fn connection_pool_max_wait(&self) -> Option<Duration> {
                Some(Duration::from_secs(1))
            }
            fn connection_pool_ttl(&self) -> Duration {
                Duration::MAX
            }
            fn connection_pool_ttl_stagger(&self) -> Duration {
                Duration::MAX
            }
            fn connect_timeout(&self) -> Duration {
                Duration::MAX
            }
            fn tcp_user_timeout(&self) -> Duration {
                Duration::ZERO
            }
            fn keepalives_idle(&self) -> Duration {
                Duration::from_secs(10)
            }
            fn keepalives_interval(&self) -> Duration {
                Duration::from_secs(5)
            }
            fn keepalives_retries(&self) -> u32 {
                5
            }
        }

        let _dyncfg = Arc::new(ConfigSet::default());
        let metrics = PostgresClientMetrics::new(&MetricsRegistry::new(), "mz_persist");
        let client_config =
            PostgresClientConfig::new(url.clone(), Arc::new(TestKnobs), metrics);
        Ok(Some(ForkBlobRefsTestConfig { url, client_config }))
    }
}

#[cfg(test)]
mod tests {
    use tracing::info;

    use super::*;

    /// Open a fresh `ForkBlobRefs` and drop the table contents so each test
    /// starts from a known state.
    async fn open_clean() -> Result<Option<ForkBlobRefs>, ExternalError> {
        let Some(config) = ForkBlobRefsTestConfig::new_for_test().map_err(|e| anyhow::anyhow!("{e}"))? else {
            info!(
                "MZ_PERSIST_EXTERNAL_STORAGE_TEST_POSTGRES_URL env not set: skipping fork_blob_refs test"
            );
            return Ok(None);
        };
        let refs = ForkBlobRefs::open(&config.url, config.client_config)
            .await
            .map_err(|e| anyhow::anyhow!("{e}"))?;
        // Clear any rows left over from prior runs.
        let client = refs.postgres_client.get_connection().await?;
        pg_batch_execute(&client, "DELETE FROM fork_blob_refs").await?;
        Ok(Some(refs))
    }

    #[mz_ore::test(tokio::test(flavor = "multi_thread"))]
    #[cfg_attr(miri, ignore)] // unsupported operation: can't call foreign function
    async fn schema_is_idempotent() -> Result<(), ExternalError> {
        let Some(config) = ForkBlobRefsTestConfig::new_for_test().map_err(|e| anyhow::anyhow!("{e}"))? else {
            return Ok(());
        };
        // Open once to apply the schema, then open again to confirm the
        // second migration is a no-op.
        let _first = ForkBlobRefs::open(&config.url, config.client_config.clone())
            .await
            .map_err(|e| anyhow::anyhow!("{e}"))?;
        let _second = ForkBlobRefs::open(&config.url, config.client_config)
            .await
            .map_err(|e| anyhow::anyhow!("{e}"))?;
        Ok(())
    }

    #[mz_ore::test(tokio::test(flavor = "multi_thread"))]
    #[cfg_attr(miri, ignore)]
    async fn bulk_insert_is_idempotent() -> Result<(), ExternalError> {
        let Some(refs) = open_clean().await? else {
            return Ok(());
        };
        let branch = Uuid::new_v4();
        let rows = vec![
            ForkBlobRef {
                blob_key: format!("blob-{}", Uuid::new_v4()),
                fork_shard_id: format!("shard-{}", Uuid::new_v4()),
                branch_id: branch,
            },
            ForkBlobRef {
                blob_key: format!("blob-{}", Uuid::new_v4()),
                fork_shard_id: format!("shard-{}", Uuid::new_v4()),
                branch_id: branch,
            },
        ];

        refs.bulk_insert(&rows).await?;
        // Re-inserting the same rows is a no-op (no error, no duplicates).
        refs.bulk_insert(&rows).await?;

        for row in &rows {
            assert!(refs.exists(&row.blob_key).await?);
        }
        let removed = refs.delete_by_branch(branch).await?;
        assert_eq!(removed, rows.len() as u64);
        Ok(())
    }

    #[mz_ore::test(tokio::test(flavor = "multi_thread"))]
    #[cfg_attr(miri, ignore)]
    async fn delete_by_branch_is_scoped() -> Result<(), ExternalError> {
        let Some(refs) = open_clean().await? else {
            return Ok(());
        };
        let branch_a = Uuid::new_v4();
        let branch_b = Uuid::new_v4();
        let key_a = format!("blob-{}", Uuid::new_v4());
        let key_b = format!("blob-{}", Uuid::new_v4());
        let shard = format!("shard-{}", Uuid::new_v4());

        refs.bulk_insert(&[
            ForkBlobRef {
                blob_key: key_a.clone(),
                fork_shard_id: shard.clone(),
                branch_id: branch_a,
            },
            ForkBlobRef {
                blob_key: key_b.clone(),
                fork_shard_id: shard.clone(),
                branch_id: branch_b,
            },
        ])
        .await?;

        let removed = refs.delete_by_branch(branch_a).await?;
        assert_eq!(removed, 1);
        assert!(!refs.exists(&key_a).await?);
        assert!(refs.exists(&key_b).await?);

        let removed = refs.delete_by_branch(branch_b).await?;
        assert_eq!(removed, 1);
        assert!(!refs.exists(&key_b).await?);
        Ok(())
    }

    #[mz_ore::test(tokio::test(flavor = "multi_thread"))]
    #[cfg_attr(miri, ignore)]
    async fn exists_handles_missing_keys() -> Result<(), ExternalError> {
        let Some(refs) = open_clean().await? else {
            return Ok(());
        };
        let missing = format!("blob-{}", Uuid::new_v4());
        assert!(!refs.exists(&missing).await?);
        Ok(())
    }
}

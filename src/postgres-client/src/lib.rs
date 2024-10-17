// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! A Postgres client that uses deadpool as a connection pool and comes with
//! common/default configuration options.

#![warn(missing_docs, missing_debug_implementations)]
#![warn(
    clippy::cast_possible_truncation,
    clippy::cast_precision_loss,
    clippy::cast_sign_loss,
    clippy::clone_on_ref_ptr
)]

pub mod error;
pub mod metrics;

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use deadpool_postgres::tokio_postgres::Config;
use deadpool_postgres::{
    Hook, HookError, HookErrorCause, Manager, ManagerConfig, Object, Pool, PoolError,
    RecyclingMethod, Runtime, Status,
};
use mz_ore::cast::{CastFrom, CastLossy};
use mz_ore::now::SYSTEM_TIME;
use mz_ore::url::SensitiveUrl;
use tracing::debug;

use crate::error::PostgresError;
use crate::metrics::PostgresClientMetrics;

/// Configuration knobs for [PostgresClient].
pub trait PostgresClientKnobs: std::fmt::Debug + Send + Sync {
    /// Maximum number of connections allowed in a pool.
    fn connection_pool_max_size(&self) -> usize;
    /// The maximum time to wait to obtain a connection, if any.
    fn connection_pool_max_wait(&self) -> Option<Duration>;
    /// Minimum TTL of a connection. It is expected that connections are
    /// routinely culled to balance load to the backing store.
    fn connection_pool_ttl(&self) -> Duration;
    /// Minimum time between TTLing connections. Helps stagger reconnections
    /// to avoid stampeding the backing store.
    fn connection_pool_ttl_stagger(&self) -> Duration;
    /// Time to wait for a connection to be made before trying.
    fn connect_timeout(&self) -> Duration;
    /// TCP user timeout for connection attempts.
    fn tcp_user_timeout(&self) -> Duration;
}

/// Configuration for creating a [PostgresClient].
#[derive(Clone, Debug)]
pub struct PostgresClientConfig {
    url: SensitiveUrl,
    knobs: Arc<dyn PostgresClientKnobs>,
    metrics: PostgresClientMetrics,
}

impl PostgresClientConfig {
    /// Returns a new [PostgresClientConfig] for use in production.
    pub fn new(
        url: SensitiveUrl,
        knobs: Arc<dyn PostgresClientKnobs>,
        metrics: PostgresClientMetrics,
    ) -> Self {
        PostgresClientConfig {
            url,
            knobs,
            metrics,
        }
    }
}

/// A Postgres client wrapper that uses deadpool as a connection pool.
pub struct PostgresClient {
    pool: Pool,
    metrics: PostgresClientMetrics,
}

impl std::fmt::Debug for PostgresClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PostgresClient").finish_non_exhaustive()
    }
}

impl PostgresClient {
    /// Open a [PostgresClient] using the given `config`.
    pub fn open(config: PostgresClientConfig) -> Result<Self, PostgresError> {
        let mut pg_config: Config = config.url.to_string_unredacted().parse()?;
        pg_config.connect_timeout(config.knobs.connect_timeout());
        pg_config.tcp_user_timeout(config.knobs.tcp_user_timeout());

        let tls = mz_tls_util::make_tls(&pg_config).map_err(|tls_err| match tls_err {
            mz_tls_util::TlsError::Generic(e) => PostgresError::Indeterminate(e),
            mz_tls_util::TlsError::OpenSsl(e) => PostgresError::Indeterminate(anyhow::anyhow!(e)),
        })?;

        let manager = Manager::from_config(
            pg_config,
            tls,
            ManagerConfig {
                recycling_method: RecyclingMethod::Fast,
            },
        );

        let last_ttl_connection = AtomicU64::new(0);
        let connections_created = config.metrics.connpool_connections_created.clone();
        let ttl_reconnections = config.metrics.connpool_ttl_reconnections.clone();
        let builder = Pool::builder(manager);
        let builder = match config.knobs.connection_pool_max_wait() {
            None => builder,
            Some(wait) => builder.wait_timeout(Some(wait)).runtime(Runtime::Tokio1),
        };
        let pool = builder
            .max_size(config.knobs.connection_pool_max_size())
            .post_create(Hook::async_fn(move |client, _| {
                connections_created.inc();
                Box::pin(async move {
                    debug!("opened new consensus postgres connection");
                    client.batch_execute(
                        "SET SESSION CHARACTERISTICS AS TRANSACTION ISOLATION LEVEL SERIALIZABLE",
                    ).await.map_err(|e| HookError::Abort(HookErrorCause::Backend(e)))
                })
            }))
            .pre_recycle(Hook::sync_fn(move |_client, conn_metrics| {
                // proactively TTL connections to rebalance load to Postgres/CRDB. this helps
                // fix skew when downstream DB operations (e.g. CRDB rolling restart) result
                // in uneven load to each node, and works to reduce the # of connections
                // maintained by the pool after bursty workloads.

                // add a bias towards TTLing older connections first
                if conn_metrics.age() < config.knobs.connection_pool_ttl() {
                    return Ok(());
                }

                let last_ttl = last_ttl_connection.load(Ordering::SeqCst);
                let now = (SYSTEM_TIME)();
                let elapsed_since_last_ttl = Duration::from_millis(now.saturating_sub(last_ttl));

                // stagger out reconnections to avoid stampeding the DB
                if elapsed_since_last_ttl > config.knobs.connection_pool_ttl_stagger()
                    && last_ttl_connection
                        .compare_exchange_weak(last_ttl, now, Ordering::SeqCst, Ordering::SeqCst)
                        .is_ok()
                {
                    ttl_reconnections.inc();
                    return Err(HookError::Continue(Some(HookErrorCause::Message(
                        "connection has been TTLed".to_string(),
                    ))));
                }

                Ok(())
            }))
            .build()
            .expect("postgres connection pool built with incorrect parameters");

        Ok(PostgresClient {
            pool,
            metrics: config.metrics,
        })
    }

    fn status_metrics(&self, status: Status) {
        self.metrics
            .connpool_available
            .set(f64::cast_lossy(status.available));
        self.metrics.connpool_size.set(u64::cast_from(status.size));
        // Don't bother reporting the maximum size of the pool... we know that from config.
    }

    /// Gets connection from the pool or waits for one to become available.
    pub async fn get_connection(&self) -> Result<Object, PoolError> {
        let start = Instant::now();
        // note that getting the pool size here requires briefly locking the pool
        self.status_metrics(self.pool.status());
        let res = self.pool.get().await;
        if let Err(PoolError::Backend(err)) = &res {
            debug!("error establishing connection: {}", err);
            self.metrics.connpool_connection_errors.inc();
        }
        self.metrics
            .connpool_acquire_seconds
            .inc_by(start.elapsed().as_secs_f64());
        self.metrics.connpool_acquires.inc();
        self.status_metrics(self.pool.status());
        res
    }
}

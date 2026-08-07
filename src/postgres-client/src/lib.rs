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

use std::collections::BTreeSet;
use std::fmt::Write;
use std::ops::{Deref, DerefMut};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use deadpool::managed::{self, Hook, HookError, Metrics, Object, Pool, RecycleResult, Timeouts};
use deadpool_postgres::tokio_postgres::{self, Config};
use deadpool_postgres::{
    ClientWrapper as DeadpoolClient, Manager as PgManager, ManagerConfig, PoolError,
    RecyclingMethod, Runtime, Status,
};
use mz_ore::cast::{CastFrom, CastLossy};
use mz_ore::metrics::Counter;
use mz_ore::now::SYSTEM_TIME;
use mz_ore::task::AbortOnDropHandle;
use mz_ore::url::SensitiveUrl;
use tracing::{debug, info};

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
    /// Time to wait for a connection to be made before retrying.
    fn connect_timeout(&self) -> Duration;
    /// TCP user timeout for connections.
    fn tcp_user_timeout(&self) -> Duration;
    /// Amount of idle time before a TCP keepalive packet is sent on a connection.
    fn keepalives_idle(&self) -> Duration;
    /// Time interval between TCP keepalive probes.
    fn keepalives_interval(&self) -> Duration;
    /// Maximum number of TCP keepalive probes that will be sent before dropping a connection.
    fn keepalives_retries(&self) -> u32;
    /// Server-side `statement_timeout` to set on each connection. A value of
    /// zero is a sentinel that means "do not set a statement timeout".
    fn statement_timeout(&self) -> Duration;
    /// Whether to proactively recycle connections whose backend node is
    /// draining. Each connection is stamped with `crdb_internal.node_id()` at
    /// creation and a background task polls `crdb_internal.gossip_liveness`
    /// for draining nodes. Connections on a draining node are discarded on
    /// their next acquire, ahead of the server's hard close at the end of its
    /// drain.
    ///
    /// Requires a CockroachDB backend whose role can read
    /// `crdb_internal.gossip_liveness`. That is not implied by ordinary
    /// database privileges: it needs `GRANT SYSTEM VIEWCLUSTERMETADATA TO
    /// <role>` (CockroachDB v23.2 and later) or membership in `admin` (older
    /// versions, which do not honor the system privilege for this table).
    /// Stamping needs no special grant.
    ///
    /// Enabling this where it cannot work is safe but pointless: the pool
    /// disables the feature for its remaining lifetime the first time either
    /// query fails permanently, so it costs one failed query rather than one
    /// per connection.
    fn drain_aware_recycling(&self) -> bool {
        false
    }
    /// Minimum time between drain culls, bounding how fast the pool sheds
    /// connections to a draining node so their replacement cost stays
    /// amortized. Should be small enough that a node's share of the pool
    /// migrates well within a drain grace window.
    fn connection_pool_drain_cull_stagger(&self) -> Duration {
        Duration::from_millis(500)
    }
}

/// The transaction isolation level applied to new connections.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum IsolationLevel {
    /// `SERIALIZABLE` — the strongest level; the historical default for consensus.
    Serializable,
    /// `READ COMMITTED` — for callers (e.g. consensus) whose queries are correct without
    /// serializable isolation (relying instead on the `PRIMARY KEY` / `FOR UPDATE` / `ON CONFLICT`).
    ReadCommitted,
}

impl IsolationLevel {
    /// The `SET SESSION CHARACTERISTICS` statement that selects this isolation level.
    fn set_characteristics_sql(self) -> &'static str {
        match self {
            IsolationLevel::Serializable => {
                "SET SESSION CHARACTERISTICS AS TRANSACTION ISOLATION LEVEL SERIALIZABLE"
            }
            IsolationLevel::ReadCommitted => {
                "SET SESSION CHARACTERISTICS AS TRANSACTION ISOLATION LEVEL READ COMMITTED"
            }
        }
    }
}

/// Resolves the isolation level to apply to a connection. It is invoked once per connection
/// creation, so a dyncfg-backed resolver lets a change take effect as the pool cycles connections.
pub type IsolationLevelFn = Arc<dyn Fn() -> IsolationLevel + Send + Sync>;

/// A connection handed out by [`PostgresClient::get_connection`]. Dereferences to a [`Client`],
/// which additionally records the [`IsolationLevel`] the connection was created under.
pub type Connection = Object<Manager>;

/// A pooled Postgres connection tagged with the [`IsolationLevel`] it was created under.
///
/// The isolation level is applied once at creation and is fixed for the life of the connection.
#[derive(Debug)]
pub struct Client {
    inner: DeadpoolClient,
    isolation: IsolationLevel,
    /// The CockroachDB node this connection landed on, if
    /// [`PostgresClientKnobs::drain_aware_recycling`] is enabled and the
    /// backend is CockroachDB. `None` otherwise.
    node_id: Option<i64>,
}

impl Client {
    /// The [`IsolationLevel`] this connection was configured with when it was created.
    pub fn isolation_level(&self) -> IsolationLevel {
        self.isolation
    }

    /// The CockroachDB node this connection landed on, if known.
    pub fn node_id(&self) -> Option<i64> {
        self.node_id
    }
}

impl Deref for Client {
    type Target = DeadpoolClient;

    fn deref(&self) -> &DeadpoolClient {
        &self.inner
    }
}

impl DerefMut for Client {
    fn deref_mut(&mut self) -> &mut DeadpoolClient {
        &mut self.inner
    }
}

/// A deadpool [`managed::Manager`] wrapping [`deadpool_postgres::Manager`]. It applies a
/// per-connection isolation level at creation and records that level on every [`Client`] it hands
/// out.
pub struct Manager {
    inner: PgManager,
    /// Resolves the isolation level to apply. Invoked once per connection so a dyncfg-backed
    /// resolver takes effect as the pool cycles connections.
    isolation: IsolationLevelFn,
    knobs: Arc<dyn PostgresClientKnobs>,
    connections_created: Counter,
    /// Whether this pool can use the `crdb_internal` introspection that
    /// drain-aware recycling needs. Starts `true` and latches to `false` the
    /// first time either the node-id stamp or the liveness poll fails
    /// permanently (see [`is_crdb_unsupported_err`]). It never latches back
    /// on, so a mistakenly enabled flag against an unsupported backend costs
    /// one failed query rather than one per connection and one per poll.
    ///
    /// NOTE: the two queries do not necessarily fail together. A role without
    /// `VIEWCLUSTERMETADATA` on a real CockroachDB can stamp connections but
    /// not read `crdb_internal.gossip_liveness`, so the poll is what latches
    /// the feature off in that case.
    drain_recycling_supported: Arc<AtomicBool>,
}

/// Whether an error means this pool will never be able to use `crdb_internal`,
/// as opposed to a transient failure worth retrying. Either the backend is not
/// CockroachDB at all (the schema, table, or function does not exist) or the
/// role lacks the privileges `gossip_liveness` requires (see
/// [`PostgresClientKnobs::drain_aware_recycling`]). Neither changes within the
/// life of a pool, so both latch the feature off instead of retrying forever.
fn is_crdb_unsupported_err(e: &tokio_postgres::Error) -> bool {
    use deadpool_postgres::tokio_postgres::error::SqlState;
    matches!(
        e.code(),
        Some(&SqlState::UNDEFINED_FUNCTION)
            | Some(&SqlState::UNDEFINED_TABLE)
            | Some(&SqlState::INVALID_SCHEMA_NAME)
            | Some(&SqlState::INSUFFICIENT_PRIVILEGE)
    )
}

impl std::fmt::Debug for Manager {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Manager")
            .field("inner", &self.inner)
            .finish_non_exhaustive()
    }
}

impl managed::Manager for Manager {
    type Type = Client;
    type Error = tokio_postgres::Error;

    async fn create(&self) -> Result<Client, tokio_postgres::Error> {
        let inner = self.inner.create().await?;
        self.connections_created.inc();

        // Resolved per connection so a dyncfg-backed isolation level takes effect as the pool
        // cycles connections. Defaults to SERIALIZABLE (see `PostgresClientConfig::new`).
        let isolation = (self.isolation)();
        let mut setup = isolation.set_characteristics_sql().to_owned();
        // A zero `statement_timeout` is our sentinel for "leave it unset". We only emit the `SET`
        // when non-zero so we don't override a timeout configured out of band.
        let statement_timeout = self.knobs.statement_timeout();
        if !statement_timeout.is_zero() {
            // A bare integer value for `statement_timeout` is interpreted as milliseconds.
            write!(
                setup,
                "; SET statement_timeout = {}",
                statement_timeout.as_millis()
            )
            .expect("writing to a String never fails");
        }
        debug!("opened new postgres connection");
        // This must surface as `tokio_postgres::Error` (the pool's error type); using
        // `mz_postgres_util` wrappers would change the error type.
        #[allow(clippy::disallowed_methods)]
        inner.batch_execute(&setup).await?;

        // Stamp the connection with the CockroachDB node it landed on, so a
        // draining node's connections can be recycled proactively.
        let node_id = if self.knobs.drain_aware_recycling()
            && self.drain_recycling_supported.load(Ordering::SeqCst)
        {
            #[allow(clippy::disallowed_methods)]
            match inner
                .query_one("SELECT crdb_internal.node_id()::INT8", &[])
                .await
            {
                Ok(row) => Some(row.get::<_, i64>(0)),
                Err(e) => {
                    if is_crdb_unsupported_err(&e) {
                        self.drain_recycling_supported
                            .store(false, Ordering::SeqCst);
                        info!("disabling drain-aware recycling, cannot stamp node ids: {e}");
                    } else {
                        debug!("unable to stamp connection with a node id: {e}");
                    }
                    None
                }
            }
        } else {
            None
        };

        Ok(Client {
            inner,
            isolation,
            node_id,
        })
    }

    async fn recycle(
        &self,
        client: &mut Client,
        metrics: &Metrics,
    ) -> RecycleResult<tokio_postgres::Error> {
        self.inner.recycle(&mut client.inner, metrics).await
    }

    fn detach(&self, client: &mut Client) {
        self.inner.detach(&mut client.inner)
    }
}

/// Configuration for creating a [PostgresClient].
#[derive(Clone)]
pub struct PostgresClientConfig {
    url: SensitiveUrl,
    knobs: Arc<dyn PostgresClientKnobs>,
    metrics: PostgresClientMetrics,
    isolation: IsolationLevelFn,
}

impl std::fmt::Debug for PostgresClientConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PostgresClientConfig")
            .field("url", &self.url)
            .finish_non_exhaustive()
    }
}

impl PostgresClientConfig {
    /// Returns a new [PostgresClientConfig] for use in production. Connections default to
    /// `SERIALIZABLE`; use [PostgresClientConfig::with_isolation] to override.
    pub fn new(
        url: SensitiveUrl,
        knobs: Arc<dyn PostgresClientKnobs>,
        metrics: PostgresClientMetrics,
    ) -> Self {
        PostgresClientConfig {
            url,
            knobs,
            metrics,
            isolation: Arc::new(|| IsolationLevel::Serializable),
        }
    }

    /// Sets the resolver that picks the isolation level applied to each new connection.
    pub fn with_isolation(mut self, isolation: IsolationLevelFn) -> Self {
        self.isolation = isolation;
        self
    }
}

/// A Postgres client wrapper that uses deadpool as a connection pool.
pub struct PostgresClient {
    pool: Pool<Manager>,
    knobs: Arc<dyn PostgresClientKnobs>,
    metrics: PostgresClientMetrics,
    /// CockroachDB nodes currently draining, maintained by the drain watchdog
    /// task and consulted by the pool's `pre_recycle` hook. Empty unless
    /// [`PostgresClientKnobs::drain_aware_recycling`] is enabled.
    draining_nodes: Arc<Mutex<BTreeSet<i64>>>,
    /// Background task that polls for draining nodes. Spawned lazily on the
    /// first acquire (opening a client does not require a Tokio runtime,
    /// acquiring does). Aborted when the client is dropped.
    drain_watchdog: std::sync::OnceLock<AbortOnDropHandle<()>>,
    /// See [`Manager::drain_recycling_supported`]. Shared with the manager so
    /// that a permanent failure from either the stamp or the poll disables
    /// both.
    drain_recycling_supported: Arc<AtomicBool>,
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

        // Configuring keepalives is important to ensure we can detect broken connections quickly.
        // TCP_USER_TIMEOUT is not sufficient as it only enforces a timeout on ACKs for transmitted
        // data, which only helps if we... transmit data.
        pg_config.keepalives(true);
        pg_config.keepalives_idle(config.knobs.keepalives_idle());
        pg_config.keepalives_interval(config.knobs.keepalives_interval());
        pg_config.keepalives_retries(config.knobs.keepalives_retries());

        let tls = mz_tls_util::make_tls(&pg_config).map_err(|tls_err| match tls_err {
            mz_tls_util::TlsError::Generic(e) => PostgresError::Indeterminate(e),
            mz_tls_util::TlsError::OpenSsl(e) => PostgresError::Indeterminate(anyhow::anyhow!(e)),
        })?;

        let pg_manager = PgManager::from_config(
            pg_config,
            tls,
            ManagerConfig {
                recycling_method: RecyclingMethod::Fast,
            },
        );
        // The isolation level and `statement_timeout` are applied inside `Manager::create` so the
        // resolved level can be recorded on each connection it hands out.
        let drain_recycling_supported = Arc::new(AtomicBool::new(true));
        let manager = Manager {
            inner: pg_manager,
            isolation: Arc::clone(&config.isolation),
            knobs: Arc::clone(&config.knobs),
            connections_created: config.metrics.connpool_connections_created.clone(),
            drain_recycling_supported: Arc::clone(&drain_recycling_supported),
        };

        let last_ttl_connection = AtomicU64::new(0);
        let last_drain_cull = AtomicU64::new(0);
        let ttl_reconnections = config.metrics.connpool_ttl_reconnections.clone();
        let knobs = Arc::clone(&config.knobs);
        let draining_nodes = Arc::new(Mutex::new(BTreeSet::new()));
        let draining_nodes_hook = Arc::clone(&draining_nodes);
        let builder = Pool::builder(manager);
        let builder = match config.knobs.connection_pool_max_wait() {
            None => builder,
            Some(wait) => builder.wait_timeout(Some(wait)).runtime(Runtime::Tokio1),
        };
        let pool = builder
            .max_size(config.knobs.connection_pool_max_size())
            .pre_recycle(Hook::sync_fn(move |client: &mut Client, conn_metrics| {
                // Discard connections whose backend node is draining, rate
                // limited to one cull per DRAIN_CULL_STAGGER so the pool
                // migrates off the node gradually instead of ejecting every
                // affected connection at once and stalling acquires behind a
                // burst of re-establishment. A rate-limited connection is
                // handed out and stays usable until a later acquire culls it
                // (or the server closes it at the end of its drain, which the
                // regular recycle check then detects).
                if let Some(node_id) = client.node_id {
                    if draining_nodes_hook
                        .lock()
                        .expect("draining_nodes lock poisoned")
                        .contains(&node_id)
                    {
                        let last_cull = last_drain_cull.load(Ordering::SeqCst);
                        let now = (SYSTEM_TIME)();
                        let elapsed = Duration::from_millis(now.saturating_sub(last_cull));
                        if elapsed > config.knobs.connection_pool_drain_cull_stagger()
                            && last_drain_cull
                                .compare_exchange_weak(
                                    last_cull,
                                    now,
                                    Ordering::SeqCst,
                                    Ordering::SeqCst,
                                )
                                .is_ok()
                        {
                            return Err(HookError::message("connection is on a draining node"));
                        }
                    }
                }

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
                    // A pre_recycle error discards this connection and the
                    // acquire proceeds with another (or a fresh) one.
                    return Err(HookError::message("connection has been TTLed"));
                }

                Ok(())
            }))
            .build()
            .expect("postgres connection pool built with incorrect parameters");

        Ok(PostgresClient {
            pool,
            knobs,
            metrics: config.metrics,
            draining_nodes,
            drain_watchdog: std::sync::OnceLock::new(),
            drain_recycling_supported,
        })
    }

    /// How often the drain watchdog polls for draining nodes. Chosen to be
    /// well under typical drain grace windows so the pool can move off a
    /// draining node before the server hard-closes its connections.
    const DRAIN_POLL_INTERVAL: Duration = Duration::from_secs(5);
    /// How long a watchdog acquire may wait for a pool connection. Kept short
    /// so the watchdog never meaningfully competes with real acquires.
    const DRAIN_POLL_ACQUIRE_TIMEOUT: Duration = Duration::from_millis(100);

    fn ensure_drain_watchdog(&self) {
        self.drain_watchdog.get_or_init(|| {
            let pool = self.pool.clone();
            let knobs = Arc::clone(&self.knobs);
            let draining_nodes = Arc::clone(&self.draining_nodes);
            let drain_recycling_supported = Arc::clone(&self.drain_recycling_supported);
            mz_ore::task::spawn(|| "postgres_client_drain_watchdog", async move {
                let mut interval = tokio::time::interval(Self::DRAIN_POLL_INTERVAL);
                interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
                loop {
                    interval.tick().await;
                    if !knobs.drain_aware_recycling()
                        || !drain_recycling_supported.load(Ordering::SeqCst)
                    {
                        continue;
                    }
                    Self::drain_poll_tick(&pool, &draining_nodes, &drain_recycling_supported).await;
                }
            })
            .abort_on_drop()
        });
    }

    /// Polls CockroachDB's gossiped liveness for draining nodes and replaces
    /// the draining set with the result.
    async fn drain_poll_tick(
        pool: &Pool<Manager>,
        draining_nodes: &Arc<Mutex<BTreeSet<i64>>>,
        drain_recycling_supported: &Arc<AtomicBool>,
    ) {
        let timeouts = Timeouts {
            wait: Some(Self::DRAIN_POLL_ACQUIRE_TIMEOUT),
            ..Timeouts::default()
        };
        let Ok(conn) = pool.timeout_get(&timeouts).await else {
            return;
        };
        // `draining` and `membership` are deliberate operator states (rolling
        // restart, decommission), so connections to matching nodes are doomed
        // and safe to cull. The join against `gossip_nodes` restricts the
        // result to current cluster members: liveness records for
        // long-decommissioned nodes linger indefinitely (frequently with
        // `draining` still set from their final drain) and would otherwise
        // pollute the set forever. Liveness `expiration` is deliberately NOT
        // consulted: an expired record is a symptom (crash, partition, clock
        // skew) that TCP keepalives already handle, and acting on it could
        // mass-cull a healthy pool when gossip itself is stale.
        #[allow(clippy::disallowed_methods)]
        let rows = conn
            .query(
                "SELECT l.node_id::INT8 \
                 FROM crdb_internal.gossip_liveness l \
                 JOIN crdb_internal.gossip_nodes n ON l.node_id = n.node_id \
                 WHERE l.draining OR l.membership != 'active'",
                &[],
            )
            .await;
        match rows {
            Ok(rows) => {
                let nodes: BTreeSet<i64> = rows.iter().map(|row| row.get(0)).collect();
                let mut set = draining_nodes.lock().expect("draining_nodes lock poisoned");
                if nodes != *set {
                    info!("draining CockroachDB nodes changed: {nodes:?}");
                    *set = nodes;
                }
            }
            Err(e) => {
                if is_crdb_unsupported_err(&e) {
                    // Vanilla Postgres: latch the feature off for the
                    // lifetime of this pool.
                    drain_recycling_supported.store(false, Ordering::SeqCst);
                    info!("disabling drain-aware recycling, cannot poll liveness: {e}");
                } else {
                    // Transient failure. Leave the set unchanged.
                    debug!("unable to poll for draining nodes: {e}");
                }
            }
        }
    }

    fn status_metrics(&self, status: Status) {
        self.metrics
            .connpool_available
            .set(f64::cast_lossy(status.available));
        self.metrics.connpool_size.set(u64::cast_from(status.size));
        // Don't bother reporting the maximum size of the pool... we know that from config.
    }

    /// The current [`Status`] of the connection pool.
    ///
    /// NOTE: this briefly locks the pool.
    pub fn status(&self) -> Status {
        self.pool.status()
    }

    /// Marks a node as draining, as the watchdog would.
    #[cfg(test)]
    fn mark_node_draining(&self, node_id: i64) {
        self.draining_nodes
            .lock()
            .expect("draining_nodes lock poisoned")
            .insert(node_id);
    }

    /// Gets connection from the pool or waits for one to become available.
    pub async fn get_connection(&self) -> Result<Connection, PoolError> {
        if self.knobs.drain_aware_recycling() {
            self.ensure_drain_watchdog();
        }
        let start = Instant::now();
        // note that getting the pool status here requires briefly locking the pool
        let status = self.pool.status();
        // Apply knob changes to the pool cap without a restart, so an operator
        // can grow (or shrink) the pool during an incident or ahead of planned
        // CRDB maintenance. Shrinking is graceful: deadpool drops surplus
        // connections as they are returned.
        let max_size = self.knobs.connection_pool_max_size();
        if status.max_size != max_size {
            self.pool.resize(max_size);
        }
        self.status_metrics(status);
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

#[cfg(test)]
mod tests {
    use std::str::FromStr;
    use std::sync::atomic::AtomicUsize;

    use mz_ore::metrics::MetricsRegistry;

    use super::*;

    #[derive(Debug)]
    struct TestKnobs {
        max_size: AtomicUsize,
        drain_aware: bool,
    }

    impl PostgresClientKnobs for TestKnobs {
        fn connection_pool_max_size(&self) -> usize {
            self.max_size.load(Ordering::SeqCst)
        }
        fn drain_aware_recycling(&self) -> bool {
            self.drain_aware
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
            Duration::from_secs(5)
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
        fn statement_timeout(&self) -> Duration {
            Duration::ZERO
        }
    }

    /// Verifies that a changed `connection_pool_max_size` knob is applied to a
    /// live pool on the next acquire, without reopening the client.
    ///
    /// Requires a running Postgres/CRDB, opted into via the same environment
    /// variable as the persist external-storage tests. No-op otherwise so
    /// `cargo test` works on unconfigured environments.
    #[mz_ore::test(tokio::test(flavor = "multi_thread"))]
    async fn pool_resize_applies_on_acquire() {
        let url = match std::env::var("MZ_PERSIST_EXTERNAL_STORAGE_TEST_POSTGRES_URL") {
            Ok(url) => SensitiveUrl::from_str(&url).expect("valid url"),
            Err(_) => return,
        };
        let knobs = Arc::new(TestKnobs {
            max_size: AtomicUsize::new(2),
            drain_aware: false,
        });
        let dyn_knobs: Arc<dyn PostgresClientKnobs> = Arc::<TestKnobs>::clone(&knobs);
        let config = PostgresClientConfig::new(
            url,
            dyn_knobs,
            PostgresClientMetrics::new(&MetricsRegistry::new(), "mz_postgres_client_test"),
        );
        let client = PostgresClient::open(config).expect("open client");

        let conn = client.get_connection().await.expect("connection");
        assert_eq!(client.status().max_size, 2);

        // Growing applies on the next acquire.
        knobs.max_size.store(5, Ordering::SeqCst);
        let conn2 = client.get_connection().await.expect("connection");
        assert_eq!(client.status().max_size, 5);

        // Shrinking applies too. Surplus connections are dropped as they are
        // returned to the pool.
        knobs.max_size.store(1, Ordering::SeqCst);
        drop(conn);
        drop(conn2);
        let _conn3 = client.get_connection().await.expect("connection");
        assert_eq!(client.status().max_size, 1);
    }

    /// Verifies that connections are stamped with their CockroachDB node and
    /// that marking the node as draining causes the pooled connection to be
    /// discarded and replaced on the next acquire. Opt-in like the other
    /// tests in this module; requires a CockroachDB backend.
    #[mz_ore::test(tokio::test(flavor = "multi_thread"))]
    async fn drain_aware_recycling_culls_stamped_connections() {
        let url = match std::env::var("MZ_PERSIST_EXTERNAL_STORAGE_TEST_POSTGRES_URL") {
            Ok(url) => SensitiveUrl::from_str(&url).expect("valid url"),
            Err(_) => return,
        };
        // A pool of exactly one connection keeps the assertions below
        // deterministic even though the drain watchdog concurrently acquires
        // from (and could otherwise grow) the pool.
        let knobs = Arc::new(TestKnobs {
            max_size: AtomicUsize::new(1),
            drain_aware: true,
        });
        let dyn_knobs: Arc<dyn PostgresClientKnobs> = Arc::<TestKnobs>::clone(&knobs);
        let config = PostgresClientConfig::new(
            url,
            dyn_knobs,
            PostgresClientMetrics::new(&MetricsRegistry::new(), "mz_postgres_client_test"),
        );
        let client = PostgresClient::open(config).expect("open client");

        let conn = client.get_connection().await.expect("connection");
        let Some(node_id) = conn.node_id() else {
            // Not a CockroachDB backend. CI runs these tests against vanilla
            // Postgres, so this is the path exercised there: verify the
            // feature detects the unsupported backend and latches itself off
            // rather than retrying `crdb_internal` on every connection.
            drop(conn);
            assert!(!client.drain_recycling_supported.load(Ordering::SeqCst));
            let created_before = client.metrics.connpool_connections_created.get();
            for _ in 0..3 {
                let conn = client.get_connection().await.expect("connection");
                assert_eq!(conn.node_id(), None);
            }
            assert_eq!(
                client.metrics.connpool_connections_created.get(),
                created_before,
                "connections should be reused once the feature latches off",
            );
            return;
        };
        drop(conn);

        // The single connection is reused while its node is healthy: no
        // matter how the watchdog's acquires interleave, nothing is created.
        let created_before = client.metrics.connpool_connections_created.get();
        drop(client.get_connection().await.expect("connection"));
        assert_eq!(
            client.metrics.connpool_connections_created.get(),
            created_before,
        );

        // Once its node is draining, the pooled connection is discarded and
        // the acquire is served by a freshly created connection. (On this
        // single-node test backend the replacement is immediately doomed too,
        // so every subsequent acquire, including the watchdog's, may create;
        // assert on "at least one new connection" rather than an exact count.
        // Whether the replacement is itself stamped is deliberately not
        // asserted: against a role that cannot read `gossip_liveness` the
        // watchdog correctly latches the feature off partway through.)
        client.mark_node_draining(node_id);
        let _conn = client.get_connection().await.expect("connection");
        assert!(client.metrics.connpool_connections_created.get() > created_before);
    }
}

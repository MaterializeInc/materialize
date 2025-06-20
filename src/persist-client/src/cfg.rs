// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

#![allow(missing_docs)]

//! The tunable knobs for persist.

use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use mz_build_info::BuildInfo;
use mz_dyncfg::{Config, ConfigDefault, ConfigSet, ConfigUpdates};
use mz_ore::instrument;
use mz_ore::now::NowFn;
use mz_persist::cfg::BlobKnobs;
use mz_persist::retry::Retry;
use mz_postgres_client::PostgresClientKnobs;
use proptest_derive::Arbitrary;
use semver::Version;
use serde::{Deserialize, Serialize};
use tokio::sync::watch;

use crate::internal::machine::{
    NEXT_LISTEN_BATCH_RETRYER_CLAMP, NEXT_LISTEN_BATCH_RETRYER_INITIAL_BACKOFF,
    NEXT_LISTEN_BATCH_RETRYER_MULTIPLIER,
};
use crate::internal::state::ROLLUP_THRESHOLD;
use crate::operators::STORAGE_SOURCE_DECODE_FUEL;
use crate::read::READER_LEASE_DURATION;

const SELF_MANAGED_VERSIONS: &[Version] = &[
    // 25.1
    Version::new(0, 130, 0),
];

/// The tunable knobs for persist.
///
/// Tuning inputs:
/// - A larger blob_target_size (capped at KEY_VAL_DATA_MAX_LEN) results in
///   fewer entries in consensus state. Before we have compaction and/or
///   incremental state, it is already growing without bound, so this is a
///   concern. OTOH, for any "reasonable" size (> 100MiB?) of blob_target_size,
///   it seems we'd end up with a pretty tremendous amount of data in the shard
///   before this became a real issue.
/// - A larger blob_target_size will results in fewer s3 operations, which are
///   charged per operation. (Hmm, maybe not if we're charged per call in a
///   multipart op. The S3Blob impl already chunks things at 8MiB.)
/// - A smaller blob_target_size will result in more even memory usage in
///   readers.
/// - A larger batch_builder_max_outstanding_parts increases throughput (to a
///   point).
/// - A smaller batch_builder_max_outstanding_parts provides a bound on the
///   amount of memory used by a writer.
/// - A larger compaction_heuristic_min_inputs means state size is larger.
/// - A smaller compaction_heuristic_min_inputs means more compactions happen
///   (higher write amp).
/// - A larger compaction_heuristic_min_updates means more consolidations are
///   discovered while reading a snapshot (higher read amp and higher space
///   amp).
/// - A smaller compaction_heuristic_min_updates means more compactions happen
///   (higher write amp).
///
/// Tuning logic:
/// - blob_target_size was initially selected to be an exact multiple of 8MiB
///   (the s3 multipart size) that was in the same neighborhood as our initial
///   max throughput (~250MiB).
/// - batch_builder_max_outstanding_parts was initially selected to be as small
///   as possible without harming pipelining. 0 means no pipelining, 1 is full
///   pipelining as long as generating data takes less time than writing to s3
///   (hopefully a fair assumption), 2 is a little extra slop on top of 1.
/// - compaction_heuristic_min_inputs was set by running the open-loop benchmark
///   with batches of size 10,240 bytes (selected to be small but such that the
///   overhead of our columnar encoding format was less than 10%) and manually
///   increased until the write amp stopped going down. This becomes much less
///   important once we have incremental state. The initial value is a
///   placeholder and should be revisited at some point.
/// - compaction_heuristic_min_updates was set via a thought experiment. This is
///   an `O(n*log(n))` upper bound on the number of unconsolidated updates that
///   would be consolidated if we compacted as the in-mem Spine does. The
///   initial value is a placeholder and should be revisited at some point.
///
/// TODO: Move these tuning notes into SessionVar descriptions once we have
/// SystemVars for most of these.
//
// TODO: The configs left here don't react dynamically to changes. Move as many
// of them to DynamicConfig as possible.
#[derive(Debug, Clone)]
pub struct PersistConfig {
    /// Info about which version of the code is running.
    pub build_version: Version,
    /// Hostname of this persist user. Stored in state and used for debugging.
    pub hostname: String,
    /// Whether this persist instance is running in a "cc" sized cluster.
    pub is_cc_active: bool,
    /// Memory limit of the process, if known.
    pub announce_memory_limit: Option<usize>,
    /// A clock to use for all leasing and other non-debugging use.
    pub now: NowFn,
    /// Persist [Config]s that can change value dynamically within the lifetime
    /// of a process.
    ///
    /// TODO(cfg): Entirely replace dynamic with this.
    pub configs: Arc<ConfigSet>,
    /// Indicates whether `configs` has been synced at least once with an
    /// upstream source.
    configs_synced_once: Arc<watch::Sender<bool>>,
    /// Whether to physically and logically compact batches in blob storage.
    pub compaction_enabled: bool,
    /// Whether the `Compactor` will process compaction requests, or drop them on the floor.
    pub compaction_process_requests: Arc<AtomicBool>,
    /// In Compactor::compact_and_apply_background, the maximum number of concurrent
    /// compaction requests that can execute for a given shard.
    pub compaction_concurrency_limit: usize,
    /// In Compactor::compact_and_apply_background, the maximum number of pending
    /// compaction requests to queue.
    pub compaction_queue_size: usize,
    /// In Compactor::compact_and_apply_background, how many updates to encode or
    /// decode before voluntarily yielding the task.
    pub compaction_yield_after_n_updates: usize,
    /// Length of time after a writer's last operation after which the writer
    /// may be expired.
    pub writer_lease_duration: Duration,
    /// Length of time between critical handles' calls to downgrade since
    pub critical_downgrade_interval: Duration,
    /// Number of worker threads to create for the [`crate::IsolatedRuntime`], defaults to the
    /// number of threads.
    pub isolated_runtime_worker_threads: usize,
}

// Impl Deref to ConfigSet for convenience of accessing the dynamic configs.
impl std::ops::Deref for PersistConfig {
    type Target = ConfigSet;
    fn deref(&self) -> &Self::Target {
        &self.configs
    }
}

impl PersistConfig {
    /// Returns a new instance of [PersistConfig] with default tuning and
    /// default ConfigSet.
    pub fn new_default_configs(build_info: &BuildInfo, now: NowFn) -> Self {
        Self::new(build_info, now, all_dyncfgs(ConfigSet::default()))
    }

    /// Returns a new instance of [PersistConfig] with default tuning and the
    /// specified ConfigSet.
    pub fn new(build_info: &BuildInfo, now: NowFn, configs: ConfigSet) -> Self {
        // Escape hatch in case we need to disable compaction.
        let compaction_disabled = mz_ore::env::is_var_truthy("MZ_PERSIST_COMPACTION_DISABLED");

        // We create receivers on demand, so we drop the initial receiver.
        let (configs_synced_once, _) = watch::channel(false);

        Self {
            build_version: build_info.semver_version(),
            is_cc_active: false,
            announce_memory_limit: None,
            now,
            configs: Arc::new(configs),
            configs_synced_once: Arc::new(configs_synced_once),
            compaction_enabled: !compaction_disabled,
            compaction_process_requests: Arc::new(AtomicBool::new(true)),
            compaction_concurrency_limit: 5,
            compaction_queue_size: 20,
            compaction_yield_after_n_updates: 100_000,
            writer_lease_duration: 60 * Duration::from_secs(60),
            critical_downgrade_interval: Duration::from_secs(30),
            isolated_runtime_worker_threads: num_cpus::get(),
            // TODO: This doesn't work with the process orchestrator. Instead,
            // separate --log-prefix into --service-name and --enable-log-prefix
            // options, where the first is always provided and the second is
            // conditionally enabled by the process orchestrator.
            hostname: std::env::var("HOSTNAME").unwrap_or_else(|_| "unknown".to_owned()),
        }
    }

    pub(crate) fn set_config<T: ConfigDefault>(&self, cfg: &Config<T>, val: T) {
        let mut updates = ConfigUpdates::default();
        updates.add(cfg, val);
        updates.apply(self)
    }

    /// Applies the provided updates to this configuration.
    ///
    /// You should prefer calling this method over mutating `self.configs`
    /// directly, so that [`Self::configs_synced_once`] can be properly
    /// maintained.
    pub fn apply_from(&self, updates: &ConfigUpdates) {
        updates.apply(&self.configs);
        self.configs_synced_once.send_replace(true);
    }

    /// Resolves when `configs` has been synced at least once with an upstream
    /// source, i.e., via [`Self::apply_from`].
    ///
    /// If `configs` has already been synced once at the time the method is
    /// called, resolves immediately.
    ///
    /// Useful in conjunction with configuration parameters that cannot be
    /// dynamically updated once set (e.g., PubSub).
    #[instrument(level = "info")]
    pub async fn configs_synced_once(&self) {
        self.configs_synced_once
            .subscribe()
            .wait_for(|synced| *synced)
            .await
            .expect("we have a borrow on sender so it cannot drop");
    }

    /// The maximum amount of work to do in the persist_source mfp_and_decode
    /// operator before yielding.
    pub fn storage_source_decode_fuel(&self) -> usize {
        STORAGE_SOURCE_DECODE_FUEL.get(self)
    }

    /// Overrides the value for "persist_reader_lease_duration".
    pub fn set_reader_lease_duration(&self, val: Duration) {
        self.set_config(&READER_LEASE_DURATION, val);
    }

    /// Overrides the value for "persist_rollup_threshold".
    pub fn set_rollup_threshold(&self, val: usize) {
        self.set_config(&ROLLUP_THRESHOLD, val);
    }

    /// Overrides the value for the "persist_next_listen_batch_retryer_*"
    /// configs.
    pub fn set_next_listen_batch_retryer(&self, val: RetryParameters) {
        self.set_config(
            &NEXT_LISTEN_BATCH_RETRYER_INITIAL_BACKOFF,
            val.initial_backoff,
        );
        self.set_config(&NEXT_LISTEN_BATCH_RETRYER_MULTIPLIER, val.multiplier);
        self.set_config(&NEXT_LISTEN_BATCH_RETRYER_CLAMP, val.clamp);
    }

    pub fn disable_compaction(&self) {
        tracing::info!("Disabling Persist Compaction");
        self.compaction_process_requests
            .store(false, std::sync::atomic::Ordering::Relaxed);
    }

    pub fn enable_compaction(&self) {
        tracing::info!("Enabling Persist Compaction");
        self.compaction_process_requests
            .store(true, std::sync::atomic::Ordering::Relaxed);
    }

    /// Returns a new instance of [PersistConfig] for tests.
    pub fn new_for_tests() -> Self {
        use mz_build_info::DUMMY_BUILD_INFO;
        use mz_ore::now::SYSTEM_TIME;

        let mut cfg = Self::new_default_configs(&DUMMY_BUILD_INFO, SYSTEM_TIME.clone());
        cfg.hostname = "tests".into();
        cfg
    }
}

#[allow(non_upper_case_globals)]
pub(crate) const MiB: usize = 1024 * 1024;

/// Adds the full set of all persist [Config]s.
///
/// TODO(cfg): Consider replacing this with a static global registry powered by
/// something like the `ctor` or `inventory` crate. This would involve managing
/// the footgun of a Config being linked into one binary but not the other.
pub fn all_dyncfgs(configs: ConfigSet) -> ConfigSet {
    mz_persist::cfg::all_dyn_configs(configs)
        .add(&crate::batch::BATCH_DELETE_ENABLED)
        .add(&crate::batch::BLOB_TARGET_SIZE)
        .add(&crate::batch::INLINE_WRITES_TOTAL_MAX_BYTES)
        .add(&crate::batch::INLINE_WRITES_SINGLE_MAX_BYTES)
        .add(&crate::batch::ENCODING_ENABLE_DICTIONARY)
        .add(&crate::batch::ENCODING_COMPRESSION_FORMAT)
        .add(&crate::batch::STRUCTURED_KEY_LOWER_LEN)
        .add(&crate::batch::MAX_RUN_LEN)
        .add(&crate::batch::MAX_RUNS)
        .add(&BLOB_OPERATION_TIMEOUT)
        .add(&BLOB_OPERATION_ATTEMPT_TIMEOUT)
        .add(&BLOB_CONNECT_TIMEOUT)
        .add(&BLOB_READ_TIMEOUT)
        .add(&crate::cfg::CONSENSUS_CONNECTION_POOL_MAX_SIZE)
        .add(&crate::cfg::CONSENSUS_CONNECTION_POOL_MAX_WAIT)
        .add(&crate::cfg::CONSENSUS_CONNECTION_POOL_TTL_STAGGER)
        .add(&crate::cfg::CONSENSUS_CONNECTION_POOL_TTL)
        .add(&crate::cfg::CRDB_CONNECT_TIMEOUT)
        .add(&crate::cfg::CRDB_TCP_USER_TIMEOUT)
        .add(&crate::cfg::USE_CRITICAL_SINCE_TXN)
        .add(&crate::cfg::USE_CRITICAL_SINCE_CATALOG)
        .add(&crate::cfg::USE_CRITICAL_SINCE_SOURCE)
        .add(&crate::cfg::USE_CRITICAL_SINCE_SNAPSHOT)
        .add(&crate::cfg::USE_GLOBAL_TXN_CACHE_SOURCE)
        .add(&BATCH_BUILDER_MAX_OUTSTANDING_PARTS)
        .add(&COMPACTION_HEURISTIC_MIN_INPUTS)
        .add(&COMPACTION_HEURISTIC_MIN_PARTS)
        .add(&COMPACTION_HEURISTIC_MIN_UPDATES)
        .add(&COMPACTION_MEMORY_BOUND_BYTES)
        .add(&GC_BLOB_DELETE_CONCURRENCY_LIMIT)
        .add(&STATE_VERSIONS_RECENT_LIVE_DIFFS_LIMIT)
        .add(&USAGE_STATE_FETCH_CONCURRENCY_LIMIT)
        .add(&crate::cli::admin::CATALOG_FORCE_COMPACTION_FUEL)
        .add(&crate::cli::admin::CATALOG_FORCE_COMPACTION_WAIT)
        .add(&crate::cli::admin::EXPRESSION_CACHE_FORCE_COMPACTION_FUEL)
        .add(&crate::cli::admin::EXPRESSION_CACHE_FORCE_COMPACTION_WAIT)
        .add(&crate::fetch::FETCH_SEMAPHORE_COST_ADJUSTMENT)
        .add(&crate::fetch::FETCH_SEMAPHORE_PERMIT_ADJUSTMENT)
        .add(&crate::fetch::FETCH_VALIDATE_PART_LOWER_BOUNDS_ON_READ)
        .add(&crate::fetch::OPTIMIZE_IGNORED_DATA_FETCH)
        .add(&crate::internal::cache::BLOB_CACHE_MEM_LIMIT_BYTES)
        .add(&crate::internal::cache::BLOB_CACHE_SCALE_WITH_THREADS)
        .add(&crate::internal::cache::BLOB_CACHE_SCALE_FACTOR_BYTES)
        .add(&crate::internal::compact::COMPACTION_MINIMUM_TIMEOUT)
        .add(&crate::internal::compact::COMPACTION_USE_MOST_RECENT_SCHEMA)
        .add(&crate::internal::compact::COMPACTION_CHECK_PROCESS_FLAG)
        .add(&crate::internal::machine::CLAIM_UNCLAIMED_COMPACTIONS)
        .add(&crate::internal::machine::CLAIM_COMPACTION_PERCENT)
        .add(&crate::internal::machine::CLAIM_COMPACTION_MIN_VERSION)
        .add(&crate::internal::machine::NEXT_LISTEN_BATCH_RETRYER_CLAMP)
        .add(&crate::internal::machine::NEXT_LISTEN_BATCH_RETRYER_FIXED_SLEEP)
        .add(&crate::internal::machine::NEXT_LISTEN_BATCH_RETRYER_INITIAL_BACKOFF)
        .add(&crate::internal::machine::NEXT_LISTEN_BATCH_RETRYER_MULTIPLIER)
        .add(&crate::internal::state::ROLLUP_THRESHOLD)
        .add(&crate::internal::state::ROLLUP_USE_ACTIVE_ROLLUP)
        .add(&crate::internal::state::GC_FALLBACK_THRESHOLD_MS)
        .add(&crate::internal::state::GC_USE_ACTIVE_GC)
        .add(&crate::internal::state::ROLLUP_FALLBACK_THRESHOLD_MS)
        .add(&crate::operators::STORAGE_SOURCE_DECODE_FUEL)
        .add(&crate::read::READER_LEASE_DURATION)
        .add(&crate::rpc::PUBSUB_CLIENT_ENABLED)
        .add(&crate::rpc::PUBSUB_PUSH_DIFF_ENABLED)
        .add(&crate::rpc::PUBSUB_SAME_PROCESS_DELEGATE_ENABLED)
        .add(&crate::rpc::PUBSUB_CONNECT_ATTEMPT_TIMEOUT)
        .add(&crate::rpc::PUBSUB_REQUEST_TIMEOUT)
        .add(&crate::rpc::PUBSUB_CONNECT_MAX_BACKOFF)
        .add(&crate::rpc::PUBSUB_CLIENT_SENDER_CHANNEL_SIZE)
        .add(&crate::rpc::PUBSUB_CLIENT_RECEIVER_CHANNEL_SIZE)
        .add(&crate::rpc::PUBSUB_SERVER_CONNECTION_CHANNEL_SIZE)
        .add(&crate::rpc::PUBSUB_STATE_CACHE_SHARD_REF_CHANNEL_SIZE)
        .add(&crate::rpc::PUBSUB_RECONNECT_BACKOFF)
        .add(&crate::stats::STATS_AUDIT_PERCENT)
        .add(&crate::stats::STATS_BUDGET_BYTES)
        .add(&crate::stats::STATS_COLLECTION_ENABLED)
        .add(&crate::stats::STATS_FILTER_ENABLED)
        .add(&crate::stats::STATS_UNTRIMMABLE_COLUMNS_EQUALS)
        .add(&crate::stats::STATS_UNTRIMMABLE_COLUMNS_PREFIX)
        .add(&crate::stats::STATS_UNTRIMMABLE_COLUMNS_SUFFIX)
        .add(&crate::fetch::PART_DECODE_FORMAT)
        .add(&crate::write::COMBINE_INLINE_WRITES)
}

impl PersistConfig {
    pub(crate) const DEFAULT_FALLBACK_ROLLUP_THRESHOLD_MULTIPLIER: usize = 3;

    pub fn set_state_versions_recent_live_diffs_limit(&self, val: usize) {
        self.set_config(&STATE_VERSIONS_RECENT_LIVE_DIFFS_LIMIT, val);
    }
}

/// Sets the maximum size of the connection pool that is used by consensus.
///
/// Requires a restart of the process to take effect.
pub const CONSENSUS_CONNECTION_POOL_MAX_SIZE: Config<usize> = Config::new(
    "persist_consensus_connection_pool_max_size",
    50,
    "The maximum size the connection pool to Postgres/CRDB will grow to.",
);

/// Sets the maximum amount of time we'll wait to acquire a connection from
/// the connection pool.
///
/// Requires a restart of the process to take effect.
const CONSENSUS_CONNECTION_POOL_MAX_WAIT: Config<Duration> = Config::new(
    "persist_consensus_connection_pool_max_wait",
    Duration::from_secs(60),
    "The amount of time we'll wait for a connection to become available.",
);

/// The minimum TTL of a connection to Postgres/CRDB before it is proactively
/// terminated. Connections are routinely culled to balance load against the
/// downstream database.
const CONSENSUS_CONNECTION_POOL_TTL: Config<Duration> = Config::new(
    "persist_consensus_connection_pool_ttl",
    Duration::from_secs(300),
    "\
    The minimum TTL of a Consensus connection to Postgres/CRDB before it is \
    proactively terminated",
);

/// The minimum time between TTLing connections to Postgres/CRDB. This delay is
/// used to stagger reconnections to avoid stampedes and high tail latencies.
/// This value should be much less than `consensus_connection_pool_ttl` so that
/// reconnections are biased towards terminating the oldest connections first. A
/// value of `consensus_connection_pool_ttl /
/// consensus_connection_pool_max_size` is likely a good place to start so that
/// all connections are rotated when the pool is fully used.
const CONSENSUS_CONNECTION_POOL_TTL_STAGGER: Config<Duration> = Config::new(
    "persist_consensus_connection_pool_ttl_stagger",
    Duration::from_secs(6),
    "The minimum time between TTLing Consensus connections to Postgres/CRDB.",
);

/// The duration to wait for a Consensus Postgres/CRDB connection to be made
/// before retrying.
pub const CRDB_CONNECT_TIMEOUT: Config<Duration> = Config::new(
    "crdb_connect_timeout",
    Duration::from_secs(5),
    "The time to connect to CockroachDB before timing out and retrying.",
);

/// The TCP user timeout for a Consensus Postgres/CRDB connection. Specifies the
/// amount of time that transmitted data may remain unacknowledged before the
/// TCP connection is forcibly closed.
pub const CRDB_TCP_USER_TIMEOUT: Config<Duration> = Config::new(
    "crdb_tcp_user_timeout",
    Duration::from_secs(30),
    "\
    The TCP timeout for connections to CockroachDB. Specifies the amount of \
    time that transmitted data may remain unacknowledged before the TCP \
    connection is forcibly closed.",
);

/// Migrate the txns code to use the critical since when opening a new read handle.
pub const USE_CRITICAL_SINCE_TXN: Config<bool> = Config::new(
    "persist_use_critical_since_txn",
    true,
    "Use the critical since (instead of the overall since) when initializing a subscribe.",
);

/// Migrate the catalog to use the critical since when opening a new read handle.
pub const USE_CRITICAL_SINCE_CATALOG: Config<bool> = Config::new(
    "persist_use_critical_since_catalog",
    false,
    "Use the critical since (instead of the overall since) for the Persist-backed catalog.",
);

/// Migrate the persist source to use the critical since when opening a new read handle.
pub const USE_CRITICAL_SINCE_SOURCE: Config<bool> = Config::new(
    "persist_use_critical_since_source",
    false,
    "Use the critical since (instead of the overall since) in the Persist source.",
);

/// Migrate snapshots to use the critical since when opening a new read handle.
pub const USE_CRITICAL_SINCE_SNAPSHOT: Config<bool> = Config::new(
    "persist_use_critical_since_snapshot",
    false,
    "Use the critical since (instead of the overall since) when taking snapshots in the controller or in fast-path peeks.",
);

/// Migrate the persist source to use a process global txn cache.
pub const USE_GLOBAL_TXN_CACHE_SOURCE: Config<bool> = Config::new(
    "use_global_txn_cache_source",
    true,
    "Use the process global txn cache (instead of an operator local one) in the Persist source.",
);

/// The maximum number of parts (s3 blobs) that [crate::batch::BatchBuilder]
/// will pipeline before back-pressuring [crate::batch::BatchBuilder::add]
/// calls on previous ones finishing.
pub const BATCH_BUILDER_MAX_OUTSTANDING_PARTS: Config<usize> = Config::new(
    "persist_batch_builder_max_outstanding_parts",
    2,
    "The number of writes a batch builder can have outstanding before we slow down the writer.",
);

/// In Compactor::compact_and_apply, we do the compaction (don't skip it)
/// if the number of inputs is at least this many. Compaction is performed
/// if any of the heuristic criteria are met (they are OR'd).
pub const COMPACTION_HEURISTIC_MIN_INPUTS: Config<usize> = Config::new(
    "persist_compaction_heuristic_min_inputs",
    8,
    "Don't skip compaction if we have more than this many hollow batches as input.",
);

/// In Compactor::compact_and_apply, we do the compaction (don't skip it)
/// if the number of batch parts is at least this many. Compaction is performed
/// if any of the heuristic criteria are met (they are OR'd).
pub const COMPACTION_HEURISTIC_MIN_PARTS: Config<usize> = Config::new(
    "persist_compaction_heuristic_min_parts",
    8,
    "Don't skip compaction if we have more than this many parts as input.",
);

/// In Compactor::compact_and_apply, we do the compaction (don't skip it)
/// if the number of updates is at least this many. Compaction is performed
/// if any of the heuristic criteria are met (they are OR'd).
pub const COMPACTION_HEURISTIC_MIN_UPDATES: Config<usize> = Config::new(
    "persist_compaction_heuristic_min_updates",
    1024,
    "Don't skip compaction if we have more than this many updates as input.",
);

/// The upper bound on compaction's memory consumption. The value must be at
/// least 4*`blob_target_size`. Increasing this value beyond the minimum allows
/// compaction to merge together more runs at once, providing greater
/// consolidation of updates, at the cost of greater memory usage.
pub const COMPACTION_MEMORY_BOUND_BYTES: Config<usize> = Config::new(
    "persist_compaction_memory_bound_bytes",
    1024 * MiB,
    "Attempt to limit compaction to this amount of memory.",
);

/// The maximum number of concurrent blob deletes during garbage collection.
pub const GC_BLOB_DELETE_CONCURRENCY_LIMIT: Config<usize> = Config::new(
    "persist_gc_blob_delete_concurrency_limit",
    32,
    "Limit the number of concurrent deletes GC can perform to this threshold.",
);

/// The # of diffs to initially scan when fetching the latest consensus state, to
/// determine which requests go down the fast vs slow path. Should be large enough
/// to fetch all live diffs in the steady-state, and small enough to query Consensus
/// at high volume. Steady-state usage should accommodate readers that require
/// seqno-holds for reasonable amounts of time, which to start we say is 10s of minutes.
///
/// This value ought to be defined in terms of `NEED_ROLLUP_THRESHOLD` to approximate
/// when we expect rollups to be written and therefore when old states will be truncated
/// by GC.
pub const STATE_VERSIONS_RECENT_LIVE_DIFFS_LIMIT: Config<usize> = Config::new(
    "persist_state_versions_recent_live_diffs_limit",
    30 * 128,
    "Fetch this many diffs when fetching recent diffs.",
);

/// The maximum number of concurrent state fetches during usage computation.
pub const USAGE_STATE_FETCH_CONCURRENCY_LIMIT: Config<usize> = Config::new(
    "persist_usage_state_fetch_concurrency_limit",
    8,
    "Limit the concurrency in of fetching in the perioding Persist-storage-usage calculation.",
);

impl PostgresClientKnobs for PersistConfig {
    fn connection_pool_max_size(&self) -> usize {
        CONSENSUS_CONNECTION_POOL_MAX_SIZE.get(self)
    }

    fn connection_pool_max_wait(&self) -> Option<Duration> {
        Some(CONSENSUS_CONNECTION_POOL_MAX_WAIT.get(self))
    }

    fn connection_pool_ttl(&self) -> Duration {
        CONSENSUS_CONNECTION_POOL_TTL.get(self)
    }

    fn connection_pool_ttl_stagger(&self) -> Duration {
        CONSENSUS_CONNECTION_POOL_TTL_STAGGER.get(self)
    }

    fn connect_timeout(&self) -> Duration {
        CRDB_CONNECT_TIMEOUT.get(self)
    }

    fn tcp_user_timeout(&self) -> Duration {
        CRDB_TCP_USER_TIMEOUT.get(self)
    }
}

#[derive(Copy, Clone, Debug, Eq, PartialEq, Arbitrary, Serialize, Deserialize)]
pub struct RetryParameters {
    pub fixed_sleep: Duration,
    pub initial_backoff: Duration,
    pub multiplier: u32,
    pub clamp: Duration,
}

impl RetryParameters {
    pub(crate) fn into_retry(self, now: SystemTime) -> Retry {
        let seed = now
            .duration_since(UNIX_EPOCH)
            .map_or(0, |x| u64::from(x.subsec_nanos()));
        Retry {
            fixed_sleep: self.fixed_sleep,
            initial_backoff: self.initial_backoff,
            multiplier: self.multiplier,
            clamp_backoff: self.clamp,
            seed,
        }
    }
}

pub(crate) const BLOB_OPERATION_TIMEOUT: Config<Duration> = Config::new(
    "persist_blob_operation_timeout",
    Duration::from_secs(180),
    "Maximum time allowed for a network call, including retry attempts.",
);

pub(crate) const BLOB_OPERATION_ATTEMPT_TIMEOUT: Config<Duration> = Config::new(
    "persist_blob_operation_attempt_timeout",
    Duration::from_secs(90),
    "Maximum time allowed for a single network call.",
);

pub(crate) const BLOB_CONNECT_TIMEOUT: Config<Duration> = Config::new(
    "persist_blob_connect_timeout",
    Duration::from_secs(7),
    "Maximum time to wait for a socket connection to be made.",
);

pub(crate) const BLOB_READ_TIMEOUT: Config<Duration> = Config::new(
    "persist_blob_read_timeout",
    Duration::from_secs(10),
    "Maximum time to wait to read the first byte of a response, including connection time.",
);

impl BlobKnobs for PersistConfig {
    fn operation_timeout(&self) -> Duration {
        BLOB_OPERATION_TIMEOUT.get(self)
    }

    fn operation_attempt_timeout(&self) -> Duration {
        BLOB_OPERATION_ATTEMPT_TIMEOUT.get(self)
    }

    fn connect_timeout(&self) -> Duration {
        BLOB_CONNECT_TIMEOUT.get(self)
    }

    fn read_timeout(&self) -> Duration {
        BLOB_READ_TIMEOUT.get(self)
    }

    fn is_cc_active(&self) -> bool {
        self.is_cc_active
    }
}

pub fn check_data_version(code_version: &Version, data_version: &Version) -> Result<(), String> {
    check_data_version_with_self_managed_versions(code_version, data_version, SELF_MANAGED_VERSIONS)
}

// If persist gets some encoded ProtoState from the future (e.g. two versions of
// code are running simultaneously against the same shard), it might have a
// field that the current code doesn't know about. This would be silently
// discarded at proto decode time. Unknown Fields [1] are a tool we can use in
// the future to help deal with this, but in the short-term, it's best to keep
// the persist read-modify-CaS loop simple for as long as we can get away with
// it (i.e. until we have to offer the ability to do rollbacks).
//
// [1]: https://developers.google.com/protocol-buffers/docs/proto3#unknowns
//
// To detect the bad situation and disallow it, we tag every version of state
// written to consensus with the version of code used to encode it. Then at
// decode time, we're able to compare the current version against any we receive
// and assert as necessary.
//
// Initially we allow any from the past (permanent backward compatibility) and
// one minor version into the future (forward compatibility). This allows us to
// run two versions concurrently for rolling upgrades. We'll have to revisit
// this logic if/when we start using major versions other than 0.
//
// We could do the same for blob data, but it shouldn't be necessary. Any blob
// data we read is going to be because we fetched it using a pointer stored in
// some persist state. If we can handle the state, we can handle the blobs it
// references, too.
pub(crate) fn check_data_version_with_self_managed_versions(
    code_version: &Version,
    data_version: &Version,
    self_managed_versions: &[Version],
) -> Result<(), String> {
    // Allow upgrades specifically between consecutive Self-Managed releases.
    let base_code_version = Version {
        patch: 0,
        ..code_version.clone()
    };
    let base_data_version = Version {
        patch: 0,
        ..data_version.clone()
    };
    if data_version >= code_version {
        for window in self_managed_versions.windows(2) {
            if base_code_version == window[0] && base_data_version <= window[1] {
                return Ok(());
            }
        }

        if let Some(last) = self_managed_versions.last() {
            if base_code_version == *last
                // kind of arbitrary, but just ensure we don't accidentally
                // upgrade too far (the previous check should ensure that a
                // new version won't take over from a too-old previous
                // version, but we want to make sure the other side also
                // doesn't get confused)
                && base_data_version
                    .minor
                    .saturating_sub(base_code_version.minor)
                    < 40
            {
                return Ok(());
            }
        }
    }

    // Allow one minor version of forward compatibility. We could avoid the
    // clone with some nested comparisons of the semver fields, but this code
    // isn't particularly performance sensitive and I find this impl easier to
    // reason about.
    let max_allowed_data_version = Version::new(
        code_version.major,
        code_version.minor.saturating_add(1),
        u64::MAX,
    );

    if &max_allowed_data_version < data_version {
        Err(format!(
            "{code_version} received persist state from the future {data_version}",
        ))
    } else {
        Ok(())
    }
}

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

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use mz_build_info::BuildInfo;
use mz_dyncfg::{Config, ConfigSet, ConfigType, ConfigUpdates};
use mz_ore::now::NowFn;
use mz_persist::cfg::BlobKnobs;
use mz_persist::retry::Retry;
use mz_postgres_client::PostgresClientKnobs;
use mz_proto::{RustType, TryFromProtoError};
use proptest_derive::Arbitrary;
use semver::Version;
use serde::{Deserialize, Serialize};

use crate::internal::machine::{
    NEXT_LISTEN_BATCH_RETRYER_CLAMP, NEXT_LISTEN_BATCH_RETRYER_INITIAL_BACKOFF,
    NEXT_LISTEN_BATCH_RETRYER_MULTIPLIER,
};
use crate::internal::state::ROLLUP_THRESHOLD;
use crate::operators::{
    PERSIST_SINK_MINIMUM_BATCH_UPDATES, STORAGE_PERSIST_SINK_MINIMUM_BATCH_UPDATES,
    STORAGE_SOURCE_DECODE_FUEL,
};
use crate::read::READER_LEASE_DURATION;
use crate::rpc::PUBSUB_CLIENT_ENABLED;

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
    /// A clock to use for all leasing and other non-debugging use.
    pub now: NowFn,
    /// Persist [Config]s that can change value dynamically within the lifetime
    /// of a process.
    ///
    /// TODO(cfg): Entirely replace dynamic with this.
    pub configs: ConfigSet,
    /// Configurations that can be dynamically updated.
    pub dynamic: Arc<DynamicConfig>,
    /// Whether to physically and logically compact batches in blob storage.
    pub compaction_enabled: bool,
    /// In Compactor::compact_and_apply_background, the maximum number of concurrent
    /// compaction requests that can execute for a given shard.
    pub compaction_concurrency_limit: usize,
    /// In Compactor::compact_and_apply_background, the maximum number of pending
    /// compaction requests to queue.
    pub compaction_queue_size: usize,
    /// In Compactor::compact_and_apply_background, how many updates to encode or
    /// decode before voluntarily yielding the task.
    pub compaction_yield_after_n_updates: usize,
    /// The maximum size of the connection pool to Postgres/CRDB when performing
    /// consensus reads and writes.
    pub consensus_connection_pool_max_size: usize,
    /// The maximum time to wait when attempting to obtain a connection from the pool.
    pub consensus_connection_pool_max_wait: Option<Duration>,
    /// Length of time after a writer's last operation after which the writer
    /// may be expired.
    pub writer_lease_duration: Duration,
    /// Length of time between critical handles' calls to downgrade since
    pub critical_downgrade_interval: Duration,
    /// Timeout per connection attempt to Persist PubSub service.
    pub pubsub_connect_attempt_timeout: Duration,
    /// Timeout per request attempt to Persist PubSub service.
    pub pubsub_request_timeout: Duration,
    /// Maximum backoff when retrying connection establishment to Persist PubSub service.
    pub pubsub_connect_max_backoff: Duration,
    /// Size of channel used to buffer send messages to PubSub service.
    pub pubsub_client_sender_channel_size: usize,
    /// Size of channel used to buffer received messages from PubSub service.
    pub pubsub_client_receiver_channel_size: usize,
    /// Size of channel used per connection to buffer broadcasted messages from PubSub server.
    pub pubsub_server_connection_channel_size: usize,
    /// Size of channel used by the state cache to broadcast shard state references.
    pub pubsub_state_cache_shard_ref_channel_size: usize,
    /// Backoff after an established connection to Persist PubSub service fails.
    pub pubsub_reconnect_backoff: Duration,
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
        Self {
            build_version: build_info.semver_version(),
            is_cc_active: false,
            now,
            configs,
            dynamic: Arc::new(DynamicConfig {
                batch_builder_max_outstanding_parts: AtomicUsize::new(2),
                compaction_heuristic_min_inputs: AtomicUsize::new(8),
                compaction_heuristic_min_parts: AtomicUsize::new(8),
                compaction_heuristic_min_updates: AtomicUsize::new(1024),
                compaction_memory_bound_bytes: AtomicUsize::new(1024 * MiB),
                gc_blob_delete_concurrency_limit: AtomicUsize::new(32),
                state_versions_recent_live_diffs_limit: AtomicUsize::new(
                    30 * ROLLUP_THRESHOLD.default(),
                ),
                usage_state_fetch_concurrency_limit: AtomicUsize::new(8),
            }),
            compaction_enabled: !compaction_disabled,
            compaction_concurrency_limit: 5,
            compaction_queue_size: 20,
            compaction_yield_after_n_updates: 100_000,
            consensus_connection_pool_max_size: 50,
            consensus_connection_pool_max_wait: Some(Duration::from_secs(60)),
            writer_lease_duration: 60 * Duration::from_secs(60),
            critical_downgrade_interval: Duration::from_secs(30),
            pubsub_connect_attempt_timeout: Duration::from_secs(5),
            pubsub_request_timeout: Duration::from_secs(5),
            pubsub_connect_max_backoff: Duration::from_secs(60),
            pubsub_client_sender_channel_size: 25,
            pubsub_client_receiver_channel_size: 25,
            pubsub_server_connection_channel_size: 25,
            pubsub_state_cache_shard_ref_channel_size: 25,
            pubsub_reconnect_backoff: Duration::from_secs(5),
            isolated_runtime_worker_threads: num_cpus::get(),
            // TODO: This doesn't work with the process orchestrator. Instead,
            // separate --log-prefix into --service-name and --enable-log-prefix
            // options, where the first is always provided and the second is
            // conditionally enabled by the process orchestrator.
            hostname: std::env::var("HOSTNAME").unwrap_or_else(|_| "unknown".to_owned()),
        }
    }

    pub(crate) fn set_config<T: ConfigType>(&self, cfg: &Config<T>, val: T) {
        let mut updates = ConfigUpdates::default();
        updates.add(cfg, val);
        updates.apply(self)
    }

    /// The minimum number of updates that justify writing out a batch in `persist_sink`'s
    /// `write_batches` operator. (If there are fewer than this minimum number of updates,
    /// they'll be forwarded on to `append_batch` to be combined and written there.)
    pub fn sink_minimum_batch_updates(&self) -> usize {
        PERSIST_SINK_MINIMUM_BATCH_UPDATES.get(self)
    }

    /// The same as `Self::sink_minimum_batch_updates`, but
    /// for storage `persist_sink`'s.
    pub fn storage_sink_minimum_batch_updates(&self) -> usize {
        STORAGE_PERSIST_SINK_MINIMUM_BATCH_UPDATES.get(self)
    }

    /// The maximum amount of work to do in the persist_source mfp_and_decode
    /// operator before yielding.
    pub fn storage_source_decode_fuel(&self) -> usize {
        STORAGE_SOURCE_DECODE_FUEL.get(self)
    }

    /// Overrides the value for "persist_pubsub_client_enabled".
    pub fn set_pubsub_client_enabled(&self, val: bool) {
        self.set_config(&PUBSUB_CLIENT_ENABLED, val);
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
        .add(&crate::cfg::CONSENSUS_CONNECTION_POOL_TTL_STAGGER)
        .add(&crate::cfg::CONSENSUS_CONNECTION_POOL_TTL)
        .add(&crate::cfg::CRDB_CONNECT_TIMEOUT)
        .add(&crate::cfg::CRDB_TCP_USER_TIMEOUT)
        .add(&crate::internal::cache::BLOB_CACHE_MEM_LIMIT_BYTES)
        .add(&crate::internal::compact::COMPACTION_MINIMUM_TIMEOUT)
        .add(&crate::internal::machine::NEXT_LISTEN_BATCH_RETRYER_CLAMP)
        .add(&crate::internal::machine::NEXT_LISTEN_BATCH_RETRYER_FIXED_SLEEP)
        .add(&crate::internal::machine::NEXT_LISTEN_BATCH_RETRYER_INITIAL_BACKOFF)
        .add(&crate::internal::machine::NEXT_LISTEN_BATCH_RETRYER_MULTIPLIER)
        .add(&crate::internal::state::ROLLUP_THRESHOLD)
        .add(&crate::operators::PERSIST_SINK_MINIMUM_BATCH_UPDATES)
        .add(&crate::operators::STORAGE_PERSIST_SINK_MINIMUM_BATCH_UPDATES)
        .add(&crate::operators::STORAGE_SOURCE_DECODE_FUEL)
        .add(&crate::read::READER_LEASE_DURATION)
        .add(&crate::rpc::PUBSUB_CLIENT_ENABLED)
        .add(&crate::rpc::PUBSUB_PUSH_DIFF_ENABLED)
        .add(&crate::stats::STATS_AUDIT_PERCENT)
        .add(&crate::stats::STATS_BUDGET_BYTES)
        .add(&crate::stats::STATS_COLLECTION_ENABLED)
        .add(&crate::stats::STATS_FILTER_ENABLED)
        .add(&crate::stats::STATS_UNTRIMMABLE_COLUMNS_EQUALS)
        .add(&crate::stats::STATS_UNTRIMMABLE_COLUMNS_PREFIX)
        .add(&crate::stats::STATS_UNTRIMMABLE_COLUMNS_SUFFIX)
}

impl PersistConfig {
    pub(crate) const DEFAULT_FALLBACK_ROLLUP_THRESHOLD_MULTIPLIER: usize = 3;

    // TODO: Get rid of this in favor of using PersistParameters at the
    // relevant callsites.
    pub fn set_state_versions_recent_live_diffs_limit(&self, val: usize) {
        self.dynamic
            .state_versions_recent_live_diffs_limit
            .store(val, DynamicConfig::STORE_ORDERING);
    }
}

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

impl PostgresClientKnobs for PersistConfig {
    fn connection_pool_max_size(&self) -> usize {
        self.consensus_connection_pool_max_size
    }

    fn connection_pool_max_wait(&self) -> Option<Duration> {
        self.consensus_connection_pool_max_wait
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

/// Persist configurations that can be dynamically updated.
///
/// Persist is expected to react to each of these such that updating the value
/// returned by the function takes effect in persist (i.e. no caching it). This
/// should happen "as promptly as reasonably possible" where that's defined by
/// the tradeoffs of complexity vs promptness. For example, we might use a
/// consistent version of `BLOB_TARGET_SIZE` for the entirety of a single
/// compaction call. However, it should _never_ require a process restart for an
/// update of these to take effect.
///
/// These are hooked up to LaunchDarkly. Specifically, LaunchDarkly configs are
/// serialized into a [PersistParameters]. In environmentd, these are applied
/// directly via [PersistParameters::apply] to the [PersistConfig] in
/// [crate::cache::PersistClientCache]. There is one `PersistClientCache` per
/// process, and every `PersistConfig` shares the same `Arc<DynamicConfig>`, so
/// this affects all [DynamicConfig] usage in the process. The
/// `PersistParameters` is also sent via the compute and storage command
/// streams, which then apply it to all computed/storaged/clusterd processes as
/// well.
#[derive(Debug)]
pub struct DynamicConfig {
    batch_builder_max_outstanding_parts: AtomicUsize,
    compaction_heuristic_min_inputs: AtomicUsize,
    compaction_heuristic_min_parts: AtomicUsize,
    compaction_heuristic_min_updates: AtomicUsize,
    compaction_memory_bound_bytes: AtomicUsize,
    gc_blob_delete_concurrency_limit: AtomicUsize,
    state_versions_recent_live_diffs_limit: AtomicUsize,
    usage_state_fetch_concurrency_limit: AtomicUsize,
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

impl DynamicConfig {
    // TODO: Decide if we can relax these.
    const LOAD_ORDERING: Ordering = Ordering::SeqCst;
    const STORE_ORDERING: Ordering = Ordering::SeqCst;

    /// The maximum number of parts (s3 blobs) that [crate::batch::BatchBuilder]
    /// will pipeline before back-pressuring [crate::batch::BatchBuilder::add]
    /// calls on previous ones finishing.
    pub fn batch_builder_max_outstanding_parts(&self) -> usize {
        self.batch_builder_max_outstanding_parts
            .load(Self::LOAD_ORDERING)
    }

    /// In Compactor::compact_and_apply, we do the compaction (don't skip it)
    /// if the number of inputs is at least this many. Compaction is performed
    /// if any of the heuristic criteria are met (they are OR'd).
    pub fn compaction_heuristic_min_inputs(&self) -> usize {
        self.compaction_heuristic_min_inputs
            .load(Self::LOAD_ORDERING)
    }

    /// In Compactor::compact_and_apply, we do the compaction (don't skip it)
    /// if the number of batch parts is at least this many. Compaction is performed
    /// if any of the heuristic criteria are met (they are OR'd).
    pub fn compaction_heuristic_min_parts(&self) -> usize {
        self.compaction_heuristic_min_parts
            .load(Self::LOAD_ORDERING)
    }

    /// In Compactor::compact_and_apply, we do the compaction (don't skip it)
    /// if the number of updates is at least this many. Compaction is performed
    /// if any of the heuristic criteria are met (they are OR'd).
    pub fn compaction_heuristic_min_updates(&self) -> usize {
        self.compaction_heuristic_min_updates
            .load(Self::LOAD_ORDERING)
    }

    /// The upper bound on compaction's memory consumption. The value must be at
    /// least 4*`blob_target_size`. Increasing this value beyond the minimum allows
    /// compaction to merge together more runs at once, providing greater
    /// consolidation of updates, at the cost of greater memory usage.
    pub fn compaction_memory_bound_bytes(&self) -> usize {
        self.compaction_memory_bound_bytes.load(Self::LOAD_ORDERING)
    }

    /// The maximum number of concurrent blob deletes during garbage collection.
    pub fn gc_blob_delete_concurrency_limit(&self) -> usize {
        self.gc_blob_delete_concurrency_limit
            .load(Self::LOAD_ORDERING)
    }

    /// The # of diffs to initially scan when fetching the latest consensus state, to
    /// determine which requests go down the fast vs slow path. Should be large enough
    /// to fetch all live diffs in the steady-state, and small enough to query Consensus
    /// at high volume. Steady-state usage should accommodate readers that require
    /// seqno-holds for reasonable amounts of time, which to start we say is 10s of minutes.
    ///
    /// This value ought to be defined in terms of `NEED_ROLLUP_THRESHOLD` to approximate
    /// when we expect rollups to be written and therefore when old states will be truncated
    /// by GC.
    pub fn state_versions_recent_live_diffs_limit(&self) -> usize {
        self.state_versions_recent_live_diffs_limit
            .load(Self::LOAD_ORDERING)
    }

    /// The maximum number of concurrent state fetches during usage computation.
    pub fn usage_state_fetch_concurrency_limit(&self) -> usize {
        self.usage_state_fetch_concurrency_limit
            .load(Self::LOAD_ORDERING)
    }

    // TODO: Get rid of these in favor of using PersistParameters at the
    // relevant callsites.
    #[cfg(test)]
    pub fn set_batch_builder_max_outstanding_parts(&self, val: usize) {
        self.batch_builder_max_outstanding_parts
            .store(val, Self::LOAD_ORDERING);
    }
    pub fn set_compaction_memory_bound_bytes(&self, val: usize) {
        self.compaction_memory_bound_bytes
            .store(val, Self::LOAD_ORDERING);
    }
}

// TODO: Replace with dynamic values when PersistConfig is integrated with LD
impl BlobKnobs for PersistConfig {
    fn operation_timeout(&self) -> Duration {
        Duration::from_secs(180)
    }

    fn operation_attempt_timeout(&self) -> Duration {
        Duration::from_secs(90)
    }

    fn connect_timeout(&self) -> Duration {
        Duration::from_secs(7)
    }

    fn read_timeout(&self) -> Duration {
        Duration::from_secs(10)
    }

    fn is_cc_active(&self) -> bool {
        self.is_cc_active
    }
}

/// Updates to values in [PersistConfig].
///
/// These reflect updates made to LaunchDarkly. They're passed from environmentd
/// through storage and compute commands and applied to PersistConfig to change
/// its values.
///
/// Parameters can be set (`Some`) or unset (`None`). Unset parameters should be
/// interpreted to mean "use the previous value".
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize, Arbitrary)]
pub struct PersistParameters {
    /// Updates to various dynamic config values.
    pub config_updates: ConfigUpdates,
}

impl PersistParameters {
    /// Update the parameter values with the set ones from `other`.
    pub fn update(&mut self, other: PersistParameters) {
        // Deconstruct self and other so we get a compile failure if new fields
        // are added.
        let Self {
            config_updates: self_config_updates,
        } = self;
        let Self {
            config_updates: other_config_updates,
        } = other;
        self_config_updates.extend(other_config_updates);
    }

    /// Return whether all parameters are unset.
    pub fn all_unset(&self) -> bool {
        // TODO: Where is this called? We could save a tiny bit of boilerplate
        // by comparing self to Self::default().
        //
        // Deconstruct self so we get a compile failure if new fields are added.
        let Self { config_updates } = self;
        config_updates.updates.is_empty()
    }

    /// Applies the parameter values to persist's in-memory config object.
    ///
    /// Note that these overrides are not all applied atomically: i.e. it's
    /// possible for persist to race with this and see some but not all of the
    /// parameters applied.
    pub fn apply(&self, cfg: &PersistConfig) {
        // Deconstruct self so we get a compile failure if new fields are added.
        let Self { config_updates } = self;
        config_updates.apply(&cfg.configs);
    }
}

impl RustType<ConfigUpdates> for PersistParameters {
    fn into_proto(&self) -> ConfigUpdates {
        self.config_updates.clone()
    }

    fn from_proto(config_updates: ConfigUpdates) -> Result<Self, TryFromProtoError> {
        Ok(Self { config_updates })
    }
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
pub fn check_data_version(code_version: &Version, data_version: &Version) -> Result<(), String> {
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

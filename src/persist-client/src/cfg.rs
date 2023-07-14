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

use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, RwLock};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use mz_build_info::BuildInfo;
use mz_ore::now::NowFn;
use mz_persist::cfg::{BlobKnobs, ConsensusKnobs};
use mz_persist::retry::Retry;
use mz_proto::{IntoRustIfSome, ProtoType, RustType, TryFromProtoError};
use proptest_derive::Arbitrary;
use semver::Version;
use serde::{Deserialize, Serialize};

include!(concat!(env!("OUT_DIR"), "/mz_persist_client.cfg.rs"));

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
    pub(crate) build_version: Version,
    /// Hostname of this persist user. Stored in state and used for debugging.
    pub hostname: String,
    /// A clock to use for all leasing and other non-debugging use.
    pub now: NowFn,
    /// Configurations that can be dynamically updated.
    pub(crate) dynamic: Arc<DynamicConfig>,
    /// Whether to physically and logically compact batches in blob storage.
    pub compaction_enabled: bool,
    /// In Compactor::compact_and_apply_background, the maximum number of concurrent
    /// compaction requests that can execute for a given shard.
    pub compaction_concurrency_limit: usize,
    /// In Compactor::compact_and_apply_background, the maximum number of pending
    /// compaction requests to queue.
    pub compaction_queue_size: usize,
    /// The maximum size of the connection pool to Postgres/CRDB when performing
    /// consensus reads and writes.
    pub consensus_connection_pool_max_size: usize,
    /// Length of time after a writer's last operation after which the writer
    /// may be expired.
    pub writer_lease_duration: Duration,
    /// Length of time after a reader's last operation after which the reader
    /// may be expired.
    pub reader_lease_duration: Duration,
    /// Length of time between critical handles' calls to downgrade since
    pub critical_downgrade_interval: Duration,
    /// Timeout per connection attempt to Persist PubSub service.
    pub pubsub_connect_attempt_timeout: Duration,
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
}

impl PersistConfig {
    /// Returns a new instance of [PersistConfig] with default tuning.
    pub fn new(build_info: &BuildInfo, now: NowFn) -> Self {
        // Escape hatch in case we need to disable compaction.
        let compaction_disabled = mz_ore::env::is_var_truthy("MZ_PERSIST_COMPACTION_DISABLED");
        Self {
            build_version: build_info.semver_version(),
            now,
            dynamic: Arc::new(DynamicConfig {
                batch_builder_max_outstanding_parts: AtomicUsize::new(2),
                blob_target_size: AtomicUsize::new(Self::DEFAULT_BLOB_TARGET_SIZE),
                blob_cache_mem_limit_bytes: AtomicUsize::new(
                    Self::DEFAULT_BLOB_CACHE_MEM_LIMIT_BYTES,
                ),
                compaction_heuristic_min_inputs: AtomicUsize::new(8),
                compaction_heuristic_min_parts: AtomicUsize::new(8),
                compaction_heuristic_min_updates: AtomicUsize::new(1024),
                compaction_memory_bound_bytes: AtomicUsize::new(1024 * MB),
                compaction_minimum_timeout: RwLock::new(Self::DEFAULT_COMPACTION_MINIMUM_TIMEOUT),
                consensus_connection_pool_ttl: RwLock::new(Self::DEFAULT_CONSENSUS_CONNPOOL_TTL),
                consensus_connection_pool_ttl_stagger: RwLock::new(
                    Self::DEFAULT_CONSENSUS_CONNPOOL_TTL_STAGGER,
                ),
                consensus_connect_timeout: RwLock::new(Self::DEFAULT_CRDB_CONNECT_TIMEOUT),
                consensus_tcp_user_timeout: RwLock::new(Self::DEFAULT_CRDB_TCP_USER_TIMEOUT),
                gc_blob_delete_concurrency_limit: AtomicUsize::new(32),
                state_versions_recent_live_diffs_limit: AtomicUsize::new(
                    30 * Self::DEFAULT_ROLLUP_THRESHOLD,
                ),
                usage_state_fetch_concurrency_limit: AtomicUsize::new(8),
                sink_minimum_batch_updates: AtomicUsize::new(
                    Self::DEFAULT_SINK_MINIMUM_BATCH_UPDATES,
                ),
                storage_sink_minimum_batch_updates: AtomicUsize::new(
                    Self::DEFAULT_SINK_MINIMUM_BATCH_UPDATES,
                ),
                next_listen_batch_retryer: RwLock::new(Self::DEFAULT_NEXT_LISTEN_BATCH_RETRYER),
                stats_audit_percent: AtomicUsize::new(Self::DEFAULT_STATS_AUDIT_PERCENT),
                stats_collection_enabled: AtomicBool::new(Self::DEFAULT_STATS_COLLECTION_ENABLED),
                stats_filter_enabled: AtomicBool::new(Self::DEFAULT_STATS_FILTER_ENABLED),
                pubsub_client_enabled: AtomicBool::new(Self::DEFAULT_PUBSUB_CLIENT_ENABLED),
                pubsub_push_diff_enabled: AtomicBool::new(Self::DEFAULT_PUBSUB_PUSH_DIFF_ENABLED),
                rollup_threshold: AtomicUsize::new(Self::DEFAULT_ROLLUP_THRESHOLD),
            }),
            compaction_enabled: !compaction_disabled,
            compaction_concurrency_limit: 5,
            compaction_queue_size: 20,
            consensus_connection_pool_max_size: 50,
            writer_lease_duration: 60 * Duration::from_secs(60),
            reader_lease_duration: Self::DEFAULT_READ_LEASE_DURATION,
            critical_downgrade_interval: Duration::from_secs(30),
            pubsub_connect_attempt_timeout: Duration::from_secs(5),
            pubsub_connect_max_backoff: Duration::from_secs(60),
            pubsub_client_sender_channel_size: 25,
            pubsub_client_receiver_channel_size: 25,
            pubsub_server_connection_channel_size: 25,
            pubsub_state_cache_shard_ref_channel_size: 25,
            pubsub_reconnect_backoff: Duration::from_secs(5),
            // TODO: This doesn't work with the process orchestrator. Instead,
            // separate --log-prefix into --service-name and --enable-log-prefix
            // options, where the first is always provided and the second is
            // conditionally enabled by the process orchestrator.
            hostname: std::env::var("HOSTNAME").unwrap_or_else(|_| "unknown".to_owned()),
        }
    }

    /// The minimum number of updates that justify writing out a batch in `persist_sink`'s
    /// `write_batches` operator. (If there are fewer than this minimum number of updates,
    /// they'll be forwarded on to `append_batch` to be combined and written there.)
    pub fn sink_minimum_batch_updates(&self) -> usize {
        self.dynamic
            .sink_minimum_batch_updates
            .load(DynamicConfig::LOAD_ORDERING)
    }

    /// The same as `Self::sink_minimum_batch_updates`, but
    /// for storage `persist_sink`'s.
    pub fn storage_sink_minimum_batch_updates(&self) -> usize {
        self.dynamic
            .storage_sink_minimum_batch_updates
            .load(DynamicConfig::LOAD_ORDERING)
    }

    /// Returns a new instance of [PersistConfig] for tests.
    #[cfg(test)]
    pub fn new_for_tests() -> Self {
        use mz_build_info::DUMMY_BUILD_INFO;
        use mz_ore::now::SYSTEM_TIME;

        let mut cfg = Self::new(&DUMMY_BUILD_INFO, SYSTEM_TIME.clone());
        cfg.hostname = "tests".into();
        cfg
    }
}

pub(crate) const MB: usize = 1024 * 1024;

impl PersistConfig {
    /// Default value for [`DynamicConfig::blob_target_size`].
    pub const DEFAULT_BLOB_TARGET_SIZE: usize = 128 * MB;
    /// Default value for [`DynamicConfig::compaction_minimum_timeout`].
    pub const DEFAULT_COMPACTION_MINIMUM_TIMEOUT: Duration = Duration::from_secs(90);
    /// Default value for [`DynamicConfig::consensus_connect_timeout`].
    pub const DEFAULT_CRDB_CONNECT_TIMEOUT: Duration = Duration::from_secs(5);
    /// Default value for [`DynamicConfig::consensus_tcp_user_timeout`].
    pub const DEFAULT_CRDB_TCP_USER_TIMEOUT: Duration = Duration::from_secs(30);
    /// Default value for [`DynamicConfig::consensus_connection_pool_ttl`].
    pub const DEFAULT_CONSENSUS_CONNPOOL_TTL: Duration = Duration::from_secs(300);
    /// Default value for [`DynamicConfig::consensus_connection_pool_ttl_stagger`].
    pub const DEFAULT_CONSENSUS_CONNPOOL_TTL_STAGGER: Duration = Duration::from_secs(6);
    /// Default value for [`DynamicConfig::stats_audit_percent`].
    pub const DEFAULT_STATS_AUDIT_PERCENT: usize = 0;
    /// Default value for [`DynamicConfig::stats_collection_enabled`].
    pub const DEFAULT_STATS_COLLECTION_ENABLED: bool = false;
    /// Default value for [`DynamicConfig::stats_filter_enabled`].
    pub const DEFAULT_STATS_FILTER_ENABLED: bool = false;
    /// Default value for [`DynamicConfig::pubsub_client_enabled`].
    pub const DEFAULT_PUBSUB_CLIENT_ENABLED: bool = true;
    /// Default value for [`DynamicConfig::pubsub_push_diff_enabled`].
    pub const DEFAULT_PUBSUB_PUSH_DIFF_ENABLED: bool = true;
    /// Default value for [`DynamicConfig::rollup_threshold`].
    pub const DEFAULT_ROLLUP_THRESHOLD: usize = 128;

    /// Default value for [`PersistConfig::sink_minimum_batch_updates`].
    pub const DEFAULT_SINK_MINIMUM_BATCH_UPDATES: usize = 0;

    /// Default value for [`DynamicConfig::next_listen_batch_retry_params`].
    pub const DEFAULT_NEXT_LISTEN_BATCH_RETRYER: RetryParameters = RetryParameters {
        initial_backoff: Duration::from_millis(4),
        multiplier: 2,
        clamp: Duration::from_secs(16),
    };

    pub(crate) const DEFAULT_FALLBACK_ROLLUP_THRESHOLD_MULTIPLIER: usize = 3;

    /// Default value for [`DynamicConfig::blob_cache_mem_limit_bytes`].
    ///
    /// This initial value was tuned via a one-time experiment that showed an
    /// environment running our demo "auction" source + mv got 90%+ cache hits
    /// with a 1 MiB cache. This doesn't scale up to prod data sizes and doesn't
    /// help with multi-process replicas, but the memory usage seems
    /// unobjectionable enough to have it for the cases that it does help.
    pub const DEFAULT_BLOB_CACHE_MEM_LIMIT_BYTES: usize = 1024 * 1024;

    // Move this to a PersistConfig field when we actually have read leases.
    //
    // MIGRATION: Remove this once we remove the ReaderState <->
    // ProtoReaderState migration.
    pub(crate) const DEFAULT_READ_LEASE_DURATION: Duration = Duration::from_secs(60 * 15);

    // TODO: Get rid of this in favor of using PersistParameters at the
    // relevant callsites.
    pub fn set_state_versions_recent_live_diffs_limit(&self, val: usize) {
        self.dynamic
            .state_versions_recent_live_diffs_limit
            .store(val, DynamicConfig::STORE_ORDERING);
    }
}

impl ConsensusKnobs for PersistConfig {
    fn connection_pool_max_size(&self) -> usize {
        self.consensus_connection_pool_max_size
    }

    fn connection_pool_ttl(&self) -> Duration {
        self.dynamic.consensus_connection_pool_ttl()
    }

    fn connection_pool_ttl_stagger(&self) -> Duration {
        self.dynamic.consensus_connection_pool_ttl_stagger()
    }

    fn connect_timeout(&self) -> Duration {
        *self
            .dynamic
            .consensus_connect_timeout
            .read()
            .expect("lock poisoned")
    }

    fn tcp_user_timeout(&self) -> Duration {
        *self
            .dynamic
            .consensus_tcp_user_timeout
            .read()
            .expect("lock poisoned")
    }
}

/// Persist configurations that can be dynamically updated.
///
/// Persist is expected to react to each of these such that updating the value
/// returned by the function takes effect in persist (i.e. no caching it). This
/// should happen "as promptly as reasonably possible" where that's defined by
/// the tradeoffs of complexity vs promptness. For example, we might use a
/// consistent version of [Self::blob_target_size] for the entirety of a single
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
    blob_target_size: AtomicUsize,
    blob_cache_mem_limit_bytes: AtomicUsize,
    compaction_heuristic_min_inputs: AtomicUsize,
    compaction_heuristic_min_parts: AtomicUsize,
    compaction_heuristic_min_updates: AtomicUsize,
    compaction_memory_bound_bytes: AtomicUsize,
    compaction_minimum_timeout: RwLock<Duration>,
    gc_blob_delete_concurrency_limit: AtomicUsize,
    state_versions_recent_live_diffs_limit: AtomicUsize,
    usage_state_fetch_concurrency_limit: AtomicUsize,
    consensus_connect_timeout: RwLock<Duration>,
    consensus_tcp_user_timeout: RwLock<Duration>,
    consensus_connection_pool_ttl: RwLock<Duration>,
    consensus_connection_pool_ttl_stagger: RwLock<Duration>,
    sink_minimum_batch_updates: AtomicUsize,
    storage_sink_minimum_batch_updates: AtomicUsize,
    stats_audit_percent: AtomicUsize,
    stats_collection_enabled: AtomicBool,
    stats_filter_enabled: AtomicBool,
    pubsub_client_enabled: AtomicBool,
    pubsub_push_diff_enabled: AtomicBool,
    rollup_threshold: AtomicUsize,

    // NB: These parameters are not atomically updated together in LD.
    // We put them under a single RwLock to reduce the cost of reads
    // and to logically group them together.
    next_listen_batch_retryer: RwLock<RetryParameters>,
}

#[derive(Copy, Clone, Debug, Eq, PartialEq, Arbitrary, Serialize, Deserialize)]
pub struct RetryParameters {
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
            initial_backoff: self.initial_backoff,
            multiplier: self.multiplier,
            clamp_backoff: self.clamp,
            seed,
        }
    }
}

impl RustType<ProtoRetryParameters> for RetryParameters {
    fn into_proto(&self) -> ProtoRetryParameters {
        ProtoRetryParameters {
            initial_backoff: Some(self.initial_backoff.into_proto()),
            multiplier: self.multiplier,
            clamp: Some(self.clamp.into_proto()),
        }
    }

    fn from_proto(proto: ProtoRetryParameters) -> Result<Self, TryFromProtoError> {
        Ok(Self {
            initial_backoff: proto
                .initial_backoff
                .into_rust_if_some("ProtoRetryParameters::initial_backoff")?,
            multiplier: proto.multiplier.into_rust()?,
            clamp: proto
                .clamp
                .into_rust_if_some("ProtoRetryParameters::clamp")?,
        })
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

    /// A target maximum size of blob payloads in bytes. If a logical "batch" is
    /// bigger than this, it will be broken up into smaller, independent pieces.
    /// This is best-effort, not a guarantee (though as of 2022-06-09, we happen
    /// to always respect it). This target size doesn't apply for an individual
    /// update that exceeds it in size, but that scenario is almost certainly a
    /// mis-use of the system.
    pub fn blob_target_size(&self) -> usize {
        self.blob_target_size.load(Self::LOAD_ORDERING)
    }

    /// Capacity of in-mem blob cache in bytes.
    pub fn blob_cache_mem_limit_bytes(&self) -> usize {
        self.blob_cache_mem_limit_bytes.load(Self::LOAD_ORDERING)
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

    /// In Compactor::compact_and_apply_background, the minimum amount of time to
    /// allow a compaction request to run before timing it out. A request may be
    /// given a timeout greater than this value depending on the inputs' size
    pub fn compaction_minimum_timeout(&self) -> Duration {
        *self
            .compaction_minimum_timeout
            .read()
            .expect("lock poisoned")
    }

    /// The minimum TTL of a connection to Postgres/CRDB before it is proactively
    /// terminated. Connections are routinely culled to balance load against the
    /// downstream database.
    pub fn consensus_connection_pool_ttl(&self) -> Duration {
        *self
            .consensus_connection_pool_ttl
            .read()
            .expect("lock poisoned")
    }
    /// The minimum time between TTLing connections to Postgres/CRDB. This delay is
    /// used to stagger reconnections to avoid stampedes and high tail latencies.
    /// This value should be much less than `consensus_connection_pool_ttl` so that
    /// reconnections are biased towards terminating the oldest connections first.
    /// A value of `consensus_connection_pool_ttl / consensus_connection_pool_max_size`
    /// is likely a good place to start so that all connections are rotated when the
    /// pool is fully used.
    pub fn consensus_connection_pool_ttl_stagger(&self) -> Duration {
        *self
            .consensus_connection_pool_ttl_stagger
            .read()
            .expect("lock poisoned")
    }
    /// The duration to wait for a Consensus Postgres/CRDB connection to be made before retrying.
    pub fn consensus_connect_timeout(&self) -> Duration {
        *self
            .consensus_connect_timeout
            .read()
            .expect("lock poisoned")
    }

    /// The TCP user timeout for a Consensus Postgres/CRDB connection. Specifies the amount
    /// of time that transmitted data may remain unacknowledged before the TCP connection is
    /// forcibly closed.
    pub fn consensus_tcp_user_timeout(&self) -> Duration {
        *self
            .consensus_tcp_user_timeout
            .read()
            .expect("lock poisoned")
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

    /// Percent of filtered data to opt in to correctness auditing.
    pub fn stats_audit_percent(&self) -> usize {
        self.stats_audit_percent.load(Self::LOAD_ORDERING)
    }

    /// Computes and stores statistics about each batch part.
    ///
    /// These can be used at read time to entirely skip fetching a part based on
    /// its statistics. See [Self::stats_filter_enabled].
    pub fn stats_collection_enabled(&self) -> bool {
        self.stats_collection_enabled.load(Self::LOAD_ORDERING)
    }

    /// Uses previously computed statistics about batch parts to entirely skip
    /// fetching them at read time.
    ///
    /// See [Self::stats_collection_enabled].
    pub fn stats_filter_enabled(&self) -> bool {
        self.stats_filter_enabled.load(Self::LOAD_ORDERING)
    }

    /// Determines whether PubSub clients should connect to the PubSub server.
    pub fn pubsub_client_enabled(&self) -> bool {
        self.pubsub_client_enabled.load(Self::LOAD_ORDERING)
    }

    /// For connected clients, determines whether to push state diffs to the PubSub server.
    /// For the server, determines whether to broadcast state diffs to subscribed clients.
    pub fn pubsub_push_diff_enabled(&self) -> bool {
        self.pubsub_push_diff_enabled.load(Self::LOAD_ORDERING)
    }

    /// Determines how often to write rollups, assigning a maintenance task
    /// after `rollup_threshold` seqnos have passed since the last rollup.
    ///
    /// Tuning note: in the absence of a long reader seqno hold, and with
    /// incremental GC, this threshold will determine about how many live
    /// diffs are held in Consensus. Lowering this value decreases the live
    /// diff count at the cost of more maintenance work + blob writes.
    pub fn rollup_threshold(&self) -> usize {
        self.rollup_threshold.load(Self::LOAD_ORDERING)
    }

    /// The maximum number of concurrent state fetches during usage computation.
    pub fn usage_state_fetch_concurrency_limit(&self) -> usize {
        self.usage_state_fetch_concurrency_limit
            .load(Self::LOAD_ORDERING)
    }

    /// Retry configuration for `next_listen_batch`.
    pub fn next_listen_batch_retry_params(&self) -> RetryParameters {
        *self
            .next_listen_batch_retryer
            .read()
            .expect("lock poisoned")
    }

    // TODO: Get rid of these in favor of using PersistParameters at the
    // relevant callsites.
    #[cfg(test)]
    pub fn set_blob_target_size(&self, val: usize) {
        self.blob_target_size.store(val, Self::LOAD_ORDERING);
    }
    #[cfg(test)]
    pub fn set_batch_builder_max_outstanding_parts(&self, val: usize) {
        self.batch_builder_max_outstanding_parts
            .store(val, Self::LOAD_ORDERING);
    }
    pub fn set_compaction_memory_bound_bytes(&self, val: usize) {
        self.compaction_memory_bound_bytes
            .store(val, Self::LOAD_ORDERING);
    }
    #[cfg(test)]
    pub fn set_rollup_threshold(&self, threshold: usize) {
        self.rollup_threshold.store(threshold, Self::STORE_ORDERING);
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
    /// Configures [`DynamicConfig::blob_target_size`].
    pub blob_target_size: Option<usize>,
    /// Configures [`DynamicConfig::blob_cache_mem_limit_bytes`].
    pub blob_cache_mem_limit_bytes: Option<usize>,
    /// Configures [`DynamicConfig::compaction_minimum_timeout`].
    pub compaction_minimum_timeout: Option<Duration>,
    /// Configures [`DynamicConfig::consensus_connect_timeout`].
    pub consensus_connect_timeout: Option<Duration>,
    /// Configures [`DynamicConfig::consensus_tcp_user_timeout`].
    pub consensus_tcp_user_timeout: Option<Duration>,
    /// Configures [`DynamicConfig::consensus_connection_pool_ttl`].
    pub consensus_connection_pool_ttl: Option<Duration>,
    /// Configures [`DynamicConfig::consensus_connection_pool_ttl_stagger`].
    pub consensus_connection_pool_ttl_stagger: Option<Duration>,
    /// Configures [`DynamicConfig::next_listen_batch_retry_params`].
    pub next_listen_batch_retryer: Option<RetryParameters>,
    /// Configures [`PersistConfig::sink_minimum_batch_updates`].
    pub sink_minimum_batch_updates: Option<usize>,
    /// Configures [`PersistConfig::storage_sink_minimum_batch_updates`].
    pub storage_sink_minimum_batch_updates: Option<usize>,
    /// Configures [`DynamicConfig::stats_audit_percent`].
    pub stats_audit_percent: Option<usize>,
    /// Configures [`DynamicConfig::stats_collection_enabled`].
    pub stats_collection_enabled: Option<bool>,
    /// Configures [`DynamicConfig::stats_filter_enabled`].
    pub stats_filter_enabled: Option<bool>,
    /// Configures [`DynamicConfig::pubsub_client_enabled`]
    pub pubsub_client_enabled: Option<bool>,
    /// Configures [`DynamicConfig::pubsub_push_diff_enabled`]
    pub pubsub_push_diff_enabled: Option<bool>,
    /// Configures [`DynamicConfig::rollup_threshold`]
    pub rollup_threshold: Option<usize>,
}

impl PersistParameters {
    /// Update the parameter values with the set ones from `other`.
    pub fn update(&mut self, other: PersistParameters) {
        // Deconstruct self and other so we get a compile failure if new fields
        // are added.
        let Self {
            blob_target_size: self_blob_target_size,
            blob_cache_mem_limit_bytes: self_blob_cache_mem_limit_bytes,
            compaction_minimum_timeout: self_compaction_minimum_timeout,
            consensus_connect_timeout: self_consensus_connect_timeout,
            consensus_tcp_user_timeout: self_consensus_tcp_user_timeout,
            consensus_connection_pool_ttl: self_consensus_connection_pool_ttl,
            consensus_connection_pool_ttl_stagger: self_consensus_connection_pool_ttl_stagger,
            sink_minimum_batch_updates: self_sink_minimum_batch_updates,
            storage_sink_minimum_batch_updates: self_storage_sink_minimum_batch_updates,
            next_listen_batch_retryer: self_next_listen_batch_retryer,
            stats_audit_percent: self_stats_audit_percent,
            stats_collection_enabled: self_stats_collection_enabled,
            stats_filter_enabled: self_stats_filter_enabled,
            pubsub_client_enabled: self_pubsub_client_enabled,
            pubsub_push_diff_enabled: self_pubsub_push_diff_enabled,
            rollup_threshold: self_rollup_threshold,
        } = self;
        let Self {
            blob_target_size: other_blob_target_size,
            blob_cache_mem_limit_bytes: other_blob_cache_mem_limit_bytes,
            compaction_minimum_timeout: other_compaction_minimum_timeout,
            consensus_connect_timeout: other_consensus_connect_timeout,
            consensus_tcp_user_timeout: other_consensus_tcp_user_timeout,
            consensus_connection_pool_ttl: other_consensus_connection_pool_ttl,
            consensus_connection_pool_ttl_stagger: other_consensus_connection_pool_ttl_stagger,
            sink_minimum_batch_updates: other_sink_minimum_batch_updates,
            storage_sink_minimum_batch_updates: other_storage_sink_minimum_batch_updates,
            next_listen_batch_retryer: other_next_listen_batch_retryer,
            stats_audit_percent: other_stats_audit_percent,
            stats_collection_enabled: other_stats_collection_enabled,
            stats_filter_enabled: other_stats_filter_enabled,
            pubsub_client_enabled: other_pubsub_client_enabled,
            pubsub_push_diff_enabled: other_pubsub_push_diff_enabled,
            rollup_threshold: other_rollup_threshold,
        } = other;
        if let Some(v) = other_blob_target_size {
            *self_blob_target_size = Some(v);
        }
        if let Some(v) = other_blob_cache_mem_limit_bytes {
            *self_blob_cache_mem_limit_bytes = Some(v);
        }
        if let Some(v) = other_compaction_minimum_timeout {
            *self_compaction_minimum_timeout = Some(v);
        }
        if let Some(v) = other_consensus_connect_timeout {
            *self_consensus_connect_timeout = Some(v);
        }
        if let Some(v) = other_consensus_tcp_user_timeout {
            *self_consensus_tcp_user_timeout = Some(v);
        }
        if let Some(v) = other_consensus_connection_pool_ttl {
            *self_consensus_connection_pool_ttl = Some(v);
        }
        if let Some(v) = other_consensus_connection_pool_ttl_stagger {
            *self_consensus_connection_pool_ttl_stagger = Some(v);
        }
        if let Some(v) = other_sink_minimum_batch_updates {
            *self_sink_minimum_batch_updates = Some(v);
        }
        if let Some(v) = other_storage_sink_minimum_batch_updates {
            *self_storage_sink_minimum_batch_updates = Some(v);
        }
        if let Some(v) = other_next_listen_batch_retryer {
            *self_next_listen_batch_retryer = Some(v);
        }
        if let Some(v) = other_stats_audit_percent {
            *self_stats_audit_percent = Some(v)
        }
        if let Some(v) = other_stats_collection_enabled {
            *self_stats_collection_enabled = Some(v)
        }
        if let Some(v) = other_stats_filter_enabled {
            *self_stats_filter_enabled = Some(v)
        }
        if let Some(v) = other_pubsub_client_enabled {
            *self_pubsub_client_enabled = Some(v)
        }
        if let Some(v) = other_pubsub_push_diff_enabled {
            *self_pubsub_push_diff_enabled = Some(v)
        }
        if let Some(v) = other_rollup_threshold {
            *self_rollup_threshold = Some(v)
        }
    }

    /// Return whether all parameters are unset.
    pub fn all_unset(&self) -> bool {
        // TODO: Where is this called? We could save a tiny bit of boilerplate
        // by comparing self to Self::default().
        //
        // Deconstruct self so we get a compile failure if new fields are added.
        let Self {
            blob_target_size,
            blob_cache_mem_limit_bytes,
            compaction_minimum_timeout,
            consensus_connect_timeout,
            consensus_tcp_user_timeout,
            consensus_connection_pool_ttl,
            consensus_connection_pool_ttl_stagger,
            sink_minimum_batch_updates,
            storage_sink_minimum_batch_updates,
            next_listen_batch_retryer,
            stats_audit_percent,
            stats_collection_enabled,
            stats_filter_enabled,
            pubsub_client_enabled,
            pubsub_push_diff_enabled,
            rollup_threshold,
        } = self;
        blob_target_size.is_none()
            && blob_cache_mem_limit_bytes.is_none()
            && compaction_minimum_timeout.is_none()
            && consensus_connect_timeout.is_none()
            && consensus_tcp_user_timeout.is_none()
            && consensus_connection_pool_ttl.is_none()
            && consensus_connection_pool_ttl_stagger.is_none()
            && sink_minimum_batch_updates.is_none()
            && storage_sink_minimum_batch_updates.is_none()
            && next_listen_batch_retryer.is_none()
            && stats_audit_percent.is_none()
            && stats_collection_enabled.is_none()
            && stats_filter_enabled.is_none()
            && pubsub_client_enabled.is_none()
            && pubsub_push_diff_enabled.is_none()
            && rollup_threshold.is_none()
    }

    /// Applies the parameter values to persist's in-memory config object.
    ///
    /// Note that these overrides are not all applied atomically: i.e. it's
    /// possible for persist to race with this and see some but not all of the
    /// parameters applied.
    pub fn apply(&self, cfg: &PersistConfig) {
        // Deconstruct self so we get a compile failure if new fields are added.
        let Self {
            blob_target_size,
            blob_cache_mem_limit_bytes,
            compaction_minimum_timeout,
            consensus_connect_timeout,
            consensus_tcp_user_timeout,
            consensus_connection_pool_ttl,
            consensus_connection_pool_ttl_stagger,
            sink_minimum_batch_updates,
            storage_sink_minimum_batch_updates,
            next_listen_batch_retryer,
            stats_audit_percent,
            stats_collection_enabled,
            stats_filter_enabled,
            pubsub_client_enabled,
            pubsub_push_diff_enabled,
            rollup_threshold,
        } = self;
        if let Some(blob_target_size) = blob_target_size {
            cfg.dynamic
                .blob_target_size
                .store(*blob_target_size, DynamicConfig::STORE_ORDERING);
        }
        if let Some(blob_cache_mem_limit_bytes) = blob_cache_mem_limit_bytes {
            cfg.dynamic
                .blob_cache_mem_limit_bytes
                .store(*blob_cache_mem_limit_bytes, DynamicConfig::STORE_ORDERING);
        }
        if let Some(compaction_minimum_timeout) = compaction_minimum_timeout {
            let mut timeout = cfg
                .dynamic
                .compaction_minimum_timeout
                .write()
                .expect("lock poisoned");
            *timeout = *compaction_minimum_timeout;
        }
        if let Some(consensus_connect_timeout) = consensus_connect_timeout {
            let mut timeout = cfg
                .dynamic
                .consensus_connect_timeout
                .write()
                .expect("lock poisoned");
            *timeout = *consensus_connect_timeout;
        }
        if let Some(consensus_tcp_user_timeout) = consensus_tcp_user_timeout {
            let mut timeout = cfg
                .dynamic
                .consensus_tcp_user_timeout
                .write()
                .expect("lock poisoned");
            *timeout = *consensus_tcp_user_timeout;
        }
        if let Some(consensus_connection_pool_ttl) = consensus_connection_pool_ttl {
            let mut ttl = cfg
                .dynamic
                .consensus_connection_pool_ttl
                .write()
                .expect("lock poisoned");
            *ttl = *consensus_connection_pool_ttl;
        }
        if let Some(consensus_connection_pool_ttl_stagger) = consensus_connection_pool_ttl_stagger {
            let mut stagger = cfg
                .dynamic
                .consensus_connection_pool_ttl_stagger
                .write()
                .expect("lock poisoned");
            *stagger = *consensus_connection_pool_ttl_stagger;
        }
        if let Some(sink_minimum_batch_updates) = sink_minimum_batch_updates {
            cfg.dynamic
                .sink_minimum_batch_updates
                .store(*sink_minimum_batch_updates, DynamicConfig::STORE_ORDERING);
        }
        if let Some(storage_sink_minimum_batch_updates) = storage_sink_minimum_batch_updates {
            cfg.dynamic.storage_sink_minimum_batch_updates.store(
                *storage_sink_minimum_batch_updates,
                DynamicConfig::STORE_ORDERING,
            );
        }
        if let Some(retry_params) = next_listen_batch_retryer {
            let mut retry = cfg
                .dynamic
                .next_listen_batch_retryer
                .write()
                .expect("lock poisoned");
            *retry = *retry_params;
        }
        if let Some(stats_audit_percent) = stats_audit_percent {
            cfg.dynamic
                .stats_audit_percent
                .store(*stats_audit_percent, DynamicConfig::STORE_ORDERING);
        }
        if let Some(stats_collection_enabled) = stats_collection_enabled {
            cfg.dynamic
                .stats_collection_enabled
                .store(*stats_collection_enabled, DynamicConfig::STORE_ORDERING);
        }
        if let Some(stats_filter_enabled) = stats_filter_enabled {
            cfg.dynamic
                .stats_filter_enabled
                .store(*stats_filter_enabled, DynamicConfig::STORE_ORDERING);
        }
        if let Some(pubsub_client_enabled) = pubsub_client_enabled {
            cfg.dynamic
                .pubsub_client_enabled
                .store(*pubsub_client_enabled, DynamicConfig::STORE_ORDERING);
        }
        if let Some(pubsub_push_diff_enabled) = pubsub_push_diff_enabled {
            cfg.dynamic
                .pubsub_push_diff_enabled
                .store(*pubsub_push_diff_enabled, DynamicConfig::STORE_ORDERING);
        }
        if let Some(rollup_threshold) = rollup_threshold {
            cfg.dynamic
                .rollup_threshold
                .store(*rollup_threshold, DynamicConfig::STORE_ORDERING);
        }
    }
}

impl RustType<ProtoPersistParameters> for PersistParameters {
    fn into_proto(&self) -> ProtoPersistParameters {
        ProtoPersistParameters {
            blob_target_size: self.blob_target_size.into_proto(),
            blob_cache_mem_limit_bytes: self.blob_cache_mem_limit_bytes.into_proto(),
            compaction_minimum_timeout: self.compaction_minimum_timeout.into_proto(),
            consensus_connect_timeout: self.consensus_connect_timeout.into_proto(),
            consensus_tcp_user_timeout: self.consensus_tcp_user_timeout.into_proto(),
            consensus_connection_pool_ttl: self.consensus_connection_pool_ttl.into_proto(),
            consensus_connection_pool_ttl_stagger: self
                .consensus_connection_pool_ttl_stagger
                .into_proto(),
            sink_minimum_batch_updates: self.sink_minimum_batch_updates.into_proto(),
            storage_sink_minimum_batch_updates: self
                .storage_sink_minimum_batch_updates
                .into_proto(),
            next_listen_batch_retryer: self.next_listen_batch_retryer.into_proto(),
            stats_audit_percent: self.stats_audit_percent.into_proto(),
            stats_collection_enabled: self.stats_collection_enabled.into_proto(),
            stats_filter_enabled: self.stats_filter_enabled.into_proto(),
            pubsub_client_enabled: self.pubsub_client_enabled.into_proto(),
            pubsub_push_diff_enabled: self.pubsub_push_diff_enabled.into_proto(),
            rollup_threshold: self.rollup_threshold.into_proto(),
        }
    }

    fn from_proto(proto: ProtoPersistParameters) -> Result<Self, TryFromProtoError> {
        Ok(Self {
            blob_target_size: proto.blob_target_size.into_rust()?,
            blob_cache_mem_limit_bytes: proto.blob_cache_mem_limit_bytes.into_rust()?,
            compaction_minimum_timeout: proto.compaction_minimum_timeout.into_rust()?,
            consensus_connect_timeout: proto.consensus_connect_timeout.into_rust()?,
            consensus_tcp_user_timeout: proto.consensus_tcp_user_timeout.into_rust()?,
            consensus_connection_pool_ttl: proto.consensus_connection_pool_ttl.into_rust()?,
            consensus_connection_pool_ttl_stagger: proto
                .consensus_connection_pool_ttl_stagger
                .into_rust()?,
            sink_minimum_batch_updates: proto.sink_minimum_batch_updates.into_rust()?,
            storage_sink_minimum_batch_updates: proto
                .storage_sink_minimum_batch_updates
                .into_rust()?,
            next_listen_batch_retryer: proto.next_listen_batch_retryer.into_rust()?,
            stats_audit_percent: proto.stats_audit_percent.into_rust()?,
            stats_collection_enabled: proto.stats_collection_enabled.into_rust()?,
            stats_filter_enabled: proto.stats_filter_enabled.into_rust()?,
            pubsub_client_enabled: proto.pubsub_client_enabled.into_rust()?,
            pubsub_push_diff_enabled: proto.pubsub_push_diff_enabled.into_rust()?,
            rollup_threshold: proto.rollup_threshold.into_rust()?,
        })
    }
}

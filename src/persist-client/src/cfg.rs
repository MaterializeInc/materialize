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

use std::time::Duration;

use mz_build_info::BuildInfo;
use mz_ore::cast::CastFrom;
use mz_ore::now::NowFn;
use mz_persist::cfg::{BlobKnobs, ConsensusKnobs};
use mz_proto::{ProtoType, RustType, TryFromProtoError};
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
#[derive(Debug, Clone)]
pub struct PersistConfig {
    /// Info about which version of the code is running.
    pub(crate) build_version: Version,
    /// A clock to use for all leasing and other non-debugging use.
    pub now: NowFn,
    /// A target maximum size of blob payloads in bytes. If a logical "batch" is
    /// bigger than this, it will be broken up into smaller, independent pieces.
    /// This is best-effort, not a guarantee (though as of 2022-06-09, we happen
    /// to always respect it). This target size doesn't apply for an individual
    /// update that exceeds it in size, but that scenario is almost certainly a
    /// mis-use of the system.
    pub blob_target_size: usize,
    /// The maximum number of parts (s3 blobs) that [crate::batch::BatchBuilder]
    /// will pipeline before back-pressuring [crate::batch::BatchBuilder::add]
    /// calls on previous ones finishing.
    pub batch_builder_max_outstanding_parts: usize,
    /// Whether to physically and logically compact batches in blob storage.
    pub compaction_enabled: bool,
    /// The upper bound on compaction's memory consumption. The value must be at
    /// least 4*`blob_target_size`. Increasing this value beyond the minimum allows
    /// compaction to merge together more runs at once, providing greater
    /// consolidation of updates, at the cost of greater memory usage.
    pub compaction_memory_bound_bytes: usize,
    /// In Compactor::compact_and_apply, we do the compaction (don't skip it)
    /// if the number of inputs is at least this many. Compaction is performed
    /// if any of the heuristic criteria are met (they are OR'd).
    pub compaction_heuristic_min_inputs: usize,
    /// In Compactor::compact_and_apply, we do the compaction (don't skip it)
    /// if the number of batch parts is at least this many. Compaction is performed
    /// if any of the heuristic criteria are met (they are OR'd).
    pub compaction_heuristic_min_parts: usize,
    /// In Compactor::compact_and_apply, we do the compaction (don't skip it)
    /// if the number of updates is at least this many. Compaction is performed
    /// if any of the heuristic criteria are met (they are OR'd).
    pub compaction_heuristic_min_updates: usize,
    /// In Compactor::compact_and_apply_background, the maximum number of concurrent
    /// compaction requests that can execute for a given shard.
    pub compaction_concurrency_limit: usize,
    /// In Compactor::compact_and_apply_background, the maximum number of pending
    /// compaction requests to queue.
    pub compaction_queue_size: usize,
    /// The maximum number of concurrent blob deletes during garbage collection.
    pub gc_blob_delete_concurrency_limit: usize,
    /// In Compactor::compact_and_apply_background, the minimum amount of time to
    /// allow a compaction request to run before timing it out. A request may be
    /// given a timeout greater than this value depending on the inputs' size
    pub compaction_minimum_timeout: Duration,
    /// The maximum size of the connection pool to Postgres/CRDB when performing
    /// consensus reads and writes.
    pub consensus_connection_pool_max_size: usize,
    /// The minimum TTL of a connection to Postgres/CRDB before it is proactively
    /// terminated. Connections are routinely culled to balance load against the
    /// downstream database.
    pub consensus_connection_pool_ttl: Duration,
    /// The minimum time between TTLing connections to Postgres/CRDB. This delay is
    /// used to stagger reconnections to avoid stampedes and high tail latencies.
    /// This value should be much less than `consensus_connection_pool_ttl` so that
    /// reconnections are biased towards terminating the oldest connections first.
    /// A value of `consensus_connection_pool_ttl / consensus_connection_pool_max_size`
    /// is likely a good place to start so that all connections are rotated when the
    /// pool is fully used.
    pub consensus_connection_pool_ttl_stagger: Duration,
    /// The # of diffs to initially scan when fetching the latest consensus state, to
    /// determine which requests go down the fast vs slow path. Should be large enough
    /// to fetch all live diffs in the steady-state, and small enough to query Consensus
    /// at high volume. Steady-state usage should accommodate readers that require
    /// seqno-holds for reasonable amounts of time, which to start we say is 10s of minutes.
    ///
    /// This value ought to be defined in terms of `NEED_ROLLUP_THRESHOLD` to approximate
    /// when we expect rollups to be written and therefore when old states will be truncated
    /// by GC.
    pub state_versions_recent_live_diffs_limit: usize,
    /// Length of time after a writer's last operation after which the writer
    /// may be expired.
    pub writer_lease_duration: Duration,
    /// Length of time after a reader's last operation after which the reader
    /// may be expired.
    pub reader_lease_duration: Duration,
    /// Length of time between critical handles' calls to downgrade since
    pub critical_downgrade_interval: Duration,
    /// Hostname of this persist user. Stored in state and used for debugging.
    pub hostname: String,
}

impl PersistConfig {
    /// Returns a new instance of [PersistConfig] with default tuning.
    pub fn new(build_info: &BuildInfo, now: NowFn) -> Self {
        // Escape hatch in case we need to disable compaction.
        let compaction_disabled = mz_ore::env::is_var_truthy("MZ_PERSIST_COMPACTION_DISABLED");
        Self {
            build_version: build_info.semver_version(),
            now,
            blob_target_size: Self::DEFAULT_BLOB_TARGET_SIZE,
            batch_builder_max_outstanding_parts: 2,
            compaction_enabled: !compaction_disabled,
            compaction_memory_bound_bytes: 1024 * MB,
            compaction_heuristic_min_inputs: 8,
            compaction_heuristic_min_parts: 8,
            compaction_heuristic_min_updates: 1024,
            compaction_concurrency_limit: 5,
            compaction_queue_size: 20,
            gc_blob_delete_concurrency_limit: 32,
            compaction_minimum_timeout: Self::DEFAULT_COMPACTION_MINIMUM_TIMEOUT,
            consensus_connection_pool_max_size: 50,
            consensus_connection_pool_ttl: Duration::from_secs(300),
            consensus_connection_pool_ttl_stagger: Duration::from_secs(6),
            state_versions_recent_live_diffs_limit: usize::cast_from(
                30 * Self::NEED_ROLLUP_THRESHOLD,
            ),
            writer_lease_duration: 60 * Duration::from_secs(60),
            reader_lease_duration: Self::DEFAULT_READ_LEASE_DURATION,
            critical_downgrade_interval: Duration::from_secs(30),
            // TODO: This doesn't work with the process orchestrator. Instead,
            // separate --log-prefix into --service-name and --enable-log-prefix
            // options, where the first is always provided and the second is
            // conditionally enabled by the process orchestrator.
            hostname: std::env::var("HOSTNAME").unwrap_or_else(|_| "unknown".to_owned()),
        }
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
    /// Default value for [`PersistConfig::blob_target_size`].
    pub const DEFAULT_BLOB_TARGET_SIZE: usize = 128 * MB;
    /// Default value for [`PersistConfig::compaction_minimum_timeout`].
    pub const DEFAULT_COMPACTION_MINIMUM_TIMEOUT: Duration = Duration::from_secs(90);

    // Move this to a PersistConfig field when we actually have read leases.
    //
    // MIGRATION: Remove this once we remove the ReaderState <->
    // ProtoReaderState migration.
    pub(crate) const DEFAULT_READ_LEASE_DURATION: Duration = Duration::from_secs(60 * 15);

    // Tuning notes: Picked arbitrarily.
    pub(crate) const NEED_ROLLUP_THRESHOLD: u64 = 128;
}

impl ConsensusKnobs for PersistConfig {
    fn connection_pool_max_size(&self) -> usize {
        self.consensus_connection_pool_max_size
    }

    fn connection_pool_ttl(&self) -> Duration {
        self.consensus_connection_pool_ttl
    }

    fn connection_pool_ttl_stagger(&self) -> Duration {
        self.consensus_connection_pool_ttl_stagger
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
    /// Configures [`PersistConfig::blob_target_size`].
    pub blob_target_size: Option<usize>,
    /// Configures [`PersistConfig::compaction_minimum_timeout`].
    pub compaction_minimum_timeout: Option<Duration>,
}

impl PersistParameters {
    /// Update the parameter values with the set ones from `other`.
    pub fn update(&mut self, other: PersistParameters) {
        if let Some(v) = other.blob_target_size {
            self.blob_target_size = Some(v);
        }
        if let Some(v) = other.compaction_minimum_timeout {
            self.compaction_minimum_timeout = Some(v);
        }
    }

    /// Return whether all parameters are unset.
    pub fn all_unset(&self) -> bool {
        self.blob_target_size.is_none() && self.compaction_minimum_timeout.is_none()
    }
}

impl RustType<ProtoPersistParameters> for PersistParameters {
    fn into_proto(&self) -> ProtoPersistParameters {
        ProtoPersistParameters {
            blob_target_size: self.blob_target_size.into_proto(),
            compaction_minimum_timeout: self.compaction_minimum_timeout.into_proto(),
        }
    }

    fn from_proto(proto: ProtoPersistParameters) -> Result<Self, TryFromProtoError> {
        Ok(Self {
            blob_target_size: proto.blob_target_size.into_rust()?,
            compaction_minimum_timeout: proto.compaction_minimum_timeout.into_rust()?,
        })
    }
}

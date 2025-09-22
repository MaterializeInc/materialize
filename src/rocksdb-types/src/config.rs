// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

// This module is mostly boilerplate, with all relevant
// documentation on `RocksDBTuningParameters`.
#![allow(missing_docs)]

//! This module offers a protobuf implementation (to be used
//! with LaunchDarkly) `RocksDBTuningParameters` that can be used
//! to tune a RocksDB instance. The supported options are carefully
//! considered to be a minimal set required to tune RocksDB to perform
//! well for the `UPSERT` usecase. This usecase is slightly odd:
//! - Very high write rate (1:1 with reads)
//! - No durability requirements
//! - Minimal space amplification
//! - Relatively relaxed read and write latency requirements
//!     - (note that `UPSERT` RocksDB instances are NOT in the
//!     critical path for any sort of query.
//!
//! The defaults (so, the values resulting from derserializing `{}`
//! into a `RocksDBTuningParameters`) should be reasonable defaults.
//!
//! The documentation on each field in `RocksDBTuningParameters` has more
//! information
//!
//! Note that the following documents are required reading to deeply understand
//! this module:
//! - <https://github.com/EighteenZi/rocksdb_wiki/blob/master/RocksDB-Tuning-Guide.md>
//! - <https://github.com/EighteenZi/rocksdb_wiki/blob/master/Compression.md>
//! - <https://github.com/facebook/rocksdb/wiki/Setup-Options-and-Basic-Tuning>
//! - <https://www.eecg.toronto.edu/~stumm/Papers/Dong-CIDR-16.pdf>
//! - <http://smalldatum.blogspot.com/2015/11/read-write-space-amplification-pick-2_23.html>

use std::fmt::Debug;
use std::str::FromStr;
use std::time::Duration;

use serde::{Deserialize, Serialize};
use uncased::UncasedStr;

/// A set of parameters to tune RocksDB. This struct is plain-old-data, and is
/// used to update `RocksDBConfig`, which contains some dynamic value for some
/// parameters.
#[derive(Serialize, Deserialize, PartialEq, Clone, Debug)]
pub struct RocksDBTuningParameters {
    /// RocksDB has 2 primary styles of compaction:
    /// - The default, usually referred to as "level" compaction
    /// - "universal"
    ///
    /// Universal is simpler and for some workloads could be
    /// better. Also, you can directly configure its space-amplification ratio
    /// (using `universal_compaction_target_ratio`). However, its unclear
    /// if the `UPSERT` workload is a good workload for universal compaction,
    /// and its also might be the case that universal compaction uses significantly
    /// more space temporarily while performing compaction.
    ///
    /// For these reasons, the default is `CompactionStyle::Level`.
    pub compaction_style: CompactionStyle,
    /// The `RocksDB` api offers a single configuration method that sets some
    /// reasonable defaults for heavy-write workloads, either
    /// <https://docs.rs/rocksdb/latest/rocksdb/struct.Options.html#method.optimize_level_style_compaction>
    /// or
    /// <https://docs.rs/rocksdb/latest/rocksdb/struct.Options.html#method.optimize_universal_style_compaction>
    /// depending on `compaction_style`. We ALSO enable this configuration, which is tuned
    /// by the size of the memtable (basically the in-memory buffer used to avoid IO). The default
    /// here is ~512MB, which is the default from here: <https://github.com/facebook/rocksdb/blob/main/include/rocksdb/options.h#L102>,
    /// and about twice the global RocksDB default.
    pub optimize_compaction_memtable_budget: usize,

    /// This option, when enabled, dynamically tunes
    /// the size of the various LSM levels to put a bound on space-amplification.
    /// With the default level-ratio of `10`, this means space-amplification is
    /// O(1.11 * the size of data). Note this is big-O notation, and the actual
    /// amplification factor depends on the workload.
    ///
    /// See <https://www.eecg.toronto.edu/~stumm/Papers/Dong-CIDR-16.pdf> for more details.
    ///
    /// This option defaults to true, as its basically free saved-space, and only applies to
    /// `CompactionStyle::Level`.
    pub level_compaction_dynamic_level_bytes: bool,

    /// The additional space-amplification used with universal compaction.
    /// Only applies to `CompactionStyle::Universal`.
    ///
    /// See `compaction_style` for more information.
    pub universal_compaction_target_ratio: i32,

    /// By default, RocksDB uses only 1 thread to perform compaction and other background tasks.
    ///
    /// The default here is the number of cores, as mentioned by
    /// <https://docs.rs/rocksdb/latest/rocksdb/struct.Options.html#method.increase_parallelism>.
    ///
    /// Note that this option is shared across all RocksDB instances that share a `rocksdb::Env`.
    pub parallelism: Option<i32>,

    /// The most important way to reduce space amplification in RocksDB is compression.
    ///
    /// In RocksDB, data on disk is stored in an LSM tree. Because the higher layers (which are
    /// smaller) will need to be read during reads that aren't cached, we want a relatively
    /// lightweight compression scheme, choosing `Lz4` as the default, which is considered almost
    /// always better than `Snappy`.
    ///
    /// The meat of the data is stored in the largest, bottom layer, which can be configured
    /// (using `bottommost_compression_type`) to use a more expensive compression scheme to save
    /// more space. The default is `Zstd`, which many think has the best compression ratio. Note
    /// that tuning the bottommost layer separately only makes sense when you have free cpu,
    /// which we have in the case of the `UPSERT` usecase.
    pub compression_type: CompressionType,

    /// See `compression_type` for more information.
    pub bottommost_compression_type: CompressionType,

    /// The size of the `multi_get` and `multi_put` batches sent to RocksDB. The default is 1024.
    pub batch_size: usize,

    /// The maximum duration for the retries when performing rocksdb actions in case of retry-able errors.
    pub retry_max_duration: Duration,

    /// The interval to dump stats in `LOG`.
    pub stats_log_interval_seconds: u32,

    /// The interval to persist stats into rocksdb.
    pub stats_persist_interval_seconds: u32,

    /// The optional block cache size in MiB for optimizing rocksdb for point lookups.
    /// If not provided there will be no optimization.
    /// <https://github.com/facebook/rocksdb/blob/main/include/rocksdb/options.h#L82-L85>
    pub point_lookup_block_cache_size_mb: Option<u32>,

    /// The number of times by which unused buffers will be reduced.
    /// For example, if the number is 2, the buffers will be reduced to being twice as small,
    /// i.e. halved.
    /// Shrinking will be disabled if value is 0;
    pub shrink_buffers_by_ratio: usize,

    /// Optional write buffer manager bytes. This needs to be set to enable write buffer manager
    /// across all rocksdb instances
    pub write_buffer_manager_memory_bytes: Option<usize>,
    /// Optional write buffer manager memory limit as a percentage of cluster limit
    pub write_buffer_manager_memory_fraction: Option<f64>,
    /// Config to enable stalls with write buffer manager
    pub write_buffer_manager_allow_stall: bool,
}

impl Default for RocksDBTuningParameters {
    fn default() -> Self {
        Self {
            compaction_style: defaults::DEFAULT_COMPACTION_STYLE,
            optimize_compaction_memtable_budget:
                defaults::DEFAULT_OPTIMIZE_COMPACTION_MEMTABLE_BUDGET,
            level_compaction_dynamic_level_bytes:
                defaults::DEFAULT_LEVEL_COMPACTION_DYNAMIC_LEVEL_BYTES,
            universal_compaction_target_ratio: defaults::DEFAULT_UNIVERSAL_COMPACTION_RATIO,
            parallelism: defaults::DEFAULT_PARALLELISM,
            compression_type: defaults::DEFAULT_COMPRESSION_TYPE,
            bottommost_compression_type: defaults::DEFAULT_BOTTOMMOST_COMPRESSION_TYPE,
            batch_size: defaults::DEFAULT_BATCH_SIZE,
            retry_max_duration: defaults::DEFAULT_RETRY_DURATION,
            stats_log_interval_seconds: defaults::DEFAULT_STATS_LOG_INTERVAL_S,
            stats_persist_interval_seconds: defaults::DEFAULT_STATS_PERSIST_INTERVAL_S,
            point_lookup_block_cache_size_mb: None,
            shrink_buffers_by_ratio: defaults::DEFAULT_SHRINK_BUFFERS_BY_RATIO,
            write_buffer_manager_memory_bytes: None,
            write_buffer_manager_memory_fraction: None,
            write_buffer_manager_allow_stall: false,
        }
    }
}

impl RocksDBTuningParameters {
    /// Build a `RocksDBTuningParameters` from strings and values from LD parameters.
    pub fn from_parameters(
        compaction_style: CompactionStyle,
        optimize_compaction_memtable_budget: usize,
        level_compaction_dynamic_level_bytes: bool,
        universal_compaction_target_ratio: i32,
        parallelism: Option<i32>,
        compression_type: CompressionType,
        bottommost_compression_type: CompressionType,
        batch_size: usize,
        retry_max_duration: Duration,
        stats_log_interval_seconds: u32,
        stats_persist_interval_seconds: u32,
        point_lookup_block_cache_size_mb: Option<u32>,
        shrink_buffers_by_ratio: usize,
        write_buffer_manager_memory_bytes: Option<usize>,
        write_buffer_manager_memory_fraction: Option<f64>,
        write_buffer_manager_allow_stall: bool,
    ) -> Result<Self, anyhow::Error> {
        Ok(Self {
            compaction_style,
            optimize_compaction_memtable_budget,
            level_compaction_dynamic_level_bytes,
            universal_compaction_target_ratio: if universal_compaction_target_ratio > 100 {
                universal_compaction_target_ratio
            } else {
                return Err(anyhow::anyhow!(
                    "universal_compaction_target_ratio ({}) must be > 100",
                    universal_compaction_target_ratio
                ));
            },
            parallelism: match parallelism {
                Some(parallelism) => {
                    if parallelism < 1 {
                        return Err(anyhow::anyhow!(
                            "parallelism({}) must be > 1, or not specified",
                            universal_compaction_target_ratio
                        ));
                    }
                    Some(parallelism)
                }
                None => None,
            },
            compression_type,
            bottommost_compression_type,
            batch_size,
            retry_max_duration,
            stats_log_interval_seconds,
            stats_persist_interval_seconds,
            point_lookup_block_cache_size_mb,
            shrink_buffers_by_ratio,
            write_buffer_manager_memory_bytes,
            write_buffer_manager_memory_fraction,
            write_buffer_manager_allow_stall,
        })
    }
}

/// The 2 primary compaction styles in RocksDB`. See `RocksDBTuningParameters::compaction_style`
/// for more information.
#[derive(Serialize, Deserialize, Clone, Copy, PartialEq, Eq, Debug)]
pub enum CompactionStyle {
    Level,
    Universal,
}

impl FromStr for CompactionStyle {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let s = UncasedStr::new(s);
        if s == "level" {
            Ok(Self::Level)
        } else if s == "universal" {
            Ok(Self::Universal)
        } else {
            Err(anyhow::anyhow!("{} is not a supported compaction style", s))
        }
    }
}

impl std::fmt::Display for CompactionStyle {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            CompactionStyle::Level => write!(f, "level"),
            CompactionStyle::Universal => write!(f, "universal"),
        }
    }
}

/// Mz-supported compression types in RocksDB`. See `RocksDBTuningParameters::compression_type`
/// for more information.
#[derive(Serialize, Deserialize, Clone, Copy, PartialEq, Eq, Debug)]
pub enum CompressionType {
    Zstd,
    Snappy,
    Lz4,
    None,
}

impl FromStr for CompressionType {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let s = UncasedStr::new(s);
        if s == "zstd" {
            Ok(Self::Zstd)
        } else if s == "snappy" {
            Ok(Self::Snappy)
        } else if s == "lz4" {
            Ok(Self::Lz4)
        } else if s == "none" {
            Ok(Self::None)
        } else {
            Err(anyhow::anyhow!("{} is not a supported compression type", s))
        }
    }
}

impl std::fmt::Display for CompressionType {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            CompressionType::Zstd => write!(f, "zstd"),
            CompressionType::Snappy => write!(f, "snappy"),
            CompressionType::Lz4 => write!(f, "lz4"),
            CompressionType::None => write!(f, "none"),
        }
    }
}

#[derive(Clone, Debug)]
pub struct RocksDbWriteBufferManagerConfig {
    /// Optional write buffer manager bytes. This needs to be set to enable write buffer manager
    /// across all rocksdb instances
    pub write_buffer_manager_memory_bytes: Option<usize>,
    /// Optional write buffer manager memory limit as a percentage of cluster limit
    pub write_buffer_manager_memory_fraction: Option<f64>,
    /// Config to enable stalls with write buffer manager
    pub write_buffer_manager_allow_stall: bool,
    /// Cluster memory limit used to calculate write buffer manager limit
    /// if `write_buffer_manager_memory_fraction` is provided
    pub cluster_memory_limit: Option<usize>,
}

/// The following are defaults (and default strings for LD parameters)
/// for `RocksDBTuningParameters`.
pub mod defaults {
    use std::time::Duration;

    use super::*;

    pub const DEFAULT_COMPACTION_STYLE: CompactionStyle = CompactionStyle::Level;

    /// From here: <https://github.com/facebook/rocksdb/blob/main/include/rocksdb/options.h#L102>
    /// And then setting it to 1/3rd from our testing in production
    pub const DEFAULT_OPTIMIZE_COMPACTION_MEMTABLE_BUDGET: usize = 512 * 1024 * 1024 / 3;

    pub const DEFAULT_LEVEL_COMPACTION_DYNAMIC_LEVEL_BYTES: bool = true;

    /// From here: <https://docs.rs/rocksdb/latest/rocksdb/struct.UniversalCompactOptions.html>
    pub const DEFAULT_UNIVERSAL_COMPACTION_RATIO: i32 = 200;

    pub const DEFAULT_PARALLELISM: Option<i32> = None;

    pub const DEFAULT_COMPRESSION_TYPE: CompressionType = CompressionType::Lz4;

    pub const DEFAULT_BOTTOMMOST_COMPRESSION_TYPE: CompressionType = CompressionType::Lz4;

    /// A reasonable default batch size for gets and puts in RocksDB. Based
    /// on advice here: <https://github.com/facebook/rocksdb/wiki/RocksDB-FAQ>.
    /// Based on our testing we are using 20 times that.
    pub const DEFAULT_BATCH_SIZE: usize = 20 * 1024;

    /// The default max duration for retrying the retry-able errors in rocksdb.
    pub const DEFAULT_RETRY_DURATION: Duration = Duration::from_secs(1);

    /// Default is 10 minutes, from <https://docs.rs/rocksdb/latest/rocksdb/struct.Options.html#method.set_stats_dump_period_sec>
    pub const DEFAULT_STATS_LOG_INTERVAL_S: u32 = 600;

    /// Default is 10 minutes, from <https://docs.rs/rocksdb/latest/rocksdb/struct.Options.html#method.set_stats_persist_period_sec>
    pub const DEFAULT_STATS_PERSIST_INTERVAL_S: u32 = 600;

    /// Default is 0, i.e. shrinking will be disabled
    pub const DEFAULT_SHRINK_BUFFERS_BY_RATIO: usize = 0;

    /// Not allowing stalls for write buffer manager. Only applicable if write buffer manager is enabled by other flags.
    pub const DEFAULT_WRITE_BUFFER_MANAGER_ALLOW_STALL: bool = false;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[mz_ore::test]
    fn defaults_equality() {
        let r = RocksDBTuningParameters::from_parameters(
            defaults::DEFAULT_COMPACTION_STYLE,
            defaults::DEFAULT_OPTIMIZE_COMPACTION_MEMTABLE_BUDGET,
            defaults::DEFAULT_LEVEL_COMPACTION_DYNAMIC_LEVEL_BYTES,
            defaults::DEFAULT_UNIVERSAL_COMPACTION_RATIO,
            defaults::DEFAULT_PARALLELISM,
            defaults::DEFAULT_COMPRESSION_TYPE,
            defaults::DEFAULT_BOTTOMMOST_COMPRESSION_TYPE,
            defaults::DEFAULT_BATCH_SIZE,
            defaults::DEFAULT_RETRY_DURATION,
            defaults::DEFAULT_STATS_LOG_INTERVAL_S,
            defaults::DEFAULT_STATS_PERSIST_INTERVAL_S,
            None,
            defaults::DEFAULT_SHRINK_BUFFERS_BY_RATIO,
            None,
            None,
            defaults::DEFAULT_WRITE_BUFFER_MANAGER_ALLOW_STALL,
        )
        .unwrap();

        assert_eq!(r, RocksDBTuningParameters::default());
    }
}

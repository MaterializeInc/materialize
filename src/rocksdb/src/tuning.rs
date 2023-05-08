// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! This module offers a `serde::Deserialize` implementation (to be used
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

use rocksdb::{DBCompactionStyle, DBCompressionType};

/// A set of parameters to tune RocksDB.
#[derive(serde::Deserialize, PartialEq, Eq, Clone, Debug)]
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
    #[serde(default)]
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
    #[serde(default = "default_compaction_optimization")]
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
    #[serde(default = "default_true")]
    pub level_compaction_dynamic_level_bytes: bool,

    /// The additional space-amplification used with universal compaction.
    /// Only applies to `CompactionStyle::Universal`.
    ///
    /// See `compaction_style` for more information.
    #[serde(default = "default_universal_ratio")]
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
    #[serde(default = "default_compression")]
    pub compression_type: CompressionType,

    /// See `compression_type` for more information.
    #[serde(default = "default_bottommost_compression")]
    pub bottommost_compression_type: CompressionType,
}

impl RocksDBTuningParameters {
    /// Setup some reasonable defaults for turning `RocksDB`
    pub fn reasonable_defaults() -> Self {
        // This is the best way to ensure this default is the same that `serde` will give.
        serde_json::from_str("{}").unwrap()
    }
    /// Apply these tuning parameters to a `rocksdb::Options`. Some may
    /// be applied to a shared `Env` underlying the `Options`.
    pub fn apply_to_options(&self, options: &mut rocksdb::Options) {
        let RocksDBTuningParameters {
            compaction_style,
            optimize_compaction_memtable_budget,
            level_compaction_dynamic_level_bytes,
            universal_compaction_target_ratio,
            parallelism,
            compression_type,
            bottommost_compression_type,
        } = self;

        options.set_compression_type((*compression_type).into());

        if *bottommost_compression_type != CompressionType::None {
            options.set_bottommost_compression_type((*bottommost_compression_type).into())
        }

        options.set_compaction_style((*compaction_style).into());
        match compaction_style {
            CompactionStyle::Level => {
                options.optimize_level_style_compaction(*optimize_compaction_memtable_budget);
                options.set_level_compaction_dynamic_level_bytes(
                    *level_compaction_dynamic_level_bytes,
                );
            }
            CompactionStyle::Universal => {
                options.optimize_universal_style_compaction(*optimize_compaction_memtable_budget);
                options.set_level_compaction_dynamic_level_bytes(
                    *level_compaction_dynamic_level_bytes,
                );

                let mut universal_options = rocksdb::UniversalCompactOptions::default();
                universal_options
                    .set_max_size_amplification_percent(*universal_compaction_target_ratio);

                options.set_universal_compaction_options(&universal_options);
            }
        }

        let parallelism = if let Some(parallelism) = parallelism {
            *parallelism
        } else {
            // TODO(guswynn): it's unclear if this should be `get_physical`. The
            // RocksDB docs do not make it clear.
            num_cpus::get()
                .try_into()
                .expect("More than 3 billion cores")
        };
        options.increase_parallelism(parallelism);
    }
}

/// The 2 primary compaction styles in RocksDB`. See `RocksDBTuningParameters::compaction_style`
/// for more information.
#[derive(serde::Deserialize, Default, Clone, Copy, PartialEq, Eq, Debug)]
pub enum CompactionStyle {
    #[serde(rename = "level")]
    #[default]
    Level,
    #[serde(rename = "universal")]
    Universal,
}

impl From<CompactionStyle> for DBCompactionStyle {
    fn from(cs: CompactionStyle) -> DBCompactionStyle {
        use CompactionStyle::*;
        match cs {
            Level => DBCompactionStyle::Level,
            Universal => DBCompactionStyle::Universal,
        }
    }
}

/// Mz-supported compression types in RocksDB`. See `RocksDBTuningParameters::compression_type`
/// for more information.
#[derive(serde::Deserialize, Default, Clone, Copy, PartialEq, Eq, Debug)]
pub enum CompressionType {
    #[serde(rename = "zstd")]
    #[default]
    Zstd,
    #[serde(rename = "snappy")]
    Snappy,
    #[serde(rename = "lz4")]
    Lz4,
    #[serde(rename = "none")]
    None,
}

impl From<CompressionType> for DBCompressionType {
    fn from(ct: CompressionType) -> DBCompressionType {
        use CompressionType::*;
        match ct {
            Zstd => DBCompressionType::Zstd,
            Snappy => DBCompressionType::Snappy,
            Lz4 => DBCompressionType::Lz4,
            None => DBCompressionType::None,
        }
    }
}

// The following are functions used to define defaults for the `serde::Deserialize` implementation
// for `RocksDBTuningParameters`.

fn default_true() -> bool {
    true
}
/// From here: <https://github.com/facebook/rocksdb/blob/main/include/rocksdb/options.h#L102>
fn default_compaction_optimization() -> usize {
    512 * 1024 * 1024
}

/// From here: <https://docs.rs/rocksdb/latest/rocksdb/struct.UniversalCompactOptions.html>
fn default_universal_ratio() -> i32 {
    200
}
fn default_compression() -> CompressionType {
    CompressionType::Lz4
}
fn default_bottommost_compression() -> CompressionType {
    CompressionType::Zstd
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ensure_default() {
        let r: RocksDBTuningParameters = serde_json::from_str("{}").unwrap();

        assert_eq!(
            r,
            RocksDBTuningParameters {
                compaction_style: CompactionStyle::Level,
                optimize_compaction_memtable_budget: 512 * 1024 * 1024,
                level_compaction_dynamic_level_bytes: true,
                universal_compaction_target_ratio: 200,
                parallelism: None,
                compression_type: CompressionType::Lz4,
                bottommost_compression_type: CompressionType::Zstd,
            }
        );
    }
}

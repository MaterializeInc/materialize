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

use std::str::FromStr;

use proptest_derive::Arbitrary;
use rocksdb::{DBCompactionStyle, DBCompressionType};
use serde::{Deserialize, Serialize};
use uncased::UncasedStr;

use mz_ore::cast::CastFrom;
use mz_proto::{RustType, TryFromProtoError};

include!(concat!(env!("OUT_DIR"), "/mz_rocksdb.tuning.rs"));

/// A set of parameters to tune RocksDB.
#[derive(Serialize, Deserialize, PartialEq, Eq, Clone, Debug, Arbitrary)]
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
        })
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
#[derive(Serialize, Deserialize, Clone, Copy, PartialEq, Eq, Debug, Arbitrary)]
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
#[derive(Serialize, Deserialize, Clone, Copy, PartialEq, Eq, Debug, Arbitrary)]
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

impl RustType<ProtoRocksDbTuningParameters> for RocksDBTuningParameters {
    fn into_proto(&self) -> ProtoRocksDbTuningParameters {
        use proto_rocks_db_tuning_parameters::{
            proto_compaction_style, proto_compression_type, ProtoCompactionStyle,
            ProtoCompressionType,
        };

        fn compression_into_proto(compression_type: &CompressionType) -> ProtoCompressionType {
            ProtoCompressionType {
                kind: Some(match compression_type {
                    CompressionType::Zstd => proto_compression_type::Kind::Zstd(()),
                    CompressionType::Snappy => proto_compression_type::Kind::Snappy(()),
                    CompressionType::Lz4 => proto_compression_type::Kind::Lz4(()),
                    CompressionType::None => proto_compression_type::Kind::None(()),
                }),
            }
        }
        ProtoRocksDbTuningParameters {
            compaction_style: Some(ProtoCompactionStyle {
                kind: Some(match self.compaction_style {
                    CompactionStyle::Level => proto_compaction_style::Kind::Level(()),
                    CompactionStyle::Universal => proto_compaction_style::Kind::Universal(()),
                }),
            }),
            optimize_compaction_memtable_budget: u64::cast_from(
                self.optimize_compaction_memtable_budget,
            ),
            level_compaction_dynamic_level_bytes: self.level_compaction_dynamic_level_bytes,
            universal_compaction_target_ratio: self.universal_compaction_target_ratio,
            parallelism: self.parallelism,
            compression_type: Some(compression_into_proto(&self.compression_type)),
            bottommost_compression_type: Some(compression_into_proto(
                &self.bottommost_compression_type,
            )),
        }
    }

    fn from_proto(proto: ProtoRocksDbTuningParameters) -> Result<Self, TryFromProtoError> {
        use proto_rocks_db_tuning_parameters::{
            proto_compaction_style, proto_compression_type, ProtoCompactionStyle,
            ProtoCompressionType,
        };

        fn compression_from_proto(
            compression_type: Option<ProtoCompressionType>,
        ) -> Result<CompressionType, TryFromProtoError> {
            match compression_type {
                Some(ProtoCompressionType {
                    kind: Some(proto_compression_type::Kind::Zstd(())),
                }) => Ok(CompressionType::Zstd),
                Some(ProtoCompressionType {
                    kind: Some(proto_compression_type::Kind::Snappy(())),
                }) => Ok(CompressionType::Snappy),
                Some(ProtoCompressionType {
                    kind: Some(proto_compression_type::Kind::Lz4(())),
                }) => Ok(CompressionType::Lz4),
                Some(ProtoCompressionType {
                    kind: Some(proto_compression_type::Kind::None(())),
                }) => Ok(CompressionType::None),
                Some(ProtoCompressionType { kind: None }) => Err(TryFromProtoError::MissingField(
                    "ProtoRocksDbTuningParameters::compression_type::kind".into(),
                )),
                None => Err(TryFromProtoError::MissingField(
                    "ProtoRocksDbTuningParameters::compression_type".into(),
                )),
            }
        }
        Ok(Self {
            compaction_style: match proto.compaction_style {
                Some(ProtoCompactionStyle {
                    kind: Some(proto_compaction_style::Kind::Level(())),
                }) => CompactionStyle::Level,
                Some(ProtoCompactionStyle {
                    kind: Some(proto_compaction_style::Kind::Universal(())),
                }) => CompactionStyle::Universal,
                Some(ProtoCompactionStyle { kind: None }) => {
                    return Err(TryFromProtoError::MissingField(
                        "ProtoRocksDbTuningParameters::compaction_style::kind".into(),
                    ))
                }
                None => {
                    return Err(TryFromProtoError::MissingField(
                        "ProtoRocksDbTuningParameters::compaction_style".into(),
                    ))
                }
            },
            optimize_compaction_memtable_budget: usize::cast_from(
                proto.optimize_compaction_memtable_budget,
            ),
            level_compaction_dynamic_level_bytes: proto.level_compaction_dynamic_level_bytes,
            universal_compaction_target_ratio: proto.universal_compaction_target_ratio,
            parallelism: proto.parallelism,
            compression_type: compression_from_proto(proto.compression_type)?,
            bottommost_compression_type: compression_from_proto(proto.bottommost_compression_type)?,
        })
    }
}

/// The following are defaults (and default strings for LD parameters)
/// for `RocksDBTuningParameters`.
pub mod defaults {
    use super::*;

    pub const DEFAULT_COMPACTION_STYLE: CompactionStyle = CompactionStyle::Level;

    /// From here: <https://github.com/facebook/rocksdb/blob/main/include/rocksdb/options.h#L102>
    pub const DEFAULT_OPTIMIZE_COMPACTION_MEMTABLE_BUDGET: usize = 512 * 1024 * 1024;

    pub const DEFAULT_LEVEL_COMPACTION_DYNAMIC_LEVEL_BYTES: bool = true;

    /// From here: <https://docs.rs/rocksdb/latest/rocksdb/struct.UniversalCompactOptions.html>
    pub const DEFAULT_UNIVERSAL_COMPACTION_RATIO: i32 = 200;

    pub const DEFAULT_PARALLELISM: Option<i32> = None;

    pub const DEFAULT_COMPRESSION_TYPE: CompressionType = CompressionType::Lz4;

    pub const DEFAULT_BOTTOMMOST_COMPRESSION_TYPE: CompressionType = CompressionType::Zstd;
}

#[cfg(test)]
mod tests {
    use super::*;
    use mz_proto::protobuf_roundtrip;
    use proptest::prelude::*;

    #[test]
    fn defaults_equality() {
        let r = RocksDBTuningParameters::from_parameters(
            defaults::DEFAULT_COMPACTION_STYLE,
            defaults::DEFAULT_OPTIMIZE_COMPACTION_MEMTABLE_BUDGET,
            defaults::DEFAULT_LEVEL_COMPACTION_DYNAMIC_LEVEL_BYTES,
            defaults::DEFAULT_UNIVERSAL_COMPACTION_RATIO,
            defaults::DEFAULT_PARALLELISM,
            defaults::DEFAULT_COMPRESSION_TYPE,
            defaults::DEFAULT_BOTTOMMOST_COMPRESSION_TYPE,
        )
        .unwrap();

        assert_eq!(r, RocksDBTuningParameters::default());
    }

    #[test]
    #[cfg_attr(miri, ignore)] // too slow
    fn rocksdb_tuning_roundtrip() {
        mz_ore::test::init_logging();
        proptest!(|(expect in any::<RocksDBTuningParameters>())| {
            let actual = protobuf_roundtrip::<_, ProtoRocksDbTuningParameters>(&expect);
            assert!(actual.is_ok());
            assert_eq!(actual.unwrap(), expect);

        });
    }
}

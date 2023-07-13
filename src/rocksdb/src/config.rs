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

//! This module handles converting `mz_rocksdb_types` into `rocksdb` types.

use rocksdb::{DBCompactionStyle, DBCompressionType};

pub use mz_rocksdb_types::config::defaults;
pub use mz_rocksdb_types::config::*;

/// An `Into` we can implement on foreign types
trait IntoRocksDBType {
    type Type;
    fn into_rocksdb(self) -> Self::Type;
}

impl IntoRocksDBType for CompactionStyle {
    type Type = DBCompactionStyle;
    fn into_rocksdb(self) -> Self::Type {
        use CompactionStyle::*;
        match self {
            Level => DBCompactionStyle::Level,
            Universal => DBCompactionStyle::Universal,
        }
    }
}

impl IntoRocksDBType for CompressionType {
    type Type = DBCompressionType;
    fn into_rocksdb(self) -> Self::Type {
        use CompressionType::*;
        match self {
            Zstd => DBCompressionType::Zstd,
            Snappy => DBCompressionType::Snappy,
            Lz4 => DBCompressionType::Lz4,
            None => DBCompressionType::None,
        }
    }
}
/// Apply these tuning parameters to a `rocksdb::Options`. Some may
/// be applied to a shared `Env` underlying the `Options`.
pub fn apply_to_options(config: &RocksDBConfig, options: &mut rocksdb::Options) {
    let RocksDBConfig {
        compaction_style,
        optimize_compaction_memtable_budget,
        level_compaction_dynamic_level_bytes,
        universal_compaction_target_ratio,
        parallelism,
        compression_type,
        bottommost_compression_type,
        retry_max_duration: _,
        dynamic: _,
    } = config;

    options.set_compression_type((*compression_type).into_rocksdb());

    if *bottommost_compression_type != CompressionType::None {
        options.set_bottommost_compression_type((*bottommost_compression_type).into_rocksdb())
    }

    options.set_compaction_style((*compaction_style).into_rocksdb());
    match compaction_style {
        CompactionStyle::Level => {
            options.optimize_level_style_compaction(*optimize_compaction_memtable_budget);
            options.set_level_compaction_dynamic_level_bytes(*level_compaction_dynamic_level_bytes);
        }
        CompactionStyle::Universal => {
            options.optimize_universal_style_compaction(*optimize_compaction_memtable_budget);
            options.set_level_compaction_dynamic_level_bytes(*level_compaction_dynamic_level_bytes);

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

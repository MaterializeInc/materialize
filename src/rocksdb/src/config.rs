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

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, Weak};
use std::time::Duration;

use mz_ore::cast::CastLossy;

use derivative::Derivative;
use rocksdb::{DBCompactionStyle, DBCompressionType, WriteBufferManager};

pub use mz_rocksdb_types::config::defaults;
pub use mz_rocksdb_types::config::*;

#[derive(Debug, Clone)]
pub struct RocksDBDynamicConfig {
    batch_size: Arc<AtomicUsize>,
}

impl RocksDBDynamicConfig {
    pub fn batch_size(&self) -> usize {
        // SeqCst is probably not required here, but its the easiest to reason about
        self.batch_size.load(Ordering::SeqCst)
    }
}

/// Configurable options for a `RocksDBInstance`. Some can be updated
/// dynamically, as cloned instances of this object will shared dynamic values.
#[derive(Clone, Derivative)]
#[derivative(Debug)]
pub struct RocksDBConfig {
    pub compaction_style: CompactionStyle,
    pub optimize_compaction_memtable_budget: usize,
    pub level_compaction_dynamic_level_bytes: bool,
    pub universal_compaction_target_ratio: i32,
    pub parallelism: Option<i32>,
    pub compression_type: CompressionType,
    pub bottommost_compression_type: CompressionType,
    pub retry_max_duration: Duration,
    pub stats_log_interval_seconds: u32,
    pub stats_persist_interval_seconds: u32,
    pub point_lookup_block_cache_size_mb: Option<u32>,
    pub shrink_buffers_by_ratio: usize,
    pub dynamic: RocksDBDynamicConfig,

    /// Write buffer manager configs
    pub write_buffer_manager_config: RocksDbWriteBufferManagerConfig,
    /// Shared write buffer manager instance,
    /// can only be instantiated once via `get_or_init_handle`
    #[derivative(Debug = "ignore")]
    pub shared_write_buffer_manager: SharedWriteBufferManager,
}

impl RocksDBConfig {
    pub fn new(
        shared_write_buffer_manager: SharedWriteBufferManager,
        cluster_memory_limit: Option<usize>,
    ) -> Self {
        Self::new_from_params(
            RocksDBTuningParameters::default(),
            shared_write_buffer_manager,
            cluster_memory_limit,
        )
    }

    fn new_from_params(
        params: RocksDBTuningParameters,
        shared_write_buffer_manager: SharedWriteBufferManager,
        cluster_memory_limit: Option<usize>,
    ) -> Self {
        let RocksDBTuningParameters {
            compaction_style,
            optimize_compaction_memtable_budget,
            level_compaction_dynamic_level_bytes,
            universal_compaction_target_ratio,
            parallelism,
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
        } = params;

        Self {
            compaction_style,
            optimize_compaction_memtable_budget,
            level_compaction_dynamic_level_bytes,
            universal_compaction_target_ratio,
            parallelism,
            compression_type,
            bottommost_compression_type,
            retry_max_duration,
            stats_log_interval_seconds,
            stats_persist_interval_seconds,
            point_lookup_block_cache_size_mb,
            shrink_buffers_by_ratio,
            dynamic: RocksDBDynamicConfig {
                batch_size: Arc::new(AtomicUsize::new(batch_size)),
            },
            write_buffer_manager_config: RocksDbWriteBufferManagerConfig {
                write_buffer_manager_memory_bytes,
                write_buffer_manager_memory_fraction,
                write_buffer_manager_allow_stall,
                cluster_memory_limit,
            },
            shared_write_buffer_manager,
        }
    }

    /// Apply the new parameters to the config. Dynamic parameters
    /// are updated in place.
    pub fn apply(&mut self, params: RocksDBTuningParameters) {
        let RocksDBTuningParameters {
            compaction_style,
            optimize_compaction_memtable_budget,
            level_compaction_dynamic_level_bytes,
            universal_compaction_target_ratio,
            parallelism,
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
        } = params;

        self.compaction_style = compaction_style;
        self.optimize_compaction_memtable_budget = optimize_compaction_memtable_budget;
        self.level_compaction_dynamic_level_bytes = level_compaction_dynamic_level_bytes;
        self.universal_compaction_target_ratio = universal_compaction_target_ratio;
        self.parallelism = parallelism;
        self.compression_type = compression_type;
        self.bottommost_compression_type = bottommost_compression_type;
        self.retry_max_duration = retry_max_duration;
        self.stats_log_interval_seconds = stats_log_interval_seconds;
        self.stats_persist_interval_seconds = stats_persist_interval_seconds;
        self.point_lookup_block_cache_size_mb = point_lookup_block_cache_size_mb;
        self.shrink_buffers_by_ratio = shrink_buffers_by_ratio;

        self.write_buffer_manager_config
            .write_buffer_manager_memory_bytes = write_buffer_manager_memory_bytes;
        self.write_buffer_manager_config
            .write_buffer_manager_memory_fraction = write_buffer_manager_memory_fraction;
        self.write_buffer_manager_config
            .write_buffer_manager_allow_stall = write_buffer_manager_allow_stall;

        // SeqCst is probably not required here, but its the easiest to reason about
        self.dynamic.batch_size.store(batch_size, Ordering::SeqCst);
    }
}

#[derive(Clone, Default)]
pub struct SharedWriteBufferManager {
    /// Keeping a Weak pointer to [WriteBufferManager] here behind an Arc and a Mutex.
    /// The strong pointers will be owned by each `RocksDBInstance`.
    /// When the rocksdb instances are cleaned up, the [WriteBufferManager] here will
    /// be cleaned up as well.
    /// Updates to config values via [RocksDbWriteBufferManagerConfig] will not update
    /// the [WriteBufferManager] once it's initialized here and there's at least one RocksDBInstance
    /// keeping a strong reference to it.
    shared: Arc<Mutex<Weak<WriteBufferManager>>>,
}

#[derive(Derivative)]
#[derivative(Debug)]
/// A handle to the [WriteBufferManager] which will be dropped when the
/// rocksdb thread is dropped.
pub struct WriteBufferManagerHandle {
    #[derivative(Debug(format_with = "fmt_write_buffer_manager"))]
    inner: Arc<WriteBufferManager>,
}

fn fmt_write_buffer_manager(
    buf: &Arc<WriteBufferManager>,
    fmt: &mut std::fmt::Formatter,
) -> Result<(), std::fmt::Error> {
    fmt.debug_struct("WriteBufferManager")
        .field("enabled", &buf.enabled())
        .field("buffer_size", &buf.get_buffer_size())
        .field("memory_usage", &buf.get_usage())
        .finish()
}

impl SharedWriteBufferManager {
    /// If a shared [WriteBufferManager] does not already exist, then it's initialized
    /// with given `initializer`.
    /// A strong reference is returned for the shared buffer manager.
    pub(crate) fn get_or_init<F>(&self, initializer: F) -> WriteBufferManagerHandle
    where
        F: FnOnce() -> WriteBufferManager,
    {
        let mut lock = self.shared.lock().expect("lock poisoned");

        let wbm = match lock.upgrade() {
            Some(wbm) => wbm,
            None => {
                let new_wbm: Arc<WriteBufferManager> = Arc::new(initializer());
                *lock = Arc::downgrade(&new_wbm);
                new_wbm
            }
        };
        WriteBufferManagerHandle { inner: wbm }
    }

    /// This will return a non-empty [WriteBufferManager] only after it has been
    /// initialized by a RocksDBInstance.
    /// This method is only used in tests.
    pub fn get(&self) -> Option<Arc<WriteBufferManager>> {
        self.shared.lock().expect("lock poisoned").upgrade()
    }
}

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
/// If configured, then a write buffer manager will be initialized
/// and a handle to it will be returned.
pub fn apply_to_options(
    config: &RocksDBConfig,
    options: &mut rocksdb::Options,
) -> Option<WriteBufferManagerHandle> {
    let RocksDBConfig {
        compaction_style,
        optimize_compaction_memtable_budget,
        level_compaction_dynamic_level_bytes,
        universal_compaction_target_ratio,
        parallelism,
        compression_type,
        bottommost_compression_type,
        retry_max_duration: _,
        stats_log_interval_seconds,
        stats_persist_interval_seconds,
        point_lookup_block_cache_size_mb,
        shrink_buffers_by_ratio: _,
        dynamic: _,
        shared_write_buffer_manager,
        write_buffer_manager_config,
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

    options.set_stats_dump_period_sec(*stats_log_interval_seconds);
    options.set_stats_persist_period_sec(*stats_persist_interval_seconds);

    if let Some(block_cache_size_mb) = point_lookup_block_cache_size_mb {
        options.optimize_for_point_lookup((*block_cache_size_mb).into());
    }

    let write_buffer_manager = get_write_buffer_manager(write_buffer_manager_config);
    let write_buffer_manager_handle = write_buffer_manager.map(|buf| {
        let handle = shared_write_buffer_manager.get_or_init(|| buf);
        options.set_write_buffer_manager(&handle.inner);
        handle
    });

    write_buffer_manager_handle
}

/// Getting write buffer manager based on configured values
pub(crate) fn get_write_buffer_manager(
    write_buffer_manager_config: &RocksDbWriteBufferManagerConfig,
) -> Option<WriteBufferManager> {
    let RocksDbWriteBufferManagerConfig {
        write_buffer_manager_memory_bytes,
        write_buffer_manager_memory_fraction,
        write_buffer_manager_allow_stall,
        cluster_memory_limit,
    } = write_buffer_manager_config;

    if write_buffer_manager_memory_bytes.is_some() {
        let current_cluster_max_buffer_limit =
            cluster_memory_limit.as_ref().and_then(|cluster_memory| {
                write_buffer_manager_memory_fraction
                    .map(|fraction| usize::cast_lossy(f64::cast_lossy(*cluster_memory) * fraction))
            });
        let write_buffer_manager_bytes = current_cluster_max_buffer_limit
            .or(*write_buffer_manager_memory_bytes)
            .unwrap();
        Some(WriteBufferManager::new_write_buffer_manager(
            write_buffer_manager_bytes,
            *write_buffer_manager_allow_stall,
        ))
    } else {
        None
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[mz_ore::test]
    fn dynamic_defaults() {
        assert_eq!(
            RocksDBConfig::new(Default::default(), None)
                .dynamic
                .batch_size
                .load(Ordering::SeqCst),
            defaults::DEFAULT_BATCH_SIZE
        )
    }

    #[mz_ore::test]
    fn test_no_default() {
        let config = RocksDbWriteBufferManagerConfig {
            write_buffer_manager_memory_bytes: None,
            write_buffer_manager_memory_fraction: Some(0.5),
            write_buffer_manager_allow_stall: false,
            cluster_memory_limit: Some(1000),
        };

        let write_buffer_manager = get_write_buffer_manager(&config);
        assert!(write_buffer_manager.is_none());
    }

    #[mz_ore::test]
    fn test_default() {
        let config = RocksDbWriteBufferManagerConfig {
            write_buffer_manager_memory_bytes: Some(1000),
            write_buffer_manager_memory_fraction: Some(0.5),
            write_buffer_manager_allow_stall: false,
            cluster_memory_limit: None,
        };

        let write_buffer_manager = get_write_buffer_manager(&config);

        assert!(write_buffer_manager.is_some());
        let write_buffer_manager = write_buffer_manager.unwrap();
        assert!(write_buffer_manager.enabled());
        assert_eq!(
            write_buffer_manager.get_buffer_size(),
            config.write_buffer_manager_memory_bytes.unwrap()
        )
    }

    #[mz_ore::test]
    fn test_calculated_cluster_limit() {
        let config = RocksDbWriteBufferManagerConfig {
            write_buffer_manager_memory_bytes: Some(30000),
            write_buffer_manager_memory_fraction: Some(0.5),
            write_buffer_manager_allow_stall: false,
            cluster_memory_limit: Some(2000),
        };

        let write_buffer_manager = get_write_buffer_manager(&config);

        assert!(write_buffer_manager.is_some());
        let write_buffer_manager = write_buffer_manager.unwrap();
        assert!(write_buffer_manager.enabled());
        // the limit should be 50% of 2000 i.e. 1000
        assert_eq!(write_buffer_manager.get_buffer_size(), 1000)
    }
}

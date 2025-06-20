// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! An abstraction presenting as a durable time-varying collection (aka shard)

#![warn(missing_docs, missing_debug_implementations)]
// #[track_caller] is currently a no-op on async functions, but that hopefully won't be the case
// forever. So we already annotate those functions now and ignore the compiler warning until
// https://github.com/rust-lang/rust/issues/87417 pans out.
#![allow(ungated_async_fn_track_caller)]

use std::fmt::Debug;
use std::marker::PhantomData;
use std::sync::Arc;

use differential_dataflow::difference::Semigroup;
use differential_dataflow::lattice::Lattice;
use itertools::Itertools;
use mz_build_info::{BuildInfo, build_info};
use mz_dyncfg::ConfigSet;
use mz_ore::{instrument, soft_assert_or_log};
use mz_persist::location::{Blob, Consensus, ExternalError};
use mz_persist_types::schema::SchemaId;
use mz_persist_types::{Codec, Codec64, Opaque};
use mz_proto::{IntoRustIfSome, ProtoType};
use semver::Version;
use timely::order::TotalOrder;
use timely::progress::{Antichain, Timestamp};

use crate::async_runtime::IsolatedRuntime;
use crate::batch::{BATCH_DELETE_ENABLED, Batch, BatchBuilder, ProtoBatch};
use crate::cache::{PersistClientCache, StateCache};
use crate::cfg::PersistConfig;
use crate::critical::{CriticalReaderId, SinceHandle};
use crate::error::InvalidUsage;
use crate::fetch::{BatchFetcher, BatchFetcherConfig};
use crate::internal::compact::{CompactConfig, Compactor};
use crate::internal::encoding::parse_id;
use crate::internal::gc::GarbageCollector;
use crate::internal::machine::{Machine, retry_external};
use crate::internal::state_versions::StateVersions;
use crate::metrics::Metrics;
use crate::read::{
    Cursor, LazyPartStats, LeasedReaderId, READER_LEASE_DURATION, ReadHandle, Since,
};
use crate::rpc::PubSubSender;
use crate::schema::CaESchema;
use crate::write::{WriteHandle, WriterId};

pub mod async_runtime;
pub mod batch;
pub mod cache;
pub mod cfg;
pub mod cli {
    //! Persist command-line utilities
    pub mod admin;
    pub mod args;
    pub mod bench;
    pub mod inspect;
}
pub mod critical;
pub mod error;
pub mod fetch;
pub mod internals_bench;
pub mod iter;
pub mod metrics {
    //! Utilities related to metrics.
    pub use crate::internal::metrics::{
        Metrics, SinkMetrics, SinkWorkerMetrics, UpdateDelta, encode_ts_metric,
    };
}
pub mod operators {
    //! [timely] operators for reading and writing persist Shards.

    use mz_dyncfg::Config;

    pub mod shard_source;

    // TODO(cfg): Move this next to the use.
    pub(crate) const STORAGE_SOURCE_DECODE_FUEL: Config<usize> = Config::new(
        "storage_source_decode_fuel",
        100_000,
        "\
        The maximum amount of work to do in the persist_source mfp_and_decode \
        operator before yielding.",
    );
}
pub mod read;
pub mod rpc;
pub mod schema;
pub mod stats;
pub mod usage;
pub mod write;

/// An implementation of the public crate interface.
mod internal {
    pub mod apply;
    pub mod cache;
    pub mod compact;
    pub mod encoding;
    pub mod gc;
    pub mod machine;
    pub mod maintenance;
    pub mod merge;
    pub mod metrics;
    pub mod paths;
    pub mod restore;
    pub mod service;
    pub mod state;
    pub mod state_diff;
    pub mod state_versions;
    pub mod trace;
    pub mod watch;

    #[cfg(test)]
    pub mod datadriven;
}

/// Persist build information.
pub const BUILD_INFO: BuildInfo = build_info!();

// Re-export for convenience.
pub use mz_persist_types::{PersistLocation, ShardId};

pub use crate::internal::encoding::Schemas;

/// Additional diagnostic information used within Persist
/// e.g. for logging, metric labels, etc.
#[derive(Clone, Debug)]
pub struct Diagnostics {
    /// A user-friendly name for the shard.
    pub shard_name: String,
    /// A purpose for the handle.
    pub handle_purpose: String,
}

impl Diagnostics {
    /// Create a new `Diagnostics` from `handle_purpose`.
    pub fn from_purpose(handle_purpose: &str) -> Self {
        Self {
            shard_name: "unknown".to_string(),
            handle_purpose: handle_purpose.to_string(),
        }
    }

    /// Create a new `Diagnostics` for testing.
    pub fn for_tests() -> Self {
        Self {
            shard_name: "test-shard-name".to_string(),
            handle_purpose: "test-purpose".to_string(),
        }
    }
}

/// A handle for interacting with the set of persist shard made durable at a
/// single [PersistLocation].
///
/// All async methods on PersistClient retry for as long as they are able, but
/// the returned [std::future::Future]s implement "cancel on drop" semantics.
/// This means that callers can add a timeout using [tokio::time::timeout] or
/// [tokio::time::timeout_at].
///
/// ```rust,no_run
/// # use std::sync::Arc;
/// # use mz_persist_types::codec_impls::StringSchema;
/// # let client: mz_persist_client::PersistClient = unimplemented!();
/// # let timeout: std::time::Duration = unimplemented!();
/// # let id = mz_persist_client::ShardId::new();
/// # let diagnostics = mz_persist_client::Diagnostics { shard_name: "".into(), handle_purpose: "".into() };
/// # async {
/// tokio::time::timeout(timeout, client.open::<String, String, u64, i64>(id,
///     Arc::new(StringSchema),Arc::new(StringSchema),diagnostics, true)).await
/// # };
/// ```
#[derive(Debug, Clone)]
pub struct PersistClient {
    cfg: PersistConfig,
    blob: Arc<dyn Blob>,
    consensus: Arc<dyn Consensus>,
    metrics: Arc<Metrics>,
    isolated_runtime: Arc<IsolatedRuntime>,
    shared_states: Arc<StateCache>,
    pubsub_sender: Arc<dyn PubSubSender>,
}

impl PersistClient {
    /// Returns a new client for interfacing with persist shards made durable to
    /// the given [Blob] and [Consensus].
    ///
    /// This is exposed mostly for testing. Persist users likely want
    /// [crate::cache::PersistClientCache::open].
    pub fn new(
        cfg: PersistConfig,
        blob: Arc<dyn Blob>,
        consensus: Arc<dyn Consensus>,
        metrics: Arc<Metrics>,
        isolated_runtime: Arc<IsolatedRuntime>,
        shared_states: Arc<StateCache>,
        pubsub_sender: Arc<dyn PubSubSender>,
    ) -> Result<Self, ExternalError> {
        // TODO: Verify somehow that blob matches consensus to prevent
        // accidental misuse.
        Ok(PersistClient {
            cfg,
            blob,
            consensus,
            metrics,
            isolated_runtime,
            shared_states,
            pubsub_sender,
        })
    }

    /// Returns a new in-mem [PersistClient] for tests and examples.
    pub async fn new_for_tests() -> Self {
        let cache = PersistClientCache::new_no_metrics();
        cache
            .open(PersistLocation::new_in_mem())
            .await
            .expect("in-mem location is valid")
    }

    /// Returns persist's [ConfigSet].
    pub fn dyncfgs(&self) -> &ConfigSet {
        &self.cfg.configs
    }

    pub fn cfg(&self) -> &PersistConfig {
        &self.cfg
    }

    async fn make_machine<K, V, T, D>(
        &self,
        shard_id: ShardId,
        diagnostics: Diagnostics,
    ) -> Result<Machine<K, V, T, D>, InvalidUsage<T>>
    where
        K: Debug + Codec,
        V: Debug + Codec,
        T: Timestamp + Lattice + Codec64 + Sync,
        D: Semigroup + Codec64 + Send + Sync,
    {
        let state_versions = StateVersions::new(
            self.cfg.clone(),
            Arc::clone(&self.consensus),
            Arc::clone(&self.blob),
            Arc::clone(&self.metrics),
        );
        let machine = Machine::<K, V, T, D>::new(
            self.cfg.clone(),
            shard_id,
            Arc::clone(&self.metrics),
            Arc::new(state_versions),
            Arc::clone(&self.shared_states),
            Arc::clone(&self.pubsub_sender),
            Arc::clone(&self.isolated_runtime),
            diagnostics.clone(),
        )
        .await?;
        Ok(machine)
    }

    /// Provides capabilities for the durable TVC identified by `shard_id` at
    /// its current since and upper frontiers.
    ///
    /// This method is a best-effort attempt to regain control of the frontiers
    /// of a shard. Its most common uses are to recover capabilities that have
    /// expired (leases) or to attempt to read a TVC that one did not create (or
    /// otherwise receive capabilities for). If the frontiers have been fully
    /// released by all other parties, this call may result in capabilities with
    /// empty frontiers (which are useless).
    ///
    /// If `shard_id` has never been used before, initializes a new shard and
    /// returns handles with `since` and `upper` frontiers set to initial values
    /// of `Antichain::from_elem(T::minimum())`.
    ///
    /// The `schema` parameter is currently unused, but should be an object
    /// that represents the schema of the data in the shard. This will be required
    /// in the future.
    #[instrument(level = "debug", fields(shard = %shard_id))]
    pub async fn open<K, V, T, D>(
        &self,
        shard_id: ShardId,
        key_schema: Arc<K::Schema>,
        val_schema: Arc<V::Schema>,
        diagnostics: Diagnostics,
        use_critical_since: bool,
    ) -> Result<(WriteHandle<K, V, T, D>, ReadHandle<K, V, T, D>), InvalidUsage<T>>
    where
        K: Debug + Codec,
        V: Debug + Codec,
        T: Timestamp + TotalOrder + Lattice + Codec64 + Sync,
        D: Semigroup + Ord + Codec64 + Send + Sync,
    {
        Ok((
            self.open_writer(
                shard_id,
                Arc::clone(&key_schema),
                Arc::clone(&val_schema),
                diagnostics.clone(),
            )
            .await?,
            self.open_leased_reader(
                shard_id,
                key_schema,
                val_schema,
                diagnostics,
                use_critical_since,
            )
            .await?,
        ))
    }

    /// [Self::open], but returning only a [ReadHandle].
    ///
    /// Use this to save latency and a bit of persist traffic if you're just
    /// going to immediately drop or expire the [WriteHandle].
    ///
    /// The `_schema` parameter is currently unused, but should be an object
    /// that represents the schema of the data in the shard. This will be required
    /// in the future.
    #[instrument(level = "debug", fields(shard = %shard_id))]
    pub async fn open_leased_reader<K, V, T, D>(
        &self,
        shard_id: ShardId,
        key_schema: Arc<K::Schema>,
        val_schema: Arc<V::Schema>,
        diagnostics: Diagnostics,
        use_critical_since: bool,
    ) -> Result<ReadHandle<K, V, T, D>, InvalidUsage<T>>
    where
        K: Debug + Codec,
        V: Debug + Codec,
        T: Timestamp + TotalOrder + Lattice + Codec64 + Sync,
        D: Semigroup + Codec64 + Send + Sync,
    {
        let machine = self.make_machine(shard_id, diagnostics.clone()).await?;
        let gc = GarbageCollector::new(machine.clone(), Arc::clone(&self.isolated_runtime));

        let reader_id = LeasedReaderId::new();
        let heartbeat_ts = (self.cfg.now)();
        let (reader_state, maintenance) = machine
            .register_leased_reader(
                &reader_id,
                &diagnostics.handle_purpose,
                READER_LEASE_DURATION.get(&self.cfg),
                heartbeat_ts,
                use_critical_since,
            )
            .await;
        maintenance.start_performing(&machine, &gc);
        let schemas = Schemas {
            id: None,
            key: key_schema,
            val: val_schema,
        };
        let reader = ReadHandle::new(
            self.cfg.clone(),
            Arc::clone(&self.metrics),
            machine,
            gc,
            Arc::clone(&self.blob),
            reader_id,
            schemas,
            reader_state.since,
            heartbeat_ts,
        )
        .await;

        Ok(reader)
    }

    /// Creates and returns a [BatchFetcher] for the given shard id.
    #[instrument(level = "debug", fields(shard = %shard_id))]
    pub async fn create_batch_fetcher<K, V, T, D>(
        &self,
        shard_id: ShardId,
        key_schema: Arc<K::Schema>,
        val_schema: Arc<V::Schema>,
        is_transient: bool,
        diagnostics: Diagnostics,
    ) -> Result<BatchFetcher<K, V, T, D>, InvalidUsage<T>>
    where
        K: Debug + Codec,
        V: Debug + Codec,
        T: Timestamp + Lattice + Codec64 + Sync,
        D: Semigroup + Codec64 + Send + Sync,
    {
        let machine = self.make_machine(shard_id, diagnostics.clone()).await?;
        let read_schemas = Schemas {
            id: None,
            key: key_schema,
            val: val_schema,
        };
        let schema_cache = machine.applier.schema_cache();
        let fetcher = BatchFetcher {
            cfg: BatchFetcherConfig::new(&self.cfg),
            blob: Arc::clone(&self.blob),
            metrics: Arc::clone(&self.metrics),
            shard_metrics: Arc::clone(&machine.applier.shard_metrics),
            shard_id,
            read_schemas,
            schema_cache,
            is_transient,
            _phantom: PhantomData,
        };

        Ok(fetcher)
    }

    /// A convenience [CriticalReaderId] for Materialize controllers.
    ///
    /// For most (soon to be all?) shards in Materialize, a centralized
    /// "controller" is the authority for when a user no longer needs to read at
    /// a given frontier. (Other uses are temporary holds where correctness of
    /// the overall system can be maintained through a lease timeout.) To make
    /// [SinceHandle] easier to work with, we offer this convenience id for
    /// Materialize controllers, so they don't have to durably record it.
    ///
    /// TODO: We're still shaking out whether the controller should be the only
    /// critical since hold or if there are other places we want them. If the
    /// former, we should remove [CriticalReaderId] and bake in the singular
    /// nature of the controller critical handle.
    ///
    /// ```rust
    /// // This prints as something that is not 0 but is visually recognizable.
    /// assert_eq!(
    ///     mz_persist_client::PersistClient::CONTROLLER_CRITICAL_SINCE.to_string(),
    ///     "c00000000-1111-2222-3333-444444444444",
    /// )
    /// ```
    pub const CONTROLLER_CRITICAL_SINCE: CriticalReaderId =
        CriticalReaderId([0, 0, 0, 0, 17, 17, 34, 34, 51, 51, 68, 68, 68, 68, 68, 68]);

    /// Provides a capability for the durable TVC identified by `shard_id` at
    /// its current since frontier.
    ///
    /// In contrast to the time-leased [ReadHandle] returned by [Self::open] and
    /// [Self::open_leased_reader], this handle and its associated capability
    /// are not leased. A [SinceHandle] does not release its since capability;
    /// downgrade to the empty antichain to hold back the since.
    /// Also unlike `ReadHandle`, the handle is not expired on drop.
    /// This is less ergonomic, but useful for "critical" since
    /// holds which must survive even lease timeouts.
    ///
    /// **IMPORTANT**: The above means that if a SinceHandle is registered and
    /// then lost, the shard's since will be permanently "stuck", forever
    /// preventing logical compaction. Users are advised to durably record
    /// (preferably in code) the intended [CriticalReaderId] _before_ registering
    /// a SinceHandle (in case the process crashes at the wrong time).
    ///
    /// If `shard_id` has never been used before, initializes a new shard and
    /// return a handle with its `since` frontier set to the initial value of
    /// `Antichain::from_elem(T::minimum())`.
    #[instrument(level = "debug", fields(shard = %shard_id))]
    pub async fn open_critical_since<K, V, T, D, O>(
        &self,
        shard_id: ShardId,
        reader_id: CriticalReaderId,
        diagnostics: Diagnostics,
    ) -> Result<SinceHandle<K, V, T, D, O>, InvalidUsage<T>>
    where
        K: Debug + Codec,
        V: Debug + Codec,
        T: Timestamp + Lattice + Codec64 + Sync,
        D: Semigroup + Codec64 + Send + Sync,
        O: Opaque + Codec64,
    {
        let machine = self.make_machine(shard_id, diagnostics.clone()).await?;
        let gc = GarbageCollector::new(machine.clone(), Arc::clone(&self.isolated_runtime));

        let (state, maintenance) = machine
            .register_critical_reader::<O>(&reader_id, &diagnostics.handle_purpose)
            .await;
        maintenance.start_performing(&machine, &gc);
        let handle = SinceHandle::new(
            machine,
            gc,
            reader_id,
            state.since,
            Codec64::decode(state.opaque.0),
        );

        Ok(handle)
    }

    /// [Self::open], but returning only a [WriteHandle].
    ///
    /// Use this to save latency and a bit of persist traffic if you're just
    /// going to immediately drop or expire the [ReadHandle].
    ///
    /// The `_schema` parameter is currently unused, but should be an object
    /// that represents the schema of the data in the shard. This will be required
    /// in the future.
    #[instrument(level = "debug", fields(shard = %shard_id))]
    pub async fn open_writer<K, V, T, D>(
        &self,
        shard_id: ShardId,
        key_schema: Arc<K::Schema>,
        val_schema: Arc<V::Schema>,
        diagnostics: Diagnostics,
    ) -> Result<WriteHandle<K, V, T, D>, InvalidUsage<T>>
    where
        K: Debug + Codec,
        V: Debug + Codec,
        T: Timestamp + TotalOrder + Lattice + Codec64 + Sync,
        D: Semigroup + Ord + Codec64 + Send + Sync,
    {
        let machine = self.make_machine(shard_id, diagnostics.clone()).await?;
        let gc = GarbageCollector::new(machine.clone(), Arc::clone(&self.isolated_runtime));

        // TODO: Because schemas are ordered, as part of the persist schema
        // changes work, we probably want to build some way to allow persist
        // users to control the order. For example, maybe a
        // `PersistClient::compare_and_append_schema(current_schema_id,
        // next_schema)`. Presumably this would then be passed in to open_writer
        // instead of us implicitly registering it here.
        // NB: The overwhelming common case is that this schema is already
        // registered. In this case, the cmd breaks early and nothing is
        // written to (or read from) CRDB.
        let (schema_id, maintenance) = machine.register_schema(&*key_schema, &*val_schema).await;
        maintenance.start_performing(&machine, &gc);
        soft_assert_or_log!(
            schema_id.is_some(),
            "unable to register schemas {:?} {:?}",
            key_schema,
            val_schema,
        );

        let writer_id = WriterId::new();
        let schemas = Schemas {
            id: schema_id,
            key: key_schema,
            val: val_schema,
        };
        let writer = WriteHandle::new(
            self.cfg.clone(),
            Arc::clone(&self.metrics),
            machine,
            gc,
            Arc::clone(&self.blob),
            writer_id,
            &diagnostics.handle_purpose,
            schemas,
        );
        Ok(writer)
    }

    /// Returns a [BatchBuilder] that can be used to write a batch of updates to
    /// blob storage which can then be appended to the given shard using
    /// [WriteHandle::compare_and_append_batch] or [WriteHandle::append_batch],
    /// or which can be read using [PersistClient::read_batches_consolidated].
    ///
    /// The builder uses a bounded amount of memory, even when the number of
    /// updates is very large. Individual records, however, should be small
    /// enough that we can reasonably chunk them up: O(KB) is definitely fine,
    /// O(MB) come talk to us.
    #[instrument(level = "debug", fields(shard = %shard_id))]
    pub async fn batch_builder<K, V, T, D>(
        &self,
        shard_id: ShardId,
        write_schemas: Schemas<K, V>,
        lower: Antichain<T>,
        max_runs: Option<usize>,
    ) -> BatchBuilder<K, V, T, D>
    where
        K: Debug + Codec,
        V: Debug + Codec,
        T: Timestamp + Lattice + Codec64 + TotalOrder + Sync,
        D: Semigroup + Ord + Codec64 + Send + Sync,
    {
        let mut compact_cfg = CompactConfig::new(&self.cfg, shard_id);
        compact_cfg.batch.max_runs = max_runs;
        WriteHandle::builder_inner(
            &self.cfg,
            compact_cfg,
            Arc::clone(&self.metrics),
            self.metrics.shards.shard(&shard_id, "peek_stash"),
            &self.metrics.user,
            Arc::clone(&self.isolated_runtime),
            Arc::clone(&self.blob),
            shard_id,
            write_schemas,
            lower,
        )
    }

    /// Turns the given [`ProtoBatch`] back into a [`Batch`] which can be used
    /// to append it to the given shard or to read it via
    /// [PersistClient::read_batches_consolidated]
    ///
    /// CAUTION: This API allows turning a [ProtoBatch] into a [Batch] multiple
    /// times, but if a batch is deleted the backing data goes away, so at that
    /// point all in-memory copies of a batch become invalid and cannot be read
    /// anymore.
    pub fn batch_from_transmittable_batch<K, V, T, D>(
        &self,
        shard_id: &ShardId,
        batch: ProtoBatch,
    ) -> Batch<K, V, T, D>
    where
        K: Debug + Codec,
        V: Debug + Codec,
        T: Timestamp + Lattice + Codec64 + Sync,
        D: Semigroup + Ord + Codec64 + Send + Sync,
    {
        let batch_shard_id: ShardId = batch
            .shard_id
            .into_rust()
            .expect("valid transmittable batch");
        assert_eq!(&batch_shard_id, shard_id);

        let shard_metrics = self.metrics.shards.shard(shard_id, "peek_stash");

        let ret = Batch {
            batch_delete_enabled: BATCH_DELETE_ENABLED.get(&self.cfg),
            metrics: Arc::clone(&self.metrics),
            shard_metrics,
            version: Version::parse(&batch.version).expect("valid transmittable batch"),
            batch: batch
                .batch
                .into_rust_if_some("ProtoBatch::batch")
                .expect("valid transmittable batch"),
            blob: Arc::clone(&self.blob),
            _phantom: std::marker::PhantomData,
        };

        assert_eq!(&ret.shard_id(), shard_id);
        ret
    }

    /// Returns a [Cursor] for reading the given batches. Yielded updates are
    /// consolidated if the given batches contain sorted runs, which is true
    /// when they have been written using a [BatchBuilder].
    ///
    /// To keep memory usage down when reading a snapshot that consolidates
    /// well, this consolidates as it goes. However, note that only the
    /// serialized data is consolidated: the deserialized data will only be
    /// consolidated if your K/V codecs are one-to-one.
    ///
    /// CAUTION: The caller needs to make sure that the given batches are
    /// readable and they have to remain readable for the lifetime of the
    /// returned [Cursor]. The caller is also responsible for the lifecycle of
    /// the batches: once the cursor and the batches are no longer needed you
    /// must call [Cursor::into_lease] to get back the batches and delete them.
    #[allow(clippy::unused_async)]
    pub async fn read_batches_consolidated<K, V, T, D>(
        &mut self,
        shard_id: ShardId,
        as_of: Antichain<T>,
        read_schemas: Schemas<K, V>,
        batches: Vec<Batch<K, V, T, D>>,
        should_fetch_part: impl for<'a> Fn(Option<&'a LazyPartStats>) -> bool,
        memory_budget_bytes: usize,
    ) -> Result<Cursor<K, V, T, D, Vec<Batch<K, V, T, D>>>, Since<T>>
    where
        K: Debug + Codec + Ord,
        V: Debug + Codec + Ord,
        T: Timestamp + Lattice + Codec64 + TotalOrder + Sync,
        D: Semigroup + Ord + Codec64 + Send + Sync,
    {
        let shard_metrics = self.metrics.shards.shard(&shard_id, "peek_stash");

        let hollow_batches = batches.iter().map(|b| b.batch.clone()).collect_vec();

        ReadHandle::read_batches_consolidated(
            &self.cfg,
            Arc::clone(&self.metrics),
            shard_metrics,
            self.metrics.read.snapshot.clone(),
            Arc::clone(&self.blob),
            shard_id,
            as_of,
            read_schemas,
            &hollow_batches,
            batches,
            should_fetch_part,
            memory_budget_bytes,
        )
    }

    /// Returns the requested schema, if known at the current state.
    pub async fn get_schema<K, V, T, D>(
        &self,
        shard_id: ShardId,
        schema_id: SchemaId,
        diagnostics: Diagnostics,
    ) -> Result<Option<(K::Schema, V::Schema)>, InvalidUsage<T>>
    where
        K: Debug + Codec,
        V: Debug + Codec,
        T: Timestamp + Lattice + Codec64 + Sync,
        D: Semigroup + Codec64 + Send + Sync,
    {
        let machine = self
            .make_machine::<K, V, T, D>(shard_id, diagnostics)
            .await?;
        Ok(machine.get_schema(schema_id))
    }

    /// Returns the latest schema registered at the current state.
    pub async fn latest_schema<K, V, T, D>(
        &self,
        shard_id: ShardId,
        diagnostics: Diagnostics,
    ) -> Result<Option<(SchemaId, K::Schema, V::Schema)>, InvalidUsage<T>>
    where
        K: Debug + Codec,
        V: Debug + Codec,
        T: Timestamp + Lattice + Codec64 + Sync,
        D: Semigroup + Codec64 + Send + Sync,
    {
        let machine = self
            .make_machine::<K, V, T, D>(shard_id, diagnostics)
            .await?;
        Ok(machine.latest_schema())
    }

    /// Registers a new latest schema for the given shard.
    ///
    /// This new schema must be [backward_compatible] with all previous schemas
    /// for this shard. If it's not, [CaESchema::Incompatible] is returned.
    ///
    /// [backward_compatible]: mz_persist_types::schema::backward_compatible
    ///
    /// To prevent races, the caller must declare what it believes to be the
    /// latest schema id. If this doesn't match reality,
    /// [CaESchema::ExpectedMismatch] is returned.
    pub async fn compare_and_evolve_schema<K, V, T, D>(
        &self,
        shard_id: ShardId,
        expected: SchemaId,
        key_schema: &K::Schema,
        val_schema: &V::Schema,
        diagnostics: Diagnostics,
    ) -> Result<CaESchema<K, V>, InvalidUsage<T>>
    where
        K: Debug + Codec,
        V: Debug + Codec,
        T: Timestamp + Lattice + Codec64 + Sync,
        D: Semigroup + Codec64 + Send + Sync,
    {
        let machine = self
            .make_machine::<K, V, T, D>(shard_id, diagnostics)
            .await?;
        let gc = GarbageCollector::new(machine.clone(), Arc::clone(&self.isolated_runtime));
        let (res, maintenance) = machine
            .compare_and_evolve_schema(expected, key_schema, val_schema)
            .await;
        maintenance.start_performing(&machine, &gc);
        Ok(res)
    }

    /// Check if the given shard is in a finalized state; ie. it can no longer be
    /// read, any data that was written to it is no longer accessible, and we've
    /// discarded references to that data from state.
    pub async fn is_finalized<K, V, T, D>(
        &self,
        shard_id: ShardId,
        diagnostics: Diagnostics,
    ) -> Result<bool, InvalidUsage<T>>
    where
        K: Debug + Codec,
        V: Debug + Codec,
        T: Timestamp + Lattice + Codec64 + Sync,
        D: Semigroup + Codec64 + Send + Sync,
    {
        let machine = self
            .make_machine::<K, V, T, D>(shard_id, diagnostics)
            .await?;
        Ok(machine.is_finalized())
    }

    /// If a shard is guaranteed to never be used again, finalize it to delete
    /// the associated data and release any associated resources. (Except for a
    /// little state in consensus we use to represent the tombstone.)
    ///
    /// The caller should ensure that both the `since` and `upper` of the shard
    /// have been advanced to `[]`: ie. the shard is no longer writable or readable.
    /// Otherwise an error is returned.
    ///
    /// Once `finalize_shard` has been called, the result of future operations on
    /// the shard are not defined. They may return errors or succeed as a noop.
    #[instrument(level = "debug", fields(shard = %shard_id))]
    pub async fn finalize_shard<K, V, T, D>(
        &self,
        shard_id: ShardId,
        diagnostics: Diagnostics,
    ) -> Result<(), InvalidUsage<T>>
    where
        K: Debug + Codec,
        V: Debug + Codec,
        T: Timestamp + Lattice + Codec64 + Sync,
        D: Semigroup + Codec64 + Send + Sync,
    {
        let machine = self
            .make_machine::<K, V, T, D>(shard_id, diagnostics)
            .await?;

        let maintenance = machine.become_tombstone().await?;
        let gc = GarbageCollector::new(machine.clone(), Arc::clone(&self.isolated_runtime));

        let () = maintenance.perform(&machine, &gc).await;

        Ok(())
    }

    /// Returns the internal state of the shard for debugging and QA.
    ///
    /// We'll be thoughtful about making unnecessary changes, but the **output
    /// of this method needs to be gated from users**, so that it's not subject
    /// to our backward compatibility guarantees.
    pub async fn inspect_shard<T: Timestamp + Lattice + Codec64>(
        &self,
        shard_id: &ShardId,
    ) -> Result<impl serde::Serialize, anyhow::Error> {
        let state_versions = StateVersions::new(
            self.cfg.clone(),
            Arc::clone(&self.consensus),
            Arc::clone(&self.blob),
            Arc::clone(&self.metrics),
        );
        // TODO: Don't fetch all live diffs. Feels like we should pull out a new
        // method in StateVersions for fetching the latest version of State of a
        // shard that might or might not exist.
        let versions = state_versions.fetch_all_live_diffs(shard_id).await;
        if versions.0.is_empty() {
            return Err(anyhow::anyhow!("{} does not exist", shard_id));
        }
        let state = state_versions
            .fetch_current_state::<T>(shard_id, versions.0)
            .await;
        let state = state.check_ts_codec(shard_id)?;
        Ok(state)
    }

    /// Test helper for a [Self::open] call that is expected to succeed.
    #[cfg(test)]
    #[track_caller]
    pub async fn expect_open<K, V, T, D>(
        &self,
        shard_id: ShardId,
    ) -> (WriteHandle<K, V, T, D>, ReadHandle<K, V, T, D>)
    where
        K: Debug + Codec,
        V: Debug + Codec,
        T: Timestamp + TotalOrder + Lattice + Codec64 + Sync,
        D: Semigroup + Ord + Codec64 + Send + Sync,
        K::Schema: Default,
        V::Schema: Default,
    {
        self.open(
            shard_id,
            Arc::new(K::Schema::default()),
            Arc::new(V::Schema::default()),
            Diagnostics::for_tests(),
            true,
        )
        .await
        .expect("codec mismatch")
    }

    /// Return the metrics being used by this client.
    ///
    /// Only exposed for tests, persistcli, and benchmarks.
    pub fn metrics(&self) -> &Arc<Metrics> {
        &self.metrics
    }
}

#[cfg(test)]
mod tests {
    use std::future::Future;
    use std::mem;
    use std::pin::Pin;
    use std::task::Context;
    use std::time::Duration;

    use differential_dataflow::consolidation::consolidate_updates;
    use differential_dataflow::lattice::Lattice;
    use futures_task::noop_waker;
    use mz_dyncfg::ConfigUpdates;
    use mz_ore::assert_ok;
    use mz_persist::indexed::encoding::BlobTraceBatchPart;
    use mz_persist::workload::DataGenerator;
    use mz_persist_types::codec_impls::{StringSchema, VecU8Schema};
    use mz_proto::protobuf_roundtrip;
    use proptest::prelude::*;
    use timely::order::PartialOrder;
    use timely::progress::Antichain;

    use crate::batch::BLOB_TARGET_SIZE;
    use crate::cache::PersistClientCache;
    use crate::cfg::BATCH_BUILDER_MAX_OUTSTANDING_PARTS;
    use crate::error::{CodecConcreteType, CodecMismatch, UpperMismatch};
    use crate::internal::paths::BlobKey;
    use crate::read::ListenEvent;

    use super::*;

    pub fn new_test_client_cache(dyncfgs: &ConfigUpdates) -> PersistClientCache {
        // Configure an aggressively small blob_target_size so we get some
        // amount of coverage of that in tests. Similarly, for max_outstanding.
        let mut cache = PersistClientCache::new_no_metrics();
        cache.cfg.set_config(&BLOB_TARGET_SIZE, 10);
        cache
            .cfg
            .set_config(&BATCH_BUILDER_MAX_OUTSTANDING_PARTS, 1);
        dyncfgs.apply(cache.cfg());

        // Enable compaction in tests to ensure we get coverage.
        cache.cfg.compaction_enabled = true;
        cache
    }

    pub async fn new_test_client(dyncfgs: &ConfigUpdates) -> PersistClient {
        let cache = new_test_client_cache(dyncfgs);
        cache
            .open(PersistLocation::new_in_mem())
            .await
            .expect("client construction failed")
    }

    pub fn all_ok<'a, K, V, T, D, I>(
        iter: I,
        as_of: T,
    ) -> Vec<((Result<K, String>, Result<V, String>), T, D)>
    where
        K: Ord + Clone + 'a,
        V: Ord + Clone + 'a,
        T: Timestamp + Lattice + Clone + 'a,
        D: Semigroup + Clone + 'a,
        I: IntoIterator<Item = &'a ((K, V), T, D)>,
    {
        let as_of = Antichain::from_elem(as_of);
        let mut ret = iter
            .into_iter()
            .map(|((k, v), t, d)| {
                let mut t = t.clone();
                t.advance_by(as_of.borrow());
                ((Ok(k.clone()), Ok(v.clone())), t, d.clone())
            })
            .collect();
        consolidate_updates(&mut ret);
        ret
    }

    pub async fn expect_fetch_part<K, V, T, D>(
        blob: &dyn Blob,
        key: &BlobKey,
        metrics: &Metrics,
        read_schemas: &Schemas<K, V>,
    ) -> (
        BlobTraceBatchPart<T>,
        Vec<((Result<K, String>, Result<V, String>), T, D)>,
    )
    where
        K: Codec,
        V: Codec,
        T: Timestamp + Codec64,
        D: Codec64,
    {
        let value = blob
            .get(key)
            .await
            .expect("failed to fetch part")
            .expect("missing part");
        let mut part =
            BlobTraceBatchPart::decode(&value, &metrics.columnar).expect("failed to decode part");
        // Ensure codec data is present even if it was not generated at write time.
        let _ = part
            .updates
            .get_or_make_codec::<K, V>(&read_schemas.key, &read_schemas.val);
        let mut updates = Vec::new();
        // TODO(bkirwi): switch to structured data in tests
        for ((k, v), t, d) in part.updates.records().expect("codec data").iter() {
            updates.push((
                (
                    K::decode(k, &read_schemas.key),
                    V::decode(v, &read_schemas.val),
                ),
                T::decode(t),
                D::decode(d),
            ));
        }
        (part, updates)
    }

    #[mz_persist_proc::test(tokio::test)]
    #[cfg_attr(miri, ignore)] // unsupported operation: returning ready events from epoll_wait is not yet implemented
    async fn sanity_check(dyncfgs: ConfigUpdates) {
        let data = [
            (("1".to_owned(), "one".to_owned()), 1, 1),
            (("2".to_owned(), "two".to_owned()), 2, 1),
            (("3".to_owned(), "three".to_owned()), 3, 1),
        ];

        let (mut write, mut read) = new_test_client(&dyncfgs)
            .await
            .expect_open::<String, String, u64, i64>(ShardId::new())
            .await;
        assert_eq!(write.upper(), &Antichain::from_elem(u64::minimum()));
        assert_eq!(read.since(), &Antichain::from_elem(u64::minimum()));

        // Write a [0,3) batch.
        write
            .expect_append(&data[..2], write.upper().clone(), vec![3])
            .await;
        assert_eq!(write.upper(), &Antichain::from_elem(3));

        // Grab a snapshot and listener as_of 1. Snapshot should only have part of what we wrote.
        assert_eq!(
            read.expect_snapshot_and_fetch(1).await,
            all_ok(&data[..1], 1)
        );

        let mut listen = read.clone("").await.expect_listen(1).await;

        // Write a [3,4) batch.
        write
            .expect_append(&data[2..], write.upper().clone(), vec![4])
            .await;
        assert_eq!(write.upper(), &Antichain::from_elem(4));

        // Listen should have part of the initial write plus the new one.
        assert_eq!(
            listen.read_until(&4).await,
            (all_ok(&data[1..], 1), Antichain::from_elem(4))
        );

        // Downgrading the since is tracked locally (but otherwise is a no-op).
        read.downgrade_since(&Antichain::from_elem(2)).await;
        assert_eq!(read.since(), &Antichain::from_elem(2));
    }

    // Sanity check that the open_reader and open_writer calls work.
    #[mz_persist_proc::test(tokio::test)]
    #[cfg_attr(miri, ignore)] // unsupported operation: returning ready events from epoll_wait is not yet implemented
    async fn open_reader_writer(dyncfgs: ConfigUpdates) {
        let data = vec![
            (("1".to_owned(), "one".to_owned()), 1, 1),
            (("2".to_owned(), "two".to_owned()), 2, 1),
            (("3".to_owned(), "three".to_owned()), 3, 1),
        ];

        let shard_id = ShardId::new();
        let client = new_test_client(&dyncfgs).await;
        let mut write1 = client
            .open_writer::<String, String, u64, i64>(
                shard_id,
                Arc::new(StringSchema),
                Arc::new(StringSchema),
                Diagnostics::for_tests(),
            )
            .await
            .expect("codec mismatch");
        let mut read1 = client
            .open_leased_reader::<String, String, u64, i64>(
                shard_id,
                Arc::new(StringSchema),
                Arc::new(StringSchema),
                Diagnostics::for_tests(),
                true,
            )
            .await
            .expect("codec mismatch");
        let mut read2 = client
            .open_leased_reader::<String, String, u64, i64>(
                shard_id,
                Arc::new(StringSchema),
                Arc::new(StringSchema),
                Diagnostics::for_tests(),
                true,
            )
            .await
            .expect("codec mismatch");
        let mut write2 = client
            .open_writer::<String, String, u64, i64>(
                shard_id,
                Arc::new(StringSchema),
                Arc::new(StringSchema),
                Diagnostics::for_tests(),
            )
            .await
            .expect("codec mismatch");

        write2.expect_compare_and_append(&data[..1], 0, 2).await;
        assert_eq!(
            read2.expect_snapshot_and_fetch(1).await,
            all_ok(&data[..1], 1)
        );
        write1.expect_compare_and_append(&data[1..], 2, 4).await;
        assert_eq!(read1.expect_snapshot_and_fetch(3).await, all_ok(&data, 3));
    }

    #[mz_persist_proc::test(tokio::test)]
    #[cfg_attr(miri, ignore)] // too slow
    async fn invalid_usage(dyncfgs: ConfigUpdates) {
        let data = vec![
            (("1".to_owned(), "one".to_owned()), 1, 1),
            (("2".to_owned(), "two".to_owned()), 2, 1),
            (("3".to_owned(), "three".to_owned()), 3, 1),
        ];

        let shard_id0 = "s00000000-0000-0000-0000-000000000000"
            .parse::<ShardId>()
            .expect("invalid shard id");
        let mut client = new_test_client(&dyncfgs).await;

        let (mut write0, mut read0) = client
            .expect_open::<String, String, u64, i64>(shard_id0)
            .await;

        write0.expect_compare_and_append(&data, 0, 4).await;

        // InvalidUsage from PersistClient methods.
        {
            fn codecs(
                k: &str,
                v: &str,
                t: &str,
                d: &str,
            ) -> (String, String, String, String, Option<CodecConcreteType>) {
                (k.to_owned(), v.to_owned(), t.to_owned(), d.to_owned(), None)
            }

            client.shared_states = Arc::new(StateCache::new_no_metrics());
            assert_eq!(
                client
                    .open::<Vec<u8>, String, u64, i64>(
                        shard_id0,
                        Arc::new(VecU8Schema),
                        Arc::new(StringSchema),
                        Diagnostics::for_tests(),
                        true,
                    )
                    .await
                    .unwrap_err(),
                InvalidUsage::CodecMismatch(Box::new(CodecMismatch {
                    requested: codecs("Vec<u8>", "String", "u64", "i64"),
                    actual: codecs("String", "String", "u64", "i64"),
                }))
            );
            assert_eq!(
                client
                    .open::<String, Vec<u8>, u64, i64>(
                        shard_id0,
                        Arc::new(StringSchema),
                        Arc::new(VecU8Schema),
                        Diagnostics::for_tests(),
                        true,
                    )
                    .await
                    .unwrap_err(),
                InvalidUsage::CodecMismatch(Box::new(CodecMismatch {
                    requested: codecs("String", "Vec<u8>", "u64", "i64"),
                    actual: codecs("String", "String", "u64", "i64"),
                }))
            );
            assert_eq!(
                client
                    .open::<String, String, i64, i64>(
                        shard_id0,
                        Arc::new(StringSchema),
                        Arc::new(StringSchema),
                        Diagnostics::for_tests(),
                        true,
                    )
                    .await
                    .unwrap_err(),
                InvalidUsage::CodecMismatch(Box::new(CodecMismatch {
                    requested: codecs("String", "String", "i64", "i64"),
                    actual: codecs("String", "String", "u64", "i64"),
                }))
            );
            assert_eq!(
                client
                    .open::<String, String, u64, u64>(
                        shard_id0,
                        Arc::new(StringSchema),
                        Arc::new(StringSchema),
                        Diagnostics::for_tests(),
                        true,
                    )
                    .await
                    .unwrap_err(),
                InvalidUsage::CodecMismatch(Box::new(CodecMismatch {
                    requested: codecs("String", "String", "u64", "u64"),
                    actual: codecs("String", "String", "u64", "i64"),
                }))
            );

            // open_reader and open_writer end up using the same checks, so just
            // verify one type each to verify the plumbing instead of the full
            // set.
            assert_eq!(
                client
                    .open_leased_reader::<Vec<u8>, String, u64, i64>(
                        shard_id0,
                        Arc::new(VecU8Schema),
                        Arc::new(StringSchema),
                        Diagnostics::for_tests(),
                        true,
                    )
                    .await
                    .unwrap_err(),
                InvalidUsage::CodecMismatch(Box::new(CodecMismatch {
                    requested: codecs("Vec<u8>", "String", "u64", "i64"),
                    actual: codecs("String", "String", "u64", "i64"),
                }))
            );
            assert_eq!(
                client
                    .open_writer::<Vec<u8>, String, u64, i64>(
                        shard_id0,
                        Arc::new(VecU8Schema),
                        Arc::new(StringSchema),
                        Diagnostics::for_tests(),
                    )
                    .await
                    .unwrap_err(),
                InvalidUsage::CodecMismatch(Box::new(CodecMismatch {
                    requested: codecs("Vec<u8>", "String", "u64", "i64"),
                    actual: codecs("String", "String", "u64", "i64"),
                }))
            );
        }

        // InvalidUsage from ReadHandle methods.
        {
            let snap = read0
                .snapshot(Antichain::from_elem(3))
                .await
                .expect("cannot serve requested as_of");

            let shard_id1 = "s11111111-1111-1111-1111-111111111111"
                .parse::<ShardId>()
                .expect("invalid shard id");
            let mut fetcher1 = client
                .create_batch_fetcher::<String, String, u64, i64>(
                    shard_id1,
                    Default::default(),
                    Default::default(),
                    false,
                    Diagnostics::for_tests(),
                )
                .await
                .unwrap();
            for part in snap {
                let (part, _lease) = part.into_exchangeable_part();
                let res = fetcher1.fetch_leased_part(part).await;
                assert_eq!(
                    res.unwrap_err(),
                    InvalidUsage::BatchNotFromThisShard {
                        batch_shard: shard_id0,
                        handle_shard: shard_id1,
                    }
                );
            }
        }

        // InvalidUsage from WriteHandle methods.
        {
            let ts3 = &data[2];
            assert_eq!(ts3.1, 3);
            let ts3 = vec![ts3.clone()];

            // WriteHandle::append also covers append_batch,
            // compare_and_append_batch, compare_and_append.
            assert_eq!(
                write0
                    .append(&ts3, Antichain::from_elem(4), Antichain::from_elem(5))
                    .await
                    .unwrap_err(),
                InvalidUsage::UpdateNotBeyondLower {
                    ts: 3,
                    lower: Antichain::from_elem(4),
                },
            );
            assert_eq!(
                write0
                    .append(&ts3, Antichain::from_elem(2), Antichain::from_elem(3))
                    .await
                    .unwrap_err(),
                InvalidUsage::UpdateBeyondUpper {
                    ts: 3,
                    expected_upper: Antichain::from_elem(3),
                },
            );
            // NB unlike the previous tests, this one has empty updates.
            assert_eq!(
                write0
                    .append(&data[..0], Antichain::from_elem(3), Antichain::from_elem(2))
                    .await
                    .unwrap_err(),
                InvalidUsage::InvalidBounds {
                    lower: Antichain::from_elem(3),
                    upper: Antichain::from_elem(2),
                },
            );

            // Tests for the BatchBuilder.
            assert_eq!(
                write0
                    .builder(Antichain::from_elem(3))
                    .finish(Antichain::from_elem(2))
                    .await
                    .unwrap_err(),
                InvalidUsage::InvalidBounds {
                    lower: Antichain::from_elem(3),
                    upper: Antichain::from_elem(2)
                },
            );
            let batch = write0
                .batch(&ts3, Antichain::from_elem(3), Antichain::from_elem(4))
                .await
                .expect("invalid usage");
            assert_eq!(
                write0
                    .append_batch(batch, Antichain::from_elem(4), Antichain::from_elem(5))
                    .await
                    .unwrap_err(),
                InvalidUsage::InvalidBatchBounds {
                    batch_lower: Antichain::from_elem(3),
                    batch_upper: Antichain::from_elem(4),
                    append_lower: Antichain::from_elem(4),
                    append_upper: Antichain::from_elem(5),
                },
            );
            let batch = write0
                .batch(&ts3, Antichain::from_elem(3), Antichain::from_elem(4))
                .await
                .expect("invalid usage");
            assert_eq!(
                write0
                    .append_batch(batch, Antichain::from_elem(2), Antichain::from_elem(3))
                    .await
                    .unwrap_err(),
                InvalidUsage::InvalidBatchBounds {
                    batch_lower: Antichain::from_elem(3),
                    batch_upper: Antichain::from_elem(4),
                    append_lower: Antichain::from_elem(2),
                    append_upper: Antichain::from_elem(3),
                },
            );
            let batch = write0
                .batch(&ts3, Antichain::from_elem(3), Antichain::from_elem(4))
                .await
                .expect("invalid usage");
            // NB unlike the others, this one uses matches! because it's
            // non-deterministic (the key)
            assert!(matches!(
                write0
                    .append_batch(batch, Antichain::from_elem(3), Antichain::from_elem(3))
                    .await
                    .unwrap_err(),
                InvalidUsage::InvalidEmptyTimeInterval { .. }
            ));
        }
    }

    #[mz_persist_proc::test(tokio::test)]
    #[cfg_attr(miri, ignore)] // unsupported operation: returning ready events from epoll_wait is not yet implemented
    async fn multiple_shards(dyncfgs: ConfigUpdates) {
        let data1 = [
            (("1".to_owned(), "one".to_owned()), 1, 1),
            (("2".to_owned(), "two".to_owned()), 2, 1),
        ];

        let data2 = [(("1".to_owned(), ()), 1, 1), (("2".to_owned(), ()), 2, 1)];

        let client = new_test_client(&dyncfgs).await;

        let (mut write1, mut read1) = client
            .expect_open::<String, String, u64, i64>(ShardId::new())
            .await;

        // Different types, so that checks would fail in case we were not separating these
        // collections internally.
        let (mut write2, mut read2) = client
            .expect_open::<String, (), u64, i64>(ShardId::new())
            .await;

        write1
            .expect_compare_and_append(&data1[..], u64::minimum(), 3)
            .await;

        write2
            .expect_compare_and_append(&data2[..], u64::minimum(), 3)
            .await;

        assert_eq!(
            read1.expect_snapshot_and_fetch(2).await,
            all_ok(&data1[..], 2)
        );

        assert_eq!(
            read2.expect_snapshot_and_fetch(2).await,
            all_ok(&data2[..], 2)
        );
    }

    #[mz_persist_proc::test(tokio::test)]
    #[cfg_attr(miri, ignore)] // unsupported operation: returning ready events from epoll_wait is not yet implemented
    async fn fetch_upper(dyncfgs: ConfigUpdates) {
        let data = [
            (("1".to_owned(), "one".to_owned()), 1, 1),
            (("2".to_owned(), "two".to_owned()), 2, 1),
        ];

        let client = new_test_client(&dyncfgs).await;

        let shard_id = ShardId::new();

        let (mut write1, _read1) = client
            .expect_open::<String, String, u64, i64>(shard_id)
            .await;

        let (mut write2, _read2) = client
            .expect_open::<String, String, u64, i64>(shard_id)
            .await;

        write1
            .expect_append(&data[..], write1.upper().clone(), vec![3])
            .await;

        // The shard-global upper does advance, even if this writer didn't advance its local upper.
        assert_eq!(write2.fetch_recent_upper().await, &Antichain::from_elem(3));

        // The writer-local upper should advance, even if it was another writer
        // that advanced the frontier.
        assert_eq!(write2.upper(), &Antichain::from_elem(3));
    }

    #[mz_persist_proc::test(tokio::test)]
    #[cfg_attr(miri, ignore)] // unsupported operation: returning ready events from epoll_wait is not yet implemented
    async fn append_with_invalid_upper(dyncfgs: ConfigUpdates) {
        let data = [
            (("1".to_owned(), "one".to_owned()), 1, 1),
            (("2".to_owned(), "two".to_owned()), 2, 1),
        ];

        let client = new_test_client(&dyncfgs).await;

        let shard_id = ShardId::new();

        let (mut write, _read) = client
            .expect_open::<String, String, u64, i64>(shard_id)
            .await;

        write
            .expect_append(&data[..], write.upper().clone(), vec![3])
            .await;

        let data = [
            (("5".to_owned(), "fünf".to_owned()), 5, 1),
            (("6".to_owned(), "sechs".to_owned()), 6, 1),
        ];
        let res = write
            .append(
                data.iter(),
                Antichain::from_elem(5),
                Antichain::from_elem(7),
            )
            .await;
        assert_eq!(
            res,
            Ok(Err(UpperMismatch {
                expected: Antichain::from_elem(5),
                current: Antichain::from_elem(3)
            }))
        );

        // Writing with an outdated upper updates the write handle's upper to the correct upper.
        assert_eq!(write.upper(), &Antichain::from_elem(3));
    }

    // Make sure that the API structs are Sync + Send, so that they can be used in async tasks.
    // NOTE: This is a compile-time only test. If it compiles, we're good.
    #[allow(unused)]
    async fn sync_send(dyncfgs: ConfigUpdates) {
        mz_ore::test::init_logging();

        fn is_send_sync<T: Send + Sync>(_x: T) -> bool {
            true
        }

        let client = new_test_client(&dyncfgs).await;

        let (write, read) = client
            .expect_open::<String, String, u64, i64>(ShardId::new())
            .await;

        assert!(is_send_sync(client));
        assert!(is_send_sync(write));
        assert!(is_send_sync(read));
    }

    #[mz_persist_proc::test(tokio::test)]
    #[cfg_attr(miri, ignore)] // unsupported operation: returning ready events from epoll_wait is not yet implemented
    async fn compare_and_append(dyncfgs: ConfigUpdates) {
        let data = vec![
            (("1".to_owned(), "one".to_owned()), 1, 1),
            (("2".to_owned(), "two".to_owned()), 2, 1),
            (("3".to_owned(), "three".to_owned()), 3, 1),
        ];

        let id = ShardId::new();
        let client = new_test_client(&dyncfgs).await;
        let (mut write1, mut read) = client.expect_open::<String, String, u64, i64>(id).await;

        let (mut write2, _read) = client.expect_open::<String, String, u64, i64>(id).await;

        assert_eq!(write1.upper(), &Antichain::from_elem(u64::minimum()));
        assert_eq!(write2.upper(), &Antichain::from_elem(u64::minimum()));
        assert_eq!(read.since(), &Antichain::from_elem(u64::minimum()));

        // Write a [0,3) batch.
        write1
            .expect_compare_and_append(&data[..2], u64::minimum(), 3)
            .await;
        assert_eq!(write1.upper(), &Antichain::from_elem(3));

        assert_eq!(
            read.expect_snapshot_and_fetch(2).await,
            all_ok(&data[..2], 2)
        );

        // Try and write with a wrong expected upper.
        let res = write2
            .compare_and_append(
                &data[..2],
                Antichain::from_elem(u64::minimum()),
                Antichain::from_elem(3),
            )
            .await;
        assert_eq!(
            res,
            Ok(Err(UpperMismatch {
                expected: Antichain::from_elem(u64::minimum()),
                current: Antichain::from_elem(3)
            }))
        );

        // A failed write updates our local cache of the shard upper.
        assert_eq!(write2.upper(), &Antichain::from_elem(3));

        // Try again with a good expected upper.
        write2.expect_compare_and_append(&data[2..], 3, 4).await;

        assert_eq!(write2.upper(), &Antichain::from_elem(4));

        assert_eq!(read.expect_snapshot_and_fetch(3).await, all_ok(&data, 3));
    }

    #[mz_persist_proc::test(tokio::test)]
    #[cfg_attr(miri, ignore)] // unsupported operation: returning ready events from epoll_wait is not yet implemented
    async fn overlapping_append(dyncfgs: ConfigUpdates) {
        mz_ore::test::init_logging_default("info");

        let data = vec![
            (("1".to_owned(), "one".to_owned()), 1, 1),
            (("2".to_owned(), "two".to_owned()), 2, 1),
            (("3".to_owned(), "three".to_owned()), 3, 1),
            (("4".to_owned(), "vier".to_owned()), 4, 1),
            (("5".to_owned(), "cinque".to_owned()), 5, 1),
        ];

        let id = ShardId::new();
        let client = new_test_client(&dyncfgs).await;

        let (mut write1, mut read) = client.expect_open::<String, String, u64, i64>(id).await;

        let (mut write2, _read) = client.expect_open::<String, String, u64, i64>(id).await;

        // Grab a listener before we do any writing
        let mut listen = read.clone("").await.expect_listen(0).await;

        // Write a [0,3) batch.
        write1
            .expect_append(&data[..2], write1.upper().clone(), vec![3])
            .await;
        assert_eq!(write1.upper(), &Antichain::from_elem(3));

        // Write a [0,5) batch with the second writer.
        write2
            .expect_append(&data[..4], write2.upper().clone(), vec![5])
            .await;
        assert_eq!(write2.upper(), &Antichain::from_elem(5));

        // Write a [3,6) batch with the first writer.
        write1
            .expect_append(&data[2..5], write1.upper().clone(), vec![6])
            .await;
        assert_eq!(write1.upper(), &Antichain::from_elem(6));

        assert_eq!(read.expect_snapshot_and_fetch(5).await, all_ok(&data, 5));

        assert_eq!(
            listen.read_until(&6).await,
            (all_ok(&data[..], 1), Antichain::from_elem(6))
        );
    }

    // Appends need to be contiguous for a shard, meaning the lower of an appended batch must not
    // be in advance of the current shard upper.
    #[mz_persist_proc::test(tokio::test)]
    #[cfg_attr(miri, ignore)] // unsupported operation: returning ready events from epoll_wait is not yet implemented
    async fn contiguous_append(dyncfgs: ConfigUpdates) {
        let data = vec![
            (("1".to_owned(), "one".to_owned()), 1, 1),
            (("2".to_owned(), "two".to_owned()), 2, 1),
            (("3".to_owned(), "three".to_owned()), 3, 1),
            (("4".to_owned(), "vier".to_owned()), 4, 1),
            (("5".to_owned(), "cinque".to_owned()), 5, 1),
        ];

        let id = ShardId::new();
        let client = new_test_client(&dyncfgs).await;

        let (mut write, mut read) = client.expect_open::<String, String, u64, i64>(id).await;

        // Write a [0,3) batch.
        write
            .expect_append(&data[..2], write.upper().clone(), vec![3])
            .await;
        assert_eq!(write.upper(), &Antichain::from_elem(3));

        // Appending a non-contiguous batch should fail.
        // Write a [5,6) batch with the second writer.
        let result = write
            .append(
                &data[4..5],
                Antichain::from_elem(5),
                Antichain::from_elem(6),
            )
            .await;
        assert_eq!(
            result,
            Ok(Err(UpperMismatch {
                expected: Antichain::from_elem(5),
                current: Antichain::from_elem(3)
            }))
        );

        // Fixing the lower to make the write contiguous should make the append succeed.
        write.expect_append(&data[2..5], vec![3], vec![6]).await;
        assert_eq!(write.upper(), &Antichain::from_elem(6));

        assert_eq!(read.expect_snapshot_and_fetch(5).await, all_ok(&data, 5));
    }

    // Per-writer appends can be non-contiguous, as long as appends to the shard from all writers
    // combined are contiguous.
    #[mz_persist_proc::test(tokio::test)]
    #[cfg_attr(miri, ignore)] // unsupported operation: returning ready events from epoll_wait is not yet implemented
    async fn noncontiguous_append_per_writer(dyncfgs: ConfigUpdates) {
        let data = vec![
            (("1".to_owned(), "one".to_owned()), 1, 1),
            (("2".to_owned(), "two".to_owned()), 2, 1),
            (("3".to_owned(), "three".to_owned()), 3, 1),
            (("4".to_owned(), "vier".to_owned()), 4, 1),
            (("5".to_owned(), "cinque".to_owned()), 5, 1),
        ];

        let id = ShardId::new();
        let client = new_test_client(&dyncfgs).await;

        let (mut write1, mut read) = client.expect_open::<String, String, u64, i64>(id).await;

        let (mut write2, _read) = client.expect_open::<String, String, u64, i64>(id).await;

        // Write a [0,3) batch with writer 1.
        write1
            .expect_append(&data[..2], write1.upper().clone(), vec![3])
            .await;
        assert_eq!(write1.upper(), &Antichain::from_elem(3));

        // Write a [3,5) batch with writer 2.
        write2.upper = Antichain::from_elem(3);
        write2
            .expect_append(&data[2..4], write2.upper().clone(), vec![5])
            .await;
        assert_eq!(write2.upper(), &Antichain::from_elem(5));

        // Write a [5,6) batch with writer 1.
        write1.upper = Antichain::from_elem(5);
        write1
            .expect_append(&data[4..5], write1.upper().clone(), vec![6])
            .await;
        assert_eq!(write1.upper(), &Antichain::from_elem(6));

        assert_eq!(read.expect_snapshot_and_fetch(5).await, all_ok(&data, 5));
    }

    // Compare_and_appends need to be contiguous for a shard, meaning the lower of an appended
    // batch needs to match the current shard upper.
    #[mz_persist_proc::test(tokio::test)]
    #[cfg_attr(miri, ignore)] // unsupported operation: returning ready events from epoll_wait is not yet implemented
    async fn contiguous_compare_and_append(dyncfgs: ConfigUpdates) {
        let data = vec![
            (("1".to_owned(), "one".to_owned()), 1, 1),
            (("2".to_owned(), "two".to_owned()), 2, 1),
            (("3".to_owned(), "three".to_owned()), 3, 1),
            (("4".to_owned(), "vier".to_owned()), 4, 1),
            (("5".to_owned(), "cinque".to_owned()), 5, 1),
        ];

        let id = ShardId::new();
        let client = new_test_client(&dyncfgs).await;

        let (mut write, mut read) = client.expect_open::<String, String, u64, i64>(id).await;

        // Write a [0,3) batch.
        write.expect_compare_and_append(&data[..2], 0, 3).await;
        assert_eq!(write.upper(), &Antichain::from_elem(3));

        // Appending a non-contiguous batch should fail.
        // Write a [5,6) batch with the second writer.
        let result = write
            .compare_and_append(
                &data[4..5],
                Antichain::from_elem(5),
                Antichain::from_elem(6),
            )
            .await;
        assert_eq!(
            result,
            Ok(Err(UpperMismatch {
                expected: Antichain::from_elem(5),
                current: Antichain::from_elem(3)
            }))
        );

        // Writing with the correct expected upper to make the write contiguous should make the
        // append succeed.
        write.expect_compare_and_append(&data[2..5], 3, 6).await;
        assert_eq!(write.upper(), &Antichain::from_elem(6));

        assert_eq!(read.expect_snapshot_and_fetch(5).await, all_ok(&data, 5));
    }

    // Per-writer compare_and_appends can be non-contiguous, as long as appends to the shard from
    // all writers combined are contiguous.
    #[mz_persist_proc::test(tokio::test)]
    #[cfg_attr(miri, ignore)] // unsupported operation: returning ready events from epoll_wait is not yet implemented
    async fn noncontiguous_compare_and_append_per_writer(dyncfgs: ConfigUpdates) {
        let data = vec![
            (("1".to_owned(), "one".to_owned()), 1, 1),
            (("2".to_owned(), "two".to_owned()), 2, 1),
            (("3".to_owned(), "three".to_owned()), 3, 1),
            (("4".to_owned(), "vier".to_owned()), 4, 1),
            (("5".to_owned(), "cinque".to_owned()), 5, 1),
        ];

        let id = ShardId::new();
        let client = new_test_client(&dyncfgs).await;

        let (mut write1, mut read) = client.expect_open::<String, String, u64, i64>(id).await;

        let (mut write2, _read) = client.expect_open::<String, String, u64, i64>(id).await;

        // Write a [0,3) batch with writer 1.
        write1.expect_compare_and_append(&data[..2], 0, 3).await;
        assert_eq!(write1.upper(), &Antichain::from_elem(3));

        // Write a [3,5) batch with writer 2.
        write2.expect_compare_and_append(&data[2..4], 3, 5).await;
        assert_eq!(write2.upper(), &Antichain::from_elem(5));

        // Write a [5,6) batch with writer 1.
        write1.expect_compare_and_append(&data[4..5], 5, 6).await;
        assert_eq!(write1.upper(), &Antichain::from_elem(6));

        assert_eq!(read.expect_snapshot_and_fetch(5).await, all_ok(&data, 5));
    }

    #[mz_ore::test]
    fn fmt_ids() {
        assert_eq!(
            format!("{}", LeasedReaderId([0u8; 16])),
            "r00000000-0000-0000-0000-000000000000"
        );
        assert_eq!(
            format!("{:?}", LeasedReaderId([0u8; 16])),
            "LeasedReaderId(00000000-0000-0000-0000-000000000000)"
        );
    }

    #[mz_persist_proc::test(tokio::test(flavor = "multi_thread"))]
    #[cfg_attr(miri, ignore)] // error: unsupported operation: integer-to-pointer casts and `ptr::from_exposed_addr` are not supported with `-Zmiri-strict-provenance`
    async fn concurrency(dyncfgs: ConfigUpdates) {
        let data = DataGenerator::small();

        const NUM_WRITERS: usize = 2;
        let id = ShardId::new();
        let client = new_test_client(&dyncfgs).await;
        let mut handles = Vec::<mz_ore::task::JoinHandle<()>>::new();
        for idx in 0..NUM_WRITERS {
            let (data, client) = (data.clone(), client.clone());

            let (batch_tx, mut batch_rx) = tokio::sync::mpsc::channel(1);

            let client1 = client.clone();
            let handle = mz_ore::task::spawn(|| format!("writer-{}", idx), async move {
                let (write, _) = client1.expect_open::<Vec<u8>, Vec<u8>, u64, i64>(id).await;
                let mut current_upper = 0;
                for batch in data.batches() {
                    let new_upper = match batch.get(batch.len() - 1) {
                        Some((_, max_ts, _)) => u64::decode(max_ts) + 1,
                        None => continue,
                    };
                    // Because we (intentionally) call open inside the task,
                    // some other writer may have raced ahead and already
                    // appended some data before this one was registered. As a
                    // result, this writer may not be starting with an upper of
                    // the initial empty antichain. This is nice because it
                    // mimics how a real HA source would work, but it means we
                    // have to skip any batches that have already been committed
                    // (otherwise our new_upper would be before our upper).
                    //
                    // Note however, that unlike a real source, our
                    // DataGenerator-derived batches are guaranteed to be
                    // chunked along the same boundaries. This means we don't
                    // have to consider partial batches when generating the
                    // updates below.
                    if PartialOrder::less_equal(&Antichain::from_elem(new_upper), write.upper()) {
                        continue;
                    }

                    let current_upper_chain = Antichain::from_elem(current_upper);
                    current_upper = new_upper;
                    let new_upper_chain = Antichain::from_elem(new_upper);
                    let mut builder = write.builder(current_upper_chain);

                    for ((k, v), t, d) in batch.iter() {
                        builder
                            .add(&k.to_vec(), &v.to_vec(), &u64::decode(t), &i64::decode(d))
                            .await
                            .expect("invalid usage");
                    }

                    let batch = builder
                        .finish(new_upper_chain)
                        .await
                        .expect("invalid usage");

                    match batch_tx.send(batch).await {
                        Ok(_) => (),
                        Err(e) => panic!("send error: {}", e),
                    }
                }
            });
            handles.push(handle);

            let handle = mz_ore::task::spawn(|| format!("appender-{}", idx), async move {
                let (mut write, _) = client.expect_open::<Vec<u8>, Vec<u8>, u64, i64>(id).await;

                while let Some(batch) = batch_rx.recv().await {
                    let lower = batch.lower().clone();
                    let upper = batch.upper().clone();
                    write
                        .append_batch(batch, lower, upper)
                        .await
                        .expect("invalid usage")
                        .expect("unexpected upper");
                }
            });
            handles.push(handle);
        }

        for handle in handles {
            let () = handle.await.expect("task failed");
        }

        let expected = data.records().collect::<Vec<_>>();
        let max_ts = expected.last().map(|(_, t, _)| *t).unwrap_or_default();
        let (_, mut read) = client.expect_open::<Vec<u8>, Vec<u8>, u64, i64>(id).await;
        assert_eq!(
            read.expect_snapshot_and_fetch(max_ts).await,
            all_ok(expected.iter(), max_ts)
        );
    }

    // Regression test for database-issues#3523. Snapshot with as_of >= upper would
    // immediately return the data currently available instead of waiting for
    // upper to advance past as_of.
    #[mz_persist_proc::test(tokio::test)]
    #[cfg_attr(miri, ignore)] // unsupported operation: returning ready events from epoll_wait is not yet implemented
    async fn regression_blocking_reads(dyncfgs: ConfigUpdates) {
        let waker = noop_waker();
        let mut cx = Context::from_waker(&waker);

        let data = [
            (("1".to_owned(), "one".to_owned()), 1, 1),
            (("2".to_owned(), "two".to_owned()), 2, 1),
            (("3".to_owned(), "three".to_owned()), 3, 1),
        ];

        let id = ShardId::new();
        let client = new_test_client(&dyncfgs).await;
        let (mut write, mut read) = client.expect_open::<String, String, u64, i64>(id).await;

        // Grab a listener as_of (aka gt) 1, which is not yet closed out.
        let mut listen = read.clone("").await.expect_listen(1).await;
        let mut listen_next = Box::pin(listen.fetch_next());
        // Intentionally don't await the listen_next, but instead manually poke
        // it for a while and assert that it doesn't resolve yet. See below for
        // discussion of some alternative ways of writing this unit test.
        for _ in 0..100 {
            assert!(
                Pin::new(&mut listen_next).poll(&mut cx).is_pending(),
                "listen::next unexpectedly ready"
            );
        }

        // Write a [0,3) batch.
        write
            .expect_compare_and_append(&data[..2], u64::minimum(), 3)
            .await;

        // The initial listen_next call should now be able to return data at 2.
        // It doesn't get 1 because the as_of was 1 and listen is strictly gt.
        assert_eq!(
            listen_next.await,
            vec![
                ListenEvent::Updates(vec![((Ok("2".to_owned()), Ok("two".to_owned())), 2, 1)]),
                ListenEvent::Progress(Antichain::from_elem(3)),
            ]
        );

        // Grab a snapshot as_of 3, which is not yet closed out. Intentionally
        // don't await the snap, but instead manually poke it for a while and
        // assert that it doesn't resolve yet.
        //
        // An alternative to this would be to run it in a task and poll the task
        // with some timeout, but this would introduce a fixed test execution
        // latency of the timeout in the happy case. Plus, it would be
        // non-deterministic.
        //
        // Another alternative (that's potentially quite interesting!) would be
        // to separate creating a snapshot immediately (which would fail if
        // as_of was >= upper) from a bit of logic that retries until that case
        // is ready.
        let mut snap = Box::pin(read.expect_snapshot_and_fetch(3));
        for _ in 0..100 {
            assert!(
                Pin::new(&mut snap).poll(&mut cx).is_pending(),
                "snapshot unexpectedly ready"
            );
        }

        // Now add the data at 3 and also unblock the snapshot.
        write.expect_compare_and_append(&data[2..], 3, 4).await;

        // Read the snapshot and check that it got all the appropriate data.
        assert_eq!(snap.await, all_ok(&data[..], 3));
    }

    #[mz_persist_proc::test(tokio::test)]
    #[cfg_attr(miri, ignore)] // unsupported operation: returning ready events from epoll_wait is not yet implemented
    async fn heartbeat_task_shutdown(dyncfgs: ConfigUpdates) {
        // Verify that the ReadHandle and WriteHandle background heartbeat tasks
        // shut down cleanly after the handle is expired.
        let mut cache = new_test_client_cache(&dyncfgs);
        cache
            .cfg
            .set_config(&READER_LEASE_DURATION, Duration::from_millis(1));
        cache.cfg.writer_lease_duration = Duration::from_millis(1);
        let (_write, mut read) = cache
            .open(PersistLocation::new_in_mem())
            .await
            .expect("client construction failed")
            .expect_open::<(), (), u64, i64>(ShardId::new())
            .await;
        let mut read_unexpired_state = read
            .unexpired_state
            .take()
            .expect("handle should have unexpired state");
        read.expire().await;
        for read_heartbeat_task in mem::take(&mut read_unexpired_state._heartbeat_tasks) {
            let () = read_heartbeat_task
                .await
                .expect("task should shutdown cleanly");
        }
    }

    /// Verify that shard finalization works with empty shards, shards that have
    /// an empty write up to the empty upper Antichain.
    #[mz_persist_proc::test(tokio::test)]
    #[cfg_attr(miri, ignore)] // unsupported operation: returning ready events from epoll_wait is not yet implemented
    async fn finalize_empty_shard(dyncfgs: ConfigUpdates) {
        const EMPTY: &[(((), ()), u64, i64)] = &[];
        let persist_client = new_test_client(&dyncfgs).await;

        let shard_id = ShardId::new();
        pub const CRITICAL_SINCE: CriticalReaderId =
            CriticalReaderId([0, 0, 0, 0, 17, 17, 34, 34, 51, 51, 68, 68, 68, 68, 68, 68]);

        let (mut write, mut read) = persist_client
            .expect_open::<(), (), u64, i64>(shard_id)
            .await;

        // Advance since and upper to empty, which is a pre-requisite for
        // finalization/tombstoning.
        let () = read.downgrade_since(&Antichain::new()).await;
        let () = write
            .compare_and_append(EMPTY, Antichain::from_elem(0), Antichain::new())
            .await
            .expect("usage should be valid")
            .expect("upper should match");

        let mut since_handle: SinceHandle<(), (), u64, i64, u64> = persist_client
            .open_critical_since(shard_id, CRITICAL_SINCE, Diagnostics::for_tests())
            .await
            .expect("invalid persist usage");

        let epoch = since_handle.opaque().clone();
        let new_since = Antichain::new();
        let downgrade = since_handle
            .compare_and_downgrade_since(&epoch, (&epoch, &new_since))
            .await;

        assert!(
            downgrade.is_ok(),
            "downgrade of critical handle must succeed"
        );

        let finalize = persist_client
            .finalize_shard::<(), (), u64, i64>(shard_id, Diagnostics::for_tests())
            .await;

        assert_ok!(finalize, "finalization must succeed");

        let is_finalized = persist_client
            .is_finalized::<(), (), u64, i64>(shard_id, Diagnostics::for_tests())
            .await
            .expect("invalid persist usage");
        assert!(is_finalized, "shard must still be finalized");
    }

    /// Verify that shard finalization works with shards that had some data
    /// written to them, plus then an empty batch to bring their upper to the
    /// empty Antichain.
    #[mz_persist_proc::test(tokio::test)]
    #[cfg_attr(miri, ignore)] // unsupported operation: returning ready events from epoll_wait is not yet implemented
    async fn finalize_shard(dyncfgs: ConfigUpdates) {
        const EMPTY: &[(((), ()), u64, i64)] = &[];
        const DATA: &[(((), ()), u64, i64)] = &[(((), ()), 0, 1)];
        let persist_client = new_test_client(&dyncfgs).await;

        let shard_id = ShardId::new();
        pub const CRITICAL_SINCE: CriticalReaderId =
            CriticalReaderId([0, 0, 0, 0, 17, 17, 34, 34, 51, 51, 68, 68, 68, 68, 68, 68]);

        let (mut write, mut read) = persist_client
            .expect_open::<(), (), u64, i64>(shard_id)
            .await;

        // Write some data.
        let () = write
            .compare_and_append(DATA, Antichain::from_elem(0), Antichain::from_elem(1))
            .await
            .expect("usage should be valid")
            .expect("upper should match");

        // Advance since and upper to empty, which is a pre-requisite for
        // finalization/tombstoning.
        let () = read.downgrade_since(&Antichain::new()).await;
        let () = write
            .compare_and_append(EMPTY, Antichain::from_elem(1), Antichain::new())
            .await
            .expect("usage should be valid")
            .expect("upper should match");

        let mut since_handle: SinceHandle<(), (), u64, i64, u64> = persist_client
            .open_critical_since(shard_id, CRITICAL_SINCE, Diagnostics::for_tests())
            .await
            .expect("invalid persist usage");

        let epoch = since_handle.opaque().clone();
        let new_since = Antichain::new();
        let downgrade = since_handle
            .compare_and_downgrade_since(&epoch, (&epoch, &new_since))
            .await;

        assert!(
            downgrade.is_ok(),
            "downgrade of critical handle must succeed"
        );

        let finalize = persist_client
            .finalize_shard::<(), (), u64, i64>(shard_id, Diagnostics::for_tests())
            .await;

        assert_ok!(finalize, "finalization must succeed");

        let is_finalized = persist_client
            .is_finalized::<(), (), u64, i64>(shard_id, Diagnostics::for_tests())
            .await
            .expect("invalid persist usage");
        assert!(is_finalized, "shard must still be finalized");
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(4096))]

        #[mz_ore::test]
        #[cfg_attr(miri, ignore)] // too slow
        fn shard_id_protobuf_roundtrip(expect in any::<ShardId>() ) {
            let actual = protobuf_roundtrip::<_, String>(&expect);
            assert_ok!(actual);
            assert_eq!(actual.unwrap(), expect);
        }
    }
}

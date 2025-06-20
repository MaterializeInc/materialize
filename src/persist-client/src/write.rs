// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Write capabilities and handles

use std::borrow::Borrow;
use std::fmt::Debug;
use std::sync::Arc;

use differential_dataflow::difference::Semigroup;
use differential_dataflow::lattice::Lattice;
use differential_dataflow::trace::Description;
use futures::StreamExt;
use futures::stream::FuturesUnordered;
use mz_dyncfg::Config;
use mz_ore::instrument;
use mz_ore::task::RuntimeExt;
use mz_persist::location::Blob;
use mz_persist_types::schema::SchemaId;
use mz_persist_types::{Codec, Codec64};
use mz_proto::{IntoRustIfSome, ProtoType};
use proptest_derive::Arbitrary;
use semver::Version;
use serde::{Deserialize, Serialize};
use timely::PartialOrder;
use timely::order::TotalOrder;
use timely::progress::{Antichain, Timestamp};
use tokio::runtime::Handle;
use tracing::{Instrument, debug_span, info, warn};
use uuid::Uuid;

use crate::batch::{
    Added, BATCH_DELETE_ENABLED, Batch, BatchBuilder, BatchBuilderConfig, BatchBuilderInternal,
    BatchParts, ProtoBatch, validate_truncate_batch,
};
use crate::error::{InvalidUsage, UpperMismatch};
use crate::fetch::{EncodedPart, FetchBatchFilter, FetchedPart, PartDecodeFormat};
use crate::internal::compact::{CompactConfig, Compactor};
use crate::internal::encoding::{Schemas, check_data_version};
use crate::internal::machine::{CompareAndAppendRes, ExpireFn, Machine};
use crate::internal::metrics::{BatchWriteMetrics, Metrics, ShardMetrics};
use crate::internal::state::{BatchPart, HandleDebugState, HollowBatch, RunOrder, RunPart};
use crate::read::ReadHandle;
use crate::schema::PartMigration;
use crate::{GarbageCollector, IsolatedRuntime, PersistConfig, ShardId, parse_id};

pub(crate) const COMBINE_INLINE_WRITES: Config<bool> = Config::new(
    "persist_write_combine_inline_writes",
    true,
    "If set, re-encode inline writes if they don't fit into the batch metadata limits.",
);

/// An opaque identifier for a writer of a persist durable TVC (aka shard).
#[derive(Arbitrary, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct WriterId(pub(crate) [u8; 16]);

impl std::fmt::Display for WriterId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "w{}", Uuid::from_bytes(self.0))
    }
}

impl std::fmt::Debug for WriterId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "WriterId({})", Uuid::from_bytes(self.0))
    }
}

impl std::str::FromStr for WriterId {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        parse_id('w', "WriterId", s).map(WriterId)
    }
}

impl From<WriterId> for String {
    fn from(writer_id: WriterId) -> Self {
        writer_id.to_string()
    }
}

impl TryFrom<String> for WriterId {
    type Error = String;

    fn try_from(s: String) -> Result<Self, Self::Error> {
        s.parse()
    }
}

impl WriterId {
    pub(crate) fn new() -> Self {
        WriterId(*Uuid::new_v4().as_bytes())
    }
}

/// A "capability" granting the ability to apply updates to some shard at times
/// greater or equal to `self.upper()`.
///
/// All async methods on ReadHandle retry for as long as they are able, but the
/// returned [std::future::Future]s implement "cancel on drop" semantics. This
/// means that callers can add a timeout using [tokio::time::timeout] or
/// [tokio::time::timeout_at].
///
/// ```rust,no_run
/// # let mut write: mz_persist_client::write::WriteHandle<String, String, u64, i64> = unimplemented!();
/// # let timeout: std::time::Duration = unimplemented!();
/// # async {
/// tokio::time::timeout(timeout, write.fetch_recent_upper()).await
/// # };
/// ```
#[derive(Debug)]
pub struct WriteHandle<K: Codec, V: Codec, T, D> {
    pub(crate) cfg: PersistConfig,
    pub(crate) metrics: Arc<Metrics>,
    pub(crate) machine: Machine<K, V, T, D>,
    pub(crate) gc: GarbageCollector<K, V, T, D>,
    pub(crate) compact: Option<Compactor<K, V, T, D>>,
    pub(crate) blob: Arc<dyn Blob>,
    pub(crate) isolated_runtime: Arc<IsolatedRuntime>,
    pub(crate) writer_id: WriterId,
    pub(crate) debug_state: HandleDebugState,
    pub(crate) write_schemas: Schemas<K, V>,

    pub(crate) upper: Antichain<T>,
    expire_fn: Option<ExpireFn>,
}

impl<K, V, T, D> WriteHandle<K, V, T, D>
where
    K: Debug + Codec,
    V: Debug + Codec,
    T: Timestamp + TotalOrder + Lattice + Codec64 + Sync,
    D: Semigroup + Ord + Codec64 + Send + Sync,
{
    pub(crate) fn new(
        cfg: PersistConfig,
        metrics: Arc<Metrics>,
        machine: Machine<K, V, T, D>,
        gc: GarbageCollector<K, V, T, D>,
        blob: Arc<dyn Blob>,
        writer_id: WriterId,
        purpose: &str,
        write_schemas: Schemas<K, V>,
    ) -> Self {
        let isolated_runtime = Arc::clone(&machine.isolated_runtime);
        let compact = cfg.compaction_enabled.then(|| {
            Compactor::new(
                cfg.clone(),
                Arc::clone(&metrics),
                write_schemas.clone(),
                gc.clone(),
            )
        });
        let debug_state = HandleDebugState {
            hostname: cfg.hostname.to_owned(),
            purpose: purpose.to_owned(),
        };
        let upper = machine.applier.clone_upper();
        let expire_fn = Self::expire_fn(machine.clone(), gc.clone(), writer_id.clone());
        WriteHandle {
            cfg,
            metrics,
            machine,
            gc,
            compact,
            blob,
            isolated_runtime,
            writer_id,
            debug_state,
            write_schemas,
            upper,
            expire_fn: Some(expire_fn),
        }
    }

    /// Creates a [WriteHandle] for the same shard from an existing
    /// [ReadHandle].
    pub fn from_read(read: &ReadHandle<K, V, T, D>, purpose: &str) -> Self {
        Self::new(
            read.cfg.clone(),
            Arc::clone(&read.metrics),
            read.machine.clone(),
            read.gc.clone(),
            Arc::clone(&read.blob),
            WriterId::new(),
            purpose,
            read.read_schemas.clone(),
        )
    }

    /// This handle's shard id.
    pub fn shard_id(&self) -> ShardId {
        self.machine.shard_id()
    }

    /// Returns the schema of this writer.
    pub fn schema_id(&self) -> Option<SchemaId> {
        self.write_schemas.id
    }

    /// A cached version of the shard-global `upper` frontier.
    ///
    /// This is the most recent upper discovered by this handle. It is
    /// potentially more stale than [Self::shared_upper] but is lock-free and
    /// allocation-free. This will always be less or equal to the shard-global
    /// `upper`.
    pub fn upper(&self) -> &Antichain<T> {
        &self.upper
    }

    /// A less-stale cached version of the shard-global `upper` frontier.
    ///
    /// This is the most recently known upper for this shard process-wide, but
    /// unlike [Self::upper] it requires a mutex and a clone. This will always be
    /// less or equal to the shard-global `upper`.
    pub fn shared_upper(&self) -> Antichain<T> {
        self.machine.applier.clone_upper()
    }

    /// Fetches and returns a recent shard-global `upper`. Importantly, this operation is
    /// linearized with write operations.
    ///
    /// This requires fetching the latest state from consensus and is therefore a potentially
    /// expensive operation.
    #[instrument(level = "debug", fields(shard = %self.machine.shard_id()))]
    pub async fn fetch_recent_upper(&mut self) -> &Antichain<T> {
        // TODO: Do we even need to track self.upper on WriteHandle or could
        // WriteHandle::upper just get the one out of machine?
        self.machine
            .applier
            .fetch_upper(|current_upper| self.upper.clone_from(current_upper))
            .await;
        &self.upper
    }

    /// Applies `updates` to this shard and downgrades this handle's upper to
    /// `upper`.
    ///
    /// The innermost `Result` is `Ok` if the updates were successfully written.
    /// If not, an `Upper` err containing the current writer upper is returned.
    /// If that happens, we also update our local `upper` to match the current
    /// upper. This is useful in cases where a timeout happens in between a
    /// successful write and returning that to the client.
    ///
    /// In contrast to [Self::compare_and_append], multiple [WriteHandle]s may
    /// be used concurrently to write to the same shard, but in this case, the
    /// data being written must be identical (in the sense of "definite"-ness).
    /// It's intended for replicated use by source ingestion, sinks, etc.
    ///
    /// All times in `updates` must be greater or equal to `lower` and not
    /// greater or equal to `upper`. A `upper` of the empty antichain "finishes"
    /// this shard, promising that no more data is ever incoming.
    ///
    /// `updates` may be empty, which allows for downgrading `upper` to
    /// communicate progress. It is possible to call this with `upper` equal to
    /// `self.upper()` and an empty `updates` (making the call a no-op).
    ///
    /// This uses a bounded amount of memory, even when `updates` is very large.
    /// Individual records, however, should be small enough that we can
    /// reasonably chunk them up: O(KB) is definitely fine, O(MB) come talk to
    /// us.
    ///
    /// The clunky multi-level Result is to enable more obvious error handling
    /// in the caller. See <http://sled.rs/errors.html> for details.
    #[instrument(level = "trace", fields(shard = %self.machine.shard_id()))]
    pub async fn append<SB, KB, VB, TB, DB, I>(
        &mut self,
        updates: I,
        lower: Antichain<T>,
        upper: Antichain<T>,
    ) -> Result<Result<(), UpperMismatch<T>>, InvalidUsage<T>>
    where
        SB: Borrow<((KB, VB), TB, DB)>,
        KB: Borrow<K>,
        VB: Borrow<V>,
        TB: Borrow<T>,
        DB: Borrow<D>,
        I: IntoIterator<Item = SB>,
        D: Send + Sync,
    {
        let batch = self.batch(updates, lower.clone(), upper.clone()).await?;
        self.append_batch(batch, lower, upper).await
    }

    /// Applies `updates` to this shard and downgrades this handle's upper to
    /// `new_upper` iff the current global upper of this shard is
    /// `expected_upper`.
    ///
    /// The innermost `Result` is `Ok` if the updates were successfully written.
    /// If not, an `Upper` err containing the current global upper is returned.
    ///
    /// In contrast to [Self::append], this linearizes mutations from all
    /// writers. It's intended for use as an atomic primitive for timestamp
    /// bindings, SQL tables, etc.
    ///
    /// All times in `updates` must be greater or equal to `expected_upper` and
    /// not greater or equal to `new_upper`. A `new_upper` of the empty
    /// antichain "finishes" this shard, promising that no more data is ever
    /// incoming.
    ///
    /// `updates` may be empty, which allows for downgrading `upper` to
    /// communicate progress. It is possible to heartbeat a writer lease by
    /// calling this with `new_upper` equal to `self.upper()` and an empty
    /// `updates` (making the call a no-op).
    ///
    /// This uses a bounded amount of memory, even when `updates` is very large.
    /// Individual records, however, should be small enough that we can
    /// reasonably chunk them up: O(KB) is definitely fine, O(MB) come talk to
    /// us.
    ///
    /// The clunky multi-level Result is to enable more obvious error handling
    /// in the caller. See <http://sled.rs/errors.html> for details.
    #[instrument(level = "trace", fields(shard = %self.machine.shard_id()))]
    pub async fn compare_and_append<SB, KB, VB, TB, DB, I>(
        &mut self,
        updates: I,
        expected_upper: Antichain<T>,
        new_upper: Antichain<T>,
    ) -> Result<Result<(), UpperMismatch<T>>, InvalidUsage<T>>
    where
        SB: Borrow<((KB, VB), TB, DB)>,
        KB: Borrow<K>,
        VB: Borrow<V>,
        TB: Borrow<T>,
        DB: Borrow<D>,
        I: IntoIterator<Item = SB>,
        D: Send + Sync,
    {
        let mut batch = self
            .batch(updates, expected_upper.clone(), new_upper.clone())
            .await?;
        match self
            .compare_and_append_batch(&mut [&mut batch], expected_upper, new_upper, true)
            .await
        {
            ok @ Ok(Ok(())) => ok,
            err => {
                // We cannot delete the batch in compare_and_append_batch()
                // because the caller owns the batch and might want to retry
                // with a different `expected_upper`. In this function, we
                // control the batch, so we have to delete it.
                batch.delete().await;
                err
            }
        }
    }

    /// Appends the batch of updates to the shard and downgrades this handle's
    /// upper to `upper`.
    ///
    /// The innermost `Result` is `Ok` if the updates were successfully written.
    /// If not, an `Upper` err containing the current writer upper is returned.
    /// If that happens, we also update our local `upper` to match the current
    /// upper. This is useful in cases where a timeout happens in between a
    /// successful write and returning that to the client.
    ///
    /// In contrast to [Self::compare_and_append_batch], multiple [WriteHandle]s
    /// may be used concurrently to write to the same shard, but in this case,
    /// the data being written must be identical (in the sense of
    /// "definite"-ness). It's intended for replicated use by source ingestion,
    /// sinks, etc.
    ///
    /// A `upper` of the empty antichain "finishes" this shard, promising that
    /// no more data is ever incoming.
    ///
    /// The batch may be empty, which allows for downgrading `upper` to
    /// communicate progress. It is possible to heartbeat a writer lease by
    /// calling this with `upper` equal to `self.upper()` and an empty `updates`
    /// (making the call a no-op).
    ///
    /// The clunky multi-level Result is to enable more obvious error handling
    /// in the caller. See <http://sled.rs/errors.html> for details.
    #[instrument(level = "trace", fields(shard = %self.machine.shard_id()))]
    pub async fn append_batch(
        &mut self,
        mut batch: Batch<K, V, T, D>,
        mut lower: Antichain<T>,
        upper: Antichain<T>,
    ) -> Result<Result<(), UpperMismatch<T>>, InvalidUsage<T>>
    where
        D: Send + Sync,
    {
        loop {
            let res = self
                .compare_and_append_batch(&mut [&mut batch], lower.clone(), upper.clone(), true)
                .await?;
            match res {
                Ok(()) => {
                    self.upper = upper;
                    return Ok(Ok(()));
                }
                Err(mismatch) => {
                    // We tried to to a non-contiguous append, that won't work.
                    if PartialOrder::less_than(&mismatch.current, &lower) {
                        self.upper.clone_from(&mismatch.current);

                        batch.delete().await;

                        return Ok(Err(mismatch));
                    } else if PartialOrder::less_than(&mismatch.current, &upper) {
                        // Cut down the Description by advancing its lower to the current shard
                        // upper and try again. IMPORTANT: We can only advance the lower, meaning
                        // we cut updates away, we must not "extend" the batch by changing to a
                        // lower that is not beyond the current lower. This invariant is checked by
                        // the first if branch: if `!(current_upper < lower)` then it holds that
                        // `lower <= current_upper`.
                        lower = mismatch.current;
                    } else {
                        // We already have updates past this batch's upper, the append is a no-op.
                        self.upper = mismatch.current;

                        // Because we return a success result, the caller will
                        // think that the batch was consumed or otherwise used,
                        // so we have to delete it here.
                        batch.delete().await;

                        return Ok(Ok(()));
                    }
                }
            }
        }
    }

    /// Appends the batch of updates to the shard and downgrades this handle's
    /// upper to `new_upper` iff the current global upper of this shard is
    /// `expected_upper`.
    ///
    /// The innermost `Result` is `Ok` if the batch was successfully written. If
    /// not, an `Upper` err containing the current global upper is returned.
    ///
    /// In contrast to [Self::append_batch], this linearizes mutations from all
    /// writers. It's intended for use as an atomic primitive for timestamp
    /// bindings, SQL tables, etc.
    ///
    /// A `new_upper` of the empty antichain "finishes" this shard, promising
    /// that no more data is ever incoming.
    ///
    /// The batch may be empty, which allows for downgrading `upper` to
    /// communicate progress. It is possible to heartbeat a writer lease by
    /// calling this with `new_upper` equal to `self.upper()` and an empty
    /// `updates` (making the call a no-op).
    ///
    /// IMPORTANT: In case of an erroneous result the caller is responsible for
    /// the lifecycle of the `batch`. It can be deleted or it can be used to
    /// retry with adjusted frontiers.
    ///
    /// The clunky multi-level Result is to enable more obvious error handling
    /// in the caller. See <http://sled.rs/errors.html> for details.
    ///
    /// If the `enforce_matching_batch_boundaries` flag is set to `false`:
    /// We no longer validate that every batch covers the entire range between
    /// the expected and new uppers, as we wish to allow combining batches that
    /// cover different subsets of that range, including subsets of that range
    /// that include no data at all. The caller is responsible for guaranteeing
    /// that the set of batches provided collectively include all updates for
    /// the entire range between the expected and new upper.
    #[instrument(level = "debug", fields(shard = %self.machine.shard_id()))]
    pub async fn compare_and_append_batch(
        &mut self,
        batches: &mut [&mut Batch<K, V, T, D>],
        expected_upper: Antichain<T>,
        new_upper: Antichain<T>,
        enforce_matching_batch_boundaries: bool,
    ) -> Result<Result<(), UpperMismatch<T>>, InvalidUsage<T>>
    where
        D: Send + Sync,
    {
        for batch in batches.iter() {
            if self.machine.shard_id() != batch.shard_id() {
                return Err(InvalidUsage::BatchNotFromThisShard {
                    batch_shard: batch.shard_id(),
                    handle_shard: self.machine.shard_id(),
                });
            }
            check_data_version(&self.cfg.build_version, &batch.version);
            if self.cfg.build_version > batch.version {
                info!(
                    shard_id =? self.machine.shard_id(),
                    batch_version =? batch.version,
                    writer_version =? self.cfg.build_version,
                    "Appending batch from the past. This is fine but should be rare. \
                    TODO: Error on very old versions once the leaked blob detector exists."
                )
            }
        }

        let lower = expected_upper.clone();
        let upper = new_upper;
        let since = Antichain::from_elem(T::minimum());
        let desc = Description::new(lower, upper, since);

        let mut received_inline_backpressure = false;
        // Every hollow part must belong to some batch, so we can clean it up when the batch is dropped...
        // but if we need to merge all our inline parts to a single run in S3, it's not correct to
        // associate that with any of our individual input batches.
        // At first, we'll try and put all the inline parts we receive into state... but if we
        // get backpressured, we retry with this builder set to `Some`, put all our inline data into
        // it, and ensure it's flushed out to S3 before including it in the batch.
        let mut inline_batch_builder: Option<(_, BatchBuilder<K, V, T, D>)> = None;
        let maintenance = loop {
            let any_batch_rewrite = batches
                .iter()
                .any(|x| x.batch.parts.iter().any(|x| x.ts_rewrite().is_some()));
            let (mut parts, mut num_updates, mut run_splits, mut run_metas) =
                (vec![], 0, vec![], vec![]);
            let mut key_storage = None;
            let mut val_storage = None;
            for batch in batches.iter() {
                let () = validate_truncate_batch(
                    &batch.batch,
                    &desc,
                    any_batch_rewrite,
                    enforce_matching_batch_boundaries,
                )?;
                for (run_meta, run) in batch.batch.runs() {
                    let start_index = parts.len();
                    for part in run {
                        if let (
                            RunPart::Single(
                                batch_part @ BatchPart::Inline {
                                    updates,
                                    ts_rewrite,
                                    schema_id: _,
                                    deprecated_schema_id: _,
                                },
                            ),
                            Some((schema_cache, builder)),
                        ) = (part, &mut inline_batch_builder)
                        {
                            let schema_migration = PartMigration::new(
                                batch_part,
                                self.write_schemas.clone(),
                                schema_cache,
                            )
                            .await
                            .expect("schemas for inline user part");

                            let encoded_part = EncodedPart::from_inline(
                                &crate::fetch::FetchConfig::from_persist_config(&self.cfg),
                                &*self.metrics,
                                self.metrics.read.compaction.clone(),
                                desc.clone(),
                                updates,
                                ts_rewrite.as_ref(),
                            );
                            let mut fetched_part = FetchedPart::new(
                                Arc::clone(&self.metrics),
                                encoded_part,
                                schema_migration,
                                FetchBatchFilter::Compaction {
                                    since: desc.since().clone(),
                                },
                                false,
                                PartDecodeFormat::Arrow,
                                None,
                            );

                            while let Some(((k, v), t, d)) =
                                fetched_part.next_with_storage(&mut key_storage, &mut val_storage)
                            {
                                builder
                                    .add(
                                        &k.expect("decoded just-encoded key data"),
                                        &v.expect("decoded just-encoded val data"),
                                        &t,
                                        &d,
                                    )
                                    .await
                                    .expect("re-encoding just-decoded data");
                            }
                        } else {
                            parts.push(part.clone())
                        }
                    }

                    let end_index = parts.len();

                    if start_index == end_index {
                        continue;
                    }

                    // Mark the boundary if this is not the first run in the batch.
                    if start_index != 0 {
                        run_splits.push(start_index);
                    }
                    run_metas.push(run_meta.clone());
                }
                num_updates += batch.batch.len;
            }

            let mut flushed_inline_batch = if let Some((_, builder)) = inline_batch_builder.take() {
                let mut finished = builder
                    .finish(desc.upper().clone())
                    .await
                    .expect("invalid usage");
                let cfg = BatchBuilderConfig::new(&self.cfg, self.shard_id());
                finished
                    .flush_to_blob(
                        &cfg,
                        &self.metrics.inline.backpressure,
                        &self.isolated_runtime,
                        &self.write_schemas,
                    )
                    .await;
                Some(finished)
            } else {
                None
            };

            if let Some(batch) = &flushed_inline_batch {
                for (run_meta, run) in batch.batch.runs() {
                    assert!(run.len() > 0);
                    let start_index = parts.len();
                    if start_index != 0 {
                        run_splits.push(start_index);
                    }
                    run_metas.push(run_meta.clone());
                    parts.extend(run.iter().cloned())
                }
            }

            let combined_batch =
                HollowBatch::new(desc.clone(), parts, num_updates, run_metas, run_splits);
            let heartbeat_timestamp = (self.cfg.now)();
            let res = self
                .machine
                .compare_and_append(
                    &combined_batch,
                    &self.writer_id,
                    &self.debug_state,
                    heartbeat_timestamp,
                )
                .await;

            match res {
                CompareAndAppendRes::Success(_seqno, maintenance) => {
                    self.upper.clone_from(desc.upper());
                    for batch in batches.iter_mut() {
                        batch.mark_consumed();
                    }
                    if let Some(batch) = &mut flushed_inline_batch {
                        batch.mark_consumed();
                    }
                    break maintenance;
                }
                CompareAndAppendRes::InvalidUsage(invalid_usage) => {
                    if let Some(batch) = flushed_inline_batch.take() {
                        batch.delete().await;
                    }
                    return Err(invalid_usage);
                }
                CompareAndAppendRes::UpperMismatch(_seqno, current_upper) => {
                    if let Some(batch) = flushed_inline_batch.take() {
                        batch.delete().await;
                    }
                    // We tried to to a compare_and_append with the wrong expected upper, that
                    // won't work. Update the cached upper to the current upper.
                    self.upper.clone_from(&current_upper);
                    return Ok(Err(UpperMismatch {
                        current: current_upper,
                        expected: expected_upper,
                    }));
                }
                CompareAndAppendRes::InlineBackpressure => {
                    // We tried to write an inline part, but there was already
                    // too much in state. Flush it out to s3 and try again.
                    assert_eq!(received_inline_backpressure, false);
                    received_inline_backpressure = true;
                    if COMBINE_INLINE_WRITES.get(&self.cfg) {
                        inline_batch_builder = Some((
                            self.machine.applier.schema_cache(),
                            self.builder(desc.lower().clone()),
                        ));
                        continue;
                    }

                    let cfg = BatchBuilderConfig::new(&self.cfg, self.shard_id());
                    // We could have a large number of inline parts (imagine the
                    // sharded persist_sink), do this flushing concurrently.
                    let flush_batches = batches
                        .iter_mut()
                        .map(|batch| async {
                            batch
                                .flush_to_blob(
                                    &cfg,
                                    &self.metrics.inline.backpressure,
                                    &self.isolated_runtime,
                                    &self.write_schemas,
                                )
                                .await
                        })
                        .collect::<FuturesUnordered<_>>();
                    let () = flush_batches.collect::<()>().await;

                    for batch in batches.iter() {
                        assert_eq!(batch.batch.inline_bytes(), 0);
                    }

                    continue;
                }
            }
        };

        maintenance.start_performing(&self.machine, &self.gc, self.compact.as_ref());

        Ok(Ok(()))
    }

    /// Turns the given [`ProtoBatch`] back into a [`Batch`] which can be used
    /// to append it to this shard.
    pub fn batch_from_transmittable_batch(&self, batch: ProtoBatch) -> Batch<K, V, T, D> {
        let shard_id: ShardId = batch
            .shard_id
            .into_rust()
            .expect("valid transmittable batch");
        assert_eq!(shard_id, self.machine.shard_id());

        let ret = Batch {
            batch_delete_enabled: BATCH_DELETE_ENABLED.get(&self.cfg),
            metrics: Arc::clone(&self.metrics),
            shard_metrics: Arc::clone(&self.machine.applier.shard_metrics),
            version: Version::parse(&batch.version).expect("valid transmittable batch"),
            batch: batch
                .batch
                .into_rust_if_some("ProtoBatch::batch")
                .expect("valid transmittable batch"),
            blob: Arc::clone(&self.blob),
            _phantom: std::marker::PhantomData,
        };
        assert_eq!(ret.shard_id(), self.machine.shard_id());
        ret
    }

    /// Returns a [BatchBuilder] that can be used to write a batch of updates to
    /// blob storage which can then be appended to this shard using
    /// [Self::compare_and_append_batch] or [Self::append_batch].
    ///
    /// It is correct to create an empty batch, which allows for downgrading
    /// `upper` to communicate progress. (see [Self::compare_and_append_batch]
    /// or [Self::append_batch])
    ///
    /// The builder uses a bounded amount of memory, even when the number of
    /// updates is very large. Individual records, however, should be small
    /// enough that we can reasonably chunk them up: O(KB) is definitely fine,
    /// O(MB) come talk to us.
    pub fn builder(&self, lower: Antichain<T>) -> BatchBuilder<K, V, T, D> {
        Self::builder_inner(
            &self.cfg,
            CompactConfig::new(&self.cfg, self.shard_id()),
            Arc::clone(&self.metrics),
            Arc::clone(&self.machine.applier.shard_metrics),
            &self.metrics.user,
            Arc::clone(&self.isolated_runtime),
            Arc::clone(&self.blob),
            self.shard_id(),
            self.write_schemas.clone(),
            lower,
        )
    }

    /// Implementation of [Self::builder], so that we can share the
    /// implementation in `PersistClient`.
    pub(crate) fn builder_inner(
        persist_cfg: &PersistConfig,
        compact_cfg: CompactConfig,
        metrics: Arc<Metrics>,
        shard_metrics: Arc<ShardMetrics>,
        user_batch_metrics: &BatchWriteMetrics,
        isolated_runtime: Arc<IsolatedRuntime>,
        blob: Arc<dyn Blob>,
        shard_id: ShardId,
        schemas: Schemas<K, V>,
        lower: Antichain<T>,
    ) -> BatchBuilder<K, V, T, D> {
        let parts = if let Some(max_runs) = compact_cfg.batch.max_runs {
            BatchParts::new_compacting::<K, V, D>(
                compact_cfg,
                Description::new(
                    lower.clone(),
                    Antichain::new(),
                    Antichain::from_elem(T::minimum()),
                ),
                max_runs,
                Arc::clone(&metrics),
                shard_metrics,
                shard_id,
                Arc::clone(&blob),
                isolated_runtime,
                user_batch_metrics,
                schemas.clone(),
            )
        } else {
            BatchParts::new_ordered::<D>(
                compact_cfg.batch,
                RunOrder::Unordered,
                Arc::clone(&metrics),
                shard_metrics,
                shard_id,
                Arc::clone(&blob),
                isolated_runtime,
                user_batch_metrics,
            )
        };
        let builder = BatchBuilderInternal::new(
            BatchBuilderConfig::new(persist_cfg, shard_id),
            parts,
            metrics,
            schemas,
            blob,
            shard_id,
            persist_cfg.build_version.clone(),
        );
        BatchBuilder::new(
            builder,
            Description::new(lower, Antichain::new(), Antichain::from_elem(T::minimum())),
        )
    }

    /// Uploads the given `updates` as one `Batch` to the blob store and returns
    /// a handle to the batch.
    #[instrument(level = "trace", fields(shard = %self.machine.shard_id()))]
    pub async fn batch<SB, KB, VB, TB, DB, I>(
        &mut self,
        updates: I,
        lower: Antichain<T>,
        upper: Antichain<T>,
    ) -> Result<Batch<K, V, T, D>, InvalidUsage<T>>
    where
        SB: Borrow<((KB, VB), TB, DB)>,
        KB: Borrow<K>,
        VB: Borrow<V>,
        TB: Borrow<T>,
        DB: Borrow<D>,
        I: IntoIterator<Item = SB>,
    {
        let iter = updates.into_iter();

        let mut builder = self.builder(lower.clone());

        for update in iter {
            let ((k, v), t, d) = update.borrow();
            let (k, v, t, d) = (k.borrow(), v.borrow(), t.borrow(), d.borrow());
            match builder.add(k, v, t, d).await {
                Ok(Added::Record | Added::RecordAndParts) => (),
                Err(invalid_usage) => return Err(invalid_usage),
            }
        }

        builder.finish(upper.clone()).await
    }

    /// Blocks until the given `frontier` is less than the upper of the shard.
    pub async fn wait_for_upper_past(&mut self, frontier: &Antichain<T>) {
        let mut watch = self.machine.applier.watch();
        let batch = self
            .machine
            .next_listen_batch(frontier, &mut watch, None, None)
            .await;
        if PartialOrder::less_than(&self.upper, batch.desc.upper()) {
            self.upper.clone_from(batch.desc.upper());
        }
        assert!(PartialOrder::less_than(frontier, &self.upper));
    }

    /// Politely expires this writer, releasing any associated state.
    ///
    /// There is a best-effort impl in Drop to expire a writer that wasn't
    /// explictly expired with this method. When possible, explicit expiry is
    /// still preferred because the Drop one is best effort and is dependant on
    /// a tokio [Handle] being available in the TLC at the time of drop (which
    /// is a bit subtle). Also, explicit expiry allows for control over when it
    /// happens.
    #[instrument(level = "debug", fields(shard = %self.machine.shard_id()))]
    pub async fn expire(mut self) {
        let Some(expire_fn) = self.expire_fn.take() else {
            return;
        };
        expire_fn.0().await;
    }

    fn expire_fn(
        machine: Machine<K, V, T, D>,
        gc: GarbageCollector<K, V, T, D>,
        writer_id: WriterId,
    ) -> ExpireFn {
        ExpireFn(Box::new(move || {
            Box::pin(async move {
                let (_, maintenance) = machine.expire_writer(&writer_id).await;
                maintenance.start_performing(&machine, &gc);
            })
        }))
    }

    /// Test helper for an [Self::append] call that is expected to succeed.
    #[cfg(test)]
    #[track_caller]
    pub async fn expect_append<L, U>(&mut self, updates: &[((K, V), T, D)], lower: L, new_upper: U)
    where
        L: Into<Antichain<T>>,
        U: Into<Antichain<T>>,
        D: Send + Sync,
    {
        self.append(updates.iter(), lower.into(), new_upper.into())
            .await
            .expect("invalid usage")
            .expect("unexpected upper");
    }

    /// Test helper for a [Self::compare_and_append] call that is expected to
    /// succeed.
    #[cfg(test)]
    #[track_caller]
    pub async fn expect_compare_and_append(
        &mut self,
        updates: &[((K, V), T, D)],
        expected_upper: T,
        new_upper: T,
    ) where
        D: Send + Sync,
    {
        self.compare_and_append(
            updates.iter().map(|((k, v), t, d)| ((k, v), t, d)),
            Antichain::from_elem(expected_upper),
            Antichain::from_elem(new_upper),
        )
        .await
        .expect("invalid usage")
        .expect("unexpected upper")
    }

    /// Test helper for a [Self::compare_and_append_batch] call that is expected
    /// to succeed.
    #[cfg(test)]
    #[track_caller]
    pub async fn expect_compare_and_append_batch(
        &mut self,
        batches: &mut [&mut Batch<K, V, T, D>],
        expected_upper: T,
        new_upper: T,
    ) {
        self.compare_and_append_batch(
            batches,
            Antichain::from_elem(expected_upper),
            Antichain::from_elem(new_upper),
            true,
        )
        .await
        .expect("invalid usage")
        .expect("unexpected upper")
    }

    /// Test helper for an [Self::append] call that is expected to succeed.
    #[cfg(test)]
    #[track_caller]
    pub async fn expect_batch(
        &mut self,
        updates: &[((K, V), T, D)],
        lower: T,
        upper: T,
    ) -> Batch<K, V, T, D> {
        self.batch(
            updates.iter(),
            Antichain::from_elem(lower),
            Antichain::from_elem(upper),
        )
        .await
        .expect("invalid usage")
    }
}

impl<K: Codec, V: Codec, T, D> Drop for WriteHandle<K, V, T, D> {
    fn drop(&mut self) {
        let Some(expire_fn) = self.expire_fn.take() else {
            return;
        };
        let handle = match Handle::try_current() {
            Ok(x) => x,
            Err(_) => {
                warn!(
                    "WriteHandle {} dropped without being explicitly expired, falling back to lease timeout",
                    self.writer_id
                );
                return;
            }
        };
        // Spawn a best-effort task to expire this write handle. It's fine if
        // this doesn't run to completion, we'd just have to wait out the lease
        // before the shard-global since is unblocked.
        //
        // Intentionally create the span outside the task to set the parent.
        let expire_span = debug_span!("drop::expire");
        handle.spawn_named(
            || format!("WriteHandle::expire ({})", self.writer_id),
            expire_fn.0().instrument(expire_span),
        );
    }
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;
    use std::sync::mpsc;

    use differential_dataflow::consolidation::consolidate_updates;
    use futures_util::FutureExt;
    use mz_dyncfg::ConfigUpdates;
    use mz_ore::collections::CollectionExt;
    use mz_ore::task;
    use serde_json::json;

    use crate::cache::PersistClientCache;
    use crate::tests::{all_ok, new_test_client};
    use crate::{PersistLocation, ShardId};

    use super::*;

    #[mz_persist_proc::test(tokio::test)]
    #[cfg_attr(miri, ignore)] // unsupported operation: returning ready events from epoll_wait is not yet implemented
    async fn empty_batches(dyncfgs: ConfigUpdates) {
        let data = [
            (("1".to_owned(), "one".to_owned()), 1, 1),
            (("2".to_owned(), "two".to_owned()), 2, 1),
            (("3".to_owned(), "three".to_owned()), 3, 1),
        ];

        let (mut write, _) = new_test_client(&dyncfgs)
            .await
            .expect_open::<String, String, u64, i64>(ShardId::new())
            .await;
        let blob = Arc::clone(&write.blob);

        // Write an initial batch.
        let mut upper = 3;
        write.expect_append(&data[..2], vec![0], vec![upper]).await;

        // Write a bunch of empty batches. This shouldn't write blobs, so the count should stay the same.
        let mut count_before = 0;
        blob.list_keys_and_metadata("", &mut |_| {
            count_before += 1;
        })
        .await
        .expect("list_keys failed");
        for _ in 0..5 {
            let new_upper = upper + 1;
            write.expect_compare_and_append(&[], upper, new_upper).await;
            upper = new_upper;
        }
        let mut count_after = 0;
        blob.list_keys_and_metadata("", &mut |_| {
            count_after += 1;
        })
        .await
        .expect("list_keys failed");
        assert_eq!(count_after, count_before);
    }

    #[mz_persist_proc::test(tokio::test)]
    #[cfg_attr(miri, ignore)] // unsupported operation: returning ready events from epoll_wait is not yet implemented
    async fn compare_and_append_batch_multi(dyncfgs: ConfigUpdates) {
        let data0 = vec![
            (("1".to_owned(), "one".to_owned()), 1, 1),
            (("2".to_owned(), "two".to_owned()), 2, 1),
            (("4".to_owned(), "four".to_owned()), 4, 1),
        ];
        let data1 = vec![
            (("1".to_owned(), "one".to_owned()), 1, 1),
            (("2".to_owned(), "two".to_owned()), 2, 1),
            (("3".to_owned(), "three".to_owned()), 3, 1),
        ];

        let (mut write, mut read) = new_test_client(&dyncfgs)
            .await
            .expect_open::<String, String, u64, i64>(ShardId::new())
            .await;

        let mut batch0 = write.expect_batch(&data0, 0, 5).await;
        let mut batch1 = write.expect_batch(&data1, 0, 4).await;

        write
            .expect_compare_and_append_batch(&mut [&mut batch0, &mut batch1], 0, 4)
            .await;

        let batch = write
            .machine
            .snapshot(&Antichain::from_elem(3))
            .await
            .expect("just wrote this")
            .into_element();

        assert!(batch.runs().count() >= 2);

        let expected = vec![
            (("1".to_owned(), "one".to_owned()), 1, 2),
            (("2".to_owned(), "two".to_owned()), 2, 2),
            (("3".to_owned(), "three".to_owned()), 3, 1),
        ];
        let mut actual = read.expect_snapshot_and_fetch(3).await;
        consolidate_updates(&mut actual);
        assert_eq!(actual, all_ok(&expected, 3));
    }

    #[mz_ore::test]
    fn writer_id_human_readable_serde() {
        #[derive(Debug, Serialize, Deserialize)]
        struct Container {
            writer_id: WriterId,
        }

        // roundtrip through json
        let id = WriterId::from_str("w00000000-1234-5678-0000-000000000000").expect("valid id");
        assert_eq!(
            id,
            serde_json::from_value(serde_json::to_value(id.clone()).expect("serializable"))
                .expect("deserializable")
        );

        // deserialize a serialized string directly
        assert_eq!(
            id,
            serde_json::from_str("\"w00000000-1234-5678-0000-000000000000\"")
                .expect("deserializable")
        );

        // roundtrip id through a container type
        let json = json!({ "writer_id": id });
        assert_eq!(
            "{\"writer_id\":\"w00000000-1234-5678-0000-000000000000\"}",
            &json.to_string()
        );
        let container: Container = serde_json::from_value(json).expect("deserializable");
        assert_eq!(container.writer_id, id);
    }

    #[mz_persist_proc::test(tokio::test)]
    #[cfg_attr(miri, ignore)] // unsupported operation: returning ready events from epoll_wait is not yet implemented
    async fn hollow_batch_roundtrip(dyncfgs: ConfigUpdates) {
        let data = vec![
            (("1".to_owned(), "one".to_owned()), 1, 1),
            (("2".to_owned(), "two".to_owned()), 2, 1),
            (("3".to_owned(), "three".to_owned()), 3, 1),
        ];

        let (mut write, mut read) = new_test_client(&dyncfgs)
            .await
            .expect_open::<String, String, u64, i64>(ShardId::new())
            .await;

        // This test is a bit more complex than it should be. It would be easier
        // if we could just compare the rehydrated batch to the original batch.
        // But a) turning a batch into a hollow batch consumes it, and b) Batch
        // doesn't have Eq/PartialEq.
        let batch = write.expect_batch(&data, 0, 4).await;
        let hollow_batch = batch.into_transmittable_batch();
        let mut rehydrated_batch = write.batch_from_transmittable_batch(hollow_batch);

        write
            .expect_compare_and_append_batch(&mut [&mut rehydrated_batch], 0, 4)
            .await;

        let expected = vec![
            (("1".to_owned(), "one".to_owned()), 1, 1),
            (("2".to_owned(), "two".to_owned()), 2, 1),
            (("3".to_owned(), "three".to_owned()), 3, 1),
        ];
        let mut actual = read.expect_snapshot_and_fetch(3).await;
        consolidate_updates(&mut actual);
        assert_eq!(actual, all_ok(&expected, 3));
    }

    #[mz_persist_proc::test(tokio::test)]
    #[cfg_attr(miri, ignore)] // unsupported operation: returning ready events from epoll_wait is not yet implemented
    async fn wait_for_upper_past(dyncfgs: ConfigUpdates) {
        let client = new_test_client(&dyncfgs).await;
        let (mut write, _) = client.expect_open::<(), (), u64, i64>(ShardId::new()).await;
        let five = Antichain::from_elem(5);

        // Upper is not past 5.
        assert_eq!(write.wait_for_upper_past(&five).now_or_never(), None);

        // Upper is still not past 5.
        write
            .expect_compare_and_append(&[(((), ()), 1, 1)], 0, 5)
            .await;
        assert_eq!(write.wait_for_upper_past(&five).now_or_never(), None);

        // Upper is past 5.
        write
            .expect_compare_and_append(&[(((), ()), 5, 1)], 5, 7)
            .await;
        assert_eq!(write.wait_for_upper_past(&five).now_or_never(), Some(()));
        assert_eq!(write.upper(), &Antichain::from_elem(7));

        // Waiting for previous uppers does not regress the handle's cached
        // upper.
        assert_eq!(
            write
                .wait_for_upper_past(&Antichain::from_elem(2))
                .now_or_never(),
            Some(())
        );
        assert_eq!(write.upper(), &Antichain::from_elem(7));
    }

    #[mz_ore::test(tokio::test(flavor = "multi_thread"))]
    #[cfg_attr(miri, ignore)] // unsupported operation: returning ready events from epoll_wait is not yet implemented
    async fn fetch_recent_upper_linearized() {
        type Timestamp = u64;
        let max_upper = 1000;

        let shard_id = ShardId::new();
        let mut clients = PersistClientCache::new_no_metrics();
        let upper_writer_client = clients.open(PersistLocation::new_in_mem()).await.unwrap();
        let (mut upper_writer, _) = upper_writer_client
            .expect_open::<(), (), Timestamp, i64>(shard_id)
            .await;
        // Clear the state cache between each client to maximally disconnect
        // them from each other.
        clients.clear_state_cache();
        let upper_reader_client = clients.open(PersistLocation::new_in_mem()).await.unwrap();
        let (mut upper_reader, _) = upper_reader_client
            .expect_open::<(), (), Timestamp, i64>(shard_id)
            .await;
        let (tx, rx) = mpsc::channel();

        let task = task::spawn(|| "upper-reader", async move {
            let mut upper = Timestamp::MIN;

            while upper < max_upper {
                while let Ok(new_upper) = rx.try_recv() {
                    upper = new_upper;
                }

                let recent_upper = upper_reader
                    .fetch_recent_upper()
                    .await
                    .as_option()
                    .cloned()
                    .expect("u64 is totally ordered and the shard is not finalized");
                assert!(
                    recent_upper >= upper,
                    "recent upper {recent_upper:?} is less than known upper {upper:?}"
                );
            }
        });

        for upper in Timestamp::MIN..max_upper {
            let next_upper = upper + 1;
            upper_writer
                .expect_compare_and_append(&[], upper, next_upper)
                .await;
            tx.send(next_upper).expect("send failed");
        }

        task.await.expect("await failed");
    }
}

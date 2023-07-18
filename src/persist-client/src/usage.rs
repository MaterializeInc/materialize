// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Introspection of storage utilization by persist

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Instant;

use futures::stream::{FuturesUnordered, StreamExt};
use mz_ore::cast::CastFrom;
use mz_persist::location::Blob;
use tokio::sync::Semaphore;
use tracing::{error, info};

use crate::cfg::PersistConfig;
use crate::internal::paths::{BlobKey, BlobKeyPrefix, PartialBlobKey, WriterKey};
use crate::internal::state::HollowBlobRef;
use crate::internal::state_versions::StateVersions;
use crate::write::WriterId;
use crate::{retry_external, Metrics, PersistClient, ShardId};

/// A breakdown of the size of various contributions to a shard's blob
/// usage that is actively referenced by any live state in Consensus.
#[derive(Clone, Debug)]
pub struct ShardUsageReferenced {
    pub(crate) batches_bytes: u64,
    pub(crate) rollup_bytes: u64,
}

impl ShardUsageReferenced {
    /// Byte size of all data referenced in state for the shard.
    pub fn size_bytes(&self) -> u64 {
        let Self {
            batches_bytes,
            rollup_bytes,
        } = self;
        *batches_bytes + *rollup_bytes
    }
}

/// The referenced blob usage for a set of shards.
#[derive(Debug)]
pub struct ShardsUsageReferenced {
    /// The data for each shard.
    pub by_shard: BTreeMap<ShardId, ShardUsageReferenced>,
}

/// A breakdown of the size of various contributions to a shard's blob (S3)
/// usage.
///
/// This is structured as a "funnel", in which the steps are additive.
/// Specifically `1=2a+2b`, `2a=3a+3b`, `3a=4a+4b`, `4a=5a+5b` (so the "a"s are
/// the funnel and the "b"s are places where data splits out of the funnel).
#[derive(Clone, Debug)]
pub struct ShardUsageAudit {
    /// 5a: Data in batches/parts referenced by the most recent version of
    /// state.
    pub current_state_batches_bytes: u64,
    /// 5b: Data in rollups referenced by the most recent version of state.
    pub current_state_rollups_bytes: u64,
    /// 4b: Data referenced by a live version of state that is not the most
    /// recent.
    ///
    /// Possible causes:
    /// - SeqNo hold
    /// - Waiting for a GC run
    pub referenced_not_current_state_bytes: u64,
    /// 3b: Data not referenced by any live version of state.
    ///
    /// Possible causes:
    /// - A batch or rollup that's about to be linked into state
    /// - A batch leaked by a crash, but the writer has not yet been force
    ///   expired
    /// - A rollup leaked by a crash, but GC has not yet advanced past the
    ///   SeqNo
    pub not_leaked_not_referenced_bytes: u64,
    /// 2b: Data that is eligible for reclamation by a (future) leaked blob
    /// cleanup task (#17322).
    ///
    /// Possible causes:
    /// - A batch or rollup written by a process which crashed (or was rolled)
    ///   before it could be linked into state.
    pub leaked_bytes: u64,
}

impl ShardUsageAudit {
    /// 4a: Data referenced by the most recent version of state.
    pub fn current_state_bytes(&self) -> u64 {
        self.current_state_batches_bytes + self.current_state_rollups_bytes
    }

    /// 3a: Data referenced by any live version of state.
    pub fn referenced_bytes(&self) -> u64 {
        self.current_state_bytes() + self.referenced_not_current_state_bytes
    }

    /// 2a: Data that would not be reclaimed by a (future) leaked blob
    /// cleanup task (#17322).
    pub fn not_leaked_bytes(&self) -> u64 {
        self.referenced_bytes() + self.not_leaked_not_referenced_bytes
    }

    /// 1: Raw blob (S3) usage.
    ///
    /// NB: Due to race conditions between reads of blob and consensus in the
    /// usage code, this might be a slight under-counting.
    pub fn total_bytes(&self) -> u64 {
        self.not_leaked_bytes() + self.leaked_bytes
    }
}

/// The blob (S3) usage of all shards in an environment.
#[derive(Clone, Debug)]
pub struct ShardsUsageAudit {
    /// The data for each shard.
    pub by_shard: BTreeMap<ShardId, ShardUsageAudit>,
    /// Data not attributable to any particular shard. This _should_ always be
    /// 0; a nonzero value indicates either persist wrote an invalid blob key,
    /// or another process is storing data under the same path (!)
    pub unattributable_bytes: u64,
}

#[derive(Clone, Debug, Default)]
struct BlobUsage {
    by_shard: BTreeMap<ShardId, ShardBlobUsage>,
    unattributable_bytes: u64,
    batch_part_bytes: u64,
    batch_part_count: u64,
    rollup_size: u64,
    rollup_count: u64,
    total_size: u64,
    total_count: u64,
}

#[derive(Clone, Debug, Default)]
struct ShardBlobUsage {
    by_writer: BTreeMap<WriterKey, u64>,
    rollup_bytes: u64,
}

impl ShardBlobUsage {
    fn total_bytes(&self) -> u64 {
        self.by_writer.values().copied().sum::<u64>() + self.rollup_bytes
    }
}

/// Provides access to storage usage metrics for a specific Blob
#[derive(Clone, Debug)]
pub struct StorageUsageClient {
    cfg: PersistConfig,
    blob: Arc<dyn Blob + Send + Sync>,
    metrics: Arc<Metrics>,
    state_versions: Arc<StateVersions>,
}

impl StorageUsageClient {
    /// Creates a new StorageUsageClient.
    pub fn open(client: PersistClient) -> Self {
        let state_versions = Arc::new(StateVersions::new(
            client.cfg.clone(),
            Arc::clone(&client.consensus),
            Arc::clone(&client.blob),
            Arc::clone(&client.metrics),
        ));
        StorageUsageClient {
            cfg: client.cfg,
            blob: client.blob,
            metrics: client.metrics,
            state_versions,
        }
    }

    /// Computes [ShardUsageReferenced] for a single shard. Suitable for customer billing.
    pub async fn shard_usage_referenced(&self, shard_id: ShardId) -> ShardUsageReferenced {
        let mut start = Instant::now();
        let states_iter = self
            .state_versions
            .fetch_all_live_states::<u64>(shard_id)
            .await;
        let states_iter = match states_iter {
            Some(x) => x,
            None => {
                return ShardUsageReferenced {
                    batches_bytes: 0,
                    rollup_bytes: 0,
                }
            }
        };
        let mut states_iter = states_iter
            .check_ts_codec()
            .expect("ts should be a u64 in all prod shards");

        let shard_metrics = &self.metrics.shards.shard(&shard_id, "unknown");
        shard_metrics
            .gc_live_diffs
            .set(u64::cast_from(states_iter.len()));

        let now = Instant::now();
        self.metrics
            .audit
            .step_state
            .inc_by(now.duration_since(start).as_secs_f64());
        start = now;

        let mut batches_bytes = 0;
        let mut rollup_bytes = 0;
        while let Some(_) = states_iter.next(|diff| {
            diff.referenced_blob_fn(|blob| match blob {
                HollowBlobRef::Batch(batch) => {
                    for part in &batch.parts {
                        batches_bytes += part.encoded_size_bytes;
                    }
                }
                HollowBlobRef::Rollup(rollup) => {
                    rollup_bytes += rollup.encoded_size_bytes.unwrap_or(1);
                }
            })
        }) {}

        let referenced = ShardUsageReferenced {
            batches_bytes: u64::cast_from(batches_bytes),
            rollup_bytes: u64::cast_from(rollup_bytes),
        };

        let current_state_sizes = states_iter.state().size_metrics();
        shard_metrics
            .usage_current_state_batches_bytes
            .set(u64::cast_from(current_state_sizes.state_batches_bytes));
        shard_metrics
            .usage_current_state_rollups_bytes
            .set(u64::cast_from(current_state_sizes.state_rollups_bytes));
        shard_metrics.usage_referenced_not_current_state_bytes.set(
            referenced.size_bytes()
                - u64::cast_from(
                    current_state_sizes.state_batches_bytes
                        + current_state_sizes.state_rollups_bytes,
                ),
        );

        self.metrics
            .audit
            .step_math
            .inc_by(now.duration_since(start).as_secs_f64());

        referenced
    }

    /// Computes [ShardUsageReferenced] for a given set of shards. Suitable for customer billing.
    pub async fn shards_usage_referenced<I>(&self, shard_ids: I) -> ShardsUsageReferenced
    where
        I: IntoIterator<Item = ShardId>,
    {
        let semaphore = Arc::new(Semaphore::new(
            self.cfg.dynamic.usage_state_fetch_concurrency_limit(),
        ));
        let by_shard_futures = FuturesUnordered::new();
        for shard_id in shard_ids {
            let semaphore = Arc::clone(&semaphore);
            let shard_usage_fut = async move {
                let _permit = semaphore
                    .acquire()
                    .await
                    .expect("acquiring permit from open semaphore");
                let shard_usage = self.shard_usage_referenced(shard_id).await;
                (shard_id, shard_usage)
            };
            by_shard_futures.push(shard_usage_fut);
        }
        let by_shard = by_shard_futures.collect().await;
        ShardsUsageReferenced { by_shard }
    }

    /// Computes [ShardUsageAudit] for a single shard.
    ///
    /// Performs a full scan of [Blob] and [mz_persist::location::Consensus] to compute a full audit
    /// of blob usage, categorizing both referenced and unreferenced blobs (see [ShardUsageAudit]
    /// for full details). While [ShardUsageAudit::referenced_bytes] is suitable for billing, prefer
    /// [Self::shard_usage_referenced] to avoid the (costly!) scan of [Blob] if the additional
    /// categorizations are not needed.
    pub async fn shard_usage_audit(&self, shard_id: ShardId) -> ShardUsageAudit {
        let mut blob_usage = self.blob_raw_usage(BlobKeyPrefix::Shard(&shard_id)).await;
        let blob_usage = blob_usage.by_shard.remove(&shard_id).unwrap_or_default();
        self.shard_usage_given_blob_usage(shard_id, &blob_usage)
            .await
    }

    /// Computes [ShardUsageAudit] for every shard in an env.
    ///
    /// See [Self::shard_usage_audit] for more details on when to use a full audit.
    pub async fn shards_usage_audit(&self) -> ShardsUsageAudit {
        let blob_usage = self.blob_raw_usage(BlobKeyPrefix::All).await;
        self.metrics
            .audit
            .blob_batch_part_bytes
            .set(blob_usage.batch_part_bytes);
        self.metrics
            .audit
            .blob_batch_part_count
            .set(blob_usage.batch_part_count);
        self.metrics
            .audit
            .blob_rollup_bytes
            .set(blob_usage.rollup_size);
        self.metrics
            .audit
            .blob_rollup_count
            .set(blob_usage.rollup_count);
        self.metrics.audit.blob_bytes.set(blob_usage.total_size);
        self.metrics.audit.blob_count.set(blob_usage.total_count);

        let semaphore = Semaphore::new(self.cfg.dynamic.usage_state_fetch_concurrency_limit());
        let by_shard_futures = FuturesUnordered::new();
        for (shard_id, total_bytes) in blob_usage.by_shard.iter() {
            let shard_usage_fut = async {
                let _permit = semaphore
                    .acquire()
                    .await
                    .expect("acquiring permit from open semaphore");
                let shard_usage = self
                    .shard_usage_given_blob_usage(*shard_id, total_bytes)
                    .await;
                (*shard_id, shard_usage)
            };
            by_shard_futures.push(shard_usage_fut);
        }

        let by_shard = by_shard_futures.collect().await;
        ShardsUsageAudit {
            by_shard,
            unattributable_bytes: blob_usage.unattributable_bytes,
        }
    }

    async fn blob_raw_usage(&self, prefix: BlobKeyPrefix<'_>) -> BlobUsage {
        retry_external(
            &self.metrics.retries.external.storage_usage_shard_size,
            || async {
                let mut start = Instant::now();
                let mut keys = 0;
                let mut usage = BlobUsage::default();
                self.blob
                    .list_keys_and_metadata(&prefix.to_string(), &mut |metadata| {
                        // Increment the step timing metrics as we go, so it
                        // doesn't all show up at the end.
                        keys += 1;
                        if keys % 100 == 0 {
                            let now = Instant::now();
                            self.metrics
                                .audit
                                .step_blob_metadata
                                .inc_by(now.duration_since(start).as_secs_f64());
                            start = now;
                        }

                        match BlobKey::parse_ids(metadata.key) {
                            Ok((shard, partial_blob_key)) => {
                                let shard_usage = usage.by_shard.entry(shard).or_default();

                                match partial_blob_key {
                                    PartialBlobKey::Batch(writer_id, _) => {
                                        usage.batch_part_bytes += metadata.size_in_bytes;
                                        usage.batch_part_count += 1;
                                        *shard_usage.by_writer.entry(writer_id).or_default() +=
                                            metadata.size_in_bytes;
                                    }
                                    PartialBlobKey::Rollup(_, _) => {
                                        usage.rollup_size += metadata.size_in_bytes;
                                        usage.rollup_count += 1;
                                        shard_usage.rollup_bytes += metadata.size_in_bytes;
                                    }
                                }
                            }
                            _ => {
                                info!("unknown blob: {}: {}", metadata.key, metadata.size_in_bytes);
                                usage.unattributable_bytes += metadata.size_in_bytes;
                            }
                        }
                        usage.total_size += metadata.size_in_bytes;
                        usage.total_count += 1;
                    })
                    .await?;
                self.metrics
                    .audit
                    .step_blob_metadata
                    .inc_by(start.elapsed().as_secs_f64());
                Ok(usage)
            },
        )
        .await
    }

    async fn shard_usage_given_blob_usage(
        &self,
        shard_id: ShardId,
        blob_usage: &ShardBlobUsage,
    ) -> ShardUsageAudit {
        let mut start = Instant::now();
        let states_iter = self
            .state_versions
            .fetch_all_live_states::<u64>(shard_id)
            .await;
        let states_iter = match states_iter {
            Some(x) => x,
            None => {
                // It's unexpected for a shard to exist in blob but not in
                // consensus, but it could happen. For example, if an initial
                // rollup has been written but the initial CaS hasn't yet
                // succeeded (or if a `bin/environmentd --reset` is interrupted
                // in dev). Be loud because it's unexpected, but handle it
                // because it can happen.
                error!(
                    concat!(
                    "shard {} existed in blob but not in consensus. This should be quite rare in ",
                    "prod, but is semi-expected in development if `bin/environmentd --reset` gets ",
                    "interrupted"),
                    shard_id
                );
                return ShardUsageAudit {
                    current_state_batches_bytes: 0,
                    current_state_rollups_bytes: 0,
                    referenced_not_current_state_bytes: 0,
                    not_leaked_not_referenced_bytes: 0,
                    leaked_bytes: blob_usage.total_bytes(),
                };
            }
        };
        let mut states_iter = states_iter
            .check_ts_codec()
            .expect("ts should be a u64 in all prod shards");
        let now = Instant::now();
        self.metrics
            .audit
            .step_state
            .inc_by(now.duration_since(start).as_secs_f64());
        start = now;

        let shard_metrics = self.metrics.shards.shard(&shard_id, "unknown");
        shard_metrics
            .gc_live_diffs
            .set(u64::cast_from(states_iter.len()));

        let mut referenced_batches_bytes = BTreeMap::new();
        let mut referenced_other_bytes = 0;
        while let Some(_) = states_iter.next(|x| {
            x.referenced_blob_fn(|x| match x {
                HollowBlobRef::Batch(x) => {
                    for part in x.parts.iter() {
                        let parsed = BlobKey::parse_ids(&part.key.complete(&shard_id));
                        if let Ok((_, PartialBlobKey::Batch(writer_id, _))) = parsed {
                            let writer_referenced_batches_bytes =
                                referenced_batches_bytes.entry(writer_id).or_default();
                            *writer_referenced_batches_bytes +=
                                u64::cast_from(part.encoded_size_bytes);
                        } else {
                            // Unexpected, but don't need to panic here.
                            referenced_other_bytes += u64::cast_from(part.encoded_size_bytes);
                        }
                    }
                }
                HollowBlobRef::Rollup(x) => {
                    referenced_other_bytes +=
                        u64::cast_from(x.encoded_size_bytes.unwrap_or_default());
                }
            })
        }) {}

        let mut current_state_batches_bytes = 0;
        let mut current_state_rollups_bytes = 0;
        states_iter.state().map_blobs(|x| match x {
            HollowBlobRef::Batch(x) => {
                for part in x.parts.iter() {
                    current_state_batches_bytes += u64::cast_from(part.encoded_size_bytes);
                }
            }
            HollowBlobRef::Rollup(x) => {
                current_state_rollups_bytes +=
                    u64::cast_from(x.encoded_size_bytes.unwrap_or_default());
            }
        });
        let current_state_bytes = current_state_batches_bytes + current_state_rollups_bytes;

        let live_writers = &states_iter.state().collections.writers;
        let ret = ShardUsageAudit::from(ShardUsageCumulativeMaybeRacy {
            current_state_batches_bytes,
            current_state_bytes,
            referenced_other_bytes,
            referenced_batches_bytes: &referenced_batches_bytes,
            // In the future, this is likely to include a "grace period" so recent but non-current
            // versions are also considered live
            minimum_version: WriterKey::for_version(&self.cfg.build_version),
            live_writers,
            blob_usage,
        });

        // Sanity check that we didn't obviously do anything wrong.
        assert_eq!(ret.total_bytes(), blob_usage.total_bytes());

        shard_metrics
            .usage_current_state_batches_bytes
            .set(ret.current_state_batches_bytes);
        shard_metrics
            .usage_current_state_rollups_bytes
            .set(ret.current_state_rollups_bytes);
        shard_metrics
            .usage_referenced_not_current_state_bytes
            .set(ret.referenced_not_current_state_bytes);
        shard_metrics
            .usage_not_leaked_not_referenced_bytes
            .set(ret.not_leaked_not_referenced_bytes);
        shard_metrics.usage_leaked_bytes.set(ret.leaked_bytes);

        self.metrics
            .audit
            .step_math
            .inc_by(start.elapsed().as_secs_f64());
        ret
    }

    /// Returns the size (in bytes) of a subset of blobs specified by
    /// [BlobKeyPrefix]
    ///
    /// Can be safely called within retry_external to ensure it succeeds
    #[cfg(test)]
    async fn size(
        &self,
        prefix: BlobKeyPrefix<'_>,
    ) -> Result<u64, mz_persist::location::ExternalError> {
        let mut total_size = 0;
        self.blob
            .list_keys_and_metadata(&prefix.to_string(), &mut |metadata| {
                total_size += metadata.size_in_bytes;
            })
            .await?;
        Ok(total_size)
    }
}

#[derive(Debug)]
struct ShardUsageCumulativeMaybeRacy<'a, T> {
    current_state_batches_bytes: u64,
    current_state_bytes: u64,
    referenced_other_bytes: u64,
    referenced_batches_bytes: &'a BTreeMap<WriterKey, u64>,
    minimum_version: WriterKey,
    live_writers: &'a BTreeMap<WriterId, T>,
    blob_usage: &'a ShardBlobUsage,
}

impl<T: std::fmt::Debug> From<ShardUsageCumulativeMaybeRacy<'_, T>> for ShardUsageAudit {
    fn from(x: ShardUsageCumulativeMaybeRacy<'_, T>) -> Self {
        let mut not_leaked_bytes = 0;
        let mut total_bytes = 0;
        for (writer_key, bytes) in x.blob_usage.by_writer.iter() {
            total_bytes += *bytes;
            let writer_key_is_live = match writer_key {
                WriterKey::Id(writer_id) => x.live_writers.contains_key(writer_id),
                version @ WriterKey::Version(_) => *version >= x.minimum_version,
            };
            if writer_key_is_live {
                not_leaked_bytes += *bytes;
            } else {
                // This writer is no longer live, so it can never again link
                // anything into state. As a result, we know that anything it
                // hasn't linked into state is now leaked and eligible for
                // reclamation by a (future) leaked blob detector.
                let writer_referenced =
                    x.referenced_batches_bytes.get(writer_key).map_or(0, |x| *x);
                // It's possible, due to races, that a writer has more
                // referenced batches in state than we saw for that writer in
                // blob. Cap it at the number of bytes we saw in blob, otherwise
                // we could hit the "blob inputs should be cumulative" panic
                // below.
                not_leaked_bytes += std::cmp::min(*bytes, writer_referenced);
            }
        }
        // For now, assume rollups aren't leaked. We could compute which rollups
        // are leaked by plumbing things more precisely, if that's necessary.
        total_bytes += x.blob_usage.rollup_bytes;
        not_leaked_bytes += x.blob_usage.rollup_bytes;

        let leaked_bytes = total_bytes
            .checked_sub(not_leaked_bytes)
            .expect("blob inputs should be cumulative");
        let referenced_batches_bytes = x.referenced_batches_bytes.values().sum::<u64>();
        let referenced_bytes = referenced_batches_bytes + x.referenced_other_bytes;
        let mut referenced_not_current_state_bytes = referenced_bytes
            .checked_sub(x.current_state_bytes)
            .expect("state inputs should be cumulative");
        let mut current_state_rollups_bytes = x
            .current_state_bytes
            .checked_sub(x.current_state_batches_bytes)
            .expect("state inputs should be cumulative");
        let mut current_state_batches_bytes = x.current_state_batches_bytes;

        // If we could transactionally read both blob and consensus, the
        // cumulative numbers would all line up. We can't, so we have to adjust
        // them up a bit to account for the race condition. We read blob first,
        // and then consensus, but the race could go either way: a blob that is
        // currently in state could be deleted from both in between the reads,
        // OR a blob could be written and linked into state in between the
        // reads. We could do a blob-state-blob sandwich, and then use
        // differences between the two blob reads to reason about what
        // specifically happens in a race, but this: (a) takes memory
        // proportional to `O(blobs)` and (b) is overkill. Instead, we adjust by
        // category.
        //
        // In the event of a discrepancy, we ensure that numbers will only get
        // smaller (by policy, we prefer to under-count for billing).
        // Concretely:
        // - If referenced_bytes (which comes from state) is > not_leaked_bytes
        //   (which is a subset of what we read from blob), then we've
        //   definitely hit the race and the funnel doesn't make sense (some of
        //   the things that are supposed to be smaller are actually bigger).
        //   Figure out how much we have to fix up the numbers and call it
        //   "possible_over_count".
        // - Then go "down" ("up"?) the funnel category by category (each of
        //   which represented here by diffs from the previous category)
        //   reducing them until we've adjusted them collectively down by
        //   "possible_over_count".
        // - First is not_leaked_not_referenced_bytes (the diff from
        //   referenced_bytes to not_leaked_bytes).
        // - Then, if necessary, carry the adjustment to
        //   referenced_not_current_state_bytes (the diff from
        //   current_state_bytes to referenced_bytes).
        // - And so on.
        // - Note that the largest possible value for possible_over_count is
        //   referenced_bytes (e.g. if we read nothing from blob). Because all
        //   the diffs add up to referenced_bytes, we're guaranteed that
        //   "possible_over_count" will have reached 0 by the time we've
        //   finished adjusting all the categories.
        let mut not_leaked_not_referenced_bytes = not_leaked_bytes.saturating_sub(referenced_bytes);
        let mut possible_over_count = referenced_bytes.saturating_sub(not_leaked_bytes);
        fn adjust(adjustment: &mut u64, val: &mut u64) {
            let x = std::cmp::min(*adjustment, *val);
            *adjustment -= x;
            *val -= x;
        }
        adjust(
            &mut possible_over_count,
            &mut not_leaked_not_referenced_bytes,
        );
        adjust(
            &mut possible_over_count,
            &mut referenced_not_current_state_bytes,
        );
        adjust(&mut possible_over_count, &mut current_state_rollups_bytes);
        adjust(&mut possible_over_count, &mut current_state_batches_bytes);
        assert_eq!(possible_over_count, 0);

        let ret = ShardUsageAudit {
            current_state_batches_bytes,
            current_state_rollups_bytes,
            referenced_not_current_state_bytes,
            not_leaked_not_referenced_bytes,
            leaked_bytes,
        };

        // These ones are guaranteed to be equal.
        debug_assert_eq!(ret.total_bytes(), total_bytes);
        debug_assert_eq!(ret.not_leaked_bytes(), not_leaked_bytes);
        // The rest might have been reduced because of the race condition.
        debug_assert!(ret.referenced_bytes() <= referenced_bytes);
        debug_assert!(ret.current_state_bytes() <= x.current_state_bytes);
        debug_assert!(ret.current_state_batches_bytes <= x.current_state_batches_bytes);
        ret
    }
}

impl std::fmt::Display for ShardUsageAudit {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            concat!(
                "total s3 contents:                  {}\n",
                "  leaked:                           {}\n",
                "  not leaked:                       {}\n",
                "    not leaked not referenced:      {}\n",
                "    referenced:                     {}\n",
                "      referenced not current state: {}\n",
                "      current state:                {}\n",
                "        current rollups:            {}\n",
                "        current batches:            {}",
            ),
            HumanBytes(self.total_bytes()),
            HumanBytes(self.leaked_bytes),
            HumanBytes(self.not_leaked_bytes()),
            HumanBytes(self.not_leaked_not_referenced_bytes),
            HumanBytes(self.referenced_bytes()),
            HumanBytes(self.referenced_not_current_state_bytes),
            HumanBytes(self.current_state_bytes()),
            HumanBytes(self.current_state_rollups_bytes),
            HumanBytes(self.current_state_batches_bytes),
        )
    }
}

pub(crate) struct HumanBytes(pub u64);

impl std::fmt::Display for HumanBytes {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.0 < 1_240u64 {
            return write!(f, "{}B", self.0);
        }
        #[allow(clippy::as_conversions)]
        let mut bytes = self.0 as f64 / 1_024f64;
        if bytes < 1_240f64 {
            return write!(f, "{:.1}KiB", bytes);
        }
        bytes = bytes / 1_024f64;
        if bytes < 1_240f64 {
            return write!(f, "{:.1}MiB", bytes);
        }
        bytes = bytes / 1_024f64;
        if bytes < 1_240f64 {
            return write!(f, "{:.1}GiB", bytes);
        }
        bytes = bytes / 1_024f64;
        write!(f, "{:.1}TiB", bytes)
    }
}

#[cfg(test)]
mod tests {
    use crate::cfg::PersistParameters;
    use bytes::Bytes;
    use mz_persist::location::{Atomicity, SeqNo};
    use semver::Version;
    use timely::progress::Antichain;

    use crate::internal::paths::{PartialRollupKey, RollupId};
    use crate::tests::new_test_client;
    use crate::ShardId;

    use super::*;

    #[mz_ore::test(tokio::test)]
    async fn size() {
        let data = vec![
            (("1".to_owned(), "one".to_owned()), 1, 1),
            (("2".to_owned(), "two".to_owned()), 2, 1),
            (("3".to_owned(), "three".to_owned()), 3, 1),
            (("4".to_owned(), "four".to_owned()), 4, 1),
        ];

        let client = new_test_client().await;
        let build_version = client.cfg.build_version.clone();
        let shard_id_one = ShardId::new();
        let shard_id_two = ShardId::new();

        // write one row into shard 1
        let (mut write, _) = client
            .expect_open::<String, String, u64, i64>(shard_id_one)
            .await;
        write.expect_append(&data[..1], vec![0], vec![2]).await;

        // write two rows into shard 2 from writer 1
        let (mut write, _) = client
            .expect_open::<String, String, u64, i64>(shard_id_two)
            .await;
        write.expect_append(&data[1..3], vec![0], vec![4]).await;
        let writer_one = WriterKey::Id(write.writer_id.clone());

        // write one row into shard 2 from writer 2
        let (mut write, _) = client
            .expect_open::<String, String, u64, i64>(shard_id_two)
            .await;
        write.expect_append(&data[4..], vec![0], vec![5]).await;
        let writer_two = WriterKey::Id(write.writer_id.clone());

        let usage = StorageUsageClient::open(client);

        let shard_one_size = usage
            .size(BlobKeyPrefix::Shard(&shard_id_one))
            .await
            .expect("must have shard size");
        let shard_two_size = usage
            .size(BlobKeyPrefix::Shard(&shard_id_two))
            .await
            .expect("must have shard size");
        let writer_one_size = usage
            .size(BlobKeyPrefix::Writer(&shard_id_two, &writer_one))
            .await
            .expect("must have shard size");
        let writer_two_size = usage
            .size(BlobKeyPrefix::Writer(&shard_id_two, &writer_two))
            .await
            .expect("must have shard size");
        let versioned_size = usage
            .size(BlobKeyPrefix::Writer(
                &shard_id_two,
                &WriterKey::for_version(&build_version),
            ))
            .await
            .expect("must have shard size");
        let rollups_size = usage
            .size(BlobKeyPrefix::Rollups(&shard_id_two))
            .await
            .expect("must have shard size");
        let all_size = usage
            .size(BlobKeyPrefix::All)
            .await
            .expect("must have shard size");

        assert!(shard_one_size > 0);
        assert!(shard_two_size > 0);
        assert!(shard_one_size < shard_two_size);
        assert_eq!(
            shard_two_size,
            writer_one_size + writer_two_size + versioned_size + rollups_size
        );
        assert_eq!(all_size, shard_one_size + shard_two_size);

        assert_eq!(
            usage.shard_usage_audit(shard_id_one).await.total_bytes(),
            shard_one_size
        );
        assert_eq!(
            usage.shard_usage_audit(shard_id_two).await.total_bytes(),
            shard_two_size
        );

        let shards_usage = usage.shards_usage_audit().await;
        assert_eq!(shards_usage.by_shard.len(), 2);
        assert_eq!(
            shards_usage
                .by_shard
                .get(&shard_id_one)
                .map(|x| x.total_bytes()),
            Some(shard_one_size)
        );
        assert_eq!(
            shards_usage
                .by_shard
                .get(&shard_id_two)
                .map(|x| x.total_bytes()),
            Some(shard_two_size)
        );
    }

    /// This is just a sanity check for the overall flow of computing ShardUsage.
    /// The edge cases are exercised in separate tests.
    #[mz_ore::test(tokio::test)]
    #[cfg_attr(miri, ignore)] // https://github.com/MaterializeInc/materialize/issues/19981
    async fn usage_sanity() {
        let data = vec![
            (("1".to_owned(), "one".to_owned()), 1, 1),
            (("2".to_owned(), "two".to_owned()), 2, 1),
            (("3".to_owned(), "three".to_owned()), 3, 1),
            (("4".to_owned(), "four".to_owned()), 4, 1),
        ];

        let shard_id = ShardId::new();
        let mut client = new_test_client().await;

        let (mut write0, _) = client
            .expect_open::<String, String, u64, i64>(shard_id)
            .await;
        // Successfully link in a batch from a writer that stays registered.
        write0.expect_compare_and_append(&data[..2], 0, 3).await;
        // Leak a batch from a writer that stays registered.
        let batch = write0
            .batch(&data[..2], Antichain::from_elem(0), Antichain::from_elem(3))
            .await
            .unwrap();
        std::mem::forget(batch);

        let (mut write1, _) = client
            .expect_open::<String, String, u64, i64>(shard_id)
            .await;

        // Successfully link in a batch from a writer that gets expired.
        write1.expect_compare_and_append(&data[2..], 3, 5).await;
        // Leak a batch from a writer that gets expired.
        let batch = write1
            .batch(&data[2..], Antichain::from_elem(3), Antichain::from_elem(5))
            .await
            .unwrap();
        std::mem::forget(batch);
        write1.expire().await;

        // Write a rollup that has an encoded size (the initial rollup has size 0);
        let maintenance = write0.machine.add_rollup_for_current_seqno().await;
        maintenance.perform(&write0.machine, &write0.gc).await;

        client.cfg.build_version.minor += 1;
        let usage = StorageUsageClient::open(client);
        let shard_usage_audit = usage.shard_usage_audit(shard_id).await;
        let shard_usage_referenced = usage.shard_usage_referenced(shard_id).await;
        // We've written data.
        assert!(shard_usage_audit.current_state_batches_bytes > 0);
        assert!(shard_usage_referenced.batches_bytes > 0);
        // There's always at least one rollup.
        assert!(shard_usage_audit.current_state_rollups_bytes > 0);
        assert!(shard_usage_referenced.rollup_bytes > 0);
        // Sadly, it's tricky (and brittle) to ensure that there is data
        // referenced by some live state, but no longer referenced by the
        // current one, so no asserts on referenced_not_current_state_bytes for
        // now.
        //
        // write0 wrote a batch, but never linked it in, but is still active.
        assert!(shard_usage_audit.not_leaked_not_referenced_bytes > 0);
        // write0 wrote a batch, but never linked it in, and is now expired.
        assert!(shard_usage_audit.leaked_bytes > 0);
    }

    #[mz_ore::test(tokio::test)]
    async fn usage_referenced() {
        mz_ore::test::init_logging();

        let data = vec![
            (("1".to_owned(), "one".to_owned()), 1, 1),
            (("2".to_owned(), "two".to_owned()), 2, 1),
            (("3".to_owned(), "three".to_owned()), 3, 1),
            (("4".to_owned(), "four".to_owned()), 4, 1),
        ];

        let shard_id = ShardId::new();
        let mut client = new_test_client().await;
        // make our bookkeeping simple by skipping compaction blobs writes
        client.cfg.compaction_enabled = false;
        // make things interesting and create multiple parts per batch
        let mut params = PersistParameters::default();
        params.blob_target_size = Some(0);
        params.apply(&client.cfg);

        let (mut write, _read) = client
            .expect_open::<String, String, u64, i64>(shard_id)
            .await;

        let mut b1 = write.expect_batch(&data[..2], 0, 3).await;
        let mut b2 = write.expect_batch(&data[2..], 2, 5).await;

        let batches_size = b1
            .batch
            .parts
            .iter()
            .map(|x| u64::cast_from(x.encoded_size_bytes))
            .sum::<u64>()
            + b2.batch
                .parts
                .iter()
                .map(|x| u64::cast_from(x.encoded_size_bytes))
                .sum::<u64>();

        write
            .expect_compare_and_append_batch(&mut [&mut b1], 0, 3)
            .await;
        write
            .expect_compare_and_append_batch(&mut [&mut b2], 3, 5)
            .await;

        let usage = StorageUsageClient::open(client);
        let shard_usage_referenced = usage.shard_usage_referenced(shard_id).await;

        // with compaction disabled, we can do an exact match on batch part byte size
        assert_eq!(shard_usage_referenced.batches_bytes, batches_size);
    }

    fn writer_id(x: char) -> WriterId {
        let x = vec![x, x, x, x].iter().collect::<String>();
        let s = format!("w{x}{x}-{x}-{x}-{x}-{x}{x}{x}");
        s.parse().unwrap()
    }

    struct TestCase {
        current_state_batches_bytes: u64,
        current_state_bytes: u64,
        referenced_other_bytes: u64,
        referenced_batches_bytes: Vec<(char, u64)>,
        live_writers: Vec<char>,
        blob_usage_by_writer: Vec<(char, u64)>,
        blob_usage_rollups: u64,
    }

    impl TestCase {
        #[track_caller]
        fn run(&self, expected: &str) {
            let referenced_batches_bytes = self
                .referenced_batches_bytes
                .iter()
                .map(|(id, b)| (WriterKey::Id(writer_id(*id)), *b))
                .collect();
            let live_writers = self
                .live_writers
                .iter()
                .map(|id| (writer_id(*id), ()))
                .collect();
            let blob_usage = ShardBlobUsage {
                by_writer: self
                    .blob_usage_by_writer
                    .iter()
                    .map(|(id, b)| (WriterKey::Id(writer_id(*id)), *b))
                    .collect(),
                rollup_bytes: self.blob_usage_rollups,
            };
            let input = ShardUsageCumulativeMaybeRacy {
                current_state_batches_bytes: self.current_state_batches_bytes,
                current_state_bytes: self.current_state_bytes,
                referenced_other_bytes: self.referenced_other_bytes,
                referenced_batches_bytes: &referenced_batches_bytes,
                minimum_version: WriterKey::for_version(&Version::new(0, 0, 1)),
                live_writers: &live_writers,
                blob_usage: &blob_usage,
            };
            let usage = ShardUsageAudit::from(input);
            let actual = format!(
                "{} {}/{} {}/{} {}/{} {}/{}",
                usage.total_bytes(),
                usage.leaked_bytes,
                usage.not_leaked_bytes(),
                usage.not_leaked_not_referenced_bytes,
                usage.referenced_bytes(),
                usage.referenced_not_current_state_bytes,
                usage.current_state_bytes(),
                usage.current_state_rollups_bytes,
                usage.current_state_batches_bytes
            );
            assert_eq!(actual, expected);
        }
    }

    #[mz_ore::test]
    fn usage_kitchen_sink() {
        TestCase {
            // - Some data in current batches
            current_state_batches_bytes: 1,
            // - Some data in current rollups: this - current_state_batches_bytes
            current_state_bytes: 2,
            // - Some data in a key we couldn't parse: this-(rollup)
            //   - This one is unexpected in prod, but it seemed nicer than a
            //     panic, ymmv
            referenced_other_bytes: 3,
            // - Some data written by a still active writer: (a, 4)
            // - Some data written by a now-expired writer: (b, 5)
            referenced_batches_bytes: vec![('a', 4), ('b', 5)],
            live_writers: vec!['a'],
            // - Some data leaked by a still active writer: (a, 7) - (a, 4)
            // - Some data leaked by a now-expired writer: (b, 8) - (b, 5)
            blob_usage_by_writer: vec![('a', 7), ('b', 8)],
            // - Some data in rollups
            blob_usage_rollups: 6,
        }
        .run("21 3/18 6/12 10/2 1/1");
    }

    #[mz_ore::test]
    fn usage_funnel() {
        // All data in current_state_batches_bytes
        TestCase {
            current_state_batches_bytes: 1,
            current_state_bytes: 1,
            referenced_other_bytes: 0,
            referenced_batches_bytes: vec![('a', 1)],
            live_writers: vec!['a'],
            blob_usage_by_writer: vec![('a', 1)],
            blob_usage_rollups: 0,
        }
        .run("1 0/1 0/1 0/1 0/1");

        // All data in current_state_rollups_bytes
        TestCase {
            current_state_batches_bytes: 0,
            current_state_bytes: 1,
            referenced_other_bytes: 0,
            referenced_batches_bytes: vec![('a', 1)],
            live_writers: vec!['a'],
            blob_usage_by_writer: vec![('a', 1)],
            blob_usage_rollups: 0,
        }
        .run("1 0/1 0/1 0/1 1/0");

        // All data in referenced_not_current_state_bytes
        TestCase {
            current_state_batches_bytes: 0,
            current_state_bytes: 0,
            referenced_other_bytes: 0,
            referenced_batches_bytes: vec![('a', 1)],
            live_writers: vec!['a'],
            blob_usage_by_writer: vec![('a', 1)],
            blob_usage_rollups: 0,
        }
        .run("1 0/1 0/1 1/0 0/0");

        // All data in not_leaked_not_referenced_bytes
        TestCase {
            current_state_batches_bytes: 0,
            current_state_bytes: 0,
            referenced_other_bytes: 0,
            referenced_batches_bytes: vec![],
            live_writers: vec!['a'],
            blob_usage_by_writer: vec![('a', 1)],
            blob_usage_rollups: 0,
        }
        .run("1 0/1 1/0 0/0 0/0");

        // All data in leaked_bytes
        TestCase {
            current_state_batches_bytes: 0,
            current_state_bytes: 0,
            referenced_other_bytes: 0,
            referenced_batches_bytes: vec![],
            live_writers: vec![],
            blob_usage_by_writer: vec![('a', 1)],
            blob_usage_rollups: 0,
        }
        .run("1 1/0 0/0 0/0 0/0");

        // No data
        TestCase {
            current_state_batches_bytes: 0,
            current_state_bytes: 0,
            referenced_other_bytes: 0,
            referenced_batches_bytes: vec![],
            live_writers: vec![],
            blob_usage_by_writer: vec![],
            blob_usage_rollups: 0,
        }
        .run("0 0/0 0/0 0/0 0/0");
    }

    #[mz_ore::test]
    fn usage_races() {
        // We took a snapshot of blob, and then before getting our states, a
        // bunch of interesting things happened to persist state. We adjust to
        // account for the race down the funnel.

        // Base case: no race
        TestCase {
            current_state_batches_bytes: 2,
            current_state_bytes: 4,
            referenced_other_bytes: 2,
            referenced_batches_bytes: vec![('a', 4)],
            live_writers: vec!['a'],
            blob_usage_by_writer: vec![('a', 8), ('b', 2)],
            blob_usage_rollups: 0,
        }
        .run("10 2/8 2/6 2/4 2/2");

        // Race was enough to affect into leaked
        TestCase {
            current_state_batches_bytes: 2,
            current_state_bytes: 4,
            referenced_other_bytes: 2,
            referenced_batches_bytes: vec![('a', 4)],
            live_writers: vec!['a'],
            blob_usage_by_writer: vec![('a', 8), ('b', 1)],
            blob_usage_rollups: 0,
        }
        .run("9 1/8 2/6 2/4 2/2");

        // Race was enough to affect into not_leaked_not_referenced_bytes
        TestCase {
            current_state_batches_bytes: 2,
            current_state_bytes: 4,
            referenced_other_bytes: 2,
            referenced_batches_bytes: vec![('a', 4)],
            live_writers: vec!['a'],
            blob_usage_by_writer: vec![('a', 7)],
            blob_usage_rollups: 0,
        }
        .run("7 0/7 1/6 2/4 2/2");

        // Race was enough to affect into referenced_not_current_state_bytes
        TestCase {
            current_state_batches_bytes: 2,
            current_state_bytes: 4,
            referenced_other_bytes: 2,
            referenced_batches_bytes: vec![('a', 4)],
            live_writers: vec!['a'],
            blob_usage_by_writer: vec![('a', 5)],
            blob_usage_rollups: 0,
        }
        .run("5 0/5 0/5 1/4 2/2");

        // Race was enough to affect into current_state_rollups_bytes
        TestCase {
            current_state_batches_bytes: 2,
            current_state_bytes: 4,
            referenced_other_bytes: 2,
            referenced_batches_bytes: vec![('a', 4)],
            live_writers: vec!['a'],
            blob_usage_by_writer: vec![('a', 3)],
            blob_usage_rollups: 0,
        }
        .run("3 0/3 0/3 0/3 1/2");

        // Race was enough to affect into current_state_batches_bytes
        TestCase {
            current_state_batches_bytes: 2,
            current_state_bytes: 4,
            referenced_other_bytes: 2,
            referenced_batches_bytes: vec![('a', 4)],
            live_writers: vec!['a'],
            blob_usage_by_writer: vec![('a', 1)],
            blob_usage_rollups: 0,
        }
        .run("1 0/1 0/1 0/1 0/1");
    }

    /// A regression test for (part of) #17752, which led to seeing the "blob
    /// inputs should be cumulative" should be cumulative panic in
    /// staging/canary.
    #[mz_ore::test]
    fn usage_regression_referenced_greater_than_blob() {
        TestCase {
            current_state_batches_bytes: 0,
            current_state_bytes: 0,
            referenced_other_bytes: 0,
            referenced_batches_bytes: vec![('a', 5)],
            live_writers: vec![],
            blob_usage_by_writer: vec![('a', 3)],
            blob_usage_rollups: 0,
        }
        .run("3 0/3 0/3 3/0 0/0");
    }

    /// Regression test for (part of) #17752, where an interrupted
    /// `bin/environmentd --reset` resulted in panic in persist usage code.
    ///
    /// This also tests a (hypothesized) race that's possible in prod where an
    /// initial rollup is written for a shard, but the initial CaS hasn't yet
    /// succeeded.
    #[mz_ore::test(tokio::test)]
    async fn usage_regression_shard_in_blob_not_consensus() {
        let client = new_test_client().await;
        let shard_id = ShardId::new();

        // Somewhat unsatisfying, we manually construct a rollup blob key.
        let key = PartialRollupKey::new(SeqNo(1), &RollupId::new());
        let key = key.complete(&shard_id);
        let () = client
            .blob
            .set(&key, Bytes::from(vec![0, 1, 2]), Atomicity::RequireAtomic)
            .await
            .unwrap();
        let usage = StorageUsageClient::open(client);
        let shards_usage = usage.shards_usage_audit().await;
        assert_eq!(shards_usage.by_shard.len(), 1);
        assert_eq!(
            shards_usage.by_shard.get(&shard_id).unwrap().leaked_bytes,
            3
        );
    }
}

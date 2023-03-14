// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! A durable, truncatable log of versions of [State].

#[cfg(debug_assertions)]
use std::collections::BTreeSet;
use std::fmt::Debug;
use std::ops::ControlFlow::{Break, Continue};
use std::sync::Arc;
use std::time::SystemTime;

use bytes::Bytes;
use differential_dataflow::difference::Semigroup;
use differential_dataflow::lattice::Lattice;
use mz_ore::cast::CastFrom;
use mz_persist::location::{
    Atomicity, Blob, Consensus, Indeterminate, SeqNo, VersionedData, SCAN_ALL,
};
use mz_persist::retry::Retry;
use mz_persist_types::{Codec, Codec64};
use timely::progress::Timestamp;
use tracing::{debug, debug_span, trace, warn, Instrument};

use crate::error::{CodecMismatch, CodecMismatchT};
use crate::internal::encoding::UntypedState;
use crate::internal::machine::{retry_determinate, retry_external};
use crate::internal::metrics::ShardMetrics;
use crate::internal::paths::{BlobKey, PartialBlobKey, PartialRollupKey, RollupId};
#[cfg(debug_assertions)]
use crate::internal::state::HollowBatch;
use crate::internal::state::{HollowBlobRef, HollowRollup, NoOpStateTransition, State, TypedState};
use crate::internal::state_diff::{StateDiff, StateFieldValDiff};
use crate::{Metrics, PersistConfig, ShardId};

/// A durable, truncatable log of versions of [State].
///
/// As persist metadata changes over time, we make its versions (each identified
/// by a [SeqNo]) durable in two ways:
/// - `rollups`: Periodic copies of the entirety of [State], written to [Blob].
/// - `diffs`: Incremental [StateDiff]s, written to [Consensus].
///
/// The following invariants are maintained at all times:
/// - A shard is initialized iff there is at least one version of it in
///   Consensus.
/// - The first version of state is written to `SeqNo(1)`. Each successive state
///   version is assigned its predecessor's SeqNo +1.
/// - `current`: The latest version of state. By definition, the largest SeqNo
///   present in Consensus.
/// - As state changes over time, we keep a range of consecutive versions
///   available. These are periodically `truncated` to prune old versions that
///   are no longer necessary.
/// - `earliest`: The first version of state that it is possible to reconstruct.
///   - Invariant: `earliest <= current.seqno_since()` (we don't garbage collect
///     versions still being used by some reader).
///   - Invariant: `earliest` is always the smallest Seqno present in Consensus.
///     - This doesn't have to be true, but we select to enforce it.
///     - Because the data stored at that smallest Seqno is an incremental diff,
///       to make this invariant work, there needs to be a rollup at either
///       `earliest-1` or `earliest`. We choose `earliest` because it seems to
///       make the code easier to reason about in practice.
///     - A consequence of the above is when we garbage collect old versions of
///       state, we're only free to truncate ones that are `<` the latest rollup
///       that is `<= current.seqno_since`.
/// - `live diffs`: The set of SeqNos present in Consensus at any given time.
/// - `live states`: The range of state versions that it is possible to
///   reconstruct: `[earliest,current]`.
///   - Because of earliest and current invariants above, the range of `live
///     diffs` and `live states` are the same.
/// - The set of known rollups are tracked in the shard state itself.
///   - For efficiency of common operations, the most recent rollup's Blob key
///     is always denormalized in each StateDiff written to Consensus. (As
///     described above, there is always a rollup at earliest, so we're
///     guaranteed that there is always at least one live rollup.)
///   - Invariant: The rollups in `current` exist in Blob.
///     - A consequence is that, if a rollup in a state you believe is `current`
///       doesn't exist, it's a guarantee that `current` has changed (or it's a
///       bug).
///   - Any rollup at a version `< earliest-1` is useless (we've lost the
///     incremental diffs between it and the live states). GC is tasked with
///     deleting these rollups from Blob before truncating diffs from Consensus.
///     Thus, any rollup at a seqno < earliest can be considered "leaked" and
///     deleted by the leaked blob detector.
///   - Note that this means, while `current`'s rollups exist, it will be common
///     for other live states to reference rollups that no longer exist.
#[derive(Debug)]
pub struct StateVersions {
    cfg: PersistConfig,
    pub(crate) consensus: Arc<dyn Consensus + Send + Sync>,
    pub(crate) blob: Arc<dyn Blob + Send + Sync>,
    metrics: Arc<Metrics>,
}

#[derive(Debug, Clone)]
pub struct RecentLiveDiffs(pub Vec<VersionedData>);

#[derive(Debug, Clone)]
pub struct AllLiveDiffs(pub Vec<VersionedData>);

#[derive(Debug, Clone)]
pub struct EncodedRollup {
    pub(crate) shard_id: ShardId,
    pub(crate) seqno: SeqNo,
    pub(crate) key: PartialRollupKey,
    buf: Bytes,
}

impl EncodedRollup {
    pub fn to_hollow(&self) -> HollowRollup {
        HollowRollup {
            key: self.key.clone(),
            encoded_size_bytes: Some(self.buf.len()),
        }
    }
}

impl StateVersions {
    pub fn new(
        cfg: PersistConfig,
        consensus: Arc<dyn Consensus + Send + Sync>,
        blob: Arc<dyn Blob + Send + Sync>,
        metrics: Arc<Metrics>,
    ) -> Self {
        StateVersions {
            cfg,
            consensus,
            blob,
            metrics,
        }
    }

    /// Fetches the `current` state of the requested shard, or creates it if
    /// uninitialized.
    pub async fn maybe_init_shard<K, V, T, D>(
        &self,
        shard_metrics: &ShardMetrics,
    ) -> Result<TypedState<K, V, T, D>, Box<CodecMismatch>>
    where
        K: Debug + Codec,
        V: Debug + Codec,
        T: Timestamp + Lattice + Codec64,
        D: Semigroup + Codec64,
    {
        let shard_id = shard_metrics.shard_id;

        // The common case is that the shard is initialized, so try that first
        let recent_live_diffs = self.fetch_recent_live_diffs::<T>(&shard_id).await;
        if !recent_live_diffs.0.is_empty() {
            return self
                .fetch_current_state(&shard_id, recent_live_diffs.0)
                .await
                .check_codecs(&shard_id);
        }

        // Shard is not initialized, try initializing it.
        let (initial_state, initial_diff) = self.write_initial_rollup(shard_metrics).await;
        let cas_res = retry_external(&self.metrics.retries.external.maybe_init_cas, || async {
            self.try_compare_and_set_current(
                "maybe_init_shard",
                shard_metrics,
                None,
                &initial_state,
                &initial_diff,
            )
            .await
            .map_err(|err| err.into())
        })
        .await;
        match cas_res {
            Ok(()) => Ok(initial_state),
            Err(live_diffs) => {
                // We lost a CaS race and someone else initialized the shard,
                // use the value included in the CaS expectation error.

                let state = self
                    .fetch_current_state(&shard_id, live_diffs)
                    .await
                    .check_codecs(&shard_id);

                // Clean up the rollup blob that we were trying to reference.
                //
                // SUBTLE: If we got an Indeterminate error in the CaS above,
                // but it actually went through, then we'll "contend" with
                // ourselves and get an expectation mismatch. Use the actual
                // fetched state to determine if our rollup actually made it in
                // and decide whether to delete based on that.
                let (_, rollup) = initial_state.latest_rollup();
                let should_delete_rollup = match state.as_ref() {
                    Ok(state) => !state
                        .collections
                        .rollups
                        .values()
                        .any(|x| &x.key == &rollup.key),
                    // If the codecs don't match, then we definitely didn't
                    // write the state.
                    Err(_codec_mismatch) => true,
                };
                if should_delete_rollup {
                    self.delete_rollup(&shard_id, &rollup.key).await;
                }

                state
            }
        }
    }

    /// Updates the state of a shard to a new `current` iff `expected` matches
    /// `current`.
    ///
    /// May be called on uninitialized shards.
    pub async fn try_compare_and_set_current<K, V, T, D>(
        &self,
        cmd_name: &str,
        shard_metrics: &ShardMetrics,
        expected: Option<SeqNo>,
        new_state: &TypedState<K, V, T, D>,
        diff: &StateDiff<T>,
    ) -> Result<Result<(), Vec<VersionedData>>, Indeterminate>
    where
        K: Debug + Codec,
        V: Debug + Codec,
        T: Timestamp + Lattice + Codec64,
        D: Semigroup + Codec64,
    {
        assert_eq!(shard_metrics.shard_id, new_state.shard_id);
        let path = new_state.shard_id.to_string();

        trace!(
            "apply_unbatched_cmd {} attempting {}\n  new_state={:?}",
            cmd_name,
            new_state.seqno(),
            new_state
        );
        let new = self.metrics.codecs.state_diff.encode(|| {
            let mut buf = Vec::new();
            diff.encode(&mut buf);
            VersionedData {
                seqno: new_state.seqno(),
                data: Bytes::from(buf),
            }
        });
        assert_eq!(new.seqno, diff.seqno_to);

        let payload_len = new.data.len();
        let cas_res = retry_determinate(
            &self.metrics.retries.determinate.apply_unbatched_cmd_cas,
            || async {
                self.consensus
                    .compare_and_set(&path, expected, new.clone())
                    .await
            },
        )
        .instrument(debug_span!("apply_unbatched_cmd::cas", payload_len))
        .await
        .map_err(|err| {
            debug!("apply_unbatched_cmd {} errored: {}", cmd_name, err);
            err
        })?;

        match cas_res {
            Ok(()) => {
                trace!(
                    "apply_unbatched_cmd {} succeeded {}\n  new_state={:?}",
                    cmd_name,
                    new_state.seqno(),
                    new_state
                );

                shard_metrics.set_since(new_state.since());
                shard_metrics.set_upper(new_state.upper());
                shard_metrics
                    .batch_part_count
                    .set(u64::cast_from(new_state.batch_part_count()));
                shard_metrics
                    .update_count
                    .set(u64::cast_from(new_state.num_updates()));
                let size_metrics = new_state.size_metrics();
                shard_metrics
                    .largest_batch_size
                    .set(u64::cast_from(size_metrics.largest_batch_bytes));
                shard_metrics
                    .usage_current_state_batches_bytes
                    .set(u64::cast_from(size_metrics.state_batches_bytes));
                shard_metrics
                    .usage_current_state_rollups_bytes
                    .set(u64::cast_from(size_metrics.state_rollups_bytes));
                shard_metrics
                    .seqnos_held
                    .set(u64::cast_from(new_state.seqnos_held()));
                shard_metrics
                    .encoded_diff_size
                    .inc_by(u64::cast_from(payload_len));
                Ok(Ok(()))
            }
            Err(live_diffs) => {
                debug!(
                    "apply_unbatched_cmd {} {} lost the CaS race, retrying: {:?} vs {:?}",
                    new_state.shard_id(),
                    cmd_name,
                    expected,
                    live_diffs.last().map(|x| x.seqno)
                );
                Ok(Err(live_diffs))
            }
        }
    }

    /// Fetches the `current` state of the requested shard.
    ///
    /// Uses the provided hint (live_diffs), which is a possibly outdated
    /// copy of all or recent live diffs, to avoid fetches where possible.
    ///
    /// Panics if called on an uninitialized shard.
    pub async fn fetch_current_state<T>(
        &self,
        shard_id: &ShardId,
        mut live_diffs: Vec<VersionedData>,
    ) -> UntypedState<T>
    where
        T: Timestamp + Lattice + Codec64,
    {
        let retry = self
            .metrics
            .retries
            .fetch_latest_state
            .stream(Retry::persist_defaults(SystemTime::now()).into_retry_stream());
        loop {
            let latest_diff = live_diffs
                .last()
                .expect("initialized shard should have at least one diff");
            let latest_diff = self
                .metrics
                .codecs
                .state_diff
                .decode(|| StateDiff::<T>::decode(&self.cfg.build_version, &latest_diff.data));
            let mut state = match self
                .fetch_rollup_at_key(shard_id, &latest_diff.latest_rollup_key)
                .await
            {
                Some(x) => x,
                None => {
                    // The rollup that this diff referenced is gone, so the diff
                    // must be out of date. Try again. Intentionally don't sleep on retry.
                    retry.retries.inc();
                    let earliest_before_refetch = live_diffs
                        .first()
                        .expect("initialized shard should have at least one diff")
                        .seqno;
                    live_diffs = self.fetch_recent_live_diffs::<T>(shard_id).await.0;

                    // We should only hit the race condition that leads to a
                    // refetch if the set of live diffs changed out from under
                    // us.
                    //
                    // TODO: Make this an assert once we're 100% sure the above
                    // is always true.
                    let earliest_after_refetch = live_diffs
                        .first()
                        .expect("initialized shard should have at least one diff")
                        .seqno;
                    if earliest_before_refetch >= earliest_after_refetch {
                        warn!("logic error: fetch_current_state refetch expects earliest live diff to advance: {} vs {}", earliest_before_refetch, earliest_after_refetch)
                    }
                    continue;
                }
            };

            let rollup_seqno = state.seqno();
            let diffs = live_diffs.iter().filter(|x| x.seqno > rollup_seqno);
            state.apply_encoded_diffs(&self.cfg, &self.metrics, diffs);
            return state;
        }
    }

    /// Updates the provided state to current.
    ///
    /// This method differs from [Self::fetch_current_state] in that it
    /// optimistically fetches only the diffs since state.seqno and only falls
    /// back to fetching all of them when necessary.
    pub async fn fetch_and_update_to_current<K, V, T, D>(
        &self,
        state: &mut TypedState<K, V, T, D>,
    ) -> Result<(), Box<CodecMismatch>>
    where
        K: Debug + Codec,
        V: Debug + Codec,
        T: Timestamp + Lattice + Codec64,
        D: Semigroup + Codec64,
    {
        let path = state.shard_id.to_string();
        let diffs_to_current =
            retry_external(&self.metrics.retries.external.fetch_state_scan, || async {
                self.consensus
                    .scan(&path, state.seqno.next(), SCAN_ALL)
                    .await
            })
            .instrument(debug_span!("fetch_state::scan"))
            .await;
        let seqno_before = state.seqno;
        let diffs_apply = diffs_to_current
            .first()
            .map_or(true, |x| x.seqno == seqno_before.next());
        if diffs_apply {
            state.apply_encoded_diffs(&self.cfg, &self.metrics, &diffs_to_current);
            Ok(())
        } else {
            let recent_live_diffs = self.fetch_recent_live_diffs::<T>(&state.shard_id).await;
            *state = self
                .fetch_current_state(&state.shard_id, recent_live_diffs.0)
                .await
                .check_codecs(&state.shard_id)?;
            Ok(())
        }
    }

    /// Returns an iterator over all live states for the requested shard.
    ///
    /// Returns None if called on an uninitialized shard.
    pub async fn fetch_all_live_states<T>(
        &self,
        shard_id: ShardId,
    ) -> Option<UntypedStateVersionsIter<T>>
    where
        T: Timestamp + Lattice + Codec64,
    {
        let retry = self
            .metrics
            .retries
            .fetch_live_states
            .stream(Retry::persist_defaults(SystemTime::now()).into_retry_stream());
        let mut all_live_diffs = self.fetch_all_live_diffs(&shard_id).await;
        loop {
            let earliest_live_diff = match all_live_diffs.0.first() {
                Some(x) => x,
                None => return None,
            };
            let state = match self
                .fetch_rollup_at_seqno(
                    &shard_id,
                    all_live_diffs.0.clone(),
                    earliest_live_diff.seqno,
                )
                .await
            {
                Some(x) => x,
                None => {
                    // We maintain an invariant that a rollup always exists for
                    // the earliest live diff. Since we didn't find out, that
                    // can only mean that the live_diffs we just fetched are
                    // obsolete (there's a race condition with gc). This should
                    // be rare in practice, so inc a counter and try again.
                    // Intentionally don't sleep on retry.
                    retry.retries.inc();
                    let earliest_before_refetch = earliest_live_diff.seqno;
                    all_live_diffs = self.fetch_all_live_diffs(&shard_id).await;

                    // We should only hit the race condition that leads to a
                    // refetch if the set of live diffs changed out from under
                    // us.
                    //
                    // TODO: Make this an assert once we're 100% sure the above
                    // is always true.
                    let earliest_after_refetch = all_live_diffs
                        .0
                        .first()
                        .expect("initialized shard should have at least one diff")
                        .seqno;
                    if earliest_before_refetch >= earliest_after_refetch {
                        warn!("logic error: fetch_current_state refetch expects earliest live diff to advance: {} vs {}", earliest_before_refetch, earliest_after_refetch)
                    }
                    continue;
                }
            };
            assert_eq!(earliest_live_diff.seqno, state.seqno());
            return Some(UntypedStateVersionsIter {
                shard_id,
                cfg: self.cfg.clone(),
                metrics: Arc::clone(&self.metrics),
                state,
                diffs: all_live_diffs.0,
            });
        }
    }

    /// Fetches all live_diffs for a shard. Intended only for when a caller needs to reconstruct
    /// _all_ states still referenced by Consensus. Prefer [Self::fetch_recent_live_diffs] when
    /// the caller simply needs to fetch the latest state.
    ///
    /// Returns an empty Vec iff called on an uninitialized shard.
    pub async fn fetch_all_live_diffs(&self, shard_id: &ShardId) -> AllLiveDiffs {
        let path = shard_id.to_string();
        let diffs = retry_external(&self.metrics.retries.external.fetch_state_scan, || async {
            self.consensus.scan(&path, SeqNo::minimum(), SCAN_ALL).await
        })
        .instrument(debug_span!("fetch_state::scan"))
        .await;
        AllLiveDiffs(diffs)
    }

    /// Fetches recent live_diffs for a shard. Intended for when a caller needs to fetch
    /// the latest state in Consensus.
    ///
    /// "Recent" is defined as either:
    /// * All of the diffs known in Consensus
    /// * All of the diffs in Consensus after the latest rollup
    pub async fn fetch_recent_live_diffs<T>(&self, shard_id: &ShardId) -> RecentLiveDiffs
    where
        T: Timestamp + Lattice + Codec64,
    {
        let path = shard_id.to_string();
        let scan_limit = self.cfg.dynamic.state_versions_recent_live_diffs_limit();
        let oldest_diffs =
            retry_external(&self.metrics.retries.external.fetch_state_scan, || async {
                self.consensus
                    .scan(&path, SeqNo::minimum(), scan_limit)
                    .await
            })
            .instrument(debug_span!("fetch_state::scan"))
            .await;

        // fast-path: we found all known diffs in a single page of our scan. we expect almost all
        // calls to go down this path, unless a reader has a very long seqno-hold on the shard.
        if oldest_diffs.len() < scan_limit {
            self.metrics.state.fetch_recent_live_diffs_fast_path.inc();
            return RecentLiveDiffs(oldest_diffs);
        }

        // slow-path: we could be arbitrarily far behind the head of Consensus (either intentionally
        // due to a long seqno-hold from a reader, or unintentionally from a bug that's preventing
        // a seqno-hold from advancing). rather than scanning a potentially unbounded number of old
        // states in Consensus, we jump to the latest state, determine the seqno of the most recent
        // rollup, and then fetch all the diffs from that point onward.
        //
        // this approach requires more network calls, but it should smooth out our access pattern
        // and use only bounded calls to Consensus. additionally, if `limit` is adequately tuned,
        // this path will only be invoked when there's an excess number of states in Consensus and
        // it might be slower to do a single long scan over unneeded rows.
        let head = retry_external(&self.metrics.retries.external.fetch_state_scan, || async {
            self.consensus.head(&path).await
        })
        .instrument(debug_span!("fetch_state::slow_path::head"))
        .await
        .expect("initialized shard should have at least 1 diff");

        let latest_diff = self
            .metrics
            .codecs
            .state_diff
            .decode(|| StateDiff::<T>::decode(&self.cfg.build_version, &head.data));

        match BlobKey::parse_ids(&latest_diff.latest_rollup_key.complete(shard_id)) {
            Ok((_shard_id, PartialBlobKey::Rollup(seqno, _rollup))) => {
                self.metrics.state.fetch_recent_live_diffs_slow_path.inc();
                let diffs =
                    retry_external(&self.metrics.retries.external.fetch_state_scan, || async {
                        // (pedantry) this call is technically unbounded, but something very strange
                        // would have had to happen to have accumulated so many states between our
                        // call to `head` and this invocation for it to become problematic
                        self.consensus.scan(&path, seqno, SCAN_ALL).await
                    })
                    .instrument(debug_span!("fetch_state::slow_path::scan"))
                    .await;
                RecentLiveDiffs(diffs)
            }
            Ok(_) => panic!(
                "invalid state diff rollup key: {}",
                latest_diff.latest_rollup_key
            ),
            Err(err) => panic!("unparseable state diff rollup key: {}", err),
        }
    }

    /// Truncates any diffs in consensus less than the given seqno.
    pub async fn truncate_diffs(&self, shard_id: &ShardId, seqno: SeqNo) {
        let path = shard_id.to_string();
        let _deleted_count = retry_external(&self.metrics.retries.external.gc_truncate, || async {
            self.consensus.truncate(&path, seqno).await
        })
        .instrument(debug_span!("gc::truncate"))
        .await;
    }

    // Writes a self-referential rollup to blob storage and returns the diff
    // that should be compare_and_set into consensus to finish initializing the
    // shard.
    async fn write_initial_rollup<K, V, T, D>(
        &self,
        shard_metrics: &ShardMetrics,
    ) -> (TypedState<K, V, T, D>, StateDiff<T>)
    where
        K: Debug + Codec,
        V: Debug + Codec,
        T: Timestamp + Lattice + Codec64,
        D: Semigroup + Codec64,
    {
        let empty_state = TypedState::new(
            self.cfg.build_version.clone(),
            shard_metrics.shard_id,
            self.cfg.hostname.clone(),
            (self.cfg.now)(),
        );
        let rollup_seqno = empty_state.seqno.next();
        let rollup = HollowRollup {
            key: PartialRollupKey::new(rollup_seqno, &RollupId::new()),
            // Chicken-and-egg problem here. We don't know the size of the
            // rollup until we encode it, but it includes a reference back to
            // itself.
            encoded_size_bytes: None,
        };
        let (applied, initial_state) = match empty_state
            .clone_apply(&self.cfg, &mut |_, _, state| {
                state.add_and_remove_rollups((rollup_seqno, &rollup), &[])
            }) {
            Continue(x) => x,
            Break(NoOpStateTransition(_)) => {
                panic!("initial state transition should not be a no-op")
            }
        };
        assert!(
            applied,
            "add_and_remove_rollups should apply to the empty state"
        );

        let rollup = self.encode_rollup_blob(shard_metrics, &initial_state, rollup.key);
        let () = self.write_rollup_blob(&rollup).await;
        assert_eq!(initial_state.seqno, rollup.seqno);

        let diff = StateDiff::from_diff(&empty_state.state, &initial_state.state);
        (initial_state, diff)
    }

    /// Encodes the given state as a rollup to be written to the specified key.
    pub fn encode_rollup_blob<K, V, T, D>(
        &self,
        shard_metrics: &ShardMetrics,
        state: &TypedState<K, V, T, D>,
        key: PartialRollupKey,
    ) -> EncodedRollup
    where
        K: Debug + Codec,
        V: Debug + Codec,
        T: Timestamp + Lattice + Codec64,
        D: Semigroup + Codec64,
    {
        let buf = self.metrics.codecs.state.encode(|| {
            let mut buf = Vec::new();
            state.encode(&mut buf);
            Bytes::from(buf)
        });
        shard_metrics
            .latest_rollup_size
            .set(u64::cast_from(buf.len()));
        EncodedRollup {
            shard_id: state.shard_id,
            seqno: state.seqno,
            key,
            buf,
        }
    }

    /// Writes the given state rollup out to blob.
    pub async fn write_rollup_blob(&self, rollup: &EncodedRollup) {
        let payload_len = rollup.buf.len();
        retry_external(&self.metrics.retries.external.rollup_set, || async {
            self.blob
                .set(
                    &rollup.key.complete(&rollup.shard_id),
                    Bytes::clone(&rollup.buf),
                    Atomicity::RequireAtomic,
                )
                .await
        })
        .instrument(debug_span!("rollup::set", payload_len))
        .await;
    }

    /// Fetches a rollup for the given SeqNo, if it exists.
    ///
    /// Uses the provided hint, which is a possibly outdated copy of all
    /// or recent live diffs, to avoid fetches where possible.
    ///
    /// Panics if called on an uninitialized shard.
    async fn fetch_rollup_at_seqno<T>(
        &self,
        shard_id: &ShardId,
        live_diffs: Vec<VersionedData>,
        seqno: SeqNo,
    ) -> Option<UntypedState<T>>
    where
        T: Timestamp + Lattice + Codec64,
    {
        let rollup_key_for_migration = live_diffs.iter().find_map(|x| {
            let diff = self
                .metrics
                .codecs
                .state_diff
                .decode(|| StateDiff::<T>::decode(&self.cfg.build_version, &x.data));
            diff.rollups
                .iter()
                .find(|x| x.key == seqno)
                .map(|x| match &x.val {
                    StateFieldValDiff::Insert(x) => x.clone(),
                    StateFieldValDiff::Update(_, x) => x.clone(),
                    StateFieldValDiff::Delete(x) => x.clone(),
                })
        });

        let state = self.fetch_current_state::<T>(shard_id, live_diffs).await;
        if let Some(rollup) = state.rollups().get(&seqno) {
            return self.fetch_rollup_at_key(shard_id, &rollup.key).await;
        }

        // MIGRATION: We maintain an invariant that the _current state_ contains
        // a rollup for the _earliest live diff_ in consensus (and that the
        // referenced rollup exists). At one point, we fixed a bug that could
        // lead to that invariant being violated.
        //
        // If the earliest live diff is X and we receive a gc req for X+Y to
        // X+Y+Z (this can happen e.g. if some cmd ignores an earlier req for X
        // to X+Y, or if they're processing concurrently and the X to X+Y req
        // loses the race), then the buggy version of gc would delete any
        // rollups strictly less than old_seqno_since (X+Y in this example). But
        // our invariant is that the rollup exists for the earliest live diff,
        // in this case X. So if the first call to gc was interrupted after this
        // but before truncate (when all the blob deletes happen), later calls
        // to gc would attempt to call `fetch_live_states` and end up infinitely
        // in its loop.
        //
        // The fix was to base which rollups are deleteable on the earliest live
        // diff, not old_seqno_since.
        //
        // Sadly, some envs in prod now violate this invariant. So, even with
        // the fix, existing shards will never successfully run gc. We add a
        // temporary migration to fix them in `fetch_rollup_at_seqno`. This
        // method normally looks in the latest version of state for the
        // specifically requested seqno. In the invariant violation case, some
        // version of state in the range `[earliest, current]` has a rollup for
        // earliest, but current doesn't. So, for the migration, if
        // fetch_rollup_at_seqno doesn't find a rollup in current, then we fall
        // back to sniffing one out of raw diffs. If this success, we increment
        // a counter and log, so we can track how often this migration is
        // bailing us out. After the next deploy, this should initially start at
        // > 0 and then settle down to 0. After the next prod envs wipe, we can
        // remove the migration.
        let rollup = rollup_key_for_migration.expect("someone should have a key for this rollup");
        tracing::info!("only found rollup for {} {} via migration", shard_id, seqno);
        self.metrics.state.rollup_at_seqno_migration.inc();
        self.fetch_rollup_at_key(shard_id, &rollup.key).await
    }

    /// Fetches the rollup at the given key, if it exists.
    async fn fetch_rollup_at_key<T>(
        &self,
        shard_id: &ShardId,
        rollup_key: &PartialRollupKey,
    ) -> Option<UntypedState<T>>
    where
        T: Timestamp + Lattice + Codec64,
    {
        retry_external(&self.metrics.retries.external.rollup_get, || async {
            self.blob.get(&rollup_key.complete(shard_id)).await
        })
        .instrument(debug_span!("rollup::get"))
        .await
        .map(|buf| {
            self.metrics
                .codecs
                .state
                .decode(|| UntypedState::decode(&self.cfg.build_version, &buf))
        })
    }

    /// Deletes the rollup at the given key, if it exists.
    pub async fn delete_rollup(&self, shard_id: &ShardId, key: &PartialRollupKey) {
        let _ = retry_external(&self.metrics.retries.external.rollup_delete, || async {
            self.blob.delete(&key.complete(shard_id)).await
        })
        .await
        .instrument(debug_span!("rollup::delete"));
    }
}

pub struct UntypedStateVersionsIter<T> {
    shard_id: ShardId,
    cfg: PersistConfig,
    metrics: Arc<Metrics>,
    state: UntypedState<T>,
    diffs: Vec<VersionedData>,
}

impl<T: Timestamp + Lattice + Codec64> UntypedStateVersionsIter<T> {
    pub fn check_codecs<K: Codec, V: Codec, D: Codec64>(
        self,
    ) -> Result<TypedStateVersionsIter<K, V, T, D>, Box<CodecMismatch>> {
        let state = self.state.check_codecs(&self.shard_id)?;
        Ok(TypedStateVersionsIter::new(
            self.cfg,
            self.metrics,
            state,
            self.diffs,
        ))
    }

    pub(crate) fn check_ts_codec(self) -> Result<StateVersionsIter<T>, CodecMismatchT> {
        let key_codec = self.state.key_codec.clone();
        let val_codec = self.state.val_codec.clone();
        let diff_codec = self.state.diff_codec.clone();
        let state = self.state.check_ts_codec(&self.shard_id)?;
        Ok(StateVersionsIter::new(
            self.cfg,
            self.metrics,
            state,
            self.diffs,
            key_codec,
            val_codec,
            diff_codec,
        ))
    }
}

/// An iterator over consecutive versions of [State].
pub struct StateVersionsIter<T> {
    cfg: PersistConfig,
    metrics: Arc<Metrics>,
    state: State<T>,
    diffs: Vec<VersionedData>,
    key_codec: String,
    val_codec: String,
    diff_codec: String,
    #[cfg(debug_assertions)]
    validator: ReferencedBlobValidator<T>,
}

impl<T: Timestamp + Lattice + Codec64> StateVersionsIter<T> {
    fn new(
        cfg: PersistConfig,
        metrics: Arc<Metrics>,
        state: State<T>,
        // diffs is stored reversed so we can efficiently pop off the Vec.
        mut diffs: Vec<VersionedData>,
        key_codec: String,
        val_codec: String,
        diff_codec: String,
    ) -> Self {
        assert!(diffs.first().map_or(true, |x| x.seqno == state.seqno));
        diffs.reverse();
        StateVersionsIter {
            cfg,
            metrics,
            state,
            diffs,
            key_codec,
            val_codec,
            diff_codec,
            #[cfg(debug_assertions)]
            validator: ReferencedBlobValidator::default(),
        }
    }

    /// Advances first to some starting state (in practice, usually the first
    /// live state), and then through each successive state, for as many diffs
    /// as this iterator was initialized with.
    ///
    /// The `inspect_diff_fn` callback can be used to inspect diffs directly as
    /// they are applied. The first call to `next` returns a
    /// [InspectDiff::FromInitial] representing a diff from the initial state.
    pub fn next<F: for<'a> FnMut(InspectDiff<'a, T>)>(
        &mut self,
        mut inspect_diff_fn: F,
    ) -> Option<&State<T>> {
        let diff = match self.diffs.pop() {
            Some(x) => x,
            None => return None,
        };
        let diff = self
            .metrics
            .codecs
            .state_diff
            .decode(|| StateDiff::decode(&self.cfg.build_version, &diff.data));

        // A bit hacky, but the first diff in StateVersionsIter is always a
        // no-op.
        if diff.seqno_to == self.state.seqno {
            let inspect = InspectDiff::FromInitial(&self.state);
            #[cfg(debug_assertions)]
            {
                inspect.referenced_blob_fn(|x| self.validator.add_inc_blob(x));
            }
            inspect_diff_fn(inspect);
        } else {
            let inspect = InspectDiff::Diff(&diff);
            #[cfg(debug_assertions)]
            {
                inspect.referenced_blob_fn(|x| self.validator.add_inc_blob(x));
            }
            inspect_diff_fn(inspect);
        }

        let diff_seqno_to = diff.seqno_to;
        self.state.apply_diffs(&self.metrics, std::iter::once(diff));
        assert_eq!(self.state.seqno, diff_seqno_to);
        #[cfg(debug_assertions)]
        {
            self.validator.validate_against_state(&self.state);
        }
        Some(&self.state)
    }

    pub fn state(&self) -> &State<T> {
        &self.state
    }

    pub fn into_proto(&self) -> impl serde::Serialize {
        self.state.into_proto(
            self.key_codec.clone(),
            self.val_codec.clone(),
            self.diff_codec.clone(),
        )
    }
}

/// An iterator over consecutive versions of [TypedState].
pub struct TypedStateVersionsIter<K, V, T, D> {
    cfg: PersistConfig,
    metrics: Arc<Metrics>,
    state: TypedState<K, V, T, D>,
    diffs: Vec<VersionedData>,
}

impl<K, V, T: Timestamp + Lattice + Codec64, D> TypedStateVersionsIter<K, V, T, D> {
    fn new(
        cfg: PersistConfig,
        metrics: Arc<Metrics>,
        state: TypedState<K, V, T, D>,
        // diffs is stored reversed so we can efficiently pop off the Vec.
        mut diffs: Vec<VersionedData>,
    ) -> Self {
        assert!(diffs.first().map_or(true, |x| x.seqno == state.seqno));
        diffs.reverse();
        TypedStateVersionsIter {
            cfg,
            metrics,
            state,
            diffs,
        }
    }

    pub fn len(&self) -> usize {
        self.diffs.len()
    }

    /// Returns the SeqNo of the next state returned by `next`.
    pub fn peek_seqno(&self) -> Option<SeqNo> {
        self.diffs.last().map(|x| x.seqno)
    }

    pub fn next(&mut self) -> Option<&TypedState<K, V, T, D>> {
        let diff = match self.diffs.pop() {
            Some(x) => x,
            None => return None,
        };
        self.state
            .apply_encoded_diffs(&self.cfg, &self.metrics, std::iter::once(&diff));
        assert_eq!(self.state.seqno, diff.seqno);
        Some(&self.state)
    }

    pub fn state(&self) -> &TypedState<K, V, T, D> {
        &self.state
    }
}

/// This represents a diff, either directly or, in the case of the FromInitial
/// variant, a diff from the initial state. (We could instead compute the diff
/// from the initial state and replace this with only a `StateDiff<T>`, but don't
/// for efficiency.)
#[derive(Debug)]
pub enum InspectDiff<'a, T> {
    FromInitial(&'a State<T>),
    Diff(&'a StateDiff<T>),
}

impl<T: Timestamp + Lattice + Codec64> InspectDiff<'_, T> {
    /// A callback invoked for each blob added this state transition.
    ///
    /// Blob removals, along with all other diffs, are ignored.
    pub fn referenced_blob_fn<F: for<'a> FnMut(HollowBlobRef<'a, T>)>(&self, f: F) {
        match self {
            InspectDiff::FromInitial(x) => x.map_blobs(f),
            InspectDiff::Diff(x) => x.map_blob_inserts(f),
        }
    }
}

#[cfg(debug_assertions)]
struct ReferencedBlobValidator<T> {
    // A copy of every batch and rollup referenced by some state iterator,
    // computed by scanning the full copy of state at each seqno.
    full_batches: BTreeSet<HollowBatch<T>>,
    full_rollups: BTreeSet<HollowRollup>,
    // A copy of every batch and rollup referenced by some state iterator,
    // computed incrementally.
    inc_batches: BTreeSet<HollowBatch<T>>,
    inc_rollups: BTreeSet<HollowRollup>,
}

#[cfg(debug_assertions)]
impl<T> Default for ReferencedBlobValidator<T> {
    fn default() -> Self {
        Self {
            full_batches: Default::default(),
            full_rollups: Default::default(),
            inc_batches: Default::default(),
            inc_rollups: Default::default(),
        }
    }
}

#[cfg(debug_assertions)]
impl<T: Timestamp + Lattice + Codec64> ReferencedBlobValidator<T> {
    fn add_inc_blob(&mut self, x: HollowBlobRef<'_, T>) {
        match x {
            HollowBlobRef::Batch(x) => assert!(self.inc_batches.insert(x.clone())),
            HollowBlobRef::Rollup(x) => assert!(self.inc_rollups.insert(x.clone())),
        }
    }
    fn validate_against_state(&mut self, x: &State<T>) {
        x.map_blobs(|x| match x {
            HollowBlobRef::Batch(x) => {
                self.full_batches.insert(x.clone());
            }
            HollowBlobRef::Rollup(x) => {
                self.full_rollups.insert(x.clone());
            }
        });
        assert_eq!(self.inc_batches, self.full_batches);
        assert_eq!(self.inc_rollups, self.full_rollups);
    }
}

#[cfg(test)]
mod tests {
    use crate::tests::new_test_client;

    use super::*;

    /// Regression test for (part of) #17752, where an interrupted
    /// `bin/environmentd --reset` resulted in panic in persist usage code.
    #[tokio::test]
    #[cfg_attr(miri, ignore)] // unsupported operation: can't call foreign function `epoll_wait` on OS `linux`
    async fn fetch_all_live_states_regression_uninitialized() {
        let client = new_test_client().await;
        let state_versions = StateVersions::new(
            client.cfg.clone(),
            Arc::clone(&client.consensus),
            Arc::clone(&client.blob),
            Arc::clone(&client.metrics),
        );
        assert!(state_versions
            .fetch_all_live_states::<u64>(ShardId::new())
            .await
            .is_none());
    }
}

// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! A durable, truncatable log of versions of [State].

use std::fmt::Debug;
use std::ops::ControlFlow::{Break, Continue};
use std::sync::Arc;
use std::time::SystemTime;

use bytes::Bytes;
use differential_dataflow::difference::Semigroup;
use differential_dataflow::lattice::Lattice;
use mz_persist::location::{Atomicity, Blob, Consensus, Indeterminate, SeqNo, VersionedData};
use mz_persist::retry::Retry;
use mz_persist_types::{Codec, Codec64};
use timely::progress::Timestamp;
use tracing::{debug, debug_span, trace, Instrument};

use crate::error::CodecMismatch;
use crate::internal::machine::{retry_determinate, retry_external};
use crate::internal::metrics::ShardMetrics;
use crate::internal::paths::{PartialRollupKey, RollupId};
use crate::internal::state::State;
use crate::internal::state_diff::StateDiff;
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
    ) -> Result<State<K, V, T, D>, CodecMismatch>
    where
        K: Debug + Codec,
        V: Debug + Codec,
        T: Timestamp + Lattice + Codec64,
        D: Semigroup + Codec64,
    {
        let shard_id = shard_metrics.shard_id;

        // The common case is that the shard is initialized, so try that first
        let live_diffs = self.fetch_live_diffs(&shard_id).await;
        if !live_diffs.is_empty() {
            return self.fetch_current_state(&shard_id, live_diffs).await;
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
            Ok(()) => {
                return Ok(initial_state);
            }
            Err(live_diffs) => {
                // We lost a CaS race and someone else initialized the shard,
                // use the value included in the CaS expectation error.

                // Clean up the rollup blob that we were trying to reference.
                let (_, rollup_key) = initial_state.latest_rollup();
                self.delete_rollup(&shard_id, rollup_key).await;

                return self.fetch_current_state(&shard_id, live_diffs).await;
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
        new_state: &State<K, V, T, D>,
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
        let new = self
            .metrics
            .codecs
            .state_diff
            .encode(|| VersionedData::from((new_state.seqno(), diff)));
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
                shard_metrics.set_upper(&new_state.upper());
                shard_metrics.set_batch_count(new_state.batch_count());
                shard_metrics.set_update_count(new_state.num_updates());
                shard_metrics.set_seqnos_held(new_state.seqnos_held());
                shard_metrics.inc_encoded_diff_size(payload_len);
                Ok(Ok(()))
            }
            Err(live_diffs) => {
                debug!(
                    "apply_unbatched_cmd {} {} lost the CaS race, retrying: {} vs {:?}",
                    new_state.shard_id(),
                    cmd_name,
                    new_state.seqno(),
                    live_diffs.last().map(|x| x.seqno)
                );
                Ok(Err(live_diffs))
            }
        }
    }

    /// Fetches the `current` state of the requested shard.
    ///
    /// Uses the provided hint (all_live_diffs), which is a possibly outdated
    /// copy of all live diffs, to avoid fetches where possible.
    ///
    /// Panics if called on an uninitialized shard.
    pub async fn fetch_current_state<K, V, T, D>(
        &self,
        shard_id: &ShardId,
        mut all_live_diffs: Vec<VersionedData>,
    ) -> Result<State<K, V, T, D>, CodecMismatch>
    where
        K: Debug + Codec,
        V: Debug + Codec,
        T: Timestamp + Lattice + Codec64,
        D: Semigroup + Codec64,
    {
        let retry = self
            .metrics
            .retries
            .fetch_latest_state
            .stream(Retry::persist_defaults(SystemTime::now()).into_retry_stream());
        loop {
            let latest_diff = all_live_diffs
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
                Some(x) => x?,
                None => {
                    // The rollup that this diff referenced is gone, so the diff
                    // must be out of date. Try again.
                    all_live_diffs = self.fetch_live_diffs(shard_id).await;
                    // Intentionally don't sleep on retry.
                    retry.retries.inc();
                    continue;
                }
            };

            let rollup_seqno = state.seqno;
            let diffs = all_live_diffs.iter().filter(|x| x.seqno > rollup_seqno);
            state.apply_encoded_diffs(&self.cfg, &self.metrics, diffs);
            return Ok(state);
        }
    }

    /// Updates the provided state to current.
    ///
    /// This method differs from [Self::fetch_current_state] in that it
    /// optimistically fetches only the diffs since state.seqno and only falls
    /// back to fetching all of them when necessary.
    pub async fn fetch_and_update_to_current<K, V, T, D>(
        &self,
        state: &mut State<K, V, T, D>,
    ) -> Result<(), CodecMismatch>
    where
        K: Debug + Codec,
        V: Debug + Codec,
        T: Timestamp + Lattice + Codec64,
        D: Semigroup + Codec64,
    {
        let path = state.shard_id.to_string();
        let diffs_to_current =
            retry_external(&self.metrics.retries.external.fetch_state_scan, || async {
                self.consensus.scan(&path, state.seqno.next()).await
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
            let all_live_diffs = self.fetch_live_diffs(&state.shard_id).await;
            *state = self
                .fetch_current_state(&state.shard_id, all_live_diffs)
                .await?;
            Ok(())
        }
    }

    /// Returns an iterator over all live states for the requested shard.
    ///
    /// Panics if called on an uninitialized shard.
    pub async fn fetch_live_states<K, V, T, D>(
        &self,
        shard_id: &ShardId,
    ) -> Result<StateVersionsIter<K, V, T, D>, CodecMismatch>
    where
        K: Debug + Codec,
        V: Debug + Codec,
        T: Timestamp + Lattice + Codec64,
        D: Semigroup + Codec64,
    {
        let retry = self
            .metrics
            .retries
            .fetch_live_states
            .stream(Retry::persist_defaults(SystemTime::now()).into_retry_stream());
        loop {
            let live_diffs = self.fetch_live_diffs(&shard_id).await;
            let earliest_live_diff = match live_diffs.first() {
                Some(x) => x,
                None => panic!("fetch_live_states should only be called on an initialized shard"),
            };
            let state = match self
                .fetch_rollup_at_seqno(shard_id, live_diffs.clone(), earliest_live_diff.seqno)
                .await
            {
                Some(x) => x?,
                None => {
                    // We maintain an invariant that a rollup always exists for
                    // the earliest live diff. Since we didn't find out, that
                    // can only mean that the live_diffs we just fetched are
                    // obsolete (there's a race condition with gc). This should
                    // be rare in practice, so inc a counter and try again.
                    // Intentionally don't sleep on retry.
                    retry.retries.inc();
                    continue;
                }
            };
            assert_eq!(earliest_live_diff.seqno, state.seqno);
            return Ok(StateVersionsIter::new(
                self.cfg.clone(),
                Arc::clone(&self.metrics),
                state,
                live_diffs,
            ));
        }
    }

    /// Fetches the live_diffs for a shard.
    ///
    /// Returns an empty Vec iff called on an uninitialized shard.
    pub async fn fetch_live_diffs(&self, shard_id: &ShardId) -> Vec<VersionedData> {
        let path = shard_id.to_string();
        retry_external(&self.metrics.retries.external.fetch_state_scan, || async {
            self.consensus.scan(&path, SeqNo::minimum()).await
        })
        .instrument(debug_span!("fetch_state::scan"))
        .await
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
    ) -> (State<K, V, T, D>, StateDiff<T>)
    where
        K: Debug + Codec,
        V: Debug + Codec,
        T: Timestamp + Lattice + Codec64,
        D: Semigroup + Codec64,
    {
        let empty_state = State::new(self.cfg.build_version.clone(), shard_metrics.shard_id);
        let rollup_seqno = empty_state.seqno.next();
        let rollup_key = PartialRollupKey::new(rollup_seqno, &RollupId::new());
        let (applied, initial_state) = match empty_state
            .clone_apply(&self.cfg.build_version, &mut |_, state| {
                state.add_and_remove_rollups((rollup_seqno, &rollup_key), &[])
            }) {
            Continue(x) => x,
            Break(x) => match x {},
        };
        assert!(
            applied,
            "add_and_remove_rollups should apply to the empty state"
        );

        let () = self
            .write_rollup_blob(shard_metrics, &initial_state, &rollup_key)
            .await;
        assert_eq!(initial_state.seqno, rollup_seqno);

        let diff = StateDiff::from_diff(&empty_state, &initial_state);
        (initial_state, diff)
    }

    /// Writes the given state as a rollup to the specified key.
    pub async fn write_rollup_blob<K, V, T, D>(
        &self,
        shard_metrics: &ShardMetrics,
        state: &State<K, V, T, D>,
        key: &PartialRollupKey,
    ) where
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
        let payload_len = buf.len();
        retry_external(&self.metrics.retries.external.rollup_set, || async {
            self.blob
                .set(
                    &key.complete(&state.shard_id),
                    Bytes::clone(&buf),
                    Atomicity::RequireAtomic,
                )
                .await
        })
        .instrument(debug_span!("rollup::set", payload_len))
        .await;
        shard_metrics.set_encoded_rollup_size(payload_len);
    }

    /// Fetches a rollup for the given SeqNo, if it exists.
    ///
    /// Uses the provided hint, which is a possibly outdated copy of all live
    /// diffs, to avoid fetches where possible.
    ///
    /// Panics if called on an uninitialized shard.
    async fn fetch_rollup_at_seqno<K, V, T, D>(
        &self,
        shard_id: &ShardId,
        all_live_diffs: Vec<VersionedData>,
        seqno: SeqNo,
    ) -> Option<Result<State<K, V, T, D>, CodecMismatch>>
    where
        K: Debug + Codec,
        V: Debug + Codec,
        T: Timestamp + Lattice + Codec64,
        D: Semigroup + Codec64,
    {
        let state = match self
            .fetch_current_state::<K, V, T, D>(shard_id, all_live_diffs)
            .await
        {
            Ok(x) => x,
            Err(err) => return Some(Err(err)),
        };
        let rollup_key = state.collections.rollups.get(&seqno)?;
        self.fetch_rollup_at_key(shard_id, rollup_key).await
    }

    /// Fetches the rollup at the given key, if it exists.
    async fn fetch_rollup_at_key<K, V, T, D>(
        &self,
        shard_id: &ShardId,
        rollup_key: &PartialRollupKey,
    ) -> Option<Result<State<K, V, T, D>, CodecMismatch>>
    where
        K: Debug + Codec,
        V: Debug + Codec,
        T: Timestamp + Lattice + Codec64,
        D: Semigroup + Codec64,
    {
        retry_external(&self.metrics.retries.external.rollup_get, || async {
            self.blob.get(&rollup_key.complete(&shard_id)).await
        })
        .instrument(debug_span!("rollup::get"))
        .await
        .map(|buf| {
            self.metrics
                .codecs
                .state
                .decode(|| State::decode(&self.cfg.build_version, &buf))
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

/// An iterator over consecutive versions of [State].
pub struct StateVersionsIter<K, V, T, D> {
    cfg: PersistConfig,
    metrics: Arc<Metrics>,
    state: State<K, V, T, D>,
    diffs: Vec<VersionedData>,
}

impl<K, V, T: Timestamp + Lattice + Codec64, D> StateVersionsIter<K, V, T, D> {
    fn new(
        cfg: PersistConfig,
        metrics: Arc<Metrics>,
        state: State<K, V, T, D>,
        // diffs is stored reversed so we can efficiently pop off the Vec.
        mut diffs: Vec<VersionedData>,
    ) -> Self {
        assert!(diffs.first().map_or(true, |x| x.seqno == state.seqno));
        diffs.reverse();
        StateVersionsIter {
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

    pub fn next(&mut self) -> Option<&State<K, V, T, D>> {
        let diff = match self.diffs.pop() {
            Some(x) => x,
            None => return None,
        };
        self.state
            .apply_encoded_diffs(&self.cfg, &self.metrics, std::iter::once(&diff));
        assert_eq!(self.state.seqno, diff.seqno);
        Some(&self.state)
    }

    pub fn into_inner(self) -> State<K, V, T, D> {
        self.state
    }
}

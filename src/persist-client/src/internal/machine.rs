// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Implementation of the persist state machine.

use std::fmt::Debug;
use std::ops::ControlFlow::{self, Continue};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime};

use differential_dataflow::difference::Semigroup;
use differential_dataflow::lattice::Lattice;
use futures::FutureExt;
use futures::future::{self, BoxFuture};
use mz_dyncfg::{Config, ConfigSet};
use mz_ore::assert_none;
use mz_ore::cast::CastFrom;
use mz_ore::error::ErrorExt;
#[allow(unused_imports)] // False positive.
use mz_ore::fmt::FormatBuffer;
use mz_ore::task::JoinHandle;
use mz_persist::location::{ExternalError, Indeterminate, SeqNo};
use mz_persist::retry::Retry;
use mz_persist_types::schema::SchemaId;
use mz_persist_types::{Codec, Codec64, Opaque};
use semver::Version;
use timely::PartialOrder;
use timely::progress::{Antichain, Timestamp};
use tracing::{Instrument, debug, info, trace_span, warn};

use crate::async_runtime::IsolatedRuntime;
use crate::batch::INLINE_WRITES_TOTAL_MAX_BYTES;
use crate::cache::StateCache;
use crate::cfg::RetryParameters;
use crate::critical::CriticalReaderId;
use crate::error::{CodecMismatch, InvalidUsage};
use crate::internal::apply::Applier;
use crate::internal::compact::CompactReq;
use crate::internal::gc::GarbageCollector;
use crate::internal::maintenance::{RoutineMaintenance, WriterMaintenance};
use crate::internal::metrics::{CmdMetrics, Metrics, MetricsRetryStream, RetryMetrics};
use crate::internal::paths::PartialRollupKey;
use crate::internal::state::{
    CompareAndAppendBreak, CriticalReaderState, HandleDebugState, HollowBatch, HollowRollup,
    IdempotencyToken, LeasedReaderState, NoOpStateTransition, Since, SnapshotErr, StateCollections,
    Upper,
};
use crate::internal::state_versions::StateVersions;
use crate::internal::trace::{ApplyMergeResult, FueledMergeRes};
use crate::internal::watch::StateWatch;
use crate::read::{LeasedReaderId, READER_LEASE_DURATION};
use crate::rpc::PubSubSender;
use crate::schema::CaESchema;
use crate::write::WriterId;
use crate::{Diagnostics, PersistConfig, ShardId};

#[derive(Debug)]
pub struct Machine<K, V, T, D> {
    pub(crate) applier: Applier<K, V, T, D>,
    pub(crate) isolated_runtime: Arc<IsolatedRuntime>,
}

// Impl Clone regardless of the type params.
impl<K, V, T: Clone, D> Clone for Machine<K, V, T, D> {
    fn clone(&self) -> Self {
        Self {
            applier: self.applier.clone(),
            isolated_runtime: Arc::clone(&self.isolated_runtime),
        }
    }
}

pub(crate) const CLAIM_UNCLAIMED_COMPACTIONS: Config<bool> = Config::new(
    "persist_claim_unclaimed_compactions",
    false,
    "If an append doesn't result in a compaction request, but there is some uncompacted batch \
    in state, compact that instead.",
);

pub(crate) const CLAIM_COMPACTION_PERCENT: Config<usize> = Config::new(
    "persist_claim_compaction_percent",
    100,
    "Claim a compaction with the given percent chance, if claiming compactions is enabled. \
    (If over 100, we'll always claim at least one; for example, if set to 365, we'll claim at least \
    three and have a 65% chance of claiming a fourth.)",
);

pub(crate) const CLAIM_COMPACTION_MIN_VERSION: Config<String> = Config::new(
    "persist_claim_compaction_min_version",
    String::new(),
    "If set to a valid version string, compact away any earlier versions if possible.",
);

impl<K, V, T, D> Machine<K, V, T, D>
where
    K: Debug + Codec,
    V: Debug + Codec,
    T: Timestamp + Lattice + Codec64 + Sync,
    D: Semigroup + Codec64,
{
    pub async fn new(
        cfg: PersistConfig,
        shard_id: ShardId,
        metrics: Arc<Metrics>,
        state_versions: Arc<StateVersions>,
        shared_states: Arc<StateCache>,
        pubsub_sender: Arc<dyn PubSubSender>,
        isolated_runtime: Arc<IsolatedRuntime>,
        diagnostics: Diagnostics,
    ) -> Result<Self, Box<CodecMismatch>> {
        let applier = Applier::new(
            cfg,
            shard_id,
            metrics,
            state_versions,
            shared_states,
            pubsub_sender,
            diagnostics,
        )
        .await?;
        Ok(Machine {
            applier,
            isolated_runtime,
        })
    }

    pub fn shard_id(&self) -> ShardId {
        self.applier.shard_id
    }

    pub fn seqno(&self) -> SeqNo {
        self.applier.seqno()
    }

    pub async fn add_rollup_for_current_seqno(&self) -> RoutineMaintenance {
        let rollup = self.applier.write_rollup_for_state().await;
        let Some(rollup) = rollup else {
            return RoutineMaintenance::default();
        };

        let (applied, maintenance) = self.add_rollup((rollup.seqno, &rollup.to_hollow())).await;
        if !applied {
            // Someone else already wrote a rollup at this seqno, so ours didn't
            // get added. Delete it.
            self.applier
                .state_versions
                .delete_rollup(&rollup.shard_id, &rollup.key)
                .await;
        }
        maintenance
    }

    pub async fn add_rollup(
        &self,
        add_rollup: (SeqNo, &HollowRollup),
    ) -> (bool, RoutineMaintenance) {
        // See the big SUBTLE comment in [Self::merge_res] for what's going on
        // here.
        let mut applied_ever_true = false;
        let metrics = Arc::clone(&self.applier.metrics);
        let (_seqno, _applied, maintenance) = self
            .apply_unbatched_idempotent_cmd(&metrics.cmds.add_rollup, |_, _, state| {
                let ret = state.add_rollup(add_rollup);
                if let Continue(applied) = ret {
                    applied_ever_true = applied_ever_true || applied;
                }
                ret
            })
            .await;
        (applied_ever_true, maintenance)
    }

    pub async fn remove_rollups(
        &self,
        remove_rollups: &[(SeqNo, PartialRollupKey)],
    ) -> (Vec<SeqNo>, RoutineMaintenance) {
        let metrics = Arc::clone(&self.applier.metrics);
        let (_seqno, removed_rollup_seqnos, maintenance) = self
            .apply_unbatched_idempotent_cmd(&metrics.cmds.remove_rollups, |_, _, state| {
                state.remove_rollups(remove_rollups)
            })
            .await;
        (removed_rollup_seqnos, maintenance)
    }

    pub async fn register_leased_reader(
        &self,
        reader_id: &LeasedReaderId,
        purpose: &str,
        lease_duration: Duration,
        heartbeat_timestamp_ms: u64,
        use_critical_since: bool,
    ) -> (LeasedReaderState<T>, RoutineMaintenance) {
        let metrics = Arc::clone(&self.applier.metrics);
        let (_seqno, (reader_state, seqno_since), maintenance) = self
            .apply_unbatched_idempotent_cmd(&metrics.cmds.register, |seqno, cfg, state| {
                state.register_leased_reader(
                    &cfg.hostname,
                    reader_id,
                    purpose,
                    seqno,
                    lease_duration,
                    heartbeat_timestamp_ms,
                    use_critical_since,
                )
            })
            .await;
        // Usually, the reader gets an initial seqno hold of the seqno at which
        // it was registered. However, on a tombstone shard the seqno hold
        // happens to get computed as the tombstone seqno + 1
        // (State::clone_apply provided seqno.next(), the non-no-op commit
        // seqno, to the work fn and this is what register_reader uses for the
        // seqno hold). The real invariant we want to protect here is that the
        // hold is >= the seqno_since, so validate that instead of anything more
        // specific.
        debug_assert!(
            reader_state.seqno >= seqno_since,
            "{} vs {}",
            reader_state.seqno,
            seqno_since,
        );
        (reader_state, maintenance)
    }

    pub async fn register_critical_reader<O: Opaque + Codec64>(
        &self,
        reader_id: &CriticalReaderId,
        purpose: &str,
    ) -> (CriticalReaderState<T>, RoutineMaintenance) {
        let metrics = Arc::clone(&self.applier.metrics);
        let (_seqno, state, maintenance) = self
            .apply_unbatched_idempotent_cmd(&metrics.cmds.register, |_seqno, cfg, state| {
                state.register_critical_reader::<O>(&cfg.hostname, reader_id, purpose)
            })
            .await;
        (state, maintenance)
    }

    pub async fn register_schema(
        &self,
        key_schema: &K::Schema,
        val_schema: &V::Schema,
    ) -> (Option<SchemaId>, RoutineMaintenance) {
        let metrics = Arc::clone(&self.applier.metrics);
        let (_seqno, state, maintenance) = self
            .apply_unbatched_idempotent_cmd(&metrics.cmds.register, |_seqno, _cfg, state| {
                state.register_schema::<K, V>(key_schema, val_schema)
            })
            .await;
        (state, maintenance)
    }

    pub async fn spine_exert(&self, fuel: usize) -> (Vec<CompactReq<T>>, RoutineMaintenance) {
        // Performance special case for no-ops, to avoid the State clones.
        if fuel == 0 || self.applier.all_batches().len() < 2 {
            return (Vec::new(), RoutineMaintenance::default());
        }

        let metrics = Arc::clone(&self.applier.metrics);
        let (_seqno, reqs, maintenance) = self
            .apply_unbatched_idempotent_cmd(&metrics.cmds.spine_exert, |_seqno, _cfg, state| {
                state.spine_exert(fuel)
            })
            .await;
        let reqs = reqs
            .into_iter()
            .map(|req| CompactReq {
                shard_id: self.shard_id(),
                desc: req.desc,
                inputs: req
                    .inputs
                    .into_iter()
                    .map(|b| Arc::unwrap_or_clone(b.batch))
                    .collect(),
            })
            .collect();
        (reqs, maintenance)
    }

    pub async fn compare_and_append(
        &self,
        batch: &HollowBatch<T>,
        writer_id: &WriterId,
        debug_info: &HandleDebugState,
        heartbeat_timestamp_ms: u64,
    ) -> CompareAndAppendRes<T> {
        let idempotency_token = IdempotencyToken::new();
        loop {
            let res = self
                .compare_and_append_idempotent(
                    batch,
                    writer_id,
                    heartbeat_timestamp_ms,
                    &idempotency_token,
                    debug_info,
                    None,
                )
                .await;
            match res {
                CompareAndAppendRes::Success(seqno, maintenance) => {
                    return CompareAndAppendRes::Success(seqno, maintenance);
                }
                CompareAndAppendRes::InvalidUsage(x) => {
                    return CompareAndAppendRes::InvalidUsage(x);
                }
                CompareAndAppendRes::InlineBackpressure => {
                    return CompareAndAppendRes::InlineBackpressure;
                }
                CompareAndAppendRes::UpperMismatch(seqno, _current_upper) => {
                    // If the state machine thinks that the shard upper is not
                    // far enough along, it could be because the caller of this
                    // method has found out that it advanced via some some
                    // side-channel that didn't update our local cache of the
                    // machine state. So, fetch the latest state and try again
                    // if we indeed get something different.
                    self.applier.fetch_and_update_state(Some(seqno)).await;
                    let (current_seqno, current_upper) =
                        self.applier.upper(|seqno, upper| (seqno, upper.clone()));

                    // We tried to to a compare_and_append with the wrong
                    // expected upper, that won't work.
                    if &current_upper != batch.desc.lower() {
                        return CompareAndAppendRes::UpperMismatch(current_seqno, current_upper);
                    } else {
                        // The upper stored in state was outdated. Retry after
                        // updating.
                    }
                }
            }
        }
    }

    async fn compare_and_append_idempotent(
        &self,
        batch: &HollowBatch<T>,
        writer_id: &WriterId,
        heartbeat_timestamp_ms: u64,
        idempotency_token: &IdempotencyToken,
        debug_info: &HandleDebugState,
        // Only exposed for testing. In prod, this always starts as None, but
        // making it a parameter allows us to simulate hitting an indeterminate
        // error on the first attempt in tests.
        mut indeterminate: Option<Indeterminate>,
    ) -> CompareAndAppendRes<T> {
        let metrics = Arc::clone(&self.applier.metrics);
        let lease_duration_ms = self
            .applier
            .cfg
            .writer_lease_duration
            .as_millis()
            .try_into()
            .expect("reasonable duration");
        // SUBTLE: Retries of compare_and_append with Indeterminate errors are
        // tricky (more discussion of this in database-issues#3680):
        //
        // - (1) We compare_and_append and get an Indeterminate error back from
        //   CRDB/Consensus. This means we don't know if it committed or not.
        // - (2) We retry it.
        // - (3) We get back an upper mismatch. The tricky bit is deciding if we
        //   conflicted with some other writer OR if the write in (1) actually
        //   went through and we're "conflicting" with ourself.
        //
        // A number of scenarios can be distinguished with per-writer
        // idempotency tokens, so I'll jump straight to the hardest one:
        //
        // - (1) A compare_and_append is issued for e.g. `[5,7)`, the consensus
        //   call makes it onto the network before the operation is cancelled
        //   (by dropping the future).
        // - (2) A compare_and_append is issued from the same WriteHandle for
        //   `[3,5)`, it uses a different conn from the consensus pool and gets
        //   an Indeterminate error.
        // - (3) The call in (1) is received by consensus and commits.
        // - (4) The retry of (2) receives an upper mismatch with an upper of 7.
        //
        // At this point, how do we determine whether (2) committed or not and
        // thus whether we should return success or upper mismatch? Getting this
        // right is very important for correctness (imagine this is a table
        // write and we either return success or failure to the client).
        //
        // - If we use per-writer IdempotencyTokens but only store the latest
        //   one in state, then the `[5,7)` one will have clobbered whatever our
        //   `[3,5)` one was.
        // - We could store every IdempotencyToken that ever committed, but that
        //   would require unbounded storage in state (non-starter).
        // - We could require that IdempotencyTokens are comparable and that
        //   each call issued by a WriteHandle uses one that is strictly greater
        //   than every call before it. A previous version of this PR tried this
        //   and it's remarkably subtle. As a result, I (Dan) have developed
        //   strong feels that our correctness protocol _should not depend on
        //   WriteHandle, only Machine_.
        // - We could require a new WriterId if a request is ever cancelled by
        //   making `compare_and_append` take ownership of `self` and then
        //   handing it back for any call polled to completion. The ergonomics
        //   of this are quite awkward and, like the previous idea, it depends
        //   on the WriteHandle impl for correctness.
        // - Any ideas that involve reading back the data are foiled by a step
        //   `(0) set the since to 100` (plus the latency and memory usage would
        //   be too unpredictable).
        //
        // The technique used here derives from the following observations:
        //
        // - In practice, we don't use compare_and_append with the sort of
        //   "regressing frontiers" described above.
        // - In practice, Indeterminate errors are rare-ish. They happen enough
        //   that we don't want to always panic on them, but this is still a
        //   useful property to build on.
        //
        // At a high level, we do just enough to be able to distinguish the
        // cases that we think will happen in practice and then leave the rest
        // for a panic! that we think we'll never see. Concretely:
        //
        // - Run compare_and_append in a loop, retrying on Indeterminate errors
        //   but noting if we've ever done that.
        // - If we haven't seen an Indeterminate error (i.e. this is the first
        //   time though the loop) then the result we got is guaranteed to be
        //   correct, so pass it up.
        // - Otherwise, any result other than an expected upper mismatch is
        //   guaranteed to be correct, so just pass it up.
        // - Otherwise examine the writer's most recent upper and break it into
        //   two cases:
        // - Case 1 `expected_upper.less_than(writer_most_recent_upper)`: it's
        //   impossible that we committed on a previous iteration because the
        //   overall upper of the shard is less_than what this call would have
        //   advanced it to. Pass up the expectation mismatch.
        // - Case 2 `!Case1`: First note that this means our IdempotencyToken
        //   didn't match, otherwise we would have gotten `AlreadyCommitted`. It
        //   also means some previous write from _this writer_ has committed an
        //   upper that is beyond the one in this call, which is a weird usage
        //   (NB can't be a future write because that would mean someone is
        //   still polling us, but `&mut self` prevents that).
        //
        // TODO: If this technique works in practice (leads to zero panics),
        // then commit to it and remove the Indeterminate from
        // [WriteHandle::compare_and_append_batch].
        let mut retry = self
            .applier
            .metrics
            .retries
            .compare_and_append_idempotent
            .stream(Retry::persist_defaults(SystemTime::now()).into_retry_stream());
        let mut writer_was_present = false;
        loop {
            let cmd_res = self
                .applier
                .apply_unbatched_cmd(&metrics.cmds.compare_and_append, |_, cfg, state| {
                    writer_was_present = state.writers.contains_key(writer_id);
                    state.compare_and_append(
                        batch,
                        writer_id,
                        heartbeat_timestamp_ms,
                        lease_duration_ms,
                        idempotency_token,
                        debug_info,
                        INLINE_WRITES_TOTAL_MAX_BYTES.get(cfg),
                        if CLAIM_UNCLAIMED_COMPACTIONS.get(cfg) {
                            CLAIM_COMPACTION_PERCENT.get(cfg)
                        } else {
                            0
                        },
                        Version::parse(&CLAIM_COMPACTION_MIN_VERSION.get(cfg))
                            .ok()
                            .as_ref(),
                    )
                })
                .await;
            let (seqno, res, routine) = match cmd_res {
                Ok(x) => x,
                Err(err) => {
                    // These are rare and interesting enough that we always log
                    // them at info!.
                    info!(
                        "compare_and_append received an indeterminate error, retrying in {:?}: {}",
                        retry.next_sleep(),
                        err
                    );
                    if indeterminate.is_none() {
                        indeterminate = Some(err);
                    }
                    retry = retry.sleep().await;
                    continue;
                }
            };
            match res {
                Ok(merge_reqs) => {
                    // We got explicit confirmation that we succeeded, so
                    // anything that happened in a previous retry is irrelevant.
                    let mut compact_reqs = Vec::with_capacity(merge_reqs.len());
                    for req in merge_reqs {
                        let req = CompactReq {
                            shard_id: self.shard_id(),
                            desc: req.desc,
                            inputs: req
                                .inputs
                                .into_iter()
                                .map(|b| Arc::unwrap_or_clone(b.batch))
                                .collect(),
                        };
                        compact_reqs.push(req);
                    }
                    let writer_maintenance = WriterMaintenance {
                        routine,
                        compaction: compact_reqs,
                    };

                    if !writer_was_present {
                        metrics.state.writer_added.inc();
                    }
                    for part in &batch.parts {
                        if part.is_inline() {
                            let bytes = u64::cast_from(part.inline_bytes());
                            metrics.inline.part_commit_bytes.inc_by(bytes);
                            metrics.inline.part_commit_count.inc();
                        }
                    }
                    return CompareAndAppendRes::Success(seqno, writer_maintenance);
                }
                Err(CompareAndAppendBreak::AlreadyCommitted) => {
                    // A previous iteration through this loop got an
                    // Indeterminate error but was successful. Sanity check this
                    // and pass along the good news.
                    assert!(indeterminate.is_some());
                    self.applier.metrics.cmds.compare_and_append_noop.inc();
                    if !writer_was_present {
                        metrics.state.writer_added.inc();
                    }
                    return CompareAndAppendRes::Success(seqno, WriterMaintenance::default());
                }
                Err(CompareAndAppendBreak::InvalidUsage(err)) => {
                    // InvalidUsage is (or should be) a deterministic function
                    // of the inputs and independent of anything in persist
                    // state. It's handed back via a Break, so we never even try
                    // to commit it. No network, no Indeterminate.
                    assert_none!(indeterminate);
                    return CompareAndAppendRes::InvalidUsage(err);
                }
                Err(CompareAndAppendBreak::InlineBackpressure) => {
                    // We tried to write an inline part, but there was already
                    // too much in state. Flush it out to s3 and try again.
                    return CompareAndAppendRes::InlineBackpressure;
                }
                Err(CompareAndAppendBreak::Upper {
                    shard_upper,
                    writer_upper,
                }) => {
                    // NB the below intentionally compares to writer_upper
                    // (because it gives a tighter bound on the bad case), but
                    // returns shard_upper (what the persist caller cares
                    // about).
                    assert!(
                        PartialOrder::less_equal(&writer_upper, &shard_upper),
                        "{:?} vs {:?}",
                        &writer_upper,
                        &shard_upper
                    );
                    if PartialOrder::less_than(&writer_upper, batch.desc.upper()) {
                        // No way this could have committed in some previous
                        // attempt of this loop: the upper of the writer is
                        // strictly less than the proposed new upper.
                        return CompareAndAppendRes::UpperMismatch(seqno, shard_upper);
                    }
                    if indeterminate.is_none() {
                        // No way this could have committed in some previous
                        // attempt of this loop: we never saw an indeterminate
                        // error (thus there was no previous iteration of the
                        // loop).
                        return CompareAndAppendRes::UpperMismatch(seqno, shard_upper);
                    }
                    // This is the bad case. We can't distinguish if some
                    // previous attempt that got an Indeterminate error
                    // succeeded or not. This should be sufficiently rare in
                    // practice (hopefully ~never) that we give up and let
                    // process restart fix things. See the big comment above for
                    // more context.
                    //
                    // NB: This is intentionally not a halt! because it's quite
                    // unexpected.
                    panic!(
                        concat!(
                            "cannot distinguish compare_and_append success or failure ",
                            "caa_lower={:?} caa_upper={:?} writer_upper={:?} shard_upper={:?} err={:?}"
                        ),
                        batch.desc.lower().elements(),
                        batch.desc.upper().elements(),
                        writer_upper.elements(),
                        shard_upper.elements(),
                        indeterminate,
                    );
                }
            };
        }
    }

    pub async fn downgrade_since(
        &self,
        reader_id: &LeasedReaderId,
        outstanding_seqno: Option<SeqNo>,
        new_since: &Antichain<T>,
        heartbeat_timestamp_ms: u64,
    ) -> (SeqNo, Since<T>, RoutineMaintenance) {
        let metrics = Arc::clone(&self.applier.metrics);
        self.apply_unbatched_idempotent_cmd(&metrics.cmds.downgrade_since, |seqno, _cfg, state| {
            state.downgrade_since(
                reader_id,
                seqno,
                outstanding_seqno,
                new_since,
                heartbeat_timestamp_ms,
            )
        })
        .await
    }

    pub async fn compare_and_downgrade_since<O: Opaque + Codec64>(
        &self,
        reader_id: &CriticalReaderId,
        expected_opaque: &O,
        (new_opaque, new_since): (&O, &Antichain<T>),
    ) -> (Result<Since<T>, (O, Since<T>)>, RoutineMaintenance) {
        let metrics = Arc::clone(&self.applier.metrics);
        let (_seqno, res, maintenance) = self
            .apply_unbatched_idempotent_cmd(
                &metrics.cmds.compare_and_downgrade_since,
                |_seqno, _cfg, state| {
                    state.compare_and_downgrade_since::<O>(
                        reader_id,
                        expected_opaque,
                        (new_opaque, new_since),
                    )
                },
            )
            .await;

        match res {
            Ok(since) => (Ok(since), maintenance),
            Err((opaque, since)) => (Err((opaque, since)), maintenance),
        }
    }

    pub async fn heartbeat_leased_reader(
        &self,
        reader_id: &LeasedReaderId,
        heartbeat_timestamp_ms: u64,
    ) -> (SeqNo, bool, RoutineMaintenance) {
        let metrics = Arc::clone(&self.applier.metrics);
        let (seqno, existed, maintenance) = self
            .apply_unbatched_idempotent_cmd(&metrics.cmds.heartbeat_reader, |_, _, state| {
                state.heartbeat_leased_reader(reader_id, heartbeat_timestamp_ms)
            })
            .await;
        (seqno, existed, maintenance)
    }

    pub async fn expire_leased_reader(
        &self,
        reader_id: &LeasedReaderId,
    ) -> (SeqNo, RoutineMaintenance) {
        let metrics = Arc::clone(&self.applier.metrics);
        let (seqno, _existed, maintenance) = self
            .apply_unbatched_idempotent_cmd(&metrics.cmds.expire_reader, |_, _, state| {
                state.expire_leased_reader(reader_id)
            })
            .await;
        (seqno, maintenance)
    }

    #[allow(dead_code)] // TODO(bkirwi): remove this when since behaviour on expiry has settled
    pub async fn expire_critical_reader(
        &self,
        reader_id: &CriticalReaderId,
    ) -> (SeqNo, RoutineMaintenance) {
        let metrics = Arc::clone(&self.applier.metrics);
        let (seqno, _existed, maintenance) = self
            .apply_unbatched_idempotent_cmd(&metrics.cmds.expire_reader, |_, _, state| {
                state.expire_critical_reader(reader_id)
            })
            .await;
        (seqno, maintenance)
    }

    pub async fn expire_writer(&self, writer_id: &WriterId) -> (SeqNo, RoutineMaintenance) {
        let metrics = Arc::clone(&self.applier.metrics);
        let (seqno, _existed, maintenance) = self
            .apply_unbatched_idempotent_cmd(&metrics.cmds.expire_writer, |_, _, state| {
                state.expire_writer(writer_id)
            })
            .await;
        metrics.state.writer_removed.inc();
        (seqno, maintenance)
    }

    pub fn is_finalized(&self) -> bool {
        self.applier.is_finalized()
    }

    /// See [crate::PersistClient::get_schema].
    pub fn get_schema(&self, schema_id: SchemaId) -> Option<(K::Schema, V::Schema)> {
        self.applier.get_schema(schema_id)
    }

    /// See [crate::PersistClient::latest_schema].
    pub fn latest_schema(&self) -> Option<(SchemaId, K::Schema, V::Schema)> {
        self.applier.latest_schema()
    }

    /// See [crate::PersistClient::compare_and_evolve_schema].
    ///
    /// TODO: Unify this with [Self::register_schema]?
    pub async fn compare_and_evolve_schema(
        &self,
        expected: SchemaId,
        key_schema: &K::Schema,
        val_schema: &V::Schema,
    ) -> (CaESchema<K, V>, RoutineMaintenance) {
        let metrics = Arc::clone(&self.applier.metrics);
        let (_seqno, state, maintenance) = self
            .apply_unbatched_idempotent_cmd(
                &metrics.cmds.compare_and_evolve_schema,
                |_seqno, _cfg, state| {
                    state.compare_and_evolve_schema::<K, V>(expected, key_schema, val_schema)
                },
            )
            .await;
        (state, maintenance)
    }

    async fn tombstone_step(&self) -> Result<(bool, RoutineMaintenance), InvalidUsage<T>> {
        let metrics = Arc::clone(&self.applier.metrics);
        let mut retry = self
            .applier
            .metrics
            .retries
            .idempotent_cmd
            .stream(Retry::persist_defaults(SystemTime::now()).into_retry_stream());
        loop {
            let res = self
                .applier
                .apply_unbatched_cmd(&metrics.cmds.become_tombstone, |_, _, state| {
                    state.become_tombstone_and_shrink()
                })
                .await;
            let err = match res {
                Ok((_seqno, Ok(()), maintenance)) => return Ok((true, maintenance)),
                Ok((_seqno, Err(NoOpStateTransition(())), maintenance)) => {
                    return Ok((false, maintenance));
                }
                Err(err) => err,
            };
            if retry.attempt() >= INFO_MIN_ATTEMPTS {
                info!(
                    "become_tombstone received an indeterminate error, retrying in {:?}: {}",
                    retry.next_sleep(),
                    err
                );
            } else {
                debug!(
                    "become_tombstone received an indeterminate error, retrying in {:?}: {}",
                    retry.next_sleep(),
                    err
                );
            }
            retry = retry.sleep().await;
        }
    }

    pub async fn become_tombstone(&self) -> Result<RoutineMaintenance, InvalidUsage<T>> {
        self.applier.check_since_upper_both_empty()?;

        let mut maintenance = RoutineMaintenance::default();

        loop {
            let (made_progress, more_maintenance) = self.tombstone_step().await?;
            maintenance.merge(more_maintenance);
            if !made_progress {
                break;
            }
        }

        Ok(maintenance)
    }

    pub async fn snapshot(&self, as_of: &Antichain<T>) -> Result<Vec<HollowBatch<T>>, Since<T>> {
        let start = Instant::now();
        let (mut seqno, mut upper) = match self.applier.snapshot(as_of) {
            Ok(x) => return Ok(x),
            Err(SnapshotErr::AsOfNotYetAvailable(seqno, Upper(upper))) => (seqno, upper),
            Err(SnapshotErr::AsOfHistoricalDistinctionsLost(Since(since))) => {
                return Err(Since(since));
            }
        };

        // The latest state still couldn't serve this as_of: watch+sleep in a
        // loop until it's ready.
        let mut watch = self.applier.watch();
        let watch = &mut watch;
        let sleeps = self
            .applier
            .metrics
            .retries
            .snapshot
            .stream(Retry::persist_defaults(SystemTime::now()).into_retry_stream());

        enum Wake<'a, K, V, T, D> {
            Watch(&'a mut StateWatch<K, V, T, D>),
            Sleep(MetricsRetryStream),
        }
        let mut watch_fut = std::pin::pin!(
            watch
                .wait_for_seqno_ge(seqno.next())
                .map(Wake::Watch)
                .instrument(trace_span!("snapshot::watch")),
        );
        let mut sleep_fut = std::pin::pin!(
            sleeps
                .sleep()
                .map(Wake::Sleep)
                .instrument(trace_span!("snapshot::sleep")),
        );

        // To reduce log spam, we log "not yet available" only once at info if
        // it passes a certain threshold. Then, if it did one info log, we log
        // again at info when it resolves.
        let mut logged_at_info = false;
        loop {
            // Use a duration based threshold here instead of the usual
            // INFO_MIN_ATTEMPTS because here we're waiting on an
            // external thing to arrive.
            if !logged_at_info && start.elapsed() >= Duration::from_millis(1024) {
                logged_at_info = true;
                info!(
                    "snapshot {} {} as of {:?} not yet available for {} upper {:?}",
                    self.applier.shard_metrics.name,
                    self.shard_id(),
                    as_of.elements(),
                    seqno,
                    upper.elements(),
                );
            } else {
                debug!(
                    "snapshot {} {} as of {:?} not yet available for {} upper {:?}",
                    self.applier.shard_metrics.name,
                    self.shard_id(),
                    as_of.elements(),
                    seqno,
                    upper.elements(),
                );
            }

            let wake = match future::select(watch_fut.as_mut(), sleep_fut.as_mut()).await {
                future::Either::Left((wake, _)) => wake,
                future::Either::Right((wake, _)) => wake,
            };
            // Note that we don't need to fetch in the Watch case, because the
            // Watch wakeup is a signal that the shared state has already been
            // updated.
            match &wake {
                Wake::Watch(_) => self.applier.metrics.watch.snapshot_woken_via_watch.inc(),
                Wake::Sleep(_) => {
                    self.applier.metrics.watch.snapshot_woken_via_sleep.inc();
                    self.applier.fetch_and_update_state(Some(seqno)).await;
                }
            }

            (seqno, upper) = match self.applier.snapshot(as_of) {
                Ok(x) => {
                    if logged_at_info {
                        info!(
                            "snapshot {} {} as of {:?} now available",
                            self.applier.shard_metrics.name,
                            self.shard_id(),
                            as_of.elements(),
                        );
                    }
                    return Ok(x);
                }
                Err(SnapshotErr::AsOfNotYetAvailable(seqno, Upper(upper))) => {
                    // The upper isn't ready yet, fall through and try again.
                    (seqno, upper)
                }
                Err(SnapshotErr::AsOfHistoricalDistinctionsLost(Since(since))) => {
                    return Err(Since(since));
                }
            };

            match wake {
                Wake::Watch(watch) => {
                    watch_fut.set(
                        watch
                            .wait_for_seqno_ge(seqno.next())
                            .map(Wake::Watch)
                            .instrument(trace_span!("snapshot::watch")),
                    );
                }
                Wake::Sleep(sleeps) => {
                    debug!(
                        "snapshot {} {} sleeping for {:?}",
                        self.applier.shard_metrics.name,
                        self.shard_id(),
                        sleeps.next_sleep()
                    );
                    sleep_fut.set(
                        sleeps
                            .sleep()
                            .map(Wake::Sleep)
                            .instrument(trace_span!("snapshot::sleep")),
                    );
                }
            }
        }
    }

    // NB: Unlike the other methods here, this one is read-only.
    pub fn verify_listen(&self, as_of: &Antichain<T>) -> Result<(), Since<T>> {
        match self.applier.verify_listen(as_of) {
            Ok(Ok(())) => Ok(()),
            Ok(Err(Upper(_))) => {
                // The upper may not be ready yet (maybe it would be ready if we
                // re-fetched state), but that's okay! One way to think of
                // Listen is as an async stream where creating the stream at any
                // legal as_of does not block but then updates trickle in once
                // they are available.
                Ok(())
            }
            Err(Since(since)) => Err(Since(since)),
        }
    }

    pub async fn next_listen_batch(
        &self,
        frontier: &Antichain<T>,
        watch: &mut StateWatch<K, V, T, D>,
        reader_id: Option<&LeasedReaderId>,
        // If Some, an override for the default listen sleep retry parameters.
        retry: Option<RetryParameters>,
    ) -> HollowBatch<T> {
        let mut seqno = match self.applier.next_listen_batch(frontier) {
            Ok(b) => return b,
            Err(seqno) => seqno,
        };

        // The latest state still doesn't have a new frontier for us:
        // watch+sleep in a loop until it does.
        let retry = retry.unwrap_or_else(|| next_listen_batch_retry_params(&self.applier.cfg));
        let sleeps = self
            .applier
            .metrics
            .retries
            .next_listen_batch
            .stream(retry.into_retry(SystemTime::now()).into_retry_stream());

        enum Wake<'a, K, V, T, D> {
            Watch(&'a mut StateWatch<K, V, T, D>),
            Sleep(MetricsRetryStream),
        }
        let mut watch_fut = std::pin::pin!(
            watch
                .wait_for_seqno_ge(seqno.next())
                .map(Wake::Watch)
                .instrument(trace_span!("snapshot::watch"))
        );
        let mut sleep_fut = std::pin::pin!(
            sleeps
                .sleep()
                .map(Wake::Sleep)
                .instrument(trace_span!("snapshot::sleep"))
        );

        loop {
            let wake = match future::select(watch_fut.as_mut(), sleep_fut.as_mut()).await {
                future::Either::Left((wake, _)) => wake,
                future::Either::Right((wake, _)) => wake,
            };
            // Note that we don't need to fetch in the Watch case, because the
            // Watch wakeup is a signal that the shared state has already been
            // updated.
            match &wake {
                Wake::Watch(_) => self.applier.metrics.watch.listen_woken_via_watch.inc(),
                Wake::Sleep(_) => {
                    self.applier.metrics.watch.listen_woken_via_sleep.inc();
                    self.applier.fetch_and_update_state(Some(seqno)).await;
                }
            }

            seqno = match self.applier.next_listen_batch(frontier) {
                Ok(b) => {
                    match &wake {
                        Wake::Watch(_) => {
                            self.applier.metrics.watch.listen_resolved_via_watch.inc()
                        }
                        Wake::Sleep(_) => {
                            self.applier.metrics.watch.listen_resolved_via_sleep.inc()
                        }
                    }
                    return b;
                }
                Err(seqno) => seqno,
            };

            // Wait a bit and try again. Intentionally don't ever log
            // this at info level.
            match wake {
                Wake::Watch(watch) => {
                    watch_fut.set(
                        watch
                            .wait_for_seqno_ge(seqno.next())
                            .map(Wake::Watch)
                            .instrument(trace_span!("snapshot::watch")),
                    );
                }
                Wake::Sleep(sleeps) => {
                    debug!(
                        "{:?}: {} {} next_listen_batch didn't find new data, retrying in {:?}",
                        reader_id,
                        self.applier.shard_metrics.name,
                        self.shard_id(),
                        sleeps.next_sleep()
                    );
                    sleep_fut.set(
                        sleeps
                            .sleep()
                            .map(Wake::Sleep)
                            .instrument(trace_span!("snapshot::sleep")),
                    );
                }
            }
        }
    }

    async fn apply_unbatched_idempotent_cmd<
        R,
        WorkFn: FnMut(
            SeqNo,
            &PersistConfig,
            &mut StateCollections<T>,
        ) -> ControlFlow<NoOpStateTransition<R>, R>,
    >(
        &self,
        cmd: &CmdMetrics,
        mut work_fn: WorkFn,
    ) -> (SeqNo, R, RoutineMaintenance) {
        let mut retry = self
            .applier
            .metrics
            .retries
            .idempotent_cmd
            .stream(Retry::persist_defaults(SystemTime::now()).into_retry_stream());
        loop {
            match self.applier.apply_unbatched_cmd(cmd, &mut work_fn).await {
                Ok((seqno, x, maintenance)) => match x {
                    Ok(x) => {
                        return (seqno, x, maintenance);
                    }
                    Err(NoOpStateTransition(x)) => {
                        return (seqno, x, maintenance);
                    }
                },
                Err(err) => {
                    if retry.attempt() >= INFO_MIN_ATTEMPTS {
                        info!(
                            "apply_unbatched_idempotent_cmd {} received an indeterminate error, retrying in {:?}: {}",
                            cmd.name,
                            retry.next_sleep(),
                            err
                        );
                    } else {
                        debug!(
                            "apply_unbatched_idempotent_cmd {} received an indeterminate error, retrying in {:?}: {}",
                            cmd.name,
                            retry.next_sleep(),
                            err
                        );
                    }
                    retry = retry.sleep().await;
                    continue;
                }
            }
        }
    }
}

impl<K, V, T, D> Machine<K, V, T, D>
where
    K: Debug + Codec,
    V: Debug + Codec,
    T: Timestamp + Lattice + Codec64 + Sync,
    D: Semigroup + Codec64 + PartialEq,
{
    pub async fn merge_res(
        &self,
        res: &FueledMergeRes<T>,
    ) -> (ApplyMergeResult, RoutineMaintenance) {
        let metrics = Arc::clone(&self.applier.metrics);

        // SUBTLE! If Machine::merge_res returns false, the blobs referenced in
        // compaction output are deleted so we don't leak them. Naively passing
        // back the value returned by State::apply_merge_res might give a false
        // negative in the presence of retries and Indeterminate errors.
        // Specifically, something like the following:
        //
        // - We try to apply_merge_res, it matches.
        // - When apply_unbatched_cmd goes to commit the new state, the
        //   Consensus::compare_and_set returns an Indeterminate error (but
        //   actually succeeds). The committed State now contains references to
        //   the compaction output blobs.
        // - Machine::apply_unbatched_idempotent_cmd retries the Indeterminate
        //   error. For whatever reason, this time though it doesn't match
        //   (maybe the batches simply get grouped difference when deserialized
        //   from state, or more unavoidably perhaps another compaction
        //   happens).
        // - This now bubbles up applied=false to the caller, which uses it as a
        //   signal that the blobs in the compaction output should be deleted so
        //   that we don't leak them.
        // - We now contain references in committed State to blobs that don't
        //   exist.
        //
        // The fix is to keep track of whether applied ever was true, even for a
        // compare_and_set that returned an Indeterminate error. This has the
        // chance of false positive (leaking a blob) but that's better than a
        // false negative (a blob we can never recover referenced by state). We
        // anyway need a mechanism to clean up leaked blobs because of process
        // crashes.
        let mut merge_result_ever_applied = ApplyMergeResult::NotAppliedNoMatch;
        let (_seqno, _apply_merge_result, maintenance) = self
            .apply_unbatched_idempotent_cmd(&metrics.cmds.merge_res, |_, _, state| {
                let ret = state.apply_merge_res::<D>(res, &Arc::clone(&metrics).columnar);
                if let Continue(result) = ret {
                    // record if we've ever applied the merge
                    if result.applied() {
                        merge_result_ever_applied = result;
                    }
                    // otherwise record the most granular reason for _not_
                    // applying the merge when there was a matching batch
                    if result.matched() && !result.applied() && !merge_result_ever_applied.applied()
                    {
                        merge_result_ever_applied = result;
                    }
                }
                ret
            })
            .await;
        (merge_result_ever_applied, maintenance)
    }
}

pub(crate) struct ExpireFn(
    /// This is stored on WriteHandle and ReadHandle, which we require to be
    /// Send + Sync, but the Future is only Send and not Sync. Instead store a
    /// FnOnce that returns the Future. This could also be made an `IntoFuture`,
    /// once producing one of those is made easier.
    pub(crate) Box<dyn FnOnce() -> BoxFuture<'static, ()> + Send + Sync + 'static>,
);

impl Debug for ExpireFn {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ExpireFn").finish_non_exhaustive()
    }
}

#[derive(Debug)]
pub(crate) enum CompareAndAppendRes<T> {
    Success(SeqNo, WriterMaintenance<T>),
    InvalidUsage(InvalidUsage<T>),
    UpperMismatch(SeqNo, Antichain<T>),
    InlineBackpressure,
}

#[cfg(test)]
impl<T: Debug> CompareAndAppendRes<T> {
    #[track_caller]
    fn unwrap(self) -> (SeqNo, WriterMaintenance<T>) {
        match self {
            CompareAndAppendRes::Success(seqno, maintenance) => (seqno, maintenance),
            x => panic!("{:?}", x),
        }
    }
}

impl<K, V, T, D> Machine<K, V, T, D>
where
    K: Debug + Codec,
    V: Debug + Codec,
    T: Timestamp + Lattice + Codec64 + Sync,
    D: Semigroup + Codec64 + Send + Sync,
{
    #[allow(clippy::unused_async)]
    pub async fn start_reader_heartbeat_tasks(
        self,
        reader_id: LeasedReaderId,
        gc: GarbageCollector<K, V, T, D>,
    ) -> Vec<JoinHandle<()>> {
        let mut ret = Vec::new();
        let metrics = Arc::clone(&self.applier.metrics);

        // TODO: In response to a production incident, this runs the heartbeat
        // task on both the in-context tokio runtime and persist's isolated
        // runtime. We think we were seeing tasks (including this one) get stuck
        // indefinitely in tokio while waiting for a runtime worker. This could
        // happen if some other task in that runtime never yields. It's possible
        // that one of the two runtimes is healthy while the other isn't (this
        // was inconclusive in the incident debugging), and the heartbeat task
        // is fairly lightweight, so run a copy in each in case that helps.
        //
        // The real fix here is to find the misbehaving task and fix it. Remove
        // this duplication when that happens.
        let name = format!("persist::heartbeat_read({},{})", self.shard_id(), reader_id);
        ret.push(mz_ore::task::spawn(|| name, {
            let machine = self.clone();
            let reader_id = reader_id.clone();
            let gc = gc.clone();
            metrics
                .tasks
                .heartbeat_read
                .instrument_task(Self::reader_heartbeat_task(machine, reader_id, gc))
        }));

        let isolated_runtime = Arc::clone(&self.isolated_runtime);
        let name = format!(
            "persist::heartbeat_read_isolated({},{})",
            self.shard_id(),
            reader_id
        );
        ret.push(
            isolated_runtime.spawn_named(
                || name,
                metrics
                    .tasks
                    .heartbeat_read
                    .instrument_task(Self::reader_heartbeat_task(self, reader_id, gc)),
            ),
        );

        ret
    }

    async fn reader_heartbeat_task(
        machine: Self,
        reader_id: LeasedReaderId,
        gc: GarbageCollector<K, V, T, D>,
    ) {
        let sleep_duration = READER_LEASE_DURATION.get(&machine.applier.cfg) / 2;
        loop {
            let before_sleep = Instant::now();
            tokio::time::sleep(sleep_duration).await;

            let elapsed_since_before_sleeping = before_sleep.elapsed();
            if elapsed_since_before_sleeping > sleep_duration + Duration::from_secs(60) {
                warn!(
                    "reader ({}) of shard ({}) went {}s between heartbeats",
                    reader_id,
                    machine.shard_id(),
                    elapsed_since_before_sleeping.as_secs_f64()
                );
            }

            let before_heartbeat = Instant::now();
            let (_seqno, existed, maintenance) = machine
                .heartbeat_leased_reader(&reader_id, (machine.applier.cfg.now)())
                .await;
            maintenance.start_performing(&machine, &gc);

            let elapsed_since_heartbeat = before_heartbeat.elapsed();
            if elapsed_since_heartbeat > Duration::from_secs(60) {
                warn!(
                    "reader ({}) of shard ({}) heartbeat call took {}s",
                    reader_id,
                    machine.shard_id(),
                    elapsed_since_heartbeat.as_secs_f64(),
                );
            }

            if !existed {
                // If the read handle was intentionally expired, this task
                // *should* be aborted before it observes the expiration. So if
                // we get here, this task somehow failed to keep the read lease
                // alive. Warn loudly, because there's now a live read handle to
                // an expired shard that will panic if used, but don't panic,
                // just in case there is some edge case that results in this
                // task observing the intentional expiration of a read handle.
                warn!(
                    "heartbeat task for reader ({}) of shard ({}) exiting due to expired lease \
                     while read handle is live",
                    reader_id,
                    machine.shard_id(),
                );
                return;
            }
        }
    }
}

pub(crate) const NEXT_LISTEN_BATCH_RETRYER_FIXED_SLEEP: Config<Duration> = Config::new(
    "persist_next_listen_batch_retryer_fixed_sleep",
    Duration::from_millis(1200), // pubsub is on by default!
    "\
    The fixed sleep when polling for new batches from a Listen or Subscribe. Skipped if zero.",
);

pub(crate) const NEXT_LISTEN_BATCH_RETRYER_INITIAL_BACKOFF: Config<Duration> = Config::new(
    "persist_next_listen_batch_retryer_initial_backoff",
    Duration::from_millis(100), // pubsub is on by default!
    "The initial backoff when polling for new batches from a Listen or Subscribe.",
);

pub(crate) const NEXT_LISTEN_BATCH_RETRYER_MULTIPLIER: Config<u32> = Config::new(
    "persist_next_listen_batch_retryer_multiplier",
    2,
    "The backoff multiplier when polling for new batches from a Listen or Subscribe.",
);

pub(crate) const NEXT_LISTEN_BATCH_RETRYER_CLAMP: Config<Duration> = Config::new(
    "persist_next_listen_batch_retryer_clamp",
    Duration::from_secs(16), // pubsub is on by default!
    "The backoff clamp duration when polling for new batches from a Listen or Subscribe.",
);

fn next_listen_batch_retry_params(cfg: &ConfigSet) -> RetryParameters {
    RetryParameters {
        fixed_sleep: NEXT_LISTEN_BATCH_RETRYER_FIXED_SLEEP.get(cfg),
        initial_backoff: NEXT_LISTEN_BATCH_RETRYER_INITIAL_BACKOFF.get(cfg),
        multiplier: NEXT_LISTEN_BATCH_RETRYER_MULTIPLIER.get(cfg),
        clamp: NEXT_LISTEN_BATCH_RETRYER_CLAMP.get(cfg),
    }
}

pub const INFO_MIN_ATTEMPTS: usize = 3;

pub async fn retry_external<R, F, WorkFn>(metrics: &RetryMetrics, mut work_fn: WorkFn) -> R
where
    F: std::future::Future<Output = Result<R, ExternalError>>,
    WorkFn: FnMut() -> F,
{
    let mut retry = metrics.stream(Retry::persist_defaults(SystemTime::now()).into_retry_stream());
    loop {
        match work_fn().await {
            Ok(x) => {
                if retry.attempt() > 0 {
                    debug!(
                        "external operation {} succeeded after failing at least once",
                        metrics.name,
                    );
                }
                return x;
            }
            Err(err) => {
                if retry.attempt() >= INFO_MIN_ATTEMPTS {
                    info!(
                        "external operation {} failed, retrying in {:?}: {}",
                        metrics.name,
                        retry.next_sleep(),
                        err.display_with_causes()
                    );
                } else {
                    debug!(
                        "external operation {} failed, retrying in {:?}: {}",
                        metrics.name,
                        retry.next_sleep(),
                        err.display_with_causes()
                    );
                }
                retry = retry.sleep().await;
            }
        }
    }
}

pub async fn retry_determinate<R, F, WorkFn>(
    metrics: &RetryMetrics,
    mut work_fn: WorkFn,
) -> Result<R, Indeterminate>
where
    F: std::future::Future<Output = Result<R, ExternalError>>,
    WorkFn: FnMut() -> F,
{
    let mut retry = metrics.stream(Retry::persist_defaults(SystemTime::now()).into_retry_stream());
    loop {
        match work_fn().await {
            Ok(x) => {
                if retry.attempt() > 0 {
                    debug!(
                        "external operation {} succeeded after failing at least once",
                        metrics.name,
                    );
                }
                return Ok(x);
            }
            Err(ExternalError::Determinate(err)) => {
                // The determinate "could not serialize access" errors
                // happen often enough in dev (which uses Postgres) that
                // it's impeding people's work. At the same time, it's been
                // a source of confusion for eng. The situation is much
                // better on CRDB and we have metrics coverage in prod, so
                // this is redundant enough that it's more hurtful than
                // helpful. As a result, this intentionally ignores
                // INFO_MIN_ATTEMPTS and always logs at debug.
                debug!(
                    "external operation {} failed, retrying in {:?}: {}",
                    metrics.name,
                    retry.next_sleep(),
                    err.display_with_causes()
                );
                retry = retry.sleep().await;
                continue;
            }
            Err(ExternalError::Indeterminate(x)) => return Err(x),
        }
    }
}

#[cfg(test)]
pub mod datadriven {
    use std::collections::BTreeMap;
    use std::pin::pin;
    use std::sync::{Arc, LazyLock};

    use anyhow::anyhow;
    use differential_dataflow::consolidation::consolidate_updates;
    use differential_dataflow::trace::Description;
    use futures::StreamExt;
    use mz_dyncfg::{ConfigUpdates, ConfigVal};
    use mz_persist::indexed::encoding::BlobTraceBatchPart;
    use mz_persist_types::codec_impls::{StringSchema, UnitSchema};

    use crate::batch::{
        BLOB_TARGET_SIZE, Batch, BatchBuilder, BatchBuilderConfig, BatchBuilderInternal,
        BatchParts, validate_truncate_batch,
    };
    use crate::cfg::COMPACTION_MEMORY_BOUND_BYTES;
    use crate::fetch::{EncodedPart, FetchConfig};
    use crate::internal::compact::{CompactConfig, CompactReq, Compactor};
    use crate::internal::datadriven::DirectiveArgs;
    use crate::internal::encoding::Schemas;
    use crate::internal::gc::GcReq;
    use crate::internal::paths::{BlobKey, BlobKeyPrefix, PartialBlobKey};
    use crate::internal::state::{BatchPart, RunOrder, RunPart};
    use crate::internal::state_versions::EncodedRollup;
    use crate::read::{Listen, ListenEvent, READER_LEASE_DURATION};
    use crate::rpc::NoopPubSubSender;
    use crate::tests::new_test_client;
    use crate::write::COMBINE_INLINE_WRITES;
    use crate::{GarbageCollector, PersistClient};

    use super::*;

    static SCHEMAS: LazyLock<Schemas<String, ()>> = LazyLock::new(|| Schemas {
        id: Some(SchemaId(0)),
        key: Arc::new(StringSchema),
        val: Arc::new(UnitSchema),
    });

    /// Shared state for a single [crate::internal::machine] [datadriven::TestFile].
    #[derive(Debug)]
    pub struct MachineState {
        pub client: PersistClient,
        pub shard_id: ShardId,
        pub state_versions: Arc<StateVersions>,
        pub machine: Machine<String, (), u64, i64>,
        pub gc: GarbageCollector<String, (), u64, i64>,
        pub batches: BTreeMap<String, HollowBatch<u64>>,
        pub rollups: BTreeMap<String, EncodedRollup>,
        pub listens: BTreeMap<String, Listen<String, (), u64, i64>>,
        pub routine: Vec<RoutineMaintenance>,
    }

    impl MachineState {
        pub async fn new(dyncfgs: &ConfigUpdates) -> Self {
            let shard_id = ShardId::new();
            let client = new_test_client(dyncfgs).await;
            // Reset blob_target_size. Individual batch writes and compactions
            // can override it with an arg.
            client
                .cfg
                .set_config(&BLOB_TARGET_SIZE, *BLOB_TARGET_SIZE.default());
            // Our structured compaction code uses slightly different estimates
            // for array size than the old path, which can affect the results of
            // some compaction tests.
            client.cfg.set_config(&COMBINE_INLINE_WRITES, false);
            let state_versions = Arc::new(StateVersions::new(
                client.cfg.clone(),
                Arc::clone(&client.consensus),
                Arc::clone(&client.blob),
                Arc::clone(&client.metrics),
            ));
            let machine = Machine::new(
                client.cfg.clone(),
                shard_id,
                Arc::clone(&client.metrics),
                Arc::clone(&state_versions),
                Arc::clone(&client.shared_states),
                Arc::new(NoopPubSubSender),
                Arc::clone(&client.isolated_runtime),
                Diagnostics::for_tests(),
            )
            .await
            .expect("codecs should match");
            let gc = GarbageCollector::new(machine.clone(), Arc::clone(&client.isolated_runtime));
            MachineState {
                shard_id,
                client,
                state_versions,
                machine,
                gc,
                batches: BTreeMap::default(),
                rollups: BTreeMap::default(),
                listens: BTreeMap::default(),
                routine: Vec::new(),
            }
        }

        fn to_batch(&self, hollow: HollowBatch<u64>) -> Batch<String, (), u64, i64> {
            Batch::new(
                true,
                Arc::clone(&self.client.metrics),
                Arc::clone(&self.client.blob),
                self.client.metrics.shards.shard(&self.shard_id, "test"),
                self.client.cfg.build_version.clone(),
                hollow,
            )
        }
    }

    /// Scans consensus and returns all states with their SeqNos
    /// and which batches they reference
    pub async fn consensus_scan(
        datadriven: &MachineState,
        args: DirectiveArgs<'_>,
    ) -> Result<String, anyhow::Error> {
        let from = args.expect("from_seqno");

        let mut states = datadriven
            .state_versions
            .fetch_all_live_states::<u64>(datadriven.shard_id)
            .await
            .expect("should only be called on an initialized shard")
            .check_ts_codec()
            .expect("shard codecs should not change");
        let mut s = String::new();
        while let Some(x) = states.next(|_| {}) {
            if x.seqno < from {
                continue;
            }
            let rollups: Vec<_> = x
                .collections
                .rollups
                .keys()
                .map(|seqno| seqno.to_string())
                .collect();
            let batches: Vec<_> = x
                .collections
                .trace
                .batches()
                .filter(|b| !b.is_empty())
                .filter_map(|b| {
                    datadriven
                        .batches
                        .iter()
                        .find(|(_, original_batch)| original_batch.parts == b.parts)
                        .map(|(batch_name, _)| batch_name.to_owned())
                })
                .collect();
            write!(
                s,
                "seqno={} batches={} rollups={}\n",
                x.seqno,
                batches.join(","),
                rollups.join(","),
            );
        }
        Ok(s)
    }

    pub async fn consensus_truncate(
        datadriven: &MachineState,
        args: DirectiveArgs<'_>,
    ) -> Result<String, anyhow::Error> {
        let to = args.expect("to_seqno");
        let removed = datadriven
            .client
            .consensus
            .truncate(&datadriven.shard_id.to_string(), to)
            .await
            .expect("valid truncation");
        Ok(format!("{}\n", removed))
    }

    pub async fn blob_scan_batches(
        datadriven: &MachineState,
        _args: DirectiveArgs<'_>,
    ) -> Result<String, anyhow::Error> {
        let key_prefix = BlobKeyPrefix::Shard(&datadriven.shard_id).to_string();

        let mut s = String::new();
        let () = datadriven
            .state_versions
            .blob
            .list_keys_and_metadata(&key_prefix, &mut |x| {
                let (_, key) = BlobKey::parse_ids(x.key).expect("key should be valid");
                if let PartialBlobKey::Batch(_, _) = key {
                    write!(s, "{}: {}b\n", x.key, x.size_in_bytes);
                }
            })
            .await?;
        Ok(s)
    }

    #[allow(clippy::unused_async)]
    pub async fn shard_desc(
        datadriven: &MachineState,
        _args: DirectiveArgs<'_>,
    ) -> Result<String, anyhow::Error> {
        Ok(format!(
            "since={:?} upper={:?}\n",
            datadriven.machine.applier.since().elements(),
            datadriven.machine.applier.clone_upper().elements()
        ))
    }

    pub async fn downgrade_since(
        datadriven: &mut MachineState,
        args: DirectiveArgs<'_>,
    ) -> Result<String, anyhow::Error> {
        let since = args.expect_antichain("since");
        let seqno = args.optional("seqno");
        let reader_id = args.expect("reader_id");
        let (_, since, routine) = datadriven
            .machine
            .downgrade_since(
                &reader_id,
                seqno,
                &since,
                (datadriven.machine.applier.cfg.now)(),
            )
            .await;
        datadriven.routine.push(routine);
        Ok(format!(
            "{} {:?}\n",
            datadriven.machine.seqno(),
            since.0.elements()
        ))
    }

    #[allow(clippy::unused_async)]
    pub async fn dyncfg(
        datadriven: &MachineState,
        args: DirectiveArgs<'_>,
    ) -> Result<String, anyhow::Error> {
        let mut updates = ConfigUpdates::default();
        for x in args.input.trim().split('\n') {
            match x.split(' ').collect::<Vec<_>>().as_slice() {
                &[name, val] => {
                    let config = datadriven
                        .client
                        .cfg
                        .entries()
                        .find(|x| x.name() == name)
                        .ok_or_else(|| anyhow!("unknown dyncfg: {}", name))?;
                    match config.val() {
                        ConfigVal::Usize(_) => {
                            let val = val.parse().map_err(anyhow::Error::new)?;
                            updates.add_dynamic(name, ConfigVal::Usize(val));
                        }
                        ConfigVal::Bool(_) => {
                            let val = val.parse().map_err(anyhow::Error::new)?;
                            updates.add_dynamic(name, ConfigVal::Bool(val));
                        }
                        x => unimplemented!("dyncfg type: {:?}", x),
                    }
                }
                x => return Err(anyhow!("expected `name val` got: {:?}", x)),
            }
        }
        updates.apply(&datadriven.client.cfg);

        Ok("ok\n".to_string())
    }

    pub async fn compare_and_downgrade_since(
        datadriven: &mut MachineState,
        args: DirectiveArgs<'_>,
    ) -> Result<String, anyhow::Error> {
        let expected_opaque: u64 = args.expect("expect_opaque");
        let new_opaque: u64 = args.expect("opaque");
        let new_since = args.expect_antichain("since");
        let reader_id = args.expect("reader_id");
        let (res, routine) = datadriven
            .machine
            .compare_and_downgrade_since(&reader_id, &expected_opaque, (&new_opaque, &new_since))
            .await;
        datadriven.routine.push(routine);
        let since = res.map_err(|(opaque, since)| {
            anyhow!("mismatch: opaque={} since={:?}", opaque, since.0.elements())
        })?;
        Ok(format!(
            "{} {} {:?}\n",
            datadriven.machine.seqno(),
            new_opaque,
            since.0.elements()
        ))
    }

    pub async fn write_rollup(
        datadriven: &mut MachineState,
        args: DirectiveArgs<'_>,
    ) -> Result<String, anyhow::Error> {
        let output = args.expect_str("output");

        let rollup = datadriven
            .machine
            .applier
            .write_rollup_for_state()
            .await
            .expect("rollup");

        datadriven
            .rollups
            .insert(output.to_string(), rollup.clone());

        Ok(format!(
            "state={} diffs=[{}, {})\n",
            rollup.seqno,
            rollup._desc.lower().first().expect("seqno"),
            rollup._desc.upper().first().expect("seqno"),
        ))
    }

    pub async fn add_rollup(
        datadriven: &mut MachineState,
        args: DirectiveArgs<'_>,
    ) -> Result<String, anyhow::Error> {
        let input = args.expect_str("input");
        let rollup = datadriven
            .rollups
            .get(input)
            .expect("unknown batch")
            .clone();

        let (applied, maintenance) = datadriven
            .machine
            .add_rollup((rollup.seqno, &rollup.to_hollow()))
            .await;

        if !applied {
            return Err(anyhow!("failed to apply rollup for: {}", rollup.seqno));
        }

        datadriven.routine.push(maintenance);
        Ok(format!("{}\n", datadriven.machine.seqno()))
    }

    pub async fn write_batch(
        datadriven: &mut MachineState,
        args: DirectiveArgs<'_>,
    ) -> Result<String, anyhow::Error> {
        let output = args.expect_str("output");
        let lower = args.expect_antichain("lower");
        let upper = args.expect_antichain("upper");
        assert!(PartialOrder::less_than(&lower, &upper));
        let since = args
            .optional_antichain("since")
            .unwrap_or_else(|| Antichain::from_elem(0));
        let target_size = args.optional("target_size");
        let parts_size_override = args.optional("parts_size_override");
        let consolidate = args.optional("consolidate").unwrap_or(true);
        let mut updates: Vec<_> = args
            .input
            .split('\n')
            .flat_map(DirectiveArgs::parse_update)
            .collect();

        let mut cfg = BatchBuilderConfig::new(&datadriven.client.cfg, datadriven.shard_id);
        if let Some(target_size) = target_size {
            cfg.blob_target_size = target_size;
        };
        if consolidate {
            consolidate_updates(&mut updates);
        }
        let run_order = if consolidate {
            cfg.preferred_order
        } else {
            RunOrder::Unordered
        };
        let parts = BatchParts::new_ordered::<i64>(
            cfg.clone(),
            run_order,
            Arc::clone(&datadriven.client.metrics),
            Arc::clone(&datadriven.machine.applier.shard_metrics),
            datadriven.shard_id,
            Arc::clone(&datadriven.client.blob),
            Arc::clone(&datadriven.client.isolated_runtime),
            &datadriven.client.metrics.user,
        );
        let builder = BatchBuilderInternal::new(
            cfg.clone(),
            parts,
            Arc::clone(&datadriven.client.metrics),
            SCHEMAS.clone(),
            Arc::clone(&datadriven.client.blob),
            datadriven.shard_id.clone(),
            datadriven.client.cfg.build_version.clone(),
        );
        let mut builder = BatchBuilder::new(builder, Description::new(lower, upper.clone(), since));
        for ((k, ()), t, d) in updates {
            builder.add(&k, &(), &t, &d).await.expect("invalid batch");
        }
        let mut batch = builder.finish(upper).await?;
        // We can only reasonably use parts_size_override with hollow batches,
        // so if it's set, flush any inline batches out.
        if parts_size_override.is_some() {
            batch
                .flush_to_blob(
                    &cfg,
                    &datadriven.client.metrics.user,
                    &datadriven.client.isolated_runtime,
                    &SCHEMAS,
                )
                .await;
        }
        let batch = batch.into_hollow_batch();

        if let Some(size) = parts_size_override {
            let mut batch = batch.clone();
            for part in batch.parts.iter_mut() {
                match part {
                    RunPart::Many(run) => run.max_part_bytes = size,
                    RunPart::Single(BatchPart::Hollow(part)) => part.encoded_size_bytes = size,
                    RunPart::Single(BatchPart::Inline { .. }) => unreachable!("flushed out above"),
                }
            }
            datadriven.batches.insert(output.to_owned(), batch);
        } else {
            datadriven.batches.insert(output.to_owned(), batch.clone());
        }
        Ok(format!("parts={} len={}\n", batch.part_count(), batch.len))
    }

    pub async fn fetch_batch(
        datadriven: &MachineState,
        args: DirectiveArgs<'_>,
    ) -> Result<String, anyhow::Error> {
        let input = args.expect_str("input");
        let stats = args.optional_str("stats");
        let batch = datadriven.batches.get(input).expect("unknown batch");

        let mut s = String::new();
        let mut stream = pin!(
            batch
                .part_stream(
                    datadriven.shard_id,
                    &*datadriven.state_versions.blob,
                    &*datadriven.state_versions.metrics
                )
                .enumerate()
        );
        while let Some((idx, part)) = stream.next().await {
            let part = &*part?;
            write!(s, "<part {idx}>\n");

            let lower = match part {
                BatchPart::Inline { updates, .. } => {
                    let updates: BlobTraceBatchPart<u64> =
                        updates.decode(&datadriven.client.metrics.columnar)?;
                    updates.structured_key_lower()
                }
                other => other.structured_key_lower(),
            };

            if let Some(lower) = lower {
                if stats == Some("lower") {
                    writeln!(s, "<key lower={}>", lower.get())
                }
            }

            match part {
                BatchPart::Hollow(part) => {
                    let blob_batch = datadriven
                        .client
                        .blob
                        .get(&part.key.complete(&datadriven.shard_id))
                        .await;
                    match blob_batch {
                        Ok(Some(_)) | Err(_) => {}
                        // don't try to fetch/print the keys of the batch part
                        // if the blob store no longer has it
                        Ok(None) => {
                            s.push_str("<empty>\n");
                            continue;
                        }
                    };
                }
                BatchPart::Inline { .. } => {}
            };
            let part = EncodedPart::fetch(
                &FetchConfig::from_persist_config(&datadriven.client.cfg),
                &datadriven.shard_id,
                datadriven.client.blob.as_ref(),
                datadriven.client.metrics.as_ref(),
                datadriven.machine.applier.shard_metrics.as_ref(),
                &datadriven.client.metrics.read.batch_fetcher,
                &batch.desc,
                part,
            )
            .await
            .expect("invalid batch part");
            let part = part
                .normalize(&datadriven.client.metrics.columnar)
                .into_part::<String, ()>(&*SCHEMAS.key, &*SCHEMAS.val);

            for ((k, _v), t, d) in part
                .decode_iter::<_, _, u64, i64>(&*SCHEMAS.key, &*SCHEMAS.val)
                .expect("valid schemas")
            {
                writeln!(s, "{k} {t} {d}");
            }
        }
        if !s.is_empty() {
            for (idx, (_meta, run)) in batch.runs().enumerate() {
                write!(s, "<run {idx}>\n");
                for part in run {
                    let part_idx = batch
                        .parts
                        .iter()
                        .position(|p| p == part)
                        .expect("part should exist");
                    write!(s, "part {part_idx}\n");
                }
            }
        }
        Ok(s)
    }

    #[allow(clippy::unused_async)]
    pub async fn truncate_batch_desc(
        datadriven: &mut MachineState,
        args: DirectiveArgs<'_>,
    ) -> Result<String, anyhow::Error> {
        let input = args.expect_str("input");
        let output = args.expect_str("output");
        let lower = args.expect_antichain("lower");
        let upper = args.expect_antichain("upper");

        let mut batch = datadriven
            .batches
            .get(input)
            .expect("unknown batch")
            .clone();
        let truncated_desc = Description::new(lower, upper, batch.desc.since().clone());
        let () = validate_truncate_batch(&batch, &truncated_desc, false, true)?;
        batch.desc = truncated_desc;
        datadriven.batches.insert(output.to_owned(), batch.clone());
        Ok(format!("parts={} len={}\n", batch.part_count(), batch.len))
    }

    #[allow(clippy::unused_async)]
    pub async fn set_batch_parts_size(
        datadriven: &mut MachineState,
        args: DirectiveArgs<'_>,
    ) -> Result<String, anyhow::Error> {
        let input = args.expect_str("input");
        let size = args.expect("size");
        let batch = datadriven.batches.get_mut(input).expect("unknown batch");
        for part in batch.parts.iter_mut() {
            match part {
                RunPart::Single(BatchPart::Hollow(x)) => x.encoded_size_bytes = size,
                _ => {
                    panic!("set_batch_parts_size only supports hollow parts")
                }
            }
        }
        Ok("ok\n".to_string())
    }

    pub async fn compact(
        datadriven: &mut MachineState,
        args: DirectiveArgs<'_>,
    ) -> Result<String, anyhow::Error> {
        let output = args.expect_str("output");
        let lower = args.expect_antichain("lower");
        let upper = args.expect_antichain("upper");
        let since = args.expect_antichain("since");
        let target_size = args.optional("target_size");
        let memory_bound = args.optional("memory_bound");

        let mut inputs = Vec::new();
        for input in args.args.get("inputs").expect("missing inputs") {
            inputs.push(
                datadriven
                    .batches
                    .get(input)
                    .expect("unknown batch")
                    .clone(),
            );
        }

        let cfg = datadriven.client.cfg.clone();
        if let Some(target_size) = target_size {
            cfg.set_config(&BLOB_TARGET_SIZE, target_size);
        };
        if let Some(memory_bound) = memory_bound {
            cfg.set_config(&COMPACTION_MEMORY_BOUND_BYTES, memory_bound);
        }
        let req = CompactReq {
            shard_id: datadriven.shard_id,
            desc: Description::new(lower, upper, since),
            inputs,
        };
        let res = Compactor::<String, (), u64, i64>::compact(
            CompactConfig::new(&cfg, datadriven.shard_id),
            Arc::clone(&datadriven.client.blob),
            Arc::clone(&datadriven.client.metrics),
            Arc::clone(&datadriven.machine.applier.shard_metrics),
            Arc::clone(&datadriven.client.isolated_runtime),
            req,
            SCHEMAS.clone(),
        )
        .await?;

        datadriven
            .batches
            .insert(output.to_owned(), res.output.clone());
        Ok(format!(
            "parts={} len={}\n",
            res.output.part_count(),
            res.output.len
        ))
    }

    pub async fn clear_blob(
        datadriven: &MachineState,
        _args: DirectiveArgs<'_>,
    ) -> Result<String, anyhow::Error> {
        let mut to_delete = vec![];
        datadriven
            .client
            .blob
            .list_keys_and_metadata("", &mut |meta| {
                to_delete.push(meta.key.to_owned());
            })
            .await?;
        for blob in &to_delete {
            datadriven.client.blob.delete(blob).await?;
        }
        Ok(format!("deleted={}\n", to_delete.len()))
    }

    pub async fn restore_blob(
        datadriven: &MachineState,
        _args: DirectiveArgs<'_>,
    ) -> Result<String, anyhow::Error> {
        let not_restored = crate::internal::restore::restore_blob(
            &datadriven.state_versions,
            datadriven.client.blob.as_ref(),
            &datadriven.client.cfg.build_version,
            datadriven.shard_id,
            &*datadriven.state_versions.metrics,
        )
        .await?;
        let mut out = String::new();
        for key in not_restored {
            writeln!(&mut out, "{key}");
        }
        Ok(out)
    }

    #[allow(clippy::unused_async)]
    pub async fn rewrite_ts(
        datadriven: &mut MachineState,
        args: DirectiveArgs<'_>,
    ) -> Result<String, anyhow::Error> {
        let input = args.expect_str("input");
        let ts_rewrite = args.expect_antichain("frontier");
        let upper = args.expect_antichain("upper");

        let batch = datadriven.batches.get_mut(input).expect("unknown batch");
        let () = batch
            .rewrite_ts(&ts_rewrite, upper)
            .map_err(|err| anyhow!("invalid rewrite: {}", err))?;
        Ok("ok\n".into())
    }

    pub async fn gc(
        datadriven: &mut MachineState,
        args: DirectiveArgs<'_>,
    ) -> Result<String, anyhow::Error> {
        let new_seqno_since = args.expect("to_seqno");

        let req = GcReq {
            shard_id: datadriven.shard_id,
            new_seqno_since,
        };
        let (maintenance, stats) =
            GarbageCollector::gc_and_truncate(&datadriven.machine, req).await;
        datadriven.routine.push(maintenance);

        Ok(format!(
            "{} batch_parts={} rollups={} truncated={} state_rollups={}\n",
            datadriven.machine.seqno(),
            stats.batch_parts_deleted_from_blob,
            stats.rollups_deleted_from_blob,
            stats
                .truncated_consensus_to
                .iter()
                .map(|x| x.to_string())
                .collect::<Vec<_>>()
                .join(","),
            stats
                .rollups_removed_from_state
                .iter()
                .map(|x| x.to_string())
                .collect::<Vec<_>>()
                .join(","),
        ))
    }

    pub async fn snapshot(
        datadriven: &MachineState,
        args: DirectiveArgs<'_>,
    ) -> Result<String, anyhow::Error> {
        let as_of = args.expect_antichain("as_of");
        let snapshot = datadriven
            .machine
            .snapshot(&as_of)
            .await
            .map_err(|err| anyhow!("{:?}", err))?;

        let mut result = String::new();

        for batch in snapshot {
            writeln!(
                result,
                "<batch {:?}-{:?}>",
                batch.desc.lower().elements(),
                batch.desc.upper().elements()
            );
            for (run, (_meta, parts)) in batch.runs().enumerate() {
                writeln!(result, "<run {run}>");
                let mut stream = pin!(
                    futures::stream::iter(parts)
                        .flat_map(|part| part.part_stream(
                            datadriven.shard_id,
                            &*datadriven.state_versions.blob,
                            &*datadriven.state_versions.metrics
                        ))
                        .enumerate()
                );

                while let Some((idx, part)) = stream.next().await {
                    let part = &*part?;
                    writeln!(result, "<part {idx}>");

                    let part = EncodedPart::fetch(
                        &FetchConfig::from_persist_config(&datadriven.client.cfg),
                        &datadriven.shard_id,
                        datadriven.client.blob.as_ref(),
                        datadriven.client.metrics.as_ref(),
                        datadriven.machine.applier.shard_metrics.as_ref(),
                        &datadriven.client.metrics.read.batch_fetcher,
                        &batch.desc,
                        part,
                    )
                    .await
                    .expect("invalid batch part");
                    let part = part
                        .normalize(&datadriven.client.metrics.columnar)
                        .into_part::<String, ()>(&*SCHEMAS.key, &*SCHEMAS.val);

                    let mut updates = Vec::new();

                    for ((k, _v), mut t, d) in part
                        .decode_iter::<_, _, u64, i64>(&*SCHEMAS.key, &*SCHEMAS.val)
                        .expect("valid schemas")
                    {
                        t.advance_by(as_of.borrow());
                        updates.push((k, t, d));
                    }

                    consolidate_updates(&mut updates);

                    for (k, t, d) in updates {
                        writeln!(result, "{k} {t} {d}");
                    }
                }
            }
        }

        Ok(result)
    }

    pub async fn register_listen(
        datadriven: &mut MachineState,
        args: DirectiveArgs<'_>,
    ) -> Result<String, anyhow::Error> {
        let output = args.expect_str("output");
        let as_of = args.expect_antichain("as_of");
        let read = datadriven
            .client
            .open_leased_reader::<String, (), u64, i64>(
                datadriven.shard_id,
                Arc::new(StringSchema),
                Arc::new(UnitSchema),
                Diagnostics::for_tests(),
                true,
            )
            .await
            .expect("invalid shard types");
        let listen = read
            .listen(as_of)
            .await
            .map_err(|err| anyhow!("{:?}", err))?;
        datadriven.listens.insert(output.to_owned(), listen);
        Ok("ok\n".into())
    }

    pub async fn listen_through(
        datadriven: &mut MachineState,
        args: DirectiveArgs<'_>,
    ) -> Result<String, anyhow::Error> {
        let input = args.expect_str("input");
        // It's not possible to listen _through_ the empty antichain, so this is
        // intentionally `expect` instead of `expect_antichain`.
        let frontier = args.expect("frontier");
        let listen = datadriven.listens.get_mut(input).expect("unknown listener");
        let mut s = String::new();
        loop {
            for event in listen.fetch_next().await {
                match event {
                    ListenEvent::Updates(x) => {
                        for ((k, _v), t, d) in x.iter() {
                            write!(s, "{} {} {}\n", k.as_ref().unwrap(), t, d);
                        }
                    }
                    ListenEvent::Progress(x) => {
                        if !x.less_than(&frontier) {
                            return Ok(s);
                        }
                    }
                }
            }
        }
    }

    pub async fn register_critical_reader(
        datadriven: &mut MachineState,
        args: DirectiveArgs<'_>,
    ) -> Result<String, anyhow::Error> {
        let reader_id = args.expect("reader_id");
        let (state, maintenance) = datadriven
            .machine
            .register_critical_reader::<u64>(&reader_id, "tests")
            .await;
        datadriven.routine.push(maintenance);
        Ok(format!(
            "{} {:?}\n",
            datadriven.machine.seqno(),
            state.since.elements(),
        ))
    }

    pub async fn register_leased_reader(
        datadriven: &mut MachineState,
        args: DirectiveArgs<'_>,
    ) -> Result<String, anyhow::Error> {
        let reader_id = args.expect("reader_id");
        let (reader_state, maintenance) = datadriven
            .machine
            .register_leased_reader(
                &reader_id,
                "tests",
                READER_LEASE_DURATION.get(&datadriven.client.cfg),
                (datadriven.client.cfg.now)(),
                false,
            )
            .await;
        datadriven.routine.push(maintenance);
        Ok(format!(
            "{} {:?}\n",
            datadriven.machine.seqno(),
            reader_state.since.elements(),
        ))
    }

    pub async fn heartbeat_leased_reader(
        datadriven: &MachineState,
        args: DirectiveArgs<'_>,
    ) -> Result<String, anyhow::Error> {
        let reader_id = args.expect("reader_id");
        let _ = datadriven
            .machine
            .heartbeat_leased_reader(&reader_id, (datadriven.client.cfg.now)())
            .await;
        Ok(format!("{} ok\n", datadriven.machine.seqno()))
    }

    pub async fn expire_critical_reader(
        datadriven: &mut MachineState,
        args: DirectiveArgs<'_>,
    ) -> Result<String, anyhow::Error> {
        let reader_id = args.expect("reader_id");
        let (_, maintenance) = datadriven.machine.expire_critical_reader(&reader_id).await;
        datadriven.routine.push(maintenance);
        Ok(format!("{} ok\n", datadriven.machine.seqno()))
    }

    pub async fn expire_leased_reader(
        datadriven: &mut MachineState,
        args: DirectiveArgs<'_>,
    ) -> Result<String, anyhow::Error> {
        let reader_id = args.expect("reader_id");
        let (_, maintenance) = datadriven.machine.expire_leased_reader(&reader_id).await;
        datadriven.routine.push(maintenance);
        Ok(format!("{} ok\n", datadriven.machine.seqno()))
    }

    pub async fn compare_and_append_batches(
        datadriven: &MachineState,
        args: DirectiveArgs<'_>,
    ) -> Result<String, anyhow::Error> {
        let expected_upper = args.expect_antichain("expected_upper");
        let new_upper = args.expect_antichain("new_upper");

        let mut batches: Vec<Batch<String, (), u64, i64>> = args
            .args
            .get("batches")
            .expect("missing batches")
            .into_iter()
            .map(|batch| {
                let hollow = datadriven
                    .batches
                    .get(batch)
                    .expect("unknown batch")
                    .clone();
                datadriven.to_batch(hollow)
            })
            .collect();

        let mut writer = datadriven
            .client
            .open_writer(
                datadriven.shard_id,
                Arc::new(StringSchema),
                Arc::new(UnitSchema),
                Diagnostics::for_tests(),
            )
            .await?;

        let mut batch_refs: Vec<_> = batches.iter_mut().collect();

        let () = writer
            .compare_and_append_batch(batch_refs.as_mut_slice(), expected_upper, new_upper, true)
            .await?
            .map_err(|err| anyhow!("upper mismatch: {:?}", err))?;

        writer.expire().await;

        Ok("ok\n".into())
    }

    pub async fn expire_writer(
        datadriven: &mut MachineState,
        args: DirectiveArgs<'_>,
    ) -> Result<String, anyhow::Error> {
        let writer_id = args.expect("writer_id");
        let (_, maintenance) = datadriven.machine.expire_writer(&writer_id).await;
        datadriven.routine.push(maintenance);
        Ok(format!("{} ok\n", datadriven.machine.seqno()))
    }

    pub(crate) async fn finalize(
        datadriven: &mut MachineState,
        _args: DirectiveArgs<'_>,
    ) -> anyhow::Result<String> {
        let maintenance = datadriven.machine.become_tombstone().await?;
        datadriven.routine.push(maintenance);
        Ok(format!("{} ok\n", datadriven.machine.seqno()))
    }

    pub(crate) fn is_finalized(
        datadriven: &MachineState,
        _args: DirectiveArgs<'_>,
    ) -> anyhow::Result<String> {
        let seqno = datadriven.machine.seqno();
        let tombstone = datadriven.machine.is_finalized();
        Ok(format!("{seqno} {tombstone}\n"))
    }

    pub async fn compare_and_append(
        datadriven: &mut MachineState,
        args: DirectiveArgs<'_>,
    ) -> Result<String, anyhow::Error> {
        let input = args.expect_str("input");
        let writer_id = args.expect("writer_id");
        let mut batch = datadriven
            .batches
            .get(input)
            .expect("unknown batch")
            .clone();
        let token = args.optional("token").unwrap_or_else(IdempotencyToken::new);
        let now = (datadriven.client.cfg.now)();

        let (id, maintenance) = datadriven
            .machine
            .register_schema(&*SCHEMAS.key, &*SCHEMAS.val)
            .await;
        assert_eq!(id, SCHEMAS.id);
        datadriven.routine.push(maintenance);
        let maintenance = loop {
            let indeterminate = args
                .optional::<String>("prev_indeterminate")
                .map(|x| Indeterminate::new(anyhow::Error::msg(x)));
            let res = datadriven
                .machine
                .compare_and_append_idempotent(
                    &batch,
                    &writer_id,
                    now,
                    &token,
                    &HandleDebugState::default(),
                    indeterminate,
                )
                .await;
            match res {
                CompareAndAppendRes::Success(_, x) => break x,
                CompareAndAppendRes::UpperMismatch(_seqno, upper) => {
                    return Err(anyhow!("{:?}", Upper(upper)));
                }
                CompareAndAppendRes::InlineBackpressure => {
                    let mut b = datadriven.to_batch(batch.clone());
                    let cfg = BatchBuilderConfig::new(&datadriven.client.cfg, datadriven.shard_id);
                    b.flush_to_blob(
                        &cfg,
                        &datadriven.client.metrics.user,
                        &datadriven.client.isolated_runtime,
                        &*SCHEMAS,
                    )
                    .await;
                    batch = b.into_hollow_batch();
                    continue;
                }
                _ => panic!("{:?}", res),
            };
        };
        // TODO: Don't throw away writer maintenance. It's slightly tricky
        // because we need a WriterId for Compactor.
        datadriven.routine.push(maintenance.routine);
        Ok(format!(
            "{} {:?}\n",
            datadriven.machine.seqno(),
            datadriven.machine.applier.clone_upper().elements(),
        ))
    }

    pub async fn apply_merge_res(
        datadriven: &mut MachineState,
        args: DirectiveArgs<'_>,
    ) -> Result<String, anyhow::Error> {
        let input = args.expect_str("input");
        let batch = datadriven
            .batches
            .get(input)
            .expect("unknown batch")
            .clone();
        let (merge_res, maintenance) = datadriven
            .machine
            .merge_res(&FueledMergeRes { output: batch })
            .await;
        datadriven.routine.push(maintenance);
        Ok(format!(
            "{} {}\n",
            datadriven.machine.seqno(),
            merge_res.applied()
        ))
    }

    pub async fn perform_maintenance(
        datadriven: &mut MachineState,
        _args: DirectiveArgs<'_>,
    ) -> Result<String, anyhow::Error> {
        let mut s = String::new();
        for maintenance in datadriven.routine.drain(..) {
            let () = maintenance
                .perform(&datadriven.machine, &datadriven.gc)
                .await;
            let () = datadriven
                .machine
                .applier
                .fetch_and_update_state(None)
                .await;
            write!(s, "{} ok\n", datadriven.machine.seqno());
        }
        Ok(s)
    }
}

#[cfg(test)]
pub mod tests {
    use std::sync::Arc;

    use mz_dyncfg::ConfigUpdates;
    use mz_ore::cast::CastFrom;
    use mz_ore::task::spawn;
    use mz_persist::intercept::{InterceptBlob, InterceptHandle};
    use mz_persist::location::SeqNo;
    use timely::progress::Antichain;

    use crate::ShardId;
    use crate::batch::BatchBuilderConfig;
    use crate::cache::StateCache;
    use crate::internal::gc::{GarbageCollector, GcReq};
    use crate::internal::state::{HandleDebugState, ROLLUP_THRESHOLD};
    use crate::tests::new_test_client;

    #[mz_persist_proc::test(tokio::test(flavor = "multi_thread"))]
    #[cfg_attr(miri, ignore)] // error: unsupported operation: integer-to-pointer casts and `ptr::from_exposed_addr` are not supported with `-Zmiri-strict-provenance`
    async fn apply_unbatched_cmd_truncate(dyncfgs: ConfigUpdates) {
        mz_ore::test::init_logging();

        let client = new_test_client(&dyncfgs).await;
        // set a low rollup threshold so GC/truncation is more aggressive
        client.cfg.set_config(&ROLLUP_THRESHOLD, 5);
        let (mut write, _) = client
            .expect_open::<String, (), u64, i64>(ShardId::new())
            .await;

        // Write a bunch of batches. This should result in a bounded number of
        // live entries in consensus.
        const NUM_BATCHES: u64 = 100;
        for idx in 0..NUM_BATCHES {
            let mut batch = write
                .expect_batch(&[((idx.to_string(), ()), idx, 1)], idx, idx + 1)
                .await;
            // Flush this batch out so the CaA doesn't get inline writes
            // backpressure.
            let cfg = BatchBuilderConfig::new(&client.cfg, write.shard_id());
            batch
                .flush_to_blob(
                    &cfg,
                    &client.metrics.user,
                    &client.isolated_runtime,
                    &write.write_schemas,
                )
                .await;
            let (_, writer_maintenance) = write
                .machine
                .compare_and_append(
                    &batch.into_hollow_batch(),
                    &write.writer_id,
                    &HandleDebugState::default(),
                    (write.cfg.now)(),
                )
                .await
                .unwrap();
            writer_maintenance
                .perform(&write.machine, &write.gc, write.compact.as_ref())
                .await;
        }
        let live_diffs = write
            .machine
            .applier
            .state_versions
            .fetch_all_live_diffs(&write.machine.shard_id())
            .await;
        // Make sure we constructed the key correctly.
        assert!(live_diffs.0.len() > 0);
        // Make sure the number of entries is bounded. (I think we could work
        // out a tighter bound than this, but the point is only that it's
        // bounded).
        let max_live_diffs = 2 * usize::cast_from(NUM_BATCHES.next_power_of_two().trailing_zeros());
        assert!(
            live_diffs.0.len() <= max_live_diffs,
            "{} vs {}",
            live_diffs.0.len(),
            max_live_diffs
        );
    }

    // A regression test for database-issues#4206, where a bug in gc led to an incremental
    // state invariant being violated which resulted in gc being permanently
    // wedged for the shard.
    #[mz_persist_proc::test(tokio::test(flavor = "multi_thread"))]
    #[cfg_attr(miri, ignore)] // error: unsupported operation: integer-to-pointer casts and `ptr::from_exposed_addr` are not supported with `-Zmiri-strict-provenance`
    async fn regression_gc_skipped_req_and_interrupted(dyncfgs: ConfigUpdates) {
        let mut client = new_test_client(&dyncfgs).await;
        let intercept = InterceptHandle::default();
        client.blob = Arc::new(InterceptBlob::new(
            Arc::clone(&client.blob),
            intercept.clone(),
        ));
        let (_, mut read) = client
            .expect_open::<String, String, u64, i64>(ShardId::new())
            .await;

        // Create a new SeqNo
        read.downgrade_since(&Antichain::from_elem(1)).await;
        let new_seqno_since = read.machine.applier.seqno_since();

        // Start a GC in the background for some SeqNo range that is not
        // contiguous compared to the last gc req (in this case, n/a) and then
        // crash when it gets to the blob deletes. In the regression case, this
        // renders the shard permanently un-gc-able.
        assert!(new_seqno_since > SeqNo::minimum());
        intercept.set_post_delete(Some(Arc::new(|_, _| panic!("boom"))));
        let machine = read.machine.clone();
        // Run this in a spawn so we can catch the boom panic
        let gc = spawn(|| "", async move {
            let req = GcReq {
                shard_id: machine.shard_id(),
                new_seqno_since,
            };
            GarbageCollector::gc_and_truncate(&machine, req).await
        });
        // Wait for gc to either panic (regression case) or finish (good case)
        // because it happens to not call blob delete.
        let _ = gc.await;

        // Allow blob deletes to go through and try GC again. In the regression
        // case, this hangs.
        intercept.set_post_delete(None);
        let req = GcReq {
            shard_id: read.machine.shard_id(),
            new_seqno_since,
        };
        let _ = GarbageCollector::gc_and_truncate(&read.machine, req.clone()).await;
    }

    // A regression test for materialize#20776, where a bug meant that compare_and_append
    // would not fetch the latest state after an upper mismatch. This meant that
    // a write that could succeed if retried on the latest state would instead
    // return an UpperMismatch.
    #[mz_persist_proc::test(tokio::test(flavor = "multi_thread"))]
    #[cfg_attr(miri, ignore)] // error: unsupported operation: integer-to-pointer casts and `ptr::from_exposed_addr` are not supported with `-Zmiri-strict-provenance`
    async fn regression_update_state_after_upper_mismatch(dyncfgs: ConfigUpdates) {
        let client = new_test_client(&dyncfgs).await;
        let mut client2 = client.clone();

        // The bug can only happen if the two WriteHandles have separate copies
        // of state, so make sure that each is given its own StateCache.
        let new_state_cache = Arc::new(StateCache::new_no_metrics());
        client2.shared_states = new_state_cache;

        let shard_id = ShardId::new();
        let (mut write1, _) = client.expect_open::<String, (), u64, i64>(shard_id).await;
        let (mut write2, _) = client2.expect_open::<String, (), u64, i64>(shard_id).await;

        let data = [
            (("1".to_owned(), ()), 1, 1),
            (("2".to_owned(), ()), 2, 1),
            (("3".to_owned(), ()), 3, 1),
            (("4".to_owned(), ()), 4, 1),
            (("5".to_owned(), ()), 5, 1),
        ];

        write1.expect_compare_and_append(&data[..1], 0, 2).await;

        // this handle's upper now lags behind. if compare_and_append fails to update
        // state after an upper mismatch then this call would (incorrectly) fail
        write2.expect_compare_and_append(&data[1..2], 2, 3).await;
    }
}

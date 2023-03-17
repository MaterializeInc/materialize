// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::cmp::Ordering;
use std::collections::BTreeMap;
use std::fmt::{Debug, Formatter};
use std::iter::Peekable;
use std::marker::PhantomData;
use std::ops::{ControlFlow, ControlFlow::Break, ControlFlow::Continue};
use std::ops::{Deref, DerefMut};
use std::slice;
use std::sync::Arc;
use std::time::Duration;

use differential_dataflow::lattice::Lattice;
use differential_dataflow::trace::Description;
use mz_ore::cast::CastFrom;
use mz_ore::now::EpochMillis;
use mz_persist::location::SeqNo;
use mz_persist_types::{Codec, Codec64, Opaque};
use semver::Version;
use timely::progress::{Antichain, Timestamp};
use timely::PartialOrder;
use tracing::info;
use uuid::Uuid;

use crate::critical::CriticalReaderId;
use crate::error::{Determinacy, InvalidUsage};
use crate::internal::encoding::parse_id;
use crate::internal::gc::GcReq;
use crate::internal::paths::{PartialBatchKey, PartialRollupKey};
use crate::internal::trace::{ApplyMergeResult, FueledMergeReq, FueledMergeRes, Trace};
use crate::read::LeasedReaderId;
use crate::stats::PartStats;
use crate::write::WriterId;
use crate::{PersistConfig, ShardId};

include!(concat!(
    env!("OUT_DIR"),
    "/mz_persist_client.internal.state.rs"
));

/// A token to disambiguate state commands that could not otherwise be
/// idempotent.
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct IdempotencyToken(pub(crate) [u8; 16]);

impl std::fmt::Display for IdempotencyToken {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "i{}", Uuid::from_bytes(self.0))
    }
}

impl std::fmt::Debug for IdempotencyToken {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "IdempotencyToken({})", Uuid::from_bytes(self.0))
    }
}

impl std::str::FromStr for IdempotencyToken {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        parse_id('i', "IdempotencyToken", s).map(IdempotencyToken)
    }
}

impl IdempotencyToken {
    pub(crate) fn new() -> Self {
        IdempotencyToken(*Uuid::new_v4().as_bytes())
    }
    pub(crate) const SENTINEL: IdempotencyToken = IdempotencyToken([17u8; 16]);
}

#[derive(Clone, Debug, PartialEq)]
pub struct LeasedReaderState<T> {
    /// The seqno capability of this reader.
    pub seqno: SeqNo,
    /// The since capability of this reader.
    pub since: Antichain<T>,
    /// UNIX_EPOCH timestamp (in millis) of this reader's most recent heartbeat
    pub last_heartbeat_timestamp_ms: u64,
    /// Duration (in millis) allowed after [Self::last_heartbeat_timestamp_ms]
    /// after which this reader may be expired
    pub lease_duration_ms: u64,
    /// For debugging.
    pub debug: HandleDebugState,
}

#[derive(Clone, Debug, PartialEq)]
pub struct OpaqueState(pub [u8; 8]);

#[derive(Clone, Debug, PartialEq)]
pub struct CriticalReaderState<T> {
    /// The since capability of this reader.
    pub since: Antichain<T>,
    /// An opaque token matched on by compare_and_downgrade_since.
    pub opaque: OpaqueState,
    /// The [Codec64] used to encode [Self::opaque].
    pub opaque_codec: String,
    /// For debugging.
    pub debug: HandleDebugState,
}

#[derive(Clone, Debug, PartialEq)]
pub struct WriterState<T> {
    /// UNIX_EPOCH timestamp (in millis) of this writer's most recent heartbeat
    pub last_heartbeat_timestamp_ms: u64,
    /// Duration (in millis) allowed after [Self::last_heartbeat_timestamp_ms]
    /// after which this writer may be expired
    pub lease_duration_ms: u64,
    /// The idempotency token of the most recent successful compare_and_append
    /// by this writer.
    pub most_recent_write_token: IdempotencyToken,
    /// The upper of the most recent successful compare_and_append by this
    /// writer.
    pub most_recent_write_upper: Antichain<T>,
    /// For debugging.
    pub debug: HandleDebugState,
}

/// Debugging info for a reader or writer.
#[derive(Clone, Debug, PartialEq)]
pub struct HandleDebugState {
    /// Hostname of the persist user that registered this writer or reader. For
    /// critical readers, this is the _most recent_ registration.
    pub hostname: String,
    /// Plaintext description of this writer or reader's intent.
    pub purpose: String,
}

/// A subset of a [HollowBatch] corresponding 1:1 to a blob.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct HollowBatchPart {
    /// Pointer usable to retrieve the updates.
    pub key: PartialBatchKey,
    /// The encoded size of this part.
    pub encoded_size_bytes: usize,
    /// Aggregate statistics about data contained in this part.
    ///
    /// Stored inside an Arc because HollowBatchPart needs to be cheaply
    /// clone-able.
    pub stats: Option<Arc<PartStats>>,
}

/// A [Batch] but with the updates themselves stored externally.
///
/// [Batch]: differential_dataflow::trace::BatchReader
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct HollowBatch<T> {
    /// Describes the times of the updates in the batch.
    pub desc: Description<T>,
    /// Pointers usable to retrieve the updates.
    pub parts: Vec<HollowBatchPart>,
    /// The number of updates in the batch.
    pub len: usize,
    /// Runs of sequential sorted batch parts, stored as indices into `parts`.
    /// ex.
    /// ```text
    ///     parts=[p1, p2, p3], runs=[]     --> run  is  [p1, p2, p2]
    ///     parts=[p1, p2, p3], runs=[1]    --> runs are [p1] and [p2, p3]
    ///     parts=[p1, p2, p3], runs=[1, 2] --> runs are [p1], [p2], [p3]
    /// ```
    pub runs: Vec<usize>,
}

impl<T: Ord> PartialOrd for HollowBatch<T> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl<T: Ord> Ord for HollowBatch<T> {
    fn cmp(&self, other: &Self) -> Ordering {
        // Deconstruct self and other so we get a compile failure if new fields
        // are added.
        let HollowBatch {
            desc: self_desc,
            parts: self_parts,
            len: self_len,
            runs: self_runs,
        } = self;
        let HollowBatch {
            desc: other_desc,
            parts: other_parts,
            len: other_len,
            runs: other_runs,
        } = other;
        (
            self_desc.lower().elements(),
            self_desc.upper().elements(),
            self_desc.since().elements(),
            self_parts,
            self_len,
            self_runs,
        )
            .cmp(&(
                other_desc.lower().elements(),
                other_desc.upper().elements(),
                other_desc.since().elements(),
                other_parts,
                other_len,
                other_runs,
            ))
    }
}

impl<T> HollowBatch<T> {
    pub(crate) fn runs(&self) -> HollowBatchRunIter<T> {
        HollowBatchRunIter {
            batch: self,
            inner: self.runs.iter().peekable(),
            emitted_implicit: false,
        }
    }
}

pub(crate) struct HollowBatchRunIter<'a, T> {
    batch: &'a HollowBatch<T>,
    inner: Peekable<slice::Iter<'a, usize>>,
    emitted_implicit: bool,
}

impl<'a, T> Iterator for HollowBatchRunIter<'a, T> {
    type Item = &'a [HollowBatchPart];

    fn next(&mut self) -> Option<Self::Item> {
        if self.batch.parts.is_empty() {
            return None;
        }

        if !self.emitted_implicit {
            self.emitted_implicit = true;
            return Some(match self.inner.peek() {
                None => &self.batch.parts,
                Some(run_end) => &self.batch.parts[0..**run_end],
            });
        }

        if let Some(run_start) = self.inner.next() {
            return Some(match self.inner.peek() {
                Some(run_end) => &self.batch.parts[*run_start..**run_end],
                None => &self.batch.parts[*run_start..],
            });
        }

        None
    }
}

/// A pointer to a rollup stored externally.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct HollowRollup {
    /// Pointer usable to retrieve the rollup.
    pub key: PartialRollupKey,
    /// The encoded size of this rollup, if known.
    pub encoded_size_bytes: Option<usize>,
}

/// A pointer to a blob stored externally.
#[derive(Debug)]
pub enum HollowBlobRef<'a, T> {
    Batch(&'a HollowBatch<T>),
    Rollup(&'a HollowRollup),
}

/// A sentinel for a state transition that was a no-op.
///
/// Critically, this also indicates that the no-op state transition was not
/// committed through compare_and_append and thus is _not linearized_.
#[derive(Debug)]
#[cfg_attr(any(test, debug_assertions), derive(PartialEq))]
pub struct NoOpStateTransition<T>(pub T);

// TODO: Document invariants.
#[derive(Debug, Clone)]
#[cfg_attr(any(test, debug_assertions), derive(PartialEq))]
pub struct StateCollections<T> {
    // - Invariant: `<= all reader.since`
    // - Invariant: Doesn't regress across state versions.
    pub(crate) last_gc_req: SeqNo,

    // - Invariant: There is a rollup with `seqno <= self.seqno_since`.
    pub(crate) rollups: BTreeMap<SeqNo, HollowRollup>,

    pub(crate) leased_readers: BTreeMap<LeasedReaderId, LeasedReaderState<T>>,
    pub(crate) critical_readers: BTreeMap<CriticalReaderId, CriticalReaderState<T>>,
    pub(crate) writers: BTreeMap<WriterId, WriterState<T>>,

    // - Invariant: `trace.since == meet(all reader.since)`
    // - Invariant: `trace.since` doesn't regress across state versions.
    // - Invariant: `trace.upper` doesn't regress across state versions.
    // - Invariant: `trace` upholds its own invariants.
    pub(crate) trace: Trace<T>,
}

#[derive(Debug)]
#[cfg_attr(test, derive(PartialEq))]
pub enum CompareAndAppendBreak<T> {
    AlreadyCommitted,
    Upper {
        shard_upper: Antichain<T>,
        writer_upper: Antichain<T>,
    },
    InvalidUsage(InvalidUsage<T>),
}

impl<T> StateCollections<T>
where
    T: Timestamp + Lattice + Codec64,
{
    pub fn add_and_remove_rollups(
        &mut self,
        add_rollup: (SeqNo, &HollowRollup),
        remove_rollups: &[(SeqNo, PartialRollupKey)],
    ) -> ControlFlow<NoOpStateTransition<bool>, bool> {
        let (rollup_seqno, rollup) = add_rollup;
        let applied = match self.rollups.get(&rollup_seqno) {
            Some(x) => x.key == rollup.key,
            None => {
                self.rollups.insert(rollup_seqno, rollup.to_owned());
                true
            }
        };
        for (seqno, key) in remove_rollups {
            let removed_key = self.rollups.remove(seqno);
            debug_assert!(
                removed_key.as_ref().map_or(true, |x| &x.key == key),
                "{} vs {:?}",
                key,
                removed_key
            );
        }
        // This state transition is a no-op if applied is false and none of
        // remove_rollups existed, but we still commit the state change so that
        // this gets linearized (maybe we're looking at old state).
        Continue(applied)
    }

    pub fn register_leased_reader(
        &mut self,
        hostname: &str,
        reader_id: &LeasedReaderId,
        purpose: &str,
        seqno: SeqNo,
        lease_duration: Duration,
        heartbeat_timestamp_ms: u64,
    ) -> ControlFlow<NoOpStateTransition<LeasedReaderState<T>>, LeasedReaderState<T>> {
        let reader_state = LeasedReaderState {
            debug: HandleDebugState {
                hostname: hostname.to_owned(),
                purpose: purpose.to_owned(),
            },
            seqno,
            since: self.trace.since().clone(),
            last_heartbeat_timestamp_ms: heartbeat_timestamp_ms,
            lease_duration_ms: u64::try_from(lease_duration.as_millis())
                .expect("lease duration as millis should fit within u64"),
        };

        // If the shard-global upper and since are both the empty antichain,
        // then no further writes can ever commit and no further reads can be
        // served. Optimize this by no-op-ing reader registration so that we can
        // settle the shard into a final unchanging tombstone state.
        if self.is_tombstone() {
            return Break(NoOpStateTransition(reader_state));
        }

        // TODO: Handle if the reader or writer already exists.
        self.leased_readers
            .insert(reader_id.clone(), reader_state.clone());
        Continue(reader_state)
    }

    pub fn register_critical_reader<O: Opaque + Codec64>(
        &mut self,
        hostname: &str,
        reader_id: &CriticalReaderId,
        purpose: &str,
    ) -> ControlFlow<NoOpStateTransition<CriticalReaderState<T>>, CriticalReaderState<T>> {
        let state = CriticalReaderState {
            debug: HandleDebugState {
                hostname: hostname.to_owned(),
                purpose: purpose.to_owned(),
            },
            since: self.trace.since().clone(),
            opaque: OpaqueState(Codec64::encode(&O::initial())),
            opaque_codec: O::codec_name(),
        };

        // We expire all readers if the upper and since both advance to the
        // empty antichain. Gracefully handle this. At the same time,
        // short-circuit the cmd application so we don't needlessly create new
        // SeqNos.
        if self.is_tombstone() {
            return Break(NoOpStateTransition(state));
        }

        let state = match self.critical_readers.get_mut(reader_id) {
            Some(existing_state) => {
                existing_state.debug = state.debug;
                existing_state.clone()
            }
            None => {
                self.critical_readers
                    .insert(reader_id.clone(), state.clone());
                state
            }
        };
        Continue(state)
    }

    pub fn register_writer(
        &mut self,
        hostname: &str,
        writer_id: &WriterId,
        purpose: &str,
        lease_duration: Duration,
        heartbeat_timestamp_ms: u64,
    ) -> ControlFlow<NoOpStateTransition<(Upper<T>, WriterState<T>)>, (Upper<T>, WriterState<T>)>
    {
        let upper = Upper(self.trace.upper().clone());
        let writer_state = WriterState {
            debug: HandleDebugState {
                hostname: hostname.to_owned(),
                purpose: purpose.to_owned(),
            },
            last_heartbeat_timestamp_ms: heartbeat_timestamp_ms,
            lease_duration_ms: u64::try_from(lease_duration.as_millis())
                .expect("lease duration as millis must fit within u64"),
            most_recent_write_token: IdempotencyToken::SENTINEL,
            most_recent_write_upper: Antichain::from_elem(T::minimum()),
        };

        // If the shard-global upper and since are both the empty antichain,
        // then no further writes can ever commit and no further reads can be
        // served. Optimize this by no-op-ing writer registration so that we can
        // settle the shard into a final unchanging tombstone state.
        if self.is_tombstone() {
            return Break(NoOpStateTransition((upper, writer_state)));
        }

        // TODO: Handle if the reader or writer already exists.
        self.writers.insert(writer_id.clone(), writer_state.clone());
        Continue((Upper(self.trace.upper().clone()), writer_state))
    }

    pub fn compare_and_append(
        &mut self,
        batch: &HollowBatch<T>,
        writer_id: &WriterId,
        heartbeat_timestamp_ms: u64,
        idempotency_token: &IdempotencyToken,
    ) -> ControlFlow<CompareAndAppendBreak<T>, Vec<FueledMergeReq<T>>> {
        // We expire all writers if the upper and since both advance to the
        // empty antichain. Gracefully handle this. At the same time,
        // short-circuit the cmd application so we don't needlessly create new
        // SeqNos.
        if self.is_tombstone() {
            assert_eq!(self.trace.upper(), &Antichain::new());
            return Break(CompareAndAppendBreak::Upper {
                shard_upper: Antichain::new(),
                // This writer might have been registered before the shard upper
                // was advanced, which would make this pessimistic in the
                // Indeterminate handling of compare_and_append at the machine
                // level, but that's fine.
                writer_upper: Antichain::new(),
            });
        }

        let writer_state = match self.writers.get_mut(writer_id) {
            Some(x) => x,
            None => {
                return Break(CompareAndAppendBreak::InvalidUsage(
                    InvalidUsage::UnknownWriter(writer_id.clone()),
                ))
            }
        };

        if PartialOrder::less_than(batch.desc.upper(), batch.desc.lower()) {
            return Break(CompareAndAppendBreak::InvalidUsage(
                InvalidUsage::InvalidBounds {
                    lower: batch.desc.lower().clone(),
                    upper: batch.desc.upper().clone(),
                },
            ));
        }

        // If the time interval is empty, the list of updates must also be
        // empty.
        if batch.desc.upper() == batch.desc.lower() && !batch.parts.is_empty() {
            return Break(CompareAndAppendBreak::InvalidUsage(
                InvalidUsage::InvalidEmptyTimeInterval {
                    lower: batch.desc.lower().clone(),
                    upper: batch.desc.upper().clone(),
                    keys: batch.parts.iter().map(|x| x.key.clone()).collect(),
                },
            ));
        }

        if idempotency_token == &writer_state.most_recent_write_token {
            // If the last write had the same idempotency_token, then this must
            // have already committed. Sanity check that the most recent write
            // upper matches and that the shard upper is at least the write
            // upper, if it's not something very suspect is going on.
            assert_eq!(batch.desc.upper(), &writer_state.most_recent_write_upper);
            assert!(
                PartialOrder::less_equal(batch.desc.upper(), self.trace.upper()),
                "{:?} vs {:?}",
                batch.desc.upper(),
                self.trace.upper()
            );
            return Break(CompareAndAppendBreak::AlreadyCommitted);
        }

        let shard_upper = self.trace.upper();
        if shard_upper != batch.desc.lower() {
            return Break(CompareAndAppendBreak::Upper {
                shard_upper: shard_upper.clone(),
                writer_upper: writer_state.most_recent_write_upper.clone(),
            });
        }

        let merge_reqs = if batch.desc.upper() != batch.desc.lower() {
            self.trace.push_batch(batch.clone())
        } else {
            Vec::new()
        };
        debug_assert_eq!(self.trace.upper(), batch.desc.upper());
        writer_state.most_recent_write_token = idempotency_token.clone();
        // The writer's most recent upper should only go forward.
        assert!(
            PartialOrder::less_equal(&writer_state.most_recent_write_upper, batch.desc.upper()),
            "{:?} vs {:?}",
            &writer_state.most_recent_write_upper,
            batch.desc.upper()
        );
        writer_state.most_recent_write_upper = batch.desc.upper().clone();

        // Also use this as an opportunity to heartbeat the writer
        writer_state.last_heartbeat_timestamp_ms = std::cmp::max(
            heartbeat_timestamp_ms,
            writer_state.last_heartbeat_timestamp_ms,
        );

        Continue(merge_reqs)
    }

    pub fn apply_merge_res(
        &mut self,
        res: &FueledMergeRes<T>,
    ) -> ControlFlow<NoOpStateTransition<ApplyMergeResult>, ApplyMergeResult> {
        // We expire all writers if the upper and since both advance to the
        // empty antichain. Gracefully handle this. At the same time,
        // short-circuit the cmd application so we don't needlessly create new
        // SeqNos.
        if self.is_tombstone() {
            return Break(NoOpStateTransition(ApplyMergeResult::NotAppliedNoMatch));
        }

        let apply_merge_result = self.trace.apply_merge_res(res);
        Continue(apply_merge_result)
    }

    pub fn downgrade_since(
        &mut self,
        reader_id: &LeasedReaderId,
        seqno: SeqNo,
        outstanding_seqno: Option<SeqNo>,
        new_since: &Antichain<T>,
        heartbeat_timestamp_ms: u64,
    ) -> ControlFlow<NoOpStateTransition<Since<T>>, Since<T>> {
        // We expire all readers if the upper and since both advance to the
        // empty antichain. Gracefully handle this. At the same time,
        // short-circuit the cmd application so we don't needlessly create new
        // SeqNos.
        if self.is_tombstone() {
            return Break(NoOpStateTransition(Since(Antichain::new())));
        }

        let reader_state = self.leased_reader(reader_id);

        // Also use this as an opportunity to heartbeat the reader and downgrade
        // the seqno capability.
        reader_state.last_heartbeat_timestamp_ms = std::cmp::max(
            heartbeat_timestamp_ms,
            reader_state.last_heartbeat_timestamp_ms,
        );

        let seqno = match outstanding_seqno {
            Some(outstanding_seqno) => {
                assert!(
                    outstanding_seqno >= reader_state.seqno,
                    "SeqNos cannot go backward; however, oldest leased SeqNo ({:?}) \
                    is behind current reader_state ({:?})",
                    outstanding_seqno,
                    reader_state.seqno,
                );
                std::cmp::min(outstanding_seqno, seqno)
            }
            None => seqno,
        };

        reader_state.seqno = seqno;

        let reader_current_since = if PartialOrder::less_than(&reader_state.since, new_since) {
            reader_state.since.clone_from(new_since);
            self.update_since();
            new_since.clone()
        } else {
            // No-op, but still commit the state change so that this gets
            // linearized.
            reader_state.since.clone()
        };

        Continue(Since(reader_current_since))
    }

    pub fn compare_and_downgrade_since<O: Opaque + Codec64>(
        &mut self,
        reader_id: &CriticalReaderId,
        expected_opaque: &O,
        (new_opaque, new_since): (&O, &Antichain<T>),
    ) -> ControlFlow<
        NoOpStateTransition<Result<Since<T>, (O, Since<T>)>>,
        Result<Since<T>, (O, Since<T>)>,
    > {
        // We expire all readers if the upper and since both advance to the
        // empty antichain. Gracefully handle this. At the same time,
        // short-circuit the cmd application so we don't needlessly create new
        // SeqNos.
        if self.is_tombstone() {
            // Match the idempotence behavior below of ignoring the token if
            // since is already advanced enough (in this case, because it's a
            // tombstone, we know it's the empty antichain).
            return Break(NoOpStateTransition(Ok(Since(Antichain::new()))));
        }

        let reader_state = self.critical_reader(reader_id);
        assert_eq!(reader_state.opaque_codec, O::codec_name());

        if &O::decode(reader_state.opaque.0) != expected_opaque {
            // No-op, but still commit the state change so that this gets
            // linearized.
            return Continue(Err((
                Codec64::decode(reader_state.opaque.0),
                Since(reader_state.since.clone()),
            )));
        }

        if PartialOrder::less_equal(&reader_state.since, new_since) {
            reader_state.since = new_since.clone();
            reader_state.opaque = OpaqueState(Codec64::encode(new_opaque));
            self.update_since();
            Continue(Ok(Since(new_since.clone())))
        } else {
            // no work to be done -- the reader state's `since` is already sufficiently
            // advanced. we may someday need to revisit this branch when it's possible
            // for two `since` frontiers to be incomparable.
            Continue(Ok(Since(reader_state.since.clone())))
        }
    }

    pub fn heartbeat_leased_reader(
        &mut self,
        reader_id: &LeasedReaderId,
        heartbeat_timestamp_ms: u64,
    ) -> ControlFlow<NoOpStateTransition<bool>, bool> {
        // We expire all readers if the upper and since both advance to the
        // empty antichain. Gracefully handle this. At the same time,
        // short-circuit the cmd application so we don't needlessly create new
        // SeqNos.
        if self.is_tombstone() {
            return Break(NoOpStateTransition(false));
        }

        match self.leased_readers.get_mut(reader_id) {
            Some(reader_state) => {
                reader_state.last_heartbeat_timestamp_ms = std::cmp::max(
                    heartbeat_timestamp_ms,
                    reader_state.last_heartbeat_timestamp_ms,
                );
                Continue(true)
            }
            // No-op, but we still commit the state change so that this gets
            // linearized (maybe we're looking at old state).
            None => Continue(false),
        }
    }

    pub fn expire_leased_reader(
        &mut self,
        reader_id: &LeasedReaderId,
    ) -> ControlFlow<NoOpStateTransition<bool>, bool> {
        // We expire all readers if the upper and since both advance to the
        // empty antichain. Gracefully handle this. At the same time,
        // short-circuit the cmd application so we don't needlessly create new
        // SeqNos.
        if self.is_tombstone() {
            return Break(NoOpStateTransition(false));
        }

        info!("Expiring reader id: {}", reader_id);
        let existed = self.leased_readers.remove(reader_id).is_some();
        if existed {
            // TODO: Re-enable this once we have #15511.
            //
            // Temporarily disabling this because we think it might be the cause
            // of the remap since bug. Specifically, a clusterd process has a
            // ReadHandle for maintaining the once and one inside a Listen. If
            // we crash and stay down for longer than the read lease duration,
            // it's possible that an expiry of them both in quick succession
            // jumps the since forward to the Listen one.
            //
            // Don't forget to update the downgrade_since when this gets
            // switched back on.
            //
            // self.update_since();
        }
        // No-op if existed is false, but still commit the state change so that
        // this gets linearized.
        Continue(existed)
    }

    pub fn expire_critical_reader(
        &mut self,
        reader_id: &CriticalReaderId,
    ) -> ControlFlow<NoOpStateTransition<bool>, bool> {
        // We expire all readers if the upper and since both advance to the
        // empty antichain. Gracefully handle this. At the same time,
        // short-circuit the cmd application so we don't needlessly create new
        // SeqNos.
        if self.is_tombstone() {
            return Break(NoOpStateTransition(false));
        }

        let existed = self.critical_readers.remove(reader_id).is_some();
        if existed {
            // TODO: Re-enable this once we have #15511.
            //
            // Temporarily disabling this because we think it might be the cause
            // of the remap since bug. Specifically, a clusterd process has a
            // ReadHandle for maintaining the once and one inside a Listen. If
            // we crash and stay down for longer than the read lease duration,
            // it's possible that an expiry of them both in quick succession
            // jumps the since forward to the Listen one.
            //
            // Don't forget to update the downgrade_since when this gets
            // switched back on.
            //
            // self.update_since();
        }
        // This state transition is a no-op if existed is false, but we still
        // commit the state change so that this gets linearized (maybe we're
        // looking at old state).
        Continue(existed)
    }

    pub fn heartbeat_writer(
        &mut self,
        writer_id: &WriterId,
        heartbeat_timestamp_ms: u64,
    ) -> ControlFlow<NoOpStateTransition<bool>, bool> {
        // We expire all writers if the upper and since both advance to the
        // empty antichain. Gracefully handle this. At the same time,
        // short-circuit the cmd application so we don't needlessly create new
        // SeqNos.
        if self.is_tombstone() {
            return Break(NoOpStateTransition(false));
        }

        match self.writers.get_mut(writer_id) {
            Some(writer_state) => {
                writer_state.last_heartbeat_timestamp_ms = std::cmp::max(
                    heartbeat_timestamp_ms,
                    writer_state.last_heartbeat_timestamp_ms,
                );
                Continue(true)
            }
            // No-op, but we still commit the state change so that this gets
            // linearized (maybe we're looking at old state).
            None => Continue(false),
        }
    }

    pub fn expire_writer(
        &mut self,
        writer_id: &WriterId,
    ) -> ControlFlow<NoOpStateTransition<bool>, bool> {
        // We expire all writers if the upper and since both advance to the
        // empty antichain. Gracefully handle this. At the same time,
        // short-circuit the cmd application so we don't needlessly create new
        // SeqNos.
        if self.is_tombstone() {
            return Break(NoOpStateTransition(false));
        }

        let existed = self.writers.remove(writer_id).is_some();
        // This state transition is a no-op if existed is false, but we still
        // commit the state change so that this gets linearized (maybe we're
        // looking at old state).
        Continue(existed)
    }

    fn leased_reader(&mut self, id: &LeasedReaderId) -> &mut LeasedReaderState<T> {
        self.leased_readers
            .get_mut(id)
            // The only (tm) ways to hit this are (1) inventing a LeasedReaderId
            // instead of getting it from Register or (2) if a lease expired.
            // (1) is a gross mis-use and (2) may happen if a reader did not get
            // to heartbeat for a long time. Readers are expected to
            // heartbeat/downgrade their since regularly.
            .unwrap_or_else(|| {
                panic!(
                    "LeasedReaderId({}) was expired due to inactivity. Did the machine go to sleep?",
                    id
                )
            })
    }

    fn critical_reader(&mut self, id: &CriticalReaderId) -> &mut CriticalReaderState<T> {
        self.critical_readers
            .get_mut(id)
            .unwrap_or_else(|| {
                panic!(
                    "Unknown CriticalReaderId({}). It was either never registered, or has been manually expired.",
                    id
                )
            })
    }

    fn update_since(&mut self) {
        let mut sinces_iter = self
            .leased_readers
            .values()
            .map(|x| &x.since)
            .chain(self.critical_readers.values().map(|x| &x.since));
        let mut since = match sinces_iter.next() {
            Some(since) => since.clone(),
            None => {
                // If there are no current readers, leave `since` unchanged so
                // it doesn't regress.
                return;
            }
        };
        while let Some(s) = sinces_iter.next() {
            since.meet_assign(s);
        }
        self.trace.downgrade_since(&since);
    }

    fn tombstone_batch() -> HollowBatch<T> {
        HollowBatch {
            desc: Description::new(
                Antichain::from_elem(T::minimum()),
                Antichain::new(),
                Antichain::new(),
            ),
            parts: Vec::new(),
            runs: Vec::new(),
            len: 0,
        }
    }

    pub(crate) fn is_tombstone(&self) -> bool {
        if self.trace.upper().is_empty()
            && self.trace.since().is_empty()
            && self.writers.is_empty()
            && self.leased_readers.is_empty()
            && self.critical_readers.is_empty()
        {
            // Gate this more expensive check behind the cheaper is_empty ones.
            let mut batches = Vec::new();
            self.trace.map_batches(|b| batches.push(b));
            if batches.len() == 1 && batches[0] == &Self::tombstone_batch() {
                return true;
            }
        }
        false
    }

    pub fn become_tombstone(&mut self) -> ControlFlow<NoOpStateTransition<()>, ()> {
        assert_eq!(self.trace.upper(), &Antichain::new());
        assert_eq!(self.trace.since(), &Antichain::new());

        if self.is_tombstone() {
            return Break(NoOpStateTransition(()));
        }

        self.writers.clear();
        self.leased_readers.clear();
        self.critical_readers.clear();
        let mut new_trace = Trace::default();
        new_trace.downgrade_since(&Antichain::new());
        let merge_reqs = new_trace.push_batch(Self::tombstone_batch());
        assert_eq!(merge_reqs, Vec::new());
        self.trace = new_trace;

        debug_assert!(self.is_tombstone());
        Continue(())
    }
}

// TODO: Document invariants.
#[derive(Debug)]
#[cfg_attr(any(test, debug_assertions), derive(PartialEq))]
pub struct State<T> {
    pub(crate) applier_version: semver::Version,
    pub(crate) shard_id: ShardId,

    pub(crate) seqno: SeqNo,
    /// A strictly increasing wall time of when this state was written, in
    /// milliseconds since the unix epoch.
    pub(crate) walltime_ms: u64,
    /// Hostname of the persist user that created this version of state. For
    /// debugging.
    pub(crate) hostname: String,
    pub(crate) collections: StateCollections<T>,
}

/// A newtype wrapper of State that guarantees the K, V, and D codecs match the
/// ones in durable storage.
pub struct TypedState<K, V, T, D> {
    pub(crate) state: State<T>,

    // According to the docs, PhantomData is to "mark things that act like they
    // own a T". State doesn't actually own K, V, or D, just the ability to
    // produce them. Using the `fn() -> T` pattern gets us the same variance as
    // T [1], but also allows State to correctly derive Send+Sync.
    //
    // [1]:
    //     https://doc.rust-lang.org/nomicon/phantom-data.html#table-of-phantomdata-patterns
    pub(crate) _phantom: PhantomData<fn() -> (K, V, D)>,
}

impl<K, V, T: Clone, D> TypedState<K, V, T, D> {
    pub(crate) fn clone(&self, applier_version: Version, hostname: String) -> Self {
        TypedState {
            state: State {
                applier_version,
                shard_id: self.shard_id.clone(),
                seqno: self.seqno.clone(),
                walltime_ms: self.walltime_ms,
                hostname,
                collections: self.collections.clone(),
            },
            _phantom: PhantomData,
        }
    }
}

impl<K, V, T: Debug, D> Debug for TypedState<K, V, T, D> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        // Deconstruct self so we get a compile failure if new fields
        // are added.
        let TypedState { state, _phantom } = self;
        f.debug_struct("TypedState").field("state", state).finish()
    }
}

// Impl PartialEq regardless of the type params.
#[cfg(any(test, debug_assertions))]
impl<K, V, T: PartialEq, D> PartialEq for TypedState<K, V, T, D> {
    fn eq(&self, other: &Self) -> bool {
        // Deconstruct self and other so we get a compile failure if new fields
        // are added.
        let TypedState {
            state: self_state,
            _phantom,
        } = self;
        let TypedState {
            state: other_state,
            _phantom,
        } = other;
        self_state == other_state
    }
}

impl<K, V, T, D> Deref for TypedState<K, V, T, D> {
    type Target = State<T>;

    fn deref(&self) -> &Self::Target {
        &self.state
    }
}

impl<K, V, T, D> DerefMut for TypedState<K, V, T, D> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.state
    }
}

impl<K, V, T, D> TypedState<K, V, T, D>
where
    K: Codec,
    V: Codec,
    T: Timestamp + Lattice + Codec64,
    D: Codec64,
{
    pub fn new(
        applier_version: Version,
        shard_id: ShardId,
        hostname: String,
        walltime_ms: u64,
    ) -> Self {
        let state = State {
            applier_version,
            shard_id,
            seqno: SeqNo::minimum(),
            walltime_ms,
            hostname,
            collections: StateCollections {
                last_gc_req: SeqNo::minimum(),
                rollups: BTreeMap::new(),
                leased_readers: BTreeMap::new(),
                critical_readers: BTreeMap::new(),
                writers: BTreeMap::new(),
                trace: Trace::default(),
            },
        };
        TypedState {
            state,
            _phantom: PhantomData,
        }
    }

    pub fn clone_apply<R, E, WorkFn>(
        &self,
        cfg: &PersistConfig,
        work_fn: &mut WorkFn,
    ) -> ControlFlow<E, (R, Self)>
    where
        WorkFn: FnMut(SeqNo, &PersistConfig, &mut StateCollections<T>) -> ControlFlow<E, R>,
    {
        let mut new_state = State {
            applier_version: cfg.build_version.clone(),
            shard_id: self.shard_id,
            seqno: self.seqno.next(),
            walltime_ms: (cfg.now)(),
            hostname: cfg.hostname.clone(),
            collections: self.collections.clone(),
        };
        // Make sure walltime_ms is strictly increasing, in case clocks are
        // offset.
        if new_state.walltime_ms <= self.walltime_ms {
            new_state.walltime_ms = self.walltime_ms + 1;
        }

        let work_ret = work_fn(new_state.seqno, cfg, &mut new_state.collections)?;
        let new_state = TypedState {
            state: new_state,
            _phantom: PhantomData,
        };
        Continue((work_ret, new_state))
    }
}

impl<T> State<T>
where
    T: Timestamp + Lattice + Codec64,
{
    pub fn shard_id(&self) -> ShardId {
        self.shard_id
    }

    pub fn seqno(&self) -> SeqNo {
        self.seqno
    }

    pub fn since(&self) -> &Antichain<T> {
        self.collections.trace.since()
    }

    pub fn upper(&self) -> &Antichain<T> {
        self.collections.trace.upper()
    }

    pub fn batch_part_count(&self) -> usize {
        self.collections.trace.num_batch_parts()
    }

    pub fn num_updates(&self) -> usize {
        self.collections.trace.num_updates()
    }

    pub fn size_metrics(&self) -> StateSizeMetrics {
        let mut ret = StateSizeMetrics::default();
        self.map_blobs(|x| match x {
            HollowBlobRef::Batch(x) => {
                let mut batch_size = 0;
                for x in x.parts.iter() {
                    batch_size += x.encoded_size_bytes;
                }
                ret.largest_batch_bytes = std::cmp::max(ret.largest_batch_bytes, batch_size);
                ret.state_batches_bytes += batch_size;
            }
            HollowBlobRef::Rollup(x) => {
                ret.state_rollups_bytes += x.encoded_size_bytes.unwrap_or_default()
            }
        });
        ret
    }

    pub fn latest_rollup(&self) -> (&SeqNo, &HollowRollup) {
        // We maintain the invariant that every version of state has at least
        // one rollup.
        self.collections
            .rollups
            .iter()
            .rev()
            .next()
            .expect("State should have at least one rollup if seqno > minimum")
    }

    pub(crate) fn seqno_since(&self) -> SeqNo {
        let mut seqno_since = self.seqno;
        for cap in self.collections.leased_readers.values() {
            seqno_since = std::cmp::min(seqno_since, cap.seqno);
        }
        // critical_readers don't hold a seqno capability.
        seqno_since
    }

    // Returns whether the cmd proposing this state has been selected to perform
    // background garbage collection work.
    //
    // If it was selected, this information is recorded in the state itself for
    // commit along with the cmd's state transition. This helps us to avoid
    // redundant work.
    //
    // Correctness does not depend on a gc assignment being executed, nor on
    // them being executed in the order they are given. But it is expected that
    // gc assignments are best-effort respected. In practice, cmds like
    // register_foo or expire_foo, where it would be awkward, ignore gc.
    pub fn maybe_gc(&mut self, is_write: bool) -> Option<GcReq> {
        // This is an arbitrary-ish threshold that scales with seqno, but never
        // gets particularly big. It probably could be much bigger and certainly
        // could use a tuning pass at some point.
        let gc_threshold = std::cmp::max(
            1,
            u64::from(self.seqno.0.next_power_of_two().trailing_zeros()),
        );
        let new_seqno_since = self.seqno_since();
        let should_gc = new_seqno_since
            .0
            .saturating_sub(self.collections.last_gc_req.0)
            >= gc_threshold;
        // Assign GC traffic preferentially to writers, falling back to anyone
        // generating new state versions if there are no writers.
        let should_gc = should_gc && (is_write || self.collections.writers.is_empty());
        // The whole point of a tombstone is that we forever keep exactly one
        // cheap, unchanging version in consensus. Force GC to run on tombstone
        // shards to make sure we clean up the final few versions before the
        // tombstone.
        //
        // However, because we write a rollup as of SeqNo X and then link it in
        // using a state transition (in this case from X to X+1), the minimum
        // number of live diffs is actually two. Detect when we're in this
        // minimal two diff state and stop the (otherwise) infinite iteration.
        let tombstone_needs_rollup = self.collections.is_tombstone() && {
            let (latest_rollup_seqno, _) = self.latest_rollup();
            latest_rollup_seqno.next() < self.seqno
        };
        let should_gc = should_gc || tombstone_needs_rollup;
        if should_gc {
            self.collections.last_gc_req = new_seqno_since;
            Some(GcReq {
                shard_id: self.shard_id,
                new_seqno_since,
            })
        } else {
            None
        }
    }

    /// Return the number of gc-ineligible state versions.
    pub fn seqnos_held(&self) -> usize {
        usize::cast_from(self.seqno.0.saturating_sub(self.seqno_since().0))
    }

    /// Expire all readers and writers up to the given walltime_ms.
    pub fn expire_at(&mut self, walltime_ms: EpochMillis) -> ExpiryMetrics {
        let mut metrics = ExpiryMetrics::default();
        let shard_id = self.shard_id();
        self.collections.leased_readers.retain(|k, v| {
            let retain = v.last_heartbeat_timestamp_ms + v.lease_duration_ms >= walltime_ms;
            if !retain {
                info!("Force expiring reader ({k}) of shard ({shard_id}) due to inactivity");
                metrics.readers_expired += 1;
            }
            retain
        });
        // critical_readers don't need forced expiration. (In fact, that's the point!)
        self.collections.writers.retain(|k, v| {
            let retain = (v.last_heartbeat_timestamp_ms + v.lease_duration_ms) >= walltime_ms;
            if !retain {
                info!("Force expiring writer ({k}) of shard ({shard_id}) due to inactivity");
                // We don't track writer expiration metrics yet.
            }
            retain
        });
        metrics
    }

    /// Returns the batches that contain updates up to (and including) the given `as_of`. The
    /// result `Vec` contains blob keys, along with a [`Description`] of what updates in the
    /// referenced parts are valid to read.
    pub fn snapshot(
        &self,
        as_of: &Antichain<T>,
    ) -> Result<Result<Vec<HollowBatch<T>>, Upper<T>>, Since<T>> {
        if PartialOrder::less_than(as_of, self.collections.trace.since()) {
            return Err(Since(self.collections.trace.since().clone()));
        }
        let upper = self.collections.trace.upper();
        if PartialOrder::less_equal(upper, as_of) {
            return Ok(Err(Upper(upper.clone())));
        }

        let mut batches = Vec::new();
        self.collections.trace.map_batches(|b| {
            if PartialOrder::less_than(as_of, b.desc.lower()) {
                return;
            }
            batches.push(b.clone());
        });
        Ok(Ok(batches))
    }

    // NB: Unlike the other methods here, this one is read-only.
    pub fn verify_listen(&self, as_of: &Antichain<T>) -> Result<Result<(), Upper<T>>, Since<T>> {
        if PartialOrder::less_than(as_of, self.collections.trace.since()) {
            return Err(Since(self.collections.trace.since().clone()));
        }
        let upper = self.collections.trace.upper();
        if PartialOrder::less_equal(upper, as_of) {
            return Ok(Err(Upper(upper.clone())));
        }
        Ok(Ok(()))
    }

    pub fn next_listen_batch(&self, frontier: &Antichain<T>) -> Option<HollowBatch<T>> {
        // TODO: Avoid the O(n^2) here: `next_listen_batch` is called once per
        // batch and this iterates through all batches to find the next one.
        let mut ret = None;
        self.collections.trace.map_batches(|b| {
            if ret.is_some() {
                return;
            }
            if PartialOrder::less_equal(b.desc.lower(), frontier)
                && PartialOrder::less_than(frontier, b.desc.upper())
            {
                ret = Some(b.clone());
            }
        });
        ret
    }

    pub fn need_rollup(&self) -> Option<SeqNo> {
        let (latest_rollup_seqno, _) = self.latest_rollup();
        if self.seqno.0.saturating_sub(latest_rollup_seqno.0) > PersistConfig::NEED_ROLLUP_THRESHOLD
        {
            Some(self.seqno)
        } else {
            None
        }
    }

    pub(crate) fn map_blobs<F: for<'a> FnMut(HollowBlobRef<'a, T>)>(&self, mut f: F) {
        self.collections
            .trace
            .map_batches(|x| f(HollowBlobRef::Batch(x)));
        for x in self.collections.rollups.values() {
            f(HollowBlobRef::Rollup(x));
        }
    }
}

#[derive(Debug, Default)]
pub struct StateSizeMetrics {
    pub largest_batch_bytes: usize,
    pub state_batches_bytes: usize,
    pub state_rollups_bytes: usize,
}

#[derive(Default)]
pub struct ExpiryMetrics {
    pub(crate) readers_expired: usize,
}

/// Wrapper for Antichain that represents a Since
#[derive(Debug, Clone, PartialEq)]
pub struct Since<T>(pub Antichain<T>);

// When used as an error, Since is determinate.
impl<T> Determinacy for Since<T> {
    const DETERMINANT: bool = true;
}

/// Wrapper for Antichain that represents an Upper
#[derive(Debug, PartialEq)]
pub struct Upper<T>(pub Antichain<T>);

// When used as an error, Upper is determinate.
impl<T> Determinacy for Upper<T> {
    const DETERMINANT: bool = true;
}

#[cfg(test)]
mod tests {
    use mz_build_info::DUMMY_BUILD_INFO;
    use mz_ore::now::SYSTEM_TIME;

    use super::*;

    use crate::InvalidUsage::{InvalidBounds, InvalidEmptyTimeInterval};

    fn hollow<T: Timestamp>(lower: T, upper: T, keys: &[&str], len: usize) -> HollowBatch<T> {
        HollowBatch {
            desc: Description::new(
                Antichain::from_elem(lower),
                Antichain::from_elem(upper),
                Antichain::from_elem(T::minimum()),
            ),
            parts: keys
                .iter()
                .map(|x| HollowBatchPart {
                    key: PartialBatchKey((*x).to_owned()),
                    encoded_size_bytes: 0,
                    stats: None,
                })
                .collect(),
            len,
            runs: vec![],
        }
    }

    #[test]
    fn downgrade_since() {
        let mut state = TypedState::<(), (), u64, i64>::new(
            DUMMY_BUILD_INFO.semver_version(),
            ShardId::new(),
            "".to_owned(),
            0,
        );
        let reader = LeasedReaderId::new();
        let seqno = SeqNo::minimum();
        let now = SYSTEM_TIME.clone();
        let _ = state.collections.register_leased_reader(
            "",
            &reader,
            "",
            seqno,
            Duration::from_secs(10),
            now(),
        );

        // The shard global since == 0 initially.
        assert_eq!(state.collections.trace.since(), &Antichain::from_elem(0));

        // Greater
        assert_eq!(
            state.collections.downgrade_since(
                &reader,
                seqno,
                None,
                &Antichain::from_elem(2),
                now()
            ),
            Continue(Since(Antichain::from_elem(2)))
        );
        assert_eq!(state.collections.trace.since(), &Antichain::from_elem(2));
        // Equal (no-op)
        assert_eq!(
            state.collections.downgrade_since(
                &reader,
                seqno,
                None,
                &Antichain::from_elem(2),
                now()
            ),
            Continue(Since(Antichain::from_elem(2)))
        );
        assert_eq!(state.collections.trace.since(), &Antichain::from_elem(2));
        // Less (no-op)
        assert_eq!(
            state.collections.downgrade_since(
                &reader,
                seqno,
                None,
                &Antichain::from_elem(1),
                now()
            ),
            Continue(Since(Antichain::from_elem(2)))
        );
        assert_eq!(state.collections.trace.since(), &Antichain::from_elem(2));

        // Create a second reader.
        let reader2 = LeasedReaderId::new();
        let _ = state.collections.register_leased_reader(
            "",
            &reader2,
            "",
            seqno,
            Duration::from_secs(10),
            now(),
        );

        // Shard since doesn't change until the meet (min) of all reader sinces changes.
        assert_eq!(
            state.collections.downgrade_since(
                &reader2,
                seqno,
                None,
                &Antichain::from_elem(3),
                now()
            ),
            Continue(Since(Antichain::from_elem(3)))
        );
        assert_eq!(state.collections.trace.since(), &Antichain::from_elem(2));
        // Shard since == 3 when all readers have since >= 3.
        assert_eq!(
            state.collections.downgrade_since(
                &reader,
                seqno,
                None,
                &Antichain::from_elem(5),
                now()
            ),
            Continue(Since(Antichain::from_elem(5)))
        );
        assert_eq!(state.collections.trace.since(), &Antichain::from_elem(3));

        // Shard since unaffected readers with since > shard since expiring.
        assert_eq!(
            state.collections.expire_leased_reader(&reader),
            Continue(true)
        );
        assert_eq!(state.collections.trace.since(), &Antichain::from_elem(3));

        // Create a third reader.
        let reader3 = LeasedReaderId::new();
        let _ = state.collections.register_leased_reader(
            "",
            &reader3,
            "",
            seqno,
            Duration::from_secs(10),
            now(),
        );

        // Shard since doesn't change until the meet (min) of all reader sinces changes.
        assert_eq!(
            state.collections.downgrade_since(
                &reader3,
                seqno,
                None,
                &Antichain::from_elem(10),
                now()
            ),
            Continue(Since(Antichain::from_elem(10)))
        );
        assert_eq!(state.collections.trace.since(), &Antichain::from_elem(3));

        // Shard since advances when reader with the minimal since expires.
        assert_eq!(
            state.collections.expire_leased_reader(&reader2),
            Continue(true)
        );
        // TODO: expiry temporarily doesn't advance since until we have #15511.
        // Switch this assertion back when we re-enable this.
        //
        // assert_eq!(state.collections.trace.since(), &Antichain::from_elem(10));
        assert_eq!(state.collections.trace.since(), &Antichain::from_elem(3));

        // Shard since unaffected when all readers are expired.
        assert_eq!(
            state.collections.expire_leased_reader(&reader3),
            Continue(true)
        );
        // TODO: expiry temporarily doesn't advance since until we have #15511.
        // Switch this assertion back when we re-enable this.
        //
        // assert_eq!(state.collections.trace.since(), &Antichain::from_elem(10));
        assert_eq!(state.collections.trace.since(), &Antichain::from_elem(3));
    }

    #[test]
    fn compare_and_append() {
        mz_ore::test::init_logging();
        let state = &mut TypedState::<String, String, u64, i64>::new(
            DUMMY_BUILD_INFO.semver_version(),
            ShardId::new(),
            "".to_owned(),
            0,
        )
        .collections;

        let writer_id = WriterId::new();
        let _ = state.register_writer("", &writer_id, "", Duration::from_secs(10), 0);
        let now = SYSTEM_TIME.clone();

        // State is initially empty.
        assert_eq!(state.trace.num_spine_batches(), 0);
        assert_eq!(state.trace.num_hollow_batches(), 0);
        assert_eq!(state.trace.num_updates(), 0);

        // Cannot insert a batch with a lower != current shard upper.
        assert_eq!(
            state.compare_and_append(
                &hollow(1, 2, &["key1"], 1),
                &writer_id,
                now(),
                &IdempotencyToken::new()
            ),
            Break(CompareAndAppendBreak::Upper {
                shard_upper: Antichain::from_elem(0),
                writer_upper: Antichain::from_elem(0)
            })
        );

        // Insert an empty batch with an upper > lower..
        assert!(state
            .compare_and_append(
                &hollow(0, 5, &[], 0),
                &writer_id,
                now(),
                &IdempotencyToken::new()
            )
            .is_continue());

        // Cannot insert a batch with a upper less than the lower.
        assert_eq!(
            state.compare_and_append(
                &hollow(5, 4, &["key1"], 1),
                &writer_id,
                now(),
                &IdempotencyToken::new()
            ),
            Break(CompareAndAppendBreak::InvalidUsage(InvalidBounds {
                lower: Antichain::from_elem(5),
                upper: Antichain::from_elem(4)
            }))
        );

        // Cannot insert a nonempty batch with an upper equal to lower.
        assert_eq!(
            state.compare_and_append(
                &hollow(5, 5, &["key1"], 1),
                &writer_id,
                now(),
                &IdempotencyToken::new()
            ),
            Break(CompareAndAppendBreak::InvalidUsage(
                InvalidEmptyTimeInterval {
                    lower: Antichain::from_elem(5),
                    upper: Antichain::from_elem(5),
                    keys: vec![PartialBatchKey("key1".to_owned())],
                }
            ))
        );

        // Can insert an empty batch with an upper equal to lower.
        assert!(state
            .compare_and_append(
                &hollow(5, 5, &[], 0),
                &writer_id,
                now(),
                &IdempotencyToken::new()
            )
            .is_continue());
    }

    #[test]
    fn snapshot() {
        mz_ore::test::init_logging();
        let now = SYSTEM_TIME.clone();

        let mut state = TypedState::<String, String, u64, i64>::new(
            DUMMY_BUILD_INFO.semver_version(),
            ShardId::new(),
            "".to_owned(),
            0,
        );
        // Cannot take a snapshot with as_of == shard upper.
        assert_eq!(
            state.snapshot(&Antichain::from_elem(0)),
            Ok(Err(Upper(Antichain::from_elem(0))))
        );

        // Cannot take a snapshot with as_of > shard upper.
        assert_eq!(
            state.snapshot(&Antichain::from_elem(5)),
            Ok(Err(Upper(Antichain::from_elem(0))))
        );

        let writer_id = WriterId::new();
        let _ = state
            .collections
            .register_writer("", &writer_id, "", Duration::from_secs(10), 0);

        // Advance upper to 5.
        assert!(state
            .collections
            .compare_and_append(
                &hollow(0, 5, &["key1"], 1),
                &writer_id,
                now(),
                &IdempotencyToken::new()
            )
            .is_continue());

        // Can take a snapshot with as_of < upper.
        assert_eq!(
            state.snapshot(&Antichain::from_elem(0)),
            Ok(Ok(vec![hollow(0, 5, &["key1"], 1)]))
        );

        // Can take a snapshot with as_of >= shard since, as long as as_of < shard_upper.
        assert_eq!(
            state.snapshot(&Antichain::from_elem(4)),
            Ok(Ok(vec![hollow(0, 5, &["key1"], 1)]))
        );

        // Cannot take a snapshot with as_of >= upper.
        assert_eq!(
            state.snapshot(&Antichain::from_elem(5)),
            Ok(Err(Upper(Antichain::from_elem(5))))
        );
        assert_eq!(
            state.snapshot(&Antichain::from_elem(6)),
            Ok(Err(Upper(Antichain::from_elem(5))))
        );

        let reader = LeasedReaderId::new();
        // Advance the since to 2.
        let _ = state.collections.register_leased_reader(
            "",
            &reader,
            "",
            SeqNo::minimum(),
            Duration::from_secs(10),
            now(),
        );
        assert_eq!(
            state.collections.downgrade_since(
                &reader,
                SeqNo::minimum(),
                None,
                &Antichain::from_elem(2),
                now()
            ),
            Continue(Since(Antichain::from_elem(2)))
        );
        assert_eq!(state.collections.trace.since(), &Antichain::from_elem(2));
        // Cannot take a snapshot with as_of < shard_since.
        assert_eq!(
            state.snapshot(&Antichain::from_elem(1)),
            Err(Since(Antichain::from_elem(2)))
        );

        // Advance the upper to 10 via an empty batch.
        assert!(state
            .collections
            .compare_and_append(
                &hollow(5, 10, &[], 0),
                &writer_id,
                now(),
                &IdempotencyToken::new()
            )
            .is_continue());

        // Can still take snapshots at times < upper.
        assert_eq!(
            state.snapshot(&Antichain::from_elem(7)),
            Ok(Ok(vec![hollow(0, 5, &["key1"], 1), hollow(5, 10, &[], 0)]))
        );

        // Cannot take snapshots with as_of >= upper.
        assert_eq!(
            state.snapshot(&Antichain::from_elem(10)),
            Ok(Err(Upper(Antichain::from_elem(10))))
        );

        // Advance upper to 15.
        assert!(state
            .collections
            .compare_and_append(
                &hollow(10, 15, &["key2"], 1),
                &writer_id,
                now(),
                &IdempotencyToken::new()
            )
            .is_continue());

        // Filter out batches whose lowers are less than the requested as of (the
        // batches that are too far in the future for the requested as_of).
        assert_eq!(
            state.snapshot(&Antichain::from_elem(9)),
            Ok(Ok(vec![hollow(0, 5, &["key1"], 1), hollow(5, 10, &[], 0)]))
        );

        // Don't filter out batches whose lowers are <= the requested as_of.
        assert_eq!(
            state.snapshot(&Antichain::from_elem(10)),
            Ok(Ok(vec![
                hollow(0, 5, &["key1"], 1),
                hollow(5, 10, &[], 0),
                hollow(10, 15, &["key2"], 1)
            ]))
        );

        assert_eq!(
            state.snapshot(&Antichain::from_elem(11)),
            Ok(Ok(vec![
                hollow(0, 5, &["key1"], 1),
                hollow(5, 10, &[], 0),
                hollow(10, 15, &["key2"], 1)
            ]))
        );
    }

    #[test]
    fn next_listen_batch() {
        mz_ore::test::init_logging();

        let mut state = TypedState::<String, String, u64, i64>::new(
            DUMMY_BUILD_INFO.semver_version(),
            ShardId::new(),
            "".to_owned(),
            0,
        );

        // Empty collection never has any batches to listen for, regardless of the
        // current frontier.
        assert_eq!(state.next_listen_batch(&Antichain::from_elem(0)), None);
        assert_eq!(state.next_listen_batch(&Antichain::new()), None);

        let writer_id = WriterId::new();
        let _ = state
            .collections
            .register_writer("", &writer_id, "", Duration::from_secs(10), 0);
        let now = SYSTEM_TIME.clone();

        // Add two batches of data, one from [0, 5) and then another from [5, 10).
        assert!(state
            .collections
            .compare_and_append(
                &hollow(0, 5, &["key1"], 1),
                &writer_id,
                now(),
                &IdempotencyToken::new()
            )
            .is_continue());
        assert!(state
            .collections
            .compare_and_append(
                &hollow(5, 10, &["key2"], 1),
                &writer_id,
                now(),
                &IdempotencyToken::new()
            )
            .is_continue());

        // All frontiers in [0, 5) return the first batch.
        for t in 0..=4 {
            assert_eq!(
                state.next_listen_batch(&Antichain::from_elem(t)),
                Some(hollow(0, 5, &["key1"], 1))
            );
        }

        // All frontiers in [5, 10) return the second batch.
        for t in 5..=9 {
            assert_eq!(
                state.next_listen_batch(&Antichain::from_elem(t)),
                Some(hollow(5, 10, &["key2"], 1))
            );
        }

        // There is no batch currently available for t = 10.
        assert_eq!(state.next_listen_batch(&Antichain::from_elem(10)), None);

        // By definition, there is no frontier ever at the empty antichain which
        // is the time after all possible times.
        assert_eq!(state.next_listen_batch(&Antichain::new()), None);
    }

    #[test]
    fn expire_writer() {
        mz_ore::test::init_logging();

        let mut state = TypedState::<String, String, u64, i64>::new(
            DUMMY_BUILD_INFO.semver_version(),
            ShardId::new(),
            "".to_owned(),
            0,
        );
        let now = SYSTEM_TIME.clone();

        let writer_id_one = WriterId::new();

        // Writer has not been registered and should be unable to write
        assert_eq!(
            state.collections.compare_and_append(
                &hollow(0, 2, &["key1"], 1),
                &writer_id_one,
                now(),
                &IdempotencyToken::new()
            ),
            Break(CompareAndAppendBreak::InvalidUsage(
                InvalidUsage::UnknownWriter(writer_id_one.clone())
            ))
        );

        assert!(state
            .collections
            .register_writer("", &writer_id_one, "", Duration::from_secs(10), 0)
            .is_continue());

        let writer_id_two = WriterId::new();
        assert!(state
            .collections
            .register_writer("", &writer_id_two, "", Duration::from_secs(10), 0)
            .is_continue());

        // Writer is registered and is now eligible to write
        assert!(state
            .collections
            .compare_and_append(
                &hollow(0, 2, &["key1"], 1),
                &writer_id_one,
                now(),
                &IdempotencyToken::new()
            )
            .is_continue());

        assert!(state
            .collections
            .expire_writer(&writer_id_one)
            .is_continue());

        // Writer has been expired and should be fenced off from further writes
        assert_eq!(
            state.collections.compare_and_append(
                &hollow(2, 5, &["key2"], 1),
                &writer_id_one,
                now(),
                &IdempotencyToken::new()
            ),
            Break(CompareAndAppendBreak::InvalidUsage(
                InvalidUsage::UnknownWriter(writer_id_one.clone())
            ))
        );

        // But other writers should still be able to write
        assert!(state
            .collections
            .compare_and_append(
                &hollow(2, 5, &["key2"], 1),
                &writer_id_two,
                now(),
                &IdempotencyToken::new()
            )
            .is_continue());
    }

    #[test]
    fn maybe_gc() {
        mz_ore::test::init_logging();
        let mut state = TypedState::<String, String, u64, i64>::new(
            DUMMY_BUILD_INFO.semver_version(),
            ShardId::new(),
            "".to_owned(),
            0,
        );

        // Empty state doesn't need gc, regardless of is_write.
        assert_eq!(state.maybe_gc(true), None);
        assert_eq!(state.maybe_gc(false), None);

        // Artificially advance the seqno so the seqno_since advances past our
        // internal gc_threshold.
        state.seqno = SeqNo(100);
        assert_eq!(state.seqno_since(), SeqNo(100));

        // When a writer is present, non-writes don't gc.
        let writer_id = WriterId::new();
        let _ = state
            .collections
            .register_writer("", &writer_id, "", Duration::from_secs(10), 0);
        assert_eq!(state.maybe_gc(false), None);

        // A write will gc though.
        assert_eq!(
            state.maybe_gc(true),
            Some(GcReq {
                shard_id: state.shard_id,
                new_seqno_since: SeqNo(100)
            })
        );

        // Artificially advance the seqno (again) so the seqno_since advances
        // past our internal gc_threshold (again).
        state.seqno = SeqNo(200);
        assert_eq!(state.seqno_since(), SeqNo(200));

        // If there are no writers, even a non-write will gc.
        let _ = state.collections.expire_writer(&writer_id);
        assert_eq!(
            state.maybe_gc(true),
            Some(GcReq {
                shard_id: state.shard_id,
                new_seqno_since: SeqNo(200)
            })
        );
    }

    #[test]
    fn idempotency_token_sentinel() {
        assert_eq!(
            IdempotencyToken::SENTINEL.to_string(),
            "i11111111-1111-1111-1111-111111111111"
        );
    }
}

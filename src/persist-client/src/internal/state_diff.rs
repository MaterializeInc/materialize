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
use std::fmt::Debug;

use differential_dataflow::lattice::Lattice;
use differential_dataflow::trace::Description;
use mz_persist::location::{SeqNo, VersionedData};
use mz_persist_types::Codec64;
use timely::progress::{Antichain, Timestamp};
use timely::PartialOrder;
use tracing::debug;

use crate::internal::paths::PartialRollupKey;
use crate::internal::state::{HollowBatch, ReaderState, State, StateCollections, WriterState};
use crate::internal::trace::Trace;
use crate::read::ReaderId;
use crate::write::WriterId;
use crate::{Metrics, PersistConfig};

use self::StateFieldValDiff::*;

#[derive(Debug)]
#[cfg_attr(any(test, debug_assertions), derive(Clone, PartialEq))]
pub enum StateFieldValDiff<V> {
    Insert(V),
    Update(V, V),
    Delete(V),
}

#[derive(Debug)]
#[cfg_attr(any(test, debug_assertions), derive(Clone, PartialEq))]
pub struct StateFieldDiff<K, V> {
    pub key: K,
    pub val: StateFieldValDiff<V>,
}

#[derive(Debug)]
#[cfg_attr(any(test, debug_assertions), derive(Clone, PartialEq))]
pub struct StateDiff<T> {
    pub(crate) applier_version: semver::Version,
    pub(crate) seqno_from: SeqNo,
    pub(crate) seqno_to: SeqNo,
    pub(crate) latest_rollup_key: PartialRollupKey,
    pub(crate) rollups: Vec<StateFieldDiff<SeqNo, PartialRollupKey>>,
    pub(crate) last_gc_req: Vec<StateFieldDiff<(), SeqNo>>,
    pub(crate) readers: Vec<StateFieldDiff<ReaderId, ReaderState<T>>>,
    pub(crate) writers: Vec<StateFieldDiff<WriterId, WriterState>>,
    pub(crate) since: Vec<StateFieldDiff<(), Antichain<T>>>,
    pub(crate) spine: Vec<StateFieldDiff<HollowBatch<T>, ()>>,
}

impl<T: Timestamp + Lattice + Codec64> StateDiff<T> {
    pub fn new(
        applier_version: semver::Version,
        seqno_from: SeqNo,
        seqno_to: SeqNo,
        latest_rollup_key: PartialRollupKey,
    ) -> Self {
        StateDiff {
            applier_version,
            seqno_from,
            seqno_to,
            latest_rollup_key,
            rollups: Vec::default(),
            last_gc_req: Vec::default(),
            readers: Vec::default(),
            writers: Vec::default(),
            since: Vec::default(),
            spine: Vec::default(),
        }
    }

    pub fn from_diff<K, V, D>(from: &State<K, V, T, D>, to: &State<K, V, T, D>) -> Self {
        // Deconstruct from and to so we get a compile failure if new
        // fields are added.
        let State {
            applier_version: _,
            shard_id: from_shard_id,
            seqno: from_seqno,
            collections:
                StateCollections {
                    last_gc_req: from_last_gc_req,
                    rollups: from_rollups,
                    readers: from_readers,
                    writers: from_writers,
                    trace: from_trace,
                },
            _phantom: _,
        } = from;
        let State {
            applier_version: to_applier_version,
            shard_id: to_shard_id,
            seqno: to_seqno,
            collections:
                StateCollections {
                    last_gc_req: to_last_gc_req,
                    rollups: to_rollups,
                    readers: to_readers,
                    writers: to_writers,
                    trace: to_trace,
                },
            _phantom: _,
        } = to;
        assert_eq!(from_shard_id, to_shard_id);

        let (_, latest_rollup_key) = to.latest_rollup();
        let mut diffs = Self::new(
            to_applier_version.clone(),
            *from_seqno,
            *to_seqno,
            latest_rollup_key.clone(),
        );
        diff_field_single(from_last_gc_req, to_last_gc_req, &mut diffs.last_gc_req);
        diff_field_sorted_iter(from_rollups.iter(), to_rollups, &mut diffs.rollups);
        diff_field_sorted_iter(from_readers.iter(), to_readers, &mut diffs.readers);
        diff_field_sorted_iter(from_writers.iter(), to_writers, &mut diffs.writers);
        diff_field_single(from_trace.since(), to_trace.since(), &mut diffs.since);
        diff_field_spine(from_trace, to_trace, &mut diffs.spine);
        diffs
    }

    #[cfg(any(test, debug_assertions))]
    #[allow(dead_code)]
    pub fn validate_roundtrip<K, V, D>(
        metrics: &Metrics,
        from_state: &State<K, V, T, D>,
        diff: &Self,
        to_state: &State<K, V, T, D>,
    ) -> Result<(), String>
    where
        K: mz_persist_types::Codec + std::fmt::Debug,
        V: mz_persist_types::Codec + std::fmt::Debug,
        D: differential_dataflow::difference::Semigroup + Codec64,
    {
        use crate::internal::state::ProtoStateDiff;
        use mz_proto::RustType;
        use prost::Message;

        let mut roundtrip_state = from_state.clone(to_state.applier_version.clone());
        roundtrip_state.apply_diff(metrics, diff.clone())?;

        if &roundtrip_state != to_state {
            // The weird spacing in this format string is so they all line up
            // when printed out.
            return Err(format!("state didn't roundtrip\n  from_state {:?}\n  to_state   {:?}\n  rt_state   {:?}\n  diff       {:?}\n", from_state, to_state, roundtrip_state, diff));
        }

        let encoded_diff = diff.into_proto().encode_to_vec();
        let roundtrip_diff = Self::from_proto(
            ProtoStateDiff::decode(encoded_diff.as_slice()).map_err(|err| err.to_string())?,
        )
        .map_err(|err| err.to_string())?;

        if &roundtrip_diff != diff {
            // The weird spacing in this format string is so they all line up
            // when printed out.
            return Err(format!(
                "diff didn't roundtrip\n  diff    {:?}\n  rt_diff {:?}",
                diff, roundtrip_diff
            ));
        }

        Ok(())
    }
}

impl<K, V, T: Timestamp + Lattice + Codec64, D> State<K, V, T, D> {
    pub fn apply_encoded_diffs<'a, I: IntoIterator<Item = &'a VersionedData>>(
        &mut self,
        cfg: &PersistConfig,
        metrics: &Metrics,
        diffs: I,
    ) {
        let mut state_seqno = self.seqno;
        let diffs = diffs.into_iter().filter_map(move |x| {
            if x.seqno == state_seqno {
                // No-op.
                return None;
            }
            let diff = metrics
                .codecs
                .state_diff
                .decode(|| StateDiff::decode(&cfg.build_version, &x.data));
            assert_eq!(diff.seqno_from, state_seqno);
            state_seqno = diff.seqno_to;
            Some(diff)
        });
        self.apply_diffs(metrics, diffs)
            .expect("state diff should apply cleanly");
    }
}

impl<K, V, T: Timestamp + Lattice, D> State<K, V, T, D> {
    // This might leave state in an invalid (umm) state when returning an error.
    // The caller is responsible for handling this.
    pub fn apply_diffs<I: IntoIterator<Item = StateDiff<T>>>(
        &mut self,
        metrics: &Metrics,
        diffs: I,
    ) -> Result<(), String> {
        for diff in diffs {
            // TODO: This could special-case batch apply for diffs where it's
            // more efficient (in particular, spine batches that hit the slow
            // path).
            self.apply_diff(metrics, diff)?;
        }
        Ok(())
    }

    // Intentionally not even pub(crate) because all callers should use
    // [Self::apply_diffs].
    fn apply_diff(&mut self, metrics: &Metrics, diff: StateDiff<T>) -> Result<(), String> {
        if self.seqno == diff.seqno_to {
            return Ok(());
        }
        if self.seqno != diff.seqno_from {
            return Err(format!(
                "could not apply diff {} -> {} to state {}",
                diff.seqno_from, diff.seqno_to, self.seqno
            ));
        }
        self.seqno = diff.seqno_to;

        apply_diffs_map("rollups", diff.rollups, &mut self.collections.rollups)?;
        apply_diffs_single(
            "last_gc_req",
            diff.last_gc_req,
            &mut self.collections.last_gc_req,
        )?;
        apply_diffs_map("readers", diff.readers, &mut self.collections.readers)?;
        apply_diffs_map("writers", diff.writers, &mut self.collections.writers)?;

        for x in diff.since {
            match x.val {
                Update(from, to) => {
                    if self.collections.trace.since() != &from {
                        return Err(format!(
                            "since update didn't match: {:?} vs {:?}",
                            self.collections.trace.since(),
                            &from
                        ));
                    }
                    self.collections.trace.downgrade_since(&to);
                }
                Insert(_) => return Err(format!("cannot insert since field")),
                Delete(_) => return Err(format!("cannot delete since field")),
            }
        }
        apply_diffs_spine(metrics, diff.spine, &mut self.collections.trace)?;

        // There's various sanity checks that this method could run (e.g. since,
        // upper, seqno_since, etc don't regress or that diff.latest_rollup ==
        // state.rollups.last()), are they a good idea? On one hand, I like
        // sanity checks, other the other, one of the goals here is to keep
        // apply logic as straightforward and unchanging as possible.
        Ok(())
    }
}

fn diff_field_single<T: PartialEq + Clone>(
    from: &T,
    to: &T,
    diffs: &mut Vec<StateFieldDiff<(), T>>,
) {
    // This could use the `diff_field_sorted_iter(once(from), once(to), diffs)`
    // general impl, but we just do the obvious thing.
    if from != to {
        diffs.push(StateFieldDiff {
            key: (),
            val: Update(from.clone(), to.clone()),
        })
    }
}

fn apply_diffs_single<X: PartialEq + Debug>(
    name: &str,
    diffs: Vec<StateFieldDiff<(), X>>,
    single: &mut X,
) -> Result<(), String> {
    for diff in diffs {
        apply_diff_single(name, diff, single)?;
    }
    Ok(())
}

fn apply_diff_single<X: PartialEq + Debug>(
    name: &str,
    diff: StateFieldDiff<(), X>,
    single: &mut X,
) -> Result<(), String> {
    match diff.val {
        Update(from, to) => {
            if single != &from {
                return Err(format!(
                    "{} update didn't match: {:?} vs {:?}",
                    name, single, &from
                ));
            }
            *single = to
        }
        Insert(_) => return Err(format!("cannot insert {} field", name)),
        Delete(_) => return Err(format!("cannot delete {} field", name)),
    }
    Ok(())
}

fn diff_field_sorted_iter<'a, K, V, IF, IT>(from: IF, to: IT, diffs: &mut Vec<StateFieldDiff<K, V>>)
where
    K: Ord + Clone + 'a,
    V: PartialEq + Clone + 'a,
    IF: IntoIterator<Item = (&'a K, &'a V)>,
    IT: IntoIterator<Item = (&'a K, &'a V)>,
{
    let (mut from, mut to) = (from.into_iter(), to.into_iter());
    let (mut f, mut t) = (from.next(), to.next());
    loop {
        match (f, t) {
            (None, None) => break,
            (Some((fk, fv)), Some((tk, tv))) => match fk.cmp(tk) {
                Ordering::Less => {
                    diffs.push(StateFieldDiff {
                        key: fk.clone(),
                        val: Delete(fv.clone()),
                    });
                    let f_next = from.next();
                    debug_assert!(f_next.as_ref().map_or(true, |(fk_next, _)| fk_next > &fk));
                    f = f_next;
                }
                Ordering::Greater => {
                    diffs.push(StateFieldDiff {
                        key: tk.clone(),
                        val: Insert(tv.clone()),
                    });
                    let t_next = to.next();
                    debug_assert!(t_next.as_ref().map_or(true, |(tk_next, _)| tk_next > &tk));
                    t = t_next;
                }
                Ordering::Equal => {
                    // TODO: regression test for this if, I missed it in the
                    // original impl :)
                    if fv != tv {
                        diffs.push(StateFieldDiff {
                            key: fk.clone(),
                            val: Update(fv.clone(), tv.clone()),
                        });
                    }
                    let f_next = from.next();
                    debug_assert!(f_next.as_ref().map_or(true, |(fk_next, _)| fk_next > &fk));
                    f = f_next;
                    let t_next = to.next();
                    debug_assert!(t_next.as_ref().map_or(true, |(tk_next, _)| tk_next > &tk));
                    t = t_next;
                }
            },
            (None, Some((tk, tv))) => {
                diffs.push(StateFieldDiff {
                    key: tk.clone(),
                    val: Insert(tv.clone()),
                });
                let t_next = to.next();
                debug_assert!(t_next.as_ref().map_or(true, |(tk_next, _)| tk_next > &tk));
                t = t_next;
            }
            (Some((fk, fv)), None) => {
                diffs.push(StateFieldDiff {
                    key: fk.clone(),
                    val: Delete(fv.clone()),
                });
                let f_next = from.next();
                debug_assert!(f_next.as_ref().map_or(true, |(fk_next, _)| fk_next > &fk));
                f = f_next;
            }
        }
    }
}

fn apply_diffs_map<K: Ord, V: PartialEq + Debug>(
    name: &str,
    diffs: Vec<StateFieldDiff<K, V>>,
    map: &mut BTreeMap<K, V>,
) -> Result<(), String> {
    for diff in diffs {
        apply_diff_map(name, diff, map)?;
    }
    Ok(())
}

// This might leave state in an invalid (umm) state when returning an error. The
// caller ultimately ends up panic'ing on error, but if that changes, we might
// want to revisit this.
fn apply_diff_map<K: Ord, V: PartialEq + Debug>(
    name: &str,
    diff: StateFieldDiff<K, V>,
    map: &mut BTreeMap<K, V>,
) -> Result<(), String> {
    match diff.val {
        Insert(to) => {
            let prev = map.insert(diff.key, to);
            if prev != None {
                return Err(format!("{} insert found existing value: {:?}", name, prev));
            }
        }
        Update(from, to) => {
            let prev = map.insert(diff.key, to);
            if prev.as_ref() != Some(&from) {
                return Err(format!(
                    "{} update didn't match: {:?} vs {:?}",
                    name,
                    prev,
                    Some(from),
                ));
            }
        }
        Delete(from) => {
            let prev = map.remove(&diff.key);
            if prev.as_ref() != Some(&from) {
                return Err(format!(
                    "{} delete didn't match: {:?} vs {:?}",
                    name,
                    prev,
                    Some(from),
                ));
            }
        }
    };
    Ok(())
}

fn diff_field_spine<T: Timestamp + Lattice>(
    from: &Trace<T>,
    to: &Trace<T>,
    diffs: &mut Vec<StateFieldDiff<HollowBatch<T>, ()>>,
) {
    let from_batches = from.batches().into_iter().map(|b| (b, &()));
    let to_batches = to.batches().into_iter().map(|b| (b, &()));
    diff_field_sorted_iter(from_batches, to_batches, diffs);
}

// This might leave state in an invalid (umm) state when returning an error. The
// caller ultimately ends up panic'ing on error, but if that changes, we might
// want to revisit this.
fn apply_diffs_spine<T: Timestamp + Lattice>(
    metrics: &Metrics,
    diffs: Vec<StateFieldDiff<HollowBatch<T>, ()>>,
    trace: &mut Trace<T>,
) -> Result<(), String> {
    match &diffs[..] {
        // Fast-path: no diffs.
        [] => return Ok(()),

        // Fast-path: batch insert.
        [StateFieldDiff {
            key,
            val: StateFieldValDiff::Insert(()),
        }] => {
            // Ignore merge_reqs because whichever process generated this diff is
            // assigned the work.
            let _merge_reqs = trace.push_batch(key.clone());
            metrics.state.apply_spine_fast_path.inc();
            return Ok(());
        }

        // Fast-path: batch insert with both new and most recent batch empty.
        // Spine will happily merge these empty batches together without a call
        // out to compaction.
        [StateFieldDiff {
            key: del,
            val: StateFieldValDiff::Delete(()),
        }, StateFieldDiff {
            key: ins,
            val: StateFieldValDiff::Insert(()),
        }] => {
            if del.keys.len() == 0
                && ins.keys.len() == 0
                && del.desc.lower() == ins.desc.lower()
                && PartialOrder::less_than(del.desc.upper(), ins.desc.upper())
            {
                // Ignore merge_reqs because whichever process generated this diff is
                // assigned the work.
                let _merge_reqs = trace.push_batch(HollowBatch {
                    desc: Description::new(
                        del.desc.upper().clone(),
                        ins.desc.upper().clone(),
                        // `keys.len() == 0` for both `del` and `ins` means we
                        // don't have to think about what the compaction
                        // frontier is for these batches (nothing in them, so nothing could have been compacted.
                        Antichain::from_elem(T::minimum()),
                    ),
                    keys: vec![],
                    len: 0,
                });
                metrics.state.apply_spine_fast_path.inc();
                return Ok(());
            }
        }
        // Fall-through
        _ => {}
    }

    // Fast-path: compaction
    //
    // TODO: This seems to tickle some existing bugs that we haven't been
    // hitting because, before incremental state, we were reconstructing Spine
    // from scratch whenever we deserialized a new version of state. Losing it
    // is unfortunate, but no worse than where we were before and we're getting
    // down to the wire with getting inc state (and its backward
    // incompatibility) into the desired release. Revisit as time permits to
    // shake out the bugs (they repro readily in CI) and re-enable.
    #[cfg(TODO)]
    if let Some((_inputs, output)) = sniff_compaction(&diffs) {
        let res = FueledMergeRes { output };
        // We can't predict how spine will arrange the batches when it's
        // hydrated. This means that something that is maintaining a Spine
        // starting at some seqno may not exactly match something else
        // maintaining the same spine starting at a different seqno. (Plus,
        // maybe these aren't even on the same version of the code and we've
        // changed the spine logic.) Because apply_merge_res is strict,
        // we're not _guaranteed_ that we can apply a compaction response
        // that was generated elsewhere. Most of the time we can, though, so
        // count the good ones and fall back to the slow path below when we
        // can't.
        if trace.apply_merge_res(&res) {
            // Maybe return the replaced batches from apply_merge_res and verify
            // that they match _inputs?
            metrics.state.apply_spine_fast_path.inc();
            return Ok(());
        }
    }

    // Something complicated is going on, so reconstruct the Trace from scratch.
    metrics.state.apply_spine_slow_path.inc();
    debug!(
        "apply_diffs_spine didn't hit a fast-path diffs={:?} trace={:?}",
        diffs, trace
    );

    let mut batches = BTreeMap::new();
    trace.map_batches(|b| assert!(batches.insert(b.clone(), ()).is_none()));
    apply_diffs_map("spine", diffs, &mut batches)?;

    let mut new_trace = Trace::default();
    new_trace.downgrade_since(trace.since());
    for (batch, ()) in batches {
        // Ignore merge_reqs because whichever process generated this diff is
        // assigned the work.
        let _merge_reqs = new_trace.push_batch(batch);
    }
    *trace = new_trace;
    Ok(())
}

// TODO: Instead of trying to sniff out a compaction from diffs, should we just
// be explicit?
#[allow(dead_code)]
fn sniff_compaction<'a, T: Timestamp + Lattice>(
    diffs: &'a [StateFieldDiff<HollowBatch<T>, ()>],
) -> Option<(Vec<&'a HollowBatch<T>>, HollowBatch<T>)> {
    // Compaction always produces exactly one output batch (with possibly many
    // parts, but we get one Insert for the whole batch.
    let mut inserts = diffs.iter().flat_map(|x| match x.val {
        StateFieldValDiff::Insert(()) => Some(&x.key),
        _ => None,
    });
    let compaction_output = match inserts.next() {
        Some(x) => x,
        None => return None,
    };
    if let Some(_) = inserts.next() {
        return None;
    }

    // Grab all deletes and sanity check that there are no updates.
    let mut compaction_inputs = Vec::with_capacity(diffs.len() - 1);
    for diff in diffs.iter() {
        match diff.val {
            StateFieldValDiff::Delete(()) => {
                compaction_inputs.push(&diff.key);
            }
            StateFieldValDiff::Insert(()) => {}
            StateFieldValDiff::Update((), ()) => {
                // Fall through to let the general case create the error
                // message.
                return None;
            }
        }
    }

    Some((compaction_inputs, compaction_output.clone()))
}

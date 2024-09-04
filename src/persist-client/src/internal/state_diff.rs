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
use std::sync::Arc;

use bytes::{Bytes, BytesMut};
use differential_dataflow::lattice::Lattice;
use mz_ore::cast::CastFrom;
use mz_persist::location::{SeqNo, VersionedData};
use mz_persist_types::Codec64;
use mz_proto::TryFromProtoError;
use timely::progress::{Antichain, Timestamp};
use tracing::debug;

use crate::critical::CriticalReaderId;
use crate::internal::paths::PartialRollupKey;
use crate::internal::state::{
    CriticalReaderState, EncodedSchemas, HollowBatch, HollowBlobRef, HollowRollup,
    LeasedReaderState, ProtoStateField, ProtoStateFieldDiffType, ProtoStateFieldDiffs, State,
    StateCollections, WriterState,
};
use crate::internal::trace::{SpineId, ThinMerge, ThinSpineBatch, Trace};
use crate::read::LeasedReaderId;
use crate::schema::SchemaId;
use crate::write::WriterId;
use crate::{Metrics, PersistConfig, ShardId};

use StateFieldValDiff::*;

#[derive(Clone, Debug)]
#[cfg_attr(any(test, debug_assertions), derive(PartialEq))]
pub enum StateFieldValDiff<V> {
    Insert(V),
    Update(V, V),
    Delete(V),
}

#[derive(Clone)]
#[cfg_attr(any(test, debug_assertions), derive(PartialEq))]
pub struct StateFieldDiff<K, V> {
    pub key: K,
    pub val: StateFieldValDiff<V>,
}

impl<K: Debug, V: Debug> std::fmt::Debug for StateFieldDiff<K, V> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StateFieldDiff")
            // In the cases we've seen in the wild, it's been more useful to
            // have the val printed first.
            .field("val", &self.val)
            .field("key", &self.key)
            .finish()
    }
}

#[derive(Debug)]
#[cfg_attr(any(test, debug_assertions), derive(Clone, PartialEq))]
pub struct StateDiff<T> {
    pub(crate) applier_version: semver::Version,
    pub(crate) seqno_from: SeqNo,
    pub(crate) seqno_to: SeqNo,
    pub(crate) walltime_ms: u64,
    pub(crate) latest_rollup_key: PartialRollupKey,
    pub(crate) rollups: Vec<StateFieldDiff<SeqNo, HollowRollup>>,
    pub(crate) hostname: Vec<StateFieldDiff<(), String>>,
    pub(crate) last_gc_req: Vec<StateFieldDiff<(), SeqNo>>,
    pub(crate) leased_readers: Vec<StateFieldDiff<LeasedReaderId, LeasedReaderState<T>>>,
    pub(crate) critical_readers: Vec<StateFieldDiff<CriticalReaderId, CriticalReaderState<T>>>,
    pub(crate) writers: Vec<StateFieldDiff<WriterId, WriterState<T>>>,
    pub(crate) schemas: Vec<StateFieldDiff<SchemaId, EncodedSchemas>>,
    pub(crate) since: Vec<StateFieldDiff<(), Antichain<T>>>,
    pub(crate) legacy_batches: Vec<StateFieldDiff<HollowBatch<T>, ()>>,
    pub(crate) hollow_batches: Vec<StateFieldDiff<SpineId, Arc<HollowBatch<T>>>>,
    pub(crate) spine_batches: Vec<StateFieldDiff<SpineId, ThinSpineBatch<T>>>,
    pub(crate) merges: Vec<StateFieldDiff<SpineId, ThinMerge<T>>>,
}

impl<T: Timestamp + Codec64> StateDiff<T> {
    pub fn new(
        applier_version: semver::Version,
        seqno_from: SeqNo,
        seqno_to: SeqNo,
        walltime_ms: u64,
        latest_rollup_key: PartialRollupKey,
    ) -> Self {
        StateDiff {
            applier_version,
            seqno_from,
            seqno_to,
            walltime_ms,
            latest_rollup_key,
            rollups: Vec::default(),
            hostname: Vec::default(),
            last_gc_req: Vec::default(),
            leased_readers: Vec::default(),
            critical_readers: Vec::default(),
            writers: Vec::default(),
            schemas: Vec::default(),
            since: Vec::default(),
            legacy_batches: Vec::default(),
            hollow_batches: Vec::default(),
            spine_batches: Vec::default(),
            merges: Vec::default(),
        }
    }

    pub fn referenced_batches(&self) -> impl Iterator<Item = StateFieldValDiff<&HollowBatch<T>>> {
        let legacy_batches = self
            .legacy_batches
            .iter()
            .filter_map(|diff| match diff.val {
                Insert(()) => Some(Insert(&diff.key)),
                Update((), ()) => None, // Ignoring a noop diff.
                Delete(()) => Some(Delete(&diff.key)),
            });
        let hollow_batches = self.hollow_batches.iter().map(|diff| match &diff.val {
            Insert(batch) => Insert(&**batch),
            Update(before, after) => Update(&**before, &**after),
            Delete(batch) => Delete(&**batch),
        });
        legacy_batches.chain(hollow_batches)
    }
}

impl<T: Timestamp + Lattice + Codec64> StateDiff<T> {
    pub fn from_diff(from: &State<T>, to: &State<T>) -> Self {
        // Deconstruct from and to so we get a compile failure if new
        // fields are added.
        let State {
            applier_version: _,
            shard_id: from_shard_id,
            seqno: from_seqno,
            hostname: from_hostname,
            walltime_ms: _, // Intentionally unused
            collections:
                StateCollections {
                    last_gc_req: from_last_gc_req,
                    rollups: from_rollups,
                    leased_readers: from_leased_readers,
                    critical_readers: from_critical_readers,
                    writers: from_writers,
                    schemas: from_schemas,
                    trace: from_trace,
                },
        } = from;
        let State {
            applier_version: to_applier_version,
            shard_id: to_shard_id,
            seqno: to_seqno,
            walltime_ms: to_walltime_ms,
            hostname: to_hostname,
            collections:
                StateCollections {
                    last_gc_req: to_last_gc_req,
                    rollups: to_rollups,
                    leased_readers: to_leased_readers,
                    critical_readers: to_critical_readers,
                    writers: to_writers,
                    schemas: to_schemas,
                    trace: to_trace,
                },
        } = to;
        assert_eq!(from_shard_id, to_shard_id);

        let (_, latest_rollup) = to.latest_rollup();
        let mut diffs = Self::new(
            to_applier_version.clone(),
            *from_seqno,
            *to_seqno,
            *to_walltime_ms,
            latest_rollup.key.clone(),
        );
        diff_field_single(from_hostname, to_hostname, &mut diffs.hostname);
        diff_field_single(from_last_gc_req, to_last_gc_req, &mut diffs.last_gc_req);
        diff_field_sorted_iter(from_rollups.iter(), to_rollups, &mut diffs.rollups);
        diff_field_sorted_iter(
            from_leased_readers.iter(),
            to_leased_readers,
            &mut diffs.leased_readers,
        );
        diff_field_sorted_iter(
            from_critical_readers.iter(),
            to_critical_readers,
            &mut diffs.critical_readers,
        );
        diff_field_sorted_iter(from_writers.iter(), to_writers, &mut diffs.writers);
        diff_field_sorted_iter(from_schemas.iter(), to_schemas, &mut diffs.schemas);
        diff_field_single(from_trace.since(), to_trace.since(), &mut diffs.since);

        let from_flat = from_trace.flatten();
        let to_flat = to_trace.flatten();
        diff_field_sorted_iter(
            from_flat.legacy_batches.iter().map(|(k, v)| (&**k, v)),
            to_flat.legacy_batches.iter().map(|(k, v)| (&**k, v)),
            &mut diffs.legacy_batches,
        );
        diff_field_sorted_iter(
            from_flat.hollow_batches.iter(),
            to_flat.hollow_batches.iter(),
            &mut diffs.hollow_batches,
        );
        diff_field_sorted_iter(
            from_flat.spine_batches.iter(),
            to_flat.spine_batches.iter(),
            &mut diffs.spine_batches,
        );
        diff_field_sorted_iter(
            from_flat.merges.iter(),
            to_flat.merges.iter(),
            &mut diffs.merges,
        );
        diffs
    }

    pub(crate) fn blob_inserts(&self) -> impl Iterator<Item = HollowBlobRef<T>> {
        let batches = self
            .referenced_batches()
            .filter_map(|spine_diff| match spine_diff {
                Insert(b) | Update(_, b) => Some(HollowBlobRef::Batch(b)),
                Delete(_) => None, // No-op
            });
        let rollups = self
            .rollups
            .iter()
            .filter_map(|rollups_diff| match &rollups_diff.val {
                StateFieldValDiff::Insert(x) | StateFieldValDiff::Update(_, x) => {
                    Some(HollowBlobRef::Rollup(x))
                }
                StateFieldValDiff::Delete(_) => None, // No-op
            });
        batches.chain(rollups)
    }

    pub(crate) fn blob_deletes(&self) -> impl Iterator<Item = HollowBlobRef<T>> {
        let batches = self
            .referenced_batches()
            .filter_map(|spine_diff| match spine_diff {
                Insert(_) => None,
                Update(a, _) | Delete(a) => Some(HollowBlobRef::Batch(a)),
            });
        let rollups = self
            .rollups
            .iter()
            .filter_map(|rollups_diff| match &rollups_diff.val {
                Insert(_) => None,
                Update(a, _) | Delete(a) => Some(HollowBlobRef::Rollup(a)),
            });
        batches.chain(rollups)
    }

    #[cfg(any(test, debug_assertions))]
    #[allow(dead_code)]
    pub fn validate_roundtrip<K, V, D>(
        metrics: &Metrics,
        from_state: &crate::internal::state::TypedState<K, V, T, D>,
        diff: &Self,
        to_state: &crate::internal::state::TypedState<K, V, T, D>,
    ) -> Result<(), String>
    where
        K: mz_persist_types::Codec + std::fmt::Debug,
        V: mz_persist_types::Codec + std::fmt::Debug,
        D: differential_dataflow::difference::Semigroup + Codec64,
    {
        use mz_proto::RustType;
        use prost::Message;

        use crate::internal::state::ProtoStateDiff;

        let mut roundtrip_state = from_state.clone(
            from_state.applier_version.clone(),
            from_state.hostname.clone(),
        );
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

impl<T: Timestamp + Lattice + Codec64> State<T> {
    pub fn apply_encoded_diffs<'a, I: IntoIterator<Item = &'a VersionedData>>(
        &mut self,
        cfg: &PersistConfig,
        metrics: &Metrics,
        diffs: I,
    ) {
        let mut state_seqno = self.seqno;
        let diffs = diffs.into_iter().filter_map(move |x| {
            if x.seqno != state_seqno.next() {
                // No-op.
                return None;
            }
            let data = x.data.clone();
            let diff = metrics
                .codecs
                .state_diff
                // Note: `x.data` is a `Bytes`, so cloning just increments a ref count
                .decode(|| StateDiff::decode(&cfg.build_version, x.data.clone()));
            assert_eq!(diff.seqno_from, state_seqno);
            state_seqno = diff.seqno_to;
            Some((diff, data))
        });
        self.apply_diffs(metrics, diffs);
    }
}

impl<T: Timestamp + Lattice + Codec64> State<T> {
    pub fn apply_diffs<I: IntoIterator<Item = (StateDiff<T>, Bytes)>>(
        &mut self,
        metrics: &Metrics,
        diffs: I,
    ) {
        for (diff, data) in diffs {
            // TODO: This could special-case batch apply for diffs where it's
            // more efficient (in particular, spine batches that hit the slow
            // path).
            match self.apply_diff(metrics, diff) {
                Ok(()) => {}
                Err(err) => {
                    // Having the full diff in the error message is critical for debugging any
                    // issues that may arise from diff application. We pass along the original
                    // Bytes it decoded from just so we can decode in this error path, while
                    // avoiding any extraneous clones in the expected Ok path.
                    let diff = StateDiff::<T>::decode(&self.applier_version, data);
                    panic!(
                        "state diff should apply cleanly: {} diff {:?} state {:?}",
                        err, diff, self
                    )
                }
            }
        }
    }

    // Intentionally not even pub(crate) because all callers should use
    // [Self::apply_diffs].
    pub(super) fn apply_diff(
        &mut self,
        metrics: &Metrics,
        diff: StateDiff<T>,
    ) -> Result<(), String> {
        // Deconstruct diff so we get a compile failure if new fields are added.
        let StateDiff {
            applier_version: diff_applier_version,
            seqno_from: diff_seqno_from,
            seqno_to: diff_seqno_to,
            walltime_ms: diff_walltime_ms,
            latest_rollup_key: _,
            rollups: diff_rollups,
            hostname: diff_hostname,
            last_gc_req: diff_last_gc_req,
            leased_readers: diff_leased_readers,
            critical_readers: diff_critical_readers,
            writers: diff_writers,
            schemas: diff_schemas,
            since: diff_since,
            legacy_batches: diff_legacy_batches,
            hollow_batches: diff_hollow_batches,
            spine_batches: diff_spine_batches,
            merges: diff_merges,
        } = diff;
        if self.seqno == diff_seqno_to {
            return Ok(());
        }
        if self.seqno != diff_seqno_from {
            return Err(format!(
                "could not apply diff {} -> {} to state {}",
                diff_seqno_from, diff_seqno_to, self.seqno
            ));
        }
        self.seqno = diff_seqno_to;
        self.applier_version = diff_applier_version;
        self.walltime_ms = diff_walltime_ms;
        force_apply_diffs_single(
            &self.shard_id,
            diff_seqno_to,
            "hostname",
            diff_hostname,
            &mut self.hostname,
            metrics,
        )?;

        // Deconstruct collections so we get a compile failure if new fields are
        // added.
        let StateCollections {
            last_gc_req,
            rollups,
            leased_readers,
            critical_readers,
            writers,
            schemas,
            trace,
        } = &mut self.collections;

        apply_diffs_map("rollups", diff_rollups, rollups)?;
        apply_diffs_single("last_gc_req", diff_last_gc_req, last_gc_req)?;
        apply_diffs_map("leased_readers", diff_leased_readers, leased_readers)?;
        apply_diffs_map("critical_readers", diff_critical_readers, critical_readers)?;
        apply_diffs_map("writers", diff_writers, writers)?;
        apply_diffs_map("schemas", diff_schemas, schemas)?;

        let structure_unchanged = diff_hollow_batches.is_empty()
            && diff_spine_batches.is_empty()
            && diff_merges.is_empty();
        let spine_unchanged =
            diff_since.is_empty() && diff_legacy_batches.is_empty() && structure_unchanged;

        if spine_unchanged {
            return Ok(());
        }

        let mut flat = {
            metrics.state.apply_spine_flattened.inc();
            let mut flat = trace.flatten();
            apply_diffs_single("since", diff_since, &mut flat.since)?;
            apply_diffs_map(
                "legacy_batches",
                diff_legacy_batches
                    .into_iter()
                    .map(|StateFieldDiff { key, val }| StateFieldDiff {
                        key: Arc::new(key),
                        val,
                    }),
                &mut flat.legacy_batches,
            )?;
            Some(flat)
        };

        if !structure_unchanged {
            let flat = flat.get_or_insert_with(|| trace.flatten());
            apply_diffs_map(
                "hollow_batches",
                diff_hollow_batches,
                &mut flat.hollow_batches,
            )?;
            apply_diffs_map("spine_batches", diff_spine_batches, &mut flat.spine_batches)?;
            apply_diffs_map("merges", diff_merges, &mut flat.merges)?;
        }

        if let Some(flat) = flat {
            *trace = Trace::unflatten(flat)?;
        }

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

// A hack to force apply a diff, making `single` equal to
// the Update `to` value, ignoring a mismatch on `from`.
// Used to migrate forward after writing down incorrect
// diffs.
//
// TODO: delete this once `hostname` has zero mismatches
fn force_apply_diffs_single<X: PartialEq + Debug>(
    shard_id: &ShardId,
    seqno: SeqNo,
    name: &str,
    diffs: Vec<StateFieldDiff<(), X>>,
    single: &mut X,
    metrics: &Metrics,
) -> Result<(), String> {
    for diff in diffs {
        force_apply_diff_single(shard_id, seqno, name, diff, single, metrics)?;
    }
    Ok(())
}

fn force_apply_diff_single<X: PartialEq + Debug>(
    shard_id: &ShardId,
    seqno: SeqNo,
    name: &str,
    diff: StateFieldDiff<(), X>,
    single: &mut X,
    metrics: &Metrics,
) -> Result<(), String> {
    match diff.val {
        Update(from, to) => {
            if single != &from {
                debug!(
                    "{}: update didn't match: {:?} vs {:?}, continuing to force apply diff to {:?} for shard {} and seqno {}",
                    name, single, &from, &to, shard_id, seqno
                );
                metrics.state.force_apply_hostname.inc();
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
    diffs: impl IntoIterator<Item = StateFieldDiff<K, V>>,
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

/// A type that facilitates the proto encoding of a [`ProtoStateFieldDiffs`]
///
/// [`ProtoStateFieldDiffs`] is a columnar encoding of [`StateFieldDiff`]s, see
/// its doc comment for more info. The underlying buffer for a [`ProtoStateFieldDiffs`]
/// is a [`Bytes`] struct, which is an immutable, shared, reference counted,
/// buffer of data. Using a [`Bytes`] struct is a very efficient way to manage data
/// becuase multiple [`Bytes`] can reference different parts of the same underlying
/// portion of memory. See its doc comment for more info.
///
/// A [`ProtoStateFieldDiffsWriter`] maintains a mutable, unique, data buffer, i.e.
/// a [`BytesMut`], which we use when encoding a [`StateFieldDiff`]. And when
/// finished encoding, we convert it into a [`ProtoStateFieldDiffs`] by "freezing" the
/// underlying buffer, converting it into a [`Bytes`] struct, so it can be shared.
///
/// [`Bytes`]: bytes::Bytes
#[derive(Debug)]
pub struct ProtoStateFieldDiffsWriter {
    data_buf: BytesMut,
    proto: ProtoStateFieldDiffs,
}

impl ProtoStateFieldDiffsWriter {
    /// Record a [`ProtoStateField`] for our columnar encoding.
    pub fn push_field(&mut self, field: ProtoStateField) {
        self.proto.fields.push(i32::from(field));
    }

    /// Record a [`ProtoStateFieldDiffType`] for our columnar encoding.
    pub fn push_diff_type(&mut self, diff_type: ProtoStateFieldDiffType) {
        self.proto.diff_types.push(i32::from(diff_type));
    }

    /// Encode a message for our columnar encoding.
    pub fn encode_proto<M: prost::Message>(&mut self, msg: &M) {
        let len_before = self.data_buf.len();
        self.data_buf.reserve(msg.encoded_len());

        // Note: we use `encode_raw` as opposed to `encode` because all `encode` does is
        // check to make sure there's enough bytes in the buffer to fit our message
        // which we know there are because we just reserved the space. When benchmarking
        // `encode_raw` does offer a slight performance improvement over `encode`.
        msg.encode_raw(&mut self.data_buf);

        // Record exactly how many bytes were written.
        let written_len = self.data_buf.len() - len_before;
        self.proto.data_lens.push(u64::cast_from(written_len));
    }

    pub fn into_proto(self) -> ProtoStateFieldDiffs {
        let ProtoStateFieldDiffsWriter {
            data_buf,
            mut proto,
        } = self;

        // Assert we didn't write into the proto's data_bytes field
        assert!(proto.data_bytes.is_empty());

        // Move our buffer into the proto
        let data_bytes = data_buf.freeze();
        proto.data_bytes = data_bytes;

        proto
    }
}

impl ProtoStateFieldDiffs {
    pub fn into_writer(mut self) -> ProtoStateFieldDiffsWriter {
        // Create a new buffer which we'll encode data into.
        let mut data_buf = BytesMut::with_capacity(self.data_bytes.len());

        // Take our existing data, and copy it into our buffer.
        let existing_data = std::mem::take(&mut self.data_bytes);
        data_buf.extend(existing_data);

        ProtoStateFieldDiffsWriter {
            data_buf,
            proto: self,
        }
    }

    pub fn iter<'a>(&'a self) -> ProtoStateFieldDiffsIter<'a> {
        let len = self.fields.len();
        assert_eq!(self.diff_types.len(), len);

        ProtoStateFieldDiffsIter {
            len,
            diff_idx: 0,
            data_idx: 0,
            data_offset: 0,
            diffs: self,
        }
    }

    pub fn validate(&self) -> Result<(), String> {
        if self.fields.len() != self.diff_types.len() {
            return Err(format!(
                "fields {} and diff_types {} lengths disagree",
                self.fields.len(),
                self.diff_types.len()
            ));
        }

        let mut expected_data_slices = 0;
        for diff_type in self.diff_types.iter() {
            // We expect one for the key.
            expected_data_slices += 1;
            // And 1 or 2 for val depending on the diff type.
            match ProtoStateFieldDiffType::try_from(*diff_type) {
                Ok(ProtoStateFieldDiffType::Insert) => expected_data_slices += 1,
                Ok(ProtoStateFieldDiffType::Update) => expected_data_slices += 2,
                Ok(ProtoStateFieldDiffType::Delete) => expected_data_slices += 1,
                Err(_) => return Err(format!("unknown diff_type {}", diff_type)),
            }
        }
        if expected_data_slices != self.data_lens.len() {
            return Err(format!(
                "expected {} data slices got {}",
                expected_data_slices,
                self.data_lens.len()
            ));
        }

        let expected_data_bytes = usize::cast_from(self.data_lens.iter().copied().sum::<u64>());
        if expected_data_bytes != self.data_bytes.len() {
            return Err(format!(
                "expected {} data bytes got {}",
                expected_data_bytes,
                self.data_bytes.len()
            ));
        }

        Ok(())
    }
}

#[derive(Debug)]
pub struct ProtoStateFieldDiff<'a> {
    pub key: &'a [u8],
    pub diff_type: ProtoStateFieldDiffType,
    pub from: &'a [u8],
    pub to: &'a [u8],
}

pub struct ProtoStateFieldDiffsIter<'a> {
    len: usize,
    diff_idx: usize,
    data_idx: usize,
    data_offset: usize,
    diffs: &'a ProtoStateFieldDiffs,
}

impl<'a> Iterator for ProtoStateFieldDiffsIter<'a> {
    type Item = Result<(ProtoStateField, ProtoStateFieldDiff<'a>), TryFromProtoError>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.diff_idx >= self.len {
            return None;
        }
        let mut next_data = || {
            let start = self.data_offset;
            let end = start + usize::cast_from(self.diffs.data_lens[self.data_idx]);
            let data = &self.diffs.data_bytes[start..end];
            self.data_idx += 1;
            self.data_offset = end;
            data
        };
        let field = match ProtoStateField::try_from(self.diffs.fields[self.diff_idx]) {
            Ok(x) => x,
            Err(_) => {
                return Some(Err(TryFromProtoError::unknown_enum_variant(format!(
                    "ProtoStateField({})",
                    self.diffs.fields[self.diff_idx]
                ))))
            }
        };
        let diff_type =
            match ProtoStateFieldDiffType::try_from(self.diffs.diff_types[self.diff_idx]) {
                Ok(x) => x,
                Err(_) => {
                    return Some(Err(TryFromProtoError::unknown_enum_variant(format!(
                        "ProtoStateFieldDiffType({})",
                        self.diffs.diff_types[self.diff_idx]
                    ))))
                }
            };
        let key = next_data();
        let (from, to): (&[u8], &[u8]) = match diff_type {
            ProtoStateFieldDiffType::Insert => (&[], next_data()),
            ProtoStateFieldDiffType::Update => (next_data(), next_data()),
            ProtoStateFieldDiffType::Delete => (next_data(), &[]),
        };
        let diff = ProtoStateFieldDiff {
            key,
            diff_type,
            from,
            to,
        };
        self.diff_idx += 1;
        Some(Ok((field, diff)))
    }
}

#[cfg(test)]
mod tests {
    use semver::Version;

    use crate::internal::paths::{PartId, PartialBatchKey, RollupId, WriterKey};
    use mz_ore::metrics::MetricsRegistry;

    use crate::internal::state::TypedState;
    use crate::internal::trace::FueledMergeRes;
    use crate::ShardId;

    use super::*;

    /// Model a situation where a "leader" is constantly making changes to its state, and a "follower"
    /// is applying those changes as diffs.
    #[mz_ore::test]
    #[cfg_attr(miri, ignore)] // too slow
    fn test_state_sync() {
        use proptest::prelude::*;

        #[derive(Debug, Clone)]
        enum Action {
            /// Append a (non)empty batch to the shard that covers the given length of time.
            Append { empty: bool, time_delta: u64 },
            /// Apply the Nth compaction request we've received to the shard state.
            Compact { req: usize },
        }

        let action_gen: BoxedStrategy<Action> = {
            prop::strategy::Union::new([
                (any::<bool>(), 1u64..10u64)
                    .prop_map(|(empty, time_delta)| Action::Append { empty, time_delta })
                    .boxed(),
                (0usize..10usize)
                    .prop_map(|req| Action::Compact { req })
                    .boxed(),
            ])
            .boxed()
        };

        fn run(actions: Vec<Action>, metrics: &Metrics) {
            let version = Version::new(0, 100, 0);
            let writer_key = WriterKey::Version(version.to_string());
            let id = ShardId::new();
            let hostname = "computer";
            let typed: TypedState<String, (), u64, i64> =
                TypedState::new(version, id, hostname.to_string(), 0);
            let mut leader = typed.state;

            let seqno = SeqNo::minimum();
            let mut lower = 0u64;
            let mut merge_reqs = vec![];

            leader.collections.rollups.insert(
                seqno,
                HollowRollup {
                    key: PartialRollupKey::new(seqno, &RollupId::new()),
                    encoded_size_bytes: None,
                },
            );
            let mut follower = leader.clone();

            for action in actions {
                // Apply the given action and the new roundtrip_structure setting and take a diff.
                let mut old_leader = leader.clone();
                match action {
                    Action::Append { empty, time_delta } => {
                        let upper = lower + time_delta;
                        let key = if empty {
                            None
                        } else {
                            let id = PartId::new();
                            Some(PartialBatchKey::new(&writer_key, &id))
                        };

                        let keys = key.as_ref().map(|k| k.0.as_str());
                        let reqs = leader.collections.trace.push_batch(
                            crate::internal::state::tests::hollow(
                                lower,
                                upper,
                                keys.as_slice(),
                                if empty { 0 } else { 1 },
                            ),
                        );
                        merge_reqs.extend(reqs);
                        lower = upper;
                    }
                    Action::Compact { req } => {
                        if !merge_reqs.is_empty() {
                            let req = merge_reqs.remove(req.min(merge_reqs.len() - 1));
                            let len = req.inputs.iter().map(|p| p.batch.len).sum();
                            let parts = req
                                .inputs
                                .into_iter()
                                .flat_map(|p| p.batch.parts.clone())
                                .collect();
                            let output = HollowBatch::new_run(req.desc, parts, len);
                            leader
                                .collections
                                .trace
                                .apply_merge_res(&FueledMergeRes { output });
                        }
                    }
                }
                leader.seqno.0 += 1;
                let diff = StateDiff::from_diff(&old_leader, &leader);

                // Validate that the diff applies to both the previous state (also checked in
                // debug asserts) and our follower that's only synchronized via diffs.
                old_leader
                    .apply_diff(metrics, diff.clone())
                    .expect("diff applies to the old version of the leader state");
                follower
                    .apply_diff(metrics, diff.clone())
                    .expect("diff applies to the synced version of the follower state");

                // TODO: once spine structure is roundtripped through diffs, assert that the follower
                // has the same batches etc. as the leader does.
            }
        }

        let config = PersistConfig::new_for_tests();
        let metrics_registry = MetricsRegistry::new();
        let metrics: Metrics = Metrics::new(&config, &metrics_registry);

        proptest!(|(actions in prop::collection::vec(action_gen, 1..20))| {
            run(actions, &metrics)
        })
    }
}

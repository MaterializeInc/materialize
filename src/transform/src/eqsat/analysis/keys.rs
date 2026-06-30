// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.

//! Unique-key analysis: tracks which sets of columns functionally determine a
//! row of a relation whose rows have multiplicity at most one.

use std::collections::{BTreeMap, BTreeSet};

use crate::eqsat::core::{Analysis, Id};
use crate::eqsat::egraph::{CNode, CombinedLang, ENode};
use crate::eqsat::ir::{Col, Rel};

use super::RelCtx;
use super::recursion::{Direction, RecAnalysis, rec_analyze};

/// A unique key: a set of columns that functionally determines a row, on a
/// relation whose rows have multiplicity at most one.
pub type Key = BTreeSet<Col>;
/// The set of known keys of a relation.
pub type KeySet = BTreeSet<Key>;

/// Unique-key analysis.
///
/// Conservative and sound: `Reduce` establishes its group-key columns as a key
/// (it emits one row per group); `Filter`/`Map`/`Project`/`Threshold` preserve
/// keys (with `Project` remapping column indices); a `Join` keys on the union
/// of one key per input (offset into the join's column layout); everything else
/// yields no keys. `merge` unions, since a key proved by any equivalent form
/// holds.
///
/// The join case is what brings key reasoning to `Join`s — the relational core
/// of `redundant_join`/`semijoin_idempotence`. It lets `reduce_elision` see
/// that grouping a join by a join-key is redundant. (Dropping a *whole* join
/// input — the rest of those transforms — needs reasoning about the join's
/// equivalence scalars, which the opaque-scalar design deliberately forbids;
/// see `COVERAGE.md`.)
#[derive(Default)]
pub struct Keys {
    pub locals: BTreeMap<usize, KeySet>,
}

impl Analysis<CombinedLang> for Keys {
    type Domain = KeySet;
    type Ctx<'a> = RelCtx<'a>;

    fn bottom(&self) -> KeySet {
        KeySet::new()
    }

    fn make(&self, node: &CNode, get: &dyn Fn(Id) -> KeySet, ctx: Self::Ctx<'_>) -> KeySet {
        let CNode::Rel(node) = node else {
            return self.bottom();
        };
        let arity = ctx.arity;
        match node {
            // Grouping emits one row per group: cols 0..|group_key| are a key.
            ENode::Reduce { group_key, .. } => {
                let mut s = KeySet::new();
                s.insert((0..group_key.len()).collect());
                s
            }
            // Selecting/appending columns keeps the input's keys (input column
            // indices are unchanged). ArrangeBy and ArrangeByMany are multiset
            // identities, so they also preserve the input's keys. IndexedFilter
            // is a Filter, so it preserves them too.
            ENode::Filter { input, .. }
            | ENode::Map { input, .. }
            | ENode::ArrangeBy { input, .. }
            | ENode::ArrangeByMany { input, .. }
            | ENode::IndexedFilter { input, .. }
            | ENode::Threshold { input } => get(*input),
            ENode::Project { input, outputs } => project_keys(&get(*input), outputs),
            // The columns of a join are the concatenation of its inputs', so a
            // key of each input (offset by that input's start column) unions to
            // a key of the join.
            ENode::Join { inputs, .. } | ENode::WcoJoin { inputs, .. } => {
                join_keys(inputs, &|i| get(i), &|i| arity(i))
            }
            // A recursive reference: keys proven by the Rel-level fixpoint.
            ENode::LocalGet { id, .. } => self.locals.get(id).cloned().unwrap_or_default(),
            // No keys established (or, for Negate/Constant/Get, not known).
            _ => KeySet::new(),
        }
    }

    fn merge(&self, mut a: KeySet, b: KeySet) -> KeySet {
        a.extend(b);
        a
    }
}

/// Keys of a join of `inputs`: pick one key per input, offset it by that
/// input's start column, and union across inputs. The result is the set of all
/// such combinations. If any input has no known key, the join has none (that
/// input's columns are then undetermined).
fn join_keys(
    inputs: &[Id],
    keys_of: &dyn Fn(Id) -> KeySet,
    arity_of: &dyn Fn(Id) -> usize,
) -> KeySet {
    let parts: Vec<(KeySet, usize)> = inputs.iter().map(|&i| (keys_of(i), arity_of(i))).collect();
    combine_join_keys(&parts)
}

/// Combine per-input `(keyset, arity)` (in column order) into the join's keys:
/// every choice of one key per input, each offset by the running column start.
/// If any input has no known key, the join has none.
pub(crate) fn combine_join_keys(parts: &[(KeySet, usize)]) -> KeySet {
    let mut offset = 0usize;
    let mut combos: Vec<Key> = vec![Key::new()];
    for (ks, ar) in parts {
        if ks.is_empty() {
            return KeySet::new();
        }
        let off = offset;
        let mut next = Vec::new();
        for base in &combos {
            for k in ks {
                let mut nk = base.clone();
                nk.extend(k.iter().map(|c| c + off));
                next.push(nk);
            }
        }
        combos = next;
        offset += ar;
    }
    combos.into_iter().collect()
}

/// Push a key set through a projection `outputs` (a list of source columns):
/// a key survives iff all its columns are retained, remapped to their output
/// positions.
pub(crate) fn project_keys(keys: &KeySet, outputs: &[usize]) -> KeySet {
    let retained: BTreeSet<usize> = outputs.iter().copied().collect();
    let mut out = KeySet::new();
    for k in keys {
        if k.is_subset(&retained) {
            let mapped: Key = (0..outputs.len())
                .filter(|&i| k.contains(&outputs[i]))
                .collect();
            out.insert(mapped);
        }
    }
    out
}

/// Whether `cand` (a candidate key, as a column set) is a superkey of some
/// known key in `keys` — i.e. it determines the row.
pub fn is_superkey(keys: &KeySet, cand: &Key) -> bool {
    keys.iter().any(|k| k.is_subset(cand))
}

/// Unique keys as a recursion-aware (least-fixpoint) under-approximation: a
/// recursive reference contributes no key until the body proves one, which is
/// the sound conservative direction. (The *precise* recursive key is a greatest
/// fixpoint, but its ⊤ — "every column subset is a key" — is unbounded, so we
/// keep the safe Lfp answer; see `COVERAGE.md`.)
pub(crate) struct KeysRec;
impl RecAnalysis for KeysRec {
    type Domain = KeySet;
    fn start(&self) -> KeySet {
        KeySet::new()
    }
    fn direction(&self) -> Direction {
        Direction::Lfp
    }
    fn make(&self, rel: &Rel, child: &dyn Fn(&Rel) -> KeySet) -> KeySet {
        match rel {
            Rel::Reduce { group_key, .. } => {
                let mut s = KeySet::new();
                s.insert((0..group_key.len()).collect());
                s
            }
            Rel::Filter { input, .. } | Rel::Map { input, .. } | Rel::Threshold { input } => {
                child(input)
            }
            Rel::Project { input, outputs } => project_keys(&child(input), outputs),
            Rel::Join { inputs, .. } | Rel::WcoJoin { inputs, .. } => {
                let parts: Vec<(KeySet, usize)> =
                    inputs.iter().map(|r| (child(r), r.arity())).collect();
                combine_join_keys(&parts)
            }
            _ => KeySet::new(),
        }
    }
}

/// Unique keys of a `Rel` (recursion-aware; conservative across `LetRec`).
pub fn rel_keys(rel: &Rel) -> KeySet {
    rec_analyze(&KeysRec, rel)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn get(name: &str, arity: usize) -> Rel {
        Rel::Get {
            name: name.into(),
            arity,
        }
    }

    fn col0() -> crate::eqsat::ir::EScalar {
        crate::eqsat::ir::EScalar::plain(mz_expr::MirScalarExpr::column(0))
    }

    #[mz_ore::test]
    fn reduce_establishes_a_key_preserved_by_filter() {
        // Filter(Reduce by #0 of R): the group key {0} is a key, kept by Filter.
        let plan = Rel::Filter {
            predicates: vec![col0()],
            input: Box::new(Rel::Reduce {
                input: Box::new(get("R", 2)),
                group_key: vec![col0()],
                aggregates: vec![],
                monotonic: false,
                expected_group_size: None,
            }),
        };
        let keys = rel_keys(&plan);
        assert!(is_superkey(&keys, &[0].into_iter().collect()));
        // A plain Get has no known keys.
        assert!(rel_keys(&get("R", 2)).is_empty());
    }

    #[mz_ore::test]
    fn join_keys_union_offset_input_keys() {
        // Join two relations each keyed on {0}. The first input occupies cols
        // 0.. and the second cols 1.. (each Reduce has arity 1), so the join's
        // key is {0, 1}.
        let keyed = |name: &str| Rel::Reduce {
            input: Box::new(get(name, 2)),
            group_key: vec![col0()],
            aggregates: vec![],
            monotonic: false,
            expected_group_size: None,
        };
        let join = Rel::Join {
            inputs: vec![keyed("R"), keyed("S")],
            equivalences: vec![],
        };
        let keys = rel_keys(&join);
        assert!(is_superkey(&keys, &[0, 1].into_iter().collect()));
        // Neither single column alone determines the join row.
        assert!(!is_superkey(&keys, &[0].into_iter().collect()));

        // A join in which one input has no known key has no known key.
        let join_unkeyed = Rel::Join {
            inputs: vec![keyed("R"), get("S", 2)],
            equivalences: vec![],
        };
        assert!(rel_keys(&join_unkeyed).is_empty());
    }
}

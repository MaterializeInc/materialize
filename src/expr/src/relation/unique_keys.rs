// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Unique keys of a relation, represented modulo column equivalences.

use std::collections::btree_map::Entry;
use std::collections::{BTreeMap, BTreeSet};

/// The unique keys of a relation, represented modulo column equivalences.
///
/// Two columns are recorded as equivalent when their values mutually determine
/// each other in every row of the relation, for example columns equated by a
/// filter predicate, or a column and an injective function of it introduced by
/// a map. Any column of a unique key can be replaced by an equivalent column
/// to obtain another unique key. A value of this type therefore describes the
/// family of all column sets that contain some key obtained from a recorded
/// key by substituting columns within their equivalence classes.
///
/// Keys are stored canonically, using only equivalence class representatives.
/// This keeps the representation polynomial in size where an explicit
/// enumeration of keys is exponential: a wide key combined with n disjoint
/// column equalities describes 2^n distinct minimal keys.
///
/// Values are kept in a normal form: representatives are the least column of
/// their class, keys contain only representatives, each key is sorted and
/// deduplicated, no key contains another, and the key list is sorted. Equal
/// values therefore describe equal families. The converse does not hold, as
/// two values can describe the same family through different equivalences,
/// for example when a class touches no key.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct UniqueKeySets {
    /// Maps each column to the representative (least column) of its
    /// equivalence class. Columns at or beyond `reps.len()` are their own
    /// representative. Trailing identity entries are trimmed.
    reps: Vec<usize>,
    /// Minimal antichain of canonical keys.
    keys: Vec<Vec<usize>>,
}

impl UniqueKeySets {
    /// A relation with no known unique keys.
    pub fn none() -> Self {
        Self::default()
    }

    /// A relation with at most one row, whose unique key is the empty set of
    /// columns. This is the greatest element of the key lattice.
    pub fn at_most_one_row() -> Self {
        Self {
            reps: Vec::new(),
            keys: vec![vec![]],
        }
    }

    /// Constructs from a list of keys, for example `ReprRelationType::keys`,
    /// with no known equivalences.
    pub fn from_keys(keys: Vec<Vec<usize>>) -> Self {
        let mut result = Self::default();
        for key in keys {
            result.add_key(key);
        }
        result
    }

    /// The canonical keys: a minimal antichain of sorted column sets, each
    /// containing only equivalence class representatives.
    pub fn keys(&self) -> &[Vec<usize>] {
        &self.keys
    }

    /// Discards the equivalences and returns the canonical keys.
    pub fn into_keys(self) -> Vec<Vec<usize>> {
        self.keys
    }

    /// The representative of `col`'s equivalence class.
    pub fn rep(&self, col: usize) -> usize {
        self.reps.get(col).copied().unwrap_or(col)
    }

    /// All columns in `col`'s equivalence class, in increasing order.
    pub fn equivalent_columns(&self, col: usize) -> impl Iterator<Item = usize> + '_ {
        let rep = self.rep(col);
        // A column with no recorded class is its own sole member. Recorded
        // representatives always lie within `reps`, so exactly one of the two
        // halves below yields anything.
        let unrecorded = (rep >= self.reps.len()).then_some(col);
        (0..self.reps.len())
            .filter(move |c| self.reps[*c] == rep)
            .chain(unrecorded)
    }

    /// True iff `cols` contains a unique key, in which case no two rows of the
    /// relation agree on all of `cols`.
    pub fn is_unique_on<I>(&self, cols: I) -> bool
    where
        I: IntoIterator<Item = usize>,
    {
        let reps: BTreeSet<usize> = cols.into_iter().map(|c| self.rep(c)).collect();
        self.keys
            .iter()
            .any(|key| key.iter().all(|c| reps.contains(c)))
    }

    /// Records that columns `a` and `b` mutually determine each other, and
    /// re-canonicalizes the keys.
    pub fn equate(&mut self, a: usize, b: usize) {
        let ra = self.rep(a);
        let rb = self.rep(b);
        if ra == rb {
            return;
        }
        let lo = std::cmp::min(ra, rb);
        let hi = std::cmp::max(ra, rb);
        if self.reps.len() <= hi {
            self.reps.extend(self.reps.len()..=hi);
        }
        for rep in self.reps.iter_mut() {
            if *rep == hi {
                *rep = lo;
            }
        }
        self.normalize_keys();
    }

    /// Adds a key, canonicalizing it and keeping the antichain minimal.
    pub fn add_key(&mut self, mut key: Vec<usize>) {
        for col in key.iter_mut() {
            *col = self.rep(*col);
        }
        key.sort_unstable();
        key.dedup();
        if self.keys.iter().any(|k| is_subset(k, &key)) {
            return;
        }
        self.keys.retain(|k| !is_subset(&key, k));
        if let Err(pos) = self.keys.binary_search(&key) {
            self.keys.insert(pos, key);
        }
    }

    /// Removes `col`'s entire equivalence class from every key.
    ///
    /// Sound only when `col` is constant across the relation, for example when
    /// a filter equates it with a literal. Every column equivalent to a
    /// constant column is itself constant, so the whole class contributes
    /// nothing to uniqueness.
    pub fn remove_constant_column(&mut self, col: usize) {
        let rep = self.rep(col);
        if !self.keys.iter().any(|key| key.contains(&rep)) {
            return;
        }
        let keys = std::mem::take(&mut self.keys);
        for mut key in keys {
            key.retain(|c| *c != rep);
            self.add_key(key);
        }
    }

    /// The keys of the relation projected onto `outputs`, where output column
    /// `i` is input column `outputs[i]`.
    ///
    /// A key survives when each of its columns has an equivalent column among
    /// the outputs. Equivalences between surviving columns are retained.
    pub fn project(&self, outputs: &[usize]) -> Self {
        let mut positions_by_rep: BTreeMap<usize, Vec<usize>> = BTreeMap::new();
        for (pos, col) in outputs.iter().enumerate() {
            positions_by_rep
                .entry(self.rep(*col))
                .or_default()
                .push(pos);
        }
        let mut result = Self::default();
        for positions in positions_by_rep.values() {
            for pair in positions.windows(2) {
                result.equate(pair[0], pair[1]);
            }
        }
        for key in &self.keys {
            let translated = key
                .iter()
                .map(|c| positions_by_rep.get(c).map(|positions| positions[0]))
                .collect::<Option<Vec<_>>>();
            if let Some(key) = translated {
                result.add_key(key);
            }
        }
        result
    }

    /// The same value with all columns shifted up by `offset`, for embedding
    /// an input relation into a join's global column space.
    pub fn offset_columns(&self, offset: usize) -> Self {
        let mut reps = Vec::new();
        if !self.reps.is_empty() {
            reps.extend(0..offset);
            reps.extend(self.reps.iter().map(|r| r + offset));
        }
        let keys = self
            .keys
            .iter()
            .map(|key| key.iter().map(|c| c + offset).collect())
            .collect();
        Self { reps, keys }
    }

    /// The same equivalences with no keys, for operators that preserve column
    /// relationships but not uniqueness, such as row duplication.
    pub fn equivalences_only(&self) -> Self {
        Self {
            reps: self.reps.clone(),
            keys: Vec::new(),
        }
    }

    /// Incorporates knowledge about the same relation from `other`.
    ///
    /// Both values must describe the same relation. The result records the
    /// union of the equivalences and of the keys.
    pub fn merge(&mut self, other: &Self) {
        for col in 0..other.reps.len() {
            let rep = other.reps[col];
            if rep != col {
                self.equate(col, rep);
            }
        }
        for key in &other.keys {
            self.add_key(key.clone());
        }
    }

    /// The greatest lower bound of `self` and `other` in the key lattice: the
    /// strongest value implied by each of them alone.
    ///
    /// Unlike `merge`, the two values need not describe the same relation.
    /// Columns are equivalent in the result when equivalent in both inputs,
    /// and each result key is the union of a key from each input.
    pub fn meet(&self, other: &Self) -> Self {
        // A value containing the empty key describes every column set, so the
        // meet is exactly the other value.
        if self.keys.first().is_some_and(|key| key.is_empty()) {
            return other.clone();
        }
        if other.keys.first().is_some_and(|key| key.is_empty()) {
            return self.clone();
        }
        let mut result = Self::default();
        // Group columns by their pair of representatives. Columns sharing the
        // pair are equivalent in both inputs, hence in the meet.
        let len = std::cmp::max(self.reps.len(), other.reps.len());
        let mut first_by_pair: BTreeMap<(usize, usize), usize> = BTreeMap::new();
        for col in 0..len {
            let pair = (self.rep(col), other.rep(col));
            match first_by_pair.entry(pair) {
                Entry::Vacant(entry) => {
                    entry.insert(col);
                }
                Entry::Occupied(entry) => {
                    result.equate(*entry.get(), col);
                }
            }
        }
        for key_a in &self.keys {
            for key_b in &other.keys {
                let union = key_a.iter().chain(key_b.iter()).copied().collect();
                result.add_key(union);
            }
        }
        // A key of one input that the other input also recognizes as a key,
        // through its own equivalences, belongs to the meet as well.
        for key in &self.keys {
            if other.is_unique_on(key.iter().copied()) {
                result.add_key(key.clone());
            }
        }
        for key in &other.keys {
            if self.is_unique_on(key.iter().copied()) {
                result.add_key(key.clone());
            }
        }
        result
    }

    /// Restores the normal form after equivalence classes have changed.
    fn normalize_keys(&mut self) {
        let keys = std::mem::take(&mut self.keys);
        for key in keys {
            self.add_key(key);
        }
        while self
            .reps
            .last()
            .is_some_and(|rep| *rep == self.reps.len() - 1)
        {
            self.reps.pop();
        }
    }
}

/// True iff sorted slice `a` is a subset of sorted slice `b`.
fn is_subset(a: &[usize], b: &[usize]) -> bool {
    a.iter().all(|x| b.binary_search(x).is_ok())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[mz_ore::test]
    fn from_keys_minimizes_antichain() {
        let keys = UniqueKeySets::from_keys(vec![vec![2, 0, 2], vec![0, 1, 2], vec![3]]);
        assert_eq!(keys.keys(), &[vec![0, 2], vec![3]]);
    }

    #[mz_ore::test]
    fn equate_canonicalizes_keys() {
        let mut keys = UniqueKeySets::from_keys(vec![vec![1, 3], vec![2, 3]]);
        keys.equate(1, 2);
        assert_eq!(keys.keys(), &[vec![1, 3]]);
        assert_eq!(keys.rep(2), 1);
        assert_eq!(keys.equivalent_columns(2).collect::<Vec<_>>(), vec![1, 2]);
    }

    #[mz_ore::test]
    fn is_unique_on_uses_equivalences() {
        let mut keys = UniqueKeySets::from_keys(vec![vec![0, 1]]);
        keys.equate(0, 5);
        keys.equate(1, 6);
        assert!(keys.is_unique_on([0, 1]));
        assert!(keys.is_unique_on([5, 6]));
        assert!(keys.is_unique_on([0, 6, 3]));
        assert!(!keys.is_unique_on([5]));
    }

    /// The pathological case for an explicit enumeration: one wide key plus n
    /// disjoint column equalities describes 2^n minimal keys, while this
    /// representation stays at a single canonical key.
    #[mz_ore::test]
    fn exponential_family_stays_compact() {
        let arity = 128;
        let equalities = 24;
        let mut keys = UniqueKeySets::from_keys(vec![(0..arity).collect()]);
        for i in 0..equalities {
            keys.equate(i, i + equalities);
        }
        assert_eq!(keys.keys().len(), 1);
        let canonical: Vec<usize> = (0..arity)
            .filter(|c| !(equalities..2 * equalities).contains(c))
            .collect();
        assert_eq!(keys.keys(), &[canonical]);
        // Every variant is still recognized as a key.
        assert!(keys.is_unique_on((equalities..arity).collect::<Vec<_>>()));
    }

    #[mz_ore::test]
    fn remove_constant_column_drops_class() {
        let mut keys = UniqueKeySets::from_keys(vec![vec![0, 1], vec![2]]);
        keys.equate(0, 3);
        // Column 3 is constant, so its class member 0 leaves the keys.
        keys.remove_constant_column(3);
        assert_eq!(keys.keys(), &[vec![1], vec![2]]);
    }

    #[mz_ore::test]
    fn project_translates_through_equivalences() {
        let mut keys = UniqueKeySets::from_keys(vec![vec![0, 1]]);
        keys.equate(0, 2);
        // Column 0 is dropped, but its equivalent column 2 survives.
        let projected = keys.project(&[2, 1]);
        assert_eq!(projected.keys(), &[vec![0, 1]]);
        // Two surviving columns of one class stay equivalent.
        let projected = keys.project(&[0, 2, 1]);
        assert_eq!(projected.keys(), &[vec![0, 2]]);
        assert_eq!(projected.rep(1), 0);
        // No equivalent column survives, so the key is lost.
        let projected = keys.project(&[1]);
        assert!(projected.keys().is_empty());
    }

    #[mz_ore::test]
    fn meet_intersects_equivalences_and_unions_keys() {
        let mut a = UniqueKeySets::from_keys(vec![vec![0]]);
        a.equate(0, 1);
        a.equate(2, 3);
        let mut b = UniqueKeySets::from_keys(vec![vec![1]]);
        b.equate(0, 1);
        let met = a.meet(&b);
        // Only the equivalence present in both inputs remains.
        assert_eq!(met.rep(1), 0);
        assert_eq!(met.rep(3), 3);
        // The key union {0} ∪ {1} canonicalizes to {0}.
        assert_eq!(met.keys(), &[vec![0]]);
    }

    /// Meeting with the lattice top must preserve the other value exactly,
    /// equivalences included. The LetRec fixpoint starts every binding at top,
    /// so losing equivalences here would silently weaken all recursive key
    /// inference.
    #[mz_ore::test]
    fn meet_with_top_preserves_the_other_value() {
        let mut a = UniqueKeySets::from_keys(vec![vec![1]]);
        a.equate(0, 1);
        let top = UniqueKeySets::at_most_one_row();
        assert_eq!(top.meet(&a), a);
        assert_eq!(a.meet(&top), a);
    }

    #[mz_ore::test]
    fn meet_keeps_keys_recognized_by_both_inputs() {
        let a = UniqueKeySets::from_keys(vec![vec![1]]);
        let mut b = UniqueKeySets::from_keys(vec![vec![0]]);
        b.equate(0, 1);
        // The pairwise union alone would report only {0, 1}, but a's key [1]
        // is also a key of b through b's equivalence {0, 1}, so it survives.
        let met = a.meet(&b);
        assert_eq!(met.keys(), &[vec![1]]);
        // The equivalence itself does not survive: it holds only in b.
        assert_eq!(met.rep(1), 1);
    }

    #[mz_ore::test]
    fn merge_unions_knowledge() {
        let mut a = UniqueKeySets::from_keys(vec![vec![0, 1]]);
        a.equate(0, 2);
        let mut b = UniqueKeySets::from_keys(vec![vec![3]]);
        b.equate(2, 4);
        a.merge(&b);
        // Equivalences union transitively: {0, 2} and {2, 4} give {0, 2, 4}.
        assert_eq!(a.rep(4), 0);
        assert_eq!(a.keys(), &[vec![0, 1], vec![3]]);
        assert!(a.is_unique_on([4, 1]));
    }

    #[mz_ore::test]
    fn offset_columns_shifts_everything() {
        let mut keys = UniqueKeySets::from_keys(vec![vec![0, 1]]);
        keys.equate(0, 2);
        let shifted = keys.offset_columns(10);
        assert_eq!(shifted.keys(), &[vec![10, 11]]);
        assert_eq!(shifted.rep(12), 10);
        assert_eq!(shifted.rep(5), 5);
    }
}

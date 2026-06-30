// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.

//! A two-axis, cardinality-free cost model.
//!
//! Plans are scored along two independent axes:
//!
//! * **MEMORY** — the size of arranged (indexed) collections that must be kept
//!   in memory.  Specifically, every operator that arranges its input contributes
//!   a term equal to the worst-case size-degree of that input.  Memory is the
//!   primary scarce resource: memory-first ordering is the default.
//! * **TIME** — the worst-case asymptotic work, measured as the degree (exponent
//!   of `N`) of every operator's processing term.  Time is the secondary axis
//!   used to break memory ties.
//!
//! Both axes use the same representation: a multiset of degrees, compared
//! lexicographically largest-first so the dominant term wins (a plan with a
//! smaller leading degree, or fewer terms at equal leading degree, is cheaper).
//!
//! ## Memory terms per operator
//!
//! * [`Rel::Reduce`] (group key), [`Rel::TopK`] (group and order keys), and
//!   [`Rel::ArrangeBy`] (its explicit key) each arrange their input — one term
//!   at `size_degree(input)`.
//! * [`Rel::Join`] persistently arranges its per-input collections and the
//!   intermediates of the chosen join order; the final whole-join output is
//!   streamed to the parent, not arranged, so it carries no memory term. The
//!   terms are one per input at `size_degree(input_i)` (with the same
//!   index-availability suppression WcoJoin uses) plus every intermediate
//!   degree from `CostModel::binary_join_terms` except the last (the
//!   transient final output). So a triangle binary-join contributes
//!   [2.0, 1.0, 1.0, 1.0] (the genuine intermediate at 2.0 plus the three
//!   input arrangements), and a 2-way binary join contributes [1.0, 1.0]
//!   (just the two input arrangements), matching WcoJoin for the 2-way case.
//! * [`Rel::WcoJoin`] arranges every input for the leapfrog/generic join —
//!   one term per input at `size_degree(input_i)` (so a triangle WcoJoin
//!   contributes [1.0, 1.0, 1.0]).
//! * All other operators do not arrange their inputs; they have no memory term.
//!
//! ## Time terms per operator
//!
//! Unchanged from the original single-axis model: every operator contributes a
//! work term equal to the degree of the data it processes (joins use the
//! AGM-bound degree of the full join, binary joins use the intermediates of the
//! best left-deep order).
//!
//! ## WcoJoin vs binary-Join on the triangle
//!
//! Binary join: TIME max term = 2.0, MEMORY = [2.0, 1.0, 1.0, 1.0].
//! WcoJoin:     TIME max term = 1.5, MEMORY = [1.0, 1.0, 1.0].
//!
//! WcoJoin dominates on **both** axes: its memory has a smaller leading term
//! (1.0 vs 2.0), and its time is lower (1.5 vs 2.0).

use crate::eqsat::ir::{EScalar, Rel};
use mz_expr::{Columns, Id, MirRelationExpr, MirScalarExpr};
use mz_repr::GlobalId;
use std::cell::RefCell;
use std::collections::{BTreeMap, BTreeSet};

/// Numerical slack for comparing degrees.
const EPS: f64 = 1e-9;

/// Maximum join arity for the exact `2^n` subset-DP join-order search in
/// [`CostModel::binary_join_terms`]. Above this, the DP (and its per-subset
/// combinatorial LP) is unaffordable, so a left-deep chain estimate is used
/// instead. Real joins are far below this; wide joins are rare and tolerate the
/// coarser estimate.
const MAX_EXACT_JOIN_INPUTS: usize = 8;

/// Identity of a persistent arrangement, used to charge each distinct one at
/// most once on the memory axis. `Node` covers the self-identifying arranging
/// operators (`ArrangeBy`/`Reduce`/`TopK`, where the node itself determines the
/// arrangement); `JoinInput` covers a join input arranged by its join key, so
/// the same collection arranged by the same key for several joins (or both
/// sides of a self-join) collapses to one term.
#[derive(PartialEq, Eq, PartialOrd, Ord)]
enum ArrId {
    Node(Rel),
    JoinInput { input: Rel, key: BTreeSet<usize> },
}

/// The two-axis abstract cost of a plan.
#[derive(Clone, Debug, PartialEq)]
pub struct Cost {
    /// Count of distinct maintained arrangements, reuse-aware: arrangements
    /// covered by an existing index or materialized view (the oracle) and
    /// arrangements shared within the plan are excluded. This is the primary
    /// memory quantity the arrangement-count objective minimizes.
    pub arrangements: usize,
    /// Memory-term degrees (sorted descending, entries ≤ EPS dropped).
    ///
    /// Each entry is the worst-case size-degree of an arranged collection that
    /// must live in memory.  A larger entry, or an extra entry, means more
    /// memory pressure.
    pub memory: Vec<f64>,
    /// Time-term degrees (sorted descending, entries ≤ EPS dropped).
    ///
    /// Each entry represents the work done by one operator proportional to
    /// `N^degree`.  A larger entry, or an extra entry, means more CPU work.
    pub time: Vec<f64>,
    /// Total node count; a structural tie-breaker so that algebraic
    /// simplifications that delete a node are strictly preferred.
    pub nodes: usize,
}

impl Cost {
    /// Compare two costs with memory as the primary axis (memory first, then
    /// time, then nodes).
    ///
    /// This is the **default ordering**: memory is the scarce resource.
    pub fn cmp_memory_first(&self, other: &Cost) -> std::cmp::Ordering {
        let ord = cmp_vecs(&self.memory, &other.memory);
        if ord != std::cmp::Ordering::Equal {
            return ord;
        }
        let ord = cmp_vecs(&self.time, &other.time);
        if ord != std::cmp::Ordering::Equal {
            return ord;
        }
        self.nodes.cmp(&other.nodes)
    }

    /// Compare two costs with time as the primary axis (time first, then
    /// memory, then nodes).
    ///
    /// Use this when optimizing for throughput at the cost of extra memory.
    pub fn cmp_time_first(&self, other: &Cost) -> std::cmp::Ordering {
        let ord = cmp_vecs(&self.time, &other.time);
        if ord != std::cmp::Ordering::Equal {
            return ord;
        }
        let ord = cmp_vecs(&self.memory, &other.memory);
        if ord != std::cmp::Ordering::Equal {
            return ord;
        }
        self.nodes.cmp(&other.nodes)
    }

    /// Total ordering delegating to the default (memory-first) comparator.
    #[allow(clippy::should_implement_trait)]
    pub fn cmp(&self, other: &Cost) -> std::cmp::Ordering {
        self.cmp_memory_first(other)
    }

    /// Whether `self` is strictly cheaper than `other` under the default
    /// (memory-first) ordering.
    pub fn lt(&self, other: &Cost) -> bool {
        self.cmp_memory_first(other) == std::cmp::Ordering::Less
    }
}

/// Lexicographic comparison of two degree vectors (descending, missing entry =
/// 0.0): returns Less if `a` has a smaller dominant term (or fewer terms at the
/// same dominant degree), Greater if larger.
pub(crate) fn cmp_vecs(a: &[f64], b: &[f64]) -> std::cmp::Ordering {
    use std::cmp::Ordering::*;
    let n = a.len().max(b.len());
    for i in 0..n {
        let av = a.get(i).copied().unwrap_or(0.0);
        let bv = b.get(i).copied().unwrap_or(0.0);
        if av > bv + EPS {
            return Greater;
        }
        if bv > av + EPS {
            return Less;
        }
    }
    Equal
}

/// Lexicographic "is `a` strictly cheaper than `b`" for delta-join scores
/// `(crosses, degrees)`: fewer forced crosses first (the keyed-ness axis), then
/// a smaller degree vector via `cmp_vecs`. Never constructs a `Cost`.
pub(crate) fn delta_score_lt(a: &(usize, Vec<f64>), b: &(usize, Vec<f64>)) -> bool {
    use std::cmp::Ordering::*;
    match a.0.cmp(&b.0) {
        Less => true,
        Greater => false,
        Equal => cmp_vecs(&a.1, &b.1) == Less,
    }
}

/// The abstract cost model.
///
/// Optionally carries arrangement availability derived from `ctx.indexes`:
/// for each global relation, the set of available index keys (each key is an
/// ordered list of [`MirScalarExpr`]s).  When non-empty, the WcoJoin memory
/// cost for an input whose join key is already covered by an available
/// arrangement is zeroed, making WcoJoin correctly cheap when arrangements
/// exist for free.
///
/// `CostModel::default()` and `CostModel::new()` produce an index-blind model
/// (empty availability), preserving the existing behavior for the logical pass
/// and for callers that do not have index information.
#[derive(Clone, Debug, Default)]
pub struct CostModel {
    /// Available arrangement keys, keyed by the `GlobalId` of the relation they
    /// belong to.  Each inner `Vec<MirScalarExpr>` is one index key (the ordered
    /// list of key columns/expressions reported by the [`IndexOracle`]).
    ///
    /// [`IndexOracle`]: crate::IndexOracle
    available: BTreeMap<GlobalId, Vec<Vec<MirScalarExpr>>>,
    /// Memoization cache for the AGM fractional-edge-cover LP solved by
    /// [`Hypergraph::agm_degree_subset`], keyed by `(hypergraph id, subset
    /// mask)`. The hypergraph id is assigned by [`Self::intern_hg`].
    ///
    /// Extraction re-costs every candidate plan across many passes, and the LP
    /// solve dominates the cost-model profile. The LP result is a pure function
    /// of its inputs, so each distinct query is solved once and reused.
    ///
    /// The key is a compact `(u32, u32)` rather than the full query signature
    /// because the `2^n` subset sweep of one join shares a single hypergraph: a
    /// per-subset `AgmKey` re-cloned the hypergraph's `arities` / `classes` /
    /// `key_covered` vectors on every lookup, which profiled as the dominant
    /// cost-model cost. The descriptor is now cloned once per join (interned via
    /// `hg_ids`) and each subset lookup constructs only a `(u32, u32)` key.
    ///
    /// The model is created fresh per optimization and used single-threaded, so
    /// `RefCell` interior mutability (needed because `cost` takes `&self`) is
    /// sound: there is no cross-thread or re-entrant access.
    agm_cache: RefCell<BTreeMap<(u32, u32), f64>>,
    /// Interning table assigning each distinct hypergraph descriptor a small id,
    /// so the AGM memo keys on `(id, subset)` instead of the full descriptor.
    /// The id-to-descriptor map is a bijection, so the memo is exactly as
    /// precise as keying on the descriptor directly.
    hg_ids: RefCell<BTreeMap<HgDesc, u32>>,
    /// The join-order search strategy. Defaults to `DpSub` (the historical
    /// behavior); an evaluation harness flips it via `MZ_EQSAT_JOIN_ORDERER`.
    join_orderer: JoinOrdererKind,
}

/// The hypergraph-structure part of an [`Hypergraph::agm_degree_subset`] query
/// signature (everything except the `subset` mask), interned to a small id.
///
/// `agm_degree_subset(degs, subset)` reads only: the per-input size degrees
/// (`degs`), the hypergraph structure (`arities`, the per-class set of input
/// indices, and the per-input `key_covered` bits, all captured in
/// [`Hypergraph::build`]), and the `subset` mask. Two queries with equal
/// `HgDesc` and `subset` therefore produce byte-identical results, so the memo
/// is exact: it never changes a cost decision.
///
/// `degs` are stored as raw IEEE-754 bits so the key is `Eq`/`Ord` and an exact
/// match requires bit-identical degrees (no float tolerance, which is correct
/// here because identical inputs yield bit-identical `size_degree` values).
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
struct HgDesc {
    /// Per-input arities (the `Hypergraph::arities` field).
    arities: Vec<usize>,
    /// Per equivalence class, the set of input indices it touches (the
    /// `Hypergraph::classes` field).
    classes: Vec<BTreeSet<usize>>,
    /// Per-input size degrees, as raw `f64` bits.
    deg_bits: Vec<u64>,
    /// Per-input key-covered bits (the `Hypergraph::key_covered` field), which
    /// `agm_degree_subset` reads to drop private cover vertices. Part of the key
    /// so the memo stays exact when two queries differ only in key coverage.
    key_covered: Vec<bool>,
}

impl CostModel {
    /// Create an index-blind cost model (empty availability).
    pub fn new() -> Self {
        CostModel {
            join_orderer: join_orderer_from_env(),
            ..Default::default()
        }
    }

    /// Create a cost model seeded with index availability.
    ///
    /// `available` maps each global relation id to the list of index keys
    /// available on it.  The WcoJoin memory cost for an input that is a direct
    /// global `Get` whose join key is covered by one of these index keys is
    /// zeroed.
    pub fn with_available(available: BTreeMap<GlobalId, Vec<Vec<MirScalarExpr>>>) -> Self {
        CostModel {
            available,
            join_orderer: join_orderer_from_env(),
            ..Default::default()
        }
    }

    /// The index-availability map, so callers building a saturation e-graph can
    /// mirror it onto the e-graph for `reads_indexed_global`.
    pub(crate) fn available(&self) -> &BTreeMap<GlobalId, Vec<Vec<MirScalarExpr>>> {
        &self.available
    }

    /// Override the join-order strategy, bypassing the env var. Test-only seam;
    /// the production/eval path selects via `MZ_EQSAT_JOIN_ORDERER`.
    #[cfg(test)]
    pub(crate) fn with_join_orderer(mut self, kind: JoinOrdererKind) -> Self {
        self.join_orderer = kind;
        self
    }

    /// The worst-case output-size degree of `rel` (exponent of `N`).
    pub fn size_degree(&self, rel: &Rel) -> f64 {
        match rel {
            Rel::Constant { .. } => 0.0,
            // A base collection or an opaque bailed subtree: treat as a base.
            Rel::Get { .. } | Rel::Opaque(_) => 1.0,
            // In the worst case none of these reduce the row count. FlatMap may
            // expand rows (each input row can produce many output rows), so its
            // degree is bounded by the input degree in the worst case used here.
            Rel::Project { input, .. }
            | Rel::Map { input, .. }
            | Rel::FlatMap { input, .. }
            | Rel::Filter { input, .. }
            | Rel::Reduce { input, .. }
            | Rel::TopK { input, .. }
            | Rel::ArrangeBy { input, .. }
            | Rel::ArrangeByMany { input, .. }
            | Rel::Negate { input }
            | Rel::Threshold { input } => self.size_degree(input),
            // An indexed filter probes an index with a fixed set of literal keys,
            // so its output is bounded by the key count, independent of the input
            // size: degree 0. Crucially this is the OUTPUT degree, so an operator
            // sitting above the lookup (e.g. a Project left behind by
            // `push_filter_past_project`) is charged a constant work term, not a
            // full input scan. Treating it as `size_degree(input)` would tie the
            // lookup against a sibling full scan on the time axis and lose the
            // lookup to a node-count tiebreak.
            Rel::IndexedFilter { .. } => 0.0,
            Rel::Union { base, inputs } => {
                let mut d = self.size_degree(base);
                for r in inputs {
                    d = d.max(self.size_degree(r));
                }
                d
            }
            Rel::Join {
                inputs,
                equivalences,
            }
            | Rel::WcoJoin {
                inputs,
                equivalences,
            } => self.join_degree(inputs, equivalences),
            Rel::Let { body, .. } | Rel::LetRec { body, .. } => self.size_degree(body),
            // Approximation: a local reference is treated as a base relation
            // (CSE'd plans are not the cost model's optimization target).
            Rel::LocalGet { .. } => 1.0,
        }
    }

    /// The abstract cost of an entire plan (both axes).
    pub fn cost(&self, rel: &Rel) -> Cost {
        let mut time = Vec::new();
        self.collect_work(rel, &mut time);
        time.retain(|d| *d > EPS);
        time.sort_by(|a, b| b.partial_cmp(a).unwrap());

        let mut memory = Vec::new();
        let arrangements = self.collect_memory(rel, &mut memory);
        memory.retain(|d| *d > EPS);
        memory.sort_by(|a, b| b.partial_cmp(a).unwrap());

        Cost {
            arrangements,
            memory,
            time,
            // Scalar-aware (SP4c): the `node_count` term is the relational tree
            // size (also the CSE ordering key, kept relational-only); the
            // `scalar_node_count` term adds scalar payload size so the tie-breaker
            // prefers cheaper scalar spellings directly.
            nodes: rel.node_count() + rel.scalar_node_count(),
        }
    }

    /// Accumulate the work-term degrees (TIME axis) of every node into `out`.
    fn collect_work(&self, rel: &Rel, out: &mut Vec<f64>) {
        match rel {
            Rel::Constant { .. } | Rel::Get { .. } | Rel::Opaque(_) => {}
            Rel::Project { input, .. }
            | Rel::Map { input, .. }
            | Rel::FlatMap { input, .. }
            | Rel::Filter { input, .. }
            | Rel::Reduce { input, .. }
            | Rel::TopK { input, .. }
            | Rel::ArrangeBy { input, .. }
            | Rel::ArrangeByMany { input, .. }
            | Rel::Negate { input }
            | Rel::Threshold { input } => out.push(self.size_degree(input)),
            // An indexed filter probes an existing index with a fixed set of
            // literal keys, so its work does not scale with the input size: the
            // term is constant (degree 0) and drops out of the time axis. This is
            // what makes the e-graph prefer it over the sibling `Filter(Get)`,
            // whose work term is the full input scan. The child `Get` is read
            // through the index, not scanned, so the recursion below adds nothing.
            Rel::IndexedFilter { .. } => out.push(0.0),
            Rel::Union { base, inputs } => {
                let mut d = self.size_degree(base);
                for r in inputs {
                    d = d.max(self.size_degree(r));
                }
                out.push(d);
            }
            Rel::WcoJoin {
                inputs,
                equivalences,
            } => out.push(self.join_degree(inputs, equivalences)),
            Rel::Join {
                inputs,
                equivalences,
            } => out.extend(self.binary_join_terms(inputs, equivalences)),
            // A `Let` computes its value once; both children are charged via the
            // recursion below, so the shared value is counted a single time. A
            // `LetRec` is charged its per-iteration body cost (the bindings
            // and body, via the recursion below); the iteration count is an
            // unknown multiplier we deliberately abstract away, exactly as we do
            // cardinalities. `LocalGet` is a leaf reference — zero compute cost
            // regardless of whether it carries a RecVersion tag (Prev/Cur/Final).
            Rel::Let { .. } | Rel::LetRec { .. } | Rel::LocalGet { .. } => {}
        }
        for c in rel.children() {
            self.collect_work(c, out);
        }
    }

    /// Accumulate the memory-term degrees (MEMORY axis) of every node into
    /// `out`, returning the count of distinct reuse-aware arrangements charged.
    ///
    /// A term is emitted for each arranged (indexed) collection that must
    /// reside in memory.
    fn collect_memory(&self, rel: &Rel, out: &mut Vec<f64>) -> usize {
        // Charge each DISTINCT persistent arrangement once. A given collection
        // arranged by a given key is built and maintained a single time, then
        // shared by every consumer (after CSE hoists the shared subtree), so a
        // structurally identical arrangement appearing again anywhere in the
        // plan adds no memory. `seen` records the arrangement identities already
        // charged.
        let mut seen: BTreeSet<ArrId> = BTreeSet::new();
        self.collect_memory_into(rel, out, &mut seen);
        seen.len()
    }

    /// Accumulate memory-term degrees, charging each distinct arrangement
    /// (tracked in `seen`) at most once. Join intermediate-result degrees are
    /// transient per join and are not deduplicated.
    fn collect_memory_into(&self, rel: &Rel, out: &mut Vec<f64>, seen: &mut BTreeSet<ArrId>) {
        match rel {
            // Reduce arranges its input by the group key; TopK maintains
            // per-group top-k state arranged by the group and order keys. The
            // arranging node fully identifies the arrangement, so an identical
            // node elsewhere denotes the same materialized arrangement and is
            // not charged again.
            Rel::Reduce { input, .. } | Rel::TopK { input, .. } => {
                if seen.insert(ArrId::Node(rel.clone())) {
                    out.push(self.size_degree(input));
                }
            }
            // ArrangeBy is an explicit arrangement of its input by `key`. An
            // explicit arrangement whose collection and key match an available
            // index already exists, so it is not charged. `arrange_by_oracle_covered`
            // matches a bare opaque global Get under the ArrangeBy against the oracle.
            Rel::ArrangeBy { input, key } => {
                if !self.arrange_by_oracle_covered(input, key)
                    && seen.insert(ArrId::Node(rel.clone()))
                {
                    out.push(self.size_degree(input));
                }
            }
            // Binary join persistently arranges its per-input collections and
            // its intermediate results; the final whole-join output is streamed
            // to the parent, not arranged here, so it carries no memory term.
            // Charge one term per input at size_degree (with the same
            // index-availability suppression WcoJoin uses, so the two join forms
            // are comparable), plus every intermediate degree from
            // binary_join_terms except the last. A per-input arrangement is
            // identified by `(input, join-key columns)`, so the same collection
            // arranged by the same key for several joins (or for both sides of a
            // self-join) is charged once. The terms come from the time-optimal
            // left-deep order, so this charges that order's memory, assuming the
            // engine evaluates a join with one order on both axes rather than
            // choosing a memory-optimal order independently. The last term of
            // binary_join_terms is the AGM degree of the full join (the final
            // output), which is transient, so it is dropped.
            Rel::Join {
                inputs,
                equivalences,
            } => {
                let mut offset = 0usize;
                for input in inputs.iter() {
                    if !self.input_already_arranged(input, offset, equivalences) {
                        let key = join_key_cols_for_input(offset, input.arity(), equivalences);
                        if seen.insert(ArrId::JoinInput {
                            input: input.clone(),
                            key,
                        }) {
                            out.push(self.size_degree(input));
                        }
                    }
                    offset += input.arity();
                }
                let mut terms = self.binary_join_terms(inputs, equivalences);
                // Drop the final-join output degree (the last term); it is
                // streamed to the parent, not persistently arranged.
                terms.pop();
                out.extend(terms);
            }
            // WcoJoin (leapfrog/generic join) arranges every input.
            // An input whose join key is already covered by an available index
            // is not charged the arrangement-build memory term: the arrangement
            // exists for free. Per-input arrangements deduplicate the same way
            // as the binary-join case.
            Rel::WcoJoin {
                inputs,
                equivalences,
            } => {
                let mut offset = 0usize;
                for input in inputs.iter() {
                    if !self.input_already_arranged(input, offset, equivalences) {
                        let key = join_key_cols_for_input(offset, input.arity(), equivalences);
                        if seen.insert(ArrId::JoinInput {
                            input: input.clone(),
                            key,
                        }) {
                            out.push(self.size_degree(input));
                        }
                    }
                    offset += input.arity();
                }
            }
            // A multi-key arrangement maintains one arrangement per key list.
            // Charge each like a single-key ArrangeBy: suppress keys an available
            // index already covers, and dedup against an equivalent single-key
            // ArrangeBy node so the same collection arranged by the same key is
            // charged once whether it came from ArrangeBy or one key of an
            // ArrangeByMany.
            Rel::ArrangeByMany { input, keys } => {
                for key in keys {
                    if !self.arrange_by_oracle_covered(input, key)
                        && seen.insert(ArrId::Node(Rel::ArrangeBy {
                            input: input.clone(),
                            key: key.clone(),
                        }))
                    {
                        out.push(self.size_degree(input));
                    }
                }
            }
            // All other operators do not arrange their inputs.
            _ => {}
        }
        // Recurse into children for all variants.
        for c in rel.children() {
            self.collect_memory_into(c, out, seen);
        }
    }

    /// Whether `input` (the join input at `inp_idx` with concatenated-column
    /// `offset`) is already arranged by the key required by `equivalences`.
    ///
    /// Returns `true` only when:
    /// 1. `input` is directly a `Rel::Opaque` wrapping a global `Get` (a base
    ///    relation with no intervening projections or filters that would shift
    ///    columns), AND
    /// 2. the join key derived from `equivalences` for this input (the set of
    ///    local column indices the equivalences reference inside this input)
    ///    matches the column set of some available index on that global id.
    ///
    /// Only direct opaque-global-get inputs are matched; wrapped inputs (with
    /// a filter or project on top) are conservatively treated as unarranged.
    fn input_already_arranged(
        &self,
        input: &Rel,
        offset: usize,
        equivalences: &[Vec<EScalar>],
    ) -> bool {
        if self.available.is_empty() {
            return false;
        }
        // Only match a bare opaque global Get.
        let gid = match global_id_from_leaf(input) {
            Some(g) => g,
            None => return false,
        };
        let keys = match self.available.get(&gid) {
            Some(ks) => ks,
            None => return false,
        };
        // Compute the set of local column indices (relative to this input's
        // arity) that the equivalences require this input to be keyed by.
        let input_arity = input.arity();
        let join_key_cols = join_key_cols_for_input(offset, input_arity, equivalences);
        if join_key_cols.is_empty() {
            // No join constraint on this input: no arrangement needed.
            return true;
        }
        // Check whether any available index covers exactly the join key columns.
        // An index whose key column set equals the join key column set means the
        // collection is already arranged by exactly what is needed.
        keys.iter().any(|key| {
            // Collect the column indices in this index key (only plain column
            // references; expressions are not matched).
            let idx_cols: BTreeSet<usize> = key.iter().filter_map(|e| e.as_column()).collect();
            idx_cols == join_key_cols
        })
    }

    /// Whether an `ArrangeBy(input, key)` is already covered by an available
    /// index. Matches only a bare opaque global `Get` under the arrangement
    /// (no intervening column-shifting wrappers), against an index whose key
    /// column set equals `key`'s column set.
    ///
    /// `pub(crate)` alias used by [`crate::eqsat::extract`] to check oracle
    /// coverage when building the ILP objective.
    pub(crate) fn arrange_by_oracle_covered_pub(&self, input: &Rel, key: &[EScalar]) -> bool {
        self.arrange_by_oracle_covered(input, key)
    }

    fn arrange_by_oracle_covered(&self, input: &Rel, key: &[EScalar]) -> bool {
        if self.available.is_empty() {
            return false;
        }
        let gid = match global_id_from_leaf(input) {
            Some(g) => g,
            None => return false,
        };
        let keys = match self.available.get(&gid) {
            Some(ks) => ks,
            None => return false,
        };
        let want: BTreeSet<usize> = key.iter().filter_map(|e| e.is_col()).collect();
        if want.len() != key.len() {
            // The key contains a non-column expression an index cannot match.
            return false;
        }
        keys.iter().any(|k| {
            let have: BTreeSet<usize> = k.iter().filter_map(|e| e.as_column()).collect();
            have == want
        })
    }

    /// Intern the hypergraph descriptor `(hg, degs)` to a small id, cloning the
    /// descriptor at most once per distinct hypergraph (not once per subset
    /// lookup). The id-to-descriptor map is a bijection, so memo keys built from
    /// it are exactly as precise as the full descriptor.
    fn intern_hg(&self, hg: &Hypergraph, degs: &[f64]) -> u32 {
        let desc = HgDesc {
            arities: hg.arities.clone(),
            classes: hg.classes.clone(),
            deg_bits: degs.iter().map(|d| d.to_bits()).collect(),
            key_covered: hg.key_covered.clone(),
        };
        let mut ids = self.hg_ids.borrow_mut();
        if let Some(id) = ids.get(&desc) {
            return *id;
        }
        let id = u32::try_from(ids.len()).expect("distinct hypergraph count fits in u32");
        ids.insert(desc, id);
        id
    }

    /// The memoized AGM-bound degree for `subset` of `hg`'s inputs.
    ///
    /// Keyed on the interned hypergraph id so the per-subset lookup constructs
    /// only a `(u32, u32)` key rather than re-cloning the hypergraph vectors.
    /// `hg_id` must be the id returned by [`Self::intern_hg`] for this `(hg,
    /// degs)`; the cached value equals the freshly computed one bit-for-bit.
    fn agm_degree_subset_memo(
        &self,
        hg_id: u32,
        hg: &Hypergraph,
        degs: &[f64],
        subset: u32,
    ) -> f64 {
        let key = (hg_id, subset);
        if let Some(v) = self.agm_cache.borrow().get(&key) {
            return *v;
        }
        let v = hg.agm_degree_subset(degs, subset);
        self.agm_cache.borrow_mut().insert(key, v);
        v
    }

    /// The AGM-bound degree of the full join.
    fn join_degree(&self, inputs: &[Rel], equivalences: &[Vec<EScalar>]) -> f64 {
        let degs: Vec<f64> = inputs.iter().map(|r| self.size_degree(r)).collect();
        let hg = Hypergraph::build(inputs, equivalences);
        let hg_id = self.intern_hg(&hg, &degs);
        let full = if inputs.len() >= 32 {
            u32::MAX
        } else {
            (1u32 << inputs.len()) - 1
        };
        self.agm_degree_subset_memo(hg_id, &hg, &degs, full)
    }

    /// The work terms of a binary-join plan: the intermediate degrees of the
    /// best left-deep order, where "best" minimises the cost vector.
    fn binary_join_terms(&self, inputs: &[Rel], equivalences: &[Vec<EScalar>]) -> Vec<f64> {
        let n = inputs.len();
        let degs: Vec<f64> = inputs.iter().map(|r| self.size_degree(r)).collect();
        let hg = Hypergraph::build(inputs, equivalences);
        if n <= 1 {
            return vec![];
        }
        let hg_id = self.intern_hg(&hg, &degs);
        if n > MAX_EXACT_JOIN_INPUTS {
            // Fallback: a left-deep chain in input order. The exact subset DP is
            // `2^n` and each subset runs a combinatorial LP, so for wide joins it
            // is unaffordable. The left-deep chain costs `n-1` LP solves and is a
            // valid (if coarser) estimate. Shared by all orderers so wide joins
            // never diverge between them.
            let mut set = 1u32;
            let mut terms = Vec::new();
            for i in 1..n {
                set |= 1 << i;
                terms.push(self.agm_degree_subset_memo(hg_id, &hg, &degs, set));
            }
            return terms;
        }
        let g = JoinGraph::from_hypergraph(&hg);
        let agm = |subset: u32| self.agm_degree_subset_memo(hg_id, &hg, &degs, subset);
        match self.join_orderer {
            JoinOrdererKind::DpSub => DpSub.work_terms(&g, &agm),
            JoinOrdererKind::Dpccp => Dpccp.work_terms(&g, &agm),
            JoinOrdererKind::DphypBushy => DphypBushy.work_terms(&g, &agm),
        }
    }

    /// Surface the cost-model-chosen left-deep join order with per-step local
    /// arrangement keys, so the eqsat emitter can commit a `Differential` plan.
    /// The order minimizes the same AGM-degree objective as `binary_join_terms`.
    pub(crate) fn binary_join_order(
        &self,
        inputs: &[Rel],
        equivalences: &[Vec<EScalar>],
    ) -> Option<JoinOrder> {
        let n = inputs.len();
        if n == 0 {
            return None;
        }
        let arities: Vec<usize> = inputs.iter().map(|r| r.arity()).collect();
        let mut offsets = Vec::with_capacity(n);
        let mut acc = 0;
        for &a in &arities {
            offsets.push(acc);
            acc += a;
        }

        let seq: Vec<usize> = if n == 1 {
            vec![0]
        } else if n > MAX_EXACT_JOIN_INPUTS {
            // Wide-join fallback: a left-deep chain in input order, mirroring
            // `binary_join_terms`'s own wide-join fallback.
            (0..n).collect()
        } else {
            let degs: Vec<f64> = inputs.iter().map(|r| self.size_degree(r)).collect();
            let hg = Hypergraph::build(inputs, equivalences);
            let hg_id = self.intern_hg(&hg, &degs);
            let agm = |subset: u32| self.agm_degree_subset_memo(hg_id, &hg, &degs, subset);
            // A step adding `add` onto the inputs in `frontier_mask` is keyed iff
            // `add` has a non-empty frontier key against the placed inputs.
            let is_keyed = |frontier_mask: u32, add: usize| -> bool {
                let placed: Vec<(usize, usize)> = (0..n)
                    .filter(|&j| frontier_mask & (1 << j) != 0)
                    .map(|j| (offsets[j], arities[j]))
                    .collect();
                !frontier_key_cols(offsets[add], arities[add], &placed, equivalences).is_empty()
            };
            best_left_deep_sequence(n, &agm, &is_keyed)?
        };

        let mut placed: Vec<(usize, usize)> = Vec::new();
        let mut steps = Vec::with_capacity(n);
        for (k, &i) in seq.iter().enumerate() {
            let key_cols = if k == 0 {
                // The start is arranged by its full join key (cols equated to any
                // other input); the first edge keys into it.
                join_key_cols_for_input(offsets[i], arities[i], equivalences)
            } else {
                frontier_key_cols(offsets[i], arities[i], &placed, equivalences)
            };
            steps.push(JoinStep { input: i, key_cols });
            placed.push((offsets[i], arities[i]));
        }
        Some(JoinOrder { steps })
    }

    /// The keyed-ness axis + intermediate degrees of a *delta* join over
    /// `inputs` under `equivalences`, summed over per-driver-rooted left-deep
    /// paths (a delta join maintains one path per input).
    ///
    /// Returns `(crosses, degrees)`:
    /// * `crosses` — number of forced cross-product steps across all driver
    ///   paths. A step extending `frontier` by input `j` is *keyed* iff some
    ///   equivalence class touches `j`, touches `frontier` (spans the boundary),
    ///   and has all its inputs within `frontier | {j}`. A non-keyed step is a
    ///   forced cross; it is counted only when neither side is constant (both the
    ///   broadcast frontier and input `j` have size-degree > `EPS`).
    /// * `degrees` — the intermediate-arrangement size-degrees across all paths,
    ///   each path's final full-join term dropped (it is streamed, identical
    ///   across spellings — mirrors `collect_memory_into`'s `terms.pop()`),
    ///   sorted descending.
    ///
    /// The keyed-ness axis (compared first by `delta_score_lt`) is what
    /// `binary_join_terms` cannot see: it costs a single best left-deep order
    /// and roots away from the cross. This is *not* wired into `cost()`/`Cost`;
    /// its only caller is the join-spelling selector.
    pub(crate) fn delta_join_terms(
        &self,
        inputs: &[Rel],
        equivalences: &[Vec<EScalar>],
    ) -> (usize, Vec<f64>) {
        let n = inputs.len();
        if n <= 1 {
            return (0, vec![]);
        }
        // Wide joins exceed the u32 input-mask width used below; a delta join
        // with this many inputs is not a re-spelling target. Bail conservatively
        // (no crosses, no degrees) so the selector keeps the canonical spelling.
        // Mirrors the >= 32 guard in `join_degree`.
        if n >= 32 {
            return (0, vec![]);
        }
        let degs: Vec<f64> = inputs.iter().map(|r| self.size_degree(r)).collect();
        let hg = Hypergraph::build(inputs, equivalences);
        let hg_id = self.intern_hg(&hg, &degs);

        // Per-input column offsets, and a column -> input-index lookup.
        let arities: Vec<usize> = inputs.iter().map(Rel::arity).collect();
        let mut offsets = vec![0usize; n];
        for i in 1..n {
            offsets[i] = offsets[i - 1] + arities[i - 1];
        }
        let input_of_col = |c: usize| -> usize {
            let mut i = 0;
            while i + 1 < n && c >= offsets[i + 1] {
                i += 1;
            }
            i
        };
        // Per equivalence class, the bitmask of inputs its scalars reference.
        let class_masks: Vec<u32> = equivalences
            .iter()
            .map(|class| {
                let mut m = 0u32;
                for escalar in class {
                    for c in escalar.cols() {
                        m |= 1u32 << input_of_col(c);
                    }
                }
                m
            })
            .collect();

        // Can any class key the step that extends `frontier` by input `j`?
        let keys_step = |frontier: u32, j: usize| -> bool {
            let jbit = 1u32 << j;
            let allowed = frontier | jbit;
            class_masks
                .iter()
                .any(|&m| m & jbit != 0 && m & frontier != 0 && (m & !allowed) == 0)
        };

        let mut crosses = 0usize;
        let mut degrees: Vec<f64> = Vec::new();

        for driver in 0..n {
            let mut frontier = 1u32 << driver;
            let mut remaining: Vec<usize> = (0..n).filter(|&i| i != driver).collect();
            let mut path: Vec<f64> = Vec::new();
            while !remaining.is_empty() {
                // Pick the next input: prefer a keyed extension, then the lowest
                // resulting AGM degree. Deterministic.
                let mut best: Option<(bool, f64, usize)> = None; // (is_cross, deg, pos)
                for (pos, &j) in remaining.iter().enumerate() {
                    let deg =
                        self.agm_degree_subset_memo(hg_id, &hg, &degs, frontier | (1u32 << j));
                    let cand = (!keys_step(frontier, j), deg, pos);
                    best = Some(match best {
                        None => cand,
                        Some(b) => {
                            if (cand.0, cand.1) < (b.0, b.1) {
                                cand
                            } else {
                                b
                            }
                        }
                    });
                }
                let (is_cross, deg, pos) = best.expect("remaining is non-empty");
                let j = remaining.remove(pos);
                if is_cross {
                    let frontier_deg = self.agm_degree_subset_memo(hg_id, &hg, &degs, frontier);
                    if frontier_deg > EPS && degs[j] > EPS {
                        crosses += 1;
                    }
                }
                frontier |= 1u32 << j;
                path.push(deg);
            }
            path.pop(); // drop the streamed final-join term
            degrees.extend(path);
        }

        degrees.retain(|d| *d > EPS);
        degrees.sort_by(|a, b| b.partial_cmp(a).unwrap());
        (crosses, degrees)
    }

    /// Surface the per-driver left-deep delta paths: one path per driver (indexed
    /// by driver position, matching the renderer's `join_orders[source_relation]`).
    /// Each returned `Vec<JoinStep>` is the LOOKUPS after the driver (the driver is
    /// not a step), keyed via `frontier_key_cols` against the accumulating per-path
    /// frontier. Returns `None` if any path has a non-keyed (cross) step, i.e. the
    /// join is disconnected and has no delta plan. Caller must invoke this only on
    /// acyclic `Rel::Join` inputs.
    pub(crate) fn delta_join_order(
        &self,
        inputs: &[Rel],
        equivalences: &[Vec<EScalar>],
    ) -> Option<Vec<Vec<JoinStep>>> {
        let n = inputs.len();
        if n == 0 {
            return None;
        }
        if n == 1 {
            return Some(vec![vec![]]);
        }
        // u32 input masks below; a delta join this wide is not a commit target.
        // Bail to the differential fallback (no delta plan). Mirrors delta_join_terms.
        if n >= 32 {
            return None;
        }
        let arities: Vec<usize> = inputs.iter().map(|r| r.arity()).collect();
        let mut offsets = Vec::with_capacity(n);
        let mut acc = 0;
        for &a in &arities {
            offsets.push(acc);
            acc += a;
        }
        let degs: Vec<f64> = inputs.iter().map(|r| self.size_degree(r)).collect();
        let hg = Hypergraph::build(inputs, equivalences);
        let hg_id = self.intern_hg(&hg, &degs);

        let mut all_paths: Vec<Vec<JoinStep>> = Vec::with_capacity(n);
        for driver in 0..n {
            let mut placed: Vec<(usize, usize)> = vec![(offsets[driver], arities[driver])];
            let mut frontier_mask = 1u32 << driver;
            let mut remaining: Vec<usize> = (0..n).filter(|&i| i != driver).collect();
            let mut path: Vec<JoinStep> = Vec::with_capacity(n - 1);
            while !remaining.is_empty() {
                // Pick the next input: prefer a keyed extension (non-empty
                // frontier_key_cols), then the lowest resulting AGM degree.
                // Deterministic, mirroring delta_join_terms.
                let mut best: Option<(bool, f64, usize, BTreeSet<usize>)> = None;
                for (pos, &j) in remaining.iter().enumerate() {
                    let key_cols = frontier_key_cols(offsets[j], arities[j], &placed, equivalences);
                    let is_cross = key_cols.is_empty();
                    let deg =
                        self.agm_degree_subset_memo(hg_id, &hg, &degs, frontier_mask | (1u32 << j));
                    let cand = (is_cross, deg, pos, key_cols);
                    best = Some(match best {
                        None => cand,
                        Some(b) => {
                            if (cand.0, cand.1) < (b.0, b.1) {
                                cand
                            } else {
                                b
                            }
                        }
                    });
                }
                let (is_cross, _deg, pos, key_cols) = best.expect("remaining is non-empty");
                if is_cross {
                    // No keyed extension exists: the join is disconnected. No delta plan.
                    return None;
                }
                let j = remaining.remove(pos);
                path.push(JoinStep { input: j, key_cols });
                placed.push((offsets[j], arities[j]));
                frontier_mask |= 1u32 << j;
            }
            all_paths.push(path);
        }
        Some(all_paths)
    }
}

/// Extract the `GlobalId` from a leaf `Rel` if it is a direct opaque global
/// `Get`.
///
/// Returns `Some(gid)` only for `Rel::Opaque(MirRelationExpr::Get { Id::Global(gid) })`.
/// All other leaves (local gets, filtered/projected inputs) return `None`.
fn global_id_from_leaf(rel: &Rel) -> Option<GlobalId> {
    if let Rel::Opaque(mir) = rel {
        if let MirRelationExpr::Get {
            id: Id::Global(gid),
            ..
        } = mir.as_ref()
        {
            return Some(*gid);
        }
    }
    None
}

/// Compute the set of local column indices (relative to the start of `input`)
/// that `equivalences` require as a join key for a specific join input.
///
/// A column `c` in the concatenated column space belongs to the input at
/// `[offset, offset + input_arity)`.  The local index is `c - offset`.  A
/// column is part of the join key for this input when its equivalence class
/// also references at least one column from another input (so it is actually
/// constrained, not merely appearing unshared).
pub(crate) fn join_key_cols_for_input(
    offset: usize,
    input_arity: usize,
    equivalences: &[Vec<EScalar>],
) -> BTreeSet<usize> {
    let mut key_cols = BTreeSet::new();
    for class in equivalences {
        // Gather columns from this class that fall inside this input's range.
        let mut local_cols: Vec<usize> = Vec::new();
        let mut touches_other = false;
        for escalar in class {
            for col in escalar.cols() {
                if col >= offset && col < offset + input_arity {
                    local_cols.push(col - offset);
                } else {
                    touches_other = true;
                }
            }
        }
        // Only include columns that are genuinely constrained across inputs.
        if touches_other {
            key_cols.extend(local_cols);
        }
    }
    key_cols
}

/// A chosen left-deep join order with per-step arrangement keys, surfaced from
/// the cost model so the eqsat emitter can build a `JoinImplementation::Differential`.
/// `steps[0]` is the starting input; `key_cols` are local to each step's input.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct JoinStep {
    /// Global input index (position in the join's `inputs`).
    pub input: usize,
    /// Column indices local to `input` forming this step's arrangement key.
    pub key_cols: BTreeSet<usize>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct JoinOrder {
    pub steps: Vec<JoinStep>,
}

/// Local key columns of the input at `[offset, offset+arity)` that are equated,
/// via some class in `equivalences`, to a column of an already-placed (frontier)
/// input. `placed` holds the `(offset, arity)` ranges of inputs placed so far.
///
/// A class contributes local key columns iff:
/// * there is at least one **simple-column** member whose column is in local
///   range (i.e. `escalar.is_col() == Some(c)` for `c` in `[offset, offset+arity)`)
/// * AND there is at least one **fully-frontier** member — either a simple
///   column in the frontier, or a complex expression whose every column
///   reference is in the frontier.
///
/// The directionality constraint matters: `find_bound_expr` (LIR lowering)
/// can compute a stream-side lookup key from a fully-frontier expression, but
/// cannot invert a function to recover a sub-expression that appears inside
/// a complex member. Restricting local contributions to top-level simple
/// columns ensures the `lookup_key` slot is always a plain local column index
/// that `find_bound_expr` can resolve via the frontier member.
// `pub(super)` so the unit-tests sub-module can call it directly.
#[cfg_attr(not(test), allow(dead_code))]
pub(super) fn frontier_key_cols(
    offset: usize,
    arity: usize,
    placed: &[(usize, usize)],
    equivalences: &[Vec<EScalar>],
) -> BTreeSet<usize> {
    let in_frontier = |c: usize| placed.iter().any(|&(o, a)| c >= o && c < o + a);
    let in_local = |c: usize| c >= offset && c < offset + arity;
    let mut key_cols = BTreeSet::new();
    for class in equivalences {
        let mut local_simple: Vec<usize> = Vec::new();
        let mut fully_frontier = false;
        for escalar in class {
            if let Some(c) = escalar.is_col() {
                if in_local(c) {
                    local_simple.push(c - offset);
                } else if in_frontier(c) {
                    // Simple frontier column — trivially computable from stream.
                    fully_frontier = true;
                }
            } else {
                // Complex expression: all column refs must be in the frontier for
                // it to serve as a stream-side key expression for `find_bound_expr`.
                let cols = escalar.cols();
                if !cols.is_empty() && cols.iter().all(|&c| in_frontier(c)) {
                    fully_frontier = true;
                }
            }
        }
        if fully_frontier && !local_simple.is_empty() {
            key_cols.extend(local_simple);
        }
    }
    key_cols
}

/// The input sequence (global indices) of the cheapest left-deep order, with
/// **keyed-ness primary**: minimize `(forced_crosses, AGM terms_cost)`
/// lexicographically. `is_keyed(frontier_mask, add)` reports whether placing
/// input `add` on top of the inputs in `frontier_mask` is a keyed step (a
/// non-keyed step is a forced cross). This mirrors `delta_join_terms`' cross
/// count and the findings doc's "keyed-ness is primary" rule. Returns `None`
/// only when `n == 0`.
fn best_left_deep_sequence(
    n: usize,
    agm: &dyn Fn(u32) -> f64,
    is_keyed: &dyn Fn(u32, usize) -> bool,
) -> Option<Vec<usize>> {
    if n == 0 {
        return None;
    }
    let full = (1u32 << n) - 1;
    // best[S] = (crosses, cost terms, predecessor subset, last input added).
    let mut best: Vec<Option<(usize, Vec<f64>, u32, usize)>> = vec![None; 1 << n];
    for i in 0..n {
        best[1 << i] = Some((0, vec![], 0, i)); // single input: 0 crosses, no work
    }
    for s in 1..=full {
        if s.count_ones() < 2 {
            continue;
        }
        let agm_s = agm(s);
        let mut sub = s;
        while sub > 0 {
            let i = sub.trailing_zeros() as usize;
            let rest = s & !(1 << i);
            if rest != 0 {
                if let Some((rest_crosses, rest_terms, _, _)) = &best[rest as usize] {
                    let cross_delta = if is_keyed(rest, i) { 0 } else { 1 };
                    let cand_crosses = rest_crosses + cross_delta;
                    let mut cand_terms = rest_terms.clone();
                    cand_terms.push(agm_s);
                    let better = best[s as usize].as_ref().is_none_or(|(c_cr, c_tm, _, _)| {
                        cand_crosses < *c_cr
                            || (cand_crosses == *c_cr
                                && terms_cost(&cand_terms).lt(&terms_cost(c_tm)))
                    });
                    if better {
                        best[s as usize] = Some((cand_crosses, cand_terms, rest, i));
                    }
                }
            }
            sub &= sub - 1;
        }
    }
    // Walk predecessors from the full set back to a singleton.
    let mut seq = Vec::with_capacity(n);
    let mut s = full;
    loop {
        let (_, _, pred, last) = best[s as usize].clone()?;
        seq.push(last);
        if pred == 0 {
            break;
        }
        s = pred;
    }
    seq.reverse();
    Some(seq)
}

/// Wrap a list of degrees in a [`Cost`] for comparison (node count ignored,
/// used for the binary-join DP which only needs to compare time vectors).
fn terms_cost(terms: &[f64]) -> Cost {
    let mut time: Vec<f64> = terms.iter().copied().filter(|d| *d > EPS).collect();
    time.sort_by(|a, b| b.partial_cmp(a).unwrap());
    Cost {
        arrangements: 0,
        memory: vec![],
        time,
        nodes: 0,
    }
}

/// Selects the join-order search strategy the cost model uses. `DpSub` is the
/// historical exhaustive DP and the production default. `Dpccp` is the
/// cross-product-free connected-subgraph DP: cheaper on sparse joins but a
/// different policy, so it can return a higher cost than `DpSub`. The two agree
/// only when no cross-product prefix is cheaper than every connected one (the
/// uniform-degree case). They diverge when a connected input is heavier than a
/// cheap cross product of others, which `DpSub` exploits and `Dpccp` cannot
/// (see `dpccp_diverges_from_dpsub` for a worked case). `Dpccp` is therefore a
/// behavior-changing orderer to evaluate, not a drop-in for `DpSub`. `DphypBushy`
/// extends `Dpccp` to full connected-subgraph bushy trees (all connected
/// bipartitions, not just single-input extensions), so it finds a lower or equal
/// cost than `Dpccp` for every connected graph. Read from `MZ_EQSAT_JOIN_ORDERER`
/// via `join_orderer_from_env` so an evaluation harness can flip it for a whole
/// run. The single reader is the seam for a later move to a system parameter or
/// feature flag.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) enum JoinOrdererKind {
    #[default]
    DpSub,
    Dpccp,
    DphypBushy,
}

fn join_orderer_from_env() -> JoinOrdererKind {
    match std::env::var("MZ_EQSAT_JOIN_ORDERER").as_deref() {
        Ok("dpccp") => JoinOrdererKind::Dpccp,
        Ok("dphypbushy") => JoinOrdererKind::DphypBushy,
        _ => JoinOrdererKind::DpSub,
    }
}

/// A pairwise adjacency view of a join's hypergraph: `neighbors[i]` is the
/// bitmask of inputs that share at least one equivalence class with input `i`
/// (the self bit is excluded). This is the join graph the connected-subgraph
/// orderers enumerate over. The AGM degree of any subset is supplied separately
/// by a memoized oracle, so `JoinGraph` carries only connectivity.
struct JoinGraph {
    n: usize,
    neighbors: Vec<u32>,
}

impl JoinGraph {
    fn from_hypergraph(hg: &Hypergraph) -> Self {
        let n = hg.n_inputs;
        let mut neighbors = vec![0u32; n];
        for class in &hg.classes {
            // Every pair of inputs in a class is mutually adjacent.
            for &i in class {
                for &j in class {
                    if i != j {
                        neighbors[i] |= 1 << j;
                    }
                }
            }
        }
        JoinGraph { n, neighbors }
    }
}

/// A join-order search strategy. Given a join's graph and a memoized AGM-bound
/// size-degree oracle, return the work terms (intermediate-result size degrees,
/// in accumulation order) of the order it chooses. The caller sorts these into
/// the cost vector, so the term order is not observable. Only the multiset is.
trait JoinOrderer: std::fmt::Debug {
    fn work_terms(&self, g: &JoinGraph, agm: &dyn Fn(u32) -> f64) -> Vec<f64>;
}

/// The exact `2^n` subset DP over all single-input left-deep splits. This is the
/// historical cost-model behavior and the production default.
#[derive(Debug)]
struct DpSub;

impl JoinOrderer for DpSub {
    fn work_terms(&self, g: &JoinGraph, agm: &dyn Fn(u32) -> f64) -> Vec<f64> {
        let n = g.n;
        // DP over subsets: best[S] is the cost vector to materialise the join
        // of the inputs in S, choosing the cheapest order.
        let full = (1u32 << n) - 1;
        let mut best: Vec<Option<Vec<f64>>> = vec![None; 1 << n];
        for i in 0..n {
            best[1 << i] = Some(vec![]); // a single input needs no join work
        }
        for s in 1..=full {
            if s.count_ones() < 2 {
                continue;
            }
            let agm_s = agm(s);
            let mut sub = s;
            while sub > 0 {
                let i = sub.trailing_zeros();
                let rest = s & !(1 << i);
                if rest != 0 {
                    if let Some(rest_terms) = &best[rest as usize] {
                        let mut cand = rest_terms.clone();
                        cand.push(agm_s);
                        let cur = &best[s as usize];
                        if cur
                            .as_ref()
                            .is_none_or(|c| terms_cost(&cand).lt(&terms_cost(c)))
                        {
                            best[s as usize] = Some(cand);
                        }
                    }
                }
                sub &= sub - 1;
            }
        }
        best[full as usize].clone().unwrap_or_default()
    }
}

/// Connected-subgraph-growth left-deep DP (Moerkotte-Neumann DPccp, restricted
/// to single-input left-deep extensions). It grows connected subsets one
/// adjacent input at a time, so it never costs a disconnected (cross-product)
/// subset, touching far fewer subsets than `DpSub` on sparse joins.
///
/// This is a cross-product-free policy, not an equivalent of `DpSub`. The two
/// return the same sorted work-term multiset only when no cross-product prefix
/// is cheaper than every connected one. They diverge when a connected input is
/// heavier than a cheap cross product of others: `DpSub` builds the cheap cross
/// product first and wins, while `Dpccp` is structurally barred from forming it
/// and must join the heavy input, over-costing the join (see
/// `dpccp_diverges_from_dpsub`). A fully disconnected join graph has no
/// all-input connected prefix, so `Dpccp` delegates to `DpSub` for it.
#[derive(Debug)]
struct Dpccp;

impl JoinOrderer for Dpccp {
    fn work_terms(&self, g: &JoinGraph, agm: &dyn Fn(u32) -> f64) -> Vec<f64> {
        let n = g.n;
        let full = (1u32 << n) - 1;
        // If the whole graph is disconnected, the connected growth can never
        // reach `full`; fall back to the exhaustive DP, which handles
        // cross-product bridging identically.
        if !graph_connected(g, full) {
            return DpSub.work_terms(g, agm);
        }
        let mut best: Vec<Option<Vec<f64>>> = vec![None; 1 << n];
        for i in 0..n {
            best[1 << i] = Some(vec![]); // a single input needs no join work
        }
        // Ascending mask order guarantees best[mask] is final before any
        // superset mask|{j} (> mask) is extended, because every extension only
        // sets a higher-value bit.
        for mask in 1..=full {
            let Some(cur) = best[mask as usize].clone() else {
                continue;
            };
            if mask == full {
                continue;
            }
            // Inputs adjacent to the current connected subset, excluding it.
            let mut ext = 0u32;
            for i in 0..n {
                if mask & (1 << i) != 0 {
                    ext |= g.neighbors[i];
                }
            }
            ext &= !mask;
            let mut e = ext;
            while e != 0 {
                let j = e.trailing_zeros();
                let t = mask | (1 << j);
                let mut cand = cur.clone();
                cand.push(agm(t)); // intermediate degree of the grown subset
                let slot = &best[t as usize];
                if slot
                    .as_ref()
                    .is_none_or(|c| terms_cost(&cand).lt(&terms_cost(c)))
                {
                    best[t as usize] = Some(cand);
                }
                e &= e - 1;
            }
        }
        best[full as usize].clone().unwrap_or_default()
    }
}

/// Connected-subgraph bushy DP (the DPhyp cost shape, restricted to the
/// pairwise join graph). For each connected subset it minimizes over ALL
/// connected bipartitions, not just the single-input left-deep splits `DpSub`
/// and `Dpccp` consider, so it can find a balanced tree with smaller
/// intermediate-result degrees. Used cost-only here (no bushy emission); a
/// disconnected join graph delegates to `DpSub`.
#[derive(Debug)]
struct DphypBushy;

impl JoinOrderer for DphypBushy {
    fn work_terms(&self, g: &JoinGraph, agm: &dyn Fn(u32) -> f64) -> Vec<f64> {
        let n = g.n;
        let full = (1u32 << n) - 1;
        if !graph_connected(g, full) {
            return DpSub.work_terms(g, agm);
        }
        let mut best: Vec<Option<Vec<f64>>> = vec![None; 1 << n];
        for i in 0..n {
            best[1 << i] = Some(vec![]); // a single input needs no join work
        }
        // Ascending mask order: every connected bipartition of S uses two
        // strictly-smaller masks, already finalized when S is reached.
        for s in 1u32..=full {
            if s.count_ones() < 2 || !graph_connected(g, s) {
                continue;
            }
            let low = 1u32 << s.trailing_zeros();
            // Enumerate sub-masks of `s` that contain the lowest bit, so each
            // unordered bipartition {s1, s2} is visited exactly once.
            let mut s1 = s;
            let mut chosen: Option<Vec<f64>> = None;
            loop {
                // proper non-empty sub-mask containing `low`
                if s1 != s && (s1 & low) != 0 {
                    let s2 = s & !s1;
                    if s2 != 0 && graph_connected(g, s1) && graph_connected(g, s2) {
                        if let (Some(t1), Some(t2)) = (&best[s1 as usize], &best[s2 as usize]) {
                            let mut cand = t1.clone();
                            cand.extend_from_slice(t2);
                            cand.push(agm(s));
                            if chosen
                                .as_ref()
                                .is_none_or(|c| terms_cost(&cand).lt(&terms_cost(c)))
                            {
                                chosen = Some(cand);
                            }
                        }
                    }
                }
                if s1 == 0 {
                    break;
                }
                s1 = (s1 - 1) & s; // next sub-mask of s (descending)
            }
            best[s as usize] = chosen;
        }
        best[full as usize].clone().unwrap_or_default()
    }
}

/// Whether the `subset` of `g`'s inputs forms a single connected component.
fn graph_connected(g: &JoinGraph, subset: u32) -> bool {
    if subset == 0 {
        return true;
    }
    let start = subset.trailing_zeros();
    let mut seen = 1u32 << start;
    let mut frontier = seen;
    while frontier != 0 {
        let mut next = 0u32;
        let mut f = frontier;
        while f != 0 {
            let i = f.trailing_zeros();
            next |= g.neighbors[i as usize] & subset & !seen;
            f &= f - 1;
        }
        seen |= next;
        frontier = next;
    }
    seen == subset
}

/// The join hypergraph: vertices are join attributes (equivalence classes) plus
/// a private vertex per input with payload columns; edges are the inputs.
struct Hypergraph {
    n_inputs: usize,
    arities: Vec<usize>,
    classes: Vec<BTreeSet<usize>>,
    /// Per input, whether its join key is a proven unique key. A key-covered
    /// input emits at most one row per distinct join-key value, so its non-key
    /// columns are functionally determined and contribute no private cover
    /// vertex to the AGM bound (see `agm_degree_subset`). Only `build` (which
    /// has the input `Rel` subtrees) can populate this; `from_arities` has only
    /// arities and conservatively leaves every entry `false`.
    key_covered: Vec<bool>,
}

impl Hypergraph {
    fn build(inputs: &[Rel], equivalences: &[Vec<EScalar>]) -> Self {
        let arities: Vec<usize> = inputs.iter().map(Rel::arity).collect();
        let mut hg = Self::from_arities(&arities, equivalences);
        // A join input whose join key is a superkey produces at most one row per
        // distinct key value, so its payload columns are functionally determined
        // by the key. Recording this lets the AGM bound drop the input's private
        // cover vertex, which is exact for a key lookup (an acyclic key join is
        // linear, not a product). `rel_keys` is a pure function of the subtree,
        // the same key lattice the saturation analysis uses.
        let mut offset = 0usize;
        let mut key_covered = Vec::with_capacity(inputs.len());
        for input in inputs {
            let arity = input.arity();
            let join_key_cols = join_key_cols_for_input(offset, arity, equivalences);
            let covered = !join_key_cols.is_empty()
                && crate::eqsat::analysis::is_superkey(
                    &crate::eqsat::analysis::rel_keys(input),
                    &join_key_cols,
                );
            key_covered.push(covered);
            offset += arity;
        }
        hg.key_covered = key_covered;
        hg
    }

    /// Build the dual hypergraph from per-input arities and the join's
    /// equivalence classes. Vertices are inputs, edges are equivalence classes,
    /// and each edge holds the set of inputs whose columns it touches. Inputs
    /// occupy contiguous output-column ranges in `arities` order.
    fn from_arities(arities: &[usize], equivalences: &[Vec<EScalar>]) -> Self {
        let mut offsets = Vec::with_capacity(arities.len());
        let mut acc = 0usize;
        for a in arities {
            offsets.push(acc);
            acc += a;
        }
        let input_of =
            |col: usize| -> Option<usize> { (0..arities.len()).rev().find(|&i| col >= offsets[i]) };

        let mut classes = Vec::new();
        for class in equivalences {
            let mut members: BTreeSet<usize> = BTreeSet::new();
            for scalar in class {
                for col in scalar.cols() {
                    if let Some(i) = input_of(col) {
                        members.insert(i);
                    }
                }
            }
            if !members.is_empty() {
                classes.push(members);
            }
        }
        Hypergraph {
            n_inputs: arities.len(),
            arities: arities.to_vec(),
            classes,
            // `from_arities` has no input subtrees, so it cannot prove keys.
            // Conservatively assume no input is key-covered; `build` refines this.
            key_covered: vec![false; arities.len()],
        }
    }

    /// Whether the join is cyclic, i.e. not alpha-acyclic, decided by GYO
    /// (Graham-Yu-Ozsoyoglu) reduction over the dual hypergraph. A worst-case
    /// optimal join asymptotically beats a binary join tree exactly when the
    /// join is cyclic; acyclic joins are handled optimally by a binary tree
    /// (Yannakakis), so this is the structural gate for creating a `WcoJoin`.
    ///
    /// GYO repeatedly removes "ears" until no edge can be removed:
    ///   * an isolated vertex (a vertex in at most one edge) is dropped, and
    ///   * an edge whose vertices are all contained in some other edge is
    ///     dropped.
    /// The hypergraph is alpha-acyclic iff this reduces it to no edges.
    fn is_cyclic(&self) -> bool {
        // Edges as the sets of inputs (vertices) they touch. Self-equality
        // classes touching a single input cannot create a cycle.
        let mut edges: Vec<BTreeSet<usize>> = self
            .classes
            .iter()
            .filter(|e| e.len() >= 2)
            .cloned()
            .collect();

        loop {
            // Step 1: drop isolated vertices. A vertex appearing in at most one
            // edge can be removed from that edge without affecting acyclicity.
            let mut vertex_count: BTreeMap<usize, usize> = BTreeMap::new();
            for e in &edges {
                for &v in e {
                    *vertex_count.entry(v).or_insert(0) += 1;
                }
            }
            let mut changed = false;
            for e in &mut edges {
                let isolated: Vec<usize> = e
                    .iter()
                    .copied()
                    .filter(|v| vertex_count.get(v).copied().unwrap_or(0) <= 1)
                    .collect();
                for v in isolated {
                    e.remove(&v);
                    changed = true;
                }
            }
            // Drop edges that became empty or singletons after vertex removal.
            let before = edges.len();
            edges.retain(|e| e.len() >= 2);
            if edges.len() != before {
                changed = true;
            }

            // Step 2: drop an ear, an edge contained in another edge.
            let mut remove_idx = None;
            'outer: for (i, ei) in edges.iter().enumerate() {
                for (j, ej) in edges.iter().enumerate() {
                    if i != j && ei.is_subset(ej) {
                        remove_idx = Some(i);
                        break 'outer;
                    }
                }
            }
            if let Some(i) = remove_idx {
                edges.remove(i);
                changed = true;
            }

            if !changed {
                break;
            }
        }

        // Leftover edges mean GYO got stuck: the join is cyclic.
        !edges.is_empty()
    }

    /// AGM-bound degree for the sub-join over the inputs selected in `subset`,
    /// with input `i` weighted by `degs[i]` (its size degree).  Solves the
    /// fractional-edge-cover LP `min Σ λᵢ degᵢ s.t. cover`.
    fn agm_degree_subset(&self, degs: &[f64], subset: u32) -> f64 {
        let edges: Vec<usize> = (0..self.n_inputs)
            .filter(|i| subset & (1 << i) != 0)
            .collect();
        if edges.is_empty() {
            return 0.0;
        }
        if edges.len() == 1 {
            return degs[edges[0]];
        }
        let var_of: BTreeMap<usize, usize> =
            edges.iter().enumerate().map(|(v, &i)| (i, v)).collect();

        let mut rows: Vec<BTreeSet<usize>> = Vec::new();
        let mut shared_touch = vec![0usize; self.n_inputs];
        for members in &self.classes {
            let in_subset: Vec<usize> = members
                .iter()
                .copied()
                .filter(|i| var_of.contains_key(i))
                .collect();
            if in_subset.len() >= 2 {
                for &i in &in_subset {
                    shared_touch[i] += 1;
                }
                rows.push(in_subset.iter().map(|i| var_of[i]).collect());
            }
        }
        // Private vertices: an input whose columns are not all join attributes
        // contributes a private attribute, forcing its cover weight to ≥ 1. A
        // key-covered input is the exception: its join key is a unique key, so
        // its payload columns are functionally determined by the join
        // attributes and add no independent output dimension. Skipping its
        // private vertex is the functional-dependency-aware AGM refinement (a
        // key lookup is linear, not a product). The shared-attribute cover rows
        // are untouched, so the worst-case-optimal guarantee for cyclic joins is
        // preserved: a pure cycle has no private vertices to drop.
        for &i in &edges {
            if shared_touch[i] < self.arities[i] && !self.key_covered[i] {
                let mut s = BTreeSet::new();
                s.insert(var_of[&i]);
                rows.push(s);
            }
        }
        let weights: Vec<f64> = edges.iter().map(|&i| degs[i]).collect();
        solve_cover_lp(edges.len(), &rows, &weights)
    }
}

/// Whether the join over `arities` inputs constrained by `equivalences` is
/// cyclic (not alpha-acyclic), via GYO reduction over the join's dual
/// hypergraph. Cyclic joins are the only ones a worst-case-optimal join can
/// beat asymptotically, so this is the structural gate for raising a `Join` to
/// a `WcoJoin`. Cheap: joins have few inputs and edges.
pub(crate) fn join_is_cyclic(arities: &[usize], equivalences: &[Vec<EScalar>]) -> bool {
    Hypergraph::from_arities(arities, equivalences).is_cyclic()
}

/// Solve `min Σ wᵢ xᵢ s.t. (each row) Σ_{i∈row} xᵢ ≥ 1, x ≥ 0` exactly, by
/// enumerating vertices of the feasible polyhedron (each makes `n_vars`
/// constraints tight).  The instances are tiny, so this is fast and exact.
fn solve_cover_lp(n_vars: usize, rows: &[BTreeSet<usize>], weights: &[f64]) -> f64 {
    if rows.is_empty() {
        return 0.0;
    }
    struct Constraint {
        coeffs: Vec<f64>,
        rhs: f64,
    }
    let mut cons: Vec<Constraint> = Vec::new();
    for r in rows {
        let mut c = vec![0.0; n_vars];
        for &i in r {
            c[i] = 1.0;
        }
        cons.push(Constraint {
            coeffs: c,
            rhs: 1.0,
        });
    }
    for i in 0..n_vars {
        let mut c = vec![0.0; n_vars];
        c[i] = 1.0;
        cons.push(Constraint {
            coeffs: c,
            rhs: 0.0,
        });
    }

    let m = cons.len();
    let mut best = f64::INFINITY;
    let mut idx: Vec<usize> = (0..n_vars).collect();
    // The vertex enumeration is `C(m, n_vars)`, which is combinatorial: a wide
    // join with many equivalence rows can make this astronomically large and
    // hang the optimizer. Cap the number of vertices examined; on overflow,
    // fall back to the trivial cover (every edge weight 1, i.e. the cross
    // product). That bound is an over-estimate, so it never makes a bad join
    // look cheap, and it only triggers on joins far larger than any we plan
    // exactly.
    let mut budget = MAX_LP_VERTICES;
    loop {
        let mut a = vec![vec![0.0; n_vars]; n_vars];
        let mut b = vec![0.0; n_vars];
        for (r, &ci) in idx.iter().enumerate() {
            a[r].copy_from_slice(&cons[ci].coeffs);
            b[r] = cons[ci].rhs;
        }
        if let Some(x) = gaussian_solve(a, b) {
            let feasible = x.iter().all(|&v| v >= -1e-9)
                && cons.iter().all(|c| {
                    let lhs: f64 = c.coeffs.iter().zip(&x).map(|(a, b)| a * b).sum();
                    lhs >= c.rhs - 1e-9
                });
            if feasible {
                let obj: f64 = weights.iter().zip(&x).map(|(w, v)| w * v.max(0.0)).sum();
                if obj < best {
                    best = obj;
                }
            }
        }
        budget -= 1;
        if budget == 0 {
            // Enumeration too large: use the conservative trivial bound.
            return weights.iter().sum();
        }
        if !next_combination(&mut idx, m) {
            break;
        }
    }
    if best.is_finite() {
        best
    } else {
        weights.iter().sum()
    }
}

/// Maximum number of LP feasible-basis vertices [`solve_cover_lp`] enumerates
/// before falling back to the trivial cover bound. Keeps the combinatorial
/// vertex enumeration from hanging on wide joins.
const MAX_LP_VERTICES: usize = 200_000;

/// Gaussian elimination with partial pivoting; `None` if singular.
#[allow(clippy::needless_range_loop)]
fn gaussian_solve(mut a: Vec<Vec<f64>>, mut b: Vec<f64>) -> Option<Vec<f64>> {
    let n = b.len();
    for col in 0..n {
        let mut piv = col;
        for r in (col + 1)..n {
            if a[r][col].abs() > a[piv][col].abs() {
                piv = r;
            }
        }
        if a[piv][col].abs() < 1e-12 {
            return None;
        }
        a.swap(col, piv);
        b.swap(col, piv);
        for r in 0..n {
            if r == col {
                continue;
            }
            let f = a[r][col] / a[col][col];
            if f != 0.0 {
                for c in col..n {
                    a[r][c] -= f * a[col][c];
                }
                b[r] -= f * b[col];
            }
        }
    }
    let mut x = vec![0.0; n];
    for i in 0..n {
        x[i] = b[i] / a[i][i];
    }
    Some(x)
}

/// Advance `idx` (strictly increasing `k` indices in `0..m`) to the next
/// combination.  Returns false when exhausted.
fn next_combination(idx: &mut [usize], m: usize) -> bool {
    let k = idx.len();
    if k == 0 {
        return false;
    }
    let mut i = k;
    while i > 0 {
        i -= 1;
        if idx[i] != i + m - k {
            idx[i] += 1;
            for j in (i + 1)..k {
                idx[j] = idx[j - 1] + 1;
            }
            return true;
        }
    }
    false
}

#[cfg(test)]
mod tests {
    use super::*;
    use mz_expr::MirScalarExpr;

    #[mz_ore::test]
    fn cost_nodes_is_scalar_aware() {
        use crate::eqsat::ir::{EScalar, Rel};
        use mz_expr::{BinaryFunc, MirScalarExpr, func};
        use mz_repr::{Datum, ReprScalarType};
        let lit1 = MirScalarExpr::literal_ok(Datum::Int64(1), ReprScalarType::Int64);
        let compute =
            MirScalarExpr::column(1).call_binary(lit1, BinaryFunc::AddInt64(func::AddInt64));
        let get = Rel::Get {
            name: "r".to_string(),
            arity: 2,
        };
        let map_compute = Rel::Map {
            scalars: vec![EScalar::plain(compute)],
            input: Box::new(get.clone()),
        };
        let map_col = Rel::Map {
            scalars: vec![EScalar::plain(MirScalarExpr::column(0))],
            input: Box::new(get),
        };
        let model = CostModel::new();
        // Same relational structure; the cheaper scalar spelling has fewer cost nodes.
        assert!(
            model.cost(&map_col).nodes < model.cost(&map_compute).nodes,
            "Cost.nodes must reflect scalar size",
        );
    }

    #[mz_ore::test]
    fn arrangement_count_dedups_and_credits_oracle() {
        use crate::eqsat::ir::{EScalar, Rel};
        use mz_expr::MirScalarExpr;
        // Two ArrangeBy nodes on the same collection+key collapse to one arrangement.
        let base = Rel::Get {
            name: "t".into(),
            arity: 2,
        };
        let key = vec![EScalar::plain(MirScalarExpr::column(0))];
        let arr = Rel::ArrangeBy {
            input: Box::new(base.clone()),
            key: key.clone(),
        };
        let plan = Rel::Union {
            base: Box::new(arr.clone()),
            inputs: vec![arr.clone()],
        };
        let model = CostModel::new();
        assert_eq!(model.cost(&plan).arrangements, 1);
    }

    fn get(name: &str, arity: usize) -> Rel {
        Rel::Get {
            name: name.into(),
            arity,
        }
    }

    fn col(c: usize) -> EScalar {
        EScalar::plain(MirScalarExpr::column(c))
    }

    /// A `Reduce` grouped on column 0 with one aggregate: arity 2, a proven
    /// unique key `{0}` (the group key), and a payload column (the aggregate)
    /// that is not part of the key.
    fn keyed_reduce(name: &str) -> Rel {
        Rel::Reduce {
            input: Box::new(get(name, 3)),
            group_key: vec![col(0)],
            aggregates: vec![mz_expr::AggregateExpr {
                func: mz_expr::AggregateFunc::MaxInt64,
                expr: MirScalarExpr::column(1),
                distinct: false,
            }],
            monotonic: false,
            expected_group_size: None,
        }
    }

    #[mz_ore::test]
    fn key_covered_join_input_drops_private_vertex() {
        let model = CostModel::new();
        let eq = |a: usize, b: usize| vec![col(a), col(b)];
        // R(#0,#1) JOIN X(#2,#3) on #1=#2. The join key for X is its local
        // column 0 (#2). When X is keyed on that column, its payload (#3) is
        // functionally determined, so X needs no private cover vertex and the
        // join is a key lookup: worst-case linear, AGM degree 1.
        let keyed_inputs = vec![get("R", 2), keyed_reduce("Xbase")];
        // The same shape with an unkeyed X keeps X's private vertex, so the
        // worst-case output is the quadratic product, AGM degree 2.
        let unkeyed_inputs = vec![get("R", 2), get("X", 2)];
        let equivalences = vec![eq(1, 2)];

        let keyed_deg = model.join_degree(&keyed_inputs, &equivalences);
        let unkeyed_deg = model.join_degree(&unkeyed_inputs, &equivalences);
        assert!(
            (keyed_deg - 1.0).abs() < 1e-6,
            "key-covered join should be linear, got {keyed_deg}"
        );
        assert!(
            (unkeyed_deg - 2.0).abs() < 1e-6,
            "unkeyed join should be quadratic, got {unkeyed_deg}"
        );
    }

    #[mz_ore::test]
    fn triangle_wcoj_beats_binary() {
        let model = CostModel::new();
        let eq = |a: usize, b: usize| vec![col(a), col(b)];
        // R(#0,#1) S(#2,#3) T(#4,#5); a:#0=#4 b:#1=#2 c:#3=#5 — a pure triangle.
        let inputs = vec![get("R", 2), get("S", 2), get("T", 2)];
        let equivalences = vec![eq(0, 4), eq(1, 2), eq(3, 5)];

        let join = Rel::Join {
            inputs: inputs.clone(),
            equivalences: equivalences.clone(),
        };
        let wcoj = Rel::WcoJoin {
            inputs,
            equivalences,
        };

        let cj = model.cost(&join);
        let cw = model.cost(&wcoj);
        // WCOJ's dominant time term is N^1.5; binary's is N^2.
        assert!((cw.time[0] - 1.5).abs() < 1e-6, "wcoj time={:?}", cw.time);
        assert!((cj.time[0] - 2.0).abs() < 1e-6, "join time={:?}", cj.time);
        assert!(cw.lt(&cj));
    }

    #[mz_ore::test]
    fn join_is_cyclic_classifies_shapes() {
        let eq = |a: usize, b: usize| vec![col(a), col(b)];

        // Triangle R(#0,#1) S(#2,#3) T(#4,#5): a:#0=#4 b:#1=#2 c:#3=#5.
        // Three edges each touching two of three inputs, no ear -> cyclic.
        assert!(join_is_cyclic(&[2, 2, 2], &[eq(0, 4), eq(1, 2), eq(3, 5)]));

        // Chain R-S-T: b:#1=#2 c:#3=#4. A path, GYO reduces it -> acyclic.
        assert!(!join_is_cyclic(&[2, 2, 2], &[eq(1, 2), eq(3, 4)]));

        // Plain 2-way join on one equivalence: a single edge -> acyclic.
        assert!(!join_is_cyclic(&[2, 2], &[eq(1, 2)]));

        // A 4-cycle R-S-T-U-R: cyclic.
        // R(#0,#1) S(#2,#3) T(#4,#5) U(#6,#7).
        // #1=#2 (R,S), #3=#4 (S,T), #5=#6 (T,U), #7=#0 (U,R). No ear -> cyclic.
        assert!(join_is_cyclic(
            &[2, 2, 2, 2],
            &[eq(1, 2), eq(3, 4), eq(5, 6), eq(7, 0)]
        ));

        // A star: a center input shares one attribute with each leaf. Acyclic.
        // C(#0) L1(#1) L2(#2) L3(#3): #0=#1, #0=#2, #0=#3.
        assert!(!join_is_cyclic(
            &[1, 1, 1, 1],
            &[eq(0, 1), eq(0, 2), eq(0, 3)]
        ));
    }

    #[mz_ore::test]
    fn indexed_filter_beats_filter_over_get() {
        use mz_expr::MirRelationExpr;
        use mz_repr::{ReprRelationType, ReprScalarType};

        let model = CostModel::new();
        let g = get("R", 2);
        let pred = col(0);
        let filter = Rel::Filter {
            predicates: vec![pred.clone()],
            input: Box::new(g.clone()),
        };
        // `committed` does not enter the cost; a trivial constant placeholder.
        let committed = MirRelationExpr::constant(
            vec![],
            ReprRelationType::new(vec![ReprScalarType::Int64.nullable(false); 2]),
        );
        let indexed = Rel::IndexedFilter {
            predicates: vec![pred],
            input: Box::new(g),
            committed: Box::new(committed),
        };
        // The plain filter scans R (a time term at degree 1.0); the indexed
        // filter probes an existing index (constant work, dropped). Same node
        // count, so the indexed form is strictly cheaper.
        assert!(
            model.cost(&indexed).lt(&model.cost(&filter)),
            "indexed filter (cost {:?}) must be cheaper than filter-over-get (cost {:?})",
            model.cost(&indexed),
            model.cost(&filter),
        );
    }

    #[mz_ore::test]
    fn merge_filters_is_cheaper_structurally() {
        let model = CostModel::new();
        let p = col;
        let two = Rel::Filter {
            predicates: vec![p(0)],
            input: Box::new(Rel::Filter {
                predicates: vec![p(1)],
                input: Box::new(get("R", 2)),
            }),
        };
        let one = Rel::Filter {
            predicates: vec![p(0), p(1)],
            input: Box::new(get("R", 2)),
        };
        // Same dominant degree, but fewer nodes => strictly cheaper.
        assert!(model.cost(&one).lt(&model.cost(&two)));
    }

    #[mz_ore::test]
    fn triangle_wcojoin_dominates() {
        let model = CostModel::new();
        let eq = |a: usize, b: usize| vec![col(a), col(b)];
        let inputs = vec![get("R", 2), get("S", 2), get("T", 2)];
        let equivalences = vec![eq(0, 4), eq(1, 2), eq(3, 5)];

        let binary = Rel::Join {
            inputs: inputs.clone(),
            equivalences: equivalences.clone(),
        };
        let wcoj = Rel::WcoJoin {
            inputs,
            equivalences,
        };

        let cb = model.cost(&binary);
        let cw = model.cost(&wcoj);

        // WcoJoin memory: one term per input at degree 1.0 each, [1.0, 1.0, 1.0].
        // Binary join memory: the genuine intermediate at degree 2.0 plus the
        // three input arrangements, [2.0, 1.0, 1.0, 1.0]. The final-join output
        // is streamed, not arranged, so it carries no memory term.
        assert_eq!(cw.memory, vec![1.0, 1.0, 1.0], "wcoj memory");
        assert_eq!(cb.memory, vec![2.0, 1.0, 1.0, 1.0], "binary memory");
        assert!(
            cw.memory.first().copied().unwrap_or(0.0) < cb.memory.first().copied().unwrap_or(0.0),
            "WcoJoin memory max={:?} must be < binary memory max={:?}",
            cw.memory.first(),
            cb.memory.first(),
        );
        // WcoJoin time: AGM bound 1.5.  Binary time: worst intermediate 2.0.
        assert!(
            cw.time.first().copied().unwrap_or(0.0) < cb.time.first().copied().unwrap_or(0.0),
            "WcoJoin time max={:?} must be < binary time max={:?}",
            cw.time.first(),
            cb.time.first(),
        );
        // Default (memory-first) ordering: WcoJoin strictly cheaper.
        assert_eq!(
            cw.cmp_memory_first(&cb),
            std::cmp::Ordering::Less,
            "WcoJoin must dominate binary join under memory-first ordering"
        );
    }

    #[mz_ore::test]
    fn memory_first_picks_lower_memory() {
        // Plan A: lower memory, higher time.
        // Plan B: lower time, higher memory.
        // Default ordering must prefer A.
        let a = Cost {
            arrangements: 0,
            memory: vec![1.0],
            time: vec![2.0],
            nodes: 1,
        };
        let b = Cost {
            arrangements: 0,
            memory: vec![2.0],
            time: vec![1.0],
            nodes: 1,
        };
        assert_eq!(
            a.cmp_memory_first(&b),
            std::cmp::Ordering::Less,
            "memory-first: lower-memory plan A must beat higher-memory plan B"
        );
        assert!(a.lt(&b), "lt() must agree with memory-first ordering");
        // Time-first ordering reverses the preference.
        assert_eq!(
            a.cmp_time_first(&b),
            std::cmp::Ordering::Greater,
            "time-first: lower-time plan B must beat higher-time plan A"
        );
    }

    #[mz_ore::test]
    fn recommendation_logic_direct() {
        // Directly verify the recommendation decision logic used in engine.rs:
        // if time-first plan differs from memory-first plan, and is strictly
        // faster (lower time) but uses more memory, a recommendation fires.

        // memory-optimal plan: good memory, high time.
        let mem_cost = Cost {
            arrangements: 0,
            memory: vec![1.0],
            time: vec![2.0],
            nodes: 3,
        };
        // time-optimal plan: bad memory, low time.
        let time_cost = Cost {
            arrangements: 0,
            memory: vec![2.0],
            time: vec![1.0],
            nodes: 3,
        };

        // The time plan is strictly faster.
        assert_eq!(
            time_cost.cmp_time_first(&mem_cost),
            std::cmp::Ordering::Less,
            "time-optimal plan must have strictly lower time cost"
        );
        // The time plan uses more memory.
        assert_eq!(
            cmp_vecs(&time_cost.memory, &mem_cost.memory),
            std::cmp::Ordering::Greater,
            "time-optimal plan must use more memory"
        );
        // The memory plan is what the default ordering picks.
        assert_eq!(
            mem_cost.cmp_memory_first(&time_cost),
            std::cmp::Ordering::Less,
            "memory-first ordering must prefer the memory-optimal plan"
        );
    }

    // Helpers for index-aware cost model tests.

    use mz_expr::{AccessStrategy, Id};
    use mz_repr::{GlobalId, ReprRelationType, ReprScalarType};

    /// Build a `Rel::Opaque` wrapping a global `Get` for the given transient id
    /// and arity.  This mirrors how `lower` handles `MirRelationExpr::Get { Id::Global }`.
    fn global_opaque(id: u64, arity: usize) -> Rel {
        let typ = ReprRelationType::new(
            (0..arity)
                .map(|_| ReprScalarType::Int64.nullable(false))
                .collect(),
        );
        Rel::Opaque(Box::new(MirRelationExpr::Get {
            id: Id::Global(GlobalId::Transient(id)),
            typ,
            access_strategy: AccessStrategy::UnknownOrLocal,
        }))
    }

    /// Build an availability map with a single index key for a global relation.
    fn avail_one(id: u64, key_cols: &[usize]) -> BTreeMap<GlobalId, Vec<Vec<MirScalarExpr>>> {
        let key: Vec<MirScalarExpr> = key_cols.iter().map(|&c| MirScalarExpr::column(c)).collect();
        let mut m = BTreeMap::new();
        m.insert(GlobalId::Transient(id), vec![key]);
        m
    }

    /// The WcoJoin memory terms for the triangle with the given cost model.
    /// Triangle: R(2), S(2), T(2) with equivalences #0=#4, #1=#2, #3=#5.
    fn triangle_wcoj_memory(model: &CostModel) -> Vec<f64> {
        let eq = |a: usize, b: usize| vec![col(a), col(b)];
        let inputs = vec![
            global_opaque(1, 2),
            global_opaque(2, 2),
            global_opaque(3, 2),
        ];
        let equivalences = vec![eq(0, 4), eq(1, 2), eq(3, 5)];
        let wcoj = Rel::WcoJoin {
            inputs,
            equivalences,
        };
        let cost = model.cost(&wcoj);
        cost.memory
    }

    #[mz_ore::test]
    fn self_join_charges_shared_input_arrangement_once() {
        // Join(X, X) on #0 = #2 arranges the same collection by the same key on
        // both sides, so that arrangement is charged once. Join(X, Y) on the
        // same key arranges two distinct collections, charging two. The
        // self-join's memory therefore has exactly one fewer term.
        let model = CostModel::new();
        let eq = |a: usize, b: usize| vec![col(a), col(b)];
        let same = Rel::Join {
            inputs: vec![global_opaque(1, 2), global_opaque(1, 2)],
            equivalences: vec![eq(0, 2)],
        };
        let distinct = Rel::Join {
            inputs: vec![global_opaque(1, 2), global_opaque(2, 2)],
            equivalences: vec![eq(0, 2)],
        };
        let m_same = model.cost(&same).memory;
        let m_distinct = model.cost(&distinct).memory;
        assert_eq!(
            m_distinct.len(),
            m_same.len() + 1,
            "self-join shares one input arrangement: same={m_same:?} distinct={m_distinct:?}"
        );
    }

    #[mz_ore::test]
    fn binary_join_charges_persistent_arrangements_not_output() {
        // A binary join's memory is its per-input arrangements plus the genuine
        // intermediates of the chosen order, NOT the streamed final output.
        let model = CostModel::new();
        let eq = |a: usize, b: usize| vec![col(a), col(b)];
        let inputs = vec![get("R", 2), get("S", 2), get("T", 2)];
        let equivalences = vec![eq(0, 4), eq(1, 2), eq(3, 5)];
        let binary = Rel::Join {
            inputs,
            equivalences,
        };
        let cb = model.cost(&binary);
        // [2.0, 1.0, 1.0, 1.0]: the genuine intermediate at 2.0 plus the three
        // input arrangements at 1.0. The final-join output (1.5 for the
        // triangle) is dropped because it is streamed, not arranged.
        assert_eq!(
            cb.memory,
            vec![2.0, 1.0, 1.0, 1.0],
            "binary triangle memory must be the intermediate plus input arrangements, not the final output"
        );
    }

    #[mz_ore::test]
    fn two_way_join_ties_wcojoin_on_memory() {
        // The core of fix B: a 2-way binary join's memory now charges only its
        // two input arrangements [1.0, 1.0], matching WcoJoin exactly, so the
        // two join forms reach parity. Before the fix, binary charged the
        // transient N^2 output [2.0] and spuriously lost to WcoJoin.
        let model = CostModel::new();
        // R(#0,#1) S(#2,#3) with #1=#2: a single edge, a plain 2-way join.
        let inputs = vec![get("R", 2), get("S", 2)];
        let equivalences = vec![vec![col(1), col(2)]];
        let binary = Rel::Join {
            inputs: inputs.clone(),
            equivalences: equivalences.clone(),
        };
        let wcoj = Rel::WcoJoin {
            inputs,
            equivalences,
        };
        let cb = model.cost(&binary);
        let cw = model.cost(&wcoj);
        assert_eq!(
            cb.memory,
            vec![1.0, 1.0],
            "2-way binary memory must be the two input arrangements"
        );
        assert_eq!(
            cw.memory, cb.memory,
            "2-way binary and WcoJoin must tie on memory"
        );
        assert_eq!(
            cb.cmp_memory_first(&cw),
            std::cmp::Ordering::Equal,
            "2-way binary join must tie WcoJoin under memory-first ordering"
        );
    }

    #[mz_ore::test]
    fn index_aware_no_arrangement_term_for_indexed_input() {
        // When input R (id=1) has an index on its join key (columns 0 and 1,
        // the local columns referenced in the #0=#4 and #1=#2 equivalences),
        // the cost model must NOT charge R's arrangement-build memory term.
        //
        // R's join key in the triangle: #0 (from eq #0=#4) and #1 (from eq
        // #1=#2).  The local columns are 0 and 1, so the index key is [0, 1].
        let available = avail_one(1, &[0, 1]);
        let model_aware = CostModel::with_available(available);
        let model_blind = CostModel::new();

        let mem_aware = triangle_wcoj_memory(&model_aware);
        let mem_blind = triangle_wcoj_memory(&model_blind);

        // Blind model: 3 memory terms (one per input, each at degree 1.0).
        assert_eq!(
            mem_blind.len(),
            3,
            "blind model must charge all 3 WcoJoin inputs; got {mem_blind:?}"
        );
        // Index-aware model: 2 memory terms (R is free, S and T are charged).
        assert_eq!(
            mem_aware.len(),
            2,
            "index-aware model must omit R's arrangement term; got {mem_aware:?}"
        );
    }

    #[mz_ore::test]
    fn index_aware_wrong_key_still_charges_arrangement() {
        // An index on R with the wrong key (only column 0, not [0,1]) does not
        // cover the full join key for R, so the arrangement term is still charged.
        let available = avail_one(1, &[0]); // partial key only
        let model = CostModel::with_available(available);
        let mem = triangle_wcoj_memory(&model);
        // All 3 inputs must still be charged.
        assert_eq!(
            mem.len(),
            3,
            "partial-key index must not suppress the arrangement term; got {mem:?}"
        );
    }

    #[mz_ore::test]
    fn flatmap_costs_no_arrangement_and_input_scaled_work() {
        // A FlatMap (table function) maintains no persistent arrangement, so its
        // arrangement count must be zero. Its work term scales with the input:
        // the degree model ignores the constant fan-out factor of the function
        // itself, so FlatMap and Map over the same input incur the same time cost.
        let model = CostModel::new();
        let fm = Rel::FlatMap {
            input: Box::new(get("R", 2)),
            func: mz_expr::TableFunc::GenerateSeriesInt64,
            exprs: vec![],
        };
        let mp = Rel::Map {
            input: Box::new(get("R", 2)),
            scalars: vec![],
        };
        let fm_cost = model.cost(&fm);
        let mp_cost = model.cost(&mp);
        assert_eq!(
            fm_cost.arrangements, 0,
            "FlatMap must maintain no persistent arrangement; got {}",
            fm_cost.arrangements
        );
        assert_eq!(
            fm_cost.time, mp_cost.time,
            "FlatMap and Map over the same input must have equal time cost: \
             FlatMap={:?} Map={:?}",
            fm_cost.time, mp_cost.time
        );
    }

    #[mz_ore::test]
    fn index_blind_triangle_still_prefers_wcoj() {
        // Regression guard: without any index availability, the index-blind
        // model (CostModel::new()) must still prefer WcoJoin over binary join
        // for the triangle.  This ensures the new index-aware path does not
        // alter the default (index-blind) behavior.
        let model = CostModel::new();
        let eq = |a: usize, b: usize| vec![col(a), col(b)];
        let inputs = vec![
            global_opaque(1, 2),
            global_opaque(2, 2),
            global_opaque(3, 2),
        ];
        let equivalences = vec![eq(0, 4), eq(1, 2), eq(3, 5)];

        let join = Rel::Join {
            inputs: inputs.clone(),
            equivalences: equivalences.clone(),
        };
        let wcoj = Rel::WcoJoin {
            inputs,
            equivalences,
        };

        let cj = model.cost(&join);
        let cw = model.cost(&wcoj);
        // WcoJoin must still dominate binary join on both axes.
        assert!(
            cw.lt(&cj),
            "index-blind WcoJoin must dominate binary join; wcoj={cw:?} join={cj:?}"
        );
        assert!(
            cw.memory.first().copied().unwrap_or(0.0) < cj.memory.first().copied().unwrap_or(0.0),
            "WcoJoin memory max must be lower than binary join memory max"
        );
    }

    #[mz_ore::test]
    fn arrange_by_many_charges_one_arrangement_per_key() {
        // A two-key ArrangeByMany maintains one arrangement per key list, so the
        // arrangement count must equal the number of distinct keys.
        let plan = Rel::ArrangeByMany {
            input: Box::new(get("R", 3)),
            keys: vec![vec![col(0)], vec![col(1)]],
        };
        let model = CostModel::new();
        assert_eq!(
            model.cost(&plan).arrangements,
            2,
            "two distinct keys => two arrangements"
        );
    }

    #[mz_ore::test]
    fn arrange_by_many_deduplicates_repeated_key() {
        // A two-key ArrangeByMany whose two key lists are identical must only
        // charge a single arrangement (the same index built twice is one index).
        let plan = Rel::ArrangeByMany {
            input: Box::new(get("R", 3)),
            keys: vec![vec![col(0)], vec![col(0)]],
        };
        let model = CostModel::new();
        assert_eq!(
            model.cost(&plan).arrangements,
            1,
            "two identical keys => one arrangement (dedup)"
        );
    }

    /// The selection plumbing routes to the chosen orderer. On this uniform
    /// 4-way star (no cross-product prefix wins) the two orderers agree, so
    /// selecting `Dpccp` leaves the cost unchanged. This exercises the dispatch,
    /// not a general equivalence (`Dpccp` diverges on heavy-input joins, see
    /// `dpccp_diverges_from_dpsub`).
    #[mz_ore::test]
    fn cost_model_orderer_selection_dispatches() {
        // 4-way star join: C(#0,#1,#2,#3) L1(#4) L2(#5) L3(#6).
        let inputs = vec![get("C", 4), get("L1", 1), get("L2", 1), get("L3", 1)];
        let equivalences = vec![
            vec![col(0), col(4)],
            vec![col(1), col(5)],
            vec![col(2), col(6)],
        ];
        let join = Rel::Join {
            inputs: inputs.clone(),
            equivalences: equivalences.clone(),
        };
        let dpsub = CostModel::new(); // default = DpSub
        let dpccp = CostModel::new().with_join_orderer(JoinOrdererKind::Dpccp);
        assert_eq!(
            dpsub.cost(&join),
            dpccp.cost(&join),
            "on a uniform star the orderers agree, so selection is cost-neutral"
        );
    }

    // ---- Dpccp graph-builder helpers ----------------------------------------

    /// A path graph on `n` inputs: input i is adjacent to i-1 and i+1.
    fn path_graph(n: usize) -> JoinGraph {
        let mut neighbors = vec![0u32; n];
        for i in 0..n {
            if i > 0 {
                neighbors[i] |= 1 << (i - 1);
            }
            if i + 1 < n {
                neighbors[i] |= 1 << (i + 1);
            }
        }
        JoinGraph { n, neighbors }
    }

    /// A star graph with `n` inputs total: input 0 is the center, adjacent to
    /// all leaves (inputs 1..n-1).
    fn star_graph(n: usize) -> JoinGraph {
        let mut neighbors = vec![0u32; n];
        for i in 1..n {
            neighbors[0] |= 1 << i;
            neighbors[i] |= 1; // bit 0 = center
        }
        JoinGraph { n, neighbors }
    }

    /// A cycle graph on `n` inputs: input i is adjacent to (i+1)%n and (i+n-1)%n.
    fn cycle_graph(n: usize) -> JoinGraph {
        let mut neighbors = vec![0u32; n];
        for i in 0..n {
            let prev = (i + n - 1) % n;
            let next = (i + 1) % n;
            neighbors[i] |= (1 << prev) | (1 << next);
        }
        JoinGraph { n, neighbors }
    }

    /// A complete graph (clique) on `n` inputs: every pair of inputs is adjacent.
    fn clique_graph(n: usize) -> JoinGraph {
        let full = (1u32 << n) - 1;
        let neighbors = (0..n)
            .map(|i| full & !(1 << i)) // all bits except self
            .collect();
        JoinGraph { n, neighbors }
    }

    /// Two isolated 2-cliques with no edge between them (a disconnected graph).
    /// Inputs 0 and 1 form one component; inputs 2 and 3 form another.
    fn disconnected_graph() -> JoinGraph {
        // 0--1    2--3  (no edge between the two pairs)
        let neighbors = vec![
            0b0010u32, // 0 -> 1
            0b0001u32, // 1 -> 0
            0b1000u32, // 2 -> 3
            0b0100u32, // 3 -> 2
        ];
        JoinGraph { n: 4, neighbors }
    }

    /// A synthetic AGM oracle that defines the AGREEMENT regime for the two
    /// orderers: a connected `subset` costs its set-bit count (uniform
    /// degree-1 inputs), and a disconnected `subset` costs `count_ones() * 10.0`
    /// so every cross-product prefix is strictly more expensive than any
    /// connected alternative. Under this oracle no cross-product prefix can win,
    /// which is exactly the condition under which `Dpccp` (cross-product-free)
    /// matches `DpSub`. This oracle does NOT model the general AGM bound, where a
    /// cheap cross product can beat a heavy connected input; that divergent case
    /// is covered separately by `dpccp_diverges_from_dpsub`.
    fn synthetic_agm(g: &JoinGraph) -> impl Fn(u32) -> f64 + '_ {
        move |subset: u32| {
            if graph_connected(g, subset) {
                // Connected: degree proportional to subset size (uniform inputs).
                subset.count_ones() as f64
            } else {
                // Disconnected: heavy penalty so cross products never win here.
                subset.count_ones() as f64 * 10.0
            }
        }
    }

    fn sorted_desc(mut v: Vec<f64>) -> Vec<f64> {
        v.sort_by(|a, b| b.partial_cmp(a).unwrap());
        v
    }

    // ---- Dpccp agreement region ---------------------------------------------

    /// In the agreement regime (no cross-product prefix is cheaper than a
    /// connected one), `Dpccp` returns the same sorted work-term multiset as
    /// `DpSub` across a battery of join shapes, including a real-oracle case.
    #[mz_ore::test]
    fn dpccp_matches_dpsub_when_no_crossproduct_wins() {
        for g in &[
            path_graph(3),
            path_graph(4),
            path_graph(5),
            star_graph(4),
            star_graph(5),
            cycle_graph(3),
            cycle_graph(4),
            clique_graph(4),
            disconnected_graph(), // two 2-cliques with no edge between
        ] {
            let agm = synthetic_agm(g);
            let a = sorted_desc(DpSub.work_terms(g, &agm));
            let b = sorted_desc(Dpccp.work_terms(g, &agm));
            assert_eq!(a, b, "DpSub vs Dpccp diverged on n={} graph", g.n);
        }

        // Real-oracle case: a 4-way star of uniform degree-1 base relations,
        // mirroring how binary_join_terms builds JoinGraph + the agm closure.
        // All inputs are equal weight, so no cross product wins and the two
        // orderers agree.
        let inputs = vec![get("C", 4), get("L1", 1), get("L2", 1), get("L3", 1)];
        let equivalences = vec![
            vec![col(0), col(4)],
            vec![col(1), col(5)],
            vec![col(2), col(6)],
        ];
        let model = CostModel::new();
        let degs: Vec<f64> = inputs.iter().map(|r| model.size_degree(r)).collect();
        let hg = Hypergraph::build(&inputs, &equivalences);
        let hg_id = model.intern_hg(&hg, &degs);
        let g = JoinGraph::from_hypergraph(&hg);
        let agm = |subset: u32| model.agm_degree_subset_memo(hg_id, &hg, &degs, subset);
        let a = sorted_desc(DpSub.work_terms(&g, &agm));
        let b = sorted_desc(Dpccp.work_terms(&g, &agm));
        assert_eq!(a, b, "DpSub vs Dpccp diverged on real 4-way star oracle");
    }

    // ---- Dpccp divergence (documents the non-equivalence) --------------------

    /// `Dpccp` is NOT equivalent to `DpSub`. On a 3-input path `L1 - C - L2`
    /// whose center `C` is heavier than the cross product of the two leaves,
    /// `DpSub` builds the cheap leaf cross product first and `Dpccp` cannot,
    /// so `Dpccp` reports a strictly higher cost. This test pins that divergence
    /// so the non-equivalence stays documented and any future change to it is
    /// deliberate.
    #[mz_ore::test]
    fn dpccp_diverges_from_dpsub() {
        // Inputs: 0 = center C, 1 = L1, 2 = L2. Center is adjacent to both
        // leaves; the leaves are not adjacent to each other.
        let g = JoinGraph {
            n: 3,
            neighbors: vec![0b110, 0b001, 0b001],
        };
        // Heavy-center oracle: joining the center with a leaf costs 3, while the
        // disconnected leaf cross product {L1,L2} costs only 2. The full join
        // costs 3.
        let agm = |subset: u32| -> f64 {
            match subset {
                0b011 | 0b101 => 3.0, // {C,L1}, {C,L2}: heavy center dominates
                0b110 => 2.0,         // {L1,L2}: cheap cross product of leaves
                0b111 => 3.0,         // full join
                _ => 1.0,             // singletons
            }
        };
        let dpsub = sorted_desc(DpSub.work_terms(&g, &agm));
        let dpccp = sorted_desc(Dpccp.work_terms(&g, &agm));
        // DpSub: (L1 x L2) then join C => intermediates [2, 3] => sorted [3, 2].
        assert_eq!(
            dpsub,
            vec![3.0, 2.0],
            "DpSub should exploit the cross product"
        );
        // Dpccp: must grow C+leaf first (cost 3) then full (3) => [3, 3].
        assert_eq!(
            dpccp,
            vec![3.0, 3.0],
            "Dpccp is barred from the cross product"
        );
        assert_ne!(
            dpsub, dpccp,
            "the two orderers diverge on a heavy-center join"
        );
    }

    // ---- DphypBushy tests ---------------------------------------------------

    /// For every connected graph in the battery, the bushy DP must return a
    /// cost no greater than the connected left-deep DP (`Dpccp`), because the
    /// connected left-deep search space (single-input bipartitions) is a strict
    /// subset of the connected bushy search space (all connected bipartitions).
    #[mz_ore::test]
    fn dphypbushy_no_worse_than_dpccp() {
        for g in &[
            path_graph(3),
            path_graph(4),
            path_graph(5),
            star_graph(4),
            star_graph(5),
            cycle_graph(3),
            cycle_graph(4),
            clique_graph(4),
        ] {
            let agm = synthetic_agm(g);
            let bushy = terms_cost(&DphypBushy.work_terms(g, &agm));
            let leftdeep = terms_cost(&Dpccp.work_terms(g, &agm));
            assert!(
                bushy.cmp(&leftdeep) != std::cmp::Ordering::Greater,
                "bushy must never cost more than connected left-deep on {}-input graph",
                g.n
            );
        }
    }

    /// On small graphs (n <= 4), the bushy DP must match the brute-force
    /// optimal bushy tree (see `bruteforce_bushy`). This is an independent
    /// oracle written as a plain exponential recursion with no memoization.
    #[mz_ore::test]
    fn dphypbushy_matches_bruteforce_small() {
        for g in &[
            path_graph(3),
            star_graph(4),
            clique_graph(4),
            cycle_graph(4),
        ] {
            let agm = synthetic_agm(g);
            let got = sorted_desc(DphypBushy.work_terms(g, &agm));
            let want = sorted_desc(bruteforce_bushy(g, &agm, (1u32 << g.n) - 1));
            assert_eq!(
                got, want,
                "DphypBushy != brute-force bushy on {}-input graph",
                g.n
            );
        }
    }

    /// Brute-force optimal bushy tree for `set` (bitmask of inputs in `g`).
    /// Recursively the optimal cost for `|set| == 1` is empty; otherwise it is
    /// the `terms_cost`-min over every connected bipartition `(s1, s2)` (s1
    /// contains set's lowest bit, both s1 and s2 connected) of
    /// `bruteforce_bushy(s1) ++ bruteforce_bushy(s2) ++ [agm(set)]`.
    ///
    /// This is the same recurrence the DP implements, but written as the
    /// obvious exponential recursion with no memoization so it serves as an
    /// independent oracle.
    fn bruteforce_bushy(g: &JoinGraph, agm: &dyn Fn(u32) -> f64, set: u32) -> Vec<f64> {
        if set.count_ones() <= 1 {
            return vec![];
        }
        let low = 1u32 << set.trailing_zeros();
        let mut best: Option<Vec<f64>> = None;
        let mut s1 = set;
        loop {
            if s1 != set && (s1 & low) != 0 {
                let s2 = set & !s1;
                if s2 != 0 && graph_connected(g, s1) && graph_connected(g, s2) {
                    let mut cand = bruteforce_bushy(g, agm, s1);
                    cand.extend(bruteforce_bushy(g, agm, s2));
                    cand.push(agm(set));
                    if best
                        .as_ref()
                        .is_none_or(|b| terms_cost(&cand).lt(&terms_cost(b)))
                    {
                        best = Some(cand);
                    }
                }
            }
            if s1 == 0 {
                break;
            }
            s1 = (s1 - 1) & set;
        }
        best.unwrap_or_default()
    }

    fn constant(arity: usize) -> Rel {
        Rel::Constant {
            card: 1,
            arity,
            col_types: None,
        }
    }

    /// An `EScalar` for the two-column expression `#a = #b` (its `cols()` is
    /// `{a, b}`, so a class containing it touches both inputs — the LEFT-JOIN
    /// null-pad key shape that turns a foreign spelling into a 3-input class).
    fn eq_expr(a: usize, b: usize) -> EScalar {
        use mz_expr::{BinaryFunc, func};
        EScalar::plain(
            MirScalarExpr::column(a)
                .call_binary(MirScalarExpr::column(b), BinaryFunc::Eq(func::Eq)),
        )
    }

    #[mz_ore::test]
    fn delta_join_local_spelling_beats_cross() {
        let model = CostModel::new();
        // Inputs: t1(2) cols{0,1}, t2(3) cols{2,3,4}, t3(3) cols{5,6,7}.
        let inputs = vec![get("t1", 2), get("t2", 3), get("t3", 3)];
        // class1 = {#0, #2}: t1.c0 = t2.c0.
        let class1 = vec![col(0), col(2)];
        // The t3 key is the null-pad expr `#X = #4`. Foreign X=0 references t1
        // AND t2 -> class2 touches {t1,t2,t3} (3 inputs); local X=2 references
        // t2 only -> class2 touches {t2,t3} (2 inputs).
        let foreign = vec![vec![col(5), eq_expr(0, 4)], class1.clone()];
        let local = vec![vec![col(5), eq_expr(2, 4)], class1.clone()];

        let foreign_score = model.delta_join_terms(&inputs, &foreign);
        let local_score = model.delta_join_terms(&inputs, &local);

        assert_eq!(
            foreign_score.0, 1,
            "foreign spelling forces one cross, got {foreign_score:?}"
        );
        assert_eq!(
            local_score.0, 0,
            "local spelling forces no cross, got {local_score:?}"
        );
        assert!(
            delta_score_lt(&local_score, &foreign_score),
            "local {local_score:?} should beat foreign {foreign_score:?}"
        );
    }

    #[mz_ore::test]
    fn keyed_ness_is_primary_over_degree() {
        // Fewer crosses wins even when the degree vector is strictly larger.
        let keyed = (0usize, vec![2.0, 2.0, 2.0]);
        let crossy = (1usize, vec![2.0, 1.0, 1.0]);
        assert!(delta_score_lt(&keyed, &crossy));
        assert!(!delta_score_lt(&crossy, &keyed));
    }

    #[mz_ore::test]
    fn delta_join_terms_wide_join_is_safe() {
        // A join with >= 32 inputs overflows u32 bitmasks used inside
        // delta_join_terms; the guard must catch this and return (0, vec![])
        // without panicking (debug mode would panic on `1u32 << 32`).
        let model = CostModel::new();
        let inputs: Vec<Rel> = (0..33).map(|i| get(&format!("t{i}"), 1)).collect();
        let result = model.delta_join_terms(&inputs, &[]);
        assert_eq!(
            result,
            (0, vec![]),
            "wide join (>= 32 inputs) must return (0, vec![]) safely; got {result:?}"
        );
    }

    #[mz_ore::test]
    fn cheap_cross_does_not_dominate_blowup() {
        let model = CostModel::new();
        // A(2) cols{0,1}, B(2) cols{2,3} joined on #1=#2; K is a constant with
        // no equivalence (a disconnected input). K forces a structural cross,
        // but K is constant (size-degree 0), so the gate must not count it.
        let inputs = vec![get("A", 2), get("B", 2), constant(2)];
        let equivalences = vec![vec![col(1), col(2)]];
        let score = model.delta_join_terms(&inputs, &equivalences);
        assert_eq!(
            score.0, 0,
            "a cross over a constant side must not be counted, got {score:?}"
        );
    }

    #[mz_ore::test]
    fn binary_join_order_voj_is_fully_keyed() {
        // VOJ shape: t1(2) cols 0-1, t2(3) cols 2-4, t3(3) cols 5-7.
        // class {#0,#2}: t1.f0 = t2.f2; class {#5, eq(#2,#4)}: #5 = (#2 = #4).
        //
        // With keyed-ness-primary ordering the chooser picks [t1, t2, t3]:
        //   - t1→t2: t2.col0(#2) = t1.col0(#0) → keyed.
        //   - t2→t3: eq_expr(2,4) is fully in {t1,t2} frontier → keyed.
        // That order has 0 forced crosses, whereas [t3, t2, t1] has 1
        // (t2 step after t3 alone gets an empty key).
        let inputs = vec![get("t1", 2), get("t2", 3), get("t3", 3)];
        let equivalences = vec![vec![col(0), col(2)], vec![col(5), eq_expr(2, 4)]];
        let model = CostModel::new();

        let order = model
            .binary_join_order(&inputs, &equivalences)
            .expect("connected VOJ has an order");

        assert_eq!(order.steps.len(), 3);
        for step in &order.steps[1..] {
            assert!(
                !step.key_cols.is_empty(),
                "keyed-ness-primary order must avoid forced crosses; step input {} got empty key",
                step.input
            );
        }
    }

    #[mz_ore::test]
    fn binary_join_order_voj_t3_keyed_from_t1_t2_frontier() {
        // Same VOJ shape: t1(2) cols 0-1, t2(3) cols 2-4, t3(3) cols 5-7.
        // class {#0,#2}: t1.f0 = t2.f2; class {#5, eq(#2,#4)}: #5 = (#2 = #4).
        //
        // Verify that `frontier_key_cols` for t3 IS non-empty when BOTH t1 and
        // t2 are already in the frontier. eq_expr(2,4) has cols {2,4} ⊆ {t1,t2},
        // so it is a fully-bound frontier expression whose local counterpart is
        // #5 (col 5 of t3 → local col 0). This means the order [t1, t2, t3]
        // IS a valid fully-keyed Differential order for this join.
        let placed_t1_t2 = vec![(0usize, 2usize), (2usize, 3usize)];
        let equivalences = vec![vec![col(0), col(2)], vec![col(5), eq_expr(2, 4)]];
        let key_cols = super::frontier_key_cols(5, 3, &placed_t1_t2, &equivalences);
        assert_eq!(
            key_cols,
            BTreeSet::from([0usize]),
            "t3's local col 0 (#5) must be keyed from the t1+t2 frontier via eq_expr(2,4); \
             got key_cols={key_cols:?}",
        );
    }

    #[mz_ore::test]
    fn binary_join_order_disconnected_returns_order_with_cross() {
        let model = CostModel::new();
        let inputs = vec![get("a", 2), get("b", 2)];
        // No equivalences: the two inputs are unconnected -> the second step is a
        // cross (empty key). The order is still produced (does not panic / None).
        let equivalences: Vec<Vec<EScalar>> = vec![];

        let order = model
            .binary_join_order(&inputs, &equivalences)
            .expect("two inputs always yield an order");
        assert_eq!(order.steps.len(), 2);
        assert!(
            order.steps[1].key_cols.is_empty(),
            "disconnected step must be a cross (empty key)"
        );
    }

    /// Characterization test: pins the work-term multiset returned by
    /// `binary_join_terms` for a 4-way star join (center input 0 shares a key
    /// with each of inputs 1, 2, 3), verifying that the `DpSub` refactor
    /// produces byte-identical output to the original inline DP.
    ///
    /// The expected values were captured from the pre-refactor implementation
    /// and hard-coded here.  If these values change, the refactor is not
    /// behavior-preserving.
    #[mz_ore::test]
    fn dpsub_work_terms_characterization() {
        // 4-way star join: C(#0,#1,#2,#3) L1(#4) L2(#5) L3(#6).
        // Equivalences: #0=#4, #1=#5, #2=#6. Center joins each leaf on one col.
        let inputs = vec![get("C", 4), get("L1", 1), get("L2", 1), get("L3", 1)];
        let equivalences = vec![
            vec![col(0), col(4)],
            vec![col(1), col(5)],
            vec![col(2), col(6)],
        ];
        let model = CostModel::new();
        let mut got = model.binary_join_terms(&inputs, &equivalences);
        got.sort_by(|a, b| b.partial_cmp(a).unwrap());
        // Expected values captured from the pre-refactor implementation.
        // The star join is acyclic; the DP selects a left-deep order joining
        // each leaf to the center one at a time. All inputs are base relations
        // (degree 1.0), so every intermediate is also 1.0: three intermediates
        // for a 4-way join (one per non-singleton subset boundary).
        assert_eq!(
            got,
            vec![1.0, 1.0, 1.0],
            "4-way star work terms (sorted desc): got {got:?}"
        );
    }

    #[mz_ore::test]
    fn delta_join_order_chain_has_keyed_paths_per_driver() {
        // 3-input chain: in0.#0 = in1.#0 ; in1.#1 = in2.#0  (each input arity 2).
        let model = CostModel::new();
        let inputs = vec![get("i0", 2), get("i1", 2), get("i2", 2)];
        let equivalences = vec![
            vec![col(0), col(2)], // in0 col0 == in1 col0
            vec![col(3), col(4)], // in1 col1 == in2 col0
        ];
        let paths = model
            .delta_join_order(&inputs, &equivalences)
            .expect("connected chain has a delta plan");
        assert_eq!(paths.len(), 3, "one path per driver");
        // Every step in every path is keyed (non-empty key_cols).
        for path in &paths {
            assert_eq!(path.len(), 2, "two lookups after the driver");
            for step in path {
                assert!(
                    !step.key_cols.is_empty(),
                    "every delta lookup must be keyed"
                );
            }
        }
    }

    #[mz_ore::test]
    fn delta_join_order_disconnected_returns_none() {
        // 2 inputs, NO equivalence between them: a cross product, no delta plan.
        let model = CostModel::new();
        let inputs = vec![get("a", 2), get("b", 2)];
        let equivalences: Vec<Vec<EScalar>> = vec![];
        assert!(
            model.delta_join_order(&inputs, &equivalences).is_none(),
            "disconnected join has no delta plan"
        );
    }
}

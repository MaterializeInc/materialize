// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.

//! Construction, interning, query, and extraction methods on [`EGraph`].

use std::collections::{BTreeMap, HashMap, HashSet};

use mz_expr::{MirRelationExpr, MirScalarExpr};
use mz_repr::{GlobalId, ReprColumnType};

use crate::analysis::equivalences::EquivalenceClasses;
use crate::eqsat::colored::{ColorId, TreeSizeColoredCost};
use crate::eqsat::colored_derive::{ColoredLayer, resolve_scalar_colored};
use crate::eqsat::core::Id;
use crate::eqsat::cost::{Cost, CostModel};
use crate::eqsat::ir::{EScalar, Rel};

use super::combined::{CNode, EGraph, Index, IndexedFilterSeed};
use super::node::{ENode, Sym};

/// The polarity demand an operator imposes on a child during extraction.
///
/// Extraction is parameterized by this demand so a multiplicity-signed
/// (`Negate`-rooted) representative is never placed directly under an operator
/// that is unsound over signed multiplicities (a non-linear reduce or a TopK).
///
/// No rule in the current set repositions a `Negate` into a new structural
/// position, so today no extraction can place a `Negate`-rooted representative
/// where this demand would forbid it. The machinery is kept as the soundness
/// foundation for a future negate-repositioning rule: such a rule may merge a
/// `Negate`-rooted form into an arbitrary class only because this demand
/// guarantees the extractor will not then route that form under a non-linear
/// operator. It is the prerequisite, not dead code.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Demand {
    /// No sign constraint: the cheapest representative wins.
    Any,
    /// The representative's output multiplicities must be non-negative.
    Nonneg,
}

/// The polarity demand a reduce imposes on its input.
///
/// A reduce with at least one aggregate is non-linear and requires a `Nonneg`
/// input, because `reduce(r) != negate(reduce(negate(r)))` for a non-linear
/// aggregate (MIN/MAX/ANY/ALL). A reduce with no aggregates is a distinct, which
/// is polarity-insensitive and takes `Any`.
///
/// This is conservative: it demands `Nonneg` for ANY aggregate. Future work can
/// refine this to allow `Any` when every aggregate is linear, via
/// `aggregate_is_input` from `crate::analysis::equivalences`.
fn reduce_input_demand(node: &ENode) -> Demand {
    match node {
        ENode::Reduce { aggregates, .. } if aggregates.is_empty() => Demand::Any,
        ENode::Reduce { .. } => Demand::Nonneg,
        _ => Demand::Any,
    }
}

impl EGraph {
    /// Set the index-availability map read by `cond_reads_indexed_global`.
    pub fn set_available(&mut self, available: BTreeMap<GlobalId, Vec<Vec<MirScalarExpr>>>) {
        self.data_mut().rel.available = available;
    }

    /// Intern a scalar expression (the same reduced form used today) as a scalar
    /// e-class and register its `EScalar` fact in the cache. Returns the scalar
    /// class id. Identical scalars hash-cons to one class, so a class may be
    /// interned more than once.
    ///
    /// The cached `lit` is re-derived from the expr via [`EScalar::lit_of`]
    /// (ignoring the caller's `lit`), so the cached fact is a deterministic
    /// function of the class's expr. This is what makes re-interning idempotent:
    /// a later `EScalar::plain` re-intern of a class whose expr is the literal
    /// `true`/`false` (as the Phase-2a rewrite path can produce) cannot clobber
    /// the genuine `Some(true/false)` to `None`. For every legitimate caller the
    /// re-derived `lit` already equals the passed one (`reduced_escalar` computes
    /// it the same way; `permute_cols` preserves literal-ness), so this is
    /// behavior-neutral and only fixes the stray-`None` case.
    pub fn intern_scalar(&mut self, escalar: &EScalar) -> Id {
        let id = crate::eqsat::scalar::lower::lower_into(self, &escalar.expr);
        let lit = EScalar::lit_of(&escalar.expr);
        self.data_mut()
            .set_escalar(id, EScalar::new(escalar.expr.clone(), lit));
        id
    }

    /// Add an entire [`Rel`], returning the e-class of its root. Relational
    /// children are added recursively; scalar payloads are interned into scalar
    /// e-classes (registering their `EScalar` facts in the cache) and stored as
    /// `Id`s. Every constructed `ENode` is wrapped in [`CNode::Rel`].
    pub fn add_rel(&mut self, rel: &Rel) -> Id {
        let node = match rel {
            Rel::Constant {
                card,
                arity,
                col_types,
            } => ENode::Constant {
                card: *card,
                arity: *arity,
                col_types: col_types.clone(),
            },
            Rel::Get { name, arity } => ENode::Get {
                name: name.clone(),
                arity: *arity,
            },
            Rel::Project { input, outputs } => ENode::Project {
                input: self.add_rel(input),
                outputs: outputs.clone(),
            },
            Rel::Map { input, scalars } => {
                let input = self.add_rel(input);
                let scalars = self.intern_scalars(scalars);
                ENode::Map { input, scalars }
            }
            Rel::FlatMap { input, func, exprs } => {
                let input = self.add_rel(input);
                let exprs = self.intern_scalars(exprs);
                ENode::FlatMap {
                    input,
                    func: func.clone(),
                    exprs,
                }
            }
            Rel::Filter { input, predicates } => {
                let input = self.add_rel(input);
                let predicates = self.intern_scalars(predicates);
                ENode::Filter { input, predicates }
            }
            Rel::Reduce {
                input,
                group_key,
                aggregates,
                monotonic,
                expected_group_size,
            } => {
                let input = self.add_rel(input);
                let group_key = self.intern_scalars(group_key);
                ENode::Reduce {
                    input,
                    group_key,
                    aggregates: aggregates.clone(),
                    monotonic: *monotonic,
                    expected_group_size: *expected_group_size,
                }
            }
            Rel::TopK { input, shape } => ENode::TopK {
                input: self.add_rel(input),
                shape: shape.clone(),
            },
            Rel::Negate { input } => ENode::Negate {
                input: self.add_rel(input),
            },
            Rel::Threshold { input } => ENode::Threshold {
                input: self.add_rel(input),
            },
            Rel::Join {
                inputs,
                equivalences,
            } => {
                let inputs: Vec<Id> = inputs.iter().map(|r| self.add_rel(r)).collect();
                let equivalences = self.intern_equivalences(equivalences);
                ENode::Join {
                    inputs,
                    equivalences,
                }
            }
            Rel::WcoJoin {
                inputs,
                equivalences,
            } => {
                let inputs: Vec<Id> = inputs.iter().map(|r| self.add_rel(r)).collect();
                let equivalences = self.intern_equivalences(equivalences);
                ENode::WcoJoin {
                    inputs,
                    equivalences,
                }
            }
            Rel::ArrangeBy { input, key } => {
                let input = self.add_rel(input);
                let key = self.intern_scalars(key);
                ENode::ArrangeBy { input, key }
            }
            Rel::ArrangeByMany { input, keys } => {
                let input = self.add_rel(input);
                let keys = keys.iter().map(|k| self.intern_scalars(k)).collect();
                ENode::ArrangeByMany { input, keys }
            }
            Rel::IndexedFilter {
                input,
                predicates,
                committed,
            } => {
                let input = self.add_rel(input);
                let predicates = self.intern_scalars(predicates);
                ENode::IndexedFilter {
                    input,
                    predicates,
                    committed: committed.clone(),
                }
            }
            Rel::Union { base, inputs } => {
                let mut ids = vec![self.add_rel(base)];
                ids.extend(inputs.iter().map(|r| self.add_rel(r)));
                ENode::Union { inputs: ids }
            }
            // An unsupported subtree, carried verbatim.
            Rel::Opaque(m) => ENode::Opaque((**m).clone()),
            // A recursive/CSE reference is an opaque leaf within a Let-free
            // fragment.
            Rel::LocalGet {
                id,
                arity,
                get,
                version,
            } => ENode::LocalGet {
                id: *id,
                arity: *arity,
                get: get.clone(),
                version: *version,
            },
            // The binding scopes are peeled by the structural optimizer; a whole
            // `Let`/`LetRec` is never added to the e-graph.
            Rel::Let { .. } | Rel::LetRec { .. } => {
                panic!("Let/LetRec are binding scopes and cannot be added to the e-graph")
            }
        };
        self.add(CNode::Rel(node))
    }

    /// Intern each equivalence class's scalars, returning the per-class ids.
    fn intern_equivalences(&mut self, equivalences: &[Vec<EScalar>]) -> Vec<Vec<Id>> {
        equivalences
            .iter()
            .map(|class| self.intern_scalars(class))
            .collect()
    }

    /// The relational matcher index: every relational e-node grouped by its
    /// operator [`Sym`], paired with its canonical class. Scalar e-classes are
    /// skipped. Call after `rebuild()` so child ids are canonical.
    pub(crate) fn rel_index(&self) -> Index {
        let mut idx: Index = HashMap::new();
        // Iterate relational classes only; scalar classes have no relational
        // e-node and would contribute nothing to the index. (M1.2)
        for id in self.rel_class_ids() {
            for node in self.rel_class_nodes(id) {
                idx.entry(node.sym()).or_default().push((id, node.clone()));
            }
        }
        idx
    }

    /// The relational e-nodes of `id`'s canonical class (skipping scalar nodes).
    pub(crate) fn rel_class_nodes(&self, id: Id) -> Vec<&ENode> {
        let rep = self.find(id);
        self.classes
            .get(&rep)
            .into_iter()
            .flatten()
            .filter_map(|n| match n {
                CNode::Rel(e) => Some(e),
                CNode::Scalar(_) => None,
            })
            .collect()
    }

    /// The canonical ids of every class that holds at least one relational node.
    /// Used by [`Self::rel_index`] (so the matcher index skips scalar classes)
    /// and emitted by the build.rs DSL codegen for any rule whose left-hand side
    /// has a pure relational-variable root (`Pat::RelVar`), bounding such a root
    /// to relational classes only.
    pub(crate) fn rel_class_ids(&self) -> Vec<Id> {
        self.classes
            .iter()
            .filter(|(_, nodes)| nodes.iter().any(|n| matches!(n, CNode::Rel(_))))
            .map(|(&id, _)| id)
            .collect()
    }

    /// True when any relational class contains at least one e-node whose operator
    /// symbol equals `sym`. Short-circuits on the first match.
    ///
    /// Used by the colored rule driver ([`crate::eqsat::colored::saturate`]) to
    /// skip `colored_saturate` entirely when no match is possible for the current
    /// colored rule set — e.g. when there is no `Filter` e-node and every colored
    /// rule is `Filter`-rooted.
    pub(crate) fn has_rel_sym(&self, sym: Sym) -> bool {
        for id in self.rel_class_ids() {
            for node in self.rel_class_nodes(id) {
                if node.sym() == sym {
                    return true;
                }
            }
        }
        false
    }

    /// Seed [`ENode::IndexedFilter`] nodes from `seeds`, equating each with the
    /// `Filter` it realizes.
    ///
    /// For every seed, find each e-class holding a `Filter { input, predicates }`
    /// whose `predicates` equal the seed's and whose `input` class is the global
    /// `Get get_id`, add an `IndexedFilter` over the same `input` and predicates
    /// carrying the seed's committed realization, and union it into that class.
    /// The two nodes are genuinely equivalent (an indexed filter is that filter),
    /// so cost-based extraction then chooses between them.
    ///
    /// Called after saturation: a literal filter wrapped in a Project only
    /// exposes the bare `Filter(Get)` node this matches once
    /// `push_filter_past_project` has fired. The pushed `Filter` carries the
    /// predicates remapped through the projection, which equal the seed's
    /// predicates lowered from the same bare filter (see
    /// `collect_indexed_filter_seeds`).
    pub fn seed_indexed_filters(&mut self, seeds: &[IndexedFilterSeed]) {
        if seeds.is_empty() {
            return;
        }
        // Intern each seed's predicates to scalar ids up front (this mutates the
        // graph), so the class scan can compare against the matched filter's
        // `Vec<Id>` directly. Identical predicates hash-cons to the same ids the
        // filter already holds, so the comparison is byte-identical to the
        // pre-SP4a `EScalar` comparison.
        let seed_pred_ids: Vec<Vec<Id>> = seeds
            .iter()
            .map(|s| self.intern_scalars(&s.predicates))
            .collect();
        // Collect first, then mutate: `add`/`union` borrow `self` mutably.
        let mut to_union: Vec<(Id, ENode)> = Vec::new();
        for (&class, nodes) in &self.classes {
            for node in nodes {
                let CNode::Rel(ENode::Filter { input, predicates }) = node else {
                    continue;
                };
                for (seed, sp_ids) in seeds.iter().zip(&seed_pred_ids) {
                    if predicates.as_slice() == sp_ids.as_slice()
                        && self.class_is_global_get(*input, seed.get_id)
                    {
                        to_union.push((
                            class,
                            ENode::IndexedFilter {
                                input: *input,
                                predicates: predicates.clone(),
                                committed: Box::new(seed.committed.clone()),
                            },
                        ));
                    }
                }
            }
        }
        if to_union.is_empty() {
            return;
        }
        for (filter_class, node) in to_union {
            let nid = self.add(CNode::Rel(node));
            self.union(nid, filter_class);
        }
        self.rebuild();
    }

    /// Whether the class of `id` contains an opaque global `Get` for `gid`.
    fn class_is_global_get(&self, id: Id, gid: GlobalId) -> bool {
        self.rel_class_nodes(id).iter().any(|n| {
            matches!(
                n,
                ENode::Opaque(MirRelationExpr::Get {
                    id: mz_expr::Id::Global(g),
                    ..
                }) if *g == gid
            )
        })
    }

    /// The arity of a class (invariant across equivalent e-nodes), or `None` if
    /// the class has no relational e-node with an acyclic derivation — e.g. a
    /// scalar e-class, or a class all of whose relational derivations are cyclic.
    ///
    /// Prefer this over [`Self::arity`] whenever the class may not be relational
    /// (mixed relational/scalar iteration). [`Self::arity`] is the asserting
    /// convenience wrapper for the common case of a known-relational class.
    pub(crate) fn try_arity(&self, id: Id) -> Option<usize> {
        self.arity_guarded(id, &mut HashSet::new())
    }

    /// The arity of a known-relational class. Panics if the class has no
    /// well-defined arity (e.g. called on a scalar class); use [`Self::try_arity`]
    /// when that is possible. Every in-tree caller passes a relational class.
    pub fn arity(&self, id: Id) -> usize {
        debug_assert!(
            !self.rel_class_nodes(id).is_empty(),
            "arity() requires a class with a well-defined arity; class {id} has no \
             relational e-node (called on a scalar class?)",
        );
        self.try_arity(id)
            .expect("class has a well-defined arity")
    }

    fn arity_guarded(&self, id: Id, visiting: &mut HashSet<Id>) -> Option<usize> {
        let id = self.find(id);
        if !visiting.insert(id) {
            // Reached `id` again on this path: this derivation is cyclic and
            // can't pin the arity. Another e-node of the class may still.
            return None;
        }
        let mut result = None;
        {
            for node in self.rel_class_nodes(id) {
                let a = match node {
                    ENode::Constant { arity, .. }
                    | ENode::Get { arity, .. }
                    | ENode::LocalGet { arity, .. } => Some(*arity),
                    ENode::Opaque(m) => Some(m.arity()),
                    ENode::Project { outputs, .. } => Some(outputs.len()),
                    ENode::Map { input, scalars } => self
                        .arity_guarded(*input, visiting)
                        .map(|a| a + scalars.len()),
                    ENode::FlatMap { input, func, .. } => self
                        .arity_guarded(*input, visiting)
                        .map(|a| a + func.output_arity()),
                    ENode::Filter { input, .. }
                    | ENode::TopK { input, .. }
                    | ENode::ArrangeBy { input, .. }
                    | ENode::ArrangeByMany { input, .. }
                    | ENode::IndexedFilter { input, .. }
                    | ENode::Negate { input }
                    | ENode::Threshold { input } => self.arity_guarded(*input, visiting),
                    ENode::Reduce {
                        group_key,
                        aggregates,
                        ..
                    } => Some(group_key.len() + aggregates.len()),
                    ENode::Join { inputs, .. } | ENode::WcoJoin { inputs, .. } => inputs
                        .iter()
                        .map(|i| self.arity_guarded(*i, visiting))
                        .sum::<Option<usize>>(),
                    ENode::Union { inputs } => self.arity_guarded(inputs[0], visiting),
                };
                if a.is_some() {
                    result = a;
                    break;
                }
            }
        }
        visiting.remove(&id);
        result
    }

    /// The column types of a class, structurally derived over the e-graph, or
    /// `None` when no e-node of the class yields a derivation.
    ///
    /// Mirrors [`mz_expr::MirRelationExpr::try_col_with_input_cols`] over the
    /// e-graph rather than over a single MIR tree. The typed leaves are
    /// `Opaque` (the carried `MirRelationExpr`) and `LocalGet` with a stored
    /// `Get` node; operators derive their types from their inputs the same way
    /// MIR does. Used at synthesis time to capture the real column types of the
    /// relation an `Empty(r)` replaces.
    ///
    /// Like [`Self::arity_guarded`], a class can be cyclic; the visited guard
    /// breaks cycles and another e-node of the class may still pin the types.
    /// Returns `None` (rather than defaulting) for any case it cannot derive, so
    /// callers can fall back deliberately.
    pub(crate) fn column_types(&self, id: Id) -> Option<Vec<ReprColumnType>> {
        self.column_types_guarded(id, &mut HashSet::new())
    }

    fn column_types_guarded(
        &self,
        id: Id,
        visiting: &mut HashSet<Id>,
    ) -> Option<Vec<ReprColumnType>> {
        let id = self.find(id);
        if !visiting.insert(id) {
            // Reached `id` again on this path: this derivation is cyclic and
            // can't pin the types. Another e-node of the class may still.
            return None;
        }
        let mut result = None;
        for node in self.rel_class_nodes(id) {
            let t = self.node_column_types(node, visiting);
            if t.is_some() {
                result = t;
                break;
            }
        }
        visiting.remove(&id);
        result
    }

    /// Derive the column types of a single e-node, recursing into inputs through
    /// [`Self::column_types_guarded`]. Returns `None` for any operator or leaf
    /// whose types cannot be derived (e.g. a child class with no acyclic
    /// derivation, or a nested `Constant` with no captured types).
    fn node_column_types(
        &self,
        node: &ENode,
        visiting: &mut HashSet<Id>,
    ) -> Option<Vec<ReprColumnType>> {
        match node {
            // A synthesized empty carries its types directly; an empty without
            // captured types cannot pin them.
            ENode::Constant { col_types, .. } => col_types.clone(),
            // Test-only base relation; its types are not modeled.
            ENode::Get { .. } => None,
            // Typed leaves: read the column types off the carried MIR node.
            ENode::Opaque(m) => Some(m.typ().column_types),
            ENode::LocalGet { get, .. } => get.as_ref().map(|g| g.typ().column_types),
            ENode::Project { input, outputs } => {
                let input = self.column_types_guarded(*input, visiting)?;
                outputs.iter().map(|&i| input.get(i).cloned()).collect()
            }
            ENode::Map { input, scalars } => {
                let mut result = self.column_types_guarded(*input, visiting)?;
                for &sid in scalars {
                    let expr = self.data().escalar(sid).expr.clone();
                    let t = MirScalarExpr::typ(&expr, &result);
                    result.push(t);
                }
                Some(result)
            }
            // FlatMap appends columns whose types are determined by the TableFunc.
            // Deriving those types here would require calling into the MIR type
            // system with the full context; returning None is the safe conservative
            // choice (the synthesized-empty path falls back to an arity placeholder).
            ENode::FlatMap { .. } => None,
            // Filter/TopK/Negate/Threshold pass their input types through. Filter
            // can strengthen nullability, but a weaker (still-nullable) type is
            // sound for an empty relation, so the plain passthrough suffices.
            ENode::Filter { input, .. }
            | ENode::TopK { input, .. }
            | ENode::ArrangeBy { input, .. }
            | ENode::ArrangeByMany { input, .. }
            | ENode::IndexedFilter { input, .. }
            | ENode::Negate { input }
            | ENode::Threshold { input } => self.column_types_guarded(*input, visiting),
            ENode::Reduce {
                input,
                group_key,
                aggregates,
                ..
            } => {
                let input = self.column_types_guarded(*input, visiting)?;
                let mut result: Vec<ReprColumnType> = group_key
                    .iter()
                    .map(|&gid| MirScalarExpr::typ(&self.data().escalar(gid).expr, &input))
                    .collect();
                result.extend(aggregates.iter().map(|agg| agg.typ(&input)));
                Some(result)
            }
            // Join/WcoJoin concatenate input types. The nullability tightening MIR
            // applies across equivalence classes is omitted: a weaker type is
            // sound for an empty relation.
            ENode::Join { inputs, .. } | ENode::WcoJoin { inputs, .. } => {
                let mut result = Vec::new();
                for i in inputs {
                    result.extend(self.column_types_guarded(*i, visiting)?);
                }
                Some(result)
            }
            // Union takes the least upper bound of its inputs' column types,
            // mirroring MIR. Any input that fails to derive, or a width or union
            // mismatch, yields `None`.
            ENode::Union { inputs } => {
                let mut iter = inputs.iter();
                let first = iter.next()?;
                let mut result = self.column_types_guarded(*first, visiting)?;
                for i in iter {
                    let other = self.column_types_guarded(*i, visiting)?;
                    if other.len() != result.len() {
                        return None;
                    }
                    for (base, col) in result.iter_mut().zip(other.iter()) {
                        *base = base.union(col).ok()?;
                    }
                }
                Some(result)
            }
        }
    }

    /// E-nodes reachable from `root`, keyed by canonical class id.
    ///
    /// Performs a BFS from `find(root)` following `child_classes`. The returned
    /// map contains every class reachable from the root (including the root
    /// itself), with each entry holding the set of e-nodes in that class.
    pub fn reachable(&self, root: Id) -> BTreeMap<Id, Vec<ENode>> {
        use std::collections::VecDeque;
        let root = self.find(root);
        let mut visited: std::collections::BTreeSet<Id> = std::collections::BTreeSet::new();
        let mut queue: VecDeque<Id> = VecDeque::new();
        let mut result: BTreeMap<Id, Vec<ENode>> = BTreeMap::new();
        visited.insert(root);
        queue.push_back(root);
        while let Some(id) = queue.pop_front() {
            let mut nodes: Vec<ENode> = self.rel_class_nodes(id).into_iter().cloned().collect();
            // `classes` stores each class's nodes in a `HashSet`, whose iteration
            // order is randomized per process. Downstream consumers (the ILP
            // extractor's variable order, golden plan text) must be deterministic,
            // so impose the stable `Ord` order over the nodes.
            //
            // Note: `ENode`'s derived `Ord` compares scalar children by their
            // `Id` (not resolved `EScalar` content), so this only fixes the
            // BFS/ILP-variable *enumeration* order — it does not decide plan
            // selection. The cost-tie-break that picks the extracted plan
            // resolves scalar `Id`→`EScalar` through the cache, so selection
            // stays content-based and byte-identical (see `extract.rs` and
            // `extraction_tiebreak_resolves_scalars_through_cache`).
            nodes.sort();
            for node in &nodes {
                for child in self.child_classes(node) {
                    if visited.insert(child) {
                        queue.push_back(child);
                    }
                }
            }
            result.insert(id, nodes);
        }
        result
    }

    /// Canonical child class ids of an e-node, in argument order.
    ///
    /// Returns the `find`-canonicalized id for each child id the e-node carries.
    pub fn child_classes(&self, node: &ENode) -> Vec<Id> {
        node.relational_children()
            .into_iter()
            .map(|c| self.find(c))
            .collect()
    }

    /// The `(arranged-child-class, key-columns)` arrangements that selecting
    /// `node` entails.
    ///
    /// Returns one entry per arrangement the operator requires its inputs to
    /// maintain in memory:
    /// * `ArrangeBy { input, key }`: one entry `(find(input), key_cols)` where
    ///   `key_cols` are the column indices referenced by `key`.
    /// * `ArrangeByMany { input, keys }`: one entry `(find(input), key_cols)` per
    ///   key list in `keys`.
    /// * `Reduce { input, group_key, .. }`: one entry `(find(input), group_key_cols)`.
    /// * `TopK { input, shape }`: one entry `(find(input), group_key_cols union order_key_cols)`.
    /// * `Join { inputs, equivalences }` and `WcoJoin { inputs, equivalences }`:
    ///   one entry per input `(find(input_i), join_key_cols_for_input_i)`.
    /// * All other operators: empty.
    pub fn arrangements_of(&self, node: &ENode) -> Vec<(Id, Vec<usize>)> {
        use crate::eqsat::cost::join_key_cols_for_input;
        match node {
            ENode::ArrangeBy { input, key } => {
                let cols: Vec<usize> =
                    key.iter().filter_map(|&s| self.data().escalar(s).is_col()).collect();
                vec![(self.find(*input), cols)]
            }
            ENode::ArrangeByMany { input, keys } => {
                // One entry per key list: each list is a separate maintained arrangement.
                keys.iter()
                    .map(|key| {
                        let cols: Vec<usize> =
                            key.iter().filter_map(|&s| self.data().escalar(s).is_col()).collect();
                        (self.find(*input), cols)
                    })
                    .collect()
            }
            ENode::Reduce {
                input, group_key, ..
            } => {
                let cols: Vec<usize> = group_key
                    .iter()
                    .filter_map(|&s| self.data().escalar(s).is_col())
                    .collect();
                vec![(self.find(*input), cols)]
            }
            ENode::TopK { input, shape } => {
                let mut cols: std::collections::BTreeSet<usize> =
                    shape.group_key.iter().copied().collect();
                for sk in &shape.order_key {
                    cols.insert(sk.column);
                }
                vec![(self.find(*input), cols.into_iter().collect())]
            }
            ENode::Join {
                inputs,
                equivalences,
            }
            | ENode::WcoJoin {
                inputs,
                equivalences,
            } => {
                let equivalences = self.resolve_equivalences(equivalences);
                let mut offset = 0usize;
                let mut result = Vec::new();
                for &inp in inputs {
                    let arity = self.arity(inp);
                    let key_cols: Vec<usize> =
                        join_key_cols_for_input(offset, arity, &equivalences)
                            .into_iter()
                            .collect();
                    result.push((self.find(inp), key_cols));
                    offset += arity;
                }
                result
            }
            _ => vec![],
        }
    }

    /// Resolve a list of scalar e-class ids to their cached `EScalar` facts.
    pub(crate) fn resolve_scalars(&self, ids: &[Id]) -> Vec<EScalar> {
        ids.iter().map(|&id| self.data().escalar(id).clone()).collect()
    }

    /// Color-aware variant of [`Self::resolve_scalars`] (SP4b): when a colored
    /// layer is present and the payload's **context class** has a color, each
    /// scalar is resolved to its cheapest congruent spelling under that color
    /// (e.g. a redundant `f(#1)` recomputation collapses to the equal column `#0`
    /// a Filter established). Without a layer or a color for `context`, falls back
    /// to the plain cache path, so the result is byte-identical to Phase-2a.
    ///
    /// **SP4d soundness:** `context` is the **input class** of the Filter/Map node
    /// whose payload is being resolved — NOT the node's own (enclosing) class. A
    /// Filter predicate / Map scalar is evaluated over the input's rows, so the
    /// equivalences that may reduce it are the input's output-equivalences, which
    /// [`derive`] records into the input class's color. Resolving under the
    /// enclosing class instead would, for a class holding sibling nodes with
    /// different inputs (`merge_filters`/`fuse_maps`), fold a sibling whose own
    /// input proves nothing — wrong results.
    ///
    /// [`derive`]: crate::eqsat::colored_derive::derive
    ///
    /// `max_cols` is parallel to `ids`: `max_cols[i]` is the exclusive
    /// column-index bound valid for the payload at position `i` in its evaluation
    /// context. It re-applies, at resolution time, the same column-range guard
    /// `reduce_escalar` enforces at recording time (see
    /// [`resolve_scalar_colored`]). The two call sites compute it per kind:
    /// Filter predicates all use `arity(input)`; a Map scalar at position `pos`
    /// uses `arity(input) + pos`.
    fn resolve_scalars_in_color(
        &self,
        ids: &[Id],
        max_cols: &[usize],
        context: Id,
        colored: Option<&mut ColoredLayer<'_>>,
    ) -> Vec<EScalar> {
        debug_assert_eq!(ids.len(), max_cols.len());
        match colored {
            Some(layer) => match layer.color_of.get(&self.find(context)).copied() {
                Some(color) => {
                    let mut out = Vec::with_capacity(ids.len());
                    for (&id, &max_col) in ids.iter().zip(max_cols) {
                        // Reborrow `layer` per id: `resolve_scalar_colored` takes
                        // `&mut ColoredLayer` (its layered `find` is `&mut self`).
                        out.push(resolve_scalar_colored(self, &mut *layer, color, id, max_col));
                    }
                    out
                }
                None => self.resolve_scalars(ids),
            },
            None => self.resolve_scalars(ids),
        }
    }

    /// Resolve a join's per-class scalar ids to their cached `EScalar` facts.
    pub(crate) fn resolve_equivalences(&self, equivalences: &[Vec<Id>]) -> Vec<Vec<EScalar>> {
        equivalences
            .iter()
            .map(|class| self.resolve_scalars(class))
            .collect()
    }

    /// Extract the cheapest plan rooted at `root` under `model`, using the
    /// memory-first comparator (the default scarce-resource ordering).
    ///
    /// Bottom-up dynamic programming: each class records the cheapest plan
    /// among its e-nodes whose children have themselves been costed, iterated
    /// to a fixpoint.  (The e-graphs we build are acyclic, so this converges
    /// in at most the depth of the e-graph.)
    ///
    /// Returns `None` when the root class has no buildable representative under
    /// the polarity constraints (a non-linear `Reduce`/`TopK` whose input class
    /// has no non-negative form), so the caller can fall back to the
    /// un-optimized fragment rather than failing.
    // Only in-crate tests use this `PeakDegree` convenience; production extracts
    // through the configured `Extractor` (see `engine`).
    #[cfg(test)]
    pub(crate) fn extract(&self, root: Id, model: &CostModel) -> Option<Rel> {
        self.extract_with(root, model, &crate::eqsat::objective::PeakDegree, None, None)
    }

    /// Extract the cheapest plan rooted at `root` under `model`, using the
    /// given extraction [`Objective`] to compare costs.
    ///
    /// Returns `None` when the root class cannot be extracted under the polarity
    /// constraints (see [`Self::extract`]).
    ///
    /// [`Objective`]: crate::eqsat::objective::Objective
    pub(crate) fn extract_with(
        &self,
        root: Id,
        model: &CostModel,
        objective: &dyn crate::eqsat::objective::Objective,
        // Optional colored layer for color-aware equality resolution (SP4b). When
        // present, `build_rel` resolves Filter predicates / Map scalars to their
        // cheapest congruent spelling under the class's color and empty-folds
        // classes the layer proved unsatisfiable. `None` reproduces Phase-2a.
        mut colored: Option<&mut ColoredLayer<'_>>,
        spellings: Option<&HashMap<Id, EquivalenceClasses>>,
    ) -> Option<Rel> {
        let cmp = |a: &Cost, b: &Cost| objective.cmp(a, b);

        // Colored relational conclusions (SP4d T10). For each color present in
        // the layer, extract the cheapest-by-tree-size e-node per colored class
        // once (caching one table per color), then map each base relational
        // class to its colored conclusion's relational e-node. Built UP FRONT
        // here — `extract_colored`/`find` take `&mut ceg` — so the immutable
        // `build_rel` walk below reads an owned table.
        //
        // Each conclusion is offered as an EXTRA extraction candidate in the
        // fixpoint below; the real `cost::CostModel` then decides
        // base-vs-colored on the built `Rel`, so a colored conclusion is taken
        // only when it genuinely wins (keeping extraction sound). When a class
        // has no relational merge under its color, its conclusion equals one of
        // its base nodes and the extra candidate is a harmless duplicate.
        //
        // A conclusion is only recorded when every child id it carries is a base
        // id (`< uf_len()`): `build_rel` resolves children through the base
        // `find`/`escalar` cache, which a colored-delta child id would not be in.
        // Conclusions referencing colored deltas are conservatively skipped.
        let conclusion_of: HashMap<Id, ENode> = match colored.as_deref_mut() {
            Some(layer) => {
                let colors: HashSet<ColorId> = layer.color_of.values().copied().collect();
                let mut tables: HashMap<ColorId, HashMap<Id, (CNode, usize)>> = HashMap::new();
                for c in colors {
                    // Skip colors with no relational delta nodes: `extract_colored`
                    // would visit only base nodes and produce no relational
                    // conclusion that differs from the base — a no-op whose omission
                    // is byte-identical. (SP4d early-out #2.)
                    if !layer.ceg.has_rel_delta_nodes(c) {
                        continue;
                    }
                    tables.insert(c, layer.ceg.extract_colored(c, &TreeSizeColoredCost));
                }
                let entries: Vec<(Id, ColorId)> =
                    layer.color_of.iter().map(|(&cls, &c)| (cls, c)).collect();
                let mut conc: HashMap<Id, ENode> = HashMap::new();
                for (cls, c) in entries {
                    let rep = layer.ceg.find(c, cls);
                    if let Some((CNode::Rel(e), _)) = tables.get(&c).and_then(|t| t.get(&rep)) {
                        let in_base = e
                            .relational_children()
                            .iter()
                            .chain(e.scalar_children().iter())
                            .all(|&ch| ch < self.uf_len());
                        if in_base {
                            conc.insert(self.find(cls), e.clone());
                        }
                    }
                }
                conc
            }
            None => HashMap::new(),
        };

        // Cost is a pure, compositional function of the built `Rel`. Extraction
        // evaluates the same `(node, children-best)` combination many times
        // across passes, and `model.cost` recomputes the whole subtree each call
        // (including the exponential `binary_join_terms` for every join in it).
        // Memoize by the built plan so each distinct `Rel` is costed once. This
        // turns the dominant `O(classes^2)` re-evaluation into one cost per
        // distinct plan, and preserves the result exactly.
        let mut cost_cache: BTreeMap<Rel, Cost> = BTreeMap::new();
        // Two best-of-class maps, one per polarity demand. `best_any` is the
        // cheapest representative with no sign constraint; `best_nonneg` is the
        // cheapest representative whose output multiplicities are non-negative.
        // Both are filled in the same fixpoint so each class can serve whichever
        // demand its parent imposes. The soundness rule (see `build_rel`) pulls a
        // non-linear reduce or TopK input from `best_nonneg`, never `best_any`.
        let mut best_any: HashMap<Id, (Cost, Rel)> = HashMap::new();
        let mut best_nonneg: HashMap<Id, (Cost, Rel)> = HashMap::new();
        for _ in 0..(self.classes.len() + 1) {
            let mut changed = false;
            for (&id, nodes) in &self.classes {
                // Candidate relational e-nodes for this class: its own base
                // nodes, plus (under a colored layer) the cheapest colored
                // conclusion for the class. Scalar classes contribute nothing.
                // The real-cost selection below decides among all candidates.
                let base_nodes = nodes.iter().filter_map(|cnode| match cnode {
                    CNode::Rel(node) => Some(node),
                    CNode::Scalar(_) => None,
                });
                for node in base_nodes.chain(conclusion_of.get(&id)) {
                    // Attempt to build each node under both demands. A node may
                    // satisfy `Any` but not `Nonneg` (e.g. a `Negate`, or any
                    // node whose nonneg-required child has no nonneg form yet).
                    for demand in [Demand::Any, Demand::Nonneg] {
                        if let Some(rel) = self.build_rel(
                            id,
                            node,
                            demand,
                            &best_any,
                            &best_nonneg,
                            colored.as_deref_mut(),
                            model,
                            spellings,
                        ) {
                            let c = match cost_cache.get(&rel) {
                                Some(c) => c.clone(),
                                None => {
                                    let c = model.cost(&rel);
                                    cost_cache.insert(rel.clone(), c.clone());
                                    c
                                }
                            };
                            let best = match demand {
                                Demand::Any => &mut best_any,
                                Demand::Nonneg => &mut best_nonneg,
                            };
                            // Break cost ties on the plan itself, so extraction
                            // is deterministic despite randomized hash-map order.
                            let better = match best.get(&id) {
                                None => true,
                                Some((bc, br)) => {
                                    cmp(&c, bc) == std::cmp::Ordering::Less
                                        || (c == *bc && rel < *br)
                                }
                            };
                            if better {
                                best.insert(id, (c, rel));
                                changed = true;
                            }
                        }
                    }
                }
            }
            if !changed {
                break;
            }
        }
        // The root has no parent, so no polarity demand: extract from `best_any`.
        // `None` when no representative could be built (the root, or some node it
        // requires, has no form satisfying the polarity constraint); the caller
        // falls back to the un-optimized fragment, which is always sound.
        best_any.get(&self.find(root)).map(|(_, r)| r.clone())
    }

    /// Rebuild a [`Rel`] from an e-node, substituting each child with its
    /// currently-best extracted plan for the demand that child imposes. Returns
    /// `None` if the chosen child map lacks a child yet, or if `demand` is
    /// `Nonneg` and `node` is a `Negate` (the one node that cannot be made
    /// non-negative).
    ///
    /// `best_any` and `best_nonneg` are the per-class cheapest representatives
    /// without and with a non-negative-multiplicity guarantee, respectively. Each
    /// child is pulled from `best_nonneg` when its computed demand is `Nonneg`,
    /// from `best_any` otherwise.
    ///
    /// `class` is the canonical class `node` belongs to, and `colored` is the
    /// optional colored layer (SP4b). When present:
    /// * a class the layer proved empty (unsatisfiable) folds to an empty
    ///   `Constant` of the class arity, regardless of `node`'s structure; and
    /// * `Filter` predicates and `Map` scalars are resolved to their cheapest
    ///   congruent spelling under `class`'s color (`color_of[class]`). All other
    ///   scalar payloads stay on the plain `escalar`-cache path.
    fn build_rel(
        &self,
        class: Id,
        node: &ENode,
        demand: Demand,
        best_any: &HashMap<Id, (Cost, Rel)>,
        best_nonneg: &HashMap<Id, (Cost, Rel)>,
        mut colored: Option<&mut ColoredLayer<'_>>,
        model: &CostModel,
        spellings: Option<&HashMap<Id, EquivalenceClasses>>,
    ) -> Option<Rel> {
        // Empty-fold (SP4b): a class the colored layer proved unsatisfiable
        // denotes the empty relation, so it extracts to an empty `Constant` of
        // the class's arity (carrying the real column types when derivable, as
        // `empty_false_filter`/`union_cancel` do). This holds for every demand —
        // an empty relation is non-negative — so it precedes the `Negate` guard.
        if let Some(layer) = colored.as_deref() {
            if layer.empty_classes.contains(&self.find(class)) {
                return Some(Rel::Constant {
                    card: 0,
                    arity: self.arity(class),
                    col_types: self.column_types(class),
                });
            }
        }
        // Only `Negate` cannot satisfy a `Nonneg` demand; every other node is
        // either sign-preserving (and so relies on its children's nonneg forms,
        // enforced by the per-child demands below) or a barrier whose output is
        // non-negative regardless of input (`Reduce`/`TopK`/`Threshold`).
        if demand == Demand::Nonneg && matches!(node, ENode::Negate { .. }) {
            return None;
        }
        // Pull a child under its own demand: `best_nonneg` for `Nonneg`,
        // `best_any` otherwise. Returns `None` until that map has costed the
        // child.
        let get = |id: Id, child_demand: Demand| {
            let best = match child_demand {
                Demand::Any => best_any,
                Demand::Nonneg => best_nonneg,
            };
            best.get(&self.find(id)).map(|(_, r)| r.clone())
        };
        Some(match node {
            ENode::Constant {
                card,
                arity,
                col_types,
            } => Rel::Constant {
                card: *card,
                arity: *arity,
                col_types: col_types.clone(),
            },
            ENode::Get { name, arity } => Rel::Get {
                name: name.clone(),
                arity: *arity,
            },
            ENode::Opaque(m) => Rel::Opaque(Box::new(m.clone())),
            ENode::LocalGet {
                id,
                arity,
                get,
                version,
            } => Rel::LocalGet {
                id: *id,
                arity: *arity,
                get: get.clone(),
                version: *version,
            },
            // Project/Map/Filter are sign-preserving: propagate the parent demand
            // to the input.
            ENode::Project { input, outputs } => Rel::Project {
                input: Box::new(get(*input, demand)?),
                outputs: outputs.clone(),
            },
            ENode::Map { input, scalars } => {
                // A Map scalar at position `pos` may reference input columns and
                // earlier Map outputs, so its valid range is `arity(input) + pos`
                // — exactly the bound `reduce_escalar` uses when recording Map
                // reductions in `derive`.
                let input_arity = self.arity(*input);
                let max_cols: Vec<usize> =
                    (0..scalars.len()).map(|pos| input_arity + pos).collect();
                Rel::Map {
                    input: Box::new(get(*input, demand)?),
                    // SP4d: resolve under the INPUT's color, not the Map class's.
                    scalars: self.resolve_scalars_in_color(
                        scalars,
                        &max_cols,
                        *input,
                        colored.as_deref_mut(),
                    ),
                }
            }
            // FlatMap is sign-preserving: propagate the parent demand to the input.
            ENode::FlatMap { input, func, exprs } => Rel::FlatMap {
                input: Box::new(get(*input, demand)?),
                func: func.clone(),
                exprs: self.resolve_scalars(exprs),
            },
            ENode::Filter { input, predicates } => {
                // Filter predicates range over the input's columns only, so the
                // valid range for every predicate is `arity(input)` — the bound
                // `reduce_escalar` uses when recording Filter reductions in
                // `derive`.
                let max_col = self.arity(*input);
                let max_cols = vec![max_col; predicates.len()];
                Rel::Filter {
                    input: Box::new(get(*input, demand)?),
                    // SP4d: resolve under the INPUT's color, not the Filter class's.
                    predicates: self.resolve_scalars_in_color(
                        predicates,
                        &max_cols,
                        *input,
                        colored.as_deref_mut(),
                    ),
                }
            }
            // An indexed filter is a filter (sign-preserving): propagate the
            // parent demand to the input. `committed` is the physical
            // realization carried verbatim for raise.
            ENode::IndexedFilter {
                input,
                predicates,
                committed,
            } => Rel::IndexedFilter {
                input: Box::new(get(*input, demand)?),
                predicates: self.resolve_scalars(predicates),
                committed: committed.clone(),
            },
            // A reduce is a barrier (its output is non-negative regardless of
            // input), so it satisfies a `Nonneg` parent on its own. Its input
            // demand comes from the soundness rule, not the parent: a non-linear
            // reduce (>=1 aggregate) requires a `Nonneg` input, because
            // `reduce(r) != negate(reduce(negate(r)))` for non-linear aggregates.
            // A distinct (no aggregates) is polarity-insensitive and takes `Any`.
            ENode::Reduce {
                input,
                group_key,
                aggregates,
                monotonic,
                expected_group_size,
            } => Rel::Reduce {
                input: Box::new(get(*input, reduce_input_demand(node))?),
                group_key: self.resolve_scalars(group_key),
                aggregates: aggregates.clone(),
                monotonic: *monotonic,
                expected_group_size: *expected_group_size,
            },
            // A TopK is a barrier and always requires a `Nonneg` input (its
            // per-group ordering is meaningless over signed multiplicities).
            ENode::TopK { input, shape } => Rel::TopK {
                input: Box::new(get(*input, Demand::Nonneg)?),
                shape: shape.clone(),
            },
            // A negate flips the sign, so its input takes `Any`. A `Nonneg`
            // demand on a negate itself was already rejected above.
            ENode::Negate { input } => Rel::Negate {
                input: Box::new(get(*input, Demand::Any)?),
            },
            // A threshold is a barrier: its output is non-negative regardless of
            // input, so the input takes `Any`.
            ENode::Threshold { input } => Rel::Threshold {
                input: Box::new(get(*input, Demand::Any)?),
            },
            // Join/WcoJoin/Union are sign-preserving in every input: propagate
            // the parent demand to all of them.
            ENode::Join {
                inputs,
                equivalences,
            } => {
                let resolved_inputs: Vec<Rel> = inputs
                    .iter()
                    .map(|i| get(*i, demand))
                    .collect::<Option<_>>()?;
                let base = self.resolve_equivalences(equivalences);
                let class_id = self.find(class);
                let equivalences = match spellings.and_then(|m| m.get(&class_id)) {
                    Some(ec) => crate::eqsat::join_spelling::select_join_spelling(
                        model,
                        &resolved_inputs,
                        &base,
                        &ec.classes,
                    ),
                    None => base,
                };
                Rel::Join {
                    inputs: resolved_inputs,
                    equivalences,
                }
            }
            ENode::WcoJoin {
                inputs,
                equivalences,
            } => Rel::WcoJoin {
                inputs: inputs
                    .iter()
                    .map(|i| get(*i, demand))
                    .collect::<Option<_>>()?,
                equivalences: self.resolve_equivalences(equivalences),
            },
            // An arrangement is a multiset identity, so it is sign-preserving:
            // propagate the parent demand to the input unchanged.
            ENode::ArrangeBy { input, key } => Rel::ArrangeBy {
                input: Box::new(get(*input, demand)?),
                key: self.resolve_scalars(key),
            },
            ENode::ArrangeByMany { input, keys } => Rel::ArrangeByMany {
                input: Box::new(get(*input, demand)?),
                keys: keys.iter().map(|k| self.resolve_scalars(k)).collect(),
            },
            ENode::Union { inputs } => {
                let mut rels = inputs
                    .iter()
                    .map(|i| get(*i, demand))
                    .collect::<Option<Vec<_>>>()?;
                let base = Box::new(rels.remove(0));
                Rel::Union { base, inputs: rels }
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use mz_expr::MirScalarExpr;

    use crate::eqsat::egraph::EGraph;
    use crate::eqsat::ir::{EScalar, Rel};

    #[mz_ore::test]
    fn try_arity_some_for_relational_class() {
        let mut eg = EGraph::new();
        let r = eg.add_rel(&Rel::Get {
            name: "r".to_string(),
            arity: 3,
        });
        eg.rebuild();
        assert_eq!(eg.try_arity(r), Some(3));
    }

    #[mz_ore::test]
    fn try_arity_none_for_scalar_class() {
        let mut eg = EGraph::new();
        let sid = eg.intern_scalar(&EScalar::plain(MirScalarExpr::column(0)));
        eg.rebuild();
        assert_eq!(eg.try_arity(sid), None, "a scalar class has no arity");
    }

    #[mz_ore::test]
    #[should_panic(expected = "well-defined arity")]
    fn arity_panics_on_scalar_class() {
        let mut eg = EGraph::new();
        let sid = eg.intern_scalar(&EScalar::plain(MirScalarExpr::column(0)));
        eg.rebuild();
        let _ = eg.arity(sid);
    }

    /// SP4d T10: relational extraction must consider colored relational
    /// conclusions (those `colored_saturate` produces) and pick one when it is
    /// genuinely cheaper than the base form.
    ///
    /// Fixture mirrors the Task 6 driver test: `Filter[#0 = #1](leaf)` with a
    /// color seeding `pred ≅ true`, so `colored_saturate` fires
    /// `drop_true_filter` and the Filter's class colored-merges with its input
    /// `leaf`. Under that color the cheapest representative of the Filter's class
    /// is the bare `leaf` (the Filter elided), so extraction must return the
    /// `Constant`, not the `Filter`. (RED without the T10 change: extraction
    /// returns the `Filter`.)
    #[mz_ore::test]
    fn extraction_picks_colored_conclusion_when_cheaper() {
        use std::collections::{HashMap, HashSet};

        use mz_expr::{BinaryFunc, func};
        use mz_repr::{Datum, ReprScalarType};

        use crate::eqsat::colored::{ColoredEGraph, colored_saturate};
        use crate::eqsat::colored_derive::ColoredLayer;
        use crate::eqsat::cost::CostModel;
        use crate::eqsat::egraph::{CNode, ENode};
        use crate::eqsat::objective::ArrangementCount;

        let mut eg = EGraph::new();
        let leaf = eg.add(CNode::Rel(ENode::Constant {
            card: 1,
            arity: 2,
            col_types: None,
        }));
        // Predicate `#0 = #1` (not a literal on its own).
        let pred = eg.intern_scalar(&EScalar::plain(
            MirScalarExpr::column(0)
                .call_binary(MirScalarExpr::column(1), BinaryFunc::Eq(func::Eq)),
        ));
        // The literal `true` scalar class.
        let tru = eg.intern_scalar(&EScalar::plain(MirScalarExpr::literal_ok(
            Datum::True,
            ReprScalarType::Bool,
        )));
        let filt = eg.add(CNode::Rel(ENode::Filter {
            predicates: vec![pred],
            input: leaf,
        }));
        eg.rebuild();

        // One flat color asserting `predicate ≅ true` (the `true` class wins the
        // equal-size union, so the colored-canonical predicate's `lit` is
        // `Some(true)` and `drop_true_filter`'s `all_true(p)` holds).
        let mut ceg = ColoredEGraph::new(&eg);
        let color = ceg.new_color(None);
        ceg.union(color, eg.find(tru), eg.find(pred));
        let mut color_of = HashMap::new();
        color_of.insert(eg.find(filt), color);
        let mut layer = ColoredLayer {
            ceg,
            color_of,
            empty_classes: HashSet::new(),
            delta_escalar: HashMap::new(),
        };

        let rounds = colored_saturate(&mut layer, &eg);
        assert!(rounds > 0, "colored_saturate ran at least one round");

        let model = CostModel::new();
        let root = eg.find(filt);
        let rel = eg
            .extract_with(root, &model, &ArrangementCount, Some(&mut layer), None)
            .expect("extraction");
        assert!(
            matches!(rel, Rel::Constant { .. }),
            "extraction must pick the colored conclusion (bare leaf), got {rel}",
        );
    }
}

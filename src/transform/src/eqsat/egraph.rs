// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.

//! An e-graph with **equality saturation** and **relational e-matching via a
//! generic (worst-case-optimal) join**.
//!
//! Greedy, cost-monotone rewriting (apply a rule only if it lowers cost) gets
//! stuck in local minima: a beneficial rewrite is often reachable only through
//! a cost-neutral or cost-increasing intermediate step. To avoid that, the
//! optimizer instead **saturates** an e-graph: it applies *every* rule
//! wherever it matches, regardless of cost, recording the resulting
//! equivalences compactly. Only at the end does it extract the cheapest plan.
//!
//! Finding all matches of a rule's left-hand side is the bottleneck, and it is
//! exactly a **conjunctive query** over the e-graph: each operator in the
//! pattern is an atom over the relation of e-nodes with that operator, and
//! shared pattern variables are join variables. Following *relational
//! e-matching* (Zhang et al.), we evaluate that query with a **generic join**,
//! which is worst-case optimal: it binds one variable at a time, intersecting
//! the candidate values across all atoms that mention it, never materializing a
//! larger intermediate than the final result could justify. That is the sense
//! in which "the engine itself explores the transform graph in a WCOJ manner".

/// An e-class identifier — re-exported from the generic core so there is one
/// definition.
pub use crate::eqsat::core::Id;

mod node;
mod combined;
mod build;
mod saturate;
pub(crate) mod view;

pub use self::node::*;
pub use self::combined::*;
pub use self::saturate::*;

/// Re-export the scalar-payload reducer so the colored-derivation pass
/// (`colored_derive`) can reuse the exact same reduction logic rather than
/// reimplementing it.
pub(crate) use self::saturate::reduce_escalar;

/// E-node budget for [`EGraph::saturate`]. Saturation stops growing the e-graph
/// once the total e-node count crosses this bound, then extracts from the
/// partially saturated graph. This caps the worst-case time and memory of an
/// otherwise combinatorial search; extraction from an incomplete saturation is
/// still sound (it just may miss rewrites a fuller search would have found).
///
/// The per-iteration generic join and, especially, the final extraction are
/// superlinear in the e-node count (extraction is a multi-pass DP that rebuilds
/// candidate plans per node per pass), so the bound is kept low: a plan that
/// explodes to a large e-graph costs seconds, which is unacceptable in the live
/// optimizer. Small plans saturate fully well under this bound and are
/// unaffected.
// `pub(crate)` so the colored rule driver (`colored::saturate`) can bound the
// per-color delta-node budget with the same limit the base loop uses.
pub(crate) const MAX_ENODES: usize = 600;

/// Maximum colored-saturation rounds per color in the colored rule driver
/// ([`crate::eqsat::colored::saturate::colored_saturate`]). Bounds the
/// per-color fixpoint so an explosive colored rule set cannot loop unboundedly;
/// kept small because each round rebuilds the per-color snapshot and closes
/// congruence over every visible e-node. Stopping early is sound (the color
/// simply carries fewer conclusions).
pub(crate) const COLORED_MAX_ITERS: usize = 4;

/// Per-rule, per-iteration match cap. A rule whose left-hand side matches
/// combinatorially can enumerate an unbounded number of assignments in a single
/// generic join, which is the dominant saturation cost. The enumeration stops at
/// this many matches, and a rule that hits the cap is banned for a growing
/// number of iterations (see [`EGraph::saturate`]). Modeled on egg's
/// `BackoffScheduler`: throttle the offending rule, keep the rest running.
// `pub(crate)` so the colored rule driver (`colored::saturate`) can cap colored
// match enumeration with the same per-rule limit the base loop uses.
pub(crate) const MATCH_LIMIT: usize = 1_000;

/// Initial ban length (in iterations) for a rule that exceeds [`MATCH_LIMIT`].
/// The ban doubles on each re-offense.
pub(self) const INITIAL_BAN_LEN: usize = 4;

/// Maximum inner fixpoint rounds for the `Equivalences` analysis when it is
/// run inside the saturation loop (once per outer iteration). The analysis is
/// NOT finite-height, so bounding iterations prevents non-termination.
///
/// This cap is intentionally much smaller than [`MAX_ANALYSIS_ITERS`](crate::eqsat::core::MAX_ANALYSIS_ITERS):
/// each inner round of the Equivalences fixpoint calls
/// `minimize_bounded(None, 100)` per merge, which is itself expensive
/// (expand/implications/minimize_once over `MirScalarExpr` sets). A tight
/// cap keeps per-round cost proportional to plan size. Stopping early is
/// sound: every derived equivalence reflects real node structure, and the
/// consumer (`colored_derive`'s color-aware extraction) is correct with fewer
/// known equivalences -- it misses optimizations, never produces incorrect plans.
// `pub(crate)` so the `colored_derive` module can drive the `Equivalences`
// analysis (post-saturation, on the frozen base) to a fixed bound.
pub(crate) const MAX_EQUIVALENCES_ANALYSIS_ITERS: usize = 4;

// Residual glue: `intern_scalars` is a private bridge called by `build.rs` (via
// `add_rel`, `intern_equivalences`, `seed_indexed_filters`). Keeping it here — in
// the parent module — makes it accessible to the child module without a
// visibility change (private items in a parent module are accessible to all
// descendant modules in Rust).
use crate::eqsat::ir::EScalar;

impl EGraph {
    /// Intern a list of `EScalar`s, returning their scalar e-class ids.
    fn intern_scalars(&mut self, scalars: &[EScalar]) -> Vec<Id> {
        scalars.iter().map(|s| self.intern_scalar(s)).collect()
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use mz_expr::MirScalarExpr;

    use super::*;
    use crate::eqsat::ir::EScalar;

    /// Intern a bare `MirScalarExpr` as a scalar e-class, returning its id.
    fn sc(eg: &mut EGraph, e: MirScalarExpr) -> Id {
        eg.intern_scalar(&EScalar::plain(e))
    }

    /// Add a relational e-node (wrapping it in `CNode::Rel`), returning its class.
    fn add(eg: &mut EGraph, node: ENode) -> Id {
        eg.add(CNode::Rel(node))
    }

    /// A `Filter` carries its scalar predicates as `scalar_children`; the core's
    /// `children`/`map_children` cover both relational inputs and scalar ids, in
    /// the documented order (relational first, then scalar).
    #[mz_ore::test]
    fn scalar_children_and_map_children_round_trip() {
        use crate::eqsat::core::Language;
        // A Filter ENode with two scalar Id children maps both through f.
        let n = ENode::Filter {
            input: 0,
            predicates: vec![5, 6],
        };
        assert_eq!(n.relational_children(), vec![0]);
        assert_eq!(n.scalar_children(), vec![5, 6]);
        let mapped = CombinedLang::map_children(&CNode::Rel(n.clone()), |x| x + 10);
        match mapped {
            CNode::Rel(ENode::Filter { input, predicates }) => {
                assert_eq!(input, 10);
                assert_eq!(predicates, vec![15, 16]);
            }
            _ => panic!("shape preserved"),
        }
        assert_eq!(CombinedLang::children(&CNode::Rel(n)), vec![0, 5, 6]);
    }

    /// `add_rel` interns scalar payloads as scalar e-classes and registers their
    /// `EScalar` facts; extraction reads them back from the cache, so a
    /// scalar-bearing plan round-trips structurally unchanged.
    #[mz_ore::test]
    fn add_rel_then_extract_is_identity_for_scalar_bearing_nodes() {
        use mz_repr::{Datum, ReprScalarType};
        // Filter[#0 = 1] over a Get.
        let pred = EScalar::plain(MirScalarExpr::column(0).call_binary(
            MirScalarExpr::literal_ok(Datum::Int64(1), ReprScalarType::Int64),
            mz_expr::BinaryFunc::Eq(mz_expr::func::Eq),
        ));
        let rel = Rel::Filter {
            predicates: vec![pred],
            input: Box::new(Rel::Get {
                name: "r".to_string(),
                arity: 1,
            }),
        };
        let mut eg = EGraph::new();
        let root = eg.add_rel(&rel);
        eg.rebuild();
        let out = eg
            .extract(root, &CostModel::new())
            .expect("scalar-bearing root must extract");
        assert_eq!(out, rel, "scalar payloads round-trip through Id + cache");
    }

    /// The `escalar` cache is NOT write-once: `intern_scalar` hash-conses, so the
    /// same scalar class can be interned more than once (e.g. a saturation
    /// rewrite folding a predicate to the shared literal `true`). The cached
    /// `lit` must be a deterministic function of the expr, so a later
    /// `EScalar::plain` re-intern cannot clobber a genuine `Some(true)` to `None`
    /// — which would make `cond_all_true`/`cond_any_false`/`cond_no_false`
    /// misfire on a *different* node's literal predicate and drop a plan rewrite.
    #[mz_ore::test]
    fn intern_scalar_cache_lit_is_deterministic_under_reintern() {
        let mut eg = EGraph::new();
        let t = MirScalarExpr::literal_true();
        // First intern with the correct lit fact, as `add_rel`'s `reduced_escalar`
        // produces for a folded predicate.
        let id1 = eg.intern_scalar(&EScalar::new(t.clone(), Some(true)));
        // Re-intern the SAME literal via `EScalar::plain` (lit `None`), as the
        // Phase-2a rewrite path does. It must NOT clobber the cached lit.
        let id2 = eg.intern_scalar(&EScalar::plain(t.clone()));
        assert_eq!(id1, id2, "identical exprs hash-cons to one scalar class");
        assert_eq!(
            eg.data().escalar(id1).lit,
            Some(true),
            "re-interning the same literal must not clobber the cached lit to None"
        );
    }

    /// The displayed `TreatAsEqual` column name is inert for `EScalar` equality,
    /// so a column class re-interned with different per-use names (as fusion does
    /// when distinct uses of the same column hash-cons to one class) must NOT cache
    /// an order-dependent last-write-wins name — that flakes EXPLAIN under HashMap's
    /// per-process randomized iteration. The cache keeps the smallest `name_key`,
    /// which prefers a present name over `None` and is order-independent.
    #[mz_ore::test]
    fn intern_scalar_cache_column_name_is_deterministic_under_reintern() {
        use std::sync::Arc;
        let col_a = || EScalar::plain(MirScalarExpr::named_column(0, Arc::from("a")));
        let col_b = || EScalar::plain(MirScalarExpr::named_column(0, Arc::from("b")));
        let col_none = || EScalar::plain(MirScalarExpr::column(0));

        // The cached displayed name (resolved from the cached expr) of class `id`.
        let cached_name = |eg: &EGraph, id| match &eg.data().escalar(id).expr {
            MirScalarExpr::Column(_, mz_ore::treat_as_equal::TreatAsEqual(n)) => n.clone(),
            other => panic!("expected a column expr, got {other:?}"),
        };

        // Two present names: the lexicographically-smallest wins, in either order.
        let mut eg1 = EGraph::new();
        let id_a = eg1.intern_scalar(&col_a());
        let id_b = eg1.intern_scalar(&col_b());
        assert_eq!(id_a, id_b, "same column index hash-conses to one scalar class");
        let mut eg2 = EGraph::new();
        eg2.intern_scalar(&col_b());
        let id_ba = eg2.intern_scalar(&col_a());
        assert_eq!(
            cached_name(&eg1, id_a),
            Some(Arc::<str>::from("a")),
            "the representative is the lexicographically-smallest present name"
        );
        assert_eq!(
            cached_name(&eg2, id_ba),
            cached_name(&eg1, id_a),
            "cached column name must be independent of intern order",
        );

        // A present name must beat an absent one (no silent name-stripping), in
        // either order — `Option`'s `None < Some` order must NOT win here.
        let mut eg3 = EGraph::new();
        let id_an = eg3.intern_scalar(&col_a());
        eg3.intern_scalar(&col_none());
        let mut eg4 = EGraph::new();
        eg4.intern_scalar(&col_none());
        let id_na = eg4.intern_scalar(&col_a());
        assert_eq!(
            cached_name(&eg3, id_an),
            Some(Arc::<str>::from("a")),
            "a present name must be preferred over None (Some-then-None) — not stripped",
        );
        assert_eq!(
            cached_name(&eg4, id_na),
            Some(Arc::<str>::from("a")),
            "a present name must be preferred over None (None-then-Some) — not stripped",
        );
    }

    use crate::eqsat::cost::CostModel;
    use crate::eqsat::ir::Rel;

    /// A `MAX` aggregate over column 0 (a non-linear aggregate).
    fn max_aggregate() -> mz_expr::AggregateExpr {
        mz_expr::AggregateExpr {
            func: mz_expr::AggregateFunc::MaxInt64,
            expr: MirScalarExpr::column(0),
            distinct: false,
        }
    }

    /// A reduce input class that holds BOTH a cheap `Negate`-rooted plan and a
    /// costlier non-negative plan must be extracted as the non-negative plan when
    /// it feeds a non-linear reduce. Picking the cheaper `Negate` form would be
    /// unsound: `reduce(r) != negate(reduce(negate(r)))` for a `MAX` aggregate.
    ///
    /// Before the polarity-aware extractor this test fails: a single best-of-class
    /// map picks the cheaper `Negate(Get)` (2 nodes, fewer time terms) over
    /// `Filter(Filter(Get))` (3 nodes), causing a `Negate` directly under the
    /// reduce.
    #[mz_ore::test]
    fn reduce_input_avoids_negate_representative() {
        let mut eg = EGraph::new();
        // Base relation `a`.
        let a = add(&mut eg, ENode::Get {
            name: "a".to_string(),
            arity: 1,
        });
        // Cheap, sign-negative representative: Negate(a). 2 nodes.
        let neg = add(&mut eg, ENode::Negate { input: a });
        // Costlier non-negative representative: Filter(Filter(a)). 3 nodes, more
        // time terms, so strictly costlier than the negate form.
        let p = sc(&mut eg, MirScalarExpr::column(0));
        let f1 = add(&mut eg, ENode::Filter {
            input: a,
            predicates: vec![p],
        });
        let pos = add(&mut eg, ENode::Filter {
            input: f1,
            predicates: vec![p],
        });
        // Merge the two representatives into one class `c`.
        eg.union(neg, pos);
        eg.rebuild();
        let c = eg.find(neg);
        // A reduce with a MAX aggregate over `c`.
        let root = add(&mut eg, ENode::Reduce {
            input: c,
            group_key: vec![],
            aggregates: vec![max_aggregate()],
            monotonic: false,
            expected_group_size: None,
        });

        let model = CostModel::new();
        let extracted = eg
            .extract(root, &model)
            .expect("well-formed root must extract");
        let Rel::Reduce { input, .. } = extracted else {
            panic!("root must extract to a Reduce");
        };
        assert!(
            !matches!(*input, Rel::Negate { .. }),
            "the non-linear reduce must not have a Negate directly as its input; got {input:?}"
        );
    }

    /// A negate-free graph extracts identically to a direct cost-minimizing
    /// extraction: the polarity machinery must not perturb the common path. Here
    /// the class holds two plans and the cheaper one (fewer nodes) is picked, as
    /// before.
    #[mz_ore::test]
    fn negate_free_graph_extracts_cheapest() {
        let mut eg = EGraph::new();
        let a = add(&mut eg, ENode::Get {
            name: "a".to_string(),
            arity: 1,
        });
        let p = sc(&mut eg, MirScalarExpr::column(0));
        // Cheap plan: a single filter.
        let cheap = add(&mut eg, ENode::Filter {
            input: a,
            predicates: vec![p],
        });
        // Costlier plan: two stacked filters.
        let mid = add(&mut eg, ENode::Filter {
            input: a,
            predicates: vec![p],
        });
        let costly = add(&mut eg, ENode::Filter {
            input: mid,
            predicates: vec![p],
        });
        eg.union(cheap, costly);
        eg.rebuild();
        let root = eg.find(cheap);

        let model = CostModel::new();
        let extracted = eg
            .extract(root, &model)
            .expect("well-formed root must extract");
        // The cheapest plan is the single filter directly over the Get.
        let Rel::Filter { input, .. } = extracted else {
            panic!("root must extract to a Filter");
        };
        assert!(
            matches!(*input, Rel::Get { .. }),
            "negate-free extraction must pick the single-filter plan; got {input:?}"
        );
    }

    /// A reduce whose input class has only non-negative representatives extracts
    /// that input unchanged: the nonneg demand is satisfied by the ordinary best
    /// plan.
    #[mz_ore::test]
    fn reduce_input_only_nonneg_extracts_unchanged() {
        let mut eg = EGraph::new();
        let a = add(&mut eg, ENode::Get {
            name: "a".to_string(),
            arity: 1,
        });
        let p = sc(&mut eg, MirScalarExpr::column(0));
        let pos = add(&mut eg, ENode::Filter {
            input: a,
            predicates: vec![p],
        });
        let root = add(&mut eg, ENode::Reduce {
            input: pos,
            group_key: vec![],
            aggregates: vec![max_aggregate()],
            monotonic: false,
            expected_group_size: None,
        });

        let model = CostModel::new();
        let extracted = eg
            .extract(root, &model)
            .expect("well-formed root must extract");
        let Rel::Reduce { input, .. } = extracted else {
            panic!("root must extract to a Reduce");
        };
        let Rel::Filter { input: inner, .. } = *input else {
            panic!("reduce input must be the Filter, unchanged");
        };
        assert!(
            matches!(*inner, Rel::Get { .. }),
            "the nonneg input must extract as Filter(Get), unchanged; got {inner:?}"
        );
    }

    use crate::eqsat::analysis::{ConstCols, ConstantColumns, LocalFacts};
    use mz_repr::{Datum, ReprScalarType};

    /// The ck480 shape `Let l0 = Filter[#0=123, #1=234](Get u1) in Union[Get l0,
    /// Get l0, Get l0]`, returned as `(definition, body)` `Rel`s. The body's
    /// references are opaque `LocalGet { id: 0 }` (no `get`, matching the engine's
    /// scope placeholders). The definition pins output columns 0 and 1 to the
    /// literals 123 and 234.
    fn ck480_def_and_body() -> (Rel, Rel) {
        fn col_eq(col: usize, val: i64) -> EScalar {
            EScalar::plain(MirScalarExpr::column(col).call_binary(
                MirScalarExpr::literal_ok(Datum::Int64(val), ReprScalarType::Int64),
                mz_expr::BinaryFunc::Eq(mz_expr::func::Eq),
            ))
        }
        let def = Rel::Filter {
            input: Box::new(Rel::Get {
                name: "u1".to_string(),
                arity: 3,
            }),
            predicates: vec![col_eq(0, 123), col_eq(1, 234)],
        };
        let get_l0 = || Rel::LocalGet {
            id: 0,
            arity: 3,
            get: None,
            version: None,
        };
        let body = Rel::Union {
            base: Box::new(get_l0()),
            inputs: vec![get_l0(), get_l0()],
        };
        (def, body)
    }

    /// The constant 123 as a stored `EScalar`, for asserting analysis output.
    fn lit_i64(val: i64) -> EScalar {
        EScalar::plain(MirScalarExpr::literal_ok(
            Datum::Int64(val),
            ReprScalarType::Int64,
        ))
    }

    /// Unioning the non-recursive `Let` definition into the body e-graph un-traps
    /// the definition's constant-column facts onto the body's `Get l0` class.
    ///
    /// This is the point of the Let-union step: on the pre-step-2 separate-fragment
    /// path the body's `Get l0` is an opaque `LocalGet` that proves no constant, so
    /// the fact `{0: 123, 1: 234}` is unreachable across the binding boundary. After
    /// adding the definition into the body's e-graph and unioning the `Get l0` class
    /// with the definition root, the fact reaches the `Get l0` class via congruence.
    #[mz_ore::test]
    fn let_union_untraps_constant_columns() {
        let (def, body) = ck480_def_and_body();
        let cc = ConstantColumns {
            locals: BTreeMap::new(),
        };
        let no_facts = LocalFacts::default();

        // Locate the body's `Get l0` class.
        let get_l0 = Rel::LocalGet {
            id: 0,
            arity: 3,
            get: None,
            version: None,
        };

        // Baseline (today's separate-fragment path): the body alone, `Get l0`
        // opaque. The fact must NOT be present.
        let mut baseline = EGraph::new();
        let _root = baseline.add_rel(&body);
        let get_class = baseline.add_rel(&get_l0);
        baseline.rebuild();
        baseline.saturate(&crate::eqsat::default_ruleset(), 100, &no_facts);
        let baseline_arity = |c: Id| baseline.arity(c);
        let baseline_ctx = crate::eqsat::analysis::RelCtx {
            arity: &baseline_arity,
            data: baseline.data(),
        };
        let baseline_cc = baseline.run_analysis(&cc, baseline_ctx);
        let baseline_fact = baseline_cc
            .get(&baseline.find(get_class))
            .cloned()
            .unwrap_or_default();
        assert!(
            baseline_fact.is_empty(),
            "separate-fragment baseline must NOT carry the constant fact on Get l0; \
             got {baseline_fact:?}"
        );

        // Prototype (step 2): body + definition in ONE e-graph, `Get l0` unioned
        // with the definition root. The fact MUST reach the `Get l0` class.
        let mut unioned = EGraph::new();
        let _root = unioned.add_rel(&body);
        let get_class = unioned.add_rel(&get_l0);
        let def_class = unioned.add_rel(&def);
        unioned.union(get_class, def_class);
        unioned.rebuild();
        unioned.saturate(&crate::eqsat::default_ruleset(), 100, &no_facts);
        let unioned_arity = |c: Id| unioned.arity(c);
        let unioned_ctx = crate::eqsat::analysis::RelCtx {
            arity: &unioned_arity,
            data: unioned.data(),
        };
        let unioned_cc = unioned.run_analysis(&cc, unioned_ctx);
        let fact = unioned_cc
            .get(&unioned.find(get_class))
            .cloned()
            .unwrap_or_default();
        let mut expected = ConstCols::new();
        expected.insert(0, lit_i64(123));
        expected.insert(1, lit_i64(234));
        assert_eq!(
            fact, expected,
            "after the union, Get l0 must carry {{0: 123, 1: 234}}; got {fact:?}"
        );
    }

    /// `cond_reads_indexed_global` walks through column- and row-preserving
    /// wrappers (here a `Project`) and reports the e-class as reading an indexed
    /// global only when the availability map carries a non-empty index for the
    /// global id the `Opaque` `Get` references.
    #[mz_ore::test]
    fn reads_indexed_global_sees_through_project_only_when_indexed() {
        use mz_expr::{AccessStrategy, Id, MirRelationExpr};
        use mz_repr::{GlobalId, ReprRelationType};

        // An Opaque global Get on gid 7.
        let get = MirRelationExpr::Get {
            id: Id::Global(GlobalId::User(7)),
            typ: ReprRelationType::empty(),
            access_strategy: AccessStrategy::UnknownOrLocal,
        };

        // Build: Project over the Opaque global Get.
        let mut eg = EGraph::new();
        let get_id = add(&mut eg, ENode::Opaque(get.clone()));
        let proj_id = add(&mut eg, ENode::Project {
            input: get_id,
            outputs: vec![0],
        });

        // No availability: false.
        assert!(!eg.cond_reads_indexed_global(proj_id));

        // gid 7 indexed: true, seen through the Project wrapper.
        let mut available: BTreeMap<GlobalId, Vec<Vec<MirScalarExpr>>> = BTreeMap::new();
        available.insert(GlobalId::User(7), vec![vec![MirScalarExpr::column(0)]]);
        eg.set_available(available);
        assert!(eg.cond_reads_indexed_global(proj_id));
        assert!(eg.cond_reads_indexed_global(get_id));
    }

    /// `pull_project_out_of_join_first` lifts a narrowing projection out of the
    /// first join input when that input reads an index-backed global `Get`, so
    /// the join input becomes the bare full-width `Get` again (which downstream
    /// `JoinImplementation` can serve from the maintained arrangement). After
    /// saturation the root join's e-class must contain a `Project` over a `Join`
    /// whose first input class holds the bare `Opaque` `Get` (arity 3), wider
    /// than the narrowed projection it replaced.
    #[mz_ore::test]
    fn pull_project_out_of_join_first_fires() {
        use crate::eqsat::analysis::LocalFacts;
        use crate::eqsat::ir::Rel;
        use mz_expr::{AccessStrategy, Id as MirId, MirRelationExpr};
        use mz_repr::{GlobalId, ReprRelationType, ReprScalarType};

        // An Opaque global Get on gid 7, arity 3, indexed on column #1.
        let gid = GlobalId::User(7);
        let get = MirRelationExpr::Get {
            id: MirId::Global(gid),
            typ: ReprRelationType::new(
                (0..3)
                    .map(|_| ReprScalarType::Int64.nullable(false))
                    .collect(),
            ),
            access_strategy: AccessStrategy::UnknownOrLocal,
        };

        // a = Project[#1, #2](Opaque Get), narrowing the 3-column read to 2.
        let a = Rel::Project {
            outputs: vec![1, 2],
            input: Box::new(Rel::Opaque(Box::new(get))),
        };
        // b = a bare arity-2 relation.
        let b = Rel::Get {
            name: "b".to_string(),
            arity: 2,
        };
        // Join on a.#1 = b.#0. In the join output, a occupies cols [0, 1] (it is
        // narrowed to 2 columns), b occupies cols [2, 3], so the equivalence is
        // #1 = #2.
        let join = Rel::Join {
            inputs: vec![a, b],
            equivalences: vec![vec![
                EScalar::plain(MirScalarExpr::column(1)),
                EScalar::plain(MirScalarExpr::column(2)),
            ]],
        };

        let mut eg = EGraph::new();
        let root = eg.add_rel(&join);

        // Make gid 7's index visible, or `reads_indexed_global(a)` is false and
        // the rule never fires.
        let mut available: BTreeMap<GlobalId, Vec<Vec<MirScalarExpr>>> = BTreeMap::new();
        available.insert(gid, vec![vec![MirScalarExpr::column(1)]]);
        eg.set_available(available);

        eg.rebuild();
        let no_facts = LocalFacts::default();
        eg.saturate(&crate::eqsat::default_ruleset(), 100, &no_facts);

        // Assert: the root join's class holds a Project whose child class holds a
        // Join whose first input class holds the bare Opaque Get (arity 3). The
        // pulled-up projection is m = [#1, #2] ++ shift(iota(2), 3) = [1, 2, 3, 4].
        let root = eg.find(root);
        let class_holds_bare_opaque_get = |id: Id| -> bool {
            eg.rel_class_nodes(id).iter().any(|n| {
                matches!(
                    n,
                    ENode::Opaque(MirRelationExpr::Get {
                        id: MirId::Global(g),
                        ..
                    }) if *g == gid
                )
            })
        };

        let found = eg.rel_class_nodes(root).iter().any(|n| {
            let ENode::Project { input, outputs } = n else {
                return false;
            };
            if outputs != &vec![1usize, 2, 3, 4] {
                return false;
            }
            // The Project's child class must hold a Join whose first input
            // class holds the bare Opaque Get.
            eg.rel_class_nodes(*input).iter().any(|inner| {
                let ENode::Join { inputs, .. } = inner else {
                    return false;
                };
                inputs
                    .first()
                    .is_some_and(|&first| class_holds_bare_opaque_get(first))
            })
        });

        assert!(
            found,
            "root join's class must contain Project[1,2,3,4] over a Join whose \
             first input is the bare Opaque Get (arity 3); classes: {:?}",
            eg.rel_class_nodes(root)
        );
    }
}

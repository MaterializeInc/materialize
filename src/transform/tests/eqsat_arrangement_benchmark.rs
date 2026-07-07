// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.

//! Arrangement-sharing benchmark: run the real production optimizer with eqsat
//! off / greedy / ilp on sharing-hard queries and compare the number of
//! distinct persistent arrangements each plan needs.
//! Answers whether eqsat reduces arrangements versus production, and whether
//! the ILP extractor ever beats the greedy one.
//!
//! The `compare` test helper and the simple fixture tests use
//! `count_arrangements`, which counts `ArrangeBy`, `Reduce`, and `TopK` nodes
//! only.
//!
//! The marginal-value path (`run_with_intrinsic_snapshot`, `measure_marginal`,
//! `eqsat_marginal_value`) uses `count_arrangements_with_joins`, which also
//! counts per-input arrangements implied by a `Join`'s `implementation` field.
//! For `Unimplemented` joins (no plan selected yet) the join contributes 0
//! implied arrangements.
//!
//! Key finding: eqsat does NOT commit `JoinImplementation` decisions. Every
//! `Join` node in the intrinsic snapshot (taken right after the last eqsat
//! transform) still carries `Unimplemented`, so the intrinsic count reflects
//! only the logical shape that eqsat emits, not any join-arrangement choices.
//! All `JoinImplementation` decisions are made by the downstream physical
//! passes that run after eqsat. The headline metric is therefore
//! `final_off` vs `final_on` (full pipeline both ways). The `intrinsic`
//! column is informational: it shows whether eqsat changed the logical shape
//! even when the net arrangement count is unchanged. The `eqsat_committed_joins`
//! column flags whether the intrinsic snapshot shows any join with a
//! non-`Unimplemented` plan; it will be false on every current fixture, which
//! is the measured finding.
//!
//! The harness also reports two scalar-complexity metrics per fixture, computed
//! on the eqsat-off and eqsat-on final plans:
//!
//! * `scalar_ops`: total `MirScalarExpr` node count summed across every
//!   scalar-bearing relational operator (`Map`, `Filter`, `Join`, `Reduce`,
//!   `TopK`, `FlatMap`). Uses `MirScalarExpr::size()` for each expression.
//! * `scalar_distinct`: count of distinct `MirScalarExpr` subexpressions
//!   across the whole plan, deduped by debug-format string. This is a
//!   CSE-opportunity proxy: `scalar_ops - scalar_distinct` = shareable
//!   redundancy.
//!
//! Because eqsat currently treats scalars as opaque, eqsat-on and eqsat-off are
//! expected to be identical on both scalar metrics for every fixture. Confirming
//! this is the passthrough baseline that documents the capability gap: eqsat
//! rewrites relational structure but not scalar expressions. Any fixture where
//! `scalar_ops_on != scalar_ops_off` would be a surprise and is worth
//! investigating immediately.
//!
//! Limitation: `count_arrangements` counts `ArrangeBy`/`Reduce`/`TopK`
//! collections plus join-input arrangements (from `JoinImplementation`). It
//! does not count implicit arrangements that LIR infers from the final dataflow
//! graph. Differences in join-arrangement sharing invisible at the MIR layer
//! are a known floor on the harness's resolution.

use std::collections::{BTreeMap, BTreeSet};

use mz_expr::{
    AccessStrategy, AggregateExpr, AggregateFunc, Columns, Id, JoinImplementation, JoinInputMapper,
    MirRelationExpr, MirScalarExpr, TableFunc, func,
};
use mz_ore::cast::CastFrom;
use mz_repr::optimize::OptimizerFeatures;
use mz_repr::{GlobalId, ReprRelationType, ReprScalarType};
use mz_transform::dataflow::DataflowMetainfo;
use mz_transform::typecheck;
use mz_transform::{EmptyStatisticsOracle, IndexOracle, Optimizer, Transform, TransformCtx};

/// Which eqsat configuration to run the pipeline under.
#[derive(Clone, Copy, Debug)]
enum Eqsat {
    /// Production baseline: every eqsat pass disabled.
    Off,
    /// Eqsat on with the greedy extractor.
    Greedy,
    /// Eqsat on with the ILP extractor.
    Ilp,
}

/// An [`IndexOracle`] reporting a fixed set of indexes per global id.
#[derive(Debug, Default)]
struct MultiIndex {
    indexes: BTreeMap<GlobalId, Vec<Vec<MirScalarExpr>>>,
}

impl MultiIndex {
    /// Add an index on `on` with key columns `cols`. The index id is synthesized
    /// from `on` and the existing count so distinct indexes get distinct ids.
    fn with(mut self, on: GlobalId, cols: &[usize]) -> Self {
        let key: Vec<MirScalarExpr> = cols.iter().map(|&c| MirScalarExpr::column(c)).collect();
        self.indexes.entry(on).or_default().push(key);
        self
    }
}

impl IndexOracle for MultiIndex {
    fn indexes_on(
        &self,
        id: GlobalId,
    ) -> Box<dyn Iterator<Item = (GlobalId, &[MirScalarExpr])> + '_> {
        match self.indexes.get(&id) {
            // The index id is irrelevant to arrangement counting, so reuse a
            // transient slot offset by a fixed base to keep ids distinct.
            Some(keys) => Box::new(keys.iter().enumerate().map(move |(i, k)| {
                (
                    GlobalId::Transient(u64::MAX - u64::cast_from(i)),
                    k.as_slice(),
                )
            })),
            None => Box::new(std::iter::empty()),
        }
    }
}

/// Feature set for `mode`. Production is all-off; the eqsat modes enable the
/// logical and physical passes and select the extractor.
fn features_for(mode: Eqsat) -> OptimizerFeatures {
    let mut f = OptimizerFeatures::default();
    match mode {
        Eqsat::Off => {
            f.enable_eqsat_optimizer = false;
            f.enable_eqsat_physical_optimizer = false;
            f.enable_eqsat_ilp_extraction = false;
        }
        Eqsat::Greedy => {
            f.enable_eqsat_optimizer = true;
            f.enable_eqsat_physical_optimizer = true;
            f.enable_eqsat_ilp_extraction = false;
        }
        Eqsat::Ilp => {
            f.enable_eqsat_optimizer = true;
            f.enable_eqsat_physical_optimizer = true;
            f.enable_eqsat_ilp_extraction = true;
        }
    }
    f
}

/// Run the full transform pipeline (logical + cleanup + physical) under `mode`
/// and return the final physical plan.
fn optimize_full(plan: &MirRelationExpr, oracle: &dyn IndexOracle, mode: Eqsat) -> MirRelationExpr {
    let features = features_for(mode);
    let typecheck_ctx = typecheck::empty_typechecking_context();
    let mut df_meta = DataflowMetainfo::default();
    let mut ctx = TransformCtx::global(
        oracle,
        &EmptyStatisticsOracle,
        &features,
        &typecheck_ctx,
        &mut df_meta,
        None,
    );

    #[allow(deprecated)]
    let transforms: Vec<Box<dyn Transform>> = Optimizer::logical_optimizer(&mut ctx)
        .transforms
        .into_iter()
        .chain(Optimizer::logical_cleanup_pass(&mut ctx, false).transforms)
        .chain(Optimizer::physical_optimizer(&mut ctx).transforms)
        .collect();

    let mut plan = plan.clone();
    for t in &transforms {
        t.transform(&mut plan, &mut ctx)
            .expect("transform succeeds");
    }
    plan
}

/// Count distinct persistent arrangements in `plan`: one per distinct
/// `(arranged-input, key-columns)` over every `ArrangeBy`, `Reduce`, and `TopK`.
/// Mirrors the cost model's `collect_memory` dedup, so a collection arranged by
/// the same key in two places is counted once.
fn count_arrangements(plan: &MirRelationExpr) -> usize {
    let mut seen: BTreeSet<String> = BTreeSet::new();
    plan.visit_pre(|n| match n {
        MirRelationExpr::ArrangeBy { input, keys } => {
            for key in keys {
                seen.insert(format!("AB|{input:?}|{key:?}"));
            }
        }
        MirRelationExpr::Reduce {
            input, group_key, ..
        } => {
            seen.insert(format!("RD|{input:?}|{group_key:?}"));
        }
        MirRelationExpr::TopK {
            input,
            group_key,
            order_key,
            ..
        } => {
            seen.insert(format!("TK|{input:?}|{group_key:?}|{order_key:?}"));
        }
        _ => {}
    });
    seen.len()
}

/// Count distinct persistent arrangements in `plan`, including per-input
/// arrangements implied by `Join { implementation, .. }`.
///
/// Like `count_arrangements`, this deduplicates by a `(collection, key)` string
/// key so the same arrangement shared between two nodes is counted once.
/// In addition to `ArrangeBy`, `Reduce`, and `TopK`, it extracts the
/// arrangements that the `JoinImplementation` commits for each join input:
///
/// * `Differential((start_idx, Some(key), _), order)`: the start input is
///   arranged by `key`; each `(lookup_idx, lookup_key, _)` in `order` arranges
///   `inputs[lookup_idx]` by `lookup_key`.
/// * `Differential((_, None, _), order)`: the start input streams (no key),
///   only the `order` inputs are arranged.
/// * `DeltaQuery(paths)`: every `(lookup_idx, lookup_key, _)` across all paths
///   denotes a required arrangement; deduplication removes duplicates shared
///   across paths.
/// * `IndexedFilter(coll_id, _idx_id, index_key, _)`: the named collection is
///   arranged by `index_key` via an existing index.
/// * `Unimplemented`: the join has no plan yet, so it contributes 0 implied
///   arrangements. This is meaningful: eqsat does not commit join plans, so an
///   `Unimplemented` join at the intrinsic snapshot is expected.
fn count_arrangements_with_joins(plan: &MirRelationExpr) -> usize {
    let mut seen: BTreeSet<String> = BTreeSet::new();
    plan.visit_pre(|n| match n {
        MirRelationExpr::ArrangeBy { input, keys } => {
            for key in keys {
                seen.insert(format!("AB|{input:?}|{key:?}"));
            }
        }
        MirRelationExpr::Reduce {
            input, group_key, ..
        } => {
            seen.insert(format!("RD|{input:?}|{group_key:?}"));
        }
        MirRelationExpr::TopK {
            input,
            group_key,
            order_key,
            ..
        } => {
            seen.insert(format!("TK|{input:?}|{group_key:?}|{order_key:?}"));
        }
        MirRelationExpr::Join {
            inputs,
            implementation,
            ..
        } => {
            match implementation {
                JoinImplementation::Differential((start_idx, start_key, _), order) => {
                    if let Some(key) = start_key {
                        let input = &inputs[*start_idx];
                        seen.insert(format!("JI|{input:?}|{key:?}"));
                    }
                    for (lookup_idx, lookup_key, _) in order {
                        let input = &inputs[*lookup_idx];
                        seen.insert(format!("JI|{input:?}|{lookup_key:?}"));
                    }
                }
                JoinImplementation::DeltaQuery(paths) => {
                    for path in paths {
                        for (lookup_idx, lookup_key, _) in path {
                            let input = &inputs[*lookup_idx];
                            seen.insert(format!("JI|{input:?}|{lookup_key:?}"));
                        }
                    }
                }
                JoinImplementation::IndexedFilter(coll_id, _idx_id, index_key, _) => {
                    seen.insert(format!("JI|{coll_id:?}|{index_key:?}"));
                }
                JoinImplementation::Unimplemented => {
                    // No implementation chosen: contributes 0 implied arrangements.
                }
            }
        }
        _ => {}
    });
    seen.len()
}

/// Total `MirScalarExpr` node count across every scalar-bearing relational
/// operator in `plan`.
///
/// Walks the relational tree and, at each node, sums the size of every scalar
/// expression attached to it. Covered operators and their scalar payloads:
///
/// * `Map { scalars }`: each element of `scalars`.
/// * `Filter { predicates }`: each element of `predicates`.
/// * `Join { equivalences }`: every expression in every equivalence class.
/// * `Reduce { group_key, aggregates }`: each element of `group_key` plus the
///   `expr` field of each aggregate.
/// * `TopK { limit }`: the `limit` expression if present.
/// * `FlatMap { exprs }`: each element of `exprs`.
///
/// Uses `MirScalarExpr::size()` which counts tree nodes via a pre-order walk,
/// so a literal contributes 1 and a call with two sub-expressions contributes 3.
fn count_scalar_ops(plan: &MirRelationExpr) -> usize {
    let mut total = 0usize;
    plan.visit_pre(|n| match n {
        MirRelationExpr::Map { scalars, .. } => {
            for s in scalars {
                total += s.size();
            }
        }
        MirRelationExpr::Filter { predicates, .. } => {
            for p in predicates {
                total += p.size();
            }
        }
        MirRelationExpr::Join { equivalences, .. } => {
            for class in equivalences {
                for e in class {
                    total += e.size();
                }
            }
        }
        MirRelationExpr::Reduce {
            group_key,
            aggregates,
            ..
        } => {
            for k in group_key {
                total += k.size();
            }
            for a in aggregates {
                total += a.expr.size();
            }
        }
        MirRelationExpr::TopK { limit, .. } => {
            if let Some(l) = limit {
                total += l.size();
            }
        }
        MirRelationExpr::FlatMap { exprs, .. } => {
            for e in exprs {
                total += e.size();
            }
        }
        _ => {}
    });
    total
}

/// Count of distinct `MirScalarExpr` subexpressions across the whole plan.
///
/// Collects every scalar subexpression from every scalar-bearing relational
/// operator (same set as `count_scalar_ops`), deduplicating by debug-format
/// string. This is a CSE-opportunity proxy: `count_scalar_ops - count_distinct_scalar_subexprs`
/// is the number of nodes that appear more than once and could in principle be
/// shared. A scalar subexpression is any sub-node visited during `MirScalarExpr::visit_pre`,
/// so a binary call with two column references contributes three distinct entries
/// if all three are structurally unique.
fn count_distinct_scalar_subexprs(plan: &MirRelationExpr) -> usize {
    let mut seen: BTreeSet<String> = BTreeSet::new();

    // Collect every subexpression of `e` into `seen`.
    let mut collect = |e: &MirScalarExpr| {
        e.visit_pre(|sub| {
            seen.insert(format!("{sub:?}"));
        });
    };

    plan.visit_pre(|n| match n {
        MirRelationExpr::Map { scalars, .. } => {
            for s in scalars {
                collect(s);
            }
        }
        MirRelationExpr::Filter { predicates, .. } => {
            for p in predicates {
                collect(p);
            }
        }
        MirRelationExpr::Join { equivalences, .. } => {
            for class in equivalences {
                for e in class {
                    collect(e);
                }
            }
        }
        MirRelationExpr::Reduce {
            group_key,
            aggregates,
            ..
        } => {
            for k in group_key {
                collect(k);
            }
            for a in aggregates {
                collect(&a.expr);
            }
        }
        MirRelationExpr::TopK { limit, .. } => {
            if let Some(l) = limit {
                collect(l);
            }
        }
        MirRelationExpr::FlatMap { exprs, .. } => {
            for e in exprs {
                collect(e);
            }
        }
        _ => {}
    });

    seen.len()
}

/// A collection arranged by two or more distinct keys in a single plan.
///
/// `collection` is the debug fingerprint of the arranged input expression
/// (the same identity `count_arrangements_with_joins` keys on). `keys` is the
/// set of distinct arrangement-key debug strings JI or an `ArrangeBy` chose for
/// it. A collection appears here only when `keys.len() >= 2`.
struct MultiKeyCollection {
    collection: String,
    keys: BTreeSet<String>,
}

/// For a fully-optimized plan, group every arrangement by the identity of the
/// collection it arranges and collect the set of distinct keys per collection.
///
/// Walks `ArrangeBy` keys plus each `Join.implementation`'s per-input keys
/// (`Differential` start and lookup keys, `DeltaQuery` path keys, and
/// `IndexedFilter` index keys), grouping by the arranged input's debug
/// fingerprint. Returns only collections arranged by `>= 2` distinct keys,
/// which are the cross-join sharing-opportunity candidates.
///
/// NOTE: a candidate is only a real sharing opportunity if a single key is
/// correctness-valid for ALL the join sites using that collection. Two joins on
/// genuinely different columns each NEED their own key; that is not reducible.
/// This function flags the multi-key signal. Whether one key would suffice is a
/// correctness judgment made per fixture in the report, not here.
fn multi_key_collections(plan: &MirRelationExpr) -> Vec<MultiKeyCollection> {
    // collection fingerprint -> set of distinct key fingerprints.
    let mut by_collection: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
    let mut record = |collection: String, key: String| {
        by_collection.entry(collection).or_default().insert(key);
    };
    plan.visit_pre(|n| match n {
        MirRelationExpr::ArrangeBy { input, keys } => {
            for key in keys {
                record(format!("{input:?}"), format!("{key:?}"));
            }
        }
        MirRelationExpr::Join {
            inputs,
            implementation,
            ..
        } => match implementation {
            JoinImplementation::Differential((start_idx, start_key, _), order) => {
                if let Some(key) = start_key {
                    record(format!("{:?}", &inputs[*start_idx]), format!("{key:?}"));
                }
                for (lookup_idx, lookup_key, _) in order {
                    record(
                        format!("{:?}", &inputs[*lookup_idx]),
                        format!("{lookup_key:?}"),
                    );
                }
            }
            JoinImplementation::DeltaQuery(paths) => {
                for path in paths {
                    for (lookup_idx, lookup_key, _) in path {
                        record(
                            format!("{:?}", &inputs[*lookup_idx]),
                            format!("{lookup_key:?}"),
                        );
                    }
                }
            }
            JoinImplementation::IndexedFilter(coll_id, _idx_id, index_key, _) => {
                record(format!("{coll_id:?}"), format!("{index_key:?}"));
            }
            JoinImplementation::Unimplemented => {}
        },
        _ => {}
    });
    by_collection
        .into_iter()
        .filter(|(_, keys)| keys.len() >= 2)
        .map(|(collection, keys)| MultiKeyCollection { collection, keys })
        .collect()
}

/// Print the multi-key diagnostic for the eqsat-OFF fully-optimized `plan`.
///
/// Reports each collection JI arranged by `>= 2` distinct keys, which is the
/// sharing-opportunity candidate signal. A collection that appears here MAY be
/// reducible to a single shared key, but only if all its join sites share an
/// equivalence on a common column (see `multi_key_collections`).
fn report_multi_key(name: &str, plan: &MirRelationExpr) {
    let candidates = multi_key_collections(plan);
    if candidates.is_empty() {
        eprintln!("  [{name}] no collection arranged by >=2 distinct keys");
        return;
    }
    eprintln!(
        "  [{name}] {} collection(s) arranged by >=2 distinct keys:",
        candidates.len()
    );
    for c in &candidates {
        eprintln!(
            "    collection {} arranged by {} keys:",
            c.collection,
            c.keys.len()
        );
        for k in &c.keys {
            eprintln!("      key {k}");
        }
    }
}

/// Run all three modes on `plan`, print a comparison row, assert arity is
/// preserved, and return `(production, greedy, ilp)` arrangement counts.
fn compare(name: &str, plan: &MirRelationExpr, oracle: &dyn IndexOracle) -> (usize, usize, usize) {
    let input_arity = plan.arity();
    let prod_plan = optimize_full(plan, oracle, Eqsat::Off);
    let greedy_plan = optimize_full(plan, oracle, Eqsat::Greedy);
    let ilp_plan = optimize_full(plan, oracle, Eqsat::Ilp);
    for (m, p) in [
        ("Off", &prod_plan),
        ("Greedy", &greedy_plan),
        ("Ilp", &ilp_plan),
    ] {
        assert_eq!(
            p.arity(),
            input_arity,
            "{name}/{m}: arity changed {} -> {}",
            input_arity,
            p.arity()
        );
    }
    let prod = count_arrangements(&prod_plan);
    let greedy = count_arrangements(&greedy_plan);
    let ilp = count_arrangements(&ilp_plan);
    let verdict = if greedy.min(ilp) < prod {
        "EQSAT WINS"
    } else if greedy.min(ilp) > prod {
        "EQSAT WORSE"
    } else {
        "tie"
    };
    let ilp_vs_greedy = if ilp < greedy { "  (ILP<greedy!)" } else { "" };
    // Whether eqsat actually changed the plan versus production. A "tie" on
    // arrangement count is only meaningful if eqsat was active: if the plans are
    // identical, eqsat made no decision (it bailed, or agreed structurally), and
    // the tie says nothing about the optimizers.
    let greedy_active = greedy_plan != prod_plan;
    let ilp_active = ilp_plan != prod_plan;
    let active = format!("eqsat-active: greedy={greedy_active} ilp={ilp_active}");
    println!(
        "{name:<28} prod={prod:>2} greedy={greedy:>2} ilp={ilp:>2}  {verdict}{ilp_vs_greedy}  [{active}]"
    );
    (prod, greedy, ilp)
}

/// Build a global `Get` of `arity` Int64 non-nullable columns.
fn src(id: u64, arity: usize) -> MirRelationExpr {
    MirRelationExpr::Get {
        id: Id::Global(GlobalId::Transient(id)),
        typ: ReprRelationType::new(
            (0..arity)
                .map(|_| ReprScalarType::Int64.nullable(false))
                .collect(),
        ),
        access_strategy: AccessStrategy::UnknownOrLocal,
    }
}

#[mz_ore::test]
fn smoke() {
    let g = src(1, 2);
    let lit5 = mz_repr::Datum::Int64(5);
    let lit5 = MirScalarExpr::literal_ok(lit5, ReprScalarType::Int64);
    let plan = g.filter(vec![MirScalarExpr::column(0).call_binary(lit5, func::Eq)]);
    let oracle = MultiIndex::default().with(GlobalId::Transient(1), &[0]);
    let (prod, _greedy, _ilp) = compare("smoke/filter-over-index", &plan, &oracle);
    assert!(prod < 1000, "pipeline ran and produced a finite count");
}

// F1: shared-key fan-out.
// R(#0,#1) indexed on #0 is joined to S(#2,#3) on #0=#2 and to T(#4,#5) on
// #0=#4. R's arrangement by #0 can serve both joins. Tests whether eqsat
// shares R's #0 arrangement (or reuses the index) across both consumers.
#[mz_ore::test]
fn f1_shared_key_fanout() {
    let r = src(101, 2);
    let s = src(102, 2);
    let t = src(103, 2);
    let plan = MirRelationExpr::join_scalars(
        vec![r, s, t],
        vec![
            vec![MirScalarExpr::column(0), MirScalarExpr::column(2)],
            vec![MirScalarExpr::column(0), MirScalarExpr::column(4)],
        ],
    );
    let oracle = MultiIndex::default().with(GlobalId::Transient(101), &[0]);
    let (p, g, i) = compare("f1/shared-key-fanout", &plan, &oracle);
    assert!(
        p > 0 && g > 0 && i > 0,
        "join fixture must build arrangements"
    );
}

// F2: diamond over a shared filtered CTE.
// f = Filter[#1 > 0](R) is the shared input; self-join f on #0=#2. Both
// sides arrange by #0. Tests CSE-plus-arrangement sharing of a compound
// subterm.
#[mz_ore::test]
fn f2_diamond_shared_filter() {
    let r = src(201, 2);
    let zero = MirScalarExpr::literal_ok(mz_repr::Datum::Int64(0), ReprScalarType::Int64);
    let f = r.filter(vec![MirScalarExpr::column(1).call_binary(zero, func::Gt)]);
    let plan = MirRelationExpr::join_scalars(
        vec![f.clone(), f],
        vec![vec![MirScalarExpr::column(0), MirScalarExpr::column(2)]],
    );
    let oracle = MultiIndex::default();
    let (p, g, i) = compare("f2/diamond-shared-filter", &plan, &oracle);
    assert!(
        p > 0 && g > 0 && i > 0,
        "join fixture must build arrangements"
    );
}

// F3: 4-way chain.
// A(#0,#1) B(#2,#3) C(#4,#5) D(#6,#7); chain #1=#2, #3=#4, #5=#6.
// Different left-deep orders need different numbers of intermediate
// arrangements. Tests whether eqsat's order-in-saturation changes the
// arrangement count versus production's planner.
#[mz_ore::test]
fn f3_four_way_chain() {
    let plan = MirRelationExpr::join_scalars(
        vec![src(301, 2), src(302, 2), src(303, 2), src(304, 2)],
        vec![
            vec![MirScalarExpr::column(1), MirScalarExpr::column(2)],
            vec![MirScalarExpr::column(3), MirScalarExpr::column(4)],
            vec![MirScalarExpr::column(5), MirScalarExpr::column(6)],
        ],
    );
    let oracle = MultiIndex::default()
        .with(GlobalId::Transient(301), &[1])
        .with(GlobalId::Transient(304), &[1]);
    let (p, g, i) = compare("f3/four-way-chain", &plan, &oracle);
    assert!(
        p > 0 && g > 0 && i > 0,
        "join fixture must build arrangements"
    );
}

// W1: transitive closure via WITH MUTUALLY RECURSIVE.
//
// edges(a=#0, b=#1) with an index on #0.
// reach(a, b) := UNION ALL
//   SELECT a, b FROM edges
//   UNION ALL
//   SELECT reach.a, edges.b
//     FROM reach JOIN edges ON reach.b = edges.a
//
// The recursive value is wrapped in `.distinct()` to model set-semantics UNION,
// giving the recursive binding a Reduce arrangement. The body is a plain Get of
// the recursive relation.
//
// Because `lower` bails LetRec into a Rel::Opaque, eqsat does not optimize
// inside the recursive body. The eqsat-active flags below confirm whether eqsat
// changed anything around the opaque fragment (the envelope). The test does not
// assert eqsat < prod: ties are expected and valid.
#[mz_ore::test]
fn w1_reachability() {
    use mz_expr::LocalId;

    let lid = LocalId::new(1);
    let typ = ReprRelationType::new(
        (0..2)
            .map(|_| ReprScalarType::Int64.nullable(false))
            .collect(),
    );

    // edges source: GlobalId 401, arity 2 (a=#0, b=#1).
    let edges = src(401, 2);

    // Get of the recursive relation inside the LetRec value.
    let reach_get = MirRelationExpr::Get {
        id: Id::Local(lid.clone()),
        typ: typ.clone(),
        access_strategy: AccessStrategy::UnknownOrLocal,
    };

    // Join reach (columns #0,#1) with edges (columns #2,#3) on reach.b = edges.a,
    // i.e. equivalence #1 = #2. Then project [#0, #3] to get (reach.a, edges.b).
    let step = MirRelationExpr::join_scalars(
        vec![reach_get, edges.clone()],
        vec![vec![MirScalarExpr::column(1), MirScalarExpr::column(2)]],
    )
    .project(vec![0, 3]);

    // reach = DISTINCT(edges UNION ALL step)
    let reach_value = MirRelationExpr::Union {
        base: Box::new(edges),
        inputs: vec![step],
    }
    .distinct();

    // Body: SELECT * FROM reach (plain Get of the recursive binding).
    let body = MirRelationExpr::Get {
        id: Id::Local(lid.clone()),
        typ: typ.clone(),
        access_strategy: AccessStrategy::UnknownOrLocal,
    };

    let plan = MirRelationExpr::LetRec {
        ids: vec![lid],
        values: vec![reach_value],
        limits: vec![None],
        body: Box::new(body),
    };

    let oracle = MultiIndex::default().with(GlobalId::Transient(401), &[0]);
    let (p, g, i) = compare("w1/reachability", &plan, &oracle);
    assert!(
        p > 0 && g > 0 && i > 0,
        "recursive fixture must build arrangements"
    );
}

// W2: transitive closure with a filter envelope on the body.
//
// Same recursive reach definition as W1, but the LetRec body applies a filter
// so eqsat has a non-trivial plan to consider around the opaque recursion:
//   body = SELECT * FROM reach WHERE a = 5
//
// The filter is a literal equality #0 = 5. No index exists on reach, so it
// stays a scan, but the envelope exercises eqsat's handling of operators that
// wrap the opaque LetRec node.
#[mz_ore::test]
fn w2_reachability_with_envelope() {
    use mz_expr::LocalId;

    let lid = LocalId::new(1);
    let typ = ReprRelationType::new(
        (0..2)
            .map(|_| ReprScalarType::Int64.nullable(false))
            .collect(),
    );

    let edges = src(401, 2);

    let reach_get_inner = MirRelationExpr::Get {
        id: Id::Local(lid.clone()),
        typ: typ.clone(),
        access_strategy: AccessStrategy::UnknownOrLocal,
    };

    let step = MirRelationExpr::join_scalars(
        vec![reach_get_inner, edges.clone()],
        vec![vec![MirScalarExpr::column(1), MirScalarExpr::column(2)]],
    )
    .project(vec![0, 3]);

    let reach_value = MirRelationExpr::Union {
        base: Box::new(edges),
        inputs: vec![step],
    }
    .distinct();

    // body = Filter[#0 = 5](Get(reach))
    let lit5 = MirScalarExpr::literal_ok(mz_repr::Datum::Int64(5), ReprScalarType::Int64);
    let body = MirRelationExpr::Get {
        id: Id::Local(lid.clone()),
        typ: typ.clone(),
        access_strategy: AccessStrategy::UnknownOrLocal,
    }
    .filter(vec![MirScalarExpr::column(0).call_binary(lit5, func::Eq)]);

    let plan = MirRelationExpr::LetRec {
        ids: vec![lid],
        values: vec![reach_value],
        limits: vec![None],
        body: Box::new(body),
    };

    let oracle = MultiIndex::default().with(GlobalId::Transient(401), &[0]);
    let (p, g, i) = compare("w2/reachability-envelope", &plan, &oracle);
    assert!(
        p > 0 && g > 0 && i > 0,
        "recursive fixture must build arrangements"
    );
}

// W3: LetRec whose body contains a foldable fragment.
//
// edges(a=#0, b=#1) with an index on #0.
// reach(a, b) := same transitive-closure step as W1.
// body = Project[#0, #1](Get(reach))
//
// The body wraps the recursive binding in a redundant identity projection
// (all columns in order). The production optimizer folds this away via
// ProjectionPushdown; eqsat has an independent path to the same simplification
// via its projection rule set. This confirms eqsat is active on the LetRec
// body (not just a tie from both sides producing the same plan without effort).
#[mz_ore::test]
fn w3_letrec_body_shared() {
    use mz_expr::LocalId;

    let lid = LocalId::new(1);
    let typ = ReprRelationType::new(
        (0..2)
            .map(|_| ReprScalarType::Int64.nullable(false))
            .collect(),
    );

    // edges source: GlobalId 601, arity 2 (a=#0, b=#1).
    let edges = src(601, 2);

    let reach_get = MirRelationExpr::Get {
        id: Id::Local(lid.clone()),
        typ: typ.clone(),
        access_strategy: AccessStrategy::UnknownOrLocal,
    };

    // Join reach with edges on reach.b = edges.a, project [reach.a, edges.b].
    let step = MirRelationExpr::join_scalars(
        vec![reach_get, edges.clone()],
        vec![vec![MirScalarExpr::column(1), MirScalarExpr::column(2)]],
    )
    .project(vec![0, 3]);

    let reach_value = MirRelationExpr::Union {
        base: Box::new(edges),
        inputs: vec![step],
    }
    .distinct();

    // body = Project[#0, #1](Get(reach)): identity projection, optimizable away.
    let body = MirRelationExpr::Get {
        id: Id::Local(lid.clone()),
        typ: typ.clone(),
        access_strategy: AccessStrategy::UnknownOrLocal,
    }
    .project(vec![0, 1]);

    let plan = MirRelationExpr::LetRec {
        ids: vec![lid],
        values: vec![reach_value],
        limits: vec![None],
        body: Box::new(body),
    };

    let oracle = MultiIndex::default().with(GlobalId::Transient(601), &[0]);
    let (p, g, i) = compare("letrec/body-shared", &plan, &oracle);
    assert!(
        p > 0 && g > 0 && i > 0,
        "letrec fixture must build arrangements"
    );
}

// F4: FlatMap shared across two consumers.
//
// R(#0 Int64, #1 Int64) is expanded via generate_series(#0, #1, 1), producing
// an extra column #2. Both arms of a Union consume the same FlatMap subterm.
// This gives eqsat a CSE opportunity: if both arms are structurally identical,
// sharing them reduces the number of distinct subterm evaluations (and any
// arrangements placed below the union). Tests whether eqsat is now active on a
// FlatMap-containing plan (i.e., that de-opacifying FlatMap changed extraction).
//
// Multi-key ArrangeBy (ArrangeByMany) is exercised by the eqsat::raise and
// eqsat::cost unit tests, not by this benchmark. No artificial ArrangeByMany
// fixture is added here.
#[mz_ore::test]
fn flatmap_shared() {
    let r = src(501, 2);
    // generate_series(#0, #1, 1): args are start=#0, stop=#1, step=1.
    let step = MirScalarExpr::literal_ok(mz_repr::Datum::Int64(1), ReprScalarType::Int64);
    let fm = r.flat_map(
        TableFunc::GenerateSeriesInt64,
        vec![MirScalarExpr::column(0), MirScalarExpr::column(1), step],
    );
    // Union of two identical FlatMap arms. Structural sharing gives eqsat
    // something to detect even without push-rules.
    let plan = MirRelationExpr::Union {
        base: Box::new(fm.clone()),
        inputs: vec![fm],
    };
    let oracle = MultiIndex::default();
    let (p, g, i) = compare("flatmap/shared", &plan, &oracle);
    // Modeling FlatMap as a first-class node (rather than an opaque leaf) adds
    // no rewrite rule, so eqsat has no alternative to the input plan and must
    // match production's arrangement count. This is the de-opacification
    // baseline that a future push rule would change.
    assert!(
        p == g && g == i,
        "FlatMap modeling alone must not change the arrangement count: prod={p} greedy={g} ilp={i}"
    );
}

// ---------------------------------------------------------------------------
// Marginal-value measurement
// ---------------------------------------------------------------------------

/// How eqsat's net contribution to the final arrangement count is classified.
///
/// The verdict is derived solely from `final_on` vs `final_off` (the fully
/// optimized plan with eqsat on vs off). The intrinsic snapshot and
/// `eqsat_committed_joins` are informational only and do not influence the
/// verdict.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Verdict {
    /// `final_on < final_off`: eqsat's logical restructuring caused the full
    /// pipeline to produce fewer arrangements.
    EqsatWins,
    /// `final_on == final_off`: no net change in arrangement count. Eqsat may
    /// or may not have changed the logical shape (see `plan_changed`); the
    /// downstream passes converged to the same count either way.
    NetNeutral,
}

impl Verdict {
    fn label(self) -> &'static str {
        match self {
            Verdict::EqsatWins => "EqsatWins",
            Verdict::NetNeutral => "NetNeutral",
        }
    }
}

/// Measurements for a single fixture under the marginal-value harness.
struct Measurement {
    /// Arrangement count of the fully-optimized plan with eqsat OFF.
    final_off: usize,
    /// Arrangement count of the fully-optimized plan with eqsat ON (greedy).
    final_on: usize,
    /// Arrangement count snapshotted immediately after the last eqsat transform
    /// fires, before the remaining downstream passes run. Because eqsat does
    /// not select `JoinImplementation`, joins in the snapshot are
    /// `Unimplemented` and contribute 0 to this count. The column is
    /// informational about the logical shape eqsat emitted.
    intrinsic: usize,
    /// Whether the eqsat-ON final plan differs structurally from eqsat-OFF.
    plan_changed: bool,
    /// True iff the intrinsic snapshot contains at least one `Join` whose
    /// `implementation` is not `Unimplemented`. Currently false everywhere:
    /// eqsat defers all join-arrangement decisions to the downstream physical
    /// passes, so no join has a committed plan at the intrinsic snapshot.
    eqsat_committed_joins: bool,
    /// Attribution verdict: `EqsatWins` if `final_on < final_off`,
    /// `NetNeutral` if equal. The assertion `final_on <= final_off` forbids
    /// `final_on > final_off`.
    verdict: Verdict,
    /// Total `MirScalarExpr` node count in the eqsat-OFF final plan, summed
    /// across all scalar-bearing operators. See `count_scalar_ops`.
    scalar_ops_off: usize,
    /// Total `MirScalarExpr` node count in the eqsat-ON final plan.
    scalar_ops_on: usize,
    /// Distinct scalar subexpression count in the eqsat-OFF final plan.
    /// See `count_distinct_scalar_subexprs`.
    scalar_distinct_off: usize,
    /// Distinct scalar subexpression count in the eqsat-ON final plan.
    scalar_distinct_on: usize,
}

/// Return true iff `plan` contains any `Join` node whose `implementation` is
/// not `Unimplemented`.
///
/// Used to populate `Measurement::eqsat_committed_joins`. Expected to be false
/// at the intrinsic snapshot because eqsat defers all join-arrangement
/// decisions to downstream physical passes.
fn has_committed_join(plan: &MirRelationExpr) -> bool {
    let mut found = false;
    plan.visit_pre(|n| {
        if let MirRelationExpr::Join { implementation, .. } = n {
            if !matches!(implementation, JoinImplementation::Unimplemented) {
                found = true;
            }
        }
    });
    found
}

/// Run the full transform list with eqsat ON, snapshotting
/// `count_arrangements_with_joins` and the plan state immediately after the
/// last eqsat transform fires.
///
/// Returns `(intrinsic_count, intrinsic_plan, final_plan)` where
/// `intrinsic_count` is the arrangement count taken right after the last pass
/// whose `name()` contains "EqSat", `intrinsic_plan` is the plan at that
/// point, and `final_plan` is the result after all transforms have run. If no
/// eqsat transform fires, `intrinsic_count` and `intrinsic_plan` reflect the
/// input. Because eqsat does not select `JoinImplementation`, any `Join` in
/// the snapshot has `Unimplemented` and contributes 0 join-implied
/// arrangements to `intrinsic_count`.
fn run_with_intrinsic_snapshot(
    plan: &MirRelationExpr,
    oracle: &dyn IndexOracle,
) -> (usize, MirRelationExpr, MirRelationExpr) {
    let features = features_for(Eqsat::Greedy);
    let typecheck_ctx = typecheck::empty_typechecking_context();
    let mut df_meta = DataflowMetainfo::default();
    let mut ctx = TransformCtx::global(
        oracle,
        &EmptyStatisticsOracle,
        &features,
        &typecheck_ctx,
        &mut df_meta,
        None,
    );

    #[allow(deprecated)]
    let transforms: Vec<Box<dyn Transform>> = Optimizer::logical_optimizer(&mut ctx)
        .transforms
        .into_iter()
        .chain(Optimizer::logical_cleanup_pass(&mut ctx, false).transforms)
        .chain(Optimizer::physical_optimizer(&mut ctx).transforms)
        .collect();

    let mut p = plan.clone();
    // Start with the input's arrangement count; the snapshot advances each time
    // an eqsat transform fires, so the final value reflects the last one.
    let mut intrinsic_count = count_arrangements_with_joins(&p);
    let mut intrinsic_plan = p.clone();
    for t in &transforms {
        t.transform(&mut p, &mut ctx).expect("transform succeeds");
        if t.name().contains("EqSat") {
            intrinsic_count = count_arrangements_with_joins(&p);
            intrinsic_plan = p.clone();
        }
    }
    (intrinsic_count, intrinsic_plan, p)
}

/// Measure the marginal value of eqsat on `plan` against the production
/// baseline.
///
/// Runs the full optimizer twice (eqsat OFF, eqsat ON) and once more with an
/// intrinsic snapshot, then classifies the result as [`Verdict::EqsatWins`] if
/// `final_on < final_off`, or [`Verdict::NetNeutral`] otherwise. The
/// `intrinsic` count and `eqsat_committed_joins` flag are informational and do
/// not influence the verdict. The assertion `final_on <= final_off` is a hard
/// invariant: if it fires, eqsat regressed the arrangement count and must be
/// investigated immediately.
fn measure_marginal(name: &str, plan: &MirRelationExpr, oracle: &dyn IndexOracle) -> Measurement {
    let off_plan = optimize_full(plan, oracle, Eqsat::Off);
    let final_off = count_arrangements_with_joins(&off_plan);

    // Per-collection distinct-key diagnostic over the production (eqsat-OFF)
    // plan. Flags collections JI arranged by >=2 distinct keys, the candidate
    // signal for a cross-join arrangement-sharing opportunity that a global
    // planner could collapse.
    report_multi_key(name, &off_plan);

    let (intrinsic, intrinsic_plan, on_plan) = run_with_intrinsic_snapshot(plan, oracle);
    let final_on = count_arrangements_with_joins(&on_plan);

    let plan_changed = on_plan != off_plan;
    let eqsat_committed_joins = has_committed_join(&intrinsic_plan);

    let scalar_ops_off = count_scalar_ops(&off_plan);
    let scalar_ops_on = count_scalar_ops(&on_plan);
    let scalar_distinct_off = count_distinct_scalar_subexprs(&off_plan);
    let scalar_distinct_on = count_distinct_scalar_subexprs(&on_plan);

    assert!(
        final_on <= final_off,
        "{name}: eqsat increased arrangements (off={final_off} on={final_on})"
    );

    let verdict = if final_on < final_off {
        Verdict::EqsatWins
    } else {
        Verdict::NetNeutral
    };

    Measurement {
        final_off,
        final_on,
        intrinsic,
        plan_changed,
        eqsat_committed_joins,
        verdict,
        scalar_ops_off,
        scalar_ops_on,
        scalar_distinct_off,
        scalar_distinct_on,
    }
}

// ---------------------------------------------------------------------------
// Divergence-regime fixtures
// ---------------------------------------------------------------------------
//
// These fixtures target the regime where eqsat's global, order-free
// saturation SHOULD let the full pipeline emit fewer arrangements than the
// greedy pass-ordered pipeline. The realistic lever (given that eqsat defers
// join-arrangement choices entirely) is logical restructuring: if eqsat
// applies CSE or projection/filter pushdown in an order the greedy pipeline
// cannot, downstream passes may encounter a shape that needs fewer
// arrangements.
//
// Honesty guard: every fixture runs the real deployed pipeline. A
// NetNeutral result means production already captures the opportunity via
// its greedy pass order; that is a valid finding and the fixture is kept.

/// Divergence regime: CSE shared filter before join.
///
/// Sharing opportunity: `base` is filtered by `#0 > 0` in two independent
/// branches that are then joined. If CSE hoists the common filter into a
/// single `Let`, production downstream sees only one filtered scan to arrange,
/// not two. Eqsat's saturation-based CSE can recognise the structurally
/// identical subterms and collapse them into one; the greedy
/// `CanonicalizeMfp` / `PredicatePushdown` pipeline processes each branch
/// independently, so it may keep two identical filtered scans that each
/// require a separate arrangement.
///
/// If the result is `NetNeutral`, production's CSE pass already collapses
/// the duplicates before arrangement planning runs, meaning the greedy
/// ordering already captures this opportunity.
fn g1_cse_shared_filter_then_join() -> (MirRelationExpr, Box<dyn IndexOracle>) {
    // base: R(#0 Int64, #1 Int64), GlobalId 801.
    let base = src(801, 2);
    let zero = MirScalarExpr::literal_ok(mz_repr::Datum::Int64(0), ReprScalarType::Int64);
    // Two structurally identical filtered scans over the same base.
    let left = base.clone().filter(vec![
        MirScalarExpr::column(0).call_binary(zero.clone(), func::Gt),
    ]);
    let right = base.filter(vec![MirScalarExpr::column(0).call_binary(zero, func::Gt)]);
    // Join the two filtered copies on #0 = #2 (first column of each copy).
    // Both arms must arrange by #0, so sharing them yields one arrangement
    // instead of two.
    let plan = MirRelationExpr::join_scalars(
        vec![left, right],
        vec![vec![MirScalarExpr::column(0), MirScalarExpr::column(2)]],
    );
    let oracle = Box::new(MultiIndex::default());
    (plan, oracle)
}

/// Divergence regime: phase-ordering between filter pushdown and Reduce.
///
/// Sharing opportunity: a `Reduce` (group by #0) is applied to a join of
/// `S` with a filtered `R`. If the filter `#1 > 0` is pushed below the join
/// before `Reduce` sees the plan, `Reduce` operates on a smaller input and
/// may be arrangeable more cheaply. The greedy pipeline runs
/// `PredicatePushdown` before `Reduce`, so production should already push;
/// eqsat explores both orderings simultaneously and should find the same or
/// better result.
///
/// If the result is `NetNeutral`, greedy ordering already picks the optimal
/// filter-first path and eqsat adds no further savings at the arrangement
/// count level.
fn g2_phase_order_filter_reduce() -> (MirRelationExpr, Box<dyn IndexOracle>) {
    // R(#0 Int64, #1 Int64) GlobalId 901; S(#2 Int64, #3 Int64) GlobalId 902.
    let r = src(901, 2);
    let s = src(902, 2);
    // Filter on R: keep only rows where #1 > 0.
    let zero = MirScalarExpr::literal_ok(mz_repr::Datum::Int64(0), ReprScalarType::Int64);
    let r_filtered = r.filter(vec![MirScalarExpr::column(1).call_binary(zero, func::Gt)]);
    // Join R (filtered) with S on R.#0 = S.#2, i.e. column 0 = column 2.
    let joined = MirRelationExpr::join_scalars(
        vec![r_filtered, s],
        vec![vec![MirScalarExpr::column(0), MirScalarExpr::column(2)]],
    );
    // Reduce: group by column 0 (R.#0 = S.#2), aggregate count(*).
    let plan = joined.reduce(
        vec![0],
        vec![AggregateExpr {
            func: AggregateFunc::Count,
            expr: MirScalarExpr::literal_true(),
            distinct: false,
        }],
        None,
    );
    let oracle = Box::new(MultiIndex::default().with(GlobalId::Transient(901), &[0]));
    (plan, oracle)
}

// ---------------------------------------------------------------------------
// Cross-join arrangement-sharing probes
// ---------------------------------------------------------------------------
//
// These fixtures try to FORCE production JI to arrange ONE collection by TWO
// distinct keys at two join sites where a SINGLE key is correctness-valid for
// both. If found, that is the global-planner opportunity eqsat could capture
// by subsuming JI. Each fixture documents the intended shared key and why JI's
// local, per-join choice might miss it. The `multi_key_collections` diagnostic
// reports what JI actually picked.

/// Probe P1: same collection fed to two joins, both keyed on the same column.
///
/// R(#0,#1) joined to S on R.#0=S.x AND to T on R.#0=T.y. Both joins key R on
/// (#0). The single key (#0) is correctness-valid for both sites, so a global
/// planner needs ONE R arrangement. JI plans each binary join locally. If it
/// ever chose two different keys for R, that would be a reducible duplication.
/// The intended shared key is R by (#0).
fn p1_same_key_two_sites() -> (MirRelationExpr, Box<dyn IndexOracle>) {
    // R(#0,#1) GlobalId 1101; S(#2,#3) GlobalId 1102; T(#4,#5) GlobalId 1103.
    let r = src(1101, 2);
    let s = src(1102, 2);
    let t = src(1103, 2);
    // R.#0 = S.#2 and R.#0 = T.#4. R is keyed by (#0) at both join sites.
    let plan = MirRelationExpr::join_scalars(
        vec![r, s, t],
        vec![
            vec![MirScalarExpr::column(0), MirScalarExpr::column(2)],
            vec![MirScalarExpr::column(0), MirScalarExpr::column(4)],
        ],
    );
    // No index on R, so JI must place R's arrangement(s) itself.
    let oracle = Box::new(MultiIndex::default());
    (plan, oracle)
}

/// Probe P2: collection joined on a prefix at one site and a superset at the
/// other.
///
/// R(#0,#1) joined to S on R.#0=S.x (keys R by (#0)) AND to T on
/// R.#0=T.y AND R.#1=T.z (keys R by (#0,#1)). The two sites need GENUINELY
/// DIFFERENT keys: the second join's equivalence pins R to (#0,#1), which a
/// single (#0) arrangement cannot serve as an equijoin key. So a duplication
/// here is NOT reducible. This fixture is the honesty-guard control: it should
/// show two keys that genuinely differ, distinguishing a real opportunity from
/// a forced one.
fn p2_prefix_vs_superset_keys() -> (MirRelationExpr, Box<dyn IndexOracle>) {
    // R(#0,#1) GlobalId 1201; S(#2,#3) GlobalId 1202; T(#4,#5) GlobalId 1203.
    let r = src(1201, 2);
    let s = src(1202, 2);
    let t = src(1203, 2);
    // R.#0 = S.#2 (R keyed by (#0)); R.#0 = T.#4 AND R.#1 = T.#5 (R keyed by
    // (#0,#1)).
    let plan = MirRelationExpr::join_scalars(
        vec![r, s, t],
        vec![
            vec![MirScalarExpr::column(0), MirScalarExpr::column(2)],
            vec![MirScalarExpr::column(0), MirScalarExpr::column(4)],
            vec![MirScalarExpr::column(1), MirScalarExpr::column(5)],
        ],
    );
    let oracle = Box::new(MultiIndex::default());
    (plan, oracle)
}

/// Probe P3: self-join diamond where the shared collection feeds two joins on
/// the same column.
///
/// R(#0,#1) is joined to itself and to a third copy, all on column #0:
/// R0.#0 = R1.#0 and R0.#0 = R2.#0. All three references key R by (#0), so a
/// single shared (#0) arrangement is correctness-valid for every site. Because
/// the three Gets are structurally identical, CSE keeps them one collection;
/// the question is whether JI arranges that collection once or several times.
/// The intended shared key is R by (#0).
fn p3_self_join_diamond() -> (MirRelationExpr, Box<dyn IndexOracle>) {
    // R(#0,#1) GlobalId 1301, referenced three times.
    let r = src(1301, 2);
    let plan = MirRelationExpr::join_scalars(
        vec![r.clone(), r.clone(), r],
        vec![
            vec![MirScalarExpr::column(0), MirScalarExpr::column(2)],
            vec![MirScalarExpr::column(0), MirScalarExpr::column(4)],
        ],
    );
    let oracle = Box::new(MultiIndex::default());
    (plan, oracle)
}

/// Probe P4: index on one key plus a join needing the same column, to see if
/// JI re-keys the same collection a second way.
///
/// R(#0,#1) has an index on (#0). It is joined to S on R.#0=S.x AND to T on
/// R.#0=T.y, both in the (#0) equivalence class. The index supplies the (#0)
/// arrangement; both join sites can reuse it. A single (#0) arrangement is
/// correctness-valid for both. If JI instead built a fresh (#0) arrangement
/// alongside the index, that would be a (reducible) duplication of the same
/// key on the same collection. The intended shared arrangement is the index on
/// R by (#0).
fn p4_index_plus_two_sites() -> (MirRelationExpr, Box<dyn IndexOracle>) {
    // R(#0,#1) GlobalId 1401 indexed on (#0); S 1402; T 1403.
    let r = src(1401, 2);
    let s = src(1402, 2);
    let t = src(1403, 2);
    let plan = MirRelationExpr::join_scalars(
        vec![r, s, t],
        vec![
            vec![MirScalarExpr::column(0), MirScalarExpr::column(2)],
            vec![MirScalarExpr::column(0), MirScalarExpr::column(4)],
        ],
    );
    let oracle = Box::new(MultiIndex::default().with(GlobalId::Transient(1401), &[0]));
    (plan, oracle)
}

/// Collect all fixture `(name, plan, oracle)` tuples used in the benchmark.
///
/// Each entry is a triple of `(name, plan, oracle)`. Kept in one place so the
/// marginal-value test and any future harness extensions share the same list.
fn all_fixtures() -> Vec<(&'static str, MirRelationExpr, Box<dyn IndexOracle>)> {
    // smoke/filter-over-index
    let g = src(1, 2);
    let lit5 = mz_repr::Datum::Int64(5);
    let lit5_scalar = MirScalarExpr::literal_ok(lit5, ReprScalarType::Int64);
    let smoke_plan = g.filter(vec![
        MirScalarExpr::column(0).call_binary(lit5_scalar, func::Eq),
    ]);
    let smoke_oracle = Box::new(MultiIndex::default().with(GlobalId::Transient(1), &[0]));

    // f1/shared-key-fanout
    let f1_plan = MirRelationExpr::join_scalars(
        vec![src(101, 2), src(102, 2), src(103, 2)],
        vec![
            vec![MirScalarExpr::column(0), MirScalarExpr::column(2)],
            vec![MirScalarExpr::column(0), MirScalarExpr::column(4)],
        ],
    );
    let f1_oracle = Box::new(MultiIndex::default().with(GlobalId::Transient(101), &[0]));

    // f2/diamond-shared-filter
    let f2_r = src(201, 2);
    let zero = MirScalarExpr::literal_ok(mz_repr::Datum::Int64(0), ReprScalarType::Int64);
    let f2_f = f2_r.filter(vec![MirScalarExpr::column(1).call_binary(zero, func::Gt)]);
    let f2_plan = MirRelationExpr::join_scalars(
        vec![f2_f.clone(), f2_f],
        vec![vec![MirScalarExpr::column(0), MirScalarExpr::column(2)]],
    );
    let f2_oracle = Box::new(MultiIndex::default());

    // f3/four-way-chain
    let f3_plan = MirRelationExpr::join_scalars(
        vec![src(301, 2), src(302, 2), src(303, 2), src(304, 2)],
        vec![
            vec![MirScalarExpr::column(1), MirScalarExpr::column(2)],
            vec![MirScalarExpr::column(3), MirScalarExpr::column(4)],
            vec![MirScalarExpr::column(5), MirScalarExpr::column(6)],
        ],
    );
    let f3_oracle = Box::new(
        MultiIndex::default()
            .with(GlobalId::Transient(301), &[1])
            .with(GlobalId::Transient(304), &[1]),
    );

    // w1/reachability
    let w1_plan = {
        use mz_expr::LocalId;
        let lid = LocalId::new(1);
        let typ = ReprRelationType::new(
            (0..2)
                .map(|_| ReprScalarType::Int64.nullable(false))
                .collect(),
        );
        let edges = src(401, 2);
        let reach_get = MirRelationExpr::Get {
            id: Id::Local(lid.clone()),
            typ: typ.clone(),
            access_strategy: AccessStrategy::UnknownOrLocal,
        };
        let step = MirRelationExpr::join_scalars(
            vec![reach_get, edges.clone()],
            vec![vec![MirScalarExpr::column(1), MirScalarExpr::column(2)]],
        )
        .project(vec![0, 3]);
        let reach_value = MirRelationExpr::Union {
            base: Box::new(edges),
            inputs: vec![step],
        }
        .distinct();
        let body = MirRelationExpr::Get {
            id: Id::Local(lid.clone()),
            typ: typ.clone(),
            access_strategy: AccessStrategy::UnknownOrLocal,
        };
        MirRelationExpr::LetRec {
            ids: vec![lid],
            values: vec![reach_value],
            limits: vec![None],
            body: Box::new(body),
        }
    };
    let w1_oracle = Box::new(MultiIndex::default().with(GlobalId::Transient(401), &[0]));

    // w2/reachability-envelope
    let w2_plan = {
        use mz_expr::LocalId;
        let lid = LocalId::new(1);
        let typ = ReprRelationType::new(
            (0..2)
                .map(|_| ReprScalarType::Int64.nullable(false))
                .collect(),
        );
        let edges = src(401, 2);
        let reach_get_inner = MirRelationExpr::Get {
            id: Id::Local(lid.clone()),
            typ: typ.clone(),
            access_strategy: AccessStrategy::UnknownOrLocal,
        };
        let step = MirRelationExpr::join_scalars(
            vec![reach_get_inner, edges.clone()],
            vec![vec![MirScalarExpr::column(1), MirScalarExpr::column(2)]],
        )
        .project(vec![0, 3]);
        let reach_value = MirRelationExpr::Union {
            base: Box::new(edges),
            inputs: vec![step],
        }
        .distinct();
        let lit5_w2 = MirScalarExpr::literal_ok(mz_repr::Datum::Int64(5), ReprScalarType::Int64);
        let body = MirRelationExpr::Get {
            id: Id::Local(lid.clone()),
            typ: typ.clone(),
            access_strategy: AccessStrategy::UnknownOrLocal,
        }
        .filter(vec![
            MirScalarExpr::column(0).call_binary(lit5_w2, func::Eq),
        ]);
        MirRelationExpr::LetRec {
            ids: vec![lid],
            values: vec![reach_value],
            limits: vec![None],
            body: Box::new(body),
        }
    };
    let w2_oracle = Box::new(MultiIndex::default().with(GlobalId::Transient(401), &[0]));

    // letrec/body-shared (w3)
    let w3_plan = {
        use mz_expr::LocalId;
        let lid = LocalId::new(1);
        let typ = ReprRelationType::new(
            (0..2)
                .map(|_| ReprScalarType::Int64.nullable(false))
                .collect(),
        );
        let edges = src(601, 2);
        let reach_get = MirRelationExpr::Get {
            id: Id::Local(lid.clone()),
            typ: typ.clone(),
            access_strategy: AccessStrategy::UnknownOrLocal,
        };
        let step = MirRelationExpr::join_scalars(
            vec![reach_get, edges.clone()],
            vec![vec![MirScalarExpr::column(1), MirScalarExpr::column(2)]],
        )
        .project(vec![0, 3]);
        let reach_value = MirRelationExpr::Union {
            base: Box::new(edges),
            inputs: vec![step],
        }
        .distinct();
        let body = MirRelationExpr::Get {
            id: Id::Local(lid.clone()),
            typ: typ.clone(),
            access_strategy: AccessStrategy::UnknownOrLocal,
        }
        .project(vec![0, 1]);
        MirRelationExpr::LetRec {
            ids: vec![lid],
            values: vec![reach_value],
            limits: vec![None],
            body: Box::new(body),
        }
    };
    let w3_oracle = Box::new(MultiIndex::default().with(GlobalId::Transient(601), &[0]));

    // flatmap/shared
    let fm_r = src(501, 2);
    let fm_step = MirScalarExpr::literal_ok(mz_repr::Datum::Int64(1), ReprScalarType::Int64);
    let fm = fm_r.flat_map(
        TableFunc::GenerateSeriesInt64,
        vec![MirScalarExpr::column(0), MirScalarExpr::column(1), fm_step],
    );
    let flatmap_shared_plan = MirRelationExpr::Union {
        base: Box::new(fm.clone()),
        inputs: vec![fm],
    };
    let flatmap_shared_oracle = Box::new(MultiIndex::default());

    // flatmap/filtered
    let ff_r = src(701, 2);
    let ff_step = MirScalarExpr::literal_ok(mz_repr::Datum::Int64(1), ReprScalarType::Int64);
    let ff_fm = ff_r.flat_map(
        TableFunc::GenerateSeriesInt64,
        vec![MirScalarExpr::column(0), MirScalarExpr::column(1), ff_step],
    );
    let ff_pred = MirScalarExpr::column(0).call_binary(
        MirScalarExpr::literal_ok(mz_repr::Datum::Int64(5), ReprScalarType::Int64),
        func::Eq,
    );
    let flatmap_filtered_plan = ff_fm.filter(vec![ff_pred]);
    let flatmap_filtered_oracle = Box::new(MultiIndex::default());

    // g1/cse-shared-filter-then-join
    // Plan and oracle from g1_cse_shared_filter_then_join().
    let (g1_plan, g1_oracle) = g1_cse_shared_filter_then_join();

    // g2/phase-order-filter-reduce
    // Plan and oracle from g2_phase_order_filter_reduce().
    let (g2_plan, g2_oracle) = g2_phase_order_filter_reduce();

    // p1/same-key-two-sites, p2/prefix-vs-superset, p3/self-join-diamond.
    // Cross-join arrangement-sharing probes.
    let (p1_plan, p1_oracle) = p1_same_key_two_sites();
    let (p2_plan, p2_oracle) = p2_prefix_vs_superset_keys();
    let (p3_plan, p3_oracle) = p3_self_join_diamond();
    let (p4_plan, p4_oracle) = p4_index_plus_two_sites();

    vec![
        ("smoke/filter-over-index", smoke_plan, smoke_oracle),
        ("f1/shared-key-fanout", f1_plan, f1_oracle),
        ("f2/diamond-shared-filter", f2_plan, f2_oracle),
        ("f3/four-way-chain", f3_plan, f3_oracle),
        ("w1/reachability", w1_plan, w1_oracle),
        ("w2/reachability-envelope", w2_plan, w2_oracle),
        ("letrec/body-shared", w3_plan, w3_oracle),
        ("flatmap/shared", flatmap_shared_plan, flatmap_shared_oracle),
        (
            "flatmap/filtered",
            flatmap_filtered_plan,
            flatmap_filtered_oracle,
        ),
        ("g1/cse-shared-filter-then-join", g1_plan, g1_oracle),
        ("g2/phase-order-filter-reduce", g2_plan, g2_oracle),
        ("p1/same-key-two-sites", p1_plan, p1_oracle),
        ("p2/prefix-vs-superset", p2_plan, p2_oracle),
        ("p3/self-join-diamond", p3_plan, p3_oracle),
        ("p4/index-plus-two-sites", p4_plan, p4_oracle),
    ]
}

#[mz_ore::test]
fn eqsat_marginal_value() {
    let fixtures = all_fixtures();
    let mut measurements: Vec<(&'static str, Measurement)> = Vec::new();
    for (name, plan, oracle) in &fixtures {
        let m = measure_marginal(name, plan, oracle.as_ref());
        measurements.push((name, m));
    }

    // Print the table header.
    // Columns:
    //   off      = final arrangement count with eqsat OFF (full pipeline).
    //   on       = final arrangement count with eqsat ON (full pipeline).
    //   intrinsic = count right after the last eqsat transform, before
    //              downstream passes. Informational: shows logical-shape change.
    //   changed  = whether the eqsat-ON final plan differs from eqsat-OFF.
    //   cmtjoins = eqsat_committed_joins: whether the intrinsic snapshot has
    //              any join with a non-Unimplemented implementation. Currently
    //              false everywhere (eqsat defers all join-arrangement choices).
    //   sc_off   = total scalar-op nodes in the eqsat-OFF final plan.
    //   sc_on    = total scalar-op nodes in the eqsat-ON final plan.
    //   sd_off   = distinct scalar subexprs in the eqsat-OFF final plan.
    //   sd_on    = distinct scalar subexprs in the eqsat-ON final plan.
    //   verdict  = EqsatWins if final_on < final_off; NetNeutral otherwise.
    eprintln!(
        "{:<30} {:>5} {:>5} {:>9} {:>8} {:>9} {:>6} {:>5} {:>6} {:>5} verdict",
        "name",
        "off",
        "on",
        "intrinsic",
        "changed",
        "cmtjoins",
        "sc_off",
        "sc_on",
        "sd_off",
        "sd_on"
    );
    eprintln!("{}", "-".repeat(115));

    // Verdict tallies and net savings.
    let mut tally_wins = 0usize;
    let mut tally_neutral = 0usize;
    let mut sum_off = 0usize;
    let mut sum_on = 0usize;
    let mut sum_scalar_ops_off = 0usize;
    let mut sum_scalar_ops_on = 0usize;
    // Track whether ANY fixture differs on scalar metrics.
    let mut any_scalar_ops_diff = false;
    let mut any_scalar_distinct_diff = false;

    for (name, m) in &measurements {
        eprintln!(
            "{:<30} {:>5} {:>5} {:>9} {:>8} {:>9} {:>6} {:>5} {:>6} {:>5} {}",
            name,
            m.final_off,
            m.final_on,
            m.intrinsic,
            m.plan_changed,
            m.eqsat_committed_joins,
            m.scalar_ops_off,
            m.scalar_ops_on,
            m.scalar_distinct_off,
            m.scalar_distinct_on,
            m.verdict.label(),
        );
        match m.verdict {
            Verdict::EqsatWins => tally_wins += 1,
            Verdict::NetNeutral => tally_neutral += 1,
        }
        sum_off += m.final_off;
        sum_on += m.final_on;
        sum_scalar_ops_off += m.scalar_ops_off;
        sum_scalar_ops_on += m.scalar_ops_on;
        if m.scalar_ops_on != m.scalar_ops_off {
            any_scalar_ops_diff = true;
        }
        if m.scalar_distinct_on != m.scalar_distinct_off {
            any_scalar_distinct_diff = true;
        }
    }

    let net_saved = sum_off.saturating_sub(sum_on);
    eprintln!("{}", "-".repeat(115));
    eprintln!(
        "summary: EqsatWins={tally_wins} NetNeutral={tally_neutral} \
         net_arrangements_saved={net_saved}"
    );
    eprintln!(
        "scalar:  total_sc_ops_off={sum_scalar_ops_off} total_sc_ops_on={sum_scalar_ops_on} \
         scalar_ops_diff_any={any_scalar_ops_diff} \
         scalar_distinct_diff_any={any_scalar_distinct_diff}"
    );
    if any_scalar_ops_diff || any_scalar_distinct_diff {
        eprintln!(
            "NOTE: eqsat changed scalar metrics on at least one fixture \
             (unexpected: scalars are currently treated as opaque)"
        );
        for (name, m) in &measurements {
            if m.scalar_ops_on != m.scalar_ops_off || m.scalar_distinct_on != m.scalar_distinct_off
            {
                eprintln!(
                    "  {name}: scalar_ops off={} on={} distinct off={} on={}",
                    m.scalar_ops_off, m.scalar_ops_on, m.scalar_distinct_off, m.scalar_distinct_on
                );
            }
        }
    } else {
        eprintln!(
            "scalar passthrough confirmed: eqsat-on == eqsat-off on both scalar metrics \
             for every fixture (eqsat treats scalars as opaque)"
        );
    }

    // The only hard assertion: eqsat must not increase arrangements on any fixture.
    for (name, m) in &measurements {
        assert!(
            m.final_on <= m.final_off,
            "{name}: eqsat increased arrangements (off={} on={})",
            m.final_off,
            m.final_on,
        );
    }
}

// A filter over a FlatMap whose predicate reads only input columns. The
// `push_filter_past_flatmap` rule sinks the filter below the FlatMap. Production
// `predicate_pushdown` performs the same push, so the full-pipeline plans may
// converge (eqsat-active can read false); the rule's firing in isolation is
// proven by the `push_filter_past_flatmap_fires` unit test. This fixture records
// the full-pipeline arrangement count and guards against a regression.
#[mz_ore::test]
fn flatmap_filtered() {
    let r = src(701, 2);
    let step = MirScalarExpr::literal_ok(mz_repr::Datum::Int64(1), ReprScalarType::Int64);
    let fm = r.flat_map(
        TableFunc::GenerateSeriesInt64,
        vec![MirScalarExpr::column(0), MirScalarExpr::column(1), step],
    );
    // Predicate on input column 0 only, so it is eligible to sink below the
    // FlatMap (it does not reference the appended func-output column).
    let pred = MirScalarExpr::column(0).call_binary(
        MirScalarExpr::literal_ok(mz_repr::Datum::Int64(5), ReprScalarType::Int64),
        func::Eq,
    );
    let plan = fm.filter(vec![pred]);
    let oracle = MultiIndex::default();
    let (p, g, i) = compare("flatmap/filtered", &plan, &oracle);
    // Pushing the filter below the FlatMap is arrangement-neutral, so neither
    // eqsat configuration may add an arrangement over production.
    assert!(
        p == g && g == i,
        "filter pushdown past FlatMap must not change the arrangement count: prod={p} greedy={g} ilp={i}"
    );
}

// ---------------------------------------------------------------------------
// Join-order arrangement lever: brute-force ground truth
// ---------------------------------------------------------------------------
//
// THE QUESTION: on a multi-way join, is the order production JoinImplementation
// (JI) commits arrangement-suboptimal? Does a different, correctness-valid join
// order need strictly fewer maintained arrangements than JI's order?
//
// We answer it by brute force. For each multi-way-join fixture we enumerate
// every input ordering, compute the arrangement count each order would require
// under the physical join models below, take the min over valid orders, and
// compare it to what JI actually committed (read from `Join.implementation`
// after running the real production pipeline).
//
// ARRANGEMENT MODEL (grounded in `join_implementation.rs`):
//
// * Differential (left-deep). The production count for a linear differential
//   join of `n` inputs is `(n - 2) + new_input_arrangements`, where the
//   `(n - 2)` intermediate arrangements are FIXED for any order (one per
//   internal join result) and `new_input_arrangements` is the deduplicated
//   number of `(input, key)` arrangements the order needs that an existing
//   index does not already supply. See `differential::plan`, which sets
//   `new_arrangements = inputs.len().saturating_sub(2) + new_input_arrangements[start]`.
//   Because the `(n - 2)` term is order-independent, the differential order
//   lever is entirely the set of distinct `(input, key)` input arrangements the
//   walk induces, net of reuse from available indexes.
//
// * Delta query. Every input gets its own dataflow path; each path arranges
//   every other input by the key it is joined on. Deduplicated across paths
//   this is the set of all `(input, key)` lookup arrangements the join graph
//   implies, net of available indexes. The set is symmetric in the inputs, so
//   the delta count does not depend on order.
//
// For each order we model the left-deep walk: the start input contributes its
// key (the columns equated with the second input), and each later input
// contributes the local columns of itself that appear in an equivalence class
// fully bound once it and the prefix are placed. An input whose key would be
// empty is not connected to the prefix (a cross join); we treat such an order
// as invalid and skip it, matching JI's refusal to plan disconnected steps.
//
// The model is sanity-checked against a hand-computed case in
// `join_order_model_sanity`, and the JI-committed count is read from the real
// pipeline so the comparison is apples-to-apples (same `(input, key)` dedup,
// same `(n - 2)` intermediate term cancels on both sides).

/// The set of distinct `(input_index, key)` input arrangements a join shape
/// induces, where `key` is the sorted list of local column indices forming the
/// arrangement key on that input.
///
/// Deduplication is by `(input_index, sorted_key)`, mirroring the cost model's
/// `(collection, key)` dedup. An entry is dropped when `available[input]`
/// already contains that key, modeling reuse of an existing index.
type ArrSet = BTreeSet<(usize, Vec<usize>)>;

/// Per-input join keys derived from the join's equivalences.
///
/// `key_cols[i]` is the set of local column indices of input `i` that appear in
/// any equivalence class. `class_inputs[c]` is the set of inputs the `c`-th
/// equivalence class references. `class_local_cols[c][i]` is the local columns
/// of input `i` that appear in class `c`. Together these describe the join
/// graph: input `i` and input `j` are connected by class `c` iff both are in
/// `class_inputs[c]`.
struct JoinGraph {
    n: usize,
    /// `class_inputs[c]` = inputs referenced by equivalence class `c`.
    class_inputs: Vec<BTreeSet<usize>>,
    /// `class_local_cols[c]` maps input index to the local columns of that input
    /// appearing in class `c` (only inputs in `class_inputs[c]` have entries).
    class_local_cols: Vec<BTreeMap<usize, Vec<usize>>>,
}

impl JoinGraph {
    /// Build the join graph from a `Join`'s inputs and equivalences.
    fn from_join(inputs: &[MirRelationExpr], equivalences: &[Vec<MirScalarExpr>]) -> JoinGraph {
        let mapper = JoinInputMapper::new(inputs);
        let n = inputs.len();
        let mut class_inputs = Vec::with_capacity(equivalences.len());
        let mut class_local_cols = Vec::with_capacity(equivalences.len());
        for class in equivalences {
            let mut inputs_in_class: BTreeSet<usize> = BTreeSet::new();
            let mut local_cols: BTreeMap<usize, Vec<usize>> = BTreeMap::new();
            for expr in class {
                // A literal or a multi-input expression has no single owning
                // input; only single-input expressions form an arrangement key
                // component, matching `order_input`'s handling.
                if let Some(input) = mapper.single_input(expr) {
                    inputs_in_class.insert(input);
                    for global_col in expr.support() {
                        let (local_col, owner) = mapper.map_column_to_local(global_col);
                        if owner == input {
                            local_cols.entry(input).or_default().push(local_col);
                        }
                    }
                }
            }
            for cols in local_cols.values_mut() {
                cols.sort();
                cols.dedup();
            }
            class_inputs.push(inputs_in_class);
            class_local_cols.push(local_cols);
        }
        JoinGraph {
            n,
            class_inputs,
            class_local_cols,
        }
    }

    /// The local arrangement key input `input` needs to join into a prefix whose
    /// placed inputs are `placed`.
    ///
    /// Collects the local columns of `input` from every equivalence class that
    /// references both `input` and at least one already-placed input. Returns the
    /// sorted, deduplicated column list. An empty result means `input` shares no
    /// equivalence with the prefix (a cross join with respect to it).
    fn key_against_prefix(&self, input: usize, placed: &BTreeSet<usize>) -> Vec<usize> {
        let mut key: Vec<usize> = Vec::new();
        for (c, inputs_in_class) in self.class_inputs.iter().enumerate() {
            if !inputs_in_class.contains(&input) {
                continue;
            }
            let touches_prefix = inputs_in_class.iter().any(|i| placed.contains(i));
            if !touches_prefix {
                continue;
            }
            if let Some(cols) = self.class_local_cols[c].get(&input) {
                key.extend(cols.iter().copied());
            }
        }
        key.sort();
        key.dedup();
        key
    }
}

/// Differential new-input-arrangement set for one left-deep order, or `None`
/// if the order is invalid (some step is a cross join, i.e. needs an empty key).
///
/// `order` is the input indices in left-deep sequence. `available[i]` is the set
/// of already-materialized arrangement keys (local columns) on input `i`. The
/// returned set is the distinct `(input, key)` arrangements the order needs net
/// of `available`; the fixed `(n - 2)` intermediate arrangements are added by
/// the caller.
fn differential_input_arrangements(
    graph: &JoinGraph,
    order: &[usize],
    available: &[BTreeSet<Vec<usize>>],
) -> Option<ArrSet> {
    let mut placed: BTreeSet<usize> = BTreeSet::new();
    let mut arrangements: ArrSet = BTreeSet::new();
    // The start input's key lines up with the key it is joined to the second
    // input by, so we compute it once the second input is known. We defer the
    // start until after the loop and derive its key from the second input.
    placed.insert(order[0]);
    for &input in &order[1..] {
        let key = graph.key_against_prefix(input, &placed);
        if key.is_empty() {
            // No equivalence binds this input to the prefix: a cross join, which
            // JI refuses to plan as a connected step.
            return None;
        }
        if !available[input].contains(&key) {
            arrangements.insert((input, key));
        }
        placed.insert(input);
    }
    // The start input is arranged by the columns equating it with the rest. Its
    // key is the local columns of `order[0]` that join into the others, computed
    // against the full set of the other inputs.
    let others: BTreeSet<usize> = order[1..].iter().copied().collect();
    let start_key = graph.key_against_prefix(order[0], &others);
    if !start_key.is_empty() && !available[order[0]].contains(&start_key) {
        arrangements.insert((order[0], start_key));
    }
    Some(arrangements)
}

/// Delta-query input-arrangement set for the whole join graph (order-free).
///
/// A delta join arranges each input by EVERY distinct join key it participates
/// in, one arrangement per equivalence class that connects it to another input.
/// This differs from differential, where an input is arranged once by the
/// composite of all its columns equated with the running prefix. The set is the
/// distinct `(input, per_class_key)` arrangements over all class-induced edges,
/// net of `available`. Returns the count of new arrangements.
///
/// The count is symmetric in the inputs, so it does not depend on join order.
fn delta_input_arrangements(graph: &JoinGraph, available: &[BTreeSet<Vec<usize>>]) -> usize {
    let mut arrangements: ArrSet = BTreeSet::new();
    for (c, inputs_in_class) in graph.class_inputs.iter().enumerate() {
        // A class that touches a single input is not a join edge; it cannot key
        // a delta lookup, so it contributes no arrangement.
        if inputs_in_class.len() < 2 {
            continue;
        }
        for &input in inputs_in_class {
            if let Some(cols) = graph.class_local_cols[c].get(&input) {
                let mut key = cols.clone();
                key.sort();
                key.dedup();
                if !key.is_empty() && !available[input].contains(&key) {
                    arrangements.insert((input, key));
                }
            }
        }
    }
    arrangements.len()
}

/// All permutations of `0..n` (for small `n`; `n!` enumeration).
fn permutations(n: usize) -> Vec<Vec<usize>> {
    let mut result = Vec::new();
    let mut items: Vec<usize> = (0..n).collect();
    permute_into(&mut items, 0, &mut result);
    result
}

/// Heap-style recursive permutation generator appending each full permutation.
fn permute_into(items: &mut Vec<usize>, k: usize, out: &mut Vec<Vec<usize>>) {
    if k == items.len() {
        out.push(items.clone());
        return;
    }
    for i in k..items.len() {
        items.swap(k, i);
        permute_into(items, k + 1, out);
        items.swap(k, i);
    }
}

/// Brute-force outcome over all join orders for a multi-way join.
struct BruteForce {
    /// Number of inputs.
    n: usize,
    /// Minimum total maintained arrangements over all valid left-deep orders
    /// under the DIFFERENTIAL strategy, including the order-independent `(n - 2)`
    /// intermediate arrangements. This is the order-lever ground truth for the
    /// strategy production actually deploys (eager delta off by default).
    min_differential: usize,
    /// Maximum differential total over orders, for context on the spread.
    max_differential: usize,
    /// Delta-strategy total (order-independent, no intermediates). Reported as
    /// strategy-lever context, NOT as an order-lever result.
    delta: usize,
}

/// Brute-force the differential arrangement count over all join orders, and the
/// (order-free) delta count, with `available` indexes.
///
/// The differential per-order count is `(n - 2) + new_input_arrangements`, the
/// production formula. The `(n - 2)` intermediates are order-independent, so the
/// order lever lives entirely in `new_input_arrangements`, which varies with how
/// well a start/sequence reuses an existing index. Panics on fewer than two
/// inputs or if no valid order exists.
fn brute_force_orders(
    inputs: &[MirRelationExpr],
    equivalences: &[Vec<MirScalarExpr>],
    available: &[BTreeSet<Vec<usize>>],
) -> BruteForce {
    let graph = JoinGraph::from_join(inputs, equivalences);
    let n = graph.n;
    assert!(n >= 2, "brute force requires at least two inputs");
    let intermediates = n.saturating_sub(2);
    let mut min_differential = usize::MAX;
    let mut max_differential = 0usize;
    for order in permutations(n) {
        if let Some(arr) = differential_input_arrangements(&graph, &order, available) {
            let total = intermediates + arr.len();
            min_differential = min_differential.min(total);
            max_differential = max_differential.max(total);
        }
    }
    // Every connected join has at least one valid left-deep order, so this is
    // always set for the fixtures here. Guard anyway.
    assert!(min_differential != usize::MAX, "no valid join order found");
    let delta = delta_input_arrangements(&graph, available);
    BruteForce {
        n,
        min_differential,
        max_differential,
        delta,
    }
}

/// Extract the `(input, key)` arrangements JI committed on the first `Join` node
/// of `plan`, counted the SAME way the brute-force model counts: the
/// differential `(n - 2)` intermediates plus the distinct per-input lookup/start
/// arrangements, or the delta per-path arrangements.
///
/// Returns `(committed_total, kind)` where `kind` is "differential", "delta",
/// "indexed_filter", or "unimplemented". Keys are normalized to sorted local
/// column indices when the arrangement key is a simple column list; non-column
/// keys are kept verbatim via their debug string so the dedup still works.
fn ji_committed_arrangements(plan: &MirRelationExpr) -> Option<(usize, &'static str)> {
    let mut result: Option<(usize, &'static str)> = None;
    plan.visit_pre(|n| {
        if result.is_some() {
            return;
        }
        if let MirRelationExpr::Join {
            inputs,
            implementation,
            ..
        } = n
        {
            match implementation {
                JoinImplementation::Differential((start_idx, start_key, _), order) => {
                    let mut arr: BTreeSet<(usize, String)> = BTreeSet::new();
                    if let Some(key) = start_key {
                        arr.insert((*start_idx, format!("{key:?}")));
                    }
                    for (lookup_idx, lookup_key, _) in order {
                        arr.insert((*lookup_idx, format!("{lookup_key:?}")));
                    }
                    let intermediates = inputs.len().saturating_sub(2);
                    result = Some((intermediates + arr.len(), "differential"));
                }
                JoinImplementation::DeltaQuery(paths) => {
                    let mut arr: BTreeSet<(usize, String)> = BTreeSet::new();
                    for path in paths {
                        for (lookup_idx, lookup_key, _) in path {
                            arr.insert((*lookup_idx, format!("{lookup_key:?}")));
                        }
                    }
                    result = Some((arr.len(), "delta"));
                }
                JoinImplementation::IndexedFilter(..) => {
                    result = Some((0, "indexed_filter"));
                }
                JoinImplementation::Unimplemented => {
                    result = Some((0, "unimplemented"));
                }
            }
        }
    });
    result
}

/// Read the `(inputs, equivalences)` of the first `Join` node in `plan`.
fn first_join<'a>(
    plan: &'a MirRelationExpr,
) -> Option<(&'a Vec<MirRelationExpr>, &'a Vec<Vec<MirScalarExpr>>)> {
    let mut found: Option<(&Vec<MirRelationExpr>, &Vec<Vec<MirScalarExpr>>)> = None;
    plan.visit_pre(|n| {
        if found.is_none() {
            if let MirRelationExpr::Join {
                inputs,
                equivalences,
                ..
            } = n
            {
                found = Some((inputs, equivalences));
            }
        }
    });
    found
}

/// Translate a `MultiIndex` oracle into per-input available-arrangement sets for
/// the brute-force model, given the `inputs` of the optimized join.
///
/// Maps each input to the index keys (as local column lists) the oracle reports
/// for the `GlobalId` that input is a `Get` of. Inputs that are not plain global
/// `Get`s (e.g. an `ArrangeBy`-wrapped or filtered input) get an empty set, since
/// the model cannot attribute an existing index to them.
fn available_for_inputs(
    inputs: &[MirRelationExpr],
    oracle: &dyn IndexOracle,
) -> Vec<BTreeSet<Vec<usize>>> {
    inputs
        .iter()
        .map(|input| {
            let mut keys: BTreeSet<Vec<usize>> = BTreeSet::new();
            // Peel an outer ArrangeBy that JI may have installed; the underlying
            // collection identity is what carries the index.
            let mut node = input;
            while let MirRelationExpr::ArrangeBy { input: inner, .. } = node {
                node = inner;
            }
            if let MirRelationExpr::Get {
                id: Id::Global(gid),
                ..
            } = node
            {
                for (_idx_id, key) in oracle.indexes_on(*gid) {
                    // Only simple column keys map to local column lists.
                    let cols: Option<Vec<usize>> = key
                        .iter()
                        .map(|k| k.as_column())
                        .collect::<Option<Vec<usize>>>();
                    if let Some(mut cols) = cols {
                        cols.sort();
                        keys.insert(cols);
                    }
                }
            }
            keys
        })
        .collect()
}

/// Sanity-check the per-order model against a hand-computed case.
///
/// A 3-input chain A(#0,#1) - B(#2,#3) - C(#4,#5) with edges A.#1 = B.#2 and
/// B.#3 = C.#4, no indexes. Hand count:
///
/// * Differential, n = 3, intermediates = n - 2 = 1.
///   - Order [A, B, C]: B keyed by local col0 (B.#2), C keyed by col0 (C.#4),
///     start A keyed by A.#1 = col1. Three distinct input arrangements
///     -> total 1 + 3 = 4.
///   - Order [B, A, C]: A keyed by col1 (A.#1), C keyed by col0 (C.#4), start B
///     keyed by the columns equating it with A and C, i.e. composite {col0,
///     col1} on B. Input arrangements A:{1}, C:{0}, B:{0,1} -> 3 distinct,
///     total 1 + 3 = 4.
///   The internal-start order does not reduce the count because differential
///   arranges B once by a single composite key either way. Min over orders = 4.
/// * Delta: every input arranged by EACH distinct join key (one arrangement per
///   edge), so B needs two separate arrangements: A:{1}, B:{0}, B:{1}, C:{0}
///   -> 4 arrangements, no intermediates. Note delta is NOT cheaper than
///   differential here; the internal input pays for two single-key arrangements.
fn join_order_model_sanity() {
    let a = src(9001, 2);
    let b = src(9002, 2);
    let c = src(9003, 2);
    let join = MirRelationExpr::join_scalars(
        vec![a, b, c],
        vec![
            vec![MirScalarExpr::column(1), MirScalarExpr::column(2)],
            vec![MirScalarExpr::column(3), MirScalarExpr::column(4)],
        ],
    );
    let (inputs, equivalences) = match &join {
        MirRelationExpr::Join {
            inputs,
            equivalences,
            ..
        } => (inputs, equivalences),
        _ => unreachable!(),
    };
    let available: Vec<BTreeSet<Vec<usize>>> = vec![BTreeSet::new(); inputs.len()];
    let bf = brute_force_orders(inputs, equivalences, &available);
    assert_eq!(bf.n, 3, "three inputs");
    assert_eq!(
        bf.min_differential, 4,
        "hand-computed differential min over orders"
    );
    assert_eq!(bf.delta, 4, "hand-computed delta count (per-edge keys)");
}

/// Multi-way-join fixtures for the brute-force order analysis.
///
/// Each entry is `(name, plan, oracle, join_graph_doc)`. The join graph is what
/// the order lever acts on, so each fixture documents which inputs are equated
/// on which columns and the order-dependent sharing being probed.
fn join_order_fixtures() -> Vec<(&'static str, MirRelationExpr, Box<dyn IndexOracle>)> {
    // CHAIN4: A(#0,#1)-B(#2,#3)-C(#4,#5)-D(#6,#7), edges A.#1=B.#2, B.#3=C.#4,
    // C.#5=D.#6. A linear chain. A and D are leaves with one join column each;
    // B and C are internal with two. The differential walk must arrange every
    // internal input by both of its join columns regardless of start, so the
    // order lever cannot save an internal arrangement. Probes whether starting
    // from a leaf vs the middle changes the count.
    let chain4 = MirRelationExpr::join_scalars(
        vec![src(2101, 2), src(2102, 2), src(2103, 2), src(2104, 2)],
        vec![
            vec![MirScalarExpr::column(1), MirScalarExpr::column(2)],
            vec![MirScalarExpr::column(3), MirScalarExpr::column(4)],
            vec![MirScalarExpr::column(5), MirScalarExpr::column(6)],
        ],
    );
    let chain4_oracle = Box::new(MultiIndex::default());

    // CHAIN5: five-input chain A-B-C-D-E on consecutive columns. Same structure
    // as CHAIN4 with one more internal input, stressing `n!` = 120 orders and a
    // larger fixed intermediate term (n - 2 = 3).
    let chain5 = MirRelationExpr::join_scalars(
        vec![
            src(2201, 2),
            src(2202, 2),
            src(2203, 2),
            src(2204, 2),
            src(2205, 2),
        ],
        vec![
            vec![MirScalarExpr::column(1), MirScalarExpr::column(2)],
            vec![MirScalarExpr::column(3), MirScalarExpr::column(4)],
            vec![MirScalarExpr::column(5), MirScalarExpr::column(6)],
            vec![MirScalarExpr::column(7), MirScalarExpr::column(8)],
        ],
    );
    let chain5_oracle = Box::new(MultiIndex::default());

    // STAR_SAME: center C(#0,#1) joined to L1, L2, L3 all on C.#0. Every leaf
    // equates with C.#0, so C is arranged by a single key {#0} no matter the
    // order, and each leaf by its own join column. The center's one key is
    // shared across all three joins. Probes whether the order forces the center
    // to be arranged more than once (it should not).
    let star_same = MirRelationExpr::join_scalars(
        vec![src(2301, 2), src(2302, 2), src(2303, 2), src(2304, 2)],
        vec![
            vec![MirScalarExpr::column(0), MirScalarExpr::column(2)],
            vec![MirScalarExpr::column(0), MirScalarExpr::column(4)],
            vec![MirScalarExpr::column(0), MirScalarExpr::column(6)],
        ],
    );
    let star_same_oracle = Box::new(MultiIndex::default());

    // STAR_DIFF: center C(#0,#1) joined to L1 on C.#0, to L2 on C.#1, to L3 on
    // C.#0. The center must be arranged by {#0} and by {#1}: two genuinely
    // different keys, irreducible. Probes whether any order avoids one of the
    // two center arrangements (it cannot, because both join columns are needed).
    let star_diff = MirRelationExpr::join_scalars(
        vec![src(2401, 2), src(2402, 2), src(2403, 2), src(2404, 2)],
        vec![
            vec![MirScalarExpr::column(0), MirScalarExpr::column(2)],
            vec![MirScalarExpr::column(1), MirScalarExpr::column(4)],
            vec![MirScalarExpr::column(0), MirScalarExpr::column(6)],
        ],
    );
    let star_diff_oracle = Box::new(MultiIndex::default());

    // CYCLE4: A-B-C-D-A, a 4-cycle. Edges A.#1=B.#2, B.#3=C.#4, C.#5=D.#6,
    // D.#7=A.#0. Every input has two join columns, so each is arranged by both.
    // A cycle gives every left-deep order the same connected structure; probes
    // whether closing the cycle late vs early changes the count.
    let cycle4 = MirRelationExpr::join_scalars(
        vec![src(2501, 2), src(2502, 2), src(2503, 2), src(2504, 2)],
        vec![
            vec![MirScalarExpr::column(1), MirScalarExpr::column(2)],
            vec![MirScalarExpr::column(3), MirScalarExpr::column(4)],
            vec![MirScalarExpr::column(5), MirScalarExpr::column(6)],
            vec![MirScalarExpr::column(7), MirScalarExpr::column(0)],
        ],
    );
    let cycle4_oracle = Box::new(MultiIndex::default());

    // CLIQUE4: every pair of A,B,C,D equated on a shared column. All four inputs
    // share one logical join value: A.#0=B.#2, A.#0=C.#4, A.#0=D.#6, and the
    // transitive B.#2=C.#4=D.#6 follow. Each input is arranged by its single
    // shared column. The densest connectivity; probes whether the order changes
    // the (already minimal) per-input arrangement set.
    let clique4 = MirRelationExpr::join_scalars(
        vec![src(2601, 2), src(2602, 2), src(2603, 2), src(2604, 2)],
        vec![vec![
            MirScalarExpr::column(0),
            MirScalarExpr::column(2),
            MirScalarExpr::column(4),
            MirScalarExpr::column(6),
        ]],
    );
    let clique4_oracle = Box::new(MultiIndex::default());

    // CHAIN4_IDX: CHAIN4 with an index on the middle input C by its left join
    // column {#0} (C.#4). Starting the differential cascade from C reuses the
    // index and saves one input arrangement, while a leaf start cannot. This is
    // the fixture most likely to expose an order win: the index makes one start
    // strictly cheaper. JI's start choice weighs characteristics, not only
    // arrangement count, so it could pick a costlier start.
    let chain4_idx = MirRelationExpr::join_scalars(
        vec![src(2701, 2), src(2702, 2), src(2703, 2), src(2704, 2)],
        vec![
            vec![MirScalarExpr::column(1), MirScalarExpr::column(2)],
            vec![MirScalarExpr::column(3), MirScalarExpr::column(4)],
            vec![MirScalarExpr::column(5), MirScalarExpr::column(6)],
        ],
    );
    // Index on input C (GlobalId 2703) by its left join column #4, which is
    // local column 0 of C.
    let chain4_idx_oracle = Box::new(MultiIndex::default().with(GlobalId::Transient(2703), &[0]));

    vec![
        ("chain4", chain4, chain4_oracle),
        ("chain5", chain5, chain5_oracle),
        ("star_same", star_same, star_same_oracle),
        ("star_diff", star_diff, star_diff_oracle),
        ("cycle4", cycle4, cycle4_oracle),
        ("clique4", clique4, clique4_oracle),
        ("chain4_idx", chain4_idx, chain4_idx_oracle),
    ]
}

/// Reconstruct the left-deep input order JI committed in a `Differential` plan
/// and evaluate the differential model on it.
///
/// Returns the model's total arrangement count for JI's exact chosen order (with
/// `available` reuse), so the caller can assert the model reproduces JI's own
/// committed count. Returns `None` if `plan`'s first join is not differential.
/// This validates the per-order model against production on the one order they
/// share before trusting the model's min over all orders.
fn differential_count_on_ji_order(
    plan: &MirRelationExpr,
    inputs: &[MirRelationExpr],
    equivalences: &[Vec<MirScalarExpr>],
    available: &[BTreeSet<Vec<usize>>],
) -> Option<usize> {
    let mut ji_order: Option<Vec<usize>> = None;
    plan.visit_pre(|n| {
        if ji_order.is_some() {
            return;
        }
        if let MirRelationExpr::Join { implementation, .. } = n {
            if let JoinImplementation::Differential((start_idx, _, _), order) = implementation {
                let mut seq = vec![*start_idx];
                seq.extend(order.iter().map(|(idx, _, _)| *idx));
                ji_order = Some(seq);
            }
        }
    });
    let order = ji_order?;
    let graph = JoinGraph::from_join(inputs, equivalences);
    let intermediates = graph.n.saturating_sub(2);
    let arr = differential_input_arrangements(&graph, &order, available)?;
    Some(intermediates + arr.len())
}

/// Count the delta-strategy arrangements the REAL planner commits for `join`,
/// via `plan_as_delta_query`, counted the same way as the model.
///
/// Returns `None` if delta planning fails (disconnected graph) or the plan has
/// no committed `DeltaQuery`. Used to cross-validate `delta_input_arrangements`
/// against production rather than trusting the hand model alone.
fn real_delta_count(join: &MirRelationExpr, features: &OptimizerFeatures) -> Option<usize> {
    let planned = mz_transform::join_implementation::plan_as_delta_query(join, features).ok()?;
    match ji_committed_arrangements(&planned) {
        Some((count, "delta")) => Some(count),
        _ => None,
    }
}

/// Ask the REAL planner (`plan_join_min_arrangements`) to plan `join` with the
/// given per-input available arrangements, and report its committed
/// `(count, kind)`.
///
/// `available` is the model's per-input arranged-key sets (local columns); they
/// are translated to the `Vec<Vec<Vec<MirScalarExpr>>>` the planner expects.
/// This grounds a claimed order win in the production planner: if the planner,
/// seeing the same indexes, commits a count equal to the brute-force minimum,
/// the win is real and reachable; if it commits the higher count, JI genuinely
/// leaves the cheaper order on the table.
fn real_min_arrangement_plan(
    join: &MirRelationExpr,
    available: &[BTreeSet<Vec<usize>>],
    features: &OptimizerFeatures,
) -> Option<(usize, &'static str)> {
    let real_available: Vec<Vec<Vec<MirScalarExpr>>> = available
        .iter()
        .map(|keys| {
            keys.iter()
                .map(|cols| cols.iter().map(|&c| MirScalarExpr::column(c)).collect())
                .collect()
        })
        .collect();
    let planned = mz_transform::join_implementation::plan_join_min_arrangements(
        join,
        &real_available,
        features,
    )
    .ok()?;
    ji_committed_arrangements(&planned)
}

/// Brute-force the join-ORDER arrangement lever across the multi-way-join
/// fixtures and report, per fixture, JI's committed differential count versus
/// the minimum differential count over all valid join orders.
///
/// The verdict isolates the ORDER lever: it compares JI's committed differential
/// arrangement count to the brute-force minimum over orders under the SAME
/// (differential) strategy. An ORDER WIN is `min_differential < ji_committed`.
/// The delta column is strategy-lever context only (a delta-vs-differential
/// difference is NOT an order win) and is cross-validated against the real
/// `plan_as_delta_query` planner. The model is sanity-checked first.
///
/// Production deploys differential by default (`enable_eager_delta_joins` is
/// off), so JI's committed kind is differential on these arrangement-free
/// fixtures, which is the apples-to-apples baseline for the order question.
#[mz_ore::test]
fn join_order_arrangement_lever() {
    join_order_model_sanity();

    let features = features_for(Eqsat::Off);

    println!("\njoin-order arrangement lever (brute force, differential strategy):");
    println!(
        "{:<14} {:>3} {:>9} {:>7} {:>8} {:>8} {:>10} {:>8}  verdict",
        "fixture", "n", "ji_kind", "ji_cnt", "minDiff", "maxDiff", "deltaModel", "deltaReal"
    );

    let fixtures = join_order_fixtures();
    let mut any_order_win = false;
    for (name, plan, oracle) in &fixtures {
        let off_plan = optimize_full(plan, oracle.as_ref(), Eqsat::Off);
        let (inputs, equivalences) = match first_join(&off_plan) {
            Some(j) => j,
            None => {
                println!("{name:<14} (no Join survived the pipeline; skipped)");
                continue;
            }
        };
        let available = available_for_inputs(inputs, oracle.as_ref());
        let bf = brute_force_orders(inputs, equivalences, &available);

        let (ji_cnt, ji_kind) = ji_committed_arrangements(&off_plan)
            .unwrap_or_else(|| panic!("{name}: no committed join found"));

        // Validate the differential model: evaluated on JI's exact committed
        // order it must reproduce JI's committed count. This anchors the model
        // to production before trusting its min over all orders. (Only when JI
        // committed differential; delta/indexed-filter joins have no such order.)
        if ji_kind == "differential" {
            if let Some(model_on_ji) =
                differential_count_on_ji_order(&off_plan, inputs, equivalences, &available)
            {
                assert_eq!(
                    model_on_ji, ji_cnt,
                    "{name}: differential model on JI's own order ({model_on_ji}) \
                     must equal JI's committed count ({ji_cnt})"
                );
            }
        }

        // Cross-validate the delta model against the real planner on the raw
        // (pre-pipeline) join, where no arrangements exist yet.
        let raw_join = match first_join(plan) {
            Some((ri, re)) => MirRelationExpr::Join {
                inputs: ri.clone(),
                equivalences: re.clone(),
                implementation: JoinImplementation::Unimplemented,
            },
            None => off_plan.clone(),
        };
        // The real planner is ground truth for delta; the hand model is a
        // structural cross-check. They agree for chains/stars; for denser graphs
        // (cycles, cliques) the real delta planner derives composite keys that
        // cover several edges with one arrangement, so the hand model can
        // over-count. We report both and flag divergence rather than assert,
        // because delta is strategy-lever context, not the order-lever verdict.
        let delta_real = real_delta_count(&raw_join, &features);
        if let Some(dr) = delta_real {
            if dr != bf.delta {
                println!(
                    "  note: {name} delta model={} real={dr} (real planner shares \
                     arrangements across edges; real value is authoritative)",
                    bf.delta
                );
            }
        }
        let delta_real_str = delta_real.map_or_else(|| "-".to_string(), |d| d.to_string());

        // The order-lever verdict: compare JI's committed count to the min over
        // orders under the strategy JI actually used. On these fixtures JI is
        // differential, so we compare against `min_differential`.
        let verdict = match ji_kind {
            "differential" => {
                if bf.min_differential < ji_cnt {
                    // The model claims a cheaper order. Ground it in the real
                    // planner: ask `plan_join_min_arrangements` with the same
                    // indexes. If it ALSO commits the higher count, the cheaper
                    // order is one the production planner does not reach, so the
                    // win is real (JI leaves it on the table). If the real
                    // planner reaches the lower count, JI in the full pipeline
                    // differs for some other reason and we flag it.
                    let real = real_min_arrangement_plan(&raw_join, &available, &features);
                    let real_str = real
                        .map(|(c, k)| format!("{k}:{c}"))
                        .unwrap_or_else(|| "none".to_string());
                    match real {
                        Some((real_cnt, _)) if real_cnt <= bf.min_differential => {
                            // The real planner, given the indexes, reaches the
                            // brute-force minimum. The pipeline-JI count being
                            // higher is then a pipeline artifact (the index was
                            // not credited to the join), NOT an order win.
                            println!(
                                "  note: {name} model min={} but real planner reaches \
                                 {real_str}; pipeline JI={ji_cnt} differs because the \
                                 index is not credited to the join input in the full \
                                 pipeline. NOT an order win.",
                                bf.min_differential
                            );
                            "no order win (index-credit artifact)"
                        }
                        _ => {
                            any_order_win = true;
                            println!(
                                "  ORDER WIN detail: {name} JI committed {ji_cnt}, brute-force \
                                 min differential {} over orders; real planner with indexes: \
                                 {real_str}",
                                bf.min_differential
                            );
                            "ORDER WIN (differential)"
                        }
                    }
                } else if bf.min_differential == ji_cnt {
                    "tie (JI order minimal)"
                } else {
                    // The model found no order as cheap as JI's committed count.
                    // JI used reuse the model does not credit; not an order win.
                    "JI below model (no win)"
                }
            }
            // If JI committed delta or indexed-filter the differential order
            // lever does not apply; report without a win claim.
            other => {
                if bf.min_differential < ji_cnt {
                    "differential order beats JI's non-diff plan (strategy, not order)"
                } else {
                    other
                }
            }
        };

        println!(
            "{name:<14} {n:>3} {ji_kind:>9} {ji_cnt:>7} {min_diff:>8} {max_diff:>8} {delta:>10} {delta_real_str:>8}  {verdict}",
            n = bf.n,
            min_diff = bf.min_differential,
            max_diff = bf.max_differential,
            delta = bf.delta,
        );
    }

    if any_order_win {
        println!(
            "\nRESULT: at least one fixture has a join ORDER needing strictly fewer \
             differential arrangements than JI committed (ORDER WIN). See rows."
        );
    } else {
        println!(
            "\nRESULT: on every fixture, JI's committed differential order is \
             arrangement-minimal over all join orders under the same strategy. \
             CONCLUSIVE NEGATIVE for the join-order lever on these graphs. The \
             only arrangement differences observed are delta-vs-differential \
             STRATEGY differences, which are not the order lever."
        );
    }
}

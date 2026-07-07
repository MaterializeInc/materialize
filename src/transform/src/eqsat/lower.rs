// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Translate a real `MirRelationExpr` into the prototype `Rel`. Supported
//! variants map structurally; every unsupported variant is carried verbatim in
//! a [`Rel::Opaque`] leaf, so the supported envelope around it still saturates.

use std::collections::BTreeMap;

use mz_expr::{Id, MirRelationExpr, MirScalarExpr};
use mz_ore::cast::CastFrom;
use mz_repr::ReprColumnType;

use crate::eqsat::ir::{EScalar, RecVersion, Rel, TopKShape};

/// Per-`LetRec` lowering context: maps an in-scope recursive id to the version
/// a reference to it should carry from the current position. Absent ids are
/// not bound by an enclosing `LetRec` (ordinary `Let` or outer scope) and stay
/// `version: None`.
#[derive(Clone, Default)]
struct LowerCtx {
    enable_wmr_lift: bool,
    rec_version: BTreeMap<usize, RecVersion>,
}

/// Lower `expr` to a `Rel`, carrying scalars as real `MirScalarExpr`s and
/// bailing unsupported subtrees verbatim into [`Rel::Opaque`].
///
/// Equivalent to `lower_with(expr, false)`: every `LocalGet.version` is `None`.
/// Use `lower_with` with `enable_wmr_lift = true` to classify `LetRec`
/// references by iteration version.
pub fn lower(expr: &MirRelationExpr) -> Rel {
    lower_with(expr, false)
}

/// Lower `expr` to a `Rel` with optional WMR-lift version tagging.
///
/// When `enable_wmr_lift` is `true`, references inside a `LetRec` carry a
/// `RecVersion` tag: `Prev` for self or later bindings, `Cur` for earlier
/// bindings (already bound this iteration), and `Final` for reads from the
/// body. When `false`, all `LocalGet.version` fields are `None` and the result
/// is identical to the pre-WMR lowering.
///
/// Supported variants lower structurally: `Project`, `Map`, `Filter`, `Join`,
/// `Reduce`, `TopK`, `Negate`, `Threshold`, `Union`, `Let`, `LetRec`,
/// `FlatMap`, `Get { Id::Local }`, a single-key `ArrangeBy` (to
/// `Rel::ArrangeBy`), and a multi-key `ArrangeBy` (to `Rel::ArrangeByMany`).
/// Everything else (`Constant`, `Get { Id::Global }`) is bailed: the whole
/// subtree is stored verbatim in a `Rel::Opaque`, so raise re-emits it exactly.
pub fn lower_with(expr: &MirRelationExpr, enable_wmr_lift: bool) -> Rel {
    lower_ctx(
        expr,
        &LowerCtx {
            enable_wmr_lift,
            ..Default::default()
        },
    )
}

fn lower_ctx(expr: &MirRelationExpr, ctx: &LowerCtx) -> Rel {
    use MirRelationExpr::*;
    match expr {
        Get {
            id: Id::Local(local),
            typ,
            ..
        } => {
            // LocalId wraps a pub(crate) u64; only From<&LocalId> for u64 is
            // available, so we go through that and then widen to usize.
            let id_usize = usize::cast_from(u64::from(local));
            let version = if ctx.enable_wmr_lift {
                ctx.rec_version.get(&id_usize).copied()
            } else {
                None
            };
            // Carry the exact original node (with its real type) so raise can
            // reconstruct it verbatim.
            Rel::LocalGet {
                id: id_usize,
                arity: typ.arity(),
                get: Some(Box::new(expr.clone())),
                version,
            }
        }
        Project { input, outputs } => Rel::Project {
            input: Box::new(lower_ctx(input, ctx)),
            outputs: outputs.clone(),
        },
        Map { input, scalars } => {
            // Scalars in a Map reference input columns AND the columns produced
            // by earlier scalars in the same Map: scalar `i` may reference
            // columns `0..input_arity + i`. Fold each against the context built
            // from the input type extended with the types of preceding scalars,
            // so a later scalar's column reference stays in bounds.
            let col_types = input.typ().column_types;
            Rel::Map {
                input: Box::new(lower_ctx(input, ctx)),
                scalars: escalars_in_map_context(scalars, col_types),
            }
        }
        Filter { input, predicates } => {
            // Predicates reference input columns; use the input type to fold.
            let col_types = input.typ().column_types;
            Rel::Filter {
                input: Box::new(lower_ctx(input, ctx)),
                predicates: escalars_in_context(predicates, &col_types),
            }
        }
        Join {
            inputs,
            equivalences,
            ..
        } => {
            // Equivalence scalars reference the concatenated input column space
            // (input i's columns offset by the sum of prior input arities), so
            // fold them against the concatenation of input column types.
            let mut col_types = Vec::new();
            for inp in inputs {
                col_types.extend(inp.typ().column_types);
            }
            Rel::Join {
                inputs: inputs.iter().map(|i| lower_ctx(i, ctx)).collect(),
                equivalences: equivalences
                    .iter()
                    .map(|class| {
                        class
                            .iter()
                            .map(|e| reduced_escalar(e, &col_types))
                            .collect()
                    })
                    .collect(),
            }
        }
        Negate { input } => Rel::Negate {
            input: Box::new(lower_ctx(input, ctx)),
        },
        Threshold { input } => Rel::Threshold {
            input: Box::new(lower_ctx(input, ctx)),
        },
        Union { base, inputs } => Rel::Union {
            base: Box::new(lower_ctx(base, ctx)),
            inputs: inputs.iter().map(|i| lower_ctx(i, ctx)).collect(),
        },
        Let { id, value, body } => Rel::Let {
            id: usize::cast_from(u64::from(id)),
            value: Box::new(lower_ctx(value, ctx)),
            body: Box::new(lower_ctx(body, ctx)),
        },
        LetRec {
            ids,
            values,
            limits,
            body,
        } => {
            // Each id is in scope in every value and in the body.
            // The recursive references lower to sealed Rel::LocalGet leaves
            // (Get { Id::Local } already lowers that way), so no e-graph cycle
            // is formed. limits are carried verbatim for faithful raise.
            let id_usizes: Vec<usize> = ids
                .iter()
                .map(|id| usize::cast_from(u64::from(id)))
                .collect();
            let bindings = id_usizes
                .iter()
                .enumerate()
                .zip(values.iter())
                .map(|((i, &id), v)| {
                    // From binding i, an id ordered before i is Cur (already
                    // bound this iteration), id i or later is Prev (the feedback
                    // variable). Outer-scope ids inherit ctx.rec_version.
                    let mut rec_version = ctx.rec_version.clone();
                    for (k, &kid) in id_usizes.iter().enumerate() {
                        rec_version.insert(
                            kid,
                            if k < i {
                                RecVersion::Cur
                            } else {
                                RecVersion::Prev
                            },
                        );
                    }
                    let inner = LowerCtx {
                        enable_wmr_lift: ctx.enable_wmr_lift,
                        rec_version,
                    };
                    (id, lower_ctx(v, &inner))
                })
                .collect();
            // Body reads converged values.
            let mut body_version = ctx.rec_version.clone();
            for &id in &id_usizes {
                body_version.insert(id, RecVersion::Final);
            }
            let body_ctx = LowerCtx {
                enable_wmr_lift: ctx.enable_wmr_lift,
                rec_version: body_version,
            };
            Rel::LetRec {
                bindings,
                limits: limits.clone(),
                body: Box::new(lower_ctx(body, &body_ctx)),
            }
        }
        Reduce {
            input,
            group_key,
            aggregates,
            monotonic,
            expected_group_size,
        } => {
            // Group keys and aggregate input expressions reference the input
            // columns; fold them against the input type.
            let col_types = input.typ().column_types;
            let aggregates = aggregates
                .iter()
                .map(|agg| {
                    let mut agg = agg.clone();
                    agg.expr.reduce(&col_types);
                    agg
                })
                .collect();
            Rel::Reduce {
                input: Box::new(lower_ctx(input, ctx)),
                group_key: group_key
                    .iter()
                    .map(|e| reduced_escalar(e, &col_types))
                    .collect(),
                aggregates,
                monotonic: *monotonic,
                expected_group_size: *expected_group_size,
            }
        }
        TopK {
            input,
            group_key,
            order_key,
            limit,
            offset,
            monotonic,
            expected_group_size,
        } => {
            // The limit is a scalar evaluated over the input; fold it.
            let col_types = input.typ().column_types;
            let limit = limit.as_ref().map(|l| {
                let mut l = l.clone();
                l.reduce(&col_types);
                l
            });
            Rel::TopK {
                input: Box::new(lower_ctx(input, ctx)),
                shape: TopKShape {
                    group_key: group_key.clone(),
                    order_key: order_key.clone(),
                    limit,
                    offset: *offset,
                    monotonic: *monotonic,
                    expected_group_size: *expected_group_size,
                },
            }
        }
        // FlatMap arguments reference the input column space; reduce each against
        // the input column types. The function itself is carried verbatim.
        FlatMap { input, func, exprs } => {
            let col_types = input.typ().column_types;
            Rel::FlatMap {
                input: Box::new(lower_ctx(input, ctx)),
                func: func.clone(),
                exprs: escalars_in_context(exprs, &col_types),
            }
        }
        // A single-key arrangement lowers to the explicit `Rel::ArrangeBy`
        // primitive so the optimizer can see, share, and reason through it. The
        // key scalars are reduced against the input column types like every
        // other scalar payload (`ReduceScalars` at lower time), so a P2
        // `produces_key` match sees the same canonical form. A multi-key
        // arrangement lowers to `Rel::ArrangeByMany` in the arm below.
        ArrangeBy { input, keys } if keys.len() == 1 => {
            let col_types = input.typ().column_types;
            Rel::ArrangeBy {
                input: Box::new(lower_ctx(input, ctx)),
                key: keys[0]
                    .iter()
                    .map(|e| reduced_escalar(e, &col_types))
                    .collect(),
            }
        }
        // A multi-key arrangement lowers to ArrangeByMany, which the cost model
        // and raise handle symmetrically with the single-key form. A zero-key
        // ArrangeBy maintains no arrangement and changes no relational fact, so
        // it is a structural no-op and lowers to its input directly. Single-key
        // stays as Rel::ArrangeBy so the arrange_idempotent rule and DSL remain
        // valid; multi-key uses Rel::ArrangeByMany.
        ArrangeBy { input, keys } => {
            let col_types = input.typ().column_types;
            if keys.len() >= 2 {
                Rel::ArrangeByMany {
                    input: Box::new(lower_ctx(input, ctx)),
                    keys: keys
                        .iter()
                        .map(|k| escalars_in_context(k, &col_types))
                        .collect(),
                }
            } else {
                // keys.len() == 0: a zero-key arrangement is a physical no-op
                // (no index is built), so it is equivalent to its bare input.
                lower_ctx(input, ctx)
            }
        }
        // Unsupported: bail the entire subtree to an opaque leaf, carried
        // verbatim. Their payloads are type/row-sensitive and no rule rewrites
        // them, so an opaque leaf makes raising trivially exact. Global Get and
        // Constant are also bailed: Global Get carries a GlobalId not a
        // structural subexpression, and Constant carries row data irrelevant to
        // relational rewrites.
        Constant { .. }
        | Get {
            id: Id::Global(_), ..
        } => Rel::Opaque(Box::new(expr.clone())),
    }
}

/// Reduce `expr` against `col_types` (its evaluation context) and wrap the
/// reduced form in an [`EScalar`], recording the `lit` fact off the reduced
/// expression. This is `ReduceScalars` performed once at lower time, so every
/// scalar canonicalization MIR's simplifier provides (constant folding, CASE
/// coalescing, literal CASE rewriting) lands in the interned payload and rides
/// through saturation unchanged.
///
/// `reduce` may use column nullability to simplify, for example rewriting
/// `IsNull(x)` to `false` when `x` is provably non-null in `col_types`. The
/// reduced form is therefore tied to the nullability of the context it was
/// folded against. Most active rules keep a scalar in an equal-or-stricter
/// context (column permutation renames indices but preserves types, filter
/// pushdown past a project or through map/negate/threshold preserves each
/// referenced column's type), where a conservatively-reduced scalar stays
/// correct. The one rule that moves a scalar from a stricter to a looser
/// context is filter-pushdown into a join input: the predicate is folded
/// against the join output type, which strengthens columns to non-null across
/// equivalence classes, then pushed onto a single input whose plain type is
/// nullable. That is sound because the join equivalence enforces the
/// strengthened non-null fact on every surviving row, so the reduced predicate
/// computes the same result after the push. This mirrors what the production
/// `predicate_pushdown` transform already does (it reduces against the join
/// output type before pushing into inputs). The soundness condition is thus
/// per-rule semantic identity, not a blanket no-relaxation rule.
fn reduced_escalar(expr: &MirScalarExpr, col_types: &[ReprColumnType]) -> EScalar {
    let mut folded = expr.clone();
    folded.reduce(col_types);
    // `lit` is the canonical deterministic function of the (reduced) expr, the
    // same derivation the combined-graph `intern_scalar` re-applies, so the two
    // intern paths agree on the cached fact for any shared scalar class.
    let lit = EScalar::lit_of(&folded);
    EScalar::new(folded, lit)
}

/// Reduce each scalar against `col_types` and wrap it in an [`EScalar`].
fn escalars_in_context(exprs: &[MirScalarExpr], col_types: &[ReprColumnType]) -> Vec<EScalar> {
    exprs
        .iter()
        .map(|e| reduced_escalar(e, col_types))
        .collect()
}

/// Like [`escalars_in_context`], but for `Map` scalars: each scalar may
/// reference the columns produced by preceding scalars in the same `Map`, so
/// the fold context grows by each scalar's type as we go. Folding against a
/// context missing those columns would index out of bounds in `typ`/`reduce`.
fn escalars_in_map_context(
    exprs: &[MirScalarExpr],
    mut col_types: Vec<ReprColumnType>,
) -> Vec<EScalar> {
    let mut out = Vec::with_capacity(exprs.len());
    for e in exprs {
        let escalar = reduced_escalar(e, &col_types);
        // Extend the context with this scalar's type so the next scalar can
        // reference it. `reduce` preserves the type, so the reduced payload's
        // type matches the original's.
        let typ = escalar.expr.typ(&col_types);
        out.push(escalar);
        col_types.push(typ);
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use mz_expr::{
        AccessStrategy, AggregateExpr, AggregateFunc, BinaryFunc, Id, LocalId, MirRelationExpr,
        MirScalarExpr,
    };
    use mz_repr::{Datum, ReprRelationType, ReprScalarType};

    use crate::eqsat::ir::{RecVersion, Rel};

    /// Walk `r` and collect `(id, version)` pairs from every `LocalGet` node.
    /// Because `Rel::children()` already returns `LetRec` binding values in
    /// addition to the body, the recursive walk reaches all LocalGet nodes
    /// inside a `LetRec` without any special-casing.
    fn collect_localget_versions(
        r: &Rel,
    ) -> std::collections::BTreeSet<(usize, Option<RecVersion>)> {
        let mut out = std::collections::BTreeSet::new();
        fn walk(r: &Rel, out: &mut std::collections::BTreeSet<(usize, Option<RecVersion>)>) {
            if let Rel::LocalGet { id, version, .. } = r {
                out.insert((*id, *version));
            }
            for c in r.children() {
                walk(c, out);
            }
        }
        walk(r, &mut out);
        out
    }

    #[mz_ore::test]
    fn lower_classifies_letrec_versions() {
        // LetRec with two bindings:
        //   binding 0 = g(1): reads id 1, which is ordered after 0, so Prev.
        //   binding 1 = g(0): reads id 0, which is ordered before 1, so Cur.
        //   body = g(0): reads id 0 from the body, so Final.
        let typ = ReprRelationType::new(vec![ReprScalarType::Int64.nullable(false)]);
        let g = |id: u64| MirRelationExpr::Get {
            id: Id::Local(LocalId::new(id)),
            typ: typ.clone(),
            access_strategy: AccessStrategy::UnknownOrLocal,
        };
        let r = MirRelationExpr::LetRec {
            ids: vec![LocalId::new(0), LocalId::new(1)],
            values: vec![g(1), g(0)],
            limits: vec![None, None],
            body: Box::new(g(0)),
        };
        let lowered = lower_with(&r, true);
        let versions = collect_localget_versions(&lowered);
        let expected: std::collections::BTreeSet<(usize, Option<RecVersion>)> = [
            (1, Some(RecVersion::Prev)),  // binding 0 reads later id 1
            (0, Some(RecVersion::Cur)),   // binding 1 reads earlier id 0
            (0, Some(RecVersion::Final)), // body reads id 0
        ]
        .into_iter()
        .collect();
        assert_eq!(versions, expected);
    }

    #[mz_ore::test]
    fn lower_flag_off_sets_no_versions() {
        // Same LetRec as above, flag off: every LocalGet has version None.
        let typ = ReprRelationType::new(vec![ReprScalarType::Int64.nullable(false)]);
        let g = |id: u64| MirRelationExpr::Get {
            id: Id::Local(LocalId::new(id)),
            typ: typ.clone(),
            access_strategy: AccessStrategy::UnknownOrLocal,
        };
        let r = MirRelationExpr::LetRec {
            ids: vec![LocalId::new(0), LocalId::new(1)],
            values: vec![g(1), g(0)],
            limits: vec![None, None],
            body: Box::new(g(0)),
        };
        let lowered = lower_with(&r, false);
        assert!(
            collect_localget_versions(&lowered)
                .iter()
                .all(|(_, v)| v.is_none()),
            "expected all versions to be None when enable_wmr_lift=false"
        );
    }

    fn base(arity: usize) -> MirRelationExpr {
        let typ = ReprRelationType::new(
            (0..arity)
                .map(|_| ReprScalarType::Int64.nullable(false))
                .collect(),
        );
        MirRelationExpr::constant(vec![], typ)
    }

    #[mz_ore::test]
    fn lower_filter_of_constant_gives_filter_over_opaque() {
        // A `Filter` whose input is `Constant` (bailed) should still produce a
        // `Rel::Filter` envelope with a `Rel::Opaque` input.
        let r = base(2).filter(vec![MirScalarExpr::column(0)]);
        let rel = lower(&r);
        match rel {
            Rel::Filter { predicates, input } => {
                assert_eq!(predicates.len(), 1);
                // The Constant input is bailed to an opaque leaf.
                assert!(
                    matches!(*input, Rel::Opaque(_)),
                    "expected Opaque under filter, got {input:?}"
                );
            }
            other => panic!("expected Filter, got {other:?}"),
        }
    }

    #[mz_ore::test]
    fn single_key_arrange_by_lowers_to_arrange_by() {
        // A single-key `ArrangeBy` lowers to the explicit `Rel::ArrangeBy`
        // primitive, preserving the input arity and the key.
        let inner = base(2);
        let arity = inner.arity();
        let r = inner.arrange_by(&[vec![MirScalarExpr::column(0)]]);
        let rel = lower(&r);
        match rel {
            Rel::ArrangeBy { input, key } => {
                assert_eq!(input.arity(), arity);
                assert_eq!(key.len(), 1);
                assert_eq!(key[0].is_col(), Some(0));
            }
            other => panic!("expected ArrangeBy, got {other:?}"),
        }
    }

    #[mz_ore::test]
    fn multi_key_arrange_by_becomes_arrange_by_many() {
        // A multi-key `ArrangeBy` is now modeled as `Rel::ArrangeByMany`, one
        // maintained index per key list. The arity of the node equals that of
        // its input because ArrangeByMany is a multiset identity.
        let inner = base(2);
        let arity = inner.arity();
        let r = inner.arrange_by(&[
            vec![MirScalarExpr::column(0)],
            vec![MirScalarExpr::column(1)],
        ]);
        let rel = lower(&r);
        match rel {
            Rel::ArrangeByMany { ref keys, .. } => {
                assert_eq!(rel.arity(), arity);
                assert_eq!(keys.len(), 2);
            }
            other => panic!("expected ArrangeByMany, got {other:?}"),
        }
    }

    #[mz_ore::test]
    fn filter_over_arrange_by_keeps_filter_envelope() {
        // A supported `Filter` wrapping a single-key `ArrangeBy` lowers to
        // `Rel::Filter` with the `ArrangeBy` lowered as its input.
        let arranged = base(2).arrange_by(&[vec![MirScalarExpr::column(0)]]);
        let r = arranged.filter(vec![MirScalarExpr::column(0)]);
        let rel = lower(&r);
        match rel {
            Rel::Filter { input, .. } => assert!(
                matches!(*input, Rel::ArrangeBy { .. }),
                "expected ArrangeBy under filter, got {input:?}"
            ),
            other => panic!("expected Filter envelope, got {other:?}"),
        }
    }

    #[mz_ore::test]
    fn lower_project() {
        let r = base(3).project(vec![2, 0]);
        let rel = lower(&r);
        match rel {
            Rel::Project { outputs, .. } => assert_eq!(outputs, vec![2, 0]),
            other => panic!("expected Project, got {other:?}"),
        }
    }

    #[mz_ore::test]
    fn lower_map() {
        let r = base(2).map(vec![MirScalarExpr::column(0), MirScalarExpr::column(1)]);
        let rel = lower(&r);
        match rel {
            Rel::Map { scalars, .. } => assert_eq!(scalars.len(), 2),
            other => panic!("expected Map, got {other:?}"),
        }
    }

    #[mz_ore::test]
    fn lower_negate_threshold() {
        let neg = lower(&base(1).negate());
        assert!(matches!(neg, Rel::Negate { .. }), "expected Negate");
        let thr = lower(&base(1).threshold());
        assert!(matches!(thr, Rel::Threshold { .. }), "expected Threshold");
    }

    #[mz_ore::test]
    fn constant_is_bailed_to_opaque_leaf() {
        // `Constant` is in the bail set; verify it becomes a `Rel::Opaque`.
        let rel = lower(&base(3));
        match rel {
            Rel::Opaque(m) => assert_eq!(m.arity(), 3),
            other => panic!("expected opaque leaf, got {other:?}"),
        }
    }

    #[mz_ore::test]
    fn filter_predicate_payload_is_reduced() {
        // `#0 AND true` over a boolean column reduces to `#0`. The stored
        // payload must be the reduced form, not the original conjunction.
        let typ = ReprRelationType::new(vec![ReprScalarType::Bool.nullable(false)]);
        let r = MirRelationExpr::constant(vec![], typ).filter(vec![
            MirScalarExpr::column(0).and(MirScalarExpr::literal_true()),
        ]);
        let rel = lower(&r);
        match rel {
            Rel::Filter { predicates, .. } => {
                assert_eq!(predicates.len(), 1);
                assert_eq!(
                    predicates[0].expr,
                    MirScalarExpr::column(0),
                    "filter predicate payload was not reduced"
                );
            }
            other => panic!("expected Filter, got {other:?}"),
        }
    }

    #[mz_ore::test]
    fn reduce_group_key_payload_is_reduced() {
        // A group key `#0 AND true` over a boolean column reduces to `#0`.
        let typ = ReprRelationType::new(vec![ReprScalarType::Bool.nullable(false)]);
        let input = MirRelationExpr::constant(vec![], typ);
        let r = MirRelationExpr::Reduce {
            input: Box::new(input),
            group_key: vec![MirScalarExpr::column(0).and(MirScalarExpr::literal_true())],
            aggregates: vec![],
            monotonic: false,
            expected_group_size: None,
        };
        let rel = lower(&r);
        match rel {
            Rel::Reduce { group_key, .. } => {
                assert_eq!(group_key.len(), 1);
                assert_eq!(
                    group_key[0].expr,
                    MirScalarExpr::column(0),
                    "reduce group key payload was not reduced"
                );
            }
            other => panic!("expected Reduce, got {other:?}"),
        }
    }

    #[mz_ore::test]
    fn map_scalar_payload_is_reduced() {
        // `1 + 1` is constant-folded to `2`. The stored Map payload must be the
        // folded literal.
        let one = MirScalarExpr::literal_ok(Datum::Int64(1), ReprScalarType::Int64);
        let sum = one
            .clone()
            .call_binary(one, BinaryFunc::AddInt64(mz_expr::func::AddInt64));
        let r = base(1).map(vec![sum]);
        let rel = lower(&r);
        match rel {
            Rel::Map { scalars, .. } => {
                assert_eq!(scalars.len(), 1);
                assert_eq!(
                    scalars[0].expr,
                    MirScalarExpr::literal_ok(Datum::Int64(2), ReprScalarType::Int64),
                    "map scalar payload was not constant-folded"
                );
            }
            other => panic!("expected Map, got {other:?}"),
        }
    }

    #[mz_ore::test]
    fn join_equivalence_payload_is_reduced() {
        // A two-input join over boolean columns with an equivalence containing
        // `#0 AND true`. Join equivalences reference the concatenated input
        // column space, so column 0 of the first input is `#0`. The payload
        // must reduce to `#0`.
        let typ = ReprRelationType::new(vec![ReprScalarType::Bool.nullable(false)]);
        let a = MirRelationExpr::constant(vec![], typ.clone());
        let b = MirRelationExpr::constant(vec![], typ);
        let r = MirRelationExpr::join_scalars(
            vec![a, b],
            vec![vec![
                MirScalarExpr::column(0).and(MirScalarExpr::literal_true()),
                MirScalarExpr::column(1),
            ]],
        );
        let rel = lower(&r);
        match rel {
            Rel::Join { equivalences, .. } => {
                assert_eq!(equivalences.len(), 1);
                assert_eq!(
                    equivalences[0][0].expr,
                    MirScalarExpr::column(0),
                    "join equivalence payload was not reduced"
                );
            }
            other => panic!("expected Join, got {other:?}"),
        }
    }

    #[mz_ore::test]
    fn reduce_aggregate_expr_payload_is_reduced() {
        // An aggregate input expression `1 + 1` is constant-folded to `2`. The
        // aggregate path reduces `agg.expr` directly (not via reduced_escalar),
        // so it needs its own coverage.
        let one = MirScalarExpr::literal_ok(Datum::Int64(1), ReprScalarType::Int64);
        let sum = one
            .clone()
            .call_binary(one, BinaryFunc::AddInt64(mz_expr::func::AddInt64));
        let r = MirRelationExpr::Reduce {
            input: Box::new(base(1)),
            group_key: vec![],
            aggregates: vec![AggregateExpr {
                func: AggregateFunc::MaxInt64,
                expr: sum,
                distinct: false,
            }],
            monotonic: false,
            expected_group_size: None,
        };
        let rel = lower(&r);
        match rel {
            Rel::Reduce { aggregates, .. } => {
                assert_eq!(aggregates.len(), 1);
                assert_eq!(
                    aggregates[0].expr,
                    MirScalarExpr::literal_ok(Datum::Int64(2), ReprScalarType::Int64),
                    "aggregate input expression was not constant-folded"
                );
            }
            other => panic!("expected Reduce, got {other:?}"),
        }
    }

    #[mz_ore::test]
    fn topk_limit_payload_is_reduced() {
        // A TopK limit `1 + 0` is constant-folded to `1`. The limit path reduces
        // the scalar directly, so it needs its own coverage.
        let one = MirScalarExpr::literal_ok(Datum::Int64(1), ReprScalarType::Int64);
        let zero = MirScalarExpr::literal_ok(Datum::Int64(0), ReprScalarType::Int64);
        let limit = one.call_binary(zero, BinaryFunc::AddInt64(mz_expr::func::AddInt64));
        let r = MirRelationExpr::TopK {
            input: Box::new(base(1)),
            group_key: vec![],
            order_key: vec![],
            limit: Some(limit),
            offset: 0,
            monotonic: false,
            expected_group_size: None,
        };
        let rel = lower(&r);
        match rel {
            Rel::TopK { shape, .. } => {
                assert_eq!(
                    shape.limit,
                    Some(MirScalarExpr::literal_ok(
                        Datum::Int64(1),
                        ReprScalarType::Int64
                    )),
                    "TopK limit was not constant-folded"
                );
            }
            other => panic!("expected TopK, got {other:?}"),
        }
    }
}

// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.

//! Recursion-aware analysis driver: the same lattice-fixpoint idea as the
//! e-class [`Analysis`], applied to `LetRec`/`LocalGet` trees.

// --- recursion: the same analysis as a fixpoint over `LetRec` ---------------
//
// The e-class `Analysis` above is solved by a monotone fixpoint over the
// e-graph (`EGraph::run_analysis`). A *recursive* binding needs the very same
// shape, just with the recursive `LocalGet` as the iterated variable: assume a
// fact for the binding, evaluate its body, and repeat to a fixpoint. That is
// what makes "the analysis framework and recursion are one mechanism" concrete
// rather than aspirational — this driver is a second instance of the identical
// idea, over `Rel` trees with `LetRec`/`LocalGet`.

use std::collections::BTreeMap;

use crate::analysis::equivalences::EquivalenceClasses;
use crate::eqsat::ir::Rel;

use super::constant_columns::ConstCols;
use super::keys::{KeySet, KeysRec};

/// The direction a recursive fixpoint is iterated from.
#[derive(Clone, Copy, PartialEq, Eq)]
pub enum Direction {
    /// Least fixpoint: start a recursive variable at ⊥ and grow. Right for
    /// facts that *under-approximate* (e.g. the keys we can be sure of — a
    /// recursive reference contributes none until proven).
    Lfp,
    /// Greatest fixpoint: start a recursive variable at ⊤ and shrink. Right for
    /// *invariants* we want to guarantee of the whole fixpoint (non-negativity,
    /// monotonicity): assume the property holds of the recursive reference and
    /// keep it only if the body preserves it.
    Gfp,
}

/// An analysis evaluated over a `Rel` tree, recursion-aware. Mirrors the
/// e-class [`Analysis`]: a lattice `Domain` with a transfer function `make`,
/// but parameterized by the [`Direction`] from which a recursive binding's
/// fixpoint is approached.
///
/// [`Analysis`]: crate::eqsat::core::Analysis
pub trait RecAnalysis {
    type Domain: Clone + Eq;

    /// The starting fact for a recursive variable (⊥ for [`Direction::Lfp`], ⊤
    /// for [`Direction::Gfp`]).
    fn start(&self) -> Self::Domain;

    fn direction(&self) -> Direction;

    /// Transfer for a non-scope node, reading each child's fact via `child`.
    /// Never called on `Let`/`LetRec`/`LocalGet` — the driver handles those.
    fn make(&self, rel: &Rel, child: &dyn Fn(&Rel) -> Self::Domain) -> Self::Domain;
}

type Env<T> = BTreeMap<usize, T>;

/// Evaluate `a` over `rel`, resolving `LetRec`/`Let`/`LocalGet` against `env`.
fn rec_eval<A: RecAnalysis>(a: &A, rel: &Rel, env: &Env<A::Domain>) -> A::Domain {
    match rel {
        Rel::LocalGet { id, .. } => env.get(id).cloned().unwrap_or_else(|| a.start()),
        Rel::Let { id, value, body } => {
            let v = rec_eval(a, value, env);
            let mut env2 = env.clone();
            env2.insert(*id, v);
            rec_eval(a, body, &env2)
        }
        Rel::LetRec { bindings, body, .. } => {
            let env2 = rec_solve(a, bindings, env);
            rec_eval(a, body, &env2)
        }
        _ => a.make(rel, &|c| rec_eval(a, c, env)),
    }
}

/// Solve a `LetRec`'s mutually-recursive bindings to a fixpoint (Gauss–Seidel:
/// each binding sees the latest facts). The transfer is monotone and the
/// lattice finite, so starting every binding at [`RecAnalysis::start`] for the
/// chosen direction converges to the least/greatest fixpoint.
pub(crate) fn rec_solve<A: RecAnalysis>(
    a: &A,
    bindings: &[(usize, Rel)],
    outer: &Env<A::Domain>,
) -> Env<A::Domain> {
    let mut env = outer.clone();
    for (id, _) in bindings {
        env.insert(*id, a.start());
    }
    loop {
        let mut changed = false;
        for (id, value) in bindings {
            let next = rec_eval(a, value, &env);
            if env.get(id) != Some(&next) {
                env.insert(*id, next);
                changed = true;
            }
        }
        if !changed {
            break;
        }
    }
    env
}

/// Run a recursion-aware analysis over a whole plan.
pub fn rec_analyze<A: RecAnalysis>(a: &A, rel: &Rel) -> A::Domain {
    rec_eval(a, rel, &Env::new())
}

/// Facts about `LocalGet`-bound relations, proven by the recursion-aware
/// fixpoints, to seed the e-class analyses while saturating a binding fragment
/// (option B: the recursion analysis feeds the in-fragment rewriter, so an
/// analysis-gated rule can fire on a provably non-negative / monotone / keyed
/// recursive reference). Maps a bound id to its fact.
#[derive(Clone, Default, Debug)]
pub struct LocalFacts {
    pub nonneg: BTreeMap<usize, bool>,
    pub monotonic: BTreeMap<usize, bool>,
    pub keys: BTreeMap<usize, KeySet>,
    /// Per-binding equivalence classes, seeded `Some(default)` for LetRec
    /// bindings (conservative: we assume no equivalences are known for the
    /// recursive reference until a fixpoint is computed). A full recursion-aware
    /// equivalences fixpoint is left as future work; the conservative seeding
    /// is sound but misses facts provable only across recursive steps.
    pub equivalences: BTreeMap<usize, Option<EquivalenceClasses>>,
    /// Per-binding constant columns. Recursive bindings are seeded absent
    /// (empty), which is sound: missing knowledge, never wrong knowledge. A full
    /// recursion-aware fixpoint is unnecessary because the analysis only ever
    /// loses facts across a recursive step.
    pub constant_columns: BTreeMap<usize, ConstCols>,
}

/// Solve the recursion fixpoints for a `LetRec`'s `bindings`, given the facts
/// `outer` already known for enclosing bound ids. Returns facts for the bound
/// ids (and the inherited outer ones), ready to seed fragment saturation.
pub fn letrec_local_facts(bindings: &[(usize, Rel)], outer: &LocalFacts) -> LocalFacts {
    // Seed each binding's equivalences conservatively at Some(default). A full
    // recursion-aware fixpoint for EquivalenceClasses is future work; for now we
    // inherit only the outer facts and seed new bindings as unknown.
    let mut equivalences = outer.equivalences.clone();
    for (id, _) in bindings {
        equivalences
            .entry(*id)
            .or_insert_with(|| Some(EquivalenceClasses::default()));
    }
    LocalFacts {
        nonneg: rec_solve(&NonNegRec, bindings, &outer.nonneg),
        monotonic: rec_solve(&MonotonicRec, bindings, &outer.monotonic),
        keys: rec_solve(&KeysRec, bindings, &outer.keys),
        equivalences,
        // Inherit only the outer constant columns; recursive bindings are seeded
        // absent (sound under-approximation). No recursion fixpoint needed.
        constant_columns: outer.constant_columns.clone(),
    }
}

/// Non-negativity as a recursion-aware (greatest-fixpoint) invariant.
struct NonNegRec;
impl RecAnalysis for NonNegRec {
    type Domain = bool;
    fn start(&self) -> bool {
        true
    }
    fn direction(&self) -> Direction {
        Direction::Gfp
    }
    fn make(&self, rel: &Rel, child: &dyn Fn(&Rel) -> bool) -> bool {
        !matches!(rel, Rel::Negate { .. }) && rel.children().iter().all(|c| child(c))
    }
}

/// Monotonicity (insert-only) as a recursion-aware (greatest-fixpoint)
/// invariant: assume the recursive collection is monotone and keep that only if
/// the binding body preserves it. A `Union(base, f(x))` recursion stays
/// monotone; introducing a `Negate(x)` or a `Reduce` over the cycle collapses
/// it to `false`.
struct MonotonicRec;
impl RecAnalysis for MonotonicRec {
    type Domain = bool;
    fn start(&self) -> bool {
        true
    }
    fn direction(&self) -> Direction {
        Direction::Gfp
    }
    fn make(&self, rel: &Rel, child: &dyn Fn(&Rel) -> bool) -> bool {
        match rel {
            Rel::Negate { .. } | Rel::Reduce { .. } | Rel::TopK { .. } => false,
            Rel::Constant { .. } | Rel::Get { .. } => true,
            other => other.children().iter().all(|c| child(c)),
        }
    }
}

/// Non-negativity of a `Rel` (recursion-aware greatest fixpoint; conservative:
/// `Negate`-free, and assumed-then-verified across a `LetRec`).
pub fn rel_non_negative(rel: &Rel) -> bool {
    rec_analyze(&NonNegRec, rel)
}

/// Monotonicity (insert-only) of a `Rel` (recursion-aware greatest fixpoint:
/// no `Negate`/`Reduce` over the recursive cycle).
pub fn rel_monotonic(rel: &Rel) -> bool {
    rec_analyze(&MonotonicRec, rel)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::eqsat::ir::EScalar;
    use mz_expr::MirScalarExpr;

    fn get(name: &str, arity: usize) -> Rel {
        Rel::Get {
            name: name.into(),
            arity,
        }
    }

    fn col0() -> EScalar {
        EScalar::plain(MirScalarExpr::column(0))
    }

    #[mz_ore::test]
    fn recursion_fixpoint_decides_monotonicity() {
        // x = R + Filter(p, x): the recursive collection is insert-only, so the
        // greatest-fixpoint analysis (assume monotone, verify the body) keeps
        // `monotonic` true — and non-negative too.
        let mono_rec = Rel::LetRec {
            bindings: vec![(
                0,
                Rel::Union {
                    base: Box::new(get("R", 2)),
                    inputs: vec![Rel::Filter {
                        predicates: vec![col0()],
                        input: Box::new(Rel::LocalGet {
                            id: 0,
                            arity: 2,
                            get: None,
                            version: None,
                        }),
                    }],
                },
            )],
            limits: vec![None],
            body: Box::new(Rel::LocalGet {
                id: 0,
                arity: 2,
                get: None,
                version: None,
            }),
        };
        assert!(rel_monotonic(&mono_rec));
        assert!(rel_non_negative(&mono_rec));

        // y = R + Negate(y): a retraction over the cycle. The assumption is
        // retracted, so both invariants collapse to false.
        let neg_rec = Rel::LetRec {
            bindings: vec![(
                0,
                Rel::Union {
                    base: Box::new(get("R", 2)),
                    inputs: vec![Rel::Negate {
                        input: Box::new(Rel::LocalGet {
                            id: 0,
                            arity: 2,
                            get: None,
                            version: None,
                        }),
                    }],
                },
            )],
            limits: vec![None],
            body: Box::new(Rel::LocalGet {
                id: 0,
                arity: 2,
                get: None,
                version: None,
            }),
        };
        assert!(!rel_monotonic(&neg_rec));
        assert!(!rel_non_negative(&neg_rec));
    }

    #[mz_ore::test]
    fn monotonic_breaks_under_negate_and_reduce() {
        assert!(rel_monotonic(&get("R", 2)));
        assert!(rel_monotonic(&Rel::Filter {
            predicates: vec![col0()],
            input: Box::new(get("R", 2)),
        }));
        assert!(!rel_monotonic(&Rel::Negate {
            input: Box::new(get("R", 2)),
        }));
        // A Reduce breaks monotonicity even though it preserves non-negativity.
        let reduced = Rel::Reduce {
            input: Box::new(get("R", 2)),
            group_key: vec![col0()],
            aggregates: vec![],
            monotonic: false,
            expected_group_size: None,
        };
        assert!(!rel_monotonic(&reduced));
        assert!(rel_non_negative(&reduced));
    }

    /// A keys/recursion analysis over a LetRec lowered with the lift must produce
    /// the same facts as without the lift, because Prev/Cur/Final are still
    /// references to the same bindings.
    ///
    /// This guards against a regression where versioned gets stop being recognized
    /// as recursive references by the fixpoint driver.
    #[mz_ore::test]
    fn versioned_get_is_recursive_reference() {
        use mz_expr::{AccessStrategy, Id, LocalId, MirRelationExpr, MirScalarExpr};
        use mz_repr::ReprRelationType;

        let typ = ReprRelationType::new(vec![mz_repr::ReprScalarType::Int64.nullable(false)]);
        let local_get = |id: u64| MirRelationExpr::Get {
            id: Id::Local(LocalId::new(id)),
            typ: typ.clone(),
            access_strategy: AccessStrategy::UnknownOrLocal,
        };
        // x = Filter(#0, x): insert-only recursion through a filter. The
        // greatest-fixpoint analysis must prove the collection monotone whether
        // or not the local references carry a RecVersion tag.
        let r = MirRelationExpr::LetRec {
            ids: vec![LocalId::new(0)],
            values: vec![local_get(0).filter(vec![MirScalarExpr::column(0)])],
            limits: vec![None],
            body: Box::new(local_get(0)),
        };
        let off = rel_monotonic(&crate::eqsat::lower::lower_with(&r, false));
        let on = rel_monotonic(&crate::eqsat::lower::lower_with(&r, true));
        assert_eq!(off, on, "versioning must not change analysis facts");
        // Both lowerings must agree that the recursion is monotone.
        assert!(off, "x = Filter(#0, x) must be monotone with lift off");
        assert!(on, "x = Filter(#0, x) must be monotone with lift on");
    }
}

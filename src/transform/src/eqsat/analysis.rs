// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.

//! A small **abstract-interpretation framework** for e-class analyses.
//!
//! Many rewrite side conditions are facts about a relation that no single
//! operator decides locally: non-negativity, unique keys, nullability,
//! monotonicity. Each is a *lattice-valued analysis* attached to every e-class
//! and solved by monotone fixpoint iteration — exactly egg's `Analysis`
//! concept. This module factors that shape out of the hand-rolled
//! `non_negative` pass so new analyses are drop-in.
//!
//! The defining property: because every e-node in a class denotes the *same*
//! relation, the per-class [`Analysis::merge`] combines facts across equivalent
//! forms toward **more precision** (a relation keeps a key proved by *any* of
//! its forms). That is why an e-graph analysis can be sharper than a
//! single-plan one — and it is the same reason `non_negative` asks for *some*
//! `Negate`-free representative.
//!
//! The driver lives on [`crate::eqsat::egraph::EGraph`] as `run_analysis`. The same
//! iteration is what a recursive `LetRec` binding would need (its analysis is a
//! fixpoint over the recursive variable); see `COVERAGE.md`.
//!
//! Per-analysis sub-modules: [`nonneg`][] ([`NonNeg`]), [`monotonic`][]
//! ([`Monotonic`]), [`keys`][] ([`Keys`], [`Key`], [`KeySet`], [`is_superkey`],
//! [`rel_keys`]), [`equivalences`][] ([`Equivalences`]), [`constant_columns`][]
//! ([`ConstantColumns`], [`ConstCols`]), and [`recursion`][] (recursion-aware
//! fixpoint driver: [`rec_analyze`], [`LocalFacts`], [`letrec_local_facts`],
//! [`rel_non_negative`], [`rel_monotonic`]).

mod constant_columns;
mod equivalences;
mod keys;
mod monotonic;
mod nonneg;
pub mod recursion;

pub use constant_columns::{ConstCols, ConstantColumns};
pub use equivalences::Equivalences;
pub use keys::{is_superkey, rel_keys, Key, KeySet, Keys};
pub use monotonic::Monotonic;
pub use nonneg::NonNeg;
pub use recursion::{
    letrec_local_facts, rec_analyze, rel_monotonic, rel_non_negative, Direction, LocalFacts,
};

use crate::eqsat::core::Id;
use crate::eqsat::egraph::CombinedData;

/// The per-run context handed to every relational analysis `make` call: an
/// arity provider plus the combined graph's scalar-fact cache, through which the
/// analyses resolve an `ENode`'s scalar `Id` payloads back to their `EScalar`s.
/// A copyable bundle of shared borrows (`Analysis::Ctx<'a>: Copy`).
#[derive(Clone, Copy)]
pub struct RelCtx<'a> {
    pub arity: &'a dyn Fn(Id) -> usize,
    pub data: &'a CombinedData,
}

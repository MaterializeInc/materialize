// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Core expression language.

#![warn(missing_debug_implementations)]

use std::collections::BTreeSet;
use std::ops::Deref;

use mz_repr::GlobalId;
use serde::{Deserialize, Serialize};

mod id;
mod interpret;
mod linear;
mod relation;
mod scalar;

pub mod explain;
pub mod virtual_syntax;
pub mod visit;

pub use id::{Id, LocalId, ProtoId, ProtoLocalId, SourceInstanceId};
pub use interpret::{ColumnSpec, ColumnSpecs, Interpreter, ResultSpec, Trace, TraceSummary};
pub use linear::plan::{MfpPlan, SafeMfpPlan};
pub use linear::util::{join_permutations, permutation_for_arrangement};
pub use linear::{
    memoize_expr, MapFilterProject, ProtoMapFilterProject, ProtoMfpPlan, ProtoSafeMfpPlan,
};
pub use relation::func::{
    AggregateFunc, AnalyzedRegex, CaptureGroupDesc, LagLeadType, NaiveOneByOneAggr, OneByOneAggr,
    TableFunc,
};
pub use relation::join_input_mapper::JoinInputMapper;
pub use relation::{
    canonicalize, compare_columns, non_nullable_columns, AccessStrategy, AggregateExpr,
    CollectionPlan, ColumnOrder, JoinImplementation, JoinInputCharacteristics, LetRecLimit,
    MirRelationExpr, ProtoAggregateExpr, ProtoAggregateFunc, ProtoColumnOrder,
    ProtoRowSetFinishing, ProtoTableFunc, RowSetFinishing, WindowFrame, WindowFrameBound,
    WindowFrameUnits, RECURSION_LIMIT,
};
pub use scalar::func::{self, BinaryFunc, UnaryFunc, UnmaterializableFunc, VariadicFunc};
pub use scalar::{
    like_pattern, EvalError, FilterCharacteristics, MirScalarExpr, ProtoDomainLimit,
    ProtoEvalError, ProtoMirScalarExpr,
};

/// A [`MirRelationExpr`] that claims to have been optimized, e.g., by an
/// `transform::Optimizer`.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, Hash)]
pub struct OptimizedMirRelationExpr(pub MirRelationExpr);

impl OptimizedMirRelationExpr {
    /// Declare that the input `expr` is optimized, without actually running it
    /// through an optimizer. This can be useful to mark as optimized literal
    /// `MirRelationExpr`s that are obviously optimal, without invoking the whole
    /// machinery of the optimizer.
    pub fn declare_optimized(expr: MirRelationExpr) -> OptimizedMirRelationExpr {
        OptimizedMirRelationExpr(expr)
    }

    /// Get mutable access to the inner [MirRelationExpr]
    ///
    /// Callers of this method need to ensure that the underlying expression stays optimized after
    /// any mutations are applied
    pub fn as_inner(&self) -> &MirRelationExpr {
        &self.0
    }

    /// Get mutable access to the inner [MirRelationExpr]
    ///
    /// Callers of this method need to ensure that the underlying expression stays optimized after
    /// any mutations are applied
    pub fn as_inner_mut(&mut self) -> &mut MirRelationExpr {
        &mut self.0
    }

    pub fn into_inner(self) -> MirRelationExpr {
        self.0
    }
}

impl Deref for OptimizedMirRelationExpr {
    type Target = MirRelationExpr;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl CollectionPlan for OptimizedMirRelationExpr {
    fn depends_on_into(&self, out: &mut BTreeSet<GlobalId>) {
        self.0.depends_on_into(out)
    }
}

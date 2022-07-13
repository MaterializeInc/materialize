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

use serde::{Deserialize, Serialize};

use mz_repr::GlobalId;

mod id;
mod linear;
mod relation;
mod scalar;

pub mod explain;
pub mod visit;

pub use relation::canonicalize;

pub use id::{Id, LocalId, PartitionId, SourceInstanceId};
pub use id::{ProtoId, ProtoLocalId};
pub use linear::{
    memoize_expr,
    plan::{MfpPlan, SafeMfpPlan},
    util::{join_permutations, permutation_for_arrangement},
    MapFilterProject, ProtoMapFilterProject, ProtoMfpPlan, ProtoSafeMfpPlan,
};
pub use relation::func::{AggregateFunc, LagLeadType, TableFunc};
pub use relation::func::{AnalyzedRegex, CaptureGroupDesc};
pub use relation::join_input_mapper::JoinInputMapper;
pub use relation::{
    compare_columns, AggregateExpr, CollectionPlan, ColumnOrder, JoinImplementation,
    MirRelationExpr, ProtoAggregateExpr, RowSetFinishing, WindowFrame, WindowFrameBound,
    WindowFrameUnits, RECURSION_LIMIT,
};
pub use relation::{ProtoAggregateFunc, ProtoColumnOrder, ProtoRowSetFinishing, ProtoTableFunc};
pub use scalar::func::{self, BinaryFunc, UnaryFunc, UnmaterializableFunc, VariadicFunc};
pub use scalar::{like_pattern, EvalError, MirScalarExpr};
pub use scalar::{ProtoDomainLimit, ProtoEvalError, ProtoMirScalarExpr};

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

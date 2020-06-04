// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Core expression language.

#![deny(missing_debug_implementations)]

use serde::{Deserialize, Serialize};

mod id;
mod linear;
mod relation;
mod scalar;

pub mod explain;

pub use id::{DummyHumanizer, GlobalId, Id, IdHumanizer, LocalId, PartitionId, SourceInstanceId};
pub use linear::MapFilterProject;
pub use relation::func::{AggregateFunc, TableFunc};
pub use relation::func::{AnalyzedRegex, CaptureGroupDesc};
pub use relation::{
    compare_columns, AggregateExpr, ColumnOrder, IdGen, JoinImplementation, RelationExpr,
    RowSetFinishing,
};
pub use scalar::func::{BinaryFunc, DateTruncTo, NullaryFunc, UnaryFunc, VariadicFunc};
pub use scalar::{like_pattern, EvalError, ScalarExpr};

/// A [`RelationExpr`] that claims to have been optimized, e.g., by an
/// [`Optimizer`].
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, Hash)]
pub struct OptimizedRelationExpr(pub RelationExpr);

impl OptimizedRelationExpr {
    /// Declare that the input `expr` is optimized, without actually running it
    /// through an optimizer. This can be useful to mark as optimized literal
    /// `RelationExpr`s that are obviously optimal, without invoking the whole
    /// machinery of the optimizer.
    pub fn declare_optimized(expr: RelationExpr) -> OptimizedRelationExpr {
        OptimizedRelationExpr(expr)
    }

    pub fn into_inner(self) -> RelationExpr {
        self.0
    }
}

impl AsRef<RelationExpr> for OptimizedRelationExpr {
    fn as_ref(&self) -> &RelationExpr {
        &self.0
    }
}

impl AsMut<RelationExpr> for OptimizedRelationExpr {
    fn as_mut(&mut self) -> &mut RelationExpr {
        &mut self.0
    }
}

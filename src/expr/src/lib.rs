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

use std::fmt;

use serde::{Deserialize, Serialize};

use repr::{ColumnType, ScalarType};

mod id;
mod linear;
mod relation;
mod scalar;

pub mod explain;

pub use relation::canonicalize;

pub use id::{GlobalId, Id, LocalId, PartitionId, SourceInstanceId};
pub use linear::{memoize_expr, MapFilterProject};
pub use relation::func::{AggregateFunc, TableFunc};
pub use relation::func::{AnalyzedRegex, CaptureGroupDesc};
pub use relation::join_input_mapper::JoinInputMapper;
pub use relation::{
    compare_columns, AggregateExpr, ColumnOrder, IdGen, JoinImplementation, MirRelationExpr,
    RowSetFinishing,
};
pub use scalar::func::{BinaryFunc, NullaryFunc, UnaryFunc, VariadicFunc};
pub use scalar::{like_pattern, EvalError, MirScalarExpr};

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

    pub fn into_inner(self) -> MirRelationExpr {
        self.0
    }
}

impl AsRef<MirRelationExpr> for OptimizedMirRelationExpr {
    fn as_ref(&self) -> &MirRelationExpr {
        &self.0
    }
}

impl AsMut<MirRelationExpr> for OptimizedMirRelationExpr {
    fn as_mut(&mut self) -> &mut MirRelationExpr {
        &mut self.0
    }
}

/// A trait for humanizing components of an expression.
pub trait ExprHumanizer: fmt::Debug {
    /// Attempts to return the a human-readable string for the relation
    /// identified by `id`.
    fn humanize_id(&self, id: GlobalId) -> Option<String>;

    /// Returns a human-readable name for the specified scalar type.
    fn humanize_scalar_type(&self, ty: &ScalarType) -> String;

    /// Returns a human-readable name for the specified scalar type.
    fn humanize_column_type(&self, ty: &ColumnType) -> String;
}

/// A bare-minimum implementation of [`ExprHumanizer`].
///
/// The `DummyHumanizer` does a poor job of humanizing expressions. It is
/// intended for use in contexts where polish is not required, like in tests or
/// while debugging.
#[derive(Debug)]
pub struct DummyHumanizer;

impl ExprHumanizer for DummyHumanizer {
    fn humanize_id(&self, _: GlobalId) -> Option<String> {
        // Returning `None` allows the caller to fall back to displaying the
        // ID, if they so desire.
        None
    }

    fn humanize_scalar_type(&self, ty: &ScalarType) -> String {
        // The debug implementation is better than nothing.
        format!("{:?}", ty)
    }

    fn humanize_column_type(&self, ty: &ColumnType) -> String {
        // The debug implementation is better than nothing.
        format!("{:?}", ty)
    }
}

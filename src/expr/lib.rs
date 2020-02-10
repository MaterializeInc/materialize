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

mod id;
mod relation;
mod scalar;

pub mod like;
pub mod pretty;
pub mod transform;

pub use id::{DummyHumanizer, GlobalId, Id, IdHumanizer, LocalId, SourceInstanceId};
pub use relation::func::{AggregateFunc, UnaryTableFunc};
pub use relation::func::{AnalyzedRegex, CaptureGroupDesc};
pub use relation::{AggregateExpr, ColumnOrder, IdGen, JoinImplementation, RelationExpr};
pub use scalar::func::{BinaryFunc, DateTruncTo, NullaryFunc, UnaryFunc, VariadicFunc};
pub use scalar::{EvalEnv, ScalarExpr};
pub use transform::OptimizedRelationExpr;

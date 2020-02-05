// Copyright 2019 Materialize, Inc. All rights reserved.
//
// This file is part of Materialize. Materialize may not be used or
// distributed without the express permission of Materialize, Inc.

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

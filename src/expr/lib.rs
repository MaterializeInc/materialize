// Copyright 2019 Materialize, Inc. All rights reserved.
//
// This file is part of Materialize. Materialize may not be used or
// distributed without the express permission of Materialize, Inc.

//! Core expression language.

pub mod like;
mod relation;
mod scalar;

pub mod transform;

pub use relation::func::AggregateFunc;
pub use relation::{AggregateExpr, RelationExpr};
pub use scalar::func::{BinaryFunc, UnaryFunc, VariadicFunc};
pub use scalar::ScalarExpr;

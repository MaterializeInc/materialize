// Copyright 2019 Materialize, Inc. All rights reserved.
//
// This file is part of Materialize. Materialize may not be used or
// distributed without the express permission of Materialize, Inc.

//! Fundamental data representation.
//!
//! This module contains the types for representing data in Materialize that all
//! layers of the stack can understand. Think of it as the _lingua franca_:
//! individual layers may use different representations internally, but they all
//! agree to use this representation at their boundaries.
//!
//! The core type is the [`Datum`] enum, which represents a literal value. The
//! most interesting variant is the composite type [`Datum::Tuple`], which is a
//! datum that contains arbitrary other datums. Perhaps surprisingly, this
//! simple composite type is sufficient to represent all composite types that an
//! external data interchange format might provide, like records, unions, and
//! maps.
//!
//! [`Datum`]: repr::Datum
//! [`Datum::Tuple`]: repr::Datum::Tuple

#![deny(missing_debug_implementations)]

mod relation;
mod row;
mod scalar;

pub use relation::{ColumnType, RelationDesc, RelationType};
pub use row::{PackableRow, Row, RowPacker, RowUnpacker, UnpackedRow};
pub use scalar::decimal;
pub use scalar::regex;
pub use scalar::{Datum, Interval, ScalarType};

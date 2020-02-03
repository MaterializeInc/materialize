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
//! * The core value type is the [`Datum`] enum, which represents a literal value.
//! * [`Row`] extends a `Datum` horizontally, and has features for efficiently doing so,
//!   and can be built via a [`RowPacker`].
//! * [`RelationDesc`] describes what it takes to extend a `Row` vertically, and
//!   corresponds most closely to what is returned from querying our dataflows

#![deny(missing_debug_implementations)]

mod relation;
mod row;
mod scalar;

pub use relation::{ColumnName, ColumnType, RelationDesc, RelationType};
pub use row::{datum_size, DatumDict, DatumList, Row, RowArena, RowPacker};
pub use scalar::{decimal, jsonb, regex, strconv};
pub use scalar::{Datum, Interval, ScalarType};

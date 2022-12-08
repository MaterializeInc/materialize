// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Fundamental data representation.
//!
//! This module contains the types for representing data in Materialize that all
//! layers of the stack can understand. Think of it as the _lingua franca_:
//! individual layers may use different representations internally, but they all
//! agree to use this representation at their boundaries.
//!
//! * The core value type is the [`Datum`] enum, which represents a literal value.
//! * [`Row`] extends a `Datum` horizontally, and has features for efficiently
//!   doing so.
//! * [`RelationDesc`] describes what it takes to extend a `Row` vertically, and
//!   corresponds most closely to what is returned from querying our dataflows

#![warn(missing_debug_implementations)]

mod datum_vec;
mod diff;
mod relation;
mod relation_and_scalar;
mod row;
mod scalar;
mod timestamp;

pub mod adt;
pub mod antichain;
pub mod chrono;
pub mod explain_new;
pub mod global_id;
pub mod strconv;
pub mod url;

pub use crate::datum_vec::{DatumVec, DatumVecBorrow};
pub use crate::diff::Diff;
pub use crate::global_id::GlobalId;
pub use crate::relation::{
    ColumnName, ColumnType, NotNullViolation, ProtoColumnName, ProtoColumnType, ProtoRelationDesc,
    ProtoRelationType, RelationDesc, RelationType,
};
pub use crate::row::{
    datum_list_size, datum_size, datums_size, row_size, DatumList, DatumMap, ProtoRow, Row,
    RowArena, RowPacker, RowRef,
};
pub use crate::scalar::{
    arb_datum, AsColumnType, Datum, DatumType, PropArray, PropDatum, PropDict, PropList,
    ProtoScalarType, ScalarBaseType, ScalarType,
};
pub use crate::timestamp::{Timestamp, TimestampManipulation};

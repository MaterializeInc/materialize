// Copyright Materialize, Inc. All rights reserved.
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

#![deny(missing_debug_implementations)]

mod cache;
mod relation;
mod row;
mod scalar;

pub mod adt;
pub mod strconv;

pub use cache::{CachedRecord, CachedRecordIter};
pub use relation::{ColumnName, ColumnType, RelationDesc, RelationType};
pub use row::{
    datum_list_size, datum_size, datums_size, DatumList, DatumMap, Row, RowArena, RowRef,
};
pub use scalar::{Datum, ScalarBaseType, ScalarType};

// Concrete types used throughout Materialize for the generic parameters in Timely/Differential Dataflow.
/// System-wide timestamp type.
pub type Timestamp = u64;
/// System-wide record count difference type.
pub type Diff = isize;

use serde::{Deserialize, Serialize};
// This probably logically belongs in `dataflow`, but source caching looks at it,
// so put it in `repr` instead.
/// The payload delivered by a source connector; either bytes or an EOF marker.
#[derive(Clone, Debug, Serialize, Deserialize, Eq, Hash, PartialEq, Ord, PartialOrd)]
pub enum MessagePayload {
    /// Data from the source connector
    Data(Vec<u8>),
    /// Forces the decoder to consider this a delimiter.
    ///
    /// For example, CSV records are normally terminated by a newline,
    /// but files might not be newline-terminated; thus we need
    /// the decoder to emit a CSV record when the end of a file is seen.
    EOF,
}

impl Default for MessagePayload {
    fn default() -> Self {
        Self::Data(vec![])
    }
}

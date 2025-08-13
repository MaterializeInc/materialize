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
// TODO(parkmycar): Remove this allow.
#![allow(unsafe_op_in_unsafe_fn)]

mod datum_vec;
mod diff;
mod relation;
mod relation_and_scalar;
mod row;
mod scalar;

pub mod adt;
pub mod antichain;
pub mod bytes;
pub mod catalog_item_id;
pub mod explain;
pub mod fixed_length;
pub mod global_id;
pub mod namespaces;
pub mod network_policy_id;
pub mod optimize;
pub mod refresh_schedule;
pub mod role_id;
pub mod stats;
pub mod strconv;
pub mod timestamp;
pub mod url;
pub mod user;

pub use crate::catalog_item_id::CatalogItemId;
pub use crate::datum_vec::{DatumVec, DatumVecBorrow};
pub use crate::diff::Diff;
pub use crate::global_id::GlobalId;
pub use crate::relation::{
    ColumnIndex, ColumnName, NotNullViolation, PropRelationDescDiff, ProtoColumnName,
    ProtoColumnType, ProtoRelationDesc, ProtoRelationType, RelationDesc, RelationDescBuilder,
    RelationVersion, RelationVersionSelector, SqlColumnType, SqlRelationType,
    VersionedRelationDesc, arb_relation_desc_diff, arb_relation_desc_projection,
    arb_row_for_relation,
};
pub use crate::row::encode::{RowColumnarDecoder, RowColumnarEncoder, preserves_order};
pub use crate::row::iter::{IntoRowIterator, RowIterator};
pub use crate::row::{
    DatumList, DatumMap, ProtoNumeric, ProtoRow, Row, RowArena, RowPacker, RowRef, SharedRow,
    datum_list_size, datum_size, datums_size, read_datum, row_size,
};
pub use crate::scalar::{
    ArrayRustType, AsColumnType, Datum, DatumType, PropArray, PropDatum, PropDict, PropList,
    ProtoScalarType, SqlScalarBaseType, SqlScalarType, arb_datum, arb_datum_for_column,
    arb_datum_for_scalar, arb_range_type,
};
pub use crate::timestamp::{Timestamp, TimestampManipulation};

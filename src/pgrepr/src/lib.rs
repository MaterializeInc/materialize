// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Representation of and serialization for PostgreSQL datums.
//!
//! This crate exports a [`Value`] type that maps directly to a PostgreSQL
//! datum. These values can be serialized using either the text or binary
//! encoding format; see the [`Format`] type for details.
//!
//! `Value`s are easily converted to and from [`repr::Datum`]s. See, for
//! example, the [`values_from_row`] function.

#![deny(clippy::as_conversions)]
#![deny(missing_docs)]

mod format;
mod types;
mod value;

pub mod oid;

pub use format::Format;
pub use types::{Type, LIST, MAP, RDN as RDNType};
pub use value::interval::Interval;
pub use value::jsonb::Jsonb;
pub use value::numeric::Numeric;
pub use value::rdn::RDN as RDNValue;
pub use value::record::Record;
pub use value::{null_datum, values_from_row, Value};

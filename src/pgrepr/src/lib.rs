// Copyright 2019-2020 Materialize Materialize, Inc. All rights reserved.
//
// This file is part of Materialize. Materialize may not be used or
// distributed without the express permission of Materialize, Inc.

//! Representation of and serialization for PostgreSQL datums.
//!
//! This crate exports a [`Value`] type that maps directly to a PostgreSQL
//! datum. These values can be serialized using either the text or binary
//! encoding format; see the [`Format`] type for details.
//!
//! `Value`s are easily converted to and from [`repr::Datum`]s. See, for
//! example, the [`values_from_row`] function.

#![forbid(missing_docs)]

mod format;
mod types;
mod value;

pub use format::Format;
pub use types::Type;
pub use value::interval::Interval;
pub use value::numeric::Numeric;
pub use value::{null_datum, values_from_row, Value};

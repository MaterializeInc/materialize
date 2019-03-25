// Copyright 2019 Timely Data, Inc. All rights reserved.
//
// This file is part of Materialize. Materialize may not be used or
// distributed without the express permission of Timely Data, Inc.

//! The fundamental representation of data in Materialize.
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

use serde::{Serialize, Deserialize};
use std::cmp::Ordering;

/// A literal value.
///
/// Note that datums may be scalar, like [`Datum::Int32`], or composite, like
/// [`Datum::Tuple`], but they are always constant.
#[serde(rename_all = "snake_case")]
#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Serialize, Deserialize)]
pub enum Datum {
    /// An unknown value.
    Null,
    /// The `true` boolean value.
    True,
    /// The `false` boolean value.
    False,
    /// A 32-bit signed integer.
    Int32(i32),
    /// A 64-bit signed integer.
    Int64(i64),
    /// A 32-bit floating point number.
    Float32(F32),
    /// A 64-bit floating point number.
    Float64(F64),
    /// A sequence of untyped bytes.
    Bytes(Vec<u8>),
    /// A sequence of Unicode codepoints encoded as UTF-8.
    String(String),
    /// A [`Tuple`].
    Tuple(Tuple),
}

/// An ordered, unnamed collection of heterogeneous [`Datum`]s.
pub type Tuple = Vec<Datum>;

/// A description of the structure of a [`Datum`].
///
/// [`Schema`] bundles the [`Type`] of a datum (e.g., Int32 or String) with
/// additional attributes, like its default value and its nullability.
///
/// It is not possible to construct a `Schema` directly from a `Datum`, as it is
/// impossible to determine anything but the datum's `Type`. Consider: a naked
/// `Datum` provides no information about its name, default value, or
/// nullability.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct Schema {
    /// The name of this datum. Perhaps surprisingly, expressions in SQL can
    /// have names, as in `SELECT 1 AS blah`.
    pub name: Option<String>,
    /// Whether this datum can be null.
    pub nullable: bool,
    /// The type (e.g., Int32 or String) of this datum.
    pub typ: Type,
}

/// The fundamental type of a [`Datum`].
#[serde(rename_all = "snake_case")]
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum Type {
    Unknown,
    /// The type of a datum that can only be null.
    ///
    /// This is uncommon. Most [`Datum:Null`]s appear with a different type.
    Null,
    Bool,
    Int32,
    Int64,
    Float32,
    Float64,
    Bytes,
    String,
    Tuple(Vec<Schema>),
    Array(Box<Schema>),
}

/// A 32-bit floating integer that implements [`Eq`] and [`Ord`].
///
/// The transitivity of the equivalence relation is provided by treating NaN
/// (not-a-number) as equal to itself, in violation of the IEEE 754 floating
/// point standard. A total ordering is provided by treating NaN as greater than
/// all other values. This non-standard behavior is necessary to allow NaNs to
/// be sorted and stored in indices, and is shared with (at least) PostgreSQL.
#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
pub struct F32(pub f32);

impl Eq for F32 {}

impl PartialEq for F32 {
    fn eq(&self, other: &F32) -> bool {
        (self.0.is_nan() && other.0.is_nan()) || (self.0 == other.0)
    }
}

impl Ord for F32 {
    fn cmp(&self, other: &F32) -> Ordering {
        match self.0.partial_cmp(&other.0) {
            Some(ordering) => ordering,
            None => match (self.0.is_nan(), other.0.is_nan()) {
                (true, true) => Ordering::Equal,
                (true, false) => Ordering::Greater,
                (false, true) => Ordering::Less,
                (false, false) => unreachable!(),
            }
        }
    }
}

impl PartialOrd for F32 {
    fn partial_cmp(&self, other: &F32) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

/// A 64-bit floating integer that implements [`Eq`] and [`Ord`].
///
/// See [`F32`] for details.
#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
pub struct F64(pub f64);

impl Eq for F64 {}

impl PartialEq for F64 {
    fn eq(&self, other: &F64) -> bool {
        (self.0.is_nan() && other.0.is_nan()) || (self.0 == other.0)
    }
}

impl Ord for F64 {
    fn cmp(&self, other: &F64) -> Ordering {
        match self.0.partial_cmp(&other.0) {
            Some(ordering) => ordering,
            None => match (self.0.is_nan(), other.0.is_nan()) {
                (true, true) => Ordering::Equal,
                (true, false) => Ordering::Greater,
                (false, true) => Ordering::Less,
                (false, false) => unreachable!(),
            }
        }
    }
}

impl PartialOrd for F64 {
    fn partial_cmp(&self, other: &F64) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_f32_ord() {
        let nan = F32(std::f32::NAN);

        assert!(nan == nan);
        assert!(nan > F32(1.0));
        assert!(F32(1.0) < nan);
        assert!(F32(1.0) == F32(1.0));
    }

    #[test]
    fn test_f64_ord() {
        let nan = F64(std::f64::NAN);

        assert!(nan == nan);
        assert!(nan > F64(1.0));
        assert!(F64(1.0) < nan);
        assert!(F64(1.0) == F64(1.0));
    }
}
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

pub mod decimal;

mod regex;

use failure::bail;
use ordered_float::OrderedFloat;
use serde::{Deserialize, Serialize};

use self::decimal::Significand;
use self::regex::Regex;

/// A literal value.
///
/// Note that datums may be scalar, like [`Datum::Int32`], or composite, like
/// [`Datum::Tuple`], but they are always constant.
#[serde(rename_all = "snake_case")]
#[derive(Clone, Debug, Eq, PartialEq, Hash, Ord, PartialOrd, Serialize, Deserialize)]
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
    Float32(OrderedFloat<f32>),
    /// A 64-bit floating point number.
    Float64(OrderedFloat<f64>),
    /// An exact decimal number, possibly with a fractional component, with up
    /// to 38 digits of precision.
    Decimal(Significand),
    /// A sequence of untyped bytes.
    Bytes(Vec<u8>),
    /// A sequence of Unicode codepoints encoded as UTF-8.
    String(String),
    /// A compiled regular expression.
    Regex(Regex),
}

impl Datum {
    pub fn is_null(&self) -> bool {
        match self {
            Datum::Null => true,
            _ => false,
        }
    }

    pub fn unwrap_bool(&self) -> bool {
        match self {
            Datum::False => false,
            Datum::True => true,
            _ => panic!("Datum::unwrap_bool called on {:?}", self),
        }
    }

    pub fn unwrap_int32(&self) -> i32 {
        match self {
            Datum::Int32(i) => *i,
            _ => panic!("Datum::unwrap_int32 called on {:?}", self),
        }
    }

    pub fn unwrap_int64(&self) -> i64 {
        match self {
            Datum::Int64(i) => *i,
            _ => panic!("Datum::unwrap_int64 called on {:?}", self),
        }
    }

    pub fn unwrap_ordered_float32(&self) -> OrderedFloat<f32> {
        match self {
            Datum::Float32(f) => *f,
            _ => panic!("Datum::unwrap_ordered_float32 called on {:?}", self),
        }
    }

    pub fn unwrap_ordered_float64(&self) -> OrderedFloat<f64> {
        match self {
            Datum::Float64(f) => *f,
            _ => panic!("Datum::unwrap_ordered_float64 called on {:?}", self),
        }
    }

    pub fn unwrap_float32(&self) -> f32 {
        match self {
            Datum::Float32(f) => f.into_inner(),
            _ => panic!("Datum::unwrap_float32 called on {:?}", self),
        }
    }

    pub fn unwrap_float64(&self) -> f64 {
        match self {
            Datum::Float64(f) => f.into_inner(),
            _ => panic!("Datum::unwrap_float64 called on {:?}", self),
        }
    }

    pub fn unwrap_decimal(&self) -> Significand {
        match self {
            Datum::Decimal(d) => *d,
            _ => panic!("Datum::unwrap_decimal called on {:?}", self),
        }
    }

    pub fn unwrap_str(&self) -> &str {
        match self {
            Datum::String(s) => s,
            _ => panic!("Datum::unwrap_string called on {:?}", self),
        }
    }

    pub fn unwrap_string(self) -> String {
        match self {
            Datum::String(s) => s,
            _ => panic!("Datum::unwrap_string called on {:?}", self),
        }
    }

    pub fn unwrap_regex(self) -> Regex {
        match self {
            Datum::Regex(r) => r,
            _ => panic!("Datum::unwrap_regex calloed on {:?}", self),
        }
    }

    pub fn is_instance_of(&self, column_type: &ColumnType) -> bool {
        match (self, &column_type.scalar_type) {
            (Datum::Null, _) if column_type.nullable => true,
            (Datum::False, ScalarType::Bool) => true,
            (Datum::True, ScalarType::Bool) => true,
            (Datum::Int32(_), ScalarType::Int32) => true,
            (Datum::Int64(_), ScalarType::Int64) => true,
            (Datum::Float32(_), ScalarType::Float32) => true,
            (Datum::Float64(_), ScalarType::Float64) => true,
            (Datum::Decimal(_), ScalarType::Decimal(_, _)) => true,
            (Datum::Bytes(_), ScalarType::Bytes) => true,
            (Datum::String(_), ScalarType::String) => true,
            (Datum::Regex(_), ScalarType::Regex) => true,
            _ => false,
        }
    }
}

impl From<bool> for Datum {
    fn from(b: bool) -> Datum {
        if b {
            Datum::True
        } else {
            Datum::False
        }
    }
}

impl From<i32> for Datum {
    fn from(i: i32) -> Datum {
        Datum::Int32(i)
    }
}

impl From<i64> for Datum {
    fn from(i: i64) -> Datum {
        Datum::Int64(i)
    }
}

impl From<OrderedFloat<f32>> for Datum {
    fn from(f: OrderedFloat<f32>) -> Datum {
        Datum::Float32(f)
    }
}

impl From<OrderedFloat<f64>> for Datum {
    fn from(f: OrderedFloat<f64>) -> Datum {
        Datum::Float64(f)
    }
}

impl From<f32> for Datum {
    fn from(f: f32) -> Datum {
        Datum::Float32(OrderedFloat(f))
    }
}

impl From<f64> for Datum {
    fn from(f: f64) -> Datum {
        Datum::Float64(OrderedFloat(f))
    }
}

impl From<i128> for Datum {
    fn from(d: i128) -> Datum {
        Datum::Decimal(Significand::new(d))
    }
}

impl From<Significand> for Datum {
    fn from(d: Significand) -> Datum {
        Datum::Decimal(d)
    }
}

impl From<String> for Datum {
    fn from(s: String) -> Datum {
        Datum::String(s)
    }
}

impl From<::regex::Regex> for Datum {
    fn from(r: ::regex::Regex) -> Datum {
        Datum::Regex(Regex(r))
    }
}

impl From<Vec<u8>> for Datum {
    fn from(b: Vec<u8>) -> Datum {
        Datum::Bytes(b)
    }
}

impl<T> From<Option<T>> for Datum
where
    Datum: From<T>,
{
    fn from(o: Option<T>) -> Datum {
        if let Some(d) = o {
            d.into()
        } else {
            Datum::Null
        }
    }
}

/// The fundamental type of a [`Datum`].
///
/// A fundamental type is what is typically thought of as a type, like "Int32"
/// or "String." The full [`ColumnType`] struct bundles additional information, like
/// an optional default value and nullability, that must also be considered part
/// of a datum's type.
#[serde(rename_all = "snake_case")]
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, Hash)]
pub enum ScalarType {
    /// The type of a datum that can only be null.
    ///
    /// This is uncommon. Most [`Datum:Null`]s appear with a different type.
    Null,
    Bool,
    Int32,
    Int64,
    Float32,
    Float64,
    /// An exact decimal number with a specified precision and scale. The
    /// precision constrains the total number of digits in the number, while the
    /// scale specifies the number of digits after the decimal point. The
    /// maximum precision is [`decimal::MAX_DECIMAL_PRECISION`]. The scale must
    /// be less than or equal to the precision.
    Decimal(u8, u8),
    Date,
    Time,
    Timestamp,
    Bytes,
    String,
    Regex,
}

impl ScalarType {
    pub fn unwrap_decimal_parts(&self) -> (u8, u8) {
        match self {
            ScalarType::Decimal(p, s) => (*p, *s),
            _ => panic!("ScalarType::unwrap_decimal_parts called on {:?}", self),
        }
    }
}

/// The type of a [`Datum`].
///
/// [`ColumnType`] bundles information about the scalar type of a datum (e.g.,
/// Int32 or String) with additional attributes, like its name and its
/// nullability.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, Hash)]
pub struct ColumnType {
    /// The name of this datum. Perhaps surprisingly, expressions in SQL can
    /// have names, as in `SELECT 1 AS blah`.
    pub name: Option<String>,
    /// Whether this datum can be null.
    pub nullable: bool,
    /// The underlying scalar type (e.g., Int32 or String) of this column.
    pub scalar_type: ScalarType,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, Hash)]
pub struct RelationType {
    pub column_types: Vec<ColumnType>,
}

impl RelationType {
    pub fn new(column_types: Vec<ColumnType>) -> Self {
        RelationType { column_types }
    }
}

impl ColumnType {
    /// Constructs a new `ColumnType` with the specified [`ScalarType`] as its
    /// underlying type. If desired, the `name` and `nullable` properties can
    /// be set with the methods of the same name.
    pub fn new(scalar_type: ScalarType) -> Self {
        ColumnType {
            name: None,
            nullable: false,
            scalar_type,
        }
    }

    pub fn union(&self, other: &Self) -> Result<Self, failure::Error> {
        let scalar_type = match (&self.scalar_type, &other.scalar_type) {
            (ScalarType::Null, s) | (s, ScalarType::Null) => s,
            (s1, s2) if s1 == s2 => s1,
            (s1, s2) => bail!("Can't union types: {:?} and {:?}", s1, s2),
        };
        Ok(ColumnType {
            // column names are taken from left, as in postgres
            name: self.name.clone(),
            scalar_type: scalar_type.clone(),
            nullable: self.nullable
                || other.nullable
                || self.scalar_type == ScalarType::Null
                || other.scalar_type == ScalarType::Null,
        })
    }

    /// Consumes this `ColumnType` and returns a new `ColumnType` with its name
    /// set to the specified string.
    pub fn name<T: Into<String>>(mut self, name: T) -> Self {
        self.name = Some(name.into());
        self
    }

    /// Consumes this `ColumnType` and returns a new `ColumnType` with its
    /// nullability set to the specified boolean.
    pub fn nullable(mut self, nullable: bool) -> Self {
        self.nullable = nullable;
        self
    }
}

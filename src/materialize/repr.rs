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

use failure::bail;
use ordered_float::OrderedFloat;
use serde::{Deserialize, Serialize};

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
    /// A sequence of untyped bytes.
    Bytes(Vec<u8>),
    /// A sequence of Unicode codepoints encoded as UTF-8.
    String(String),
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

    pub fn scalar_type(&self) -> ScalarType {
        match self {
            Datum::Null => ScalarType::Null,
            Datum::False => ScalarType::Bool,
            Datum::True => ScalarType::Bool,
            Datum::Int32(_) => ScalarType::Int32,
            Datum::Int64(_) => ScalarType::Int64,
            Datum::Float32(_) => ScalarType::Float32,
            Datum::Float64(_) => ScalarType::Float64,
            Datum::Bytes(_) => ScalarType::Bytes,
            Datum::String(_) => ScalarType::String,
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

impl From<String> for Datum {
    fn from(s: String) -> Datum {
        Datum::String(s)
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
    Decimal(usize, usize),
    Date,
    Time,
    Timestamp,
    Bytes,
    String,
}

/// The type of a [`Datum`].
///
/// [`ColumnType`] bundles information about the fundamental type of a datum (e.g.,
/// Int32 or String) with additional attributes, like its default value and its
/// nullability.
///
/// It is not possible to construct a `ColumnType` directly from a `Datum`, as it is
/// impossible to determine anything but the type's `scalar_type`. Consider: a naked
/// `Datum` provides no information about its name, default value, or
/// nullability.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, Hash)]
pub struct ColumnType {
    /// The name of this datum. Perhaps surprisingly, expressions in SQL can
    /// have names, as in `SELECT 1 AS blah`.
    pub name: Option<String>,
    /// Whether this datum can be null.
    pub nullable: bool,
    /// The fundamental type (e.g., Int32 or String) of this datum.
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

impl Datum {
    pub fn is_instance_of(&self, column_typ: &ColumnType) -> bool {
        self.scalar_type().is_instance_of(column_typ)
    }
}

impl ScalarType {
    pub fn is_instance_of(&self, column_typ: &ColumnType) -> bool {
        self == &column_typ.scalar_type || (self == &ScalarType::Null && column_typ.nullable)
    }
}

impl ColumnType {
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

    pub fn name(mut self, name: String) -> Self {
        self.name = Some(name);
        self
    }

    pub fn nullable(mut self) -> Self {
        self.nullable = true;
        self
    }
}

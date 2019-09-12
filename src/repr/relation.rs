// Copyright 2019 Materialize, Inc. All rights reserved.
//
// This file is part of Materialize. Materialize may not be used or
// distributed without the express permission of Materialize, Inc.

use failure::bail;
use serde::{Deserialize, Serialize};

use crate::ScalarType;

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

impl ColumnType {
    /// Constructs a new `ColumnType` with the specified [`ScalarType`] as its
    /// underlying type. If desired, the `name` and `nullable` properties can
    /// be set with the methods of the same name.
    pub fn new(scalar_type: ScalarType) -> Self {
        ColumnType {
            name: None,
            nullable: if let ScalarType::Null = scalar_type {
                true
            } else {
                false
            },
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

/// The type for a relation
///
/// aka a View, this is a vec of [`ColumnType`]
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, Hash)]
pub struct RelationType {
    pub column_types: Vec<ColumnType>,
}

impl RelationType {
    pub fn new(column_types: Vec<ColumnType>) -> Self {
        RelationType { column_types }
    }
}

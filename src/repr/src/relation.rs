// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::fmt;
use std::iter;
use std::vec;

use failure::bail;
use serde::{Deserialize, Serialize};

use crate::ScalarType;

/// The type of a [`Datum`](crate::Datum).
///
/// [`ColumnType`] bundles information about the scalar type of a datum (e.g.,
/// Int32 or String) with additional attributes, like its nullability.
#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Serialize, Deserialize, Hash)]
pub struct ColumnType {
    /// Whether this datum can be null.
    pub nullable: bool,
    /// The underlying scalar type (e.g., Int32 or String) of this column.
    pub scalar_type: ScalarType,
}

impl ColumnType {
    /// Constructs a new `ColumnType` with the specified [`ScalarType`] as its
    /// underlying type. If desired, the `nullable` property can be set with the
    /// methods of the same name.
    pub fn new(scalar_type: ScalarType) -> Self {
        ColumnType {
            nullable: false,
            scalar_type,
        }
    }

    pub fn union(&self, other: &Self) -> Result<Self, failure::Error> {
        if self.scalar_type != other.scalar_type {
            bail!(
                "Can't union types: {:?} and {:?}",
                self.scalar_type,
                other.scalar_type
            );
        }
        Ok(ColumnType {
            scalar_type: self.scalar_type.clone(),
            nullable: self.nullable || other.nullable,
        })
    }

    /// Consumes this `ColumnType` and returns a new `ColumnType` with its
    /// nullability set to the specified boolean.
    pub fn nullable(mut self, nullable: bool) -> Self {
        self.nullable = nullable;
        self
    }

    /// Required outside of builder contexts.
    pub fn set_nullable(&mut self, nullable: bool) {
        self.nullable = nullable
    }
}

impl fmt::Display for ColumnType {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "{}{}",
            self.scalar_type,
            if self.nullable { "?" } else { "" }
        )
    }
}

/// The type for a relation.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, Hash)]
pub struct RelationType {
    /// The type for each column, in order.
    pub column_types: Vec<ColumnType>,
    /// Sets of indices that are "keys" for the collection.
    ///
    /// Each element in this list is a set of column indices, each with the property
    /// that the collection contains at most one record with each distinct set of values
    /// for each column. Alternately, for a specific set of values assigned to the these
    /// columns there is at most one record.
    ///
    /// A collection can contain multiple sets of keys, although it is common to have
    /// either zero or one sets of key indices.
    pub keys: Vec<Vec<usize>>,
}

impl RelationType {
    /// Creates a relation type representing the relation with no columns.
    pub fn empty() -> Self {
        RelationType::new(vec![])
    }

    /// Creates a new instance from specified column types.
    pub fn new(column_types: Vec<ColumnType>) -> Self {
        RelationType {
            column_types,
            keys: Vec::new(),
        }
    }

    /// Adds a set of indices as keys for the colleciton.
    pub fn add_keys(mut self, mut indices: Vec<usize>) -> Self {
        indices.sort();
        if !self.keys.contains(&indices) {
            self.keys.push(indices);
        }
        self
    }

    /// The number of columns in the relation type.
    pub fn arity(&self) -> usize {
        self.column_types.len()
    }
}

/// The name of a column in a [`RelationDesc`].
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, Hash)]
pub struct ColumnName(String);

impl ColumnName {
    /// Returns this column name as a `str`.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for ColumnName {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl From<String> for ColumnName {
    fn from(s: String) -> ColumnName {
        ColumnName(s)
    }
}

impl From<&str> for ColumnName {
    fn from(s: &str) -> ColumnName {
        ColumnName(s.into())
    }
}

impl From<&ColumnName> for ColumnName {
    fn from(n: &ColumnName) -> ColumnName {
        n.clone()
    }
}

/// A complete relation description. Bundles together a `RelationType` with
/// additional metadata, like the names of each column.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, Hash)]
pub struct RelationDesc {
    typ: RelationType,
    names: Vec<Option<ColumnName>>,
}

impl RelationDesc {
    /// Constructs a new `RelationDesc` that represents a relation with no
    /// columns and no keys.
    pub fn empty() -> RelationDesc {
        RelationDesc {
            typ: RelationType::empty(),
            names: vec![],
        }
    }

    /// Constructs a new `RelationDesc` from a `RelationType` and a list of
    /// column names.
    pub fn new<I, N>(typ: RelationType, names: I) -> RelationDesc
    where
        I: IntoIterator<Item = Option<N>>,
        N: Into<ColumnName>,
    {
        let names: Vec<_> = names.into_iter().map(|n| n.map(Into::into)).collect();
        assert_eq!(typ.column_types.len(), names.len());
        RelationDesc { typ, names }
    }

    pub fn from_cols(cols: Vec<(ColumnType, Option<String>)>) -> RelationDesc {
        Self::new(
            RelationType::new(cols.iter().map(|(typ, _)| typ.clone()).collect()),
            cols.into_iter().map(|(_, name)| name),
        )
    }

    pub fn concat(mut self, other: Self) -> Self {
        let self_len = self.typ.column_types.len();
        self.names.extend(other.names);
        self.typ.column_types.extend(other.typ.column_types);
        for k in other.typ.keys {
            let k = k.into_iter().map(|idx| idx + self_len).collect();
            self = self.add_keys(k);
        }
        self
    }

    pub fn add_cols<I, N>(&mut self, cols: I)
    where
        I: IntoIterator<Item = (Option<N>, ColumnType)>,
        N: Into<ColumnName>,
    {
        for (name, typ) in cols.into_iter() {
            self.typ.column_types.push(typ);
            self.names.push(name.map(Into::into));
        }
    }

    /// Adds a new named, nonnullable column with the specified scalar type.
    pub fn add_nonnull_column<N>(self, name: N, scalar_type: ScalarType) -> RelationDesc
    where
        N: Into<ColumnName>,
    {
        self.add_column(name, ColumnType::new(scalar_type))
    }

    /// Adds a new named column with the specified column type.
    pub fn add_column<N>(mut self, name: N, column_type: ColumnType) -> RelationDesc
    where
        N: Into<ColumnName>,
    {
        self.typ.column_types.push(column_type);
        self.names.push(Some(name.into()));
        self
    }

    /// Adds a set of indices as keys for the relation.
    pub fn add_keys(mut self, mut indices: Vec<usize>) -> Self {
        indices.sort();
        if !self.typ.keys.contains(&indices) {
            self.typ.keys.push(indices);
        }
        self
    }

    /// Deletes all keys for the relation
    pub fn clear_keys(&mut self) {
        self.typ.keys.clear()
    }

    pub fn typ(&self) -> &RelationType {
        &self.typ
    }

    pub fn iter(&self) -> impl Iterator<Item = (Option<&ColumnName>, &ColumnType)> {
        self.iter_names().zip(self.iter_types())
    }

    pub fn iter_types(&self) -> impl Iterator<Item = &ColumnType> {
        self.typ.column_types.iter()
    }

    pub fn iter_names(&self) -> impl Iterator<Item = Option<&ColumnName>> {
        self.names.iter().map(|n| n.as_ref())
    }

    pub fn get_by_name(&self, name: &ColumnName) -> Option<(usize, &ColumnType)> {
        self.iter_names()
            .position(|n| n == Some(name))
            .map(|i| (i, &self.typ.column_types[i]))
    }

    pub fn get_unambiguous_name(&self, i: usize) -> Option<&ColumnName> {
        let name = self.names[i].as_ref();
        if self.iter_names().filter(|n| n == &name).count() == 1 {
            name
        } else {
            None
        }
    }
}

impl IntoIterator for RelationDesc {
    type Item = (Option<ColumnName>, ColumnType);
    type IntoIter = iter::Zip<vec::IntoIter<Option<ColumnName>>, vec::IntoIter<ColumnType>>;

    fn into_iter(self) -> Self::IntoIter {
        self.names.into_iter().zip(self.typ.column_types)
    }
}

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

use anyhow::bail;
use serde::{Deserialize, Serialize};

use crate::ScalarType;

/// The type of a [`Datum`](crate::Datum).
///
/// [`ColumnType`] bundles information about the scalar type of a datum (e.g.,
/// Int32 or String) with its nullability.
///
/// To construct a column type, either initialize the struct directly, or
/// use the [`ScalarType::nullable`] method.
#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Serialize, Deserialize, Hash)]
pub struct ColumnType {
    /// Whether this datum can be null.
    pub nullable: bool,
    /// The underlying scalar type (e.g., Int32 or String) of this column.
    pub scalar_type: ScalarType,
}

impl ColumnType {
    pub fn union(&self, other: &Self) -> Result<Self, anyhow::Error> {
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
}

/// The type of a relation.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, Hash)]
pub struct RelationType {
    /// The type for each column, in order.
    pub column_types: Vec<ColumnType>,
    /// Sets of indices that are "keys" for the collection.
    ///
    /// Each element in this list is a set of column indices, each with the
    /// property that the collection contains at most one record with each
    /// distinct set of values for each column. Alternately, for a specific set
    /// of values assigned to the these columns there is at most one record.
    ///
    /// A collection can contain multiple sets of keys, although it is common to
    /// have either zero or one sets of key indices.
    pub keys: Vec<Vec<usize>>,
}

impl RelationType {
    /// Constructs a `RelationType` representing the relation with no columns and
    /// no keys.
    pub fn empty() -> Self {
        RelationType::new(vec![])
    }

    /// Constructs a new `RelationType` from specified column types.
    ///
    /// The `RelationType` will have no keys.
    pub fn new(column_types: Vec<ColumnType>) -> Self {
        RelationType {
            column_types,
            keys: Vec::new(),
        }
    }

    /// Adds a new key for the relation.
    pub fn with_key(mut self, mut indices: Vec<usize>) -> Self {
        indices.sort_unstable();
        if !self.keys.contains(&indices) {
            self.keys.push(indices);
        }
        self
    }

    pub fn with_keys(mut self, keys: Vec<Vec<usize>>) -> Self {
        for key in keys {
            self = self.with_key(key)
        }
        self
    }

    /// Computes the number of columns in the relation.
    pub fn arity(&self) -> usize {
        self.column_types.len()
    }

    /// Gets the index of the columns used when creating a default index.
    pub fn default_key(&self) -> Vec<usize> {
        if let Some(key) = self.keys.first() {
            if key.is_empty() {
                (0..self.column_types.len()).collect()
            } else {
                key.clone()
            }
        } else {
            (0..self.column_types.len()).collect()
        }
    }
}

/// The name of a column in a [`RelationDesc`].
#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Serialize, Deserialize, Hash)]
pub struct ColumnName(String);

impl ColumnName {
    /// Returns this column name as a `str`.
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Returns a mutable reference to the string underlying this column name.
    pub fn as_mut_str(&mut self) -> &mut String {
        &mut self.0
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

/// A description of the shape of a relation.
///
/// It bundles a [`RelationType`] with the name of each column in the relation.
/// Individual column names are optional.
///
/// # Examples
///
/// A `RelationDesc`s is typically constructed via its builder API:
///
/// ```
/// use repr::{ColumnType, RelationDesc, ScalarType};
///
/// let desc = RelationDesc::empty()
///     .with_column("id", ScalarType::Int64.nullable(false))
///     .with_column("price", ScalarType::Float64.nullable(true));
/// ```
///
/// In more complicated cases, like when constructing a `RelationDesc` in
/// response to user input, it may be more convenient to construct a relation
/// type first, and imbue it with column names to form a `RelationDesc` later:
///
/// ```
/// use repr::RelationDesc;
///
/// # fn plan_query(_: &str) -> repr::RelationType { repr::RelationType::new(vec![]) }
/// let relation_type = plan_query("SELECT * FROM table");
/// let names = (0..relation_type.arity()).map(|i| match i {
///     0 => Some("first"),
///     1 => Some("second"),
///     // Leave the rest of the columns unnamed.
///     _ => None,
/// });
/// let desc = RelationDesc::new(relation_type, names);
/// ```
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, Hash)]
pub struct RelationDesc {
    typ: RelationType,
    names: Vec<Option<ColumnName>>,
}

impl RelationDesc {
    /// Constructs a new `RelationDesc` that represents the empty relation
    /// with no columns and no keys.
    pub fn empty() -> Self {
        RelationDesc {
            typ: RelationType::empty(),
            names: vec![],
        }
    }

    /// Constructs a new `RelationDesc` from a `RelationType` and an iterator
    /// over column names.
    ///
    /// # Panics
    ///
    /// Panics if the arity of the `RelationType` is not equal to the number of
    /// items in `names`.
    pub fn new<I, N>(typ: RelationType, names: I) -> Self
    where
        I: IntoIterator<Item = Option<N>>,
        N: Into<ColumnName>,
    {
        let names: Vec<_> = names.into_iter().map(|n| n.map(Into::into)).collect();
        assert_eq!(typ.column_types.len(), names.len());
        RelationDesc { typ, names }
    }

    pub fn from_names_and_types<I, T, N>(iter: I) -> Self
    where
        I: IntoIterator<Item = (Option<N>, T)>,
        T: Into<ColumnType>,
        N: Into<ColumnName>,
    {
        let (names, types): (Vec<_>, Vec<_>) = iter.into_iter().unzip();
        let types = types.into_iter().map(Into::into).collect();
        let typ = RelationType::new(types);
        Self::new(typ, names)
    }
    /// Concatenates a `RelationDesc` onto the end of this `RelationDesc`.
    pub fn concat(mut self, other: Self) -> Self {
        let self_len = self.typ.column_types.len();
        self.names.extend(other.names);
        self.typ.column_types.extend(other.typ.column_types);
        for k in other.typ.keys {
            let k = k.into_iter().map(|idx| idx + self_len).collect();
            self = self.with_key(k);
        }
        self
    }

    /// Appends a named column with the specified column type.
    pub fn with_column<N>(mut self, name: N, column_type: ColumnType) -> Self
    where
        N: Into<ColumnName>,
    {
        self.typ.column_types.push(column_type);
        self.names.push(Some(name.into()));
        self
    }

    /// Adds a new key for the relation.
    pub fn with_key(mut self, indices: Vec<usize>) -> Self {
        self.typ = self.typ.with_key(indices);
        self
    }

    /// Drops all existing keys.
    pub fn without_keys(mut self) -> Self {
        self.typ.keys.clear();
        self
    }

    /// Builds a new relation description with the column names replaced with
    /// new names.
    ///
    /// # Panics
    ///
    /// Panics if the arity of the relation type does not match the number of
    /// items in `names`.
    pub fn with_names<I, N>(self, names: I) -> Self
    where
        I: IntoIterator<Item = Option<N>>,
        N: Into<ColumnName>,
    {
        Self::new(self.typ, names)
    }

    /// Computes the number of columns in the relation.
    pub fn arity(&self) -> usize {
        self.typ.arity()
    }

    /// Returns the relation type underlying this relation description.
    pub fn typ(&self) -> &RelationType {
        &self.typ
    }

    /// Returns an iterator over the columns in this relation.
    pub fn iter(&self) -> impl Iterator<Item = (Option<&ColumnName>, &ColumnType)> {
        self.iter_names().zip(self.iter_types())
    }

    /// Returns an iterator over the types of the columns in this relation.
    pub fn iter_types(&self) -> impl Iterator<Item = &ColumnType> {
        self.typ.column_types.iter()
    }

    /// Returns an iterator over the names of the columns in this relation.
    pub fn iter_names(&self) -> impl Iterator<Item = Option<&ColumnName>> {
        self.names.iter().map(|n| n.as_ref())
    }

    /// Finds a column by name.
    ///
    /// Returns the index and type of the column named `name`. If no column with
    /// the specified name exists, returns `None`. If multiple columns have the
    /// specified name, the leftmost column is returned.
    pub fn get_by_name(&self, name: &ColumnName) -> Option<(usize, &ColumnType)> {
        self.iter_names()
            .position(|n| n == Some(name))
            .map(|i| (i, &self.typ.column_types[i]))
    }

    /// Gets the name of the `i`th column.
    ///
    /// # Panics
    ///
    /// Panics if `i` is not a valid column index.
    pub fn get_name(&self, i: usize) -> Option<&ColumnName> {
        self.names[i].as_ref()
    }

    /// Gets the name of the `i`th column if that column name is unambiguous.
    ///
    /// If at least one other column has the same name as the `i`th column,
    /// returns `None`. If the `i`th column has no name, returns `None`.
    ///
    /// # Panics
    ///
    /// Panics if `i` is not a valid column index.
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

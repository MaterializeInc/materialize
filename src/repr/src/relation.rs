// Copyright Materialize, Inc. and contributors. All rights reserved.
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
use proptest_derive::Arbitrary;
use serde::{Deserialize, Serialize};

use mz_lowertest::MzReflect;
use mz_ore::str::StrExt;

use crate::proto::newapi::IntoRustIfSome;
use crate::proto::newapi::{ProtoType, RustType, TryFromProtoError};
use crate::{Datum, ScalarType};

use crate::relation_and_scalar::proto_relation_type::ProtoKey;
pub use crate::relation_and_scalar::{ProtoColumnName, ProtoColumnType, ProtoRelationType};

/// The type of a [`Datum`](crate::Datum).
///
/// [`ColumnType`] bundles information about the scalar type of a datum (e.g.,
/// Int32 or String) with its nullability.
///
/// To construct a column type, either initialize the struct directly, or
/// use the [`ScalarType::nullable`] method.
#[derive(
    Arbitrary, Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Serialize, Deserialize, Hash, MzReflect,
)]
pub struct ColumnType {
    /// The underlying scalar type (e.g., Int32 or String) of this column.
    pub scalar_type: ScalarType,
    /// Whether this datum can be null.
    #[serde(default = "return_true")]
    pub nullable: bool,
}

/// This method exists solely for the purpose of making ColumnType nullable by
/// default in unit tests. The default value of a bool is false, and the only
/// way to make an object take on any other value by default is to pass it a
/// function that returns the desired default value. See
/// <https://github.com/serde-rs/serde/issues/1030>
#[inline(always)]
fn return_true() -> bool {
    true
}

impl ColumnType {
    pub fn union(&self, other: &Self) -> Result<Self, anyhow::Error> {
        match (self.scalar_type.clone(), other.scalar_type.clone()) {
            (scalar_type, other_scalar_type) if scalar_type.base_eq(&other_scalar_type) => {
                Ok(ColumnType {
                    scalar_type,
                    nullable: self.nullable || other.nullable,
                })
            }
            (
                ScalarType::Record { fields, custom_id },
                ScalarType::Record {
                    fields: other_fields,
                    custom_id: other_custom_id,
                },
            ) => {
                if custom_id != other_custom_id {
                    bail!(
                        "Can't union types: {:?} and {:?}",
                        self.scalar_type,
                        other.scalar_type
                    );
                };

                let mut union_fields: Vec<(ColumnName, ColumnType)> = vec![];
                for (field, other_field) in fields.iter().zip(other_fields.iter()) {
                    if field.0 != other_field.0 {
                        bail!(
                            "Can't union types: {:?} and {:?}",
                            self.scalar_type,
                            other.scalar_type
                        );
                    } else {
                        let union_column_type = field.1.union(&other_field.1)?;
                        union_fields.push((field.0.clone(), union_column_type));
                    };
                }

                Ok(ColumnType {
                    scalar_type: ScalarType::Record {
                        fields: union_fields,
                        custom_id,
                    },
                    nullable: self.nullable || other.nullable,
                })
            }
            _ => bail!(
                "Can't union types: {:?} and {:?}",
                self.scalar_type,
                other.scalar_type
            ),
        }
    }

    /// Consumes this `ColumnType` and returns a new `ColumnType` with its
    /// nullability set to the specified boolean.
    pub fn nullable(mut self, nullable: bool) -> Self {
        self.nullable = nullable;
        self
    }
}

impl RustType<ProtoColumnType> for ColumnType {
    fn into_proto(&self) -> ProtoColumnType {
        ProtoColumnType {
            nullable: self.nullable,
            scalar_type: Some(self.scalar_type.into_proto()),
        }
    }

    fn from_proto(proto: ProtoColumnType) -> Result<Self, TryFromProtoError> {
        Ok(ColumnType {
            nullable: proto.nullable,
            scalar_type: proto
                .scalar_type
                .into_rust_if_some("ProtoColumnType::scalar_type")?,
        })
    }
}

/// The type of a relation.
#[derive(Arbitrary, Clone, Debug, Eq, PartialEq, Serialize, Deserialize, Hash, MzReflect)]
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
    #[serde(default)]
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

impl RustType<ProtoRelationType> for RelationType {
    fn into_proto(&self) -> ProtoRelationType {
        ProtoRelationType {
            column_types: self.column_types.into_proto(),
            keys: self.keys.into_proto(),
        }
    }

    fn from_proto(proto: ProtoRelationType) -> Result<Self, TryFromProtoError> {
        Ok(RelationType {
            column_types: proto.column_types.into_rust()?,
            keys: proto.keys.into_rust()?,
        })
    }
}

impl RustType<ProtoKey> for Vec<usize> {
    fn into_proto(self: &Self) -> ProtoKey {
        ProtoKey {
            keys: self.into_proto(),
        }
    }

    fn from_proto(proto: ProtoKey) -> Result<Self, TryFromProtoError> {
        proto.keys.into_rust()
    }
}

/// The name of a column in a [`RelationDesc`].
#[derive(
    Arbitrary, Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Serialize, Deserialize, Hash, MzReflect,
)]
pub struct ColumnName(pub(crate) String);

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

impl RustType<ProtoColumnName> for ColumnName {
    fn into_proto(&self) -> ProtoColumnName {
        ProtoColumnName {
            value: Some(self.0.clone()),
        }
    }

    fn from_proto(proto: ProtoColumnName) -> Result<Self, TryFromProtoError> {
        Ok(ColumnName(proto.value.ok_or_else(|| {
            TryFromProtoError::missing_field("ProtoColumnName::value")
        })?))
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
/// use mz_repr::{ColumnType, RelationDesc, ScalarType};
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
/// use mz_repr::RelationDesc;
///
/// # fn plan_query(_: &str) -> mz_repr::RelationType { mz_repr::RelationType::new(vec![]) }
/// let relation_type = plan_query("SELECT * FROM table");
/// let names = (0..relation_type.arity()).map(|i| match i {
///     0 => "first",
///     1 => "second",
///     _ => "unknown",
/// });
/// let desc = RelationDesc::new(relation_type, names);
/// ```
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, Hash, MzReflect)]
pub struct RelationDesc {
    typ: RelationType,
    names: Vec<ColumnName>,
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
        I: IntoIterator<Item = N>,
        N: Into<ColumnName>,
    {
        let names: Vec<_> = names.into_iter().map(|name| name.into()).collect();
        assert_eq!(typ.column_types.len(), names.len());
        RelationDesc { typ, names }
    }

    pub fn from_names_and_types<I, T, N>(iter: I) -> Self
    where
        I: IntoIterator<Item = (N, T)>,
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

    /// Appends a column with the specified name and type.
    pub fn with_column<N>(mut self, name: N, column_type: ColumnType) -> Self
    where
        N: Into<ColumnName>,
    {
        self.typ.column_types.push(column_type);
        self.names.push(name.into());
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
        I: IntoIterator<Item = N>,
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
    pub fn iter(&self) -> impl Iterator<Item = (&ColumnName, &ColumnType)> {
        self.iter_names().zip(self.iter_types())
    }

    /// Returns an iterator over the types of the columns in this relation.
    pub fn iter_types(&self) -> impl Iterator<Item = &ColumnType> {
        self.typ.column_types.iter()
    }

    /// Returns an iterator over the names of the columns in this relation.
    pub fn iter_names(&self) -> impl Iterator<Item = &ColumnName> {
        self.names.iter()
    }

    /// Finds a column by name.
    ///
    /// Returns the index and type of the column named `name`. If no column with
    /// the specified name exists, returns `None`. If multiple columns have the
    /// specified name, the leftmost column is returned.
    pub fn get_by_name(&self, name: &ColumnName) -> Option<(usize, &ColumnType)> {
        self.iter_names()
            .position(|n| n == name)
            .map(|i| (i, &self.typ.column_types[i]))
    }

    /// Gets the name of the `i`th column.
    ///
    /// # Panics
    ///
    /// Panics if `i` is not a valid column index.
    pub fn get_name(&self, i: usize) -> &ColumnName {
        &self.names[i]
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
        let name = &self.names[i];
        if self.iter_names().filter(|n| *n == name).count() == 1 {
            Some(name)
        } else {
            None
        }
    }

    /// Verifies that `d` meets all of the constraints for the `i`th column of `self`.
    ///
    /// n.b. The only constraint MZ currently supports in NOT NULL, but this
    /// structure will  be simple to extend.
    pub fn constraints_met(&self, i: usize, d: &Datum) -> Result<(), NotNullViolation> {
        let name = &self.names[i];
        let typ = &self.typ.column_types[i];
        if d == &Datum::Null && !typ.nullable {
            Err(NotNullViolation(name.clone()))
        } else {
            Ok(())
        }
    }
}

impl IntoIterator for RelationDesc {
    type Item = (ColumnName, ColumnType);
    type IntoIter = iter::Zip<vec::IntoIter<ColumnName>, vec::IntoIter<ColumnType>>;

    fn into_iter(self) -> Self::IntoIter {
        self.names.into_iter().zip(self.typ.column_types)
    }
}

/// Expression violated not-null constraint on named column
#[derive(Debug, PartialEq, Eq)]
pub struct NotNullViolation(pub ColumnName);

impl fmt::Display for NotNullViolation {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "null value in column {} violates not-null constraint",
            self.0.as_str().quoted()
        )
    }
}

// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::{BTreeMap, BTreeSet};
use std::rc::Rc;
use std::{fmt, vec};

use anyhow::bail;
use itertools::Itertools;
use mz_lowertest::MzReflect;
use mz_ore::cast::CastFrom;
use mz_ore::str::StrExt;
use mz_ore::{assert_none, assert_ok};
use mz_persist_types::schema::SchemaId;
use mz_proto::{IntoRustIfSome, ProtoType, RustType, TryFromProtoError};
use proptest::prelude::*;
use proptest::strategy::{Strategy, Union};
use proptest_derive::Arbitrary;
use serde::{Deserialize, Serialize};
use timely::Container;

use crate::relation_and_scalar::proto_relation_type::ProtoKey;
pub use crate::relation_and_scalar::{
    ProtoColumnMetadata, ProtoColumnName, ProtoColumnType, ProtoRelationDesc, ProtoRelationType,
    ProtoRelationVersion,
};
use crate::{Datum, Row, ScalarType, arb_datum_for_column};

/// The type of a [`Datum`].
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
        match (&self.scalar_type, &other.scalar_type) {
            (scalar_type, other_scalar_type) if scalar_type == other_scalar_type => {
                Ok(ColumnType {
                    scalar_type: scalar_type.clone(),
                    nullable: self.nullable || other.nullable,
                })
            }
            (scalar_type, other_scalar_type) if scalar_type.base_eq(other_scalar_type) => {
                Ok(ColumnType {
                    scalar_type: scalar_type.without_modifiers(),
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

                let mut union_fields = Vec::with_capacity(fields.len());
                for ((name, typ), (other_name, other_typ)) in
                    fields.iter().zip_eq(other_fields.iter())
                {
                    if name != other_name {
                        bail!(
                            "Can't union types: {:?} and {:?}",
                            self.scalar_type,
                            other.scalar_type
                        );
                    } else {
                        let union_column_type = typ.union(other_typ)?;
                        union_fields.push((name.clone(), union_column_type));
                    };
                }

                Ok(ColumnType {
                    scalar_type: ScalarType::Record {
                        fields: union_fields.into(),
                        custom_id: *custom_id,
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

impl fmt::Display for ColumnType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let nullable = if self.nullable { "Null" } else { "NotNull" };
        f.write_fmt(format_args!("{:?}:{}", self.scalar_type, nullable))
    }
}

/// The type of a relation.
#[derive(
    Arbitrary, Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Serialize, Deserialize, Hash, MzReflect,
)]
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

    /// Returns all the [`ColumnType`]s, in order, for this relation.
    pub fn columns(&self) -> &[ColumnType] {
        &self.column_types
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
    fn into_proto(&self) -> ProtoKey {
        ProtoKey {
            keys: self.into_proto(),
        }
    }

    fn from_proto(proto: ProtoKey) -> Result<Self, TryFromProtoError> {
        proto.keys.into_rust()
    }
}

/// The name of a column in a [`RelationDesc`].
#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Serialize, Deserialize, Hash, MzReflect)]
pub struct ColumnName(Box<str>);

impl ColumnName {
    /// Returns this column name as a `str`.
    #[inline(always)]
    pub fn as_str(&self) -> &str {
        &*self
    }

    /// Returns this column name as a `&mut Box<str>`.
    pub fn as_mut_boxed_str(&mut self) -> &mut Box<str> {
        &mut self.0
    }

    /// Returns if this [`ColumnName`] is similar to the provided one.
    pub fn is_similar(&self, other: &ColumnName) -> bool {
        const SIMILARITY_THRESHOLD: f64 = 0.6;

        let a_lowercase = self.to_lowercase();
        let b_lowercase = other.to_lowercase();

        strsim::normalized_levenshtein(&a_lowercase, &b_lowercase) >= SIMILARITY_THRESHOLD
    }
}

impl std::ops::Deref for ColumnName {
    type Target = str;

    #[inline(always)]
    fn deref(&self) -> &Self::Target {
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
        ColumnName(s.into())
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
            value: Some(self.0.to_string()),
        }
    }

    fn from_proto(proto: ProtoColumnName) -> Result<Self, TryFromProtoError> {
        Ok(ColumnName(
            proto
                .value
                .ok_or_else(|| TryFromProtoError::missing_field("ProtoColumnName::value"))?
                .into(),
        ))
    }
}

impl From<ColumnName> for mz_sql_parser::ast::Ident {
    fn from(value: ColumnName) -> Self {
        // Note: ColumnNames are known to be less than the max length of an Ident (I think?).
        mz_sql_parser::ast::Ident::new_unchecked(value.0)
    }
}

impl proptest::arbitrary::Arbitrary for ColumnName {
    type Parameters = ();
    type Strategy = BoxedStrategy<ColumnName>;

    fn arbitrary_with(_args: Self::Parameters) -> Self::Strategy {
        // Long column names are generally uninteresting, and can greatly
        // increase the runtime for a test case, so bound the max length.
        let mut weights = vec![(50, Just(1..8)), (20, Just(8..16))];
        if std::env::var("PROPTEST_LARGE_DATA").is_ok() {
            weights.extend([
                (5, Just(16..128)),
                (1, Just(128..1024)),
                (1, Just(1024..4096)),
            ]);
        }
        let name_length = Union::new_weighted(weights);

        // Non-ASCII characters are also generally uninteresting and can make
        // debugging harder.
        let char_strat = Rc::new(Union::new_weighted(vec![
            (50, proptest::char::range('A', 'z').boxed()),
            (1, any::<char>().boxed()),
        ]));

        name_length
            .prop_flat_map(move |length| proptest::collection::vec(Rc::clone(&char_strat), length))
            .prop_map(|chars| ColumnName(chars.into_iter().collect::<Box<str>>()))
            .no_shrink()
            .boxed()
    }
}

/// Default name of a column (when no other information is known).
pub const UNKNOWN_COLUMN_NAME: &str = "?column?";

/// Stable index of a column in a [`RelationDesc`].
#[derive(
    Clone, Copy, Debug, Eq, PartialEq, PartialOrd, Ord, Serialize, Deserialize, Hash, MzReflect,
)]
pub struct ColumnIndex(usize);

static_assertions::assert_not_impl_all!(ColumnIndex: Arbitrary);

impl ColumnIndex {
    /// Returns a stable identifier for this [`ColumnIndex`].
    pub fn to_stable_name(&self) -> String {
        self.0.to_string()
    }

    pub fn to_raw(&self) -> usize {
        self.0
    }

    pub fn from_raw(val: usize) -> Self {
        ColumnIndex(val)
    }
}

/// The version a given column was added at.
#[derive(
    Clone,
    Copy,
    Debug,
    Eq,
    PartialEq,
    PartialOrd,
    Ord,
    Serialize,
    Deserialize,
    Hash,
    MzReflect,
    Arbitrary,
)]
pub struct RelationVersion(u64);

impl RelationVersion {
    /// Returns the "root" or "initial" version of a [`RelationDesc`].
    pub fn root() -> Self {
        RelationVersion(0)
    }

    /// Returns an instance of [`RelationVersion`] which is "one" higher than `self`.
    pub fn bump(&self) -> Self {
        let next_version = self
            .0
            .checked_add(1)
            .expect("added more than u64::MAX columns?");
        RelationVersion(next_version)
    }

    /// Consume a [`RelationVersion`] returning the raw value.
    ///
    /// Should __only__ be used for serialization.
    pub fn into_raw(self) -> u64 {
        self.0
    }

    /// Create a [`RelationVersion`] from a raw value.
    ///
    /// Should __only__ be used for serialization.
    pub fn from_raw(val: u64) -> RelationVersion {
        RelationVersion(val)
    }
}

impl From<RelationVersion> for SchemaId {
    fn from(value: RelationVersion) -> Self {
        SchemaId(usize::cast_from(value.0))
    }
}

impl From<mz_sql_parser::ast::Version> for RelationVersion {
    fn from(value: mz_sql_parser::ast::Version) -> Self {
        RelationVersion(value.into_inner())
    }
}

impl fmt::Display for RelationVersion {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "v{}", self.0)
    }
}

impl From<RelationVersion> for mz_sql_parser::ast::Version {
    fn from(value: RelationVersion) -> Self {
        mz_sql_parser::ast::Version::new(value.0)
    }
}

impl RustType<ProtoRelationVersion> for RelationVersion {
    fn into_proto(&self) -> ProtoRelationVersion {
        ProtoRelationVersion { value: self.0 }
    }

    fn from_proto(proto: ProtoRelationVersion) -> Result<Self, TryFromProtoError> {
        Ok(RelationVersion(proto.value))
    }
}

/// Metadata (other than type) for a column in a [`RelationDesc`].
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, Hash, MzReflect)]
struct ColumnMetadata {
    /// Name of the column.
    name: ColumnName,
    /// Index into a [`RelationType`] for this column.
    typ_idx: usize,
    /// Version this column was added at.
    added: RelationVersion,
    /// Version this column was dropped at.
    dropped: Option<RelationVersion>,
}

/// A description of the shape of a relation.
///
/// It bundles a [`RelationType`] with `ColumnMetadata` for each column in
/// the relation.
///
/// # Examples
///
/// A `RelationDesc`s is typically constructed via its builder API:
///
/// ```
/// use mz_repr::{ColumnType, RelationDesc, ScalarType};
///
/// let desc = RelationDesc::builder()
///     .with_column("id", ScalarType::Int64.nullable(false))
///     .with_column("price", ScalarType::Float64.nullable(true))
///     .finish();
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
///
/// Next to the [`RelationType`] we maintain a map of `ColumnIndex` to
/// `ColumnMetadata`, where [`ColumnIndex`] is a stable identifier for a
/// column throughout the lifetime of the relation. This allows a
/// [`RelationDesc`] to represent a projection over a version of itself.
///
/// ```
/// use std::collections::BTreeSet;
/// use mz_repr::{ColumnIndex, RelationDesc, ScalarType};
///
/// let desc = RelationDesc::builder()
///     .with_column("name", ScalarType::String.nullable(false))
///     .with_column("email", ScalarType::String.nullable(false))
///     .finish();
///
/// // Project away the second column.
/// let demands = BTreeSet::from([1]);
/// let proj = desc.apply_demand(&demands);
///
/// // We projected away the first column.
/// assert!(!proj.contains_index(&ColumnIndex::from_raw(0)));
/// // But retained the second.
/// assert!(proj.contains_index(&ColumnIndex::from_raw(1)));
///
/// // The underlying `RelationType` also contains a single column.
/// assert_eq!(proj.typ().arity(), 1);
/// ```
///
/// To maintain this stable mapping and track the lifetime of a column (e.g.
/// when adding or dropping a column) we use `ColumnMetadata`. It maintains
/// the index in [`RelationType`] that corresponds to a given column, and the
/// version at which this column was added or dropped.
///
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, Hash, MzReflect)]
pub struct RelationDesc {
    typ: RelationType,
    metadata: BTreeMap<ColumnIndex, ColumnMetadata>,
}

impl RustType<ProtoRelationDesc> for RelationDesc {
    fn into_proto(&self) -> ProtoRelationDesc {
        let (names, metadata): (Vec<_>, Vec<_>) = self
            .metadata
            .values()
            .map(|meta| {
                let metadata = ProtoColumnMetadata {
                    added: Some(meta.added.into_proto()),
                    dropped: meta.dropped.map(|v| v.into_proto()),
                };
                (meta.name.into_proto(), metadata)
            })
            .unzip();

        // `metadata` Migration Logic: We wrote some `ProtoRelationDesc`s into Persist before the
        // metadata field was added. To make sure our serialization roundtrips the same as before
        // we added the field, we omit `metadata` if all of the values are equal to the default.
        //
        // Note: This logic needs to exist approximately forever.
        let is_all_default_metadata = metadata.iter().all(|meta| {
            meta.added == Some(RelationVersion::root().into_proto()) && meta.dropped == None
        });
        let metadata = if is_all_default_metadata {
            Vec::new()
        } else {
            metadata
        };

        ProtoRelationDesc {
            typ: Some(self.typ.into_proto()),
            names,
            metadata,
        }
    }

    fn from_proto(proto: ProtoRelationDesc) -> Result<Self, TryFromProtoError> {
        // `metadata` Migration Logic: We wrote some `ProtoRelationDesc`s into Persist before the
        // metadata field was added. If the field doesn't exist we fill it in with default values,
        // and when converting into_proto we omit these fields so the serialized bytes roundtrip.
        //
        // Note: This logic needs to exist approximately forever.
        let proto_metadata: Box<dyn Iterator<Item = _>> = if proto.metadata.is_empty() {
            let val = ProtoColumnMetadata {
                added: Some(RelationVersion::root().into_proto()),
                dropped: None,
            };
            Box::new(itertools::repeat_n(val, proto.names.len()))
        } else {
            Box::new(proto.metadata.into_iter())
        };

        let metadata = proto
            .names
            .into_iter()
            .zip_eq(proto_metadata)
            .enumerate()
            .map(|(idx, (name, metadata))| {
                let meta = ColumnMetadata {
                    name: name.into_rust()?,
                    typ_idx: idx,
                    added: metadata.added.into_rust_if_some("ColumnMetadata::added")?,
                    dropped: metadata.dropped.into_rust()?,
                };
                Ok::<_, TryFromProtoError>((ColumnIndex(idx), meta))
            })
            .collect::<Result<_, _>>()?;

        Ok(RelationDesc {
            typ: proto.typ.into_rust_if_some("ProtoRelationDesc::typ")?,
            metadata,
        })
    }
}

impl RelationDesc {
    /// Returns a [`RelationDescBuilder`] that can be used to construct a [`RelationDesc`].
    pub fn builder() -> RelationDescBuilder {
        RelationDescBuilder::default()
    }

    /// Constructs a new `RelationDesc` that represents the empty relation
    /// with no columns and no keys.
    pub fn empty() -> Self {
        RelationDesc {
            typ: RelationType::empty(),
            metadata: BTreeMap::default(),
        }
    }

    /// Check if the `RelationDesc` is empty.
    pub fn is_empty(&self) -> bool {
        self == &Self::empty()
    }

    /// Returns the number of columns in this [`RelationDesc`].
    pub fn len(&self) -> usize {
        self.typ().column_types.len()
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
        let metadata: BTreeMap<_, _> = names
            .into_iter()
            .enumerate()
            .map(|(idx, name)| {
                let col_idx = ColumnIndex(idx);
                let metadata = ColumnMetadata {
                    name: name.into(),
                    typ_idx: idx,
                    added: RelationVersion::root(),
                    dropped: None,
                };
                (col_idx, metadata)
            })
            .collect();

        // TODO(parkmycar): Add better validation here.
        assert_eq!(typ.column_types.len(), metadata.len());

        RelationDesc { typ, metadata }
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
    ///
    /// # Panics
    ///
    /// Panics if either `self` or `other` have columns that were added at a
    /// [`RelationVersion`] other than [`RelationVersion::root`] or if any
    /// columns were dropped.
    ///
    /// TODO(parkmycar): Move this method to [`RelationDescBuilder`].
    pub fn concat(mut self, other: Self) -> Self {
        let self_len = self.typ.column_types.len();

        for (typ, (_col_idx, meta)) in other
            .typ
            .column_types
            .into_iter()
            .zip_eq(other.metadata.into_iter())
        {
            assert_eq!(meta.added, RelationVersion::root());
            assert_none!(meta.dropped);

            let new_idx = self.typ.columns().len();
            let new_meta = ColumnMetadata {
                name: meta.name,
                typ_idx: new_idx,
                added: RelationVersion::root(),
                dropped: None,
            };

            self.typ.column_types.push(typ);
            let prev = self.metadata.insert(ColumnIndex(new_idx), new_meta);

            assert_eq!(self.metadata.len(), self.typ.columns().len());
            assert_none!(prev);
        }

        for k in other.typ.keys {
            let k = k.into_iter().map(|idx| idx + self_len).collect();
            self = self.with_key(k);
        }
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
        self.metadata.values().map(|meta| {
            let typ = &self.typ.columns()[meta.typ_idx];
            (&meta.name, typ)
        })
    }

    /// Returns an iterator over the types of the columns in this relation.
    pub fn iter_types(&self) -> impl Iterator<Item = &ColumnType> {
        self.typ.column_types.iter()
    }

    /// Returns an iterator over the names of the columns in this relation.
    pub fn iter_names(&self) -> impl Iterator<Item = &ColumnName> {
        self.metadata.values().map(|meta| &meta.name)
    }

    /// Returns an iterator over the columns in this relation, with all their metadata.
    pub fn iter_all(&self) -> impl Iterator<Item = (&ColumnIndex, &ColumnName, &ColumnType)> {
        self.metadata.iter().map(|(col_idx, metadata)| {
            let col_typ = &self.typ.columns()[metadata.typ_idx];
            (col_idx, &metadata.name, col_typ)
        })
    }

    /// Returns an iterator over the names of the columns in this relation that are "similar" to
    /// the provided `name`.
    pub fn iter_similar_names<'a>(
        &'a self,
        name: &'a ColumnName,
    ) -> impl Iterator<Item = &'a ColumnName> {
        self.iter_names().filter(|n| n.is_similar(name))
    }

    /// Returns whether this [`RelationDesc`] contains a column at the specified index.
    pub fn contains_index(&self, idx: &ColumnIndex) -> bool {
        self.metadata.contains_key(idx)
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
    ///
    /// TODO(parkmycar): Migrate all uses of this to [`RelationDesc::get_name_idx`].
    pub fn get_name(&self, i: usize) -> &ColumnName {
        // TODO(parkmycar): Refactor this to use `ColumnIndex`.
        self.get_name_idx(&ColumnIndex(i))
    }

    /// Gets the name of the column at `idx`.
    ///
    /// # Panics
    ///
    /// Panics if no column exists at `idx`.
    pub fn get_name_idx(&self, idx: &ColumnIndex) -> &ColumnName {
        &self.metadata.get(idx).expect("should exist").name
    }

    /// Mutably gets the name of the `i`th column.
    ///
    /// # Panics
    ///
    /// Panics if `i` is not a valid column index.
    pub fn get_name_mut(&mut self, i: usize) -> &mut ColumnName {
        // TODO(parkmycar): Refactor this to use `ColumnIndex`.
        &mut self
            .metadata
            .get_mut(&ColumnIndex(i))
            .expect("should exist")
            .name
    }

    /// Gets the [`ColumnType`] of the column at `idx`.
    ///
    /// # Panics
    ///
    /// Panics if no column exists at `idx`.
    pub fn get_type(&self, idx: &ColumnIndex) -> &ColumnType {
        let typ_idx = self.metadata.get(idx).expect("should exist").typ_idx;
        &self.typ.column_types[typ_idx]
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
        let name = self.get_name(i);
        if self.iter_names().filter(|n| *n == name).count() == 1 {
            Some(name)
        } else {
            None
        }
    }

    /// Verifies that `d` meets all of the constraints for the `i`th column of `self`.
    ///
    /// n.b. The only constraint MZ currently supports in NOT NULL, but this
    /// structure will be simple to extend.
    pub fn constraints_met(&self, i: usize, d: &Datum) -> Result<(), NotNullViolation> {
        let name = self.get_name(i);
        let typ = &self.typ.column_types[i];
        if d == &Datum::Null && !typ.nullable {
            Err(NotNullViolation(name.clone()))
        } else {
            Ok(())
        }
    }

    /// Creates a new [`RelationDesc`] retaining only the columns specified in `demands`.
    pub fn apply_demand(&self, demands: &BTreeSet<usize>) -> RelationDesc {
        let mut new_desc = self.clone();

        // Update ColumnMetadata.
        let mut removed = 0;
        new_desc.metadata.retain(|idx, metadata| {
            let retain = demands.contains(&idx.0);
            if !retain {
                removed += 1;
            } else {
                metadata.typ_idx -= removed;
            }
            retain
        });

        // Update ColumnType.
        let mut idx = 0;
        new_desc.typ.column_types.retain(|_| {
            let keep = demands.contains(&idx);
            idx += 1;
            keep
        });

        new_desc
    }
}

impl Arbitrary for RelationDesc {
    type Parameters = ();
    type Strategy = BoxedStrategy<RelationDesc>;

    fn arbitrary_with(_args: Self::Parameters) -> Self::Strategy {
        let mut weights = vec![(100, Just(0..4)), (50, Just(4..8)), (25, Just(8..16))];
        if std::env::var("PROPTEST_LARGE_DATA").is_ok() {
            weights.extend([
                (12, Just(16..32)),
                (6, Just(32..64)),
                (3, Just(64..128)),
                (1, Just(128..256)),
            ]);
        }
        let num_columns = Union::new_weighted(weights);

        num_columns.prop_flat_map(arb_relation_desc).boxed()
    }
}

/// Returns a [`Strategy`] that generates an arbitrary [`RelationDesc`] with a number columns
/// within the range provided.
pub fn arb_relation_desc(num_cols: std::ops::Range<usize>) -> impl Strategy<Value = RelationDesc> {
    proptest::collection::btree_map(any::<ColumnName>(), any::<ColumnType>(), num_cols)
        .prop_map(RelationDesc::from_names_and_types)
}

/// Returns a [`Strategy`] that generates a projection of the provided [`RelationDesc`].
pub fn arb_relation_desc_projection(desc: RelationDesc) -> impl Strategy<Value = RelationDesc> {
    let mask: Vec<_> = (0..desc.len()).map(|_| any::<bool>()).collect();
    mask.prop_map(move |mask| {
        let demands: BTreeSet<_> = mask
            .into_iter()
            .enumerate()
            .filter_map(|(idx, keep)| keep.then_some(idx))
            .collect();
        desc.apply_demand(&demands)
    })
}

impl IntoIterator for RelationDesc {
    type Item = (ColumnName, ColumnType);
    type IntoIter = Box<dyn Iterator<Item = (ColumnName, ColumnType)>>;

    fn into_iter(self) -> Self::IntoIter {
        let iter = self
            .metadata
            .into_values()
            .zip_eq(self.typ.column_types)
            .map(|(meta, typ)| (meta.name, typ));
        Box::new(iter)
    }
}

/// Returns a [`Strategy`] that yields arbitrary [`Row`]s for the provided [`RelationDesc`].
pub fn arb_row_for_relation(desc: &RelationDesc) -> impl Strategy<Value = Row> + use<> {
    let datums: Vec<_> = desc
        .typ()
        .columns()
        .iter()
        .cloned()
        .map(arb_datum_for_column)
        .collect();
    datums.prop_map(|x| Row::pack(x.iter().map(Datum::from)))
}

/// Expression violated not-null constraint on named column
#[derive(Debug, PartialEq, Eq)]
pub struct NotNullViolation(pub ColumnName);

impl fmt::Display for NotNullViolation {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "null value in column {} violates not-null constraint",
            self.0.quoted()
        )
    }
}

/// A builder for a [`RelationDesc`].
#[derive(Clone, Default, Debug, PartialEq, Eq)]
pub struct RelationDescBuilder {
    /// Columns of the relation.
    columns: Vec<(ColumnName, ColumnType)>,
    /// Sets of indices that are "keys" for the collection.
    keys: Vec<Vec<usize>>,
}

impl RelationDescBuilder {
    /// Appends a column with the specified name and type.
    pub fn with_column<N: Into<ColumnName>>(
        mut self,
        name: N,
        ty: ColumnType,
    ) -> RelationDescBuilder {
        let name = name.into();
        self.columns.push((name, ty));
        self
    }

    /// Appends the provided columns to the builder.
    pub fn with_columns<I, T, N>(mut self, iter: I) -> Self
    where
        I: IntoIterator<Item = (N, T)>,
        T: Into<ColumnType>,
        N: Into<ColumnName>,
    {
        self.columns
            .extend(iter.into_iter().map(|(name, ty)| (name.into(), ty.into())));
        self
    }

    /// Adds a new key for the relation.
    pub fn with_key(mut self, mut indices: Vec<usize>) -> RelationDescBuilder {
        indices.sort_unstable();
        if !self.keys.contains(&indices) {
            self.keys.push(indices);
        }
        self
    }

    /// Removes all previously inserted keys.
    pub fn without_keys(mut self) -> RelationDescBuilder {
        self.keys.clear();
        assert_eq!(self.keys.len(), 0);
        self
    }

    /// Concatenates a [`RelationDescBuilder`] onto the end of this [`RelationDescBuilder`].
    pub fn concat(mut self, other: Self) -> Self {
        let self_len = self.columns.len();

        self.columns.extend(other.columns);
        for k in other.keys {
            let k = k.into_iter().map(|idx| idx + self_len).collect();
            self = self.with_key(k);
        }

        self
    }

    /// Finish the builder, returning a [`RelationDesc`].
    pub fn finish(self) -> RelationDesc {
        let mut desc = RelationDesc::from_names_and_types(self.columns);
        desc.typ = desc.typ.with_keys(self.keys);
        desc
    }
}

/// Describes a [`RelationDesc`] at a specific version of a [`VersionedRelationDesc`].
#[derive(Debug, Copy, Clone, Hash, PartialEq, Eq, PartialOrd, Ord, Serialize)]
pub enum RelationVersionSelector {
    Specific(RelationVersion),
    Latest,
}

impl RelationVersionSelector {
    pub fn specific(version: u64) -> Self {
        RelationVersionSelector::Specific(RelationVersion(version))
    }
}

/// A wrapper around [`RelationDesc`] that provides an interface for adding
/// columns and generating new versions.
///
/// TODO(parkmycar): Using an immutable data structure for RelationDesc would
/// be great.
#[derive(Debug, Clone, Serialize)]
pub struct VersionedRelationDesc {
    inner: RelationDesc,
}

impl VersionedRelationDesc {
    pub fn new(inner: RelationDesc) -> Self {
        VersionedRelationDesc { inner }
    }

    /// Adds a new column to this [`RelationDesc`], creating a new version of the [`RelationDesc`].
    ///
    /// # Panics
    ///
    /// * Panics if a column with `name` already exists that hasn't been dropped.
    ///
    /// Note: For building a [`RelationDesc`] see [`RelationDescBuilder::with_column`].
    #[must_use]
    pub fn add_column<N, T>(&mut self, name: N, typ: T) -> RelationVersion
    where
        N: Into<ColumnName>,
        T: Into<ColumnType>,
    {
        let latest_version = self.latest_version();
        let new_version = latest_version.bump();

        let name = name.into();
        let existing = self
            .inner
            .metadata
            .iter()
            .find(|(_, meta)| meta.name == name && meta.dropped.is_none());
        if let Some(existing) = existing {
            panic!("column named '{name}' already exists! {existing:?}");
        }

        let next_idx = self.inner.metadata.len();
        let col_meta = ColumnMetadata {
            name,
            typ_idx: next_idx,
            added: new_version,
            dropped: None,
        };

        self.inner.typ.column_types.push(typ.into());
        let prev = self.inner.metadata.insert(ColumnIndex(next_idx), col_meta);

        assert_none!(prev, "column index overlap!");
        self.validate();

        new_version
    }

    /// Drops the column `name` from this [`RelationDesc`]. If there are multiple columns with
    /// `name` drops the left-most one that hasn't already been dropped.
    ///
    /// TODO(parkmycar): Add handling for dropping a column that is currently used as a key.
    ///
    /// # Panics
    ///
    /// Panics if a column with `name` does not exist or the dropped column was used as a key.
    #[must_use]
    pub fn drop_column<N>(&mut self, name: N) -> RelationVersion
    where
        N: Into<ColumnName>,
    {
        let name = name.into();
        let latest_version = self.latest_version();
        let new_version = latest_version.bump();

        let col = self
            .inner
            .metadata
            .values_mut()
            .find(|meta| meta.name == name && meta.dropped.is_none())
            .expect("column to exist");

        // Make sure the column hadn't been previously dropped.
        assert_none!(col.dropped, "column was already dropped");
        col.dropped = Some(new_version);

        // Make sure the column isn't being used as a key.
        let dropped_key = self
            .inner
            .typ
            .keys
            .iter()
            .any(|keys| keys.iter().any(|key| *key == col.typ_idx));
        assert!(!dropped_key, "column being dropped was used as a key");

        self.validate();
        new_version
    }

    /// Returns the [`RelationDesc`] at the latest version.
    pub fn latest(&self) -> RelationDesc {
        self.inner.clone()
    }

    /// Returns this [`RelationDesc`] at the specified version.
    pub fn at_version(&self, version: RelationVersionSelector) -> RelationDesc {
        // Get all of the changes from the start, up to whatever version was requested.
        //
        // TODO(parkmycar): We should probably panic on unknown verisons?
        let up_to_version = match version {
            RelationVersionSelector::Latest => RelationVersion(u64::MAX),
            RelationVersionSelector::Specific(v) => v,
        };

        let valid_columns = self.inner.metadata.iter().filter(|(_col_idx, meta)| {
            let added = meta.added <= up_to_version;
            let dropped = meta
                .dropped
                .map(|dropped_at| up_to_version >= dropped_at)
                .unwrap_or(false);

            added && !dropped
        });

        let mut column_types = Vec::new();
        let mut column_metas = BTreeMap::new();

        // N.B. At this point we need to be careful because col_idx might not
        // equal typ_idx.
        //
        // For example, consider columns "a", "b", and "c" with indexes 0, 1,
        // and 2. If we drop column "b" then we'll have "a" and "c" with column
        // indexes 0 and 2, but their indices in RelationType will be 0 and 1.
        for (col_idx, meta) in valid_columns {
            let new_meta = ColumnMetadata {
                name: meta.name.clone(),
                typ_idx: column_types.len(),
                added: meta.added.clone(),
                dropped: meta.dropped.clone(),
            };
            column_types.push(self.inner.typ.columns()[meta.typ_idx].clone());
            column_metas.insert(*col_idx, new_meta);
        }

        // Remap keys in case a column with an index less than that of a key was
        // dropped.
        //
        // For example, consider columns "a", "b", and "c" where "a" and "c" are
        // keys and "b" was dropped.
        let keys = self
            .inner
            .typ
            .keys
            .iter()
            .map(|keys| {
                keys.iter()
                    .map(|key_idx| {
                        let metadata = column_metas
                            .get(&ColumnIndex(*key_idx))
                            .expect("found key for column that doesn't exist");
                        metadata.typ_idx
                    })
                    .collect()
            })
            .collect();

        let relation_type = RelationType { column_types, keys };

        RelationDesc {
            typ: relation_type,
            metadata: column_metas,
        }
    }

    pub fn latest_version(&self) -> RelationVersion {
        self.inner
            .metadata
            .values()
            // N.B. Dropped is always greater than added.
            .map(|meta| meta.dropped.unwrap_or(meta.added))
            .max()
            // If there aren't any columns we're implicitly the root version.
            .unwrap_or_else(RelationVersion::root)
    }

    /// Validates internal contraints of the [`RelationDesc`] are correct.
    ///
    /// # Panics
    ///
    /// Panics if a constraint is not satisfied.
    fn validate(&self) {
        fn validate_inner(desc: &RelationDesc) -> Result<(), anyhow::Error> {
            if desc.typ.column_types.len() != desc.metadata.len() {
                anyhow::bail!("mismatch between number of types and metadatas");
            }

            for (col_idx, meta) in &desc.metadata {
                if col_idx.0 > desc.metadata.len() {
                    anyhow::bail!("column index out of bounds");
                }
                if meta.added >= meta.dropped.unwrap_or(RelationVersion(u64::MAX)) {
                    anyhow::bail!("column was added after it was dropped?");
                }
                if desc.typ().columns().get(meta.typ_idx).is_none() {
                    anyhow::bail!("typ_idx incorrect");
                }
            }

            for keys in &desc.typ.keys {
                for key in keys {
                    if *key >= desc.typ.column_types.len() {
                        anyhow::bail!("key index was out of bounds!");
                    }
                }
            }

            let versions = desc
                .metadata
                .values()
                .map(|meta| meta.dropped.unwrap_or(meta.added));
            let mut max = 0;
            let mut sum = 0;
            for version in versions {
                max = std::cmp::max(max, version.0);
                sum += version.0;
            }

            // Other than RelationVersion(0), we should never have duplicate
            // versions and they should always increase by 1. In other words, the
            // sum of all RelationVersions should be the sum of [0, max].
            //
            // N.B. n * (n + 1) / 2 = sum of [0, n]
            //
            // While I normally don't like tricks like this, it allows us to
            // validate that our column versions are correct in O(n) time and
            // without allocations.
            if sum != (max * (max + 1) / 2) {
                anyhow::bail!("there is a duplicate or missing relation version");
            }

            Ok(())
        }

        assert_ok!(validate_inner(&self.inner), "validate failed! {self:?}");
    }
}

/// Diffs that can be generated proptest and applied to a [`RelationDesc`] to
/// exercise schema migrations.
#[derive(Debug)]
pub enum PropRelationDescDiff {
    AddColumn { name: ColumnName, typ: ColumnType },
    DropColumn { name: ColumnName },
    ToggleNullability { name: ColumnName },
    ChangeType { name: ColumnName, typ: ColumnType },
}

impl PropRelationDescDiff {
    pub fn apply(self, desc: &mut RelationDesc) {
        match self {
            PropRelationDescDiff::AddColumn { name, typ } => {
                let new_idx = desc.metadata.len();
                let meta = ColumnMetadata {
                    name,
                    typ_idx: new_idx,
                    added: RelationVersion(0),
                    dropped: None,
                };
                let prev = desc.metadata.insert(ColumnIndex(new_idx), meta);
                desc.typ.column_types.push(typ);

                assert_none!(prev);
                assert_eq!(desc.metadata.len(), desc.typ.column_types.len());
            }
            PropRelationDescDiff::DropColumn { name } => {
                let next_version = desc
                    .metadata
                    .values()
                    .map(|meta| meta.dropped.unwrap_or(meta.added))
                    .max()
                    .unwrap_or_else(RelationVersion::root)
                    .bump();
                let Some(metadata) = desc.metadata.values_mut().find(|meta| meta.name == name)
                else {
                    return;
                };
                if metadata.dropped.is_none() {
                    metadata.dropped = Some(next_version);
                }
            }
            PropRelationDescDiff::ToggleNullability { name } => {
                let Some((pos, _)) = desc.get_by_name(&name) else {
                    return;
                };
                let col_type = desc
                    .typ
                    .column_types
                    .get_mut(pos)
                    .expect("ColumnNames and ColumnTypes out of sync!");
                col_type.nullable = !col_type.nullable;
            }
            PropRelationDescDiff::ChangeType { name, typ } => {
                let Some((pos, _)) = desc.get_by_name(&name) else {
                    return;
                };
                let col_type = desc
                    .typ
                    .column_types
                    .get_mut(pos)
                    .expect("ColumnNames and ColumnTypes out of sync!");
                *col_type = typ;
            }
        }
    }
}

/// Generates a set of [`PropRelationDescDiff`]s based on some source [`RelationDesc`].
pub fn arb_relation_desc_diff(
    source: &RelationDesc,
) -> impl Strategy<Value = Vec<PropRelationDescDiff>> + use<> {
    let source = Rc::new(source.clone());
    let num_source_columns = source.typ.columns().len();

    let num_add_columns = Union::new_weighted(vec![(100, Just(0..8)), (1, Just(8..64))]);
    let add_columns_strat = num_add_columns
        .prop_flat_map(|num_columns| {
            proptest::collection::vec((any::<ColumnName>(), any::<ColumnType>()), num_columns)
        })
        .prop_map(|cols| {
            cols.into_iter()
                .map(|(name, typ)| PropRelationDescDiff::AddColumn { name, typ })
                .collect::<Vec<_>>()
        });

    // If the source RelationDesc is empty there is nothing else to do.
    if num_source_columns == 0 {
        return add_columns_strat.boxed();
    }

    let source_ = Rc::clone(&source);
    let drop_columns_strat = (0..num_source_columns).prop_perturb(move |num_columns, mut rng| {
        let mut set = BTreeSet::default();
        for _ in 0..num_columns {
            let col_idx = rng.random_range(0..num_source_columns);
            set.insert(source_.get_name(col_idx).clone());
        }
        set.into_iter()
            .map(|name| PropRelationDescDiff::DropColumn { name })
            .collect::<Vec<_>>()
    });

    let source_ = Rc::clone(&source);
    let toggle_nullability_strat =
        (0..num_source_columns).prop_perturb(move |num_columns, mut rng| {
            let mut set = BTreeSet::default();
            for _ in 0..num_columns {
                let col_idx = rng.random_range(0..num_source_columns);
                set.insert(source_.get_name(col_idx).clone());
            }
            set.into_iter()
                .map(|name| PropRelationDescDiff::ToggleNullability { name })
                .collect::<Vec<_>>()
        });

    let source_ = Rc::clone(&source);
    let change_type_strat = (0..num_source_columns)
        .prop_perturb(move |num_columns, mut rng| {
            let mut set = BTreeSet::default();
            for _ in 0..num_columns {
                let col_idx = rng.random_range(0..num_source_columns);
                set.insert(source_.get_name(col_idx).clone());
            }
            set
        })
        .prop_flat_map(|cols| {
            proptest::collection::vec(any::<ColumnType>(), cols.len())
                .prop_map(move |types| (cols.clone(), types))
        })
        .prop_map(|(cols, types)| {
            cols.into_iter()
                .zip_eq(types)
                .map(|(name, typ)| PropRelationDescDiff::ChangeType { name, typ })
                .collect::<Vec<_>>()
        });

    (
        add_columns_strat,
        drop_columns_strat,
        toggle_nullability_strat,
        change_type_strat,
    )
        .prop_map(|(adds, drops, toggles, changes)| {
            adds.into_iter()
                .chain(drops)
                .chain(toggles)
                .chain(changes)
                .collect::<Vec<_>>()
        })
        .prop_shuffle()
        .boxed()
}

#[cfg(test)]
mod tests {
    use super::*;
    use prost::Message;

    #[mz_ore::test]
    #[cfg_attr(miri, ignore)] // unsupported operation: can't call foreign function `pipe2` on OS `linux`
    fn smoktest_at_version() {
        let desc = RelationDesc::builder()
            .with_column("a", ScalarType::Bool.nullable(true))
            .with_column("z", ScalarType::String.nullable(false))
            .finish();

        let mut versioned_desc = VersionedRelationDesc {
            inner: desc.clone(),
        };
        versioned_desc.validate();

        let latest = versioned_desc.at_version(RelationVersionSelector::Latest);
        assert_eq!(desc, latest);

        let v0 = versioned_desc.at_version(RelationVersionSelector::specific(0));
        assert_eq!(desc, v0);

        let v3 = versioned_desc.at_version(RelationVersionSelector::specific(3));
        assert_eq!(desc, v3);

        let v1 = versioned_desc.add_column("b", ScalarType::Bytes.nullable(false));
        assert_eq!(v1, RelationVersion(1));

        let v1 = versioned_desc.at_version(RelationVersionSelector::Specific(v1));
        insta::assert_json_snapshot!(v1.metadata, @r###"
        {
          "0": {
            "name": "a",
            "typ_idx": 0,
            "added": 0,
            "dropped": null
          },
          "1": {
            "name": "z",
            "typ_idx": 1,
            "added": 0,
            "dropped": null
          },
          "2": {
            "name": "b",
            "typ_idx": 2,
            "added": 1,
            "dropped": null
          }
        }
        "###);

        // Check that V0 doesn't show the new column.
        let v0_b = versioned_desc.at_version(RelationVersionSelector::specific(0));
        assert!(v0.iter().eq(v0_b.iter()));

        let v2 = versioned_desc.drop_column("z");
        assert_eq!(v2, RelationVersion(2));

        let v2 = versioned_desc.at_version(RelationVersionSelector::Specific(v2));
        insta::assert_json_snapshot!(v2.metadata, @r###"
        {
          "0": {
            "name": "a",
            "typ_idx": 0,
            "added": 0,
            "dropped": null
          },
          "2": {
            "name": "b",
            "typ_idx": 1,
            "added": 1,
            "dropped": null
          }
        }
        "###);

        // Check that V0 and V1 are still correct.
        let v0_c = versioned_desc.at_version(RelationVersionSelector::specific(0));
        assert!(v0.iter().eq(v0_c.iter()));

        let v1_b = versioned_desc.at_version(RelationVersionSelector::specific(1));
        assert!(v1.iter().eq(v1_b.iter()));

        insta::assert_json_snapshot!(versioned_desc.inner.metadata, @r###"
        {
          "0": {
            "name": "a",
            "typ_idx": 0,
            "added": 0,
            "dropped": null
          },
          "1": {
            "name": "z",
            "typ_idx": 1,
            "added": 0,
            "dropped": 2
          },
          "2": {
            "name": "b",
            "typ_idx": 2,
            "added": 1,
            "dropped": null
          }
        }
        "###);
    }

    #[mz_ore::test]
    #[cfg_attr(miri, ignore)] // unsupported operation: can't call foreign function `pipe2` on OS `linux`
    fn test_dropping_columns_with_keys() {
        let desc = RelationDesc::builder()
            .with_column("a", ScalarType::Bool.nullable(true))
            .with_column("z", ScalarType::String.nullable(false))
            .with_key(vec![1])
            .finish();

        let mut versioned_desc = VersionedRelationDesc {
            inner: desc.clone(),
        };
        versioned_desc.validate();

        let v1 = versioned_desc.drop_column("a");
        assert_eq!(v1, RelationVersion(1));

        // Make sure the key index for 'z' got remapped since 'a' was dropped.
        let v1 = versioned_desc.at_version(RelationVersionSelector::Specific(v1));
        insta::assert_json_snapshot!(v1, @r###"
        {
          "typ": {
            "column_types": [
              {
                "scalar_type": "String",
                "nullable": false
              }
            ],
            "keys": [
              [
                0
              ]
            ]
          },
          "metadata": {
            "1": {
              "name": "z",
              "typ_idx": 0,
              "added": 0,
              "dropped": null
            }
          }
        }
        "###);

        // Make sure the key index of 'z' is correct when all columns are present.
        let v0 = versioned_desc.at_version(RelationVersionSelector::specific(0));
        insta::assert_json_snapshot!(v0, @r###"
        {
          "typ": {
            "column_types": [
              {
                "scalar_type": "Bool",
                "nullable": true
              },
              {
                "scalar_type": "String",
                "nullable": false
              }
            ],
            "keys": [
              [
                1
              ]
            ]
          },
          "metadata": {
            "0": {
              "name": "a",
              "typ_idx": 0,
              "added": 0,
              "dropped": 1
            },
            "1": {
              "name": "z",
              "typ_idx": 1,
              "added": 0,
              "dropped": null
            }
          }
        }
        "###);
    }

    #[mz_ore::test]
    #[cfg_attr(miri, ignore)] // unsupported operation: can't call foreign function `pipe2` on OS `linux`
    fn roundtrip_relation_desc_without_metadata() {
        let typ = ProtoRelationType {
            column_types: vec![
                ScalarType::String.nullable(false).into_proto(),
                ScalarType::Bool.nullable(true).into_proto(),
            ],
            keys: vec![],
        };
        let proto = ProtoRelationDesc {
            typ: Some(typ),
            names: vec![
                ColumnName("a".into()).into_proto(),
                ColumnName("b".into()).into_proto(),
            ],
            metadata: vec![],
        };
        let desc: RelationDesc = proto.into_rust().unwrap();

        insta::assert_json_snapshot!(desc, @r###"
        {
          "typ": {
            "column_types": [
              {
                "scalar_type": "String",
                "nullable": false
              },
              {
                "scalar_type": "Bool",
                "nullable": true
              }
            ],
            "keys": []
          },
          "metadata": {
            "0": {
              "name": "a",
              "typ_idx": 0,
              "added": 0,
              "dropped": null
            },
            "1": {
              "name": "b",
              "typ_idx": 1,
              "added": 0,
              "dropped": null
            }
          }
        }
        "###);
    }

    #[mz_ore::test]
    #[should_panic(expected = "column named 'a' already exists!")]
    fn test_add_column_with_same_name_panics() {
        let desc = RelationDesc::builder()
            .with_column("a", ScalarType::Bool.nullable(true))
            .finish();
        let mut versioned = VersionedRelationDesc::new(desc);

        let _ = versioned.add_column("a", ScalarType::String.nullable(false));
    }

    #[mz_ore::test]
    #[cfg_attr(miri, ignore)] // unsupported operation: can't call foreign function `pipe2` on OS `linux`
    fn test_add_column_with_same_name_prev_dropped() {
        let desc = RelationDesc::builder()
            .with_column("a", ScalarType::Bool.nullable(true))
            .finish();
        let mut versioned = VersionedRelationDesc::new(desc);

        let v1 = versioned.drop_column("a");
        let v1 = versioned.at_version(RelationVersionSelector::Specific(v1));
        insta::assert_json_snapshot!(v1, @r###"
        {
          "typ": {
            "column_types": [],
            "keys": []
          },
          "metadata": {}
        }
        "###);

        let v2 = versioned.add_column("a", ScalarType::String.nullable(false));
        let v2 = versioned.at_version(RelationVersionSelector::Specific(v2));
        insta::assert_json_snapshot!(v2, @r###"
        {
          "typ": {
            "column_types": [
              {
                "scalar_type": "String",
                "nullable": false
              }
            ],
            "keys": []
          },
          "metadata": {
            "1": {
              "name": "a",
              "typ_idx": 0,
              "added": 2,
              "dropped": null
            }
          }
        }
        "###);
    }

    #[mz_ore::test]
    #[cfg_attr(miri, ignore)]
    fn apply_demand() {
        let desc = RelationDesc::builder()
            .with_column("a", ScalarType::String.nullable(true))
            .with_column("b", ScalarType::Int64.nullable(false))
            .with_column("c", ScalarType::Time.nullable(false))
            .finish();
        let desc = desc.apply_demand(&BTreeSet::from([0, 2]));
        assert_eq!(desc.arity(), 2);
        // TODO(parkmycar): Move validate onto RelationDesc.
        VersionedRelationDesc::new(desc).validate();
    }

    #[mz_ore::test]
    #[cfg_attr(miri, ignore)]
    fn smoketest_column_index_stable_ident() {
        let idx_a = ColumnIndex(42);
        // Note(parkmycar): This should never change.
        assert_eq!(idx_a.to_stable_name(), "42");
    }

    #[mz_ore::test]
    #[cfg_attr(miri, ignore)] // too slow
    fn proptest_relation_desc_roundtrips() {
        fn testcase(og: RelationDesc) {
            let bytes = og.into_proto().encode_to_vec();
            let proto = ProtoRelationDesc::decode(&bytes[..]).unwrap();
            let rnd = RelationDesc::from_proto(proto).unwrap();

            assert_eq!(og, rnd);
        }

        proptest!(|(desc in any::<RelationDesc>())| {
            testcase(desc);
        });

        let strat = any::<RelationDesc>().prop_flat_map(|desc| {
            arb_relation_desc_diff(&desc).prop_map(move |diffs| (desc.clone(), diffs))
        });

        proptest!(|((mut desc, diffs) in strat)| {
            for diff in diffs {
                diff.apply(&mut desc);
            };
            testcase(desc);
        });
    }
}

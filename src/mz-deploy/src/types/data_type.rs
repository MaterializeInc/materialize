// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! The type of a column in a data contract.
//!
//! [`DataType`] is the in-memory form. [`TypeLock`] and [`FieldLock`] are its
//! on-disk form, shared by `types.lock` and the build artifact, and are the
//! only place the tag vocabulary (`array`, `list`, `map`, `record`) is defined.

use serde::{Deserialize, Serialize};
use std::fmt;
use thiserror::Error;

/// On-disk tag for [`DataType::Array`].
const ARRAY_TAG: &str = "array";
/// On-disk tag for [`DataType::List`].
const LIST_TAG: &str = "list";
/// On-disk tag for [`DataType::Map`].
const MAP_TAG: &str = "map";
/// On-disk tag for [`DataType::Record`].
const RECORD_TAG: &str = "record";

/// A column's type, described structurally.
///
/// [`Named`] covers every type the SQL grammar can spell directly. The other
/// variants exist because it cannot: an anonymous record has no data-type
/// syntax at all, and the catalog reports an anonymous list or map without its
/// element type.
///
/// A user-defined type is a [`Named`] holding its fully-qualified name. That
/// qualification is load-bearing on disk: it is what keeps a user type called
/// `record` from colliding with the tag for an anonymous record.
///
/// [`Named`]: DataType::Named
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DataType {
    /// A type spelled directly in SQL, e.g. `integer`, `numeric(39,2)`,
    /// `app.public.my_type`.
    Named(String),
    Array(Box<DataType>),
    List(Box<DataType>),
    /// Materialize map keys are always `text`, so only the value type varies.
    Map(Box<DataType>),
    Record(Vec<RecordField>),
}

/// One field of a [`DataType::Record`].
///
/// Fields carry their own nullability because `SqlScalarType::Record` holds a
/// full column type per field, while its list, map, and array variants hold a
/// bare scalar type.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecordField {
    pub name: String,
    pub r#type: DataType,
    pub nullable: bool,
}

impl DataType {
    /// Shorthand for a leaf type.
    pub fn named(name: impl Into<String>) -> Self {
        DataType::Named(name.into())
    }

    /// Whether a record appears anywhere in this type.
    ///
    /// A schema free of records is expressible as a `CREATE TABLE`; one
    /// containing a record is not, and has to be built out of helper relations.
    pub fn contains_record(&self) -> bool {
        match self {
            DataType::Named(_) => false,
            DataType::Array(inner) | DataType::List(inner) | DataType::Map(inner) => {
                inner.contains_record()
            }
            DataType::Record(_) => true,
        }
    }

    /// Whether this type is one of the pseudo-type tokens the catalog reports
    /// in place of a structural type it cannot spell.
    ///
    /// These are exactly the columns that need a `pg_typeof` probe at capture
    /// time, and the ones no stub can be built from.
    pub fn is_pseudo_token(&self) -> bool {
        match self {
            DataType::Named(name) => name == RECORD_TAG || name == LIST_TAG || name == MAP_TAG,
            _ => false,
        }
    }

    /// Whether a pseudo-type token appears anywhere in this type.
    pub fn contains_pseudo_token(&self) -> bool {
        match self {
            DataType::Named(_) => self.is_pseudo_token(),
            DataType::Array(inner) | DataType::List(inner) | DataType::Map(inner) => {
                inner.contains_pseudo_token()
            }
            DataType::Record(fields) => fields.iter().any(|f| f.r#type.contains_pseudo_token()),
        }
    }
}

/// Renders the type as Materialize humanizes it, which is valid data-type
/// syntax for every variant except [`DataType::Record`].
///
/// This is a display and hashing form. It is never parsed back: structure is
/// recovered from [`TypeLock`], not from this string.
impl fmt::Display for DataType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            DataType::Named(name) => f.write_str(name),
            DataType::Array(inner) => write!(f, "{}[]", inner),
            DataType::List(inner) => write!(f, "{} list", inner),
            DataType::Map(value) => write!(f, "map[text=>{}]", value),
            DataType::Record(fields) => {
                f.write_str("record(")?;
                for (i, field) in fields.iter().enumerate() {
                    if i > 0 {
                        f.write_str(",")?;
                    }
                    write!(f, "{}: {}", field.name, field.r#type)?;
                    if field.nullable {
                        f.write_str("?")?;
                    }
                }
                f.write_str(")")
            }
        }
    }
}

impl DataType {
    /// Serialize for storage in a text column of the build artifact.
    pub(crate) fn to_json(&self) -> String {
        serde_json::to_string(&TypeLock::from_data_type(self))
            .expect("TypeLock is always serializable")
    }

    /// Parse a value written by [`DataType::to_json`].
    pub(crate) fn from_json(json: &str) -> Result<DataType, DataTypeJsonError> {
        serde_json::from_str::<TypeLock>(json)?
            .into_data_type()
            .map_err(DataTypeJsonError::Structure)
    }
}

/// A stored type that could not be read back.
#[derive(Error, Debug)]
pub(crate) enum DataTypeJsonError {
    #[error("malformed stored type")]
    Malformed(#[from] serde_json::Error),
    #[error(transparent)]
    Structure(TypeLockError),
}

/// A structural payload that does not belong on the type it was recorded with.
#[derive(Error, Debug)]
pub enum TypeLockError {
    #[error("type `{tag}` does not take an element type")]
    UnexpectedElement { tag: String },
    #[error("type `{tag}` requires an element type")]
    MissingElement { tag: String },
    #[error("type `{tag}` does not take fields")]
    UnexpectedFields { tag: String },
}

/// On-disk form of a [`DataType`]: the type's name, plus the structural payload
/// the SQL grammar cannot spell.
///
/// `of` carries the element type of an array or list and the value type of a
/// map; `fields` carries a record's fields. Exactly one of them is present, and
/// only for the tags that take it.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub(crate) struct TypeLock {
    #[serde(rename = "type")]
    pub name: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub of: Option<Box<TypeLock>>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub fields: Vec<FieldLock>,
}

/// On-disk form of a [`RecordField`]: a [`TypeLock`] widened with the field's
/// name and nullability.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub(crate) struct FieldLock {
    pub name: String,
    #[serde(rename = "type")]
    pub type_name: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub of: Option<Box<TypeLock>>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub fields: Vec<FieldLock>,
    pub nullable: bool,
}

impl TypeLock {
    pub(crate) fn from_data_type(r#type: &DataType) -> Self {
        let (name, of, fields) = split_data_type(r#type);
        TypeLock { name, of, fields }
    }

    pub(crate) fn into_data_type(self) -> Result<DataType, TypeLockError> {
        join_data_type(self.name, self.of, self.fields)
    }
}

impl FieldLock {
    pub(crate) fn from_record_field(field: &RecordField) -> Self {
        let (type_name, of, fields) = split_data_type(&field.r#type);
        FieldLock {
            name: field.name.clone(),
            type_name,
            of,
            fields,
            nullable: field.nullable,
        }
    }

    pub(crate) fn into_record_field(self) -> Result<RecordField, TypeLockError> {
        Ok(RecordField {
            name: self.name,
            r#type: join_data_type(self.type_name, self.of, self.fields)?,
            nullable: self.nullable,
        })
    }
}

/// Decompose a type into the three keys the on-disk form carries.
///
/// Callers that widen the type with a name and nullability, such as a column
/// entry, use this directly instead of going through [`TypeLock`].
pub(crate) fn split_data_type(
    r#type: &DataType,
) -> (String, Option<Box<TypeLock>>, Vec<FieldLock>) {
    match r#type {
        DataType::Named(name) => (name.clone(), None, Vec::new()),
        DataType::Array(inner) => (
            ARRAY_TAG.into(),
            Some(Box::new(TypeLock::from_data_type(inner))),
            Vec::new(),
        ),
        DataType::List(inner) => (
            LIST_TAG.into(),
            Some(Box::new(TypeLock::from_data_type(inner))),
            Vec::new(),
        ),
        DataType::Map(value) => (
            MAP_TAG.into(),
            Some(Box::new(TypeLock::from_data_type(value))),
            Vec::new(),
        ),
        DataType::Record(fields) => (
            RECORD_TAG.into(),
            None,
            fields.iter().map(FieldLock::from_record_field).collect(),
        ),
    }
}

/// Rebuild a type from the three on-disk keys, rejecting a payload that does
/// not belong on the named tag.
pub(crate) fn join_data_type(
    name: String,
    of: Option<Box<TypeLock>>,
    fields: Vec<FieldLock>,
) -> Result<DataType, TypeLockError> {
    enum Tag {
        Array,
        List,
        Map,
        Record,
        Named,
    }

    let tag = match name.as_str() {
        ARRAY_TAG => Tag::Array,
        LIST_TAG => Tag::List,
        MAP_TAG => Tag::Map,
        RECORD_TAG => Tag::Record,
        _ => Tag::Named,
    };

    let takes_element = matches!(tag, Tag::Array | Tag::List | Tag::Map);
    if of.is_some() && !takes_element {
        return Err(TypeLockError::UnexpectedElement { tag: name });
    }
    if !fields.is_empty() && !matches!(tag, Tag::Record) {
        return Err(TypeLockError::UnexpectedFields { tag: name });
    }

    // A container or record tag with no payload is the pseudo-type token the
    // catalog reports for a type it cannot spell, which `lock` records when it
    // cannot probe the column. Keep it as a `Named` type: the type is then
    // reported against the column that uses it, rather than making the whole
    // file unreadable.
    match tag {
        Tag::List | Tag::Map if of.is_none() => return Ok(DataType::Named(name)),
        Tag::Record if fields.is_empty() => return Ok(DataType::Named(name)),
        _ => {}
    }

    let element = match of {
        Some(of) => Some(Box::new(of.into_data_type()?)),
        None if takes_element => return Err(TypeLockError::MissingElement { tag: name }),
        None => None,
    };

    Ok(match tag {
        // `element` is `Some` for every arm that reads it: `takes_element` is
        // true exactly there, and a missing element already returned above.
        Tag::Array => DataType::Array(element.expect("array element")),
        Tag::List => DataType::List(element.expect("list element")),
        Tag::Map => DataType::Map(element.expect("map value")),
        Tag::Record => DataType::Record(
            fields
                .into_iter()
                .map(FieldLock::into_record_field)
                .collect::<Result<_, _>>()?,
        ),
        Tag::Named => DataType::Named(name),
    })
}

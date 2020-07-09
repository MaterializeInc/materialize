// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Logic for parsing and interacting with schemas in Avro format.
use std::borrow::Cow;
use std::collections::{HashMap, HashSet};
use std::fmt;

use digest::Digest;
use failure::{Error, Fail};
use log::{debug, warn};
use serde::{
    ser::{SerializeMap, SerializeSeq},
    Serialize, Serializer,
};
use serde_json::{self, Map, Value};

use crate::reader::SchemaResolver;
use crate::types;
use crate::util::MapHelper;
use failure::_core::fmt::Formatter;
use itertools::Itertools;
use std::cell::RefCell;
use std::collections::hash_map::Entry;
use std::convert::TryFrom;
use std::fmt::Display;
use std::rc::Rc;
use types::{DecimalValue, Value as AvroValue};

pub fn resolve_schemas(writer_schema: &Schema, reader_schema: &Schema) -> Result<Schema, Error> {
    let r_indices = reader_schema.indices.clone();
    let (reader_to_writer_names, writer_to_reader_names): (HashMap<_, _>, HashMap<_, _>) =
        writer_schema
            .indices
            .iter()
            .flat_map(|(name, widx)| {
                r_indices
                    .get(name)
                    .map(|ridx| ((*ridx, *widx), (*widx, *ridx)))
            })
            .unzip();
    let reader_fullnames = reader_schema
        .indices
        .iter()
        .map(|(f, i)| (*i, f))
        .collect::<HashMap<_, _>>();
    let mut resolver = SchemaResolver {
        named: Default::default(),
        indices: Default::default(),
        writer_to_reader_names,
        reader_to_writer_names,
        reader_to_resolved_names: Default::default(),
        reader_fullnames,
        reader_schema,
    };
    let writer_node = writer_schema.top_node_or_named();
    let reader_node = reader_schema.top_node_or_named();
    let inner = resolver.resolve(writer_node, reader_node)?;
    let sch = Schema {
        named: resolver.named.into_iter().map(Option::unwrap).collect(),
        indices: resolver.indices,
        top: inner,
    };
    Ok(sch)
}

/// Describes errors happened while parsing Avro schemas.
#[derive(Fail, Debug)]
#[fail(display = "Failed to parse schema: {}", _0)]
pub struct ParseSchemaError(String);

impl ParseSchemaError {
    pub fn new<S>(msg: S) -> ParseSchemaError
    where
        S: Into<String>,
    {
        ParseSchemaError(msg.into())
    }
}

/// Represents an Avro schema fingerprint
/// More information about Avro schema fingerprints can be found in the
/// [Avro Schema Fingerprint documentation](https://avro.apache.org/docs/current/spec.html#schema_fingerprints)
pub struct SchemaFingerprint {
    pub bytes: Vec<u8>,
}

impl fmt::Display for SchemaFingerprint {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "{}",
            self.bytes
                .iter()
                .map(|byte| format!("{:02x}", byte))
                .collect::<Vec<String>>()
                .join("")
        )
    }
}

#[derive(Clone, Debug, PartialEq)]
pub enum SchemaPieceOrNamed {
    Piece(SchemaPiece),
    Named(usize),
}

impl SchemaPieceOrNamed {
    #[inline(always)]
    pub fn get_piece_and_name<'a>(
        &'a self,
        root: &'a Schema,
    ) -> (&'a SchemaPiece, Option<&'a FullName>) {
        self.as_ref().get_piece_and_name(root)
    }

    #[inline(always)]
    pub fn as_ref(&self) -> SchemaPieceRefOrNamed {
        match self {
            SchemaPieceOrNamed::Piece(piece) => SchemaPieceRefOrNamed::Piece(piece),
            SchemaPieceOrNamed::Named(index) => SchemaPieceRefOrNamed::Named(*index),
        }
    }
}

impl From<SchemaPiece> for SchemaPieceOrNamed {
    #[inline(always)]
    fn from(piece: SchemaPiece) -> Self {
        Self::Piece(piece)
    }
}

#[derive(Clone, Debug, PartialEq)]
pub enum SchemaPiece {
    /// A `null` Avro schema.
    Null,
    /// A `boolean` Avro schema.
    Boolean,
    /// An `int` Avro schema.
    Int,
    /// A `long` Avro schema.
    Long,
    /// A `float` Avro schema.
    Float,
    /// A `double` Avro schema.
    Double,
    /// An `Int` Avro schema with a semantic type being days since the unix epoch.
    Date,
    /// An `Int64` Avro schema with a semantic type being milliseconds since the unix epoch.
    ///
    /// https://avro.apache.org/docs/current/spec.html#Timestamp+%28millisecond+precision%29
    TimestampMilli,
    /// An `Int64` Avro schema with a semantic type being microseconds since the unix epoch.
    ///
    /// https://avro.apache.org/docs/current/spec.html#Timestamp+%28microsecond+precision%29
    TimestampMicro,
    /// A `bytes` Avro schema with a logical type of `decimal` and
    /// the specified precision and scale.
    ///
    /// If the underlying type is `fixed`,
    /// the `fixed_size` field specifies the size.
    Decimal {
        precision: usize,
        scale: usize,
        fixed_size: Option<usize>,
    },
    /// A `bytes` Avro schema.
    /// `Bytes` represents a sequence of 8-bit unsigned bytes.
    Bytes,
    /// A `string` Avro schema.
    /// `String` represents a unicode character sequence.
    String,
    /// A `string` Avro schema that is tagged as representing JSON data
    Json,
    /// A `array` Avro schema. Avro arrays are required to have the same type for each element.
    /// This variant holds the `Schema` for the array element type.
    Array(Box<SchemaPieceOrNamed>),
    /// A `map` Avro schema.
    /// `Map` holds a pointer to the `Schema` of its values, which must all be the same schema.
    /// `Map` keys are assumed to be `string`.
    Map(Box<SchemaPieceOrNamed>),
    /// A `union` Avro schema.
    Union(UnionSchema),
    /// A value written as `int` and read as `long`
    ResolveIntLong,
    /// A value written as `int` and read as `float`
    ResolveIntFloat,
    /// A value written as `int` and read as `double`
    ResolveIntDouble,
    /// A value written as `long` and read as `float`
    ResolveLongFloat,
    /// A value written as `long` and read as `double`
    ResolveLongDouble,
    /// A value written as `float` and read as `double`
    ResolveFloatDouble,
    /// A concrete (i.e., non-`union`) type in the writer,
    /// resolved against one specific variant of a `union` in the writer.
    ResolveConcreteUnion {
        /// The index of the variant in the reader
        index: usize,
        /// The concrete type
        inner: Box<SchemaPieceOrNamed>,
    },
    /// A union in the writer, resolved against a union in the reader.
    /// The two schemas may have different variants and the variants may be in a different order.
    ResolveUnionUnion {
        /// A mapping of the fields in the writer to those in the reader.
        /// If the `i`th element is `None`, the `i`th field in the writer
        /// did not match any field in the reader (or even if it matched by name, resolution failed).
        /// If the `i`th element is `(j, piece)`, then the `i`th field of the writer
        /// matched the `j`th field of the reader, and `piece` is their resolved node.
        permutation: Vec<Option<(usize, SchemaPieceOrNamed)>>,
    },
    /// The inverse of `ResolveConcreteUnion`
    ResolveUnionConcrete {
        index: usize,
        inner: Box<SchemaPieceOrNamed>,
    },
    /// A `record` Avro schema.
    ///
    /// The `lookup` table maps field names to their position in the `Vec`
    /// of `fields`.
    Record {
        doc: Documentation,
        fields: Vec<RecordField>,
        lookup: HashMap<String, usize>,
    },
    /// An `enum` Avro schema.
    Enum {
        doc: Documentation,
        symbols: Vec<String>,
    },
    /// A `fixed` Avro schema.
    Fixed { size: usize },
    /// A record in the writer, resolved against a record in the reader.
    /// The two schemas may have different fields and the fields may be in a different order.
    ResolveRecord {
        /// Fields that do not exist in the writer schema, but had a default
        /// value specified in the reader schema, which we use.
        defaults: Vec<ResolvedDefaultValueField>,
        /// Fields in the order of their appearance in the writer schema.
        /// `Present` if they could be resolved against a field in the reader schema;
        /// `Absent` otherwise.
        fields: Vec<ResolvedRecordField>,
        /// The size of `defaults`, plus the number of `Present` values in `fields`.
        n_reader_fields: usize,
    },
    /// An enum in the writer, resolved against an enum in the reader.
    /// The two schemas may have different values and the values may be in a different order.
    ResolveEnum {
        doc: Documentation,
        /// Symbols in the reader schema if they exist in the writer schema,
        /// or `None` otherwise.
        symbols: Vec<Option<String>>,
        // TODO(brennan) - These should support default values
    },
}

/// Represents any valid Avro schema
/// More information about Avro schemas can be found in the
/// [Avro Specification](https://avro.apache.org/docs/current/spec.html#schemas)
#[derive(Clone, Debug, PartialEq)]
pub struct Schema {
    pub(crate) named: Vec<NamedSchemaPiece>,
    pub(crate) indices: HashMap<FullName, usize>,
    pub top: SchemaPieceOrNamed,
}

impl Schema {
    pub fn top_node(&self) -> SchemaNode {
        let (inner, name) = self.top.get_piece_and_name(self);
        SchemaNode {
            root: self,
            inner,
            name,
        }
    }
    pub fn top_node_or_named(&self) -> SchemaNodeOrNamed {
        SchemaNodeOrNamed {
            root: self,
            inner: self.top.as_ref(),
        }
    }
    pub fn lookup(&self, idx: usize) -> &NamedSchemaPiece {
        &self.named[idx]
    }
}

/// This type is used to simplify enum variant comparison between `Schema` and `types::Value`.
/// It may have utility as part of the public API, but defining as `pub(crate)` for now.
///
/// **NOTE** This type was introduced due to a limitation of `mem::discriminant` requiring a _value_
/// be constructed in order to get the discriminant, which makes it difficult to implement a
/// function that maps from `Discriminant<Schema> -> Discriminant<Value>`. Conversion into this
/// intermediate type should be especially fast, as the number of enum variants is small, which
/// _should_ compile into a jump-table for the conversion.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) enum SchemaKind {
    // Fixed-length types
    Null,
    Boolean,
    Int,
    Long,
    Float,
    Date,
    DateTime,
    Double,
    // Variable-length types
    Bytes,
    Decimal,
    String,
    Array,
    Map,
    Union,
    Record,
    Enum,
    Fixed,
    // This can arise in resolved schemas, particularly when a union resolves to a non-union.
    // We would need to do a lookup to find the actual type.
    Unknown,
}

impl<'a> From<&'a SchemaPiece> for SchemaKind {
    #[inline(always)]
    fn from(piece: &'a SchemaPiece) -> SchemaKind {
        match piece {
            SchemaPiece::Null => SchemaKind::Null,
            SchemaPiece::Boolean => SchemaKind::Boolean,
            SchemaPiece::Int => SchemaKind::Int,
            SchemaPiece::Long => SchemaKind::Long,
            SchemaPiece::Float => SchemaKind::Float,
            SchemaPiece::Double => SchemaKind::Double,
            SchemaPiece::Date => SchemaKind::Date,
            SchemaPiece::TimestampMilli | SchemaPiece::TimestampMicro => SchemaKind::DateTime,
            SchemaPiece::Decimal { .. } => SchemaKind::Decimal,
            SchemaPiece::Bytes => SchemaKind::Bytes,
            SchemaPiece::String => SchemaKind::String,
            SchemaPiece::Array(_) => SchemaKind::Array,
            SchemaPiece::Map(_) => SchemaKind::Map,
            SchemaPiece::Union(_) => SchemaKind::Union,
            SchemaPiece::ResolveUnionUnion { .. } => SchemaKind::Union,
            SchemaPiece::ResolveIntLong => SchemaKind::Long,
            SchemaPiece::ResolveIntFloat => SchemaKind::Float,
            SchemaPiece::ResolveIntDouble => SchemaKind::Double,
            SchemaPiece::ResolveLongFloat => SchemaKind::Float,
            SchemaPiece::ResolveLongDouble => SchemaKind::Double,
            SchemaPiece::ResolveFloatDouble => SchemaKind::Double,
            SchemaPiece::ResolveConcreteUnion { .. } => SchemaKind::Union,
            SchemaPiece::ResolveUnionConcrete { inner: _, .. } => SchemaKind::Unknown,
            SchemaPiece::Record { .. } => SchemaKind::Record,
            SchemaPiece::Enum { .. } => SchemaKind::Enum,
            SchemaPiece::Fixed { .. } => SchemaKind::Fixed,
            SchemaPiece::ResolveRecord { .. } => SchemaKind::Record,
            SchemaPiece::ResolveEnum { .. } => SchemaKind::Enum,
            SchemaPiece::Json => SchemaKind::String,
        }
    }
}

impl<'a> From<SchemaNode<'a>> for SchemaKind {
    #[inline(always)]
    fn from(schema: SchemaNode<'a>) -> SchemaKind {
        SchemaKind::from(schema.inner)
    }
}

impl<'a> From<&'a Schema> for SchemaKind {
    #[inline(always)]
    fn from(schema: &'a Schema) -> SchemaKind {
        Self::from(schema.top_node())
    }
}

impl<'a> From<&'a types::Value> for SchemaKind {
    #[inline(always)]
    fn from(value: &'a types::Value) -> SchemaKind {
        match value {
            types::Value::Null => SchemaKind::Null,
            types::Value::Boolean(_) => SchemaKind::Boolean,
            types::Value::Int(_) => SchemaKind::Int,
            types::Value::Long(_) => SchemaKind::Long,
            types::Value::Float(_) => SchemaKind::Float,
            types::Value::Double(_) => SchemaKind::Double,
            types::Value::Date(_) => SchemaKind::Date,
            types::Value::Timestamp(_) => SchemaKind::DateTime,
            // Variable-length types
            types::Value::Decimal { .. } => SchemaKind::Decimal,
            types::Value::Bytes(_) => SchemaKind::Bytes,
            types::Value::String(_) => SchemaKind::String,
            types::Value::Array(_) => SchemaKind::Array,
            types::Value::Map(_) => SchemaKind::Map,
            types::Value::Union(_, _) => SchemaKind::Union,
            types::Value::Record(_) => SchemaKind::Record,
            types::Value::Enum(_, _) => SchemaKind::Enum,
            types::Value::Fixed(_, _) => SchemaKind::Fixed,
            types::Value::Json(_) => SchemaKind::String,
        }
    }
}

/// Represents names for `record`, `enum` and `fixed` Avro schemas.
///
/// Each of these `Schema`s have a `fullname` composed of two parts:
///   * a name
///   * a namespace
///
/// `aliases` can also be defined, to facilitate schema evolution.
///
/// More information about schema names can be found in the
/// [Avro specification](https://avro.apache.org/docs/current/spec.html#names)
#[derive(Clone, Debug, PartialEq)]
pub struct Name {
    pub name: String,
    pub namespace: Option<String>,
    pub aliases: Option<Vec<String>>,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
pub struct FullName {
    name: String,
    namespace: String,
}

impl FullName {
    pub fn from_parts(name: &str, namespace: Option<&str>, default_namespace: &str) -> FullName {
        if let Some(ns) = namespace {
            FullName {
                name: name.to_owned(),
                namespace: ns.to_owned(),
            }
        } else {
            let mut split = name.rsplitn(2, '.');
            let name = split.next().unwrap();
            let namespace = split.next().unwrap_or(default_namespace);

            FullName {
                name: name.into(),
                namespace: namespace.into(),
            }
        }
    }
}

impl Display for FullName {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}.{}", self.namespace, self.name)
    }
}

/// Represents documentation for complex Avro schemas.
pub type Documentation = Option<String>;

impl Name {
    /// Create a new `Name`.
    /// No `namespace` nor `aliases` will be defined.
    pub fn new(name: &str) -> Name {
        Name {
            name: name.to_owned(),
            namespace: None,
            aliases: None,
        }
    }

    /// Parse a `serde_json::Value` into a `Name`.
    fn parse(complex: &Map<String, Value>) -> Result<Self, Error> {
        let name = complex
            .name()
            .ok_or_else(|| ParseSchemaError::new("No `name` field"))?;

        let namespace = complex.string("namespace");

        let aliases: Option<Vec<String>> = complex
            .get("aliases")
            .and_then(|aliases| aliases.as_array())
            .and_then(|aliases| {
                aliases
                    .iter()
                    .map(|alias| alias.as_str())
                    .map(|alias| alias.map(|a| a.to_string()))
                    .collect::<Option<_>>()
            });
        if let Some(ns) = &namespace {
            if name.find('.').is_some() {
                return Err(ParseSchemaError::new(format!(
                    "Name {} has dot, but namespace also specified: {}",
                    name, ns
                ))
                .into());
            }
        }

        Ok(Name {
            name,
            namespace,
            aliases,
        })
    }

    /// Return the `fullname` of this `Name`
    ///
    /// More information about fullnames can be found in the
    /// [Avro specification](https://avro.apache.org/docs/current/spec.html#names)
    pub fn fullname(&self, default_namespace: &str) -> FullName {
        FullName::from_parts(&self.name, self.namespace.as_deref(), default_namespace)
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct ResolvedDefaultValueField {
    pub name: String,
    pub doc: Documentation,
    pub default: types::Value,
    pub order: RecordFieldOrder,
    pub position: usize,
}

#[derive(Clone, Debug, PartialEq)]
pub enum ResolvedRecordField {
    Absent(Schema),
    Present(RecordField),
}

/// Represents a `field` in a `record` Avro schema.
#[derive(Clone, Debug, PartialEq)]
pub struct RecordField {
    /// Name of the field.
    pub name: String,
    /// Documentation of the field.
    pub doc: Documentation,
    /// Default value of the field.
    /// This value will be used when reading Avro datum if schema resolution
    /// is enabled.
    pub default: Option<Value>,
    /// Schema of the field.
    pub schema: SchemaPieceOrNamed,
    /// Order of the field.
    ///
    /// **NOTE** This currently has no effect.
    pub order: RecordFieldOrder,
    /// Position of the field in the list of `field` of its parent `Schema`
    pub position: usize,
}

/// Represents any valid order for a `field` in a `record` Avro schema.
#[derive(Copy, Clone, Debug, PartialEq)]
pub enum RecordFieldOrder {
    Ascending,
    Descending,
    Ignore,
}

impl RecordField {}

#[derive(Debug, Clone)]
pub struct UnionSchema {
    schemas: Vec<SchemaPieceOrNamed>,

    // Used to ensure uniqueness of anonymous schema inputs, and provide constant time finding of the
    // schema index given a value.
    anon_variant_index: HashMap<SchemaKind, usize>,

    // Same as above, for named input references
    named_variant_index: HashMap<usize, usize>,
}

impl UnionSchema {
    pub(crate) fn new(schemas: Vec<SchemaPieceOrNamed>) -> Result<Self, Error> {
        let mut avindex = HashMap::new();
        let mut nvindex = HashMap::new();
        for (i, schema) in schemas.iter().enumerate() {
            match schema {
                SchemaPieceOrNamed::Piece(sp) => {
                    if let SchemaPiece::Union(_) = sp {
                        return Err(ParseSchemaError::new(
                            "Unions may not directly contain a union",
                        )
                        .into());
                    }
                    let kind = SchemaKind::from(sp);
                    if avindex.insert(kind, i).is_some() {
                        return Err(
                            ParseSchemaError::new("Unions cannot contain duplicate types").into(),
                        );
                    }
                }
                SchemaPieceOrNamed::Named(idx) => {
                    if nvindex.insert(*idx, i).is_some() {
                        return Err(
                            ParseSchemaError::new("Unions cannot contain duplicate types").into(),
                        );
                    }
                }
            }
        }
        Ok(UnionSchema {
            schemas,
            anon_variant_index: avindex,
            named_variant_index: nvindex,
        })
    }

    /// Returns a slice to all variants of this schema.
    pub fn variants(&self) -> &[SchemaPieceOrNamed] {
        &self.schemas
    }

    /// Returns true if the first variant of this `UnionSchema` is `Null`.
    pub fn is_nullable(&self) -> bool {
        !self.schemas.is_empty() && self.schemas[0] == SchemaPieceOrNamed::Piece(SchemaPiece::Null)
    }

    pub fn resolve_piece(&self, sp: &SchemaPiece) -> Option<(usize, &SchemaPieceOrNamed)> {
        self.anon_variant_index
            .get(&SchemaKind::from(sp))
            .map(|idx| (*idx, &self.schemas[*idx]))
    }

    pub fn resolve_ref(
        &self,
        other: SchemaPieceRefOrNamed,
        names_map: &HashMap<usize, usize>,
    ) -> Option<(usize, &SchemaPieceOrNamed)> {
        match other {
            SchemaPieceRefOrNamed::Piece(sp) => self.resolve_piece(sp),
            SchemaPieceRefOrNamed::Named(idx) => names_map
                .get(&idx)
                .and_then(|idx| self.named_variant_index.get(idx))
                .map(|idx| (*idx, &self.schemas[*idx])),
        }
    }

    #[inline(always)]
    pub fn resolve(
        &self,
        other: &SchemaPieceOrNamed,
        names_map: &HashMap<usize, usize>,
    ) -> Option<(usize, &SchemaPieceOrNamed)> {
        self.resolve_ref(other.as_ref(), names_map)
    }
}

// No need to compare variant_index, it is derivative of schemas.
impl PartialEq for UnionSchema {
    fn eq(&self, other: &UnionSchema) -> bool {
        self.schemas.eq(&other.schemas)
    }
}

#[derive(Default)]
struct SchemaParser {
    named: Vec<Option<NamedSchemaPiece>>,
    indices: HashMap<FullName, usize>,
}

impl SchemaParser {
    fn parse(mut self, value: &Value) -> Result<Schema, Error> {
        let top = self.parse_inner("", value)?;
        let SchemaParser { named, indices } = self;
        Ok(Schema {
            named: named.into_iter().map(|o| o.unwrap()).collect(),
            indices,
            top,
        })
    }

    fn parse_inner(
        &mut self,
        default_namespace: &str,
        value: &Value,
    ) -> Result<SchemaPieceOrNamed, Error> {
        match *value {
            Value::String(ref t) => {
                let name = FullName::from_parts(t.as_str(), None, default_namespace);
                if let Some(idx) = self.indices.get(&name) {
                    Ok(SchemaPieceOrNamed::Named(*idx))
                } else {
                    Ok(SchemaPieceOrNamed::Piece(Schema::parse_primitive(
                        t.as_str(),
                    )?))
                }
            }
            Value::Object(ref data) => self.parse_complex(default_namespace, data),
            Value::Array(ref data) => Ok(SchemaPieceOrNamed::Piece(
                self.parse_union(default_namespace, data)?,
            )),
            _ => Err(ParseSchemaError::new("Must be a JSON string, object or array").into()),
        }
    }

    fn alloc_name(&mut self, fullname: FullName) -> Result<usize, Error> {
        let idx = match self.indices.entry(fullname) {
            Entry::Vacant(ve) => *ve.insert(self.named.len()),
            Entry::Occupied(oe) => {
                return Err(ParseSchemaError::new(format!(
                    "Sub-schema with name {} encountered multiple times",
                    oe.key()
                ))
                .into())
            }
        };
        self.named.push(None);
        Ok(idx)
    }

    fn insert(&mut self, index: usize, schema: NamedSchemaPiece) {
        assert!(self.named[index].is_none());
        self.named[index] = Some(schema);
    }

    fn parse_named_type(
        &mut self,
        type_name: &str,
        default_namespace: &str,
        complex: &Map<String, Value>,
    ) -> Result<usize, Error> {
        let name = Name::parse(complex)?;
        match name.name.as_str() {
            "null" | "boolean" | "int" | "long" | "float" | "double" | "bytes" | "string" => {
                return Err(ParseSchemaError::new(format!(
                    "{} may not be used as a custom type name",
                    name.name
                ))
                .into())
            }
            _ => {}
        };
        let fullname = name.fullname(default_namespace);
        let default_namespace = fullname.namespace.clone();
        let idx = self.alloc_name(fullname.clone())?;
        let piece = match type_name {
            "record" => self.parse_record(&default_namespace, complex),
            "enum" => self.parse_enum(complex),
            "fixed" => self.parse_fixed(&default_namespace, complex),
            _ => unreachable!("Unknown named type kind: {}", type_name),
        }?;

        self.insert(
            idx,
            NamedSchemaPiece {
                name: fullname,
                piece,
            },
        );

        Ok(idx)
    }

    /// Parse a `serde_json::Value` representing a complex Avro type into a
    /// `Schema`.
    ///
    /// Avro supports "recursive" definition of types.
    /// e.g: {"type": {"type": "string"}}
    fn parse_complex(
        &mut self,
        default_namespace: &str,
        complex: &Map<String, Value>,
    ) -> Result<SchemaPieceOrNamed, Error> {
        match complex.get("type") {
            Some(&Value::String(ref t)) => Ok(match t.as_str() {
                "record" | "enum" | "fixed" => SchemaPieceOrNamed::Named(self.parse_named_type(
                    t,
                    default_namespace,
                    complex,
                )?),
                "array" => SchemaPieceOrNamed::Piece(self.parse_array(default_namespace, complex)?),
                "map" => SchemaPieceOrNamed::Piece(self.parse_map(default_namespace, complex)?),
                "bytes" => SchemaPieceOrNamed::Piece(Self::parse_bytes(complex)?),
                "int" => SchemaPieceOrNamed::Piece(Self::parse_int(complex)?),
                "long" => SchemaPieceOrNamed::Piece(Self::parse_long(complex)?),
                "string" => SchemaPieceOrNamed::Piece(Self::parse_string(complex)),
                other => {
                    let name = FullName {
                        name: other.into(),
                        namespace: default_namespace.into(),
                    };
                    if let Some(idx) = self.indices.get(&name) {
                        SchemaPieceOrNamed::Named(*idx)
                    } else {
                        SchemaPieceOrNamed::Piece(Schema::parse_primitive(t.as_str())?)
                    }
                }
            }),
            Some(&Value::Object(ref data)) => match data.get("type") {
                Some(ref value) => self.parse_inner(default_namespace, value),
                None => Err(
                    ParseSchemaError::new(format!("Unknown complex type: {:?}", complex)).into(),
                ),
            },
            _ => Err(ParseSchemaError::new("No `type` in complex type").into()),
        }
    }

    /// Parse a `serde_json::Value` representing a Avro record type into a
    /// `Schema`.
    fn parse_record(
        &mut self,
        default_namespace: &str,
        complex: &Map<String, Value>,
    ) -> Result<SchemaPiece, Error> {
        let mut lookup = HashMap::new();

        let fields: Vec<RecordField> = complex
            .get("fields")
            .and_then(|fields| fields.as_array())
            .ok_or_else(|| ParseSchemaError::new("No `fields` in record").into())
            .and_then(|fields| {
                fields
                    .iter()
                    .filter_map(|field| field.as_object())
                    .enumerate()
                    .map(|(position, field)| {
                        self.parse_record_field(default_namespace, field, position)
                    })
                    .collect::<Result<_, _>>()
            })?;

        for field in &fields {
            lookup.insert(field.name.clone(), field.position);
        }

        Ok(SchemaPiece::Record {
            doc: complex.doc(),
            fields,
            lookup,
        })
    }

    /// Parse a `serde_json::Value` into a `RecordField`.
    fn parse_record_field(
        &mut self,
        default_namespace: &str,
        field: &Map<String, Value>,
        position: usize,
    ) -> Result<RecordField, Error> {
        let name = field
            .name()
            .ok_or_else(|| ParseSchemaError::new("No `name` in record field"))?;

        let schema = field
            .get("type")
            .ok_or_else(|| ParseSchemaError::new("No `type` in record field").into())
            .and_then(|type_| self.parse_inner(default_namespace, type_))?;

        let default = field.get("default").cloned();

        let order = field
            .get("order")
            .and_then(|order| order.as_str())
            .and_then(|order| match order {
                "ascending" => Some(RecordFieldOrder::Ascending),
                "descending" => Some(RecordFieldOrder::Descending),
                "ignore" => Some(RecordFieldOrder::Ignore),
                _ => None,
            })
            .unwrap_or_else(|| RecordFieldOrder::Ascending);

        Ok(RecordField {
            name,
            doc: field.doc(),
            default,
            schema,
            order,
            position,
        })
    }

    /// Parse a `serde_json::Value` representing a Avro enum type into a
    /// `Schema`.
    fn parse_enum(&mut self, complex: &Map<String, Value>) -> Result<SchemaPiece, Error> {
        let symbols: Vec<String> = complex
            .get("symbols")
            .and_then(|v| v.as_array())
            .ok_or_else(|| ParseSchemaError::new("No `symbols` field in enum"))
            .and_then(|symbols| {
                symbols
                    .iter()
                    .map(|symbol| symbol.as_str().map(|s| s.to_string()))
                    .collect::<Option<_>>()
                    .ok_or_else(|| ParseSchemaError::new("Unable to parse `symbols` in enum"))
            })?;

        let mut unique_symbols: HashSet<&String> = HashSet::new();
        for symbol in symbols.iter() {
            if unique_symbols.contains(symbol) {
                return Err(ParseSchemaError::new(format!(
                    "Enum symbols must be unique, found multiple: {}",
                    symbol
                ))
                .into());
            } else {
                unique_symbols.insert(symbol);
            }
        }

        Ok(SchemaPiece::Enum {
            doc: complex.doc(),
            symbols,
        })
    }

    /// Parse a `serde_json::Value` representing a Avro array type into a
    /// `Schema`.
    fn parse_array(
        &mut self,
        default_namespace: &str,
        complex: &Map<String, Value>,
    ) -> Result<SchemaPiece, Error> {
        complex
            .get("items")
            .ok_or_else(|| ParseSchemaError::new("No `items` in array").into())
            .and_then(|items| self.parse_inner(default_namespace, items))
            .map(|schema| SchemaPiece::Array(Box::new(schema)))
    }

    /// Parse a `serde_json::Value` representing a Avro map type into a
    /// `Schema`.
    fn parse_map(
        &mut self,
        default_namespace: &str,
        complex: &Map<String, Value>,
    ) -> Result<SchemaPiece, Error> {
        complex
            .get("values")
            .ok_or_else(|| ParseSchemaError::new("No `values` in map").into())
            .and_then(|items| self.parse_inner(default_namespace, items))
            .map(|schema| SchemaPiece::Map(Box::new(schema)))
    }

    /// Parse a `serde_json::Value` representing a Avro union type into a
    /// `Schema`.
    fn parse_union(
        &mut self,
        default_namespace: &str,
        items: &[Value],
    ) -> Result<SchemaPiece, Error> {
        items
            .iter()
            .map(|value| self.parse_inner(default_namespace, value))
            .collect::<Result<Vec<_>, _>>()
            .and_then(|schemas| Ok(SchemaPiece::Union(UnionSchema::new(schemas)?)))
    }

    /// Parse a `serde_json::Value` representing a logical decimal type into a
    /// `Schema`.
    fn parse_decimal(complex: &Map<String, Value>) -> Result<(usize, usize), Error> {
        let precision = complex
            .get("precision")
            .and_then(|v| v.as_i64())
            .ok_or_else(|| ParseSchemaError::new("No `precision` in decimal"))?;

        let scale = complex.get("scale").and_then(|v| v.as_i64()).unwrap_or(0);

        if scale < 0 {
            return Err(ParseSchemaError::new("Decimal scale must be greater than zero").into());
        }

        if precision < 0 {
            return Err(
                ParseSchemaError::new("Decimal precision must be greater than zero").into(),
            );
        }

        if scale > precision {
            return Err(ParseSchemaError::new("Decimal scale is greater than precision").into());
        }

        Ok((precision as usize, scale as usize))
    }

    /// Parse a `serde_json::Value` representing an Avro bytes type into a
    /// `Schema`.
    fn parse_bytes(complex: &Map<String, Value>) -> Result<SchemaPiece, Error> {
        let logical_type = complex.get("logicalType").and_then(|v| v.as_str());

        Ok(match logical_type {
            Some("decimal") => {
                let (precision, scale) = Self::parse_decimal(complex)?;
                SchemaPiece::Decimal {
                    precision,
                    scale,
                    fixed_size: None,
                }
            }
            _ => {
                debug!("parsing complex type as regular bytes: {:?}", complex);
                SchemaPiece::Bytes
            }
        })
    }

    /// Parse a [`serde_json::Value`] representing an Avro Int type
    ///
    /// If the complex type has a `connect.name` tag (as [emitted by
    /// Debezium][1]) that matches a `Date` tag, we specify that the correct
    /// schema to use is `Date`.
    ///
    /// [1]: https://debezium.io/docs/connectors/mysql/#temporal-values
    fn parse_int(complex: &Map<String, Value>) -> Result<SchemaPiece, Error> {
        const AVRO_DATE: &str = "date";
        const DEBEZIUM_DATE: &str = "io.debezium.time.Date";
        const KAFKA_DATE: &str = "org.apache.kafka.connect.data.Date";
        if let Some(name) = complex.get("connect.name") {
            if name == DEBEZIUM_DATE || name == KAFKA_DATE {
                if name == KAFKA_DATE {
                    warn!("using deprecated debezium date format");
                }
                return Ok(SchemaPiece::Date);
            }
        }
        // Put this after the custom semantic types so that the debezium
        // warning is emitted, since the logicalType tag shows up in the
        // deprecated debezium format :-/
        if let Some(name) = complex.get("logicalType") {
            if name == AVRO_DATE {
                return Ok(SchemaPiece::Date);
            }
        }
        if !complex.is_empty() {
            debug!("parsing complex type as regular int: {:?}", complex);
        }
        Ok(SchemaPiece::Int)
    }

    /// Parse a [`serde_json::Value`] representing an Avro Int64/Long type
    ///
    /// The debezium/kafka types are document at [the debezium site][1], and the
    /// avro ones are documented at [Avro][2].
    ///
    /// [1]: https://debezium.io/docs/connectors/mysql/#temporal-values
    /// [2]: https://avro.apache.org/docs/1.9.0/spec.html
    fn parse_long(complex: &Map<String, Value>) -> Result<SchemaPiece, Error> {
        const AVRO_MILLI_TS: &str = "timestamp-millis";
        const AVRO_MICRO_TS: &str = "timestamp-micros";

        const CONNECT_MILLI_TS: &[&str] = &[
            "io.debezium.time.Timestamp",
            "org.apache.kafka.connect.data.Timestamp",
        ];
        const CONNECT_MICRO_TS: &str = "io.debezium.time.MicroTimestamp";

        if let Some(serde_json::Value::String(name)) = complex.get("connect.name") {
            if CONNECT_MILLI_TS.contains(&&**name) {
                return Ok(SchemaPiece::TimestampMilli);
            }
            if name == CONNECT_MICRO_TS {
                return Ok(SchemaPiece::TimestampMicro);
            }
        }
        if let Some(name) = complex.get("logicalType") {
            if name == AVRO_MILLI_TS {
                return Ok(SchemaPiece::TimestampMilli);
            }
            if name == AVRO_MICRO_TS {
                return Ok(SchemaPiece::TimestampMicro);
            }
        }
        if !complex.is_empty() {
            debug!("parsing complex type as regular long: {:?}", complex);
        }
        Ok(SchemaPiece::Long)
    }

    fn parse_string(complex: &Map<String, Value>) -> SchemaPiece {
        const CONNECT_JSON: &str = "io.debezium.data.Json";

        if let Some(serde_json::Value::String(name)) = complex.get("connect.name") {
            if CONNECT_JSON == name.as_str() {
                return SchemaPiece::Json;
            }
        }
        debug!("parsing complex type as regular string: {:?}", complex);
        SchemaPiece::String
    }

    /// Parse a `serde_json::Value` representing a Avro fixed type into a
    /// `Schema`.
    fn parse_fixed(
        &mut self,
        _default_namespace: &str,
        complex: &Map<String, Value>,
    ) -> Result<SchemaPiece, Error> {
        let _name = Name::parse(complex)?;

        let size = complex
            .get("size")
            .and_then(|v| v.as_i64())
            .ok_or_else(|| ParseSchemaError::new("No `size` in fixed"))?;

        let logical_type = complex.get("logicalType").and_then(|v| v.as_str());

        Ok(match logical_type {
            Some("decimal") => {
                let (precision, scale) = Self::parse_decimal(complex)?;
                let max = ((2_usize.pow((8 * size - 1) as u32) - 1) as f64).log10() as usize;
                if precision > max {
                    return Err(ParseSchemaError::new(format!(
                        "Decimal precision {} requires more than {} bytes of space",
                        precision, size,
                    ))
                    .into());
                }
                SchemaPiece::Decimal {
                    precision,
                    scale,
                    fixed_size: Some(size as usize),
                }
            }
            _ => SchemaPiece::Fixed {
                size: size as usize,
            },
        })
    }
}

impl Schema {
    /// Create a `Schema` from a string representing a JSON Avro schema.
    pub fn parse_str(input: &str) -> Result<Self, Error> {
        let value = serde_json::from_str(input)?;
        Self::parse(&value)
    }

    /// Create a `Schema` from a `serde_json::Value` representing a JSON Avro
    /// schema.
    pub fn parse(value: &Value) -> Result<Self, Error> {
        let p = SchemaParser {
            named: vec![],
            indices: Default::default(),
        };
        p.parse(value)
    }

    /// Converts `self` into its [Parsing Canonical Form].
    ///
    /// [Parsing Canonical Form]:
    /// https://avro.apache.org/docs/1.8.2/spec.html#Parsing+Canonical+Form+for+Schemas
    pub fn canonical_form(&self) -> String {
        let json = serde_json::to_value(self).unwrap();
        parsing_canonical_form(&json)
    }

    /// Generate [fingerprint] of Schema's [Parsing Canonical Form].
    ///
    /// [Parsing Canonical Form]:
    /// https://avro.apache.org/docs/1.8.2/spec.html#Parsing+Canonical+Form+for+Schemas
    /// [fingerprint]:
    /// https://avro.apache.org/docs/current/spec.html#schema_fingerprints
    pub fn fingerprint<D: Digest>(&self) -> SchemaFingerprint {
        let mut d = D::new();
        d.input(self.canonical_form());
        SchemaFingerprint {
            bytes: d.result().to_vec(),
        }
    }

    /// Parse a `serde_json::Value` representing a primitive Avro type into a
    /// `Schema`.
    fn parse_primitive(primitive: &str) -> Result<SchemaPiece, Error> {
        match primitive {
            "null" => Ok(SchemaPiece::Null),
            "boolean" => Ok(SchemaPiece::Boolean),
            "int" => Ok(SchemaPiece::Int),
            "long" => Ok(SchemaPiece::Long),
            "double" => Ok(SchemaPiece::Double),
            "float" => Ok(SchemaPiece::Float),
            "bytes" => Ok(SchemaPiece::Bytes),
            "string" => Ok(SchemaPiece::String),
            other => Err(ParseSchemaError::new(format!("Unknown type: {}", other)).into()),
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct NamedSchemaPiece {
    pub(crate) name: FullName,
    pub(crate) piece: SchemaPiece,
}

#[derive(Copy, Clone, Debug)]
pub struct SchemaNode<'a> {
    pub root: &'a Schema,
    pub inner: &'a SchemaPiece,
    pub name: Option<&'a FullName>,
}

#[derive(Copy, Clone)]
pub enum SchemaPieceRefOrNamed<'a> {
    Piece(&'a SchemaPiece),
    Named(usize),
}

impl<'a> SchemaPieceRefOrNamed<'a> {
    #[inline(always)]
    pub fn get_piece_and_name(self, root: &'a Schema) -> (&'a SchemaPiece, Option<&'a FullName>) {
        match self {
            SchemaPieceRefOrNamed::Piece(sp) => (sp, None),
            SchemaPieceRefOrNamed::Named(index) => {
                let named_piece = root.lookup(index);
                (&named_piece.piece, Some(&named_piece.name))
            }
        }
    }
}

#[derive(Copy, Clone)]
pub struct SchemaNodeOrNamed<'a> {
    pub root: &'a Schema,
    pub inner: SchemaPieceRefOrNamed<'a>,
}

impl<'a> SchemaNodeOrNamed<'a> {
    #[inline(always)]
    pub fn lookup(self) -> SchemaNode<'a> {
        let (inner, name) = self.inner.get_piece_and_name(self.root);
        SchemaNode {
            root: self.root,
            inner,
            name,
        }
    }
    #[inline(always)]
    pub fn step(self, next: &'a SchemaPieceOrNamed) -> Self {
        self.step_ref(next.as_ref())
    }
    #[inline(always)]
    pub fn step_ref(self, next: SchemaPieceRefOrNamed<'a>) -> Self {
        Self {
            root: self.root,
            inner: match next {
                SchemaPieceRefOrNamed::Piece(piece) => SchemaPieceRefOrNamed::Piece(piece),
                SchemaPieceRefOrNamed::Named(index) => SchemaPieceRefOrNamed::Named(index),
            },
        }
    }

    pub fn to_schema(self) -> Schema {
        let mut cloner = SchemaSubtreeDeepCloner {
            old_root: self.root,
            old_to_new_names: Default::default(),
            named: Default::default(),
        };
        let piece = cloner.clone_piece_or_named(self.inner);
        let named: Vec<NamedSchemaPiece> = cloner.named.into_iter().map(Option::unwrap).collect();
        let indices: HashMap<FullName, usize> = named
            .iter()
            .enumerate()
            .map(|(i, nsp)| (nsp.name.clone(), i))
            .collect();
        Schema {
            named,
            indices,
            top: piece,
        }
    }
}

struct SchemaSubtreeDeepCloner<'a> {
    old_root: &'a Schema,
    old_to_new_names: HashMap<usize, usize>,
    named: Vec<Option<NamedSchemaPiece>>,
}

impl<'a> SchemaSubtreeDeepCloner<'a> {
    fn clone_piece(&mut self, piece: &SchemaPiece) -> SchemaPiece {
        match piece {
            SchemaPiece::Null => SchemaPiece::Null,
            SchemaPiece::Boolean => SchemaPiece::Boolean,
            SchemaPiece::Int => SchemaPiece::Int,
            SchemaPiece::Long => SchemaPiece::Long,
            SchemaPiece::Float => SchemaPiece::Float,
            SchemaPiece::Double => SchemaPiece::Double,
            SchemaPiece::Date => SchemaPiece::Date,
            SchemaPiece::TimestampMilli => SchemaPiece::TimestampMilli,
            SchemaPiece::TimestampMicro => SchemaPiece::TimestampMicro,
            SchemaPiece::Json => SchemaPiece::Json,
            SchemaPiece::Decimal {
                scale,
                precision,
                fixed_size,
            } => SchemaPiece::Decimal {
                scale: *scale,
                precision: *precision,
                fixed_size: *fixed_size,
            },
            SchemaPiece::Bytes => SchemaPiece::Bytes,
            SchemaPiece::String => SchemaPiece::String,
            SchemaPiece::Array(inner) => {
                SchemaPiece::Array(Box::new(self.clone_piece_or_named(inner.as_ref().as_ref())))
            }
            SchemaPiece::Map(inner) => {
                SchemaPiece::Map(Box::new(self.clone_piece_or_named(inner.as_ref().as_ref())))
            }
            SchemaPiece::Union(us) => SchemaPiece::Union(UnionSchema {
                schemas: us
                    .schemas
                    .iter()
                    .map(|s| self.clone_piece_or_named(s.as_ref()))
                    .collect(),
                anon_variant_index: us.anon_variant_index.clone(),
                named_variant_index: us.named_variant_index.clone(),
            }),
            SchemaPiece::ResolveIntLong => SchemaPiece::ResolveIntLong,
            SchemaPiece::ResolveIntFloat => SchemaPiece::ResolveIntFloat,
            SchemaPiece::ResolveIntDouble => SchemaPiece::ResolveIntDouble,
            SchemaPiece::ResolveLongFloat => SchemaPiece::ResolveLongFloat,
            SchemaPiece::ResolveLongDouble => SchemaPiece::ResolveLongDouble,
            SchemaPiece::ResolveFloatDouble => SchemaPiece::ResolveFloatDouble,
            SchemaPiece::ResolveConcreteUnion { index, inner } => {
                SchemaPiece::ResolveConcreteUnion {
                    index: *index,
                    inner: Box::new(self.clone_piece_or_named(inner.as_ref().as_ref())),
                }
            }
            SchemaPiece::ResolveUnionUnion { permutation } => SchemaPiece::ResolveUnionUnion {
                permutation: permutation
                    .iter()
                    .map(|o| {
                        o.as_ref()
                            .map(|(idx, piece)| (*idx, self.clone_piece_or_named(piece.as_ref())))
                    })
                    .collect(),
            },
            SchemaPiece::ResolveUnionConcrete { index, inner } => {
                SchemaPiece::ResolveUnionConcrete {
                    index: *index,
                    inner: Box::new(self.clone_piece_or_named(inner.as_ref().as_ref())),
                }
            }
            SchemaPiece::Record {
                doc,
                fields,
                lookup,
            } => SchemaPiece::Record {
                doc: doc.clone(),
                fields: fields
                    .iter()
                    .map(|rf| RecordField {
                        name: rf.name.clone(),
                        doc: rf.doc.clone(),
                        default: rf.default.clone(),
                        schema: self.clone_piece_or_named(rf.schema.as_ref()),
                        order: rf.order,
                        position: rf.position,
                    })
                    .collect(),
                lookup: lookup.clone(),
            },
            SchemaPiece::Enum { doc, symbols } => SchemaPiece::Enum {
                doc: doc.clone(),
                symbols: symbols.clone(),
            },
            SchemaPiece::Fixed { size } => SchemaPiece::Fixed { size: *size },
            SchemaPiece::ResolveRecord {
                defaults,
                fields,
                n_reader_fields,
            } => SchemaPiece::ResolveRecord {
                defaults: defaults.clone(),
                fields: fields
                    .iter()
                    .map(|rf| match rf {
                        ResolvedRecordField::Present(rf) => {
                            ResolvedRecordField::Present(RecordField {
                                name: rf.name.clone(),
                                doc: rf.doc.clone(),
                                default: rf.default.clone(),
                                schema: self.clone_piece_or_named(rf.schema.as_ref()),
                                order: rf.order,
                                position: rf.position,
                            })
                        }
                        ResolvedRecordField::Absent(writer_schema) => {
                            ResolvedRecordField::Absent(writer_schema.clone())
                        }
                    })
                    .collect(),
                n_reader_fields: *n_reader_fields,
            },
            SchemaPiece::ResolveEnum { doc, symbols } => SchemaPiece::ResolveEnum {
                doc: doc.clone(),
                symbols: symbols.clone(),
            },
        }
    }
    fn clone_piece_or_named(&mut self, piece: SchemaPieceRefOrNamed) -> SchemaPieceOrNamed {
        match piece {
            SchemaPieceRefOrNamed::Piece(piece) => self.clone_piece(piece).into(),
            SchemaPieceRefOrNamed::Named(index) => {
                let new_index = match self.old_to_new_names.entry(index) {
                    Entry::Vacant(ve) => {
                        let new_index = self.named.len();
                        self.named.push(None);
                        ve.insert(new_index);
                        let old_named_piece = self.old_root.lookup(index);
                        let new_named_piece = NamedSchemaPiece {
                            name: old_named_piece.name.clone(),
                            piece: self.clone_piece(&old_named_piece.piece),
                        };
                        self.named[new_index] = Some(new_named_piece);
                        new_index
                    }
                    Entry::Occupied(oe) => *oe.get(),
                };
                SchemaPieceOrNamed::Named(new_index)
            }
        }
    }
}

impl<'a> SchemaNode<'a> {
    #[inline(always)]
    pub fn step(self, next: &'a SchemaPieceOrNamed) -> Self {
        let (inner, name) = next.get_piece_and_name(self.root);
        Self {
            root: self.root,
            inner,
            name,
        }
    }

    pub fn json_to_value(self, json: &serde_json::Value) -> Result<AvroValue, ParseSchemaError> {
        use serde_json::Value::*;
        let val = match (json, self.inner) {
            // A default value always matches the first variant of a union
            (json, SchemaPiece::Union(us)) => match us.schemas.first() {
                Some(variant) => {
                    AvroValue::Union(0, Box::new(self.step(variant).json_to_value(json)?))
                }
                None => return Err(ParseSchemaError("Union schema has no variants".to_owned())),
            },
            (Null, SchemaPiece::Null) => AvroValue::Null,
            (Bool(b), SchemaPiece::Boolean) => AvroValue::Boolean(*b),
            (Number(n), piece) => {
                match piece {
                    SchemaPiece::Int => {
                        let i =
                            n.as_i64()
                                .and_then(|i| i32::try_from(i).ok())
                                .ok_or_else(|| {
                                    ParseSchemaError(format!("{} is not a 32-bit integer", n))
                                })?;
                        AvroValue::Int(i)
                    }
                    SchemaPiece::Long => {
                        let i = n.as_i64().ok_or_else(|| {
                            ParseSchemaError(format!("{} is not a 64-bit integer", n))
                        })?;
                        AvroValue::Long(i)
                    }
                    SchemaPiece::Float => {
                        // Unwrap is okay -- in standard json, (i.e., not using the `arbitrary_precision`
                        // feature of serde), all numbers are representible as doubles.
                        AvroValue::Float(n.as_f64().unwrap() as f32)
                    }
                    SchemaPiece::Double => AvroValue::Double(n.as_f64().unwrap()),
                    _ => {
                        return Err(ParseSchemaError(format!(
                            "Unexpected number in default: {}",
                            n
                        )))
                    }
                }
            }
            (String(s), SchemaPiece::Bytes) => AvroValue::Bytes(s.clone().into_bytes()),
            (
                String(s),
                SchemaPiece::Decimal {
                    precision, scale, ..
                },
            ) => AvroValue::Decimal(DecimalValue {
                precision: *precision,
                scale: *scale,
                unscaled: s.clone().into_bytes(),
            }),
            (String(s), SchemaPiece::String) => AvroValue::String(s.clone()),
            (Object(map), SchemaPiece::Record { fields, .. }) => {
                let field_values = fields
                    .iter()
                    .map(|rf| {
                        let jval = map.get(&rf.name).ok_or_else(|| {
                            ParseSchemaError(format!(
                                "Field not found in default value: {}",
                                rf.name
                            ))
                        })?;
                        let value = self.step(&rf.schema).json_to_value(jval)?;
                        Ok((rf.name.clone(), value))
                    })
                    .collect::<Result<Vec<(std::string::String, AvroValue)>, ParseSchemaError>>()?;
                AvroValue::Record(field_values)
            }
            (String(s), SchemaPiece::Enum { symbols, .. }) => {
                match symbols.iter().find_position(|sym| s == *sym) {
                    Some((index, sym)) => AvroValue::Enum(index as i32, sym.clone()),
                    None => return Err(ParseSchemaError(format!("Enum variant not found: {}", s))),
                }
            }
            (Array(vals), SchemaPiece::Array(inner)) => {
                let node = self.step(&**inner);
                let vals = vals
                    .iter()
                    .map(|val| node.json_to_value(val))
                    .collect::<Result<Vec<_>, ParseSchemaError>>()?;
                AvroValue::Array(vals)
            }
            (Object(map), SchemaPiece::Map(inner)) => {
                let node = self.step(&**inner);
                let map = map
                    .iter()
                    .map(|(k, v)| node.json_to_value(v).map(|v| (k.clone(), v)))
                    .collect::<Result<HashMap<_, _>, ParseSchemaError>>()?;
                AvroValue::Map(map)
            }
            (String(s), SchemaPiece::Fixed { size }) if s.len() == *size => {
                AvroValue::Fixed(*size, s.clone().into_bytes())
            }
            _ => {
                return Err(ParseSchemaError(format!(
                    "Json default value {} does not match schema",
                    json
                )))
            }
        };
        Ok(val)
    }
}

#[derive(Clone)]
struct SchemaSerContext<'a> {
    node: SchemaNodeOrNamed<'a>,
    // This does not logically need Rc<RefCell<_>> semantics --
    // it is only ever mutated in one stack frame at a time.
    // But AFAICT serde doesn't expose a way to
    // provide some mutable context to every node in the tree...
    seen_named: Rc<RefCell<HashMap<usize, String>>>,
}

#[derive(Clone)]
struct RecordFieldSerContext<'a> {
    outer: &'a SchemaSerContext<'a>,
    inner: &'a RecordField,
}

impl<'a> SchemaSerContext<'a> {
    fn step(&'a self, next: SchemaPieceRefOrNamed<'a>) -> Self {
        Self {
            node: self.node.step_ref(next),
            seen_named: self.seen_named.clone(),
        }
    }
}

impl<'a> Serialize for SchemaSerContext<'a> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match self.node.inner {
            SchemaPieceRefOrNamed::Piece(piece) => match piece {
                SchemaPiece::Null => serializer.serialize_str("null"),
                SchemaPiece::Boolean => serializer.serialize_str("boolean"),
                SchemaPiece::Int => serializer.serialize_str("int"),
                SchemaPiece::Long => serializer.serialize_str("long"),
                SchemaPiece::Float => serializer.serialize_str("float"),
                SchemaPiece::Double => serializer.serialize_str("double"),
                SchemaPiece::Date => {
                    let mut map = serializer.serialize_map(Some(2))?;
                    map.serialize_entry("type", "int")?;
                    map.serialize_entry("logicalType", "date")?;
                    map.end()
                }
                SchemaPiece::TimestampMilli | SchemaPiece::TimestampMicro => {
                    let mut map = serializer.serialize_map(Some(2))?;
                    map.serialize_entry("type", "long")?;
                    if piece == &SchemaPiece::TimestampMilli {
                        map.serialize_entry("logicalType", "timestamp-millis")?;
                    } else {
                        map.serialize_entry("logicalType", "timestamp-micros")?;
                    }
                    map.end()
                }
                SchemaPiece::Decimal {
                    precision,
                    scale,
                    fixed_size,
                } => {
                    let mut map = serializer.serialize_map(None)?;
                    map.serialize_entry("type", "bytes")?;
                    if let Some(fixed_size) = fixed_size {
                        map.serialize_entry("size", fixed_size)?;
                    }
                    map.serialize_entry("precision", precision)?;
                    map.serialize_entry("scale", scale)?;
                    map.end()
                }
                SchemaPiece::Bytes => serializer.serialize_str("bytes"),
                SchemaPiece::String => serializer.serialize_str("string"),
                SchemaPiece::Array(inner) => {
                    let mut map = serializer.serialize_map(Some(2))?;
                    map.serialize_entry("type", "array")?;
                    map.serialize_entry("items", &self.step(inner.as_ref().as_ref()))?;
                    map.end()
                }
                SchemaPiece::Map(inner) => {
                    let mut map = serializer.serialize_map(Some(2))?;
                    map.serialize_entry("type", "map")?;
                    map.serialize_entry("values", &self.step(inner.as_ref().as_ref()))?;
                    map.end()
                }
                SchemaPiece::Union(inner) => {
                    let variants = inner.variants();
                    let mut seq = serializer.serialize_seq(Some(variants.len()))?;
                    for v in variants {
                        seq.serialize_element(&self.step(v.as_ref()))?;
                    }
                    seq.end()
                }
                SchemaPiece::Json => {
                    let mut map = serializer.serialize_map(Some(2))?;
                    map.serialize_entry("type", "string")?;
                    map.serialize_entry("connect.name", "io.debezium.data.Json")?;
                    map.end()
                }
                SchemaPiece::Record { .. }
                | SchemaPiece::Enum { .. }
                | SchemaPiece::Fixed { .. } => {
                    unreachable!("Unexpected named schema piece in anonymous schema position")
                }
                SchemaPiece::ResolveIntLong
                | SchemaPiece::ResolveIntFloat
                | SchemaPiece::ResolveIntDouble
                | SchemaPiece::ResolveLongFloat
                | SchemaPiece::ResolveLongDouble
                | SchemaPiece::ResolveFloatDouble
                | SchemaPiece::ResolveConcreteUnion { .. }
                | SchemaPiece::ResolveUnionUnion { .. }
                | SchemaPiece::ResolveUnionConcrete { .. }
                | SchemaPiece::ResolveRecord { .. }
                | SchemaPiece::ResolveEnum { .. } => {
                    panic!("Attempted to serialize resolved schema")
                }
            },
            SchemaPieceRefOrNamed::Named(index) => {
                let mut map = self.seen_named.borrow_mut();
                let named_piece = match map.get(&index) {
                    Some(name) => {
                        return serializer.serialize_str(name.as_str());
                    }
                    None => self.node.root.lookup(index),
                };
                let name = named_piece.name.to_string();
                map.insert(index, name.clone());
                std::mem::drop(map);
                match &named_piece.piece {
                    SchemaPiece::Record { doc, fields, .. } => {
                        let mut map = serializer.serialize_map(None)?;
                        map.serialize_entry("type", "record")?;
                        map.serialize_entry("name", &name)?;
                        if let Some(ref docstr) = doc {
                            map.serialize_entry("doc", docstr)?;
                        }
                        // TODO (brennan) - serialize aliases
                        map.serialize_entry(
                            "fields",
                            &fields
                                .iter()
                                .map(|f| RecordFieldSerContext {
                                    outer: self,
                                    inner: f,
                                })
                                .collect::<Vec<_>>(),
                        )?;
                        map.end()
                    }
                    SchemaPiece::Enum { symbols, .. } => {
                        let mut map = serializer.serialize_map(None)?;
                        map.serialize_entry("type", "enum")?;
                        map.serialize_entry("name", &name)?;
                        map.serialize_entry("symbols", symbols)?;
                        map.end()
                    }
                    SchemaPiece::Fixed { size } => {
                        let mut map = serializer.serialize_map(None)?;
                        map.serialize_entry("type", "fixed")?;
                        map.serialize_entry("name", &name)?;
                        map.serialize_entry("size", size)?;
                        map.end()
                    }
                    SchemaPiece::Null
                    | SchemaPiece::Boolean
                    | SchemaPiece::Int
                    | SchemaPiece::Long
                    | SchemaPiece::Float
                    | SchemaPiece::Double
                    | SchemaPiece::Date
                    | SchemaPiece::TimestampMilli
                    | SchemaPiece::TimestampMicro
                    | SchemaPiece::Decimal { .. }
                    | SchemaPiece::Bytes
                    | SchemaPiece::String
                    | SchemaPiece::Array(_)
                    | SchemaPiece::Map(_)
                    | SchemaPiece::Union(_)
                    | SchemaPiece::Json => {
                        unreachable!("Unexpected anonymous schema piece in named schema position")
                    }
                    SchemaPiece::ResolveIntLong
                    | SchemaPiece::ResolveIntFloat
                    | SchemaPiece::ResolveIntDouble
                    | SchemaPiece::ResolveLongFloat
                    | SchemaPiece::ResolveLongDouble
                    | SchemaPiece::ResolveFloatDouble
                    | SchemaPiece::ResolveConcreteUnion { .. }
                    | SchemaPiece::ResolveUnionUnion { .. }
                    | SchemaPiece::ResolveUnionConcrete { .. }
                    | SchemaPiece::ResolveRecord { .. }
                    | SchemaPiece::ResolveEnum { .. } => {
                        panic!("Attempted to serialize resolved schema")
                    }
                }
            }
        }
    }
}

impl Serialize for Schema {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let ctx = SchemaSerContext {
            node: SchemaNodeOrNamed {
                root: self,
                inner: self.top.as_ref(),
            },
            seen_named: Rc::new(RefCell::new(Default::default())),
        };
        ctx.serialize(serializer)
    }
}

impl<'a> Serialize for RecordFieldSerContext<'a> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut map = serializer.serialize_map(None)?;
        map.serialize_entry("name", &self.inner.name)?;
        map.serialize_entry("type", &self.outer.step(self.inner.schema.as_ref()))?;
        if let Some(default) = &self.inner.default {
            map.serialize_entry("default", default)?;
        }
        map.end()
    }
}

/// Parses a **valid** avro schema into the Parsing Canonical Form.
/// https://avro.apache.org/docs/1.8.2/spec.html#Parsing+Canonical+Form+for+Schemas
fn parsing_canonical_form(schema: &serde_json::Value) -> String {
    match schema {
        serde_json::Value::Object(map) => pcf_map(map),
        serde_json::Value::String(s) => pcf_string(s),
        serde_json::Value::Array(v) => pcf_array(v),
        _ => unreachable!(),
    }
}

fn pcf_map(schema: &Map<String, serde_json::Value>) -> String {
    // Look for the namespace variant up front.
    let ns = schema.get("namespace").and_then(|v| v.as_str());
    let mut fields = Vec::new();
    for (k, v) in schema {
        // Reduce primitive types to their simple form. ([PRIMITIVE] rule)
        if schema.len() == 1 && k == "type" {
            // Invariant: function is only callable from a valid schema, so this is acceptable.
            if let serde_json::Value::String(s) = v {
                return pcf_string(s);
            }
        }

        // Strip out unused fields ([STRIP] rule)
        if field_ordering_position(k).is_none() {
            continue;
        }

        // Fully qualify the name, if it isn't already ([FULLNAMES] rule).
        if k == "name" {
            // Invariant: Only valid schemas. Must be a string.
            let name = v.as_str().unwrap();
            let n = match ns {
                Some(namespace) if !name.contains('.') => {
                    Cow::Owned(format!("{}.{}", namespace, name))
                }
                _ => Cow::Borrowed(name),
            };

            fields.push((k, format!("{}:{}", pcf_string(k), pcf_string(&*n))));
            continue;
        }

        // Strip off quotes surrounding "size" type, if they exist ([INTEGERS] rule).
        if k == "size" {
            let i = match v.as_str() {
                Some(s) => s.parse::<i64>().expect("Only valid schemas are accepted!"),
                None => v.as_i64().unwrap(),
            };
            fields.push((k, format!("{}:{}", pcf_string(k), i)));
            continue;
        }

        // For anything else, recursively process the result.
        fields.push((
            k,
            format!("{}:{}", pcf_string(k), parsing_canonical_form(v)),
        ));
    }

    // Sort the fields by their canonical ordering ([ORDER] rule).
    fields.sort_unstable_by_key(|(k, _)| field_ordering_position(k).unwrap());
    let inter = fields
        .into_iter()
        .map(|(_, v)| v)
        .collect::<Vec<_>>()
        .join(",");
    format!("{{{}}}", inter)
}

fn pcf_array(arr: &[serde_json::Value]) -> String {
    let inter = arr
        .iter()
        .map(parsing_canonical_form)
        .collect::<Vec<String>>()
        .join(",");
    format!("[{}]", inter)
}

fn pcf_string(s: &str) -> String {
    format!("\"{}\"", s)
}

// Used to define the ordering and inclusion of fields.
fn field_ordering_position(field: &str) -> Option<usize> {
    let v = match field {
        "name" => 1,
        "type" => 2,
        "fields" => 3,
        "symbols" => 4,
        "items" => 5,
        "values" => 6,
        "size" => 7,
        _ => return None,
    };

    Some(v)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_invalid_schema() {
        assert!(Schema::parse_str("invalid").is_err());
    }

    #[test]
    fn test_primitive_schema() {
        assert_eq!(
            SchemaPieceOrNamed::Piece(SchemaPiece::Null),
            Schema::parse_str("\"null\"").unwrap().top
        );
        assert_eq!(
            SchemaPieceOrNamed::Piece(SchemaPiece::Int),
            Schema::parse_str("\"int\"").unwrap().top
        );
        assert_eq!(
            SchemaPieceOrNamed::Piece(SchemaPiece::Double),
            Schema::parse_str("\"double\"").unwrap().top
        );
    }

    #[test]
    fn test_array_schema() {
        let schema = Schema::parse_str(r#"{"type": "array", "items": "string"}"#).unwrap();
        assert_eq!(
            SchemaPieceOrNamed::Piece(SchemaPiece::Array(Box::new(SchemaPieceOrNamed::Piece(
                SchemaPiece::String
            )))),
            schema.top
        );
    }

    #[test]
    fn test_map_schema() {
        let schema = Schema::parse_str(r#"{"type": "map", "values": "double"}"#).unwrap();
        assert_eq!(
            SchemaPieceOrNamed::Piece(SchemaPiece::Map(Box::new(SchemaPieceOrNamed::Piece(
                SchemaPiece::Double
            )))),
            schema.top
        );
    }

    #[test]
    fn test_union_schema() {
        let schema = Schema::parse_str(r#"["null", "int"]"#).unwrap();
        assert_eq!(
            SchemaPieceOrNamed::Piece(SchemaPiece::Union(
                UnionSchema::new(vec![
                    SchemaPieceOrNamed::Piece(SchemaPiece::Null),
                    SchemaPieceOrNamed::Piece(SchemaPiece::Int)
                ])
                .unwrap()
            )),
            schema.top
        );
    }

    #[test]
    fn test_union_unsupported_schema() {
        let schema = Schema::parse_str(r#"["null", ["null", "int"], "string"]"#);
        assert!(schema.is_err());
    }

    #[test]
    fn test_multi_union_schema() {
        let schema = Schema::parse_str(r#"["null", "int", "float", "string", "bytes"]"#);
        assert!(schema.is_ok());
        let schema = schema.unwrap();
        let node = schema.top_node();
        assert_eq!(SchemaKind::from(&schema), SchemaKind::Union);
        let union_schema = match node.inner {
            SchemaPiece::Union(u) => u,
            _ => unreachable!(),
        };
        assert_eq!(union_schema.variants().len(), 5);
        let mut variants = union_schema.variants().iter();
        assert_eq!(
            SchemaKind::from(node.step(variants.next().unwrap())),
            SchemaKind::Null
        );
        assert_eq!(
            SchemaKind::from(node.step(variants.next().unwrap())),
            SchemaKind::Int
        );
        assert_eq!(
            SchemaKind::from(node.step(variants.next().unwrap())),
            SchemaKind::Float
        );
        assert_eq!(
            SchemaKind::from(node.step(variants.next().unwrap())),
            SchemaKind::String
        );
        assert_eq!(
            SchemaKind::from(node.step(variants.next().unwrap())),
            SchemaKind::Bytes
        );
        assert_eq!(variants.next(), None);
    }

    #[test]
    fn test_record_schema() {
        let schema = Schema::parse_str(
            r#"
                {
                    "type": "record",
                    "name": "test",
                    "fields": [
                        {"name": "a", "type": "long", "default": 42},
                        {"name": "b", "type": "string"}
                    ]
                }
            "#,
        )
        .unwrap();

        let mut lookup = HashMap::new();
        lookup.insert("a".to_owned(), 0);
        lookup.insert("b".to_owned(), 1);

        let expected = SchemaPiece::Record {
            doc: None,
            fields: vec![
                RecordField {
                    name: "a".to_string(),
                    doc: None,
                    default: Some(Value::Number(42i64.into())),
                    schema: SchemaPiece::Long.into(),
                    order: RecordFieldOrder::Ascending,
                    position: 0,
                },
                RecordField {
                    name: "b".to_string(),
                    doc: None,
                    default: None,
                    schema: SchemaPiece::String.into(),
                    order: RecordFieldOrder::Ascending,
                    position: 1,
                },
            ],
            lookup,
        };

        assert_eq!(&expected, schema.top_node().inner);
    }

    #[test]
    fn test_enum_schema() {
        let schema = Schema::parse_str(
                r#"{"type": "enum", "name": "Suit", "symbols": ["diamonds", "spades", "clubs", "hearts"]}"#,
            ).unwrap();

        let expected = SchemaPiece::Enum {
            doc: None,
            symbols: vec![
                "diamonds".to_owned(),
                "spades".to_owned(),
                "clubs".to_owned(),
                "hearts".to_owned(),
            ],
        };

        assert_eq!(&expected, schema.top_node().inner);
    }

    #[test]
    fn test_fixed_schema() {
        let schema = Schema::parse_str(r#"{"type": "fixed", "name": "test", "size": 16}"#).unwrap();

        let expected = SchemaPiece::Fixed { size: 16usize };

        assert_eq!(&expected, schema.top_node().inner);
    }

    #[test]
    fn test_date_schema() {
        let kinds = &[
            r#"{
                    "type": "int",
                    "name": "datish",
                    "logicalType": "date"
                }"#,
            r#"{
                    "type": "int",
                    "name": "datish",
                    "connect.name": "io.debezium.time.Date"
                }"#,
            r#"{
                    "type": "int",
                    "name": "datish",
                    "connect.name": "org.apache.kafka.connect.data.Date"
                }"#,
        ];
        for kind in kinds {
            let schema = Schema::parse_str(kind).unwrap();
            assert_eq!(schema.top_node().inner, &SchemaPiece::Date);

            assert_eq!(
                serde_json::to_string(&schema).unwrap(),
                r#"{"type":"int","logicalType":"date"}"#
            );
        }
    }

    #[test]
    fn test_decimal_schemas() {
        let schema = Schema::parse_str(
            r#"{
                "type": "fixed",
                "name": "dec",
                "size": 8,
                "logicalType": "decimal",
                "precision": 12,
                "scale": 5
            }"#,
        )
        .unwrap();
        let expected = SchemaPiece::Decimal {
            precision: 12,
            scale: 5,
            fixed_size: Some(8),
        };
        assert_eq!(schema.top_node().inner, &expected);

        let schema = Schema::parse_str(
            r#"{
                "type": "bytes",
                "logicalType": "decimal",
                "precision": 12,
                "scale": 5
            }"#,
        )
        .unwrap();
        let expected = SchemaPiece::Decimal {
            precision: 12,
            scale: 5,
            fixed_size: None,
        };
        assert_eq!(schema.top_node().inner, &expected);

        let res = Schema::parse_str(
            r#"{
                "type": "bytes",
                "logicalType": "decimal",
                "precision": 12,
                "scale": 13
            }"#,
        );
        assert_eq!(
            res.unwrap_err().to_string(),
            "Failed to parse schema: Decimal scale is greater than precision"
        );

        let res = Schema::parse_str(
            r#"{
                "type": "bytes",
                "logicalType": "decimal",
                "precision": -12
            }"#,
        );
        assert_eq!(
            res.unwrap_err().to_string(),
            "Failed to parse schema: Decimal precision must be greater than zero"
        );

        let res = Schema::parse_str(
            r#"{
                "type": "bytes",
                "logicalType": "decimal",
                "precision": 12,
                "scale": -5
            }"#,
        );
        assert_eq!(
            res.unwrap_err().to_string(),
            "Failed to parse schema: Decimal scale must be greater than zero"
        );

        let res = Schema::parse_str(
            r#"{
                "type": "fixed",
                "name": "dec",
                "size": 5,
                "logicalType": "decimal",
                "precision": 12,
                "scale": 5
            }"#,
        );
        assert_eq!(
            res.unwrap_err().to_string(),
            "Failed to parse schema: Decimal precision 12 requires more than 5 bytes of space"
        );
    }

    #[test]
    fn test_no_documentation() {
        let schema =
            Schema::parse_str(r#"{"type": "enum", "name": "Coin", "symbols": ["heads", "tails"]}"#)
                .unwrap();

        let doc = match schema.top_node().inner {
            SchemaPiece::Enum { doc, .. } => doc.clone(),
            _ => panic!(),
        };

        assert!(doc.is_none());
    }

    #[test]
    fn test_documentation() {
        let schema = Schema::parse_str(
                r#"{"type": "enum", "name": "Coin", "doc": "Some documentation", "symbols": ["heads", "tails"]}"#
            ).unwrap();

        let doc = match schema.top_node().inner {
            SchemaPiece::Enum { doc, .. } => doc.clone(),
            _ => None,
        };

        assert_eq!("Some documentation".to_owned(), doc.unwrap());
    }

    // Tests to ensure Schema is Send + Sync. These tests don't need to _do_ anything, if they can
    // compile, they pass.
    #[test]
    fn test_schema_is_send() {
        fn send<S: Send>(_s: S) {}

        let schema = Schema {
            named: vec![],
            indices: Default::default(),
            top: SchemaPiece::Null.into(),
        };
        send(schema);
    }

    #[test]
    fn test_schema_is_sync() {
        fn sync<S: Sync>(_s: S) {}

        let schema = Schema {
            named: vec![],
            indices: Default::default(),
            top: SchemaPiece::Null.into(),
        };
        sync(&schema);
        sync(schema);
    }

    #[test]
    fn test_schema_fingerprint() {
        use md5::Md5;
        use sha2::Sha256;

        let raw_schema = r#"
        {
            "type": "record",
            "name": "test",
            "fields": [
                {"name": "a", "type": "long", "default": 42},
                {"name": "b", "type": "string"}
            ]
        }
    "#;

        let schema = Schema::parse_str(raw_schema).unwrap();
        assert_eq!(
            "5ecb2d1f0eaa647d409e6adbd5d70cd274d85802aa9167f5fe3b73ba70b32c76",
            format!("{}", schema.fingerprint::<Sha256>())
        );

        assert_eq!(
            "a2c99a3f40ea2eea32593d63b483e962",
            format!("{}", schema.fingerprint::<Md5>())
        );
    }
}

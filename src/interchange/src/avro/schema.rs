// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Conversion from Avro schemas to Materialize `RelationDesc`s.
//!
//! A few notes for posterity on how this conversion happens are in order.
//!
//! If the schema is an Avro record, we flatten it to its fields, which become the columns
//! of the relation.
//!
//! Each individual field is then converted to its SQL equivalent. For most types, this
//! conversion is the obvious one. The only non-trivial counterexample is Avro unions.
//!
//! Since Avro types are not nullable by default, the typical way normal (i.e., nullable)
//! SQL fields are represented in Avro is by a union of the underlying type with the
//! singleton type { Null }; in Avro schema notation, this is `["null", "TheType"]`.
//! We shall call union types following this pattern _Nullability-Pattern Unions_.
//! We shall call all other union types (e.g. `["MyType1", "MyType2"]` or `["null", "MyType1", "MyType2"]`) _Essential Unions_.
//! Since there is an obvious way to represent Nullability-Pattern Unions, but not Essential Unions, in the SQL type system,
//! we must handle Essential Unions with a bit of a hack (at least until Materialize supports union or sum types, which may be never).
//!
//! When an Essential Union appears as one of the fields of a record, we expand
//! it to _n_ columns in SQL, where _n_ is the number of non-null variants in the union. These
//! columns will be given names created by pasting their index at the end of the overall name
//! of the field. For example, if an Essential Union in a field named `"Foo"` has schema `[int, bool]`, it will expand to the columns `"Foo1": bool, "Foo2": int`. There is an implicit constraint upheld be the source pipeline that only one such column will be non-`null` at a time
//!
//! When an Essential Union appears _elsewhere_ than as one of the fields of a record,
//! there is nothing we can do, because we expect to be able to turn it into exactly one
//! SQL type, not a series of them. Thus, in these cases, we just bail. For example, it's
//! not possible to ingest an array or map whose element type is an Essential Union.

use std::collections::hash_map::Entry;
use std::collections::{HashMap, HashSet};
use std::fmt;
use std::str::FromStr;
use std::time::Duration;

use anyhow::{anyhow, bail, Context};
use mz_ore::collections::CollectionExt;
use tracing::warn;

use mz_avro::error::Error as AvroError;
use mz_avro::schema::{resolve_schemas, Schema, SchemaNode, SchemaPiece, SchemaPieceOrNamed};
use mz_ore::cast::CastFrom;
use mz_ore::retry::Retry;
use mz_repr::adt::numeric::{NumericMaxScale, NUMERIC_DATUM_MAX_PRECISION};
use mz_repr::{ColumnName, ColumnType, RelationDesc, ScalarType};

use super::is_null;

pub fn parse_schema(schema: &str) -> anyhow::Result<Schema> {
    let schema = serde_json::from_str(schema)?;
    Ok(Schema::parse(&schema)?)
}

/// Converts an Apache Avro schema into a list of column names and types.
// TODO(petrosagg): find a way to make this a TryFrom impl somewhere
pub fn schema_to_relationdesc(schema: Schema) -> Result<RelationDesc, anyhow::Error> {
    // TODO(petrosagg): call directly into validate_schema_2 and do the Record flattening once
    // we're in RelationDesc land
    Ok(RelationDesc::from_names_and_types(validate_schema_1(
        schema.top_node(),
    )?))
}

/// Convert an Avro schema to a series of columns and names, flattening the top-level record,
/// if the top node is indeed a record.
fn validate_schema_1(schema: SchemaNode) -> anyhow::Result<Vec<(ColumnName, ColumnType)>> {
    let mut columns = vec![];
    let mut seen_avro_nodes = Default::default();
    match schema.inner {
        SchemaPiece::Record { fields, .. } => {
            for f in fields {
                columns.extend(get_named_columns(
                    &mut seen_avro_nodes,
                    schema.step(&f.schema),
                    Some(&f.name),
                )?);
            }
        }
        _ => {
            columns.extend(get_named_columns(&mut seen_avro_nodes, schema, None)?);
        }
    }
    Ok(columns)
}

/// Get the series of (one or more) SQL columns corresponding to an Avro union.
/// See module comments for details.
fn get_union_columns<'a>(
    seen_avro_nodes: &mut HashSet<usize>,
    schema: SchemaNode<'a>,
    base_name: Option<&str>,
) -> anyhow::Result<Vec<(ColumnName, ColumnType)>> {
    let us = match schema.inner {
        SchemaPiece::Union(us) => us,
        _ => panic!("This function should only be called on unions."),
    };
    let mut columns = vec![];
    let vs = us.variants();
    if vs.is_empty() || (vs.len() == 1 && is_null(&vs[0])) {
        bail!(anyhow!("Empty or null-only unions are not supported"));
    } else {
        for (i, v) in vs.iter().filter(|v| !is_null(v)).enumerate() {
            let named_idx = match v {
                SchemaPieceOrNamed::Named(idx) => Some(*idx),
                _ => None,
            };
            if let Some(named_idx) = named_idx {
                if !seen_avro_nodes.insert(named_idx) {
                    bail!(
                        "Recursive types are not supported: {}",
                        v.get_human_name(schema.root)
                    );
                }
            }
            let node = schema.step(v);
            if let SchemaPiece::Union(_) = node.inner {
                unreachable!("Internal error: directly nested avro union!");
            }

            let name = if vs.len() == 1 || (vs.len() == 2 && vs.iter().any(is_null)) {
                // There is only one non-null variant in the
                // union, so we can use the field name directly.
                base_name
                    .map(|n| n.to_owned())
                    .or_else(|| {
                        v.get_piece_and_name(schema.root)
                            .1
                            .map(|full_name| full_name.base_name().to_owned())
                    })
                    .unwrap_or_else(|| "?column?".into())
            } else {
                // There are multiple non-null variants in the
                // union, so we need to invent field names for
                // each variant.
                base_name
                    .map(|n| format!("{}{}", n, i + 1))
                    .or_else(|| {
                        v.get_piece_and_name(schema.root)
                            .1
                            .map(|full_name| full_name.base_name().to_owned())
                    })
                    .unwrap_or_else(|| "?column?".into())
            };

            // If there is more than one variant in the union,
            // the column's output type is nullable, as this
            // column will be null whenever it is uninhabited.
            let ty = validate_schema_2(seen_avro_nodes, node)?;
            columns.push((name.into(), ty.nullable(vs.len() > 1)));
            if let Some(named_idx) = named_idx {
                seen_avro_nodes.remove(&named_idx);
            }
        }
    }
    Ok(columns)
}

fn get_named_columns<'a>(
    seen_avro_nodes: &mut HashSet<usize>,
    schema: SchemaNode<'a>,
    base_name: Option<&str>,
) -> anyhow::Result<Vec<(ColumnName, ColumnType)>> {
    if let SchemaPiece::Union(_) = schema.inner {
        get_union_columns(seen_avro_nodes, schema, base_name)
    } else {
        let scalar_type = validate_schema_2(seen_avro_nodes, schema)?;
        Ok(vec![(
            // TODO(benesch): we should do better than this when there's no base
            // name, e.g., invent a name based on the type.
            base_name.unwrap_or("?column?").into(),
            scalar_type.nullable(false),
        )])
    }
}

/// Get the single column corresponding to a schema node.
/// It is an error if this node should correspond to more than one column
/// (because it is an Essential Union in the sense described in the module docs).
fn validate_schema_2(
    seen_avro_nodes: &mut HashSet<usize>,
    schema: SchemaNode,
) -> anyhow::Result<ScalarType> {
    Ok(match schema.inner {
        SchemaPiece::Union(_) => {
            let columns = get_union_columns(seen_avro_nodes, schema, None)?;
            if columns.len() != 1 {
                bail!("Union of more than one non-null type not valid here");
            }
            let (_column_name, column_type) = columns.into_element();
            // It's okay to lose the nullability information here, as it's not relevant to
            // any higher layer. This will either be included in an array or map type,
            // where all values are nullable. It can't be included as a top-level column
            // or as a record type, where nullability is actually tracked, because in
            // those cases we will have already gone through the `Union` code path in
            // `get_named_columns`.
            column_type.scalar_type
        }
        SchemaPiece::Null => bail!("null outside of union types is not supported"),
        SchemaPiece::Boolean => ScalarType::Bool,
        SchemaPiece::Int => ScalarType::Int32,
        SchemaPiece::Long => ScalarType::Int64,
        SchemaPiece::Float => ScalarType::Float32,
        SchemaPiece::Double => ScalarType::Float64,
        SchemaPiece::Date => ScalarType::Date,
        SchemaPiece::TimestampMilli => ScalarType::Timestamp,
        SchemaPiece::TimestampMicro => ScalarType::Timestamp,
        SchemaPiece::Decimal {
            precision, scale, ..
        } => {
            if *precision > usize::cast_from(NUMERIC_DATUM_MAX_PRECISION) {
                bail!(
                    "decimals with precision greater than {} are not supported",
                    NUMERIC_DATUM_MAX_PRECISION
                )
            }
            ScalarType::Numeric {
                max_scale: Some(NumericMaxScale::try_from(*scale)?),
            }
        }
        SchemaPiece::Bytes | SchemaPiece::Fixed { .. } => ScalarType::Bytes,
        SchemaPiece::String | SchemaPiece::Enum { .. } => ScalarType::String,

        SchemaPiece::Json => ScalarType::Jsonb,
        SchemaPiece::Uuid => ScalarType::Uuid,
        SchemaPiece::Record { fields, .. } => {
            let mut columns = vec![];
            for f in fields {
                let named_idx = match &f.schema {
                    SchemaPieceOrNamed::Named(idx) => Some(*idx),
                    _ => None,
                };
                if let Some(named_idx) = named_idx {
                    if !seen_avro_nodes.insert(named_idx) {
                        bail!(
                            "Recursive types are not supported: {}",
                            f.schema.get_human_name(schema.root)
                        );
                    }
                }
                let next_node = schema.step(&f.schema);
                columns.extend(
                    get_named_columns(seen_avro_nodes, next_node, Some(&f.name))?.into_iter(),
                );
                if let Some(named_idx) = named_idx {
                    seen_avro_nodes.remove(&named_idx);
                }
            }
            ScalarType::Record {
                fields: columns,
                custom_oid: None,
                custom_name: None,
            }
        }
        SchemaPiece::Array(inner) => {
            let named_idx = match inner.as_ref() {
                SchemaPieceOrNamed::Named(idx) => Some(*idx),
                _ => None,
            };
            if let Some(named_idx) = named_idx {
                if !seen_avro_nodes.insert(named_idx) {
                    bail!(
                        "Recursive types are not supported: {}",
                        inner.get_human_name(schema.root)
                    );
                }
            }
            let next_node = schema.step(inner);
            let ret = ScalarType::List {
                element_type: Box::new(validate_schema_2(seen_avro_nodes, next_node)?),
                custom_oid: None,
            };
            if let Some(named_idx) = named_idx {
                seen_avro_nodes.remove(&named_idx);
            }
            ret
        }
        SchemaPiece::Map(inner) => ScalarType::Map {
            value_type: Box::new(validate_schema_2(seen_avro_nodes, schema.step(inner))?),
            custom_oid: None,
        },

        _ => bail!("Unsupported type in schema: {:?}", schema.inner),
    })
}

pub struct ConfluentAvroResolver {
    reader_schema: Schema,
    writer_schemas: Option<SchemaCache>,
    confluent_wire_format: bool,
}

impl ConfluentAvroResolver {
    pub fn new(
        reader_schema: &str,
        config: Option<mz_ccsr::ClientConfig>,
        confluent_wire_format: bool,
    ) -> anyhow::Result<Self> {
        let reader_schema = parse_schema(reader_schema)?;
        let writer_schemas = config.map(SchemaCache::new).transpose()?;
        Ok(Self {
            reader_schema,
            writer_schemas,
            confluent_wire_format,
        })
    }

    pub async fn resolve<'a, 'b>(
        &'a mut self,
        mut bytes: &'b [u8],
    ) -> anyhow::Result<(&'b [u8], &'a Schema, Option<i32>)> {
        let (resolved_schema, schema_id) = match &mut self.writer_schemas {
            Some(cache) => {
                debug_assert!(
                    self.confluent_wire_format,
                    "We should have set 'confluent_wire_format' everywhere \
                     that can lead to this branch"
                );
                // XXX(guswynn): use destructuring assignments when they are stable
                let (schema_id, adjusted_bytes) = crate::confluent::extract_avro_header(bytes)?;
                bytes = adjusted_bytes;
                let schema = cache
                    .get(schema_id, &self.reader_schema)
                    .await
                    .with_context(|| {
                        format!("failed to resolve Avro schema (id = {})", schema_id)
                    })?;
                (schema, Some(schema_id))
            }

            // If we haven't been asked to use a schema registry, we have no way
            // to discover the writer's schema. That's ok; we'll just use the
            // reader's schema and hope it lines up.
            None => {
                if self.confluent_wire_format {
                    // validate and just move the bytes buffer ahead
                    let (_, adjusted_bytes) = crate::confluent::extract_avro_header(bytes)?;
                    bytes = adjusted_bytes;
                }
                (&self.reader_schema, None)
            }
        };
        Ok((bytes, resolved_schema, schema_id))
    }
}

impl fmt::Debug for ConfluentAvroResolver {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("ConfluentAvroResolver")
            .field("reader_schema", &self.reader_schema)
            .field(
                "write_schema",
                if self.writer_schemas.is_some() {
                    &"some"
                } else {
                    &"none"
                },
            )
            .finish()
    }
}

#[derive(Debug)]
struct SchemaCache {
    cache: HashMap<i32, Result<Schema, AvroError>>,
    ccsr_client: mz_ccsr::Client,
}

impl SchemaCache {
    fn new(schema_registry: mz_ccsr::ClientConfig) -> Result<SchemaCache, anyhow::Error> {
        Ok(SchemaCache {
            cache: HashMap::new(),
            ccsr_client: schema_registry.build()?,
        })
    }

    /// Looks up the writer schema for ID. If the schema is literally identical
    /// to the reader schema, as determined by the reader schema fingerprint
    /// that this schema cache was initialized with, returns the schema directly.
    /// If not, performs schema resolution on the reader and writer and
    /// returns the result.
    async fn get(&mut self, id: i32, reader_schema: &Schema) -> anyhow::Result<&Schema> {
        let entry = match self.cache.entry(id) {
            Entry::Occupied(o) => o.into_mut(),
            Entry::Vacant(v) => {
                // An issue with _fetching_ the schema should be returned
                // immediately, and not cached, since it might get better on the
                // next retry.
                let ccsr_client = &self.ccsr_client;
                let response = Retry::default()
                    .max_duration(Duration::from_secs(30))
                    .retry_async(|state| async move {
                        let res = ccsr_client.get_schema_by_id(id).await;
                        match res {
                            Err(e) => {
                                if let Some(timeout) = state.next_backoff {
                                    warn!("transient failure fetching schema id {}: {:?}, retrying in {:?}", id, e, timeout);
                                }
                                Err(e)
                            }
                            _ => res,
                        }
                    })
                    .await?;
                // Now, we've gotten some json back, so we want to cache it (regardless of whether it's a valid
                // avro schema, it won't change).
                //
                // However, we can't just cache it directly, since resolving schemas takes significant CPU work,
                // which  we don't want to repeat for every record. So, parse and resolve it, and cache the
                // result (whether schema or error).
                let result = Schema::from_str(&response.raw).and_then(|schema| {
                    // Schema fingerprints don't actually capture whether two schemas are meaningfully
                    // different, because they strip out logical types. Thus, resolve in all cases.
                    let resolved = resolve_schemas(&schema, reader_schema)?;
                    Ok(resolved)
                });
                v.insert(result)
            }
        };
        entry.as_ref().map_err(|e| anyhow::Error::new(e.clone()))
    }
}

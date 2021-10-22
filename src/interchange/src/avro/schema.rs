// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::hash_map::Entry;
use std::collections::{HashMap, HashSet};
use std::fmt;
use std::str::FromStr;
use std::time::Duration;

use anyhow::{anyhow, bail, Error};
use log::warn;

use byteorder::{BigEndian, ByteOrder};
use mz_avro::error::Error as AvroError;
use mz_avro::schema::{resolve_schemas, Schema, SchemaNode, SchemaPiece, SchemaPieceOrNamed};
use ore::retry::Retry;
use repr::adt::numeric::NUMERIC_DATUM_MAX_PRECISION;
use repr::{ColumnName, ColumnType, RelationDesc, ScalarType};

use super::{cdc_v2, is_null, EnvelopeType};

pub fn parse_schema(schema: &str) -> anyhow::Result<Schema> {
    let schema = serde_json::from_str(schema)?;
    Ok(Schema::parse(&schema)?)
}

/// Validates an Avro key schema for use as a source.
///
/// An Avro key schema is valid for our purposes iff every field
/// mentioned in the key schema exists in the specified relation
/// type with the same type. If the schema is valid, returns a
/// vector describing the order and position of the primary key
/// columns.
pub fn validate_key_schema(
    key_schema: &str,
    value_desc: &RelationDesc,
) -> anyhow::Result<Vec<usize>> {
    let key_schema = parse_schema(key_schema)?;
    let key_desc = validate_schema_1(key_schema.top_node())?;
    let mut indices = Vec::new();
    for (name, key_type) in key_desc.iter() {
        match value_desc.get_by_name(name) {
            Some((index, value_type)) if key_type == value_type => {
                indices.push(index);
            }
            Some((_, value_type)) => bail!(
                "key and value column types do not match: key {:?} vs. value {:?}",
                key_type,
                value_type,
            ),
            None => bail!("Value schema missing primary key column: {}", name),
        }
    }
    Ok(indices)
}

/// Converts an Apache Avro schema into a list of column names and types.
pub fn validate_value_schema(
    schema: &str,
    envelope: EnvelopeType,
) -> anyhow::Result<Vec<(ColumnName, ColumnType)>> {
    let schema = parse_schema(schema)?;
    let node = schema.top_node();

    let row_schema = match envelope {
        EnvelopeType::Debezium => {
            // The top-level record needs to be a diff "envelope" that contains
            // `before` and `after` fields, where the `before` and `after` fields
            // have the same schema.
            match node.inner {
                SchemaPiece::Record { fields, .. } => {
                    let before = fields.iter().find(|f| f.name == "before");
                    let after = fields.iter().find(|f| f.name == "after");
                    match (before, after) {
                        (Some(before), Some(after)) => {
                            let left = node.step(&before.schema);
                            let right = node.step(&after.schema);
                            match (left.inner, right.inner) {
                                (SchemaPiece::Union(before), SchemaPiece::Union(after)) => {
                                    if before.variants().len() != 2 {
                                        bail!("Source schema 'before' field has the wrong number of variants");
                                    }
                                    if after.variants().len() != 2 {
                                        bail!("Source schema 'after' field has the wrong number of variants");
                                    }
                                    let before_null =
                                        before.variants().iter().position(|s| is_null(s));
                                    let after_null =
                                        before.variants().iter().position(|s| is_null(s));
                                    if before_null != after_null {
                                        bail!("Source schema 'before' and 'after' fields do not match.");
                                    }

                                    let null_idx = match before_null {
                                        Some(null_idx) => null_idx,
                                        None => bail!("Source schema 'before'/'after' fields are not of expected type")
                                    };
                                    let record_idx = 1 - null_idx;
                                    let (before_piece, after_piece) = (
                                        &before.variants()[record_idx],
                                        &after.variants()[record_idx],
                                    );
                                    let before_name = match before_piece {
                                        SchemaPieceOrNamed::Piece(_) => bail!(
                                            "Source schema 'before' field should be a record."
                                        ),
                                        SchemaPieceOrNamed::Named(name) => name,
                                    };
                                    let after_name = match after_piece {
                                        SchemaPieceOrNamed::Piece(_) => {
                                            bail!("Source schema 'after' field should be a record.")
                                        }
                                        SchemaPieceOrNamed::Named(name) => name,
                                    };
                                    if before_name != after_name {
                                        bail!("Source schema 'before' and 'after' fields should be the same named record.");
                                    }
                                    match schema.lookup(*before_name).piece {
                                        SchemaPiece::Record { .. } => node,
                                        _ => bail!("Source schema 'before' and 'after' fields should contain a record."),
                                    }
                                }
                                (_, SchemaPiece::Union(_)) => {
                                    bail!("Source schema 'before' field should be a union.")
                                }
                                (SchemaPiece::Union(_), _) => {
                                    bail!("Source schema 'after' field should be a union.")
                                }
                                (_, _) => bail!(
                                    "Source schema 'before' and 'after' fields should be unions."
                                ),
                            }
                        }
                        (None, _) => bail!("source schema is missing 'before' field"),
                        (_, None) => bail!("source schema is missing 'after' field"),
                    }
                }
                _ => bail!("Top-level envelope must be a record."),
            }
        }
        EnvelopeType::Upsert => match node.inner {
            SchemaPiece::Record { .. } => schema.top_node(),
            _ => bail!("upsert schema can only be record, got: {:?}", schema.top),
        },
        EnvelopeType::CdcV2 => cdc_v2::extract_data_columns(&schema)?,
        EnvelopeType::None => schema.top_node(),
    };

    // The diff envelope is sane. Convert the actual record schema for the row.
    validate_schema_1(row_schema)
}

fn validate_schema_1(schema: SchemaNode) -> anyhow::Result<Vec<(ColumnName, ColumnType)>> {
    match schema.inner {
        SchemaPiece::Record { fields, .. } => {
            let mut columns = vec![];
            let mut seen_avro_nodes = Default::default();
            for f in fields {
                columns.extend(get_named_columns(
                    &mut seen_avro_nodes,
                    schema.step(&f.schema),
                    &f.name,
                )?);
            }
            Ok(columns)
        }
        _ => bail!("row schemas must be records, got: {:?}", schema.inner),
    }
}

fn get_named_columns<'a>(
    seen_avro_nodes: &mut HashSet<usize>,
    schema: SchemaNode<'a>,
    base_name: &str,
) -> anyhow::Result<Vec<(ColumnName, ColumnType)>> {
    if let SchemaPiece::Union(us) = schema.inner {
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
                    base_name.to_string()
                } else {
                    // There are multiple non-null variants in the
                    // union, so we need to invent field names for
                    // each variant.
                    format!("{}{}", &base_name, i + 1)
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
    } else {
        let scalar_type = validate_schema_2(seen_avro_nodes, schema)?;
        Ok(vec![(base_name.into(), scalar_type.nullable(false))])
    }
}

fn validate_schema_2(
    seen_avro_nodes: &mut HashSet<usize>,
    schema: SchemaNode,
) -> anyhow::Result<ScalarType> {
    Ok(match schema.inner {
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
            if *precision > NUMERIC_DATUM_MAX_PRECISION {
                bail!(
                    "decimals with precision greater than {} are not supported",
                    NUMERIC_DATUM_MAX_PRECISION
                )
            }
            ScalarType::Numeric {
                scale: Some(u8::try_from(*scale).unwrap()),
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
                columns.extend(get_named_columns(seen_avro_nodes, next_node, &f.name)?.into_iter());
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
        config: Option<ccsr::ClientConfig>,
        confluent_wire_format: bool,
    ) -> anyhow::Result<Self> {
        let reader_schema = parse_schema(reader_schema)?;
        let writer_schemas = config.map(|sr| SchemaCache::new(sr)).transpose()?;
        Ok(Self {
            reader_schema,
            writer_schemas,
            confluent_wire_format,
        })
    }

    pub async fn resolve<'a, 'b>(
        &'a mut self,
        mut bytes: &'b [u8],
    ) -> anyhow::Result<(&'b [u8], &'a Schema)> {
        let mut extract_schema_id = || {
            // The first byte is a magic byte (0) that indicates the Confluent
            // serialization format version, and the next four bytes are a big
            // endian 32-bit schema ID.
            //
            // https://docs.confluent.io/current/schema-registry/docs/serializer-formatter.html#wire-format
            if bytes.len() < 5 {
                bail!(
                        "Confluent-style avro datum is too few bytes: expected at least 5 bytes, got {}",
                        bytes.len()
                    );
            }
            let magic = bytes[0];
            let schema_id = BigEndian::read_i32(&bytes[1..5]);
            bytes = &bytes[5..];

            if magic != 0 {
                bail!(
                    "wrong Confluent-style avro serialization magic: expected 0, got {}",
                    magic
                );
            }
            Ok(schema_id)
        };

        let resolved_schema = match &mut self.writer_schemas {
            Some(cache) => {
                debug_assert!(
                    self.confluent_wire_format,
                    "We should have set 'confluent_wire_format' everywhere \
                     that can lead to this branch"
                );
                let schema_id = extract_schema_id()?;
                cache
                    .get(schema_id, &self.reader_schema)
                    .await
                    .map_err(Error::msg)?
            }

            // If we haven't been asked to use a schema registry, we have no way
            // to discover the writer's schema. That's ok; we'll just use the
            // reader's schema and hope it lines up.
            None => {
                if self.confluent_wire_format {
                    // validate and just move the bytes buffer ahead
                    extract_schema_id()?;
                }
                &self.reader_schema
            }
        };
        Ok((bytes, resolved_schema))
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
    ccsr_client: ccsr::Client,
}

impl SchemaCache {
    fn new(schema_registry: ccsr::ClientConfig) -> Result<SchemaCache, anyhow::Error> {
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
                    .retry(|state| async move {
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

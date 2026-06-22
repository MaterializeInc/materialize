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

use std::collections::btree_map::Entry;
use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::sync::Arc;

use anyhow::{Context, anyhow, bail};
use mz_avro::error::Error as AvroError;
use mz_avro::schema::{
    ParseSchemaError, Schema, SchemaNode, SchemaPiece, SchemaPieceOrNamed, resolve_schemas,
};
use mz_ore::cast::CastFrom;
use mz_ore::collections::CollectionExt;
use mz_ore::future::OreFutureExt;
use mz_ore::retry::Retry;
use mz_repr::adt::numeric::{NUMERIC_DATUM_MAX_PRECISION, NumericMaxScale};
use mz_repr::adt::timestamp::TimestampPrecision;
use mz_repr::{ColumnName, RelationDesc, SqlColumnType, SqlScalarType, UNKNOWN_COLUMN_NAME};
use tracing::warn;
use uuid::Uuid;

use crate::avro::is_null;

pub fn parse_schema(schema: &str, references: &[String]) -> anyhow::Result<Schema> {
    let schema: serde_json::Value = serde_json::from_str(schema)?;
    // Parse reference schemas incrementally: each reference may depend on previous ones.
    // References must be provided in dependency order (dependencies first).
    let mut parsed_refs: Vec<Schema> = Vec::with_capacity(references.len());
    for reference in references {
        let ref_json: serde_json::Value = serde_json::from_str(reference)?;
        let parsed = Schema::parse_with_references(&ref_json, &parsed_refs)?;
        parsed_refs.push(parsed);
    }
    Ok(Schema::parse_with_references(&schema, &parsed_refs)?)
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
fn validate_schema_1(schema: SchemaNode) -> anyhow::Result<Vec<(ColumnName, SqlColumnType)>> {
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
    seen_avro_nodes: &mut BTreeSet<usize>,
    schema: SchemaNode<'a>,
    base_name: Option<&str>,
) -> anyhow::Result<Vec<(ColumnName, SqlColumnType)>> {
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
            with_recursion_guard(seen_avro_nodes, schema.root, v, |seen| {
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
                        .unwrap_or_else(|| UNKNOWN_COLUMN_NAME.into())
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
                        .unwrap_or_else(|| UNKNOWN_COLUMN_NAME.into())
                };

                // If there is more than one variant in the union,
                // the column's output type is nullable, as this
                // column will be null whenever it is uninhabited.
                let ty = validate_schema_2(seen, node)?;
                columns.push((name.into(), ty.nullable(vs.len() > 1)));
                Ok(())
            })?;
        }
    }
    Ok(columns)
}

fn get_named_columns<'a>(
    seen_avro_nodes: &mut BTreeSet<usize>,
    schema: SchemaNode<'a>,
    base_name: Option<&str>,
) -> anyhow::Result<Vec<(ColumnName, SqlColumnType)>> {
    if let SchemaPiece::Union(_) = schema.inner {
        get_union_columns(seen_avro_nodes, schema, base_name)
    } else {
        let scalar_type = validate_schema_2(seen_avro_nodes, schema)?;
        Ok(vec![(
            // TODO(benesch): we should do better than this when there's no base
            // name, e.g., invent a name based on the type.
            base_name.unwrap_or(UNKNOWN_COLUMN_NAME).into(),
            scalar_type.nullable(false),
        )])
    }
}

/// Get the single column corresponding to a schema node.
/// It is an error if this node should correspond to more than one column
/// (because it is an Essential Union in the sense described in the module docs).
fn validate_schema_2(
    seen_avro_nodes: &mut BTreeSet<usize>,
    schema: SchemaNode,
) -> anyhow::Result<SqlScalarType> {
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
        SchemaPiece::Boolean => SqlScalarType::Bool,
        SchemaPiece::Int => SqlScalarType::Int32,
        SchemaPiece::Long => SqlScalarType::Int64,
        SchemaPiece::Float => SqlScalarType::Float32,
        SchemaPiece::Double => SqlScalarType::Float64,
        SchemaPiece::Date => SqlScalarType::Date,
        SchemaPiece::TimestampMilli => SqlScalarType::Timestamp {
            precision: Some(TimestampPrecision::try_from(3).unwrap()),
        },
        SchemaPiece::TimestampMicro => SqlScalarType::Timestamp {
            precision: Some(TimestampPrecision::try_from(6).unwrap()),
        },
        SchemaPiece::Decimal {
            precision, scale, ..
        } => {
            if *precision > usize::cast_from(NUMERIC_DATUM_MAX_PRECISION) {
                bail!(
                    "decimals with precision greater than {} are not supported",
                    NUMERIC_DATUM_MAX_PRECISION
                )
            }
            SqlScalarType::Numeric {
                max_scale: Some(NumericMaxScale::try_from(*scale)?),
            }
        }
        SchemaPiece::Bytes | SchemaPiece::Fixed { .. } => SqlScalarType::Bytes,
        SchemaPiece::String | SchemaPiece::Enum { .. } => SqlScalarType::String,

        SchemaPiece::Json => SqlScalarType::Jsonb,
        SchemaPiece::Uuid => SqlScalarType::Uuid,
        SchemaPiece::Record { fields, .. } => {
            let mut columns = vec![];
            for f in fields {
                with_recursion_guard(seen_avro_nodes, schema.root, &f.schema, |seen| {
                    columns.extend(get_named_columns(
                        seen,
                        schema.step(&f.schema),
                        Some(&f.name),
                    )?);
                    Ok(())
                })?;
            }
            SqlScalarType::Record {
                fields: columns.into(),
                custom_id: None,
            }
        }
        SchemaPiece::Array(inner) => {
            with_recursion_guard(seen_avro_nodes, schema.root, inner.as_ref(), |seen| {
                Ok(SqlScalarType::List {
                    element_type: Box::new(validate_schema_2(seen, schema.step(inner))?),
                    custom_id: None,
                })
            })?
        }
        SchemaPiece::Map(inner) => {
            with_recursion_guard(seen_avro_nodes, schema.root, inner.as_ref(), |seen| {
                Ok(SqlScalarType::Map {
                    value_type: Box::new(validate_schema_2(seen, schema.step(inner))?),
                    custom_id: None,
                })
            })?
        }
        _ => bail!("Unsupported type in schema: {:?}", schema.inner),
    })
}

/// Runs `f` with `node` marked as on the current resolution path, bailing if it's
/// already on the path (a cycle). The mark is cleared on exit so sibling reuse of a
/// named type isn't flagged.
fn with_recursion_guard<T>(
    seen: &mut BTreeSet<usize>,
    root: &Schema,
    node: &SchemaPieceOrNamed,
    f: impl FnOnce(&mut BTreeSet<usize>) -> anyhow::Result<T>,
) -> anyhow::Result<T> {
    let named_idx = match node {
        SchemaPieceOrNamed::Named(idx) => Some(*idx),
        SchemaPieceOrNamed::Piece(_) => None,
    };
    if let Some(named_idx) = named_idx {
        if !seen.insert(named_idx) {
            bail!(
                "Recursive types are not supported: {}",
                node.get_human_name(root)
            );
        }
    }
    let result = f(seen);
    if let Some(named_idx) = named_idx {
        seen.remove(&named_idx);
    }
    result
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A named type that refers back to itself cannot be represented in the SQL
    /// type system. Recursion can be introduced through any container that holds
    /// a named reference: record fields (directly or via a union), arrays, and
    /// maps. Each should be rejected rather than recursed into forever.
    fn assert_recursive(schema: &str) {
        let err = schema_to_relationdesc(parse_schema(schema, &[]).expect("schema should parse"))
            .expect_err("recursive schema should be rejected");
        assert!(
            err.to_string()
                .contains("Recursive types are not supported"),
            "unexpected error: {err}"
        );
    }

    #[mz_ore::test]
    fn recursive_record_field() {
        assert_recursive(r#"{"type":"record","name":"a","fields":[{"name":"f","type":"a"}]}"#);
    }

    #[mz_ore::test]
    fn recursive_union() {
        assert_recursive(
            r#"{"type":"record","name":"a","fields":[{"name":"f","type":["a","null"]}]}"#,
        );
    }

    #[mz_ore::test]
    fn recursive_array() {
        assert_recursive(
            r#"{"type":"record","name":"a","fields":[{"name":"f","type":{"type":"array","items":"a"}}]}"#,
        );
    }

    #[mz_ore::test]
    fn recursive_map() {
        assert_recursive(
            r#"{"type":"record","name":"a","fields":[{"name":"f","type":{"type":"map","values":"a"}}]}"#,
        );
    }

    /// Reusing a named type in sibling positions is a diamond, not a cycle, and
    /// must not be flagged as recursive. Guards against the path-tracking set
    /// failing to release a node after it leaves the current path.
    #[mz_ore::test]
    fn repeated_named_type_is_not_recursive() {
        let schema = r#"{
            "type": "record",
            "name": "outer",
            "fields": [
                {"name": "a", "type": {"type": "record", "name": "inner", "fields": [{"name": "x", "type": "int"}]}},
                {"name": "b", "type": "inner"}
            ]
        }"#;
        let desc = schema_to_relationdesc(parse_schema(schema, &[]).expect("schema should parse"))
            .expect("diamond reuse of a named type should be allowed");
        assert_eq!(desc.arity(), 2);
    }

    #[mz_ore::test]
    fn registry_name_from_schema_arn_commercial() {
        assert_eq!(
            registry_name_from_schema_arn(
                "arn:aws:glue:us-east-1:123456789012:schema/myreg/myschema"
            ),
            Some("myreg")
        );
    }

    #[mz_ore::test]
    fn registry_name_from_schema_arn_china_partition() {
        assert_eq!(
            registry_name_from_schema_arn(
                "arn:aws-cn:glue:cn-north-1:123456789012:schema/myreg/myschema"
            ),
            Some("myreg")
        );
    }

    #[mz_ore::test]
    fn registry_name_from_schema_arn_govcloud_partition() {
        assert_eq!(
            registry_name_from_schema_arn(
                "arn:aws-us-gov:glue:us-gov-west-1:123456789012:schema/myreg/myschema"
            ),
            Some("myreg")
        );
    }

    #[mz_ore::test]
    fn registry_name_from_schema_arn_rejects_non_aws_partition() {
        assert_eq!(
            registry_name_from_schema_arn(
                "arn:gcp:glue:us-east-1:123456789012:schema/myreg/myschema"
            ),
            None
        );
    }

    #[mz_ore::test]
    fn registry_name_from_schema_arn_rejects_missing_arn_prefix() {
        // Bare `schema/...` fragments must not parse — see the anchor on
        // `arn:` in [`registry_name_from_schema_arn`].
        assert_eq!(registry_name_from_schema_arn("schema/myreg/myschema"), None);
    }

    #[mz_ore::test]
    fn registry_name_from_schema_arn_rejects_non_glue_service() {
        assert_eq!(
            registry_name_from_schema_arn(
                "arn:aws:s3:us-east-1:123456789012:schema/myreg/myschema"
            ),
            None
        );
    }

    #[mz_ore::test]
    fn registry_name_from_schema_arn_rejects_missing_schema_segment() {
        assert_eq!(
            registry_name_from_schema_arn("arn:aws:glue:us-east-1:123456789012:table/myreg/foo"),
            None
        );
    }

    #[mz_ore::test]
    fn registry_name_from_schema_arn_rejects_empty_registry() {
        assert_eq!(
            registry_name_from_schema_arn("arn:aws:glue:us-east-1:123456789012:schema//myschema"),
            None
        );
    }

    #[mz_ore::test]
    fn registry_name_from_schema_arn_allows_schema_name_with_slashes() {
        // Glue schema *names* aren't restricted the way registry names are.
        // Only the segment before the first `/` matters for the registry.
        assert_eq!(
            registry_name_from_schema_arn(
                "arn:aws:glue:us-east-1:123456789012:schema/myreg/path/like/name"
            ),
            Some("myreg")
        );
    }
}

/// Identifier carried in a wire-format header that points at the writer's
/// schema. Different schema registries key their writer schemas differently:
/// Confluent uses a sequential `i32`, AWS Glue uses a UUID. Callers do not
/// have to care which kind of key they're holding — the resolver routes it
/// back to the matching cache.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WriterSchemaKey {
    Confluent(i32),
    Glue(Uuid),
}

impl fmt::Display for WriterSchemaKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            WriterSchemaKey::Confluent(id) => write!(f, "Confluent schema id {}", id),
            WriterSchemaKey::Glue(uuid) => write!(f, "Glue schema-version {}", uuid),
        }
    }
}

/// Provides writer schemas to an [`AvroSchemaResolver`].
///
/// Mirrors the `WireFormat<C>` enum on the catalog side: a decoder can run
/// without any wire-format framing, with Confluent framing, or with AWS
/// Glue framing (each of the framed variants optionally without a
/// registry to fetch from). Each variant owns its cache type by
/// construction, so the resolver cannot mis-route a key to the wrong
/// cache.
pub enum WriterSchemaProvider {
    /// No wire-format framing. The resolver always returns the reader
    /// schema and never consumes header bytes.
    None,
    /// Confluent framing. `cache: None` means strip-and-discard the
    /// schema id (no registry attached); `cache: Some` means fetch from
    /// the cache.
    Confluent { cache: Option<SchemaCache> },
    /// AWS Glue framing. `cache: None` means strip-and-discard the UUID
    /// (no registry attached); `cache: Some` means fetch from the cache.
    Glue { cache: Option<GlueSchemaCache> },
}

impl fmt::Debug for WriterSchemaProvider {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let tag = match self {
            WriterSchemaProvider::None => "none",
            WriterSchemaProvider::Confluent { cache: None } => "confluent (no cache)",
            WriterSchemaProvider::Confluent { cache: Some(_) } => "confluent",
            WriterSchemaProvider::Glue { cache: None } => "glue (no cache)",
            WriterSchemaProvider::Glue { cache: Some(_) } => "glue",
        };
        f.debug_tuple("WriterSchemaProvider").field(&tag).finish()
    }
}

impl WriterSchemaProvider {
    /// Build the Confluent variant from an optional CCSR client. `None`
    /// means "Confluent framing but no registry to fetch from" — the
    /// resolver will strip the header and fall back to the reader schema.
    pub fn confluent(ccsr_client: Option<mz_ccsr::Client>) -> Self {
        let cache = ccsr_client.map(SchemaCache::new);
        WriterSchemaProvider::Confluent { cache }
    }

    /// Build the Glue variant from an optional (client, registry-name)
    /// pair. `None` means "Glue framing but no registry to fetch from" —
    /// the resolver strips the UUID header and falls back to the reader
    /// schema. When the pair is supplied, the registry name is the one
    /// the catalog connection points at; the cache rejects any UUID
    /// whose `SchemaArn` doesn't sit under that registry. (Schema
    /// *names* aren't unique across Glue registries, so the registry
    /// name is the only meaningful scope here.) Pairing the two in a
    /// tuple makes the "no client → no registry name" case unrepresentable.
    pub fn glue(
        client_with_registry: Option<(mz_aws_glue_schema_registry::Client, String)>,
    ) -> Self {
        let cache = client_with_registry.map(|(c, registry)| GlueSchemaCache::new(c, registry));
        WriterSchemaProvider::Glue { cache }
    }
}

pub struct AvroSchemaResolver {
    reader_schema: Schema,
    writer_schemas: WriterSchemaProvider,
}

impl AvroSchemaResolver {
    pub fn new(
        reader_schema: &str,
        reader_reference_schemas: &[String],
        writer_schemas: WriterSchemaProvider,
    ) -> anyhow::Result<Self> {
        // parse_schema handles incremental parsing of references (dependencies first)
        let reader_schema = parse_schema(reader_schema, reader_reference_schemas)?;
        Ok(Self {
            reader_schema,
            writer_schemas,
        })
    }

    pub async fn resolve<'a, 'b>(
        &'a mut self,
        mut bytes: &'b [u8],
    ) -> anyhow::Result<anyhow::Result<(&'b [u8], &'a Schema, Option<WriterSchemaKey>)>> {
        let (resolved_schema, key) = match &mut self.writer_schemas {
            WriterSchemaProvider::None => (&self.reader_schema, None),

            WriterSchemaProvider::Confluent { cache: None } => {
                // Validate the header (so we surface producer/consumer
                // framing mismatches early) and discard the schema id —
                // there is no registry to look it up in.
                match crate::confluent::extract_avro_header(bytes) {
                    Ok((_id, adjusted_bytes)) => {
                        bytes = adjusted_bytes;
                        (&self.reader_schema, None)
                    }
                    Err(err) => return Ok(Err(err)),
                }
            }

            WriterSchemaProvider::Confluent { cache: Some(cache) } => {
                let (id, adjusted_bytes) = match crate::confluent::extract_avro_header(bytes) {
                    Ok(ok) => ok,
                    Err(err) => return Ok(Err(err)),
                };
                bytes = adjusted_bytes;
                let result = cache
                    .get(id, &self.reader_schema)
                    // The outer Result describes transient errors so use ?
                    // here to propagate; the inner Result is the cached
                    // permanent outcome (parsed schema or parse error) and
                    // is handled below.
                    .await?
                    .with_context(|| format!("failed to resolve Avro schema (id = {id})"));
                let schema = match result {
                    Ok(schema) => schema,
                    Err(err) => return Ok(Err(err)),
                };
                (schema, Some(WriterSchemaKey::Confluent(id)))
            }

            WriterSchemaProvider::Glue { cache: None } => {
                // Strip + discard the header; no registry to look up.
                match crate::glue::extract_avro_header(bytes) {
                    Ok((_uuid, adjusted_bytes)) => {
                        bytes = adjusted_bytes;
                        (&self.reader_schema, None)
                    }
                    Err(err) => return Ok(Err(err)),
                }
            }

            WriterSchemaProvider::Glue { cache: Some(cache) } => {
                let (uuid, adjusted_bytes) = match crate::glue::extract_avro_header(bytes) {
                    Ok(ok) => ok,
                    Err(err) => return Ok(Err(err)),
                };
                bytes = adjusted_bytes;
                let result = cache
                    .get(uuid, &self.reader_schema)
                    .await?
                    .with_context(|| format!("failed to resolve Avro schema (uuid = {uuid})"));
                let schema = match result {
                    Ok(schema) => schema,
                    Err(err) => return Ok(Err(err)),
                };
                (schema, Some(WriterSchemaKey::Glue(uuid)))
            }
        };
        Ok(Ok((bytes, resolved_schema, key)))
    }
}

impl fmt::Debug for AvroSchemaResolver {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("AvroSchemaResolver")
            .field("reader_schema", &self.reader_schema)
            .field("writer_schemas", &self.writer_schemas)
            .finish()
    }
}

/// Glue-side analogue of [`SchemaCache`].
///
/// Differences from the CSR cache:
/// * Keys are UUIDs (Glue schema-version IDs), not `i32`s.
/// * Glue schemas are single definitions — no `references` field on
///   `GetSchemaVersion`, so the cache does not chase a dependency graph.
/// * No outer retry layer. `aws-sdk-glue` ships a "standard" retry policy
///   by default that handles transient errors; layering our own
///   `Retry::default()` on top would only amplify backoff.
#[derive(Debug)]
pub struct GlueSchemaCache {
    cache: BTreeMap<Uuid, Result<Schema, AvroError>>,
    glue_client: mz_aws_glue_schema_registry::Client,
    /// The registry name the catalog connection points at. Per-UUID
    /// fetches are cross-checked against this; mismatches become decode
    /// errors. See type-level docs.
    expected_registry: String,
}

impl GlueSchemaCache {
    fn new(glue_client: mz_aws_glue_schema_registry::Client, expected_registry: String) -> Self {
        GlueSchemaCache {
            cache: BTreeMap::new(),
            glue_client,
            expected_registry,
        }
    }

    /// Look up the writer schema for `uuid`, fetching from Glue on a
    /// cache miss. Mirrors [`SchemaCache::get`]: the outer `Result`
    /// surfaces transient errors (network, auth, throttling — already
    /// retried by the SDK) that the caller may retry on; the inner
    /// `Result` carries permanent failures (schema not found,
    /// non-`Available` lifecycle, registry mismatch, parse failure) that
    /// get cached so the same bad UUID does not re-hit Glue on every
    /// record.
    async fn get(
        &mut self,
        uuid: Uuid,
        reader_schema: &Schema,
    ) -> anyhow::Result<anyhow::Result<&Schema>> {
        let entry = match self.cache.entry(uuid) {
            Entry::Occupied(o) => o.into_mut(),
            Entry::Vacant(v) => {
                let parsed: Result<Schema, AvroError> = match self
                    .glue_client
                    .get_schema_version_by_id(uuid)
                    .await
                {
                    Ok(version) => {
                        Self::parse_version(version, uuid, &self.expected_registry, reader_schema)
                    }
                    // Permanent: UUID does not exist in any visible
                    // registry. Cache so we don't re-fetch on every
                    // record carrying this UUID.
                    Err(mz_aws_glue_schema_registry::GetSchemaVersionError::NotFound) => Err(
                        ParseSchemaError::new(format!("Glue schema version {uuid} not found"))
                            .into(),
                    ),
                    // Transient: SDK has already retried; surface to the
                    // outer Result so the source can decide. The explicit
                    // `Other` arm (rather than a wildcard) makes any
                    // future variant a compile error rather than a
                    // silent reclassification.
                    Err(e @ mz_aws_glue_schema_registry::GetSchemaVersionError::Other(_)) => {
                        return Err(e.into());
                    }
                };
                v.insert(parsed)
            }
        };
        Ok(entry.as_ref().map_err(|e| anyhow::Error::new(e.clone())))
    }

    /// Validate and parse a fetched [`mz_aws_glue_schema_registry::SchemaVersion`] into an Avro schema.
    ///
    /// All failures here are permanent (a retry would return the same
    /// `SchemaVersion`) and get cached by the caller.
    fn parse_version(
        version: mz_aws_glue_schema_registry::SchemaVersion,
        uuid: Uuid,
        expected_registry: &str,
        reader_schema: &Schema,
    ) -> Result<Schema, AvroError> {
        use mz_aws_glue_schema_registry::SchemaVersionLifecycleStatus;

        // Reject anything not Available. Pending/Failure versions may
        // still carry a `definition` from the SDK, but decoding records
        // against them would yield garbage.
        if !matches!(
            version.lifecycle_status,
            Some(SchemaVersionLifecycleStatus::Available)
        ) {
            return Err(ParseSchemaError::new(format!(
                "Glue schema version {uuid} is not Available (status: {:?}); \
                 refusing to decode",
                version.lifecycle_status
            ))
            .into());
        }
        let definition = version.definition.ok_or_else(|| {
            ParseSchemaError::new(format!(
                "Glue schema version {uuid} returned without a definition"
            ))
        })?;
        // Enforce that the fetched version actually lives in the
        // registry our catalog connection points at. Without this, any
        // UUID the credentials can resolve would decode silently,
        // defeating the per-connection scope.
        let arn = version.schema_arn.as_deref().ok_or_else(|| {
            ParseSchemaError::new(format!(
                "Glue schema version {uuid} returned without a SchemaArn; \
                 cannot verify registry membership"
            ))
        })?;
        let actual_registry = registry_name_from_schema_arn(arn).ok_or_else(|| {
            ParseSchemaError::new(format!(
                "Glue SchemaArn {arn:?} did not match the expected \
                 arn:aws[-...]:glue:<region>:<account>:schema/<registry>/<schema> form"
            ))
        })?;
        if actual_registry != expected_registry {
            return Err(ParseSchemaError::new(format!(
                "Glue schema version {uuid} lives in registry {actual_registry:?} \
                 but this source is configured for registry {expected:?}; \
                 refusing to decode",
                expected = expected_registry,
            ))
            .into());
        }
        let value: serde_json::Value = serde_json::from_str(&definition)
            .map_err(|e| ParseSchemaError::new(format!("Error parsing JSON: {e}")))?;
        let schema = Schema::parse_with_references(&value, &[])?;
        resolve_schemas(&schema, reader_schema)
    }
}

/// Parse the registry name out of a Glue `SchemaArn`.
///
/// Glue schema ARNs have the shape
/// `arn:<partition>:glue:<region>:<account>:schema/<registry>/<schema>`,
/// where `<partition>` is `aws`, `aws-cn`, `aws-us-gov`, etc. We anchor
/// on `arn:` and the `:glue:` segment so a fragment like
/// `:schema/foo/bar` alone won't parse. Returns `None` if the input
/// doesn't match — we surface that as a decode error rather than
/// panicking, so schemas in unexpected partitions or future ARN shapes
/// don't crash the source.
fn registry_name_from_schema_arn(arn: &str) -> Option<&str> {
    let rest = arn.strip_prefix("arn:")?;
    let (partition, after_partition) = rest.split_once(":glue:")?;
    if !partition.starts_with("aws") {
        return None;
    }
    let (_, after_schema) = after_partition.split_once(":schema/")?;
    let (registry, _) = after_schema.split_once('/')?;
    if registry.is_empty() {
        return None;
    }
    Some(registry)
}

/// Cache of writer schemas fetched from a Confluent Schema Registry. Held
/// inside [`WriterSchemaProvider::Confluent`]; the type is named pub because that
/// variant's field is reachable through the pub enum, but it has no pub
/// constructor or methods — only [`WriterSchemaProvider::confluent`] can build one.
#[derive(Debug)]
pub struct SchemaCache {
    cache: BTreeMap<i32, Result<Schema, AvroError>>,
    ccsr_client: Arc<mz_ccsr::Client>,
}

impl SchemaCache {
    fn new(ccsr_client: mz_ccsr::Client) -> SchemaCache {
        SchemaCache {
            cache: BTreeMap::new(),
            ccsr_client: Arc::new(ccsr_client),
        }
    }

    /// Looks up the writer schema for ID. If the schema is literally identical
    /// to the reader schema, as determined by the reader schema fingerprint
    /// that this schema cache was initialized with, returns the schema directly.
    /// If not, performs schema resolution on the reader and writer and
    /// returns the result.
    ///
    /// This method also handles schema references: if the schema references types
    /// defined in other schemas, those schemas are fetched and their types are made
    /// available during parsing.
    async fn get(
        &mut self,
        id: i32,
        reader_schema: &Schema,
    ) -> anyhow::Result<anyhow::Result<&Schema>> {
        let entry = match self.cache.entry(id) {
            Entry::Occupied(o) => o.into_mut(),
            Entry::Vacant(v) => {
                // An issue with _fetching_ the schema should be returned
                // immediately, and not cached, since it might get better on the
                // next retry.
                let ccsr_client = Arc::clone(&self.ccsr_client);

                // Fetch schema with its references (if any)
                let (primary_subject, reference_subjects) = Retry::default()
                    // Twice the timeout of the ccsr client so we can attempt 2 requests.
                    .max_duration(ccsr_client.timeout() * 2)
                    // Canceling because ultimately it's just non-mutating HTTP requests.
                    .retry_async_canceling(move |state| {
                        let ccsr_client = Arc::clone(&ccsr_client);
                        async move {
                            let res = ccsr_client.get_subject_and_references_by_id(id).await;
                            match res {
                                Err(e) => {
                                    if let Some(timeout) = state.next_backoff {
                                        warn!(
                                            "transient failure fetching \
                                                schema id {}: {:?}, retrying in {:?}",
                                            id, e, timeout
                                        );
                                    }
                                    Err(anyhow::Error::from(e))
                                }
                                _ => Ok(res?),
                            }
                        }
                    })
                    .run_in_task(|| format!("fetch_avro_schema:{}", id))
                    .await?;

                // Now, we've gotten some json back, so we want to cache it (regardless of whether it's a valid
                // avro schema, it won't change).
                //
                // However, we can't just cache it directly, since resolving schemas takes significant CPU work,
                // which we don't want to repeat for every record. So, parse and resolve it, and cache the
                // result (whether schema or error).
                let result = Self::parse_with_references(
                    &primary_subject,
                    &reference_subjects,
                    reader_schema,
                );
                v.insert(result)
            }
        };
        Ok(entry.as_ref().map_err(|e| anyhow::Error::new(e.clone())))
    }

    /// Parse a schema along with its references and resolve against the reader schema.
    fn parse_with_references(
        primary_subject: &mz_ccsr::Subject,
        reference_subjects: &[mz_ccsr::Subject],
        reader_schema: &Schema,
    ) -> Result<Schema, AvroError> {
        // Parse referenced schemas incrementally: each reference may depend on previous ones.
        let mut reference_schemas: Vec<Schema> = Vec::with_capacity(reference_subjects.len());
        for subject in reference_subjects {
            let ref_json: serde_json::Value = serde_json::from_str(&subject.schema.raw)
                .map_err(|e| ParseSchemaError::new(format!("Error parsing JSON: {}", e)))?;
            let parsed = Schema::parse_with_references(&ref_json, &reference_schemas)?;
            reference_schemas.push(parsed);
        }

        // Parse primary schema, using references, if present.
        let primary_value: serde_json::Value = serde_json::from_str(&primary_subject.schema.raw)
            .map_err(|e| ParseSchemaError::new(format!("Error parsing JSON: {}", e)))?;
        let schema = Schema::parse_with_references(&primary_value, &reference_schemas)?;

        // Schema fingerprints don't actually capture whether two schemas are meaningfully
        // different, because they strip out logical types. Thus, resolve in all cases.
        let resolved = resolve_schemas(&schema, reader_schema)?;
        Ok(resolved)
    }
}

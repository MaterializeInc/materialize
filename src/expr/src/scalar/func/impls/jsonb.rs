// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::BTreeMap;
use std::fmt;

use mz_expr_derive::sqlfunc;
use mz_repr::adt::jsonb::{Jsonb, JsonbRef};
use mz_repr::adt::mz_acl_item::{AclMode, MzAclItem};
use mz_repr::adt::numeric::{self, Numeric, NumericMaxScale};
use mz_repr::role_id::RoleId;
use mz_repr::{ArrayRustType, Datum, Row, RowPacker, SqlColumnType, SqlScalarType, strconv};
use mz_sql_parser::ast::display::AstDisplay;
use mz_sql_parser::ast::{
    AstInfo, AvroSchema, ConnectionOption, ConnectionOptionName, CreateConnectionType,
    CreateSinkConnection, CreateSubsourceOptionName, Format, FormatSpecifier,
    IcebergSinkConfigOptionName, IcebergSinkMode, KafkaSinkConfigOptionName,
    KafkaSourceConfigOptionName, PgConfigOptionName, ProtobufSchema, RawClusterName, RawItemName,
    SinkEnvelope, SourceEnvelope, SourceErrorPolicy, UnresolvedItemName, Value, WithOptionValue,
};
use prost::Message as _;
use serde::{Deserialize, Serialize};
use serde_json::json;

use crate::EvalError;
use crate::scalar::func::EagerUnaryFunc;
use crate::scalar::func::impls::numeric::*;

#[sqlfunc(
    sqlname = "jsonb_to_text",
    preserves_uniqueness = false,
    inverse = to_unary!(super::CastStringToJsonb)
)]
pub fn cast_jsonb_to_string<'a>(a: JsonbRef<'a>) -> String {
    let mut buf = String::new();
    strconv::format_jsonb(&mut buf, a);
    buf
}

#[sqlfunc(sqlname = "jsonb_to_smallint", is_monotone = true)]
fn cast_jsonb_to_int16<'a>(a: JsonbRef<'a>) -> Result<i16, EvalError> {
    match a.into_datum() {
        Datum::Numeric(a) => cast_numeric_to_int16(a.into_inner()),
        datum => Err(EvalError::InvalidJsonbCast {
            from: jsonb_typeof(JsonbRef::from_datum(datum)).into(),
            to: "smallint".into(),
        }),
    }
}

#[sqlfunc(sqlname = "jsonb_to_integer", is_monotone = true)]
fn cast_jsonb_to_int32<'a>(a: JsonbRef<'a>) -> Result<i32, EvalError> {
    match a.into_datum() {
        Datum::Numeric(a) => cast_numeric_to_int32(a.into_inner()),
        datum => Err(EvalError::InvalidJsonbCast {
            from: jsonb_typeof(JsonbRef::from_datum(datum)).into(),
            to: "integer".into(),
        }),
    }
}

#[sqlfunc(sqlname = "jsonb_to_bigint", is_monotone = true)]
fn cast_jsonb_to_int64<'a>(a: JsonbRef<'a>) -> Result<i64, EvalError> {
    match a.into_datum() {
        Datum::Numeric(a) => cast_numeric_to_int64(a.into_inner()),
        datum => Err(EvalError::InvalidJsonbCast {
            from: jsonb_typeof(JsonbRef::from_datum(datum)).into(),
            to: "bigint".into(),
        }),
    }
}

#[sqlfunc(sqlname = "jsonb_to_real", is_monotone = true)]
fn cast_jsonb_to_float32<'a>(a: JsonbRef<'a>) -> Result<f32, EvalError> {
    match a.into_datum() {
        Datum::Numeric(a) => cast_numeric_to_float32(a.into_inner()),
        datum => Err(EvalError::InvalidJsonbCast {
            from: jsonb_typeof(JsonbRef::from_datum(datum)).into(),
            to: "real".into(),
        }),
    }
}

#[sqlfunc(sqlname = "jsonb_to_double", is_monotone = true)]
fn cast_jsonb_to_float64<'a>(a: JsonbRef<'a>) -> Result<f64, EvalError> {
    match a.into_datum() {
        Datum::Numeric(a) => cast_numeric_to_float64(a.into_inner()),
        datum => Err(EvalError::InvalidJsonbCast {
            from: jsonb_typeof(JsonbRef::from_datum(datum)).into(),
            to: "double precision".into(),
        }),
    }
}

#[derive(
    Ord,
    PartialOrd,
    Clone,
    Debug,
    Eq,
    PartialEq,
    Serialize,
    Deserialize,
    Hash
)]
pub struct CastJsonbToNumeric(pub Option<NumericMaxScale>);

impl EagerUnaryFunc for CastJsonbToNumeric {
    type Input<'a> = JsonbRef<'a>;
    type Output<'a> = Result<Numeric, EvalError>;

    fn call<'a>(&self, a: Self::Input<'a>) -> Self::Output<'a> {
        match a.into_datum() {
            Datum::Numeric(mut num) => match self.0 {
                None => Ok(num.into_inner()),
                Some(scale) => {
                    if numeric::rescale(&mut num.0, scale.into_u8()).is_err() {
                        return Err(EvalError::NumericFieldOverflow);
                    };
                    Ok(num.into_inner())
                }
            },
            datum => Err(EvalError::InvalidJsonbCast {
                from: jsonb_typeof(JsonbRef::from_datum(datum)).into(),
                to: "numeric".into(),
            }),
        }
    }

    fn output_sql_type(&self, input: SqlColumnType) -> SqlColumnType {
        SqlScalarType::Numeric { max_scale: self.0 }.nullable(input.nullable)
    }

    fn is_monotone(&self) -> bool {
        true
    }
}

impl fmt::Display for CastJsonbToNumeric {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str("jsonb_to_numeric")
    }
}

#[sqlfunc(sqlname = "jsonb_to_boolean", is_monotone = true)]
fn cast_jsonb_to_bool<'a>(a: JsonbRef<'a>) -> Result<bool, EvalError> {
    match a.into_datum() {
        Datum::True => Ok(true),
        Datum::False => Ok(false),
        datum => Err(EvalError::InvalidJsonbCast {
            from: jsonb_typeof(JsonbRef::from_datum(datum)).into(),
            to: "boolean".into(),
        }),
    }
}

#[sqlfunc(sqlname = "jsonbable_to_jsonb")]
fn cast_jsonbable_to_jsonb<'a>(a: JsonbRef<'a>) -> JsonbRef<'a> {
    match a.into_datum() {
        Datum::Numeric(n) => {
            let n = n.into_inner();
            let datum = if n.is_finite() {
                Datum::from(n)
            } else if n.is_nan() {
                Datum::String("NaN")
            } else if n.is_negative() {
                Datum::String("-Infinity")
            } else {
                Datum::String("Infinity")
            };
            JsonbRef::from_datum(datum)
        }
        datum => JsonbRef::from_datum(datum),
    }
}

#[sqlfunc]
fn jsonb_array_length<'a>(a: JsonbRef<'a>) -> Result<Option<i32>, EvalError> {
    match a.into_datum() {
        Datum::List(list) => {
            let count = list.iter().count();
            match i32::try_from(count) {
                Ok(len) => Ok(Some(len)),
                Err(_) => Err(EvalError::Int32OutOfRange(count.to_string().into())),
            }
        }
        _ => Ok(None),
    }
}

#[sqlfunc]
fn jsonb_typeof<'a>(a: JsonbRef<'a>) -> &'a str {
    match a.into_datum() {
        Datum::Map(_) => "object",
        Datum::List(_) => "array",
        Datum::String(_) => "string",
        Datum::Numeric(_) => "number",
        Datum::True | Datum::False => "boolean",
        Datum::JsonNull => "null",
        d => panic!("Not jsonb: {:?}", d),
    }
}

#[sqlfunc]
fn jsonb_strip_nulls<'a>(a: JsonbRef<'a>) -> Jsonb {
    fn strip_nulls(a: Datum, row: &mut RowPacker) {
        match a {
            Datum::Map(dict) => row.push_dict_with(|row| {
                for (k, v) in dict.iter() {
                    match v {
                        Datum::JsonNull => (),
                        _ => {
                            row.push(Datum::String(k));
                            strip_nulls(v, row);
                        }
                    }
                }
            }),
            Datum::List(list) => row.push_list_with(|row| {
                for elem in list.iter() {
                    strip_nulls(elem, row);
                }
            }),
            _ => row.push(a),
        }
    }
    let mut row = Row::default();
    strip_nulls(a.into_datum(), &mut row.packer());
    Jsonb::from_row(row)
}

// NOTE: no budget pre-check, see the exception on `crate::func::check_build_fits_budget`.
#[sqlfunc]
fn jsonb_pretty<'a>(a: JsonbRef<'a>) -> String {
    let mut buf = String::new();
    strconv::format_jsonb_pretty(&mut buf, a);
    buf
}

/// Converts a JSONB `Datum` into a `u64`.
fn jsonb_datum_to_u64<'a>(d: Datum<'a>) -> Result<u64, String> {
    let Datum::Numeric(n) = d else {
        return Err("expected numeric value".into());
    };

    let mut cx = numeric::cx_datum();
    cx.try_into_u64(n.0)
        .map_err(|_| format!("number out of u64 range: {n}"))
}

/// Decodes a JSONB object of shape `{"bitflags": <u64>}` into an `AclMode`.
///
/// Shared decoder for `parse_catalog_privileges` (which embeds the object as
/// the `acl_mode` field of each privilege) and `parse_catalog_acl_mode` (which
/// receives the object at the top level).
fn jsonb_datum_to_acl_mode(d: Datum) -> Result<AclMode, String> {
    let Datum::Map(dict) = d else {
        return Err(format!("unexpected acl_mode: {d}"));
    };
    let mut bits = None;
    for (key, val) in dict.iter() {
        match key {
            "bitflags" => bits = Some(jsonb_datum_to_u64(val)?),
            other => return Err(format!("unexpected acl_mode field: {other}")),
        }
    }
    let bits = bits.ok_or_else(|| "missing acl_mode bitflags".to_string())?;
    AclMode::from_bits(bits).ok_or_else(|| format!("invalid acl_mode bitflags: {bits}"))
}

/// Converts a JSONB `Datum` into a `RoleId`.
fn jsonb_datum_to_role_id(d: Datum) -> Result<RoleId, String> {
    match d {
        Datum::String("Public") => Ok(RoleId::Public),
        Datum::String(other) => Err(format!("unexpected role ID variant: {other}")),
        Datum::Map(dict) => {
            let (key, val) = dict.iter().next().ok_or_else(|| "empty".to_string())?;
            let n = jsonb_datum_to_u64(val)?;
            match key {
                "User" => Ok(RoleId::User(n)),
                "System" => Ok(RoleId::System(n)),
                "Predefined" => Ok(RoleId::Predefined(n)),
                other => Err(format!("unexpected role ID variant: {other}")),
            }
        }
        _ => Err("expected string or object".into()),
    }
}

/// Converts a catalog JSON-serialized ID value into the appropriate string format.
///
/// Supports all of Materialize's various ID types of the form `<prefix><u64>`.
#[sqlfunc]
fn parse_catalog_id<'a>(a: JsonbRef<'a>) -> Result<String, EvalError> {
    let parse = || match a.into_datum() {
        // Unit variant, e.g. "Public"
        Datum::String(variant) => match variant {
            "Explain" => Ok("e".to_string()),
            "Public" => Ok("p".to_string()),
            other => Err(format!("unexpected ID variant: {other}")),
        },
        // Newtype variant, e.g. {"User": 1}
        Datum::Map(dict) => {
            let (key, val) = dict.iter().next().ok_or_else(|| "empty".to_string())?;
            let prefix = match key {
                "IntrospectionSourceIndex" => "si",
                "Predefined" => "g",
                "System" => "s",
                "Transient" => "t",
                "User" => "u",
                other => return Err(format!("unexpected ID variant: {other}")),
            };
            let n = jsonb_datum_to_u64(val)?;
            Ok(format!("{prefix}{n}"))
        }
        _ => Err("expected string or object".into()),
    };

    parse().map_err(|e| EvalError::InvalidCatalogJson(e.into()))
}

/// Converts a catalog JSON-serialized privilege array into an `mz_aclitem[]`.
#[sqlfunc]
fn parse_catalog_privileges<'a>(a: JsonbRef<'a>) -> Result<ArrayRustType<MzAclItem>, EvalError> {
    let parse_one = |datum| match datum {
        Datum::Map(dict) => {
            let mut grantee = None;
            let mut grantor = None;
            let mut acl_mode = None;
            for (key, val) in dict.iter() {
                match key {
                    "grantee" => {
                        let id = jsonb_datum_to_role_id(val)?;
                        grantee = Some(id);
                    }
                    "grantor" => {
                        let id = jsonb_datum_to_role_id(val)?;
                        grantor = Some(id);
                    }
                    "acl_mode" => {
                        acl_mode = Some(jsonb_datum_to_acl_mode(val)?);
                    }
                    other => return Err(format!("unexpected privilege field: {other}")),
                }
            }
            Ok(MzAclItem {
                grantee: grantee.ok_or_else(|| format!("missing grantee: {dict:?}"))?,
                grantor: grantor.ok_or_else(|| "missing grantor in privilege".to_string())?,
                acl_mode: acl_mode.ok_or_else(|| "missing acl_mode in privilege".to_string())?,
            })
        }
        other => Err(format!("expected object in array, found: {other}")),
    };

    let parse = || match a.into_datum() {
        Datum::List(list) => {
            let mut result = Vec::new();
            for item in list.iter() {
                result.push(parse_one(item)?);
            }
            Ok(result)
        }
        _ => Err("expected array".to_string()),
    };

    parse()
        .map(ArrayRustType)
        .map_err(|e| EvalError::InvalidCatalogJson(e.into()))
}

/// Converts a catalog JSON-serialized `AclMode` bitflags object into a
/// PostgreSQL ACL char-code string (e.g. `{"bitflags": 514}` → `"ar"`).
#[sqlfunc]
fn parse_catalog_acl_mode<'a>(a: JsonbRef<'a>) -> Result<String, EvalError> {
    jsonb_datum_to_acl_mode(a.into_datum())
        .map(|mode| mode.to_string())
        .map_err(|e| EvalError::InvalidCatalogJson(e.into()))
}

/// Extracts the string form of a `WithOptionValue`, matching how the planner
/// coerces option values. The blanket `TryFromValue<WithOptionValue<T>>` impl in
/// `src/sql/src/plan/with_options.rs` accepts a quoted string, a bare identifier,
/// and a 1-part unresolved item name, all yielding a string. Any other variant
/// yields None.
///
/// `AstDisplay` does not re-quote bare identifiers or 1-part names, so those
/// forms persist unquoted in `create_sql`. Matching only `Value::String` here
/// would miss them and silently fall back to a default, so the catalog-raw
/// parsers below route their string options through this helper.
fn option_string<T: AstInfo>(value: &WithOptionValue<T>) -> Option<String> {
    match value {
        WithOptionValue::Value(Value::String(s)) => Some(s.clone()),
        WithOptionValue::Ident(ident) => Some(ident.clone().into_string()),
        WithOptionValue::UnresolvedItemName(UnresolvedItemName(parts)) if parts.len() == 1 => {
            Some(parts[0].clone().into_string())
        }
        _ => None,
    }
}

/// Parses a catalog `create_sql` string into a JSONB object.
///
/// The returned JSONB does not fully reflect the parsed SQL and instead contains only fields
/// required by current callers.
///
// TODO: This function isn't parsing JSONB and therefore shouldn't live in the `jsonb` module.
//       Consider moving all the `parse_catalog_*` functions into their own module.
#[sqlfunc]
fn parse_catalog_create_sql<'a>(a: &'a str) -> Result<Jsonb, EvalError> {
    fn get_cluster_id(in_cluster: RawClusterName) -> Result<String, &'static str> {
        match in_cluster {
            RawClusterName::Resolved(s) => Ok(s),
            RawClusterName::Unresolved(_) => Err("unresolved cluster name"),
        }
    }

    fn get_item_id(item: RawItemName) -> Result<String, &'static str> {
        match item {
            RawItemName::Id(id, _, _) => Ok(id),
            RawItemName::Name(_) => Err("unresolved item name"),
        }
    }

    fn format_name<T: AstInfo>(fmt: &Format<T>) -> &'static str {
        match fmt {
            Format::Bytes => "bytes",
            Format::Avro(_) => "avro",
            Format::Protobuf(_) => "protobuf",
            Format::Regex(_) => "regex",
            Format::Csv { .. } => "csv",
            Format::Json { .. } => "json",
            Format::Text => "text",
        }
    }

    let parse = || -> Result<serde_json::Value, String> {
        let mut stmts = mz_sql_parser::parser::parse_statements(a)
            .map_err(|e| format!("failed to parse create_sql: {e}"))?;
        let stmt = match stmts.len() {
            1 => stmts.remove(0).ast,
            n => return Err(format!("expected a single statement, found {n}")),
        };

        let mut info = BTreeMap::<&str, serde_json::Value>::new();

        use mz_sql_parser::ast::Statement::*;
        let item_type = match stmt {
            CreateSecret(_) => "secret",
            CreateConnection(stmt) => {
                let connection_type = stmt.connection_type.as_str();
                info.insert("connection_type", json!(connection_type));

                "connection"
            }
            CreateView(stmt) => {
                let mut definition = stmt.definition.query.to_ast_string_stable();
                // PostgreSQL appends a semicolon in `pg_views.definition`, we
                // do the same for compatibility's sake.
                definition.push(';');
                info.insert("definition", json!(definition));

                "view"
            }
            CreateMaterializedView(stmt) => {
                let Some(in_cluster) = stmt.in_cluster else {
                    return Err("missing IN CLUSTER".into());
                };
                let cluster_id = match in_cluster {
                    RawClusterName::Unresolved(ident) => ident.into_string(),
                    RawClusterName::Resolved(s) => s,
                };
                info.insert("cluster_id", json!(cluster_id));

                let mut definition = stmt.query.to_ast_string_stable();
                definition.push(';');
                info.insert("definition", json!(definition));

                "materialized-view"
            }
            CreateTable(_) => "table",
            CreateTableFromSource(stmt) => {
                let source_id = get_item_id(stmt.source)?;
                info.insert("source_id", json!(source_id));

                "table"
            }
            CreateSource(stmt) => {
                let Some(in_cluster) = stmt.in_cluster else {
                    return Err("missing IN CLUSTER".into());
                };
                let cluster_id = get_cluster_id(in_cluster)?;
                info.insert("cluster_id", json!(cluster_id));

                use mz_sql_parser::ast::CreateSourceConnection::*;
                let (source_type, connection) = match stmt.connection {
                    Kafka { connection, .. } => ("kafka", Some(connection)),
                    Postgres { connection, .. } => ("postgres", Some(connection)),
                    MySql { connection, .. } => ("mysql", Some(connection)),
                    SqlServer { connection, .. } => ("sql-server", Some(connection)),
                    LoadGenerator { .. } => ("load-generator", None),
                };
                info.insert("source_type", json!(source_type));
                if let Some(conn) = connection {
                    let conn_id = get_item_id(conn)?;
                    info.insert("connection_id", json!(conn_id));
                }

                let is_debezium = matches!(
                    stmt.envelope,
                    Some(mz_sql_parser::ast::SourceEnvelope::Debezium)
                );

                // An old-syntax kafka source ingests into its own relation, so an
                // omitted ENVELOPE means the default ENVELOPE NONE and the pre-MV
                // packer reported 'none'. A new-syntax source (no progress
                // subsource, hence no EXPOSE PROGRESS AS in create_sql) ingests
                // nothing itself. Its envelopes live on the per-table exports, so
                // its own envelope_type stays absent (SQL NULL), matching released
                // behavior. `progress_subsource.is_some()` is the planner's own
                // old-vs-new discriminator (see `OldSyntaxIngestion` in
                // plan_create_source). Non-kafka sources carry no envelope either.
                // See the `mz_sources.envelope_type` column.
                let envelope_type = match &stmt.envelope {
                    Some(envelope) => {
                        use mz_sql_parser::ast::SourceEnvelope::*;
                        Some(match envelope {
                            None => "none",
                            Debezium => "debezium",
                            Upsert { .. } => "upsert",
                            CdcV2 => "materialize",
                        })
                    }
                    None if source_type == "kafka" && stmt.progress_subsource.is_some() => {
                        Some("none")
                    }
                    None => None,
                };
                if let Some(envelope_type) = envelope_type {
                    info.insert("envelope_type", json!(envelope_type));
                }

                if let Some(format_spec) = stmt.format {
                    match &format_spec {
                        FormatSpecifier::Bare(fmt) => {
                            // Debezium sources with a single format spec implicitly use
                            // the same format for both key and value.
                            if is_debezium {
                                info.insert("key_format", json!(format_name(fmt)));
                            }
                            info.insert("value_format", json!(format_name(fmt)));
                        }
                        FormatSpecifier::KeyValue { key, value } => {
                            info.insert("key_format", json!(format_name(key)));
                            info.insert("value_format", json!(format_name(value)));
                        }
                    }
                }

                "source"
            }
            CreateWebhookSource(stmt) => {
                if stmt.is_table {
                    "table"
                } else {
                    info.insert("source_type", json!("webhook"));
                    if let Some(in_cluster) = stmt.in_cluster {
                        let cluster_id = get_cluster_id(in_cluster)?;
                        info.insert("cluster_id", json!(cluster_id));
                    }
                    "source"
                }
            }
            CreateSubsource(stmt) => {
                use mz_sql_parser::ast::CreateSubsourceOptionName;
                let is_progress = stmt
                    .with_options
                    .iter()
                    .any(|o| matches!(o.name, CreateSubsourceOptionName::Progress));
                let source_type = if is_progress { "progress" } else { "subsource" };
                info.insert("source_type", json!(source_type));

                if let Some(of_source) = stmt.of_source {
                    let of_source_id = get_item_id(of_source)?;
                    info.insert("of_source_id", json!(of_source_id));
                }

                "subsource"
            }
            // Everything the mz_sinks, mz_kafka_sinks and mz_iceberg_sinks
            // views read. The Rust side of each value lives in `Sink` and
            // `StorageSinkConnection`, so those and this have to move together.
            //
            // NOTE: we bail below if a sink has no resolved `IN CLUSTER`, no
            // `TOPIC` on kafka, or no `NAMESPACE`/`TABLE` on iceberg. Planning
            // guarantees all four. But if one ever slipped through it would
            // take down every view built on this function, not just the sink
            // ones.
            CreateSink(stmt) => {
                let Some(in_cluster) = stmt.in_cluster else {
                    return Err("missing IN CLUSTER".into());
                };
                info.insert("cluster_id", json!(get_cluster_id(in_cluster)?));

                match stmt.connection {
                    CreateSinkConnection::Kafka {
                        connection,
                        options,
                        key: sink_key,
                        ..
                    } => {
                        info.insert("sink_type", json!("kafka"));
                        info.insert("connection_id", json!(get_item_id(connection)?));

                        let topic = options
                            .into_iter()
                            .find(|o| o.name == KafkaSinkConfigOptionName::Topic)
                            .and_then(|o| o.value.as_ref().and_then(option_string))
                            .ok_or("kafka sink missing TOPIC")?;
                        info.insert("topic", json!(topic));

                        if let Some(envelope) = stmt.envelope {
                            let envelope_type = match envelope {
                                SinkEnvelope::Upsert => "upsert",
                                SinkEnvelope::Debezium => "debezium",
                            };
                            info.insert("envelope_type", json!(envelope_type));
                        }

                        if let Some(format_spec) = stmt.format {
                            // A key format only survives if the sink has a
                            // `KEY`. Without one `kafka_sink_builder` throws
                            // away the key half of a key/value spec, and does
                            // not copy a bare spec over to the key either.
                            let (key_format, value_format) =
                                match (&format_spec, sink_key.is_some()) {
                                    (FormatSpecifier::Bare(fmt), false) => (None, format_name(fmt)),
                                    (FormatSpecifier::Bare(fmt), true) => {
                                        (Some(format_name(fmt)), format_name(fmt))
                                    }
                                    (FormatSpecifier::KeyValue { value, .. }, false) => {
                                        (None, format_name(value))
                                    }
                                    (FormatSpecifier::KeyValue { key, value }, true) => {
                                        (Some(format_name(key)), format_name(value))
                                    }
                                };
                            if let Some(key_format) = key_format {
                                info.insert("key_format", json!(key_format));
                            }
                            info.insert("value_format", json!(value_format));

                            // The deprecated combined `format`. Only avro/avro
                            // and json/json collapse to a single name.
                            // Everything else, text/text and bytes/bytes
                            // included, gets the composite form.
                            let combined = match key_format {
                                None => value_format.to_string(),
                                Some(key_format)
                                    if key_format == value_format
                                        && matches!(value_format, "avro" | "json") =>
                                {
                                    value_format.to_string()
                                }
                                Some(key_format) => {
                                    format!("key-{key_format}-value-{value_format}")
                                }
                            };
                            info.insert("format", json!(combined));
                        }
                    }
                    CreateSinkConnection::Iceberg {
                        catalog_connection,
                        options,
                        ..
                    } => {
                        info.insert("sink_type", json!("iceberg"));
                        // The catalog connection, not the optional AWS one.
                        info.insert("connection_id", json!(get_item_id(catalog_connection)?));

                        let mut namespace = None;
                        let mut table = None;
                        for option in options {
                            match option.name {
                                IcebergSinkConfigOptionName::Namespace => {
                                    namespace = option.value.as_ref().and_then(option_string)
                                }
                                IcebergSinkConfigOptionName::Table => {
                                    table = option.value.as_ref().and_then(option_string)
                                }
                            }
                        }
                        info.insert(
                            "namespace",
                            json!(namespace.ok_or("iceberg sink missing NAMESPACE")?),
                        );
                        info.insert("table", json!(table.ok_or("iceberg sink missing TABLE")?));

                        // Iceberg spells the envelope `MODE`, and has no format
                        // columns at all.
                        if let Some(mode) = stmt.mode {
                            let envelope_type = match mode {
                                IcebergSinkMode::Upsert => "upsert",
                                IcebergSinkMode::Append => "append",
                            };
                            info.insert("envelope_type", json!(envelope_type));
                        }
                    }
                }

                "sink"
            }
            CreateMetricSink(stmt) => {
                let Some(in_cluster) = stmt.in_cluster else {
                    return Err("missing IN CLUSTER".into());
                };
                let cluster_id = get_cluster_id(in_cluster)?;
                info.insert("cluster_id", json!(cluster_id));
                let from_id = get_item_id(stmt.from)?;
                info.insert("from_id", json!(from_id));
                "metric-sink"
            }
            CreateIndex(stmt) => {
                let Some(in_cluster) = stmt.in_cluster else {
                    return Err("missing IN CLUSTER".into());
                };
                let cluster_id = get_cluster_id(in_cluster)?;
                info.insert("cluster_id", json!(cluster_id));
                let on_id = get_item_id(stmt.on_name)?;
                info.insert("on_id", json!(on_id));
                "index"
            }
            // The column lists ride along as an ordered array of pairs rather
            // than two parallel arrays, so `mz_foreign_key_columns` can unnest
            // it with ordinality and cannot mis-zip the two sides.
            CreateForeignKey(stmt) => {
                info.insert("referencing_id", json!(get_item_id(stmt.on_name)?));
                info.insert("referenced_id", json!(get_item_id(stmt.references)?));
                if stmt.columns.len() != stmt.referenced_columns.len() {
                    return Err("foreign key column lists differ in length".into());
                }
                let columns: Vec<_> = stmt
                    .columns
                    .into_iter()
                    .zip(stmt.referenced_columns)
                    .map(|(referencing, referenced)| {
                        json!({
                            "referencing": referencing.into_string(),
                            "referenced": referenced.into_string(),
                        })
                    })
                    .collect();
                info.insert("columns", json!(columns));
                "foreign-key"
            }
            CreateType(_) => "type",
            // NOTE: every statement that creates a catalog item needs an arm above. These
            // catalog views run this over every item row before their type filter drops the
            // unwanted rows, so one unclassified `create_sql` takes out `mz_objects`,
            // `mz_indexes`, and every sibling view at once. The match is exhaustive to make
            // that a compile error here, not a runtime failure.
            Select(_)
            | Insert(_)
            | Copy(_)
            | Update(_)
            | Delete(_)
            | CreateDatabase(_)
            | CreateSchema(_)
            | CreateRole(_)
            | CreateCluster(_)
            | CreateClusterReplica(_)
            | CreateNetworkPolicy(_)
            | AlterCluster(_)
            | AlterOwner(_)
            | AlterObjectRename(_)
            | AlterObjectSwap(_)
            | AlterRetainHistory(_)
            | AlterIndex(_)
            | AlterSecret(_)
            | AlterSetCluster(_)
            | AlterSink(_)
            | AlterSource(_)
            | AlterSystemSet(_)
            | AlterSystemReset(_)
            | AlterSystemResetAll(_)
            | AlterConnection(_)
            | AlterNetworkPolicy(_)
            | AlterRole(_)
            | AlterTableAddColumn(_)
            | AlterMaterializedViewApplyReplacement(_)
            | Discard(_)
            | DropObjects(_)
            | DropOwned(_)
            | SetVariable(_)
            | ResetVariable(_)
            | Show(_)
            | StartTransaction(_)
            | SetTransaction(_)
            | Commit(_)
            | Rollback(_)
            | Subscribe(_)
            | ExplainPlan(_)
            | ExplainPushdown(_)
            | ExplainTimestamp(_)
            | ExplainSinkSchema(_)
            | ExplainAnalyzeObject(_)
            | ExplainAnalyzeCluster(_)
            | Declare(_)
            | Fetch(_)
            | Close(_)
            | Prepare(_)
            | Execute(_)
            | ExecuteUnitTest(_)
            | Deallocate(_)
            | Raise(_)
            | GrantRole(_)
            | RevokeRole(_)
            | GrantPrivileges(_)
            | RevokePrivileges(_)
            | AlterDefaultPrivileges(_)
            | ReassignOwned(_)
            | ValidateConnection(_)
            | Comment(_) => return Err("not a CREATE item statement".into()),
        };
        info.insert("type", json!(item_type));

        let info = info.into_iter().map(|(k, v)| (k.to_string(), v)).collect();
        Ok(info)
    };

    let val = parse().map_err(|e| EvalError::InvalidCatalogJson(e.into()))?;
    let jsonb = Jsonb::from_serde_json(val).expect("valid JSONB");
    Ok(jsonb)
}

/// Minimal decoder for `ProtoPostgresSourcePublicationDetails`. The
/// canonical proto lives in `mz-storage-types`, which depends on
/// `mz-expr`, so we redeclare the two tags we read here. Upstream tag
/// renumbers slip past silently. The `mz_postgres_sources` lockdown
/// SLTs catch them.
#[derive(Clone, PartialEq, ::prost::Message)]
struct PostgresPublicationDetailsSubset {
    #[prost(string, tag = "2")]
    slot: String,
    #[prost(uint64, optional, tag = "3")]
    timeline_id: Option<u64>,
}

/// Extracts postgres source publication details (slot, timeline_id) from a
/// catalog `create_sql`. Returns:
///
/// - jsonb `{"slot": <text>, "timeline_id": <u64 | null>}` for
///   `CREATE SOURCE ... FROM POSTGRES CONNECTION ... (DETAILS = ...)` statements.
/// - jsonb `null` for any other statement.
///
/// Errors if the statement fails to parse, is a postgres source without
/// a `DETAILS` option, or if the `DETAILS` value can't be hex- and
/// proto-decoded.
#[sqlfunc]
fn parse_postgres_source_details<'a>(a: &'a str) -> Result<Jsonb, EvalError> {
    let parse = || -> Result<serde_json::Value, String> {
        let mut stmts = mz_sql_parser::parser::parse_statements(a)
            .map_err(|e| format!("failed to parse create_sql: {e}"))?;
        let stmt = match stmts.len() {
            1 => stmts.remove(0).ast,
            n => return Err(format!("expected a single statement, found {n}")),
        };

        use mz_sql_parser::ast::CreateSourceConnection;
        use mz_sql_parser::ast::Statement::CreateSource;
        let options = match stmt {
            CreateSource(stmt) => match stmt.connection {
                CreateSourceConnection::Postgres { options, .. } => options,
                _ => return Ok(serde_json::Value::Null),
            },
            _ => return Ok(serde_json::Value::Null),
        };

        let details_hex = options
            .into_iter()
            .find(|opt| opt.name == PgConfigOptionName::Details)
            .and_then(|opt| match opt.value {
                Some(WithOptionValue::Value(Value::String(s))) => Some(s),
                _ => None,
            })
            .ok_or("missing DETAILS option on postgres source")?;

        let details_bytes =
            hex::decode(&details_hex).map_err(|e| format!("DETAILS is not valid hex: {e}"))?;

        let details = PostgresPublicationDetailsSubset::decode(&*details_bytes)
            .map_err(|e| format!("DETAILS is not a valid publication-details proto: {e}"))?;

        Ok(json!({
            "slot": details.slot,
            "timeline_id": details.timeline_id,
        }))
    };

    let val = parse().map_err(|e| EvalError::InvalidCatalogJson(e.into()))?;
    let jsonb = Jsonb::from_serde_json(val).expect("valid JSONB");
    Ok(jsonb)
}

/// Extracts kafka source configuration (topic, group id prefix, connection
/// id) from a catalog `create_sql`. Returns:
///
/// - jsonb `{"topic": <text>, "group_id_prefix": <text | null>, "connection_id": <text>}`
///   for `CREATE SOURCE ... FROM KAFKA CONNECTION ... (TOPIC = ..., [GROUP ID PREFIX = ...])`
///   statements.
/// - jsonb `null` for any other statement.
///
/// Errors if the statement fails to parse, is a kafka source without a
/// `TOPIC` option, or references an unresolved connection name (i.e. one
/// that hasn't been through purification).
#[sqlfunc]
fn parse_kafka_source_details<'a>(a: &'a str) -> Result<Jsonb, EvalError> {
    fn get_item_id(item: RawItemName) -> Result<String, &'static str> {
        match item {
            RawItemName::Id(id, _, _) => Ok(id),
            RawItemName::Name(_) => Err("unresolved item name"),
        }
    }

    let parse = || -> Result<serde_json::Value, String> {
        let mut stmts = mz_sql_parser::parser::parse_statements(a)
            .map_err(|e| format!("failed to parse create_sql: {e}"))?;
        let stmt = match stmts.len() {
            1 => stmts.remove(0).ast,
            n => return Err(format!("expected a single statement, found {n}")),
        };

        use mz_sql_parser::ast::CreateSourceConnection;
        use mz_sql_parser::ast::Statement::CreateSource;
        let (connection, options) = match stmt {
            CreateSource(stmt) => match stmt.connection {
                CreateSourceConnection::Kafka {
                    connection,
                    options,
                } => (connection, options),
                _ => return Ok(serde_json::Value::Null),
            },
            _ => return Ok(serde_json::Value::Null),
        };

        let connection_id = get_item_id(connection)?;

        let mut topic: Option<String> = None;
        let mut group_id_prefix: Option<String> = None;
        for opt in options {
            let string_value = opt.value.as_ref().and_then(option_string);
            match opt.name {
                KafkaSourceConfigOptionName::Topic => topic = string_value,
                KafkaSourceConfigOptionName::GroupIdPrefix => group_id_prefix = string_value,
                _ => {}
            }
        }

        let topic = topic.ok_or("missing TOPIC option on kafka source")?;

        Ok(json!({
            "topic": topic,
            "group_id_prefix": group_id_prefix,
            "connection_id": connection_id,
        }))
    };

    let val = parse().map_err(|e| EvalError::InvalidCatalogJson(e.into()))?;
    let jsonb = Jsonb::from_serde_json(val).expect("valid JSONB");
    Ok(jsonb)
}

/// Extracts source-export (source table) metadata from a catalog `create_sql`.
///
/// Returns, for a `CREATE TABLE ... FROM SOURCE` or a non-progress
/// `CREATE SUBSOURCE ... OF SOURCE ...` statement:
///
/// ```json
/// {
///   "source_id": "<parent source item id>",
///   "external_reference": ["part1", "part2", ...],
///   "envelope_type": <text | null>,
///   "key_format": <text | null>,
///   "value_format": <text | null>
/// }
/// ```
///
/// `envelope_type`, `key_format`, and `value_format` are always null for a
/// `CREATE SUBSOURCE` (the postgres/mysql/sql-server exports that use the old
/// subsource syntax carry neither format nor envelope). They may also be null
/// for a `CREATE TABLE ... FROM SOURCE` that omits FORMAT/ENVELOPE.
///
/// Returns jsonb `null` for progress subsources and for any statement that is
/// not a source export. The caller distinguishes the four source-table views
/// by joining `source_id` against `mz_sources` and filtering on the parent's
/// type, so this helper stays connection-type agnostic.
///
/// Errors if the statement fails to parse, references an unresolved item name,
/// or is a non-progress subsource missing its OF SOURCE or EXTERNAL REFERENCE.
///
/// The `key_format`/`value_format` derivation mirrors the runtime
/// `DataSourceDesc::formats()` that the removed `pack_kafka_source_tables_update`
/// packer read. A bare FORMAT only carries a key when it resolves to an
/// encoding that has one, which among bare formats is only Avro or Protobuf
/// read from a Confluent Schema Registry whose purified seed carries a key
/// schema. A KEY FORMAT ... VALUE FORMAT ... spec always carries both.
#[sqlfunc]
fn parse_source_export_details<'a>(a: &'a str) -> Result<Jsonb, EvalError> {
    fn get_item_id(item: RawItemName) -> Result<String, &'static str> {
        match item {
            RawItemName::Id(id, _, _) => Ok(id),
            RawItemName::Name(_) => Err("unresolved item name"),
        }
    }

    fn format_name<T: AstInfo>(fmt: &Format<T>) -> &'static str {
        match fmt {
            Format::Bytes => "bytes",
            Format::Avro(_) => "avro",
            Format::Protobuf(_) => "protobuf",
            Format::Regex(_) => "regex",
            Format::Csv { .. } => "csv",
            Format::Json { .. } => "json",
            Format::Text => "text",
        }
    }

    // A bare FORMAT resolves to an encoding with a key only for Avro or
    // Protobuf read from a schema registry whose purified seed carries a key
    // schema. Every other bare format is value-only.
    fn bare_format_has_key<T: AstInfo>(fmt: &Format<T>) -> bool {
        match fmt {
            Format::Avro(AvroSchema::Csr { csr_connection }) => csr_connection
                .seed
                .as_ref()
                .is_some_and(|seed| seed.key_schema.is_some()),
            Format::Protobuf(ProtobufSchema::Csr { csr_connection }) => csr_connection
                .seed
                .as_ref()
                .is_some_and(|seed| seed.key.is_some()),
            _ => false,
        }
    }

    fn key_value_formats<T: AstInfo>(
        spec: &FormatSpecifier<T>,
    ) -> (Option<&'static str>, Option<&'static str>) {
        match spec {
            FormatSpecifier::KeyValue { key, value } => {
                (Some(format_name(key)), Some(format_name(value)))
            }
            FormatSpecifier::Bare(fmt) => {
                let value = Some(format_name(fmt));
                let key = bare_format_has_key(fmt).then(|| format_name(fmt));
                (key, value)
            }
        }
    }

    fn envelope_name(envelope: &SourceEnvelope) -> &'static str {
        match envelope {
            SourceEnvelope::None => "none",
            SourceEnvelope::Debezium => "debezium",
            SourceEnvelope::Upsert {
                value_decode_err_policy,
            } => {
                if value_decode_err_policy
                    .iter()
                    .any(|p| matches!(p, SourceErrorPolicy::Inline { .. }))
                {
                    "upsert-value-err-inline"
                } else {
                    "upsert"
                }
            }
            SourceEnvelope::CdcV2 => "materialize",
        }
    }

    let parse = || -> Result<serde_json::Value, String> {
        let mut stmts = mz_sql_parser::parser::parse_statements(a)
            .map_err(|e| format!("failed to parse create_sql: {e}"))?;
        let stmt = match stmts.len() {
            1 => stmts.remove(0).ast,
            n => return Err(format!("expected a single statement, found {n}")),
        };

        use mz_sql_parser::ast::Statement::{CreateSubsource, CreateTableFromSource};
        match stmt {
            CreateTableFromSource(stmt) => {
                let source_id = get_item_id(stmt.source)?;
                let external_reference = stmt
                    .external_reference
                    .ok_or("missing external reference on CREATE TABLE FROM SOURCE")?
                    .0
                    .into_iter()
                    .map(|ident| ident.into_string())
                    .collect::<Vec<_>>();

                let envelope_type = stmt.envelope.as_ref().map(envelope_name);
                let (key_format, value_format) = match &stmt.format {
                    Some(spec) => key_value_formats(spec),
                    None => (None, None),
                };

                Ok(json!({
                    "source_id": source_id,
                    "external_reference": external_reference,
                    "envelope_type": envelope_type,
                    "key_format": key_format,
                    "value_format": value_format,
                }))
            }
            CreateSubsource(stmt) => {
                // Progress subsources track ingestion progress and are not
                // source tables. They have no external reference.
                let is_progress = stmt
                    .with_options
                    .iter()
                    .any(|o| matches!(o.name, CreateSubsourceOptionName::Progress));
                if is_progress {
                    return Ok(serde_json::Value::Null);
                }

                let source_id = stmt
                    .of_source
                    .ok_or("non-progress CREATE SUBSOURCE without OF SOURCE")
                    .and_then(get_item_id)?;

                let external_reference = stmt
                    .with_options
                    .into_iter()
                    .find(|o| matches!(o.name, CreateSubsourceOptionName::ExternalReference))
                    .and_then(|o| match o.value {
                        Some(WithOptionValue::UnresolvedItemName(name)) => Some(name),
                        _ => None,
                    })
                    .ok_or("CREATE SUBSOURCE missing EXTERNAL REFERENCE option")?
                    .0
                    .into_iter()
                    .map(|ident| ident.into_string())
                    .collect::<Vec<_>>();

                Ok(json!({
                    "source_id": source_id,
                    "external_reference": external_reference,
                    "envelope_type": serde_json::Value::Null,
                    "key_format": serde_json::Value::Null,
                    "value_format": serde_json::Value::Null,
                }))
            }
            _ => Ok(serde_json::Value::Null),
        }
    };

    let val = parse().map_err(|e| EvalError::InvalidCatalogJson(e.into()))?;
    let jsonb = Jsonb::from_serde_json(val).expect("valid JSONB");
    Ok(jsonb)
}

/// Extracts connection-detail metadata from a catalog `create_sql`.
///
/// Returns a per-connection-type object with the fields that the
/// `mz_kafka_connections`, `mz_ssh_tunnel_connections`, and `mz_aws_connections`
/// builtin views need. For everything else (other connection types, including
/// aws-privatelink whose only detail is context-derived, and non-connection
/// statements) it returns jsonb `null`, so callers filter on `IS NOT NULL` and
/// gate on the connection type separately (via
/// `parse_catalog_create_sql(...)->>'connection_type'`, the way `mz_connections`
/// already does).
///
/// The shape per type:
///
/// ```json
/// // kafka
/// { "brokers": ["host:port", ...], "progress_topic": <text | null> }
/// // ssh-tunnel
/// { "public_key_1": "<text>", "public_key_2": "<text>" }
/// // aws
/// {
///   "auth_kind": "credentials" | "assume-role",
///   "endpoint": <text | null>, "region": <text | null>,
///   "access_key_id": <text | null>, "access_key_id_secret_id": <text | null>,
///   "secret_access_key_secret_id": <text | null>,
///   "session_token": <text | null>, "session_token_secret_id": <text | null>,
///   "assume_role_arn": <text | null>, "assume_role_session_name": <text | null>
/// }
/// ```
///
/// `progress_topic` is null when the connection does not set an explicit
/// `PROGRESS TOPIC`. The default (`_materialize-progress-<env>-<conn_id>`) is
/// reconstructed by the view, not here, because it needs the environment id and
/// the connection's own id. Values derived only from environment context
/// (AWS principal, external id, trust policy, privatelink principal) are also
/// left to the view. This keeps the helper a pure function of the `create_sql`.
///
/// For aws, an option is either an inline value or a secret reference. Inline
/// values land in `access_key_id`/`session_token`; a secret reference lands in
/// the matching `*_secret_id` as the referenced secret's catalog item id (the
/// persisted `create_sql` stores resolved references as `[uNNN AS name]`).
/// `auth_kind` is `assume-role` when `ASSUME ROLE ARN` is present, else
/// `credentials`, matching the `AwsAuth` variant the removed packer read.
///
/// Errors if the statement fails to parse.
#[sqlfunc]
fn parse_connection_details<'a>(a: &'a str) -> Result<Jsonb, EvalError> {
    // The persisted `create_sql` stores an inline broker as a single `BROKER`
    // option and a broker list as a `BROKERS (...)` sequence. Either way we
    // only need the addresses, which are present regardless of the tunnel
    // (direct, SSH, or PrivateLink).
    fn broker_addresses<T: AstInfo>(values: &[ConnectionOption<T>]) -> Vec<String> {
        let mut brokers = Vec::new();
        for opt in values {
            match (&opt.name, &opt.value) {
                (ConnectionOptionName::Broker, Some(WithOptionValue::ConnectionKafkaBroker(b))) => {
                    brokers.push(b.address.clone());
                }
                (ConnectionOptionName::Brokers, Some(WithOptionValue::Sequence(seq))) => {
                    for v in seq {
                        if let WithOptionValue::ConnectionKafkaBroker(b) = v {
                            brokers.push(b.address.clone());
                        }
                    }
                }
                _ => {}
            }
        }
        brokers
    }

    fn string_option<T: AstInfo>(
        values: &[ConnectionOption<T>],
        name: ConnectionOptionName,
    ) -> Option<String> {
        values
            .iter()
            .find(|o| o.name == name)
            .and_then(|o| o.value.as_ref())
            .and_then(option_string)
    }

    // The catalog id of the secret a `SECRET ...` option references. Resolved
    // references persist as `RawItemName::Id`, so an unresolved name yields
    // None (the same treatment `parse_source_export_details` gives item names).
    fn secret_id_option<T: AstInfo<ItemName = RawItemName>>(
        values: &[ConnectionOption<T>],
        name: ConnectionOptionName,
    ) -> Option<String> {
        values.iter().find_map(|o| match &o.value {
            Some(WithOptionValue::Secret(RawItemName::Id(id, _, _))) if o.name == name => {
                Some(id.clone())
            }
            _ => None,
        })
    }

    let parse = || -> Result<serde_json::Value, String> {
        let mut stmts = mz_sql_parser::parser::parse_statements(a)
            .map_err(|e| format!("failed to parse create_sql: {e}"))?;
        let stmt = match stmts.len() {
            1 => stmts.remove(0).ast,
            n => return Err(format!("expected a single statement, found {n}")),
        };

        use mz_sql_parser::ast::Statement::CreateConnection;
        let stmt = match stmt {
            CreateConnection(stmt) => stmt,
            _ => return Ok(serde_json::Value::Null),
        };

        match stmt.connection_type {
            CreateConnectionType::Kafka => Ok(json!({
                "brokers": broker_addresses(&stmt.values),
                "progress_topic": string_option(&stmt.values, ConnectionOptionName::ProgressTopic),
            })),
            CreateConnectionType::Ssh => Ok(json!({
                "public_key_1": string_option(&stmt.values, ConnectionOptionName::PublicKey1),
                "public_key_2": string_option(&stmt.values, ConnectionOptionName::PublicKey2),
            })),
            CreateConnectionType::Aws => {
                let assume_role_arn =
                    string_option(&stmt.values, ConnectionOptionName::AssumeRoleArn);
                let auth_kind = if assume_role_arn.is_some() {
                    "assume-role"
                } else {
                    "credentials"
                };
                Ok(json!({
                    "auth_kind": auth_kind,
                    // Planning coerces an empty ENDPOINT to None (see
                    // `src/sql/src/plan/statement/ddl/connection.rs`), so the
                    // removed packer wrote NULL for `ENDPOINT = ''`. Match that.
                    "endpoint": string_option(&stmt.values, ConnectionOptionName::Endpoint)
                        .filter(|s| !s.is_empty()),
                    "region": string_option(&stmt.values, ConnectionOptionName::Region),
                    "access_key_id": string_option(&stmt.values, ConnectionOptionName::AccessKeyId),
                    "access_key_id_secret_id":
                        secret_id_option(&stmt.values, ConnectionOptionName::AccessKeyId),
                    "secret_access_key_secret_id":
                        secret_id_option(&stmt.values, ConnectionOptionName::SecretAccessKey),
                    "session_token": string_option(&stmt.values, ConnectionOptionName::SessionToken),
                    "session_token_secret_id":
                        secret_id_option(&stmt.values, ConnectionOptionName::SessionToken),
                    "assume_role_arn": assume_role_arn,
                    "assume_role_session_name":
                        string_option(&stmt.values, ConnectionOptionName::AssumeRoleSessionName),
                }))
            }
            _ => Ok(serde_json::Value::Null),
        }
    };

    let val = parse().map_err(|e| EvalError::InvalidCatalogJson(e.into()))?;
    let jsonb = Jsonb::from_serde_json(val).expect("valid JSONB");
    Ok(jsonb)
}

#[cfg(test)]
mod tests {
    use mz_repr::adt::jsonb::Jsonb;
    use prost::Message as _;
    use serde_json::json;

    use crate::EvalError;

    /// Encode the two proto fields our decoder cares about, using the same
    /// tag numbering as the canonical proto.
    fn encode_pg_details(slot: &str, timeline_id: Option<u64>) -> String {
        let details = super::PostgresPublicationDetailsSubset {
            slot: slot.to_string(),
            timeline_id,
        };
        hex::encode(details.encode_to_vec())
    }

    fn pg_source_sql(details_hex: &str) -> String {
        format!(
            "CREATE SOURCE \"materialize\".\"public\".\"pg_src\" \
             IN CLUSTER [u42] \
             FROM POSTGRES CONNECTION [u10 AS \"materialize\".\"public\".\"pg_conn\"] \
             (DETAILS = '{details_hex}', PUBLICATION = 'mz_source') \
             FOR ALL TABLES"
        )
    }

    fn kafka_source_sql(with_prefix: bool) -> String {
        let prefix_opt = if with_prefix {
            ", GROUP ID PREFIX 'my-prefix-'"
        } else {
            ""
        };
        format!(
            "CREATE SOURCE \"materialize\".\"public\".\"k_src\" \
             IN CLUSTER [u42] \
             FROM KAFKA CONNECTION [u11 AS \"materialize\".\"public\".\"k_conn\"] \
             (TOPIC 'test'{prefix_opt}) FORMAT TEXT"
        )
    }

    fn as_serde(jsonb: Jsonb) -> serde_json::Value {
        jsonb.as_ref().to_serde_json()
    }

    // --- parse_postgres_source_details ---------------------------------------

    #[mz_ore::test]
    #[cfg_attr(miri, ignore)] // error: unsupported operation: can't call foreign function `decContextDefault` on OS `linux`
    fn pg_happy_path_with_timeline() {
        let hex = encode_pg_details("materialize_abc", Some(42));
        let sql = pg_source_sql(&hex);
        let out = super::parse_postgres_source_details(&sql).expect("ok");
        assert_eq!(
            as_serde(out),
            json!({ "slot": "materialize_abc", "timeline_id": 42 }),
        );
    }

    #[mz_ore::test]
    fn pg_happy_path_null_timeline() {
        // Pre-2024 sources have no timeline_id field. The decoder must
        // surface that as JSON null, not error.
        let hex = encode_pg_details("materialize_legacy", None);
        let sql = pg_source_sql(&hex);
        let out = super::parse_postgres_source_details(&sql).expect("ok");
        assert_eq!(
            as_serde(out),
            json!({ "slot": "materialize_legacy", "timeline_id": null }),
        );
    }

    #[mz_ore::test]
    fn pg_non_postgres_source_returns_null_jsonb() {
        let sql = "CREATE SOURCE \"materialize\".\"public\".\"lg\" \
             IN CLUSTER [u42] FROM LOAD GENERATOR COUNTER";
        let out = super::parse_postgres_source_details(sql).expect("ok");
        assert_eq!(as_serde(out), serde_json::Value::Null);
    }

    #[mz_ore::test]
    #[cfg_attr(miri, ignore)] // error: unsupported operation: can't call foreign function `rust_psm_stack_pointer` on OS `linux`
    fn pg_non_create_source_returns_null_jsonb() {
        let sql = "CREATE VIEW v AS SELECT 1";
        let out = super::parse_postgres_source_details(sql).expect("ok");
        assert_eq!(as_serde(out), serde_json::Value::Null);
    }

    #[mz_ore::test]
    fn pg_missing_details_option_errors() {
        let sql = "CREATE SOURCE \"materialize\".\"public\".\"pg_src\" \
             IN CLUSTER [u42] \
             FROM POSTGRES CONNECTION [u10 AS \"materialize\".\"public\".\"pg_conn\"] \
             (PUBLICATION = 'mz_source') FOR ALL TABLES";
        let err = super::parse_postgres_source_details(sql).unwrap_err();
        assert!(
            matches!(err, EvalError::InvalidCatalogJson(msg) if msg.contains("missing DETAILS")),
            "wrong error variant/message"
        );
    }

    #[mz_ore::test]
    fn pg_malformed_hex_errors() {
        let sql = pg_source_sql("not-hex!!");
        let err = super::parse_postgres_source_details(&sql).unwrap_err();
        assert!(
            matches!(err, EvalError::InvalidCatalogJson(msg) if msg.contains("valid hex")),
            "wrong error variant/message"
        );
    }

    #[mz_ore::test]
    fn pg_malformed_proto_errors() {
        // Valid hex, garbage bytes. Prost decoding fails on unexpected wire
        // format.
        let sql = pg_source_sql("ffff");
        let err = super::parse_postgres_source_details(&sql).unwrap_err();
        assert!(
            matches!(err, EvalError::InvalidCatalogJson(msg) if msg.contains("publication-details proto")),
            "wrong error variant/message"
        );
    }

    // --- parse_kafka_source_details ------------------------------------------

    #[mz_ore::test]
    fn kafka_happy_path_with_prefix() {
        let sql = kafka_source_sql(true);
        let out = super::parse_kafka_source_details(&sql).expect("ok");
        assert_eq!(
            as_serde(out),
            json!({
                "topic": "test",
                "group_id_prefix": "my-prefix-",
                "connection_id": "u11",
            }),
        );
    }

    #[mz_ore::test]
    fn kafka_happy_path_without_prefix() {
        let sql = kafka_source_sql(false);
        let out = super::parse_kafka_source_details(&sql).expect("ok");
        assert_eq!(
            as_serde(out),
            json!({
                "topic": "test",
                "group_id_prefix": null,
                "connection_id": "u11",
            }),
        );
    }

    #[mz_ore::test]
    fn kafka_non_kafka_source_returns_null_jsonb() {
        let sql = "CREATE SOURCE \"materialize\".\"public\".\"lg\" \
             IN CLUSTER [u42] FROM LOAD GENERATOR COUNTER";
        let out = super::parse_kafka_source_details(sql).expect("ok");
        assert_eq!(as_serde(out), serde_json::Value::Null);
    }

    #[mz_ore::test]
    fn kafka_missing_topic_errors() {
        let sql = "CREATE SOURCE \"materialize\".\"public\".\"k_src\" \
             IN CLUSTER [u42] \
             FROM KAFKA CONNECTION [u11 AS \"materialize\".\"public\".\"k_conn\"] \
             FORMAT TEXT";
        let err = super::parse_kafka_source_details(sql).unwrap_err();
        assert!(
            matches!(err, EvalError::InvalidCatalogJson(msg) if msg.contains("missing TOPIC")),
            "wrong error variant/message"
        );
    }

    #[mz_ore::test]
    fn kafka_unquoted_topic_and_prefix() {
        // Planning accepts a bare identifier for TOPIC / GROUP ID PREFIX, and it
        // persists unquoted in create_sql. Matching only quoted strings would
        // drop TOPIC and error the whole mz_kafka_source_tables view.
        let sql = "CREATE SOURCE \"materialize\".\"public\".\"k_src\" \
             IN CLUSTER [u42] \
             FROM KAFKA CONNECTION [u11 AS \"materialize\".\"public\".\"k_conn\"] \
             (TOPIC = my_topic, GROUP ID PREFIX = my_prefix) FORMAT TEXT";
        let out = super::parse_kafka_source_details(sql).expect("ok");
        let out = as_serde(out);
        assert_eq!(out["topic"], json!("my_topic"));
        assert_eq!(out["group_id_prefix"], json!("my_prefix"));
    }

    #[mz_ore::test]
    fn kafka_unresolved_connection_errors() {
        // A bare-name connection reference never happens after purification,
        // but the decoder must reject it explicitly rather than silently
        // dropping the connection_id.
        let sql = "CREATE SOURCE \"materialize\".\"public\".\"k_src\" \
             IN CLUSTER [u42] \
             FROM KAFKA CONNECTION k_conn (TOPIC 'test') FORMAT TEXT";
        let err = super::parse_kafka_source_details(sql).unwrap_err();
        assert!(
            matches!(err, EvalError::InvalidCatalogJson(msg) if msg.contains("unresolved item name")),
            "wrong error variant/message"
        );
    }

    // --- parse_source_export_details -----------------------------------------

    fn table_from_source_sql(reference: &str, suffix: &str) -> String {
        format!(
            "CREATE TABLE \"materialize\".\"public\".\"tbl\" \
             FROM SOURCE [u1 AS \"materialize\".\"public\".\"src\"] \
             (REFERENCE = {reference}){suffix}"
        )
    }

    #[mz_ore::test]
    fn export_table_postgres_style_no_format() {
        // Postgres/mysql/sql-server tables carry a multi-part external
        // reference and no format or envelope.
        let sql = table_from_source_sql("\"db\".\"public\".\"t\"", "");
        let out = super::parse_source_export_details(&sql).expect("ok");
        assert_eq!(
            as_serde(out),
            json!({
                "source_id": "u1",
                "external_reference": ["db", "public", "t"],
                "envelope_type": null,
                "key_format": null,
                "value_format": null,
            }),
        );
    }

    #[mz_ore::test]
    fn export_table_kafka_bare_value_only() {
        // A bare non-registry FORMAT is value-only: no key format.
        let sql = table_from_source_sql("\"topic\"", " FORMAT TEXT ENVELOPE NONE");
        let out = super::parse_source_export_details(&sql).expect("ok");
        assert_eq!(
            as_serde(out),
            json!({
                "source_id": "u1",
                "external_reference": ["topic"],
                "envelope_type": "none",
                "key_format": null,
                "value_format": "text",
            }),
        );
    }

    #[mz_ore::test]
    fn export_table_kafka_omitted_envelope_is_null() {
        // Omitting ENVELOPE persists as absent in create_sql, so this
        // source-type-agnostic helper reports null. The mz_kafka_source_tables
        // view is responsible for defaulting kafka's null envelope to 'none'.
        let sql = table_from_source_sql("\"topic\"", " FORMAT TEXT");
        let out = super::parse_source_export_details(&sql).expect("ok");
        assert_eq!(as_serde(out)["envelope_type"], serde_json::Value::Null);
    }

    #[mz_ore::test]
    fn export_table_kafka_key_value_format() {
        let sql = table_from_source_sql(
            "\"topic\"",
            " KEY FORMAT TEXT VALUE FORMAT TEXT ENVELOPE NONE",
        );
        let out = super::parse_source_export_details(&sql).expect("ok");
        assert_eq!(
            as_serde(out),
            json!({
                "source_id": "u1",
                "external_reference": ["topic"],
                "envelope_type": "none",
                "key_format": "text",
                "value_format": "text",
            }),
        );
    }

    #[mz_ore::test]
    #[cfg_attr(miri, ignore)] // error: unsupported operation: can't call foreign function `rust_psm_stack_pointer` on OS `linux`
    fn export_table_kafka_bare_avro_seed_with_key() {
        // A bare Avro CSR format whose seed carries a key schema resolves to
        // an encoding with a key, so key_format mirrors value_format. This is
        // the upsert/debezium path.
        let sql = table_from_source_sql(
            "\"topic\"",
            " FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY \
             CONNECTION [u5 AS \"materialize\".\"public\".\"csr\"] \
             SEED KEY SCHEMA 'k' VALUE SCHEMA 'v' ENVELOPE UPSERT",
        );
        let out = super::parse_source_export_details(&sql).expect("ok");
        assert_eq!(
            as_serde(out),
            json!({
                "source_id": "u1",
                "external_reference": ["topic"],
                "envelope_type": "upsert",
                "key_format": "avro",
                "value_format": "avro",
            }),
        );
    }

    #[mz_ore::test]
    #[cfg_attr(miri, ignore)] // error: unsupported operation: can't call foreign function `rust_psm_stack_pointer` on OS `linux`
    fn export_table_kafka_bare_avro_seed_without_key() {
        // A bare Avro CSR seed with only a value schema is value-only.
        let sql = table_from_source_sql(
            "\"topic\"",
            " FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY \
             CONNECTION [u5 AS \"materialize\".\"public\".\"csr\"] \
             SEED VALUE SCHEMA 'v' ENVELOPE NONE",
        );
        let out = super::parse_source_export_details(&sql).expect("ok");
        assert_eq!(
            as_serde(out),
            json!({
                "source_id": "u1",
                "external_reference": ["topic"],
                "envelope_type": "none",
                "key_format": null,
                "value_format": "avro",
            }),
        );
    }

    #[mz_ore::test]
    fn export_subsource_non_progress() {
        // Old-syntax subsource: external reference lives in a WITH option, and
        // there is never a format or envelope.
        let sql = "CREATE SUBSOURCE \"materialize\".\"public\".\"sub\" (id int4) \
             OF SOURCE [u1 AS \"materialize\".\"public\".\"src\"] \
             WITH (EXTERNAL REFERENCE = \"db\".\"public\".\"t\")";
        let out = super::parse_source_export_details(sql).expect("ok");
        assert_eq!(
            as_serde(out),
            json!({
                "source_id": "u1",
                "external_reference": ["db", "public", "t"],
                "envelope_type": null,
                "key_format": null,
                "value_format": null,
            }),
        );
    }

    #[mz_ore::test]
    fn export_progress_subsource_returns_null_jsonb() {
        let sql = "CREATE SUBSOURCE \"materialize\".\"public\".\"progress\" (id int4) \
             WITH (PROGRESS)";
        let out = super::parse_source_export_details(sql).expect("ok");
        assert_eq!(as_serde(out), serde_json::Value::Null);
    }

    #[mz_ore::test]
    #[cfg_attr(miri, ignore)] // error: unsupported operation: can't call foreign function `rust_psm_stack_pointer` on OS `linux`
    fn export_non_source_export_returns_null_jsonb() {
        let sql = "CREATE VIEW v AS SELECT 1";
        let out = super::parse_source_export_details(sql).expect("ok");
        assert_eq!(as_serde(out), serde_json::Value::Null);
    }

    #[mz_ore::test]
    fn export_unresolved_source_name_errors() {
        let sql = "CREATE TABLE \"materialize\".\"public\".\"tbl\" \
             FROM SOURCE src (REFERENCE = \"topic\")";
        let err = super::parse_source_export_details(sql).unwrap_err();
        assert!(
            matches!(err, EvalError::InvalidCatalogJson(msg) if msg.contains("unresolved item name")),
            "wrong error variant/message"
        );
    }

    // --- parse_connection_details --------------------------------------------

    #[mz_ore::test]
    fn connection_kafka_single_broker_default_progress() {
        // No explicit PROGRESS TOPIC: the helper leaves it null and the view
        // reconstructs the default.
        let sql = "CREATE CONNECTION \"materialize\".\"public\".\"c\" TO KAFKA \
             (BROKER = 'localhost:9092', SECURITY PROTOCOL = plaintext)";
        let out = super::parse_connection_details(sql).expect("ok");
        assert_eq!(
            as_serde(out),
            json!({
                "brokers": ["localhost:9092"],
                "progress_topic": null,
            }),
        );
    }

    #[mz_ore::test]
    fn connection_kafka_explicit_progress_topic() {
        let sql = "CREATE CONNECTION \"materialize\".\"public\".\"c\" TO KAFKA \
             (BROKER = 'localhost:9092', PROGRESS TOPIC = 'override', \
              SECURITY PROTOCOL = plaintext)";
        let out = super::parse_connection_details(sql).expect("ok");
        assert_eq!(
            as_serde(out),
            json!({
                "brokers": ["localhost:9092"],
                "progress_topic": "override",
            }),
        );
    }

    #[mz_ore::test]
    fn connection_kafka_broker_list() {
        let sql = "CREATE CONNECTION \"materialize\".\"public\".\"c\" TO KAFKA \
             (BROKERS ('b1:9092', 'b2:9092'), SECURITY PROTOCOL = plaintext)";
        let out = super::parse_connection_details(sql).expect("ok");
        assert_eq!(
            as_serde(out),
            json!({
                "brokers": ["b1:9092", "b2:9092"],
                "progress_topic": null,
            }),
        );
    }

    #[mz_ore::test]
    fn connection_ssh_public_keys() {
        let sql = "CREATE CONNECTION \"materialize\".\"public\".\"c\" TO SSH TUNNEL \
             (HOST = 'ssh.example.com', PORT = 22, USER = 'mz', \
              PUBLIC KEY 1 = 'ssh-ed25519 AAAA', PUBLIC KEY 2 = 'ssh-ed25519 BBBB')";
        let out = super::parse_connection_details(sql).expect("ok");
        assert_eq!(
            as_serde(out),
            json!({
                "public_key_1": "ssh-ed25519 AAAA",
                "public_key_2": "ssh-ed25519 BBBB",
            }),
        );
    }

    #[mz_ore::test]
    fn connection_aws_credentials_inline_key() {
        // Inline ACCESS KEY ID, secret SECRET ACCESS KEY. Assume-role columns
        // stay null and auth_kind is credentials.
        let sql = "CREATE CONNECTION \"materialize\".\"public\".\"c\" TO AWS \
             (ACCESS KEY ID = 'AKIAEXAMPLE', \
              SECRET ACCESS KEY = SECRET [u1 AS \"materialize\".\"public\".\"sk\"])";
        let out = super::parse_connection_details(sql).expect("ok");
        assert_eq!(
            as_serde(out),
            json!({
                "auth_kind": "credentials",
                "endpoint": null,
                "region": null,
                "access_key_id": "AKIAEXAMPLE",
                "access_key_id_secret_id": null,
                "secret_access_key_secret_id": "u1",
                "session_token": null,
                "session_token_secret_id": null,
                "assume_role_arn": null,
                "assume_role_session_name": null,
            }),
        );
    }

    #[mz_ore::test]
    fn connection_aws_credentials_secret_key_and_session_token() {
        // Every credential provided as a secret reference lands in the matching
        // *_secret_id column as the referenced secret's catalog id.
        let sql = "CREATE CONNECTION \"materialize\".\"public\".\"c\" TO AWS \
             (ENDPOINT = 'http://localhost', REGION = 'us-east-1', \
              ACCESS KEY ID = SECRET [u1 AS \"materialize\".\"public\".\"ak\"], \
              SECRET ACCESS KEY = SECRET [u2 AS \"materialize\".\"public\".\"sk\"], \
              SESSION TOKEN = SECRET [u3 AS \"materialize\".\"public\".\"st\"])";
        let out = super::parse_connection_details(sql).expect("ok");
        assert_eq!(
            as_serde(out),
            json!({
                "auth_kind": "credentials",
                "endpoint": "http://localhost",
                "region": "us-east-1",
                "access_key_id": null,
                "access_key_id_secret_id": "u1",
                "secret_access_key_secret_id": "u2",
                "session_token": null,
                "session_token_secret_id": "u3",
                "assume_role_arn": null,
                "assume_role_session_name": null,
            }),
        );
    }

    #[mz_ore::test]
    fn connection_aws_assume_role() {
        // Assume-role sets auth_kind and the assume-role columns; credential
        // columns stay null.
        let sql = "CREATE CONNECTION \"materialize\".\"public\".\"c\" TO AWS \
             (ASSUME ROLE ARN 'arn:aws:iam::123:role/mz', \
              ASSUME ROLE SESSION NAME 'sess')";
        let out = super::parse_connection_details(sql).expect("ok");
        assert_eq!(
            as_serde(out),
            json!({
                "auth_kind": "assume-role",
                "endpoint": null,
                "region": null,
                "access_key_id": null,
                "access_key_id_secret_id": null,
                "secret_access_key_secret_id": null,
                "session_token": null,
                "session_token_secret_id": null,
                "assume_role_arn": "arn:aws:iam::123:role/mz",
                "assume_role_session_name": "sess",
            }),
        );
    }

    #[mz_ore::test]
    fn connection_kafka_unquoted_progress_topic() {
        // A bare identifier PROGRESS TOPIC persists unquoted in create_sql. The
        // helper must surface it, else the view substitutes the default topic.
        let sql = "CREATE CONNECTION \"materialize\".\"public\".\"c\" TO KAFKA \
             (BROKER = 'localhost:9092', PROGRESS TOPIC = my_topic, \
              SECURITY PROTOCOL = plaintext)";
        let out = super::parse_connection_details(sql).expect("ok");
        assert_eq!(as_serde(out)["progress_topic"], json!("my_topic"));
    }

    #[mz_ore::test]
    fn connection_aws_unquoted_option_values() {
        // Planning accepts bare identifiers for these options and persists them
        // unquoted. The helper must surface them, not fall back to NULL.
        let sql = "CREATE CONNECTION \"materialize\".\"public\".\"c\" TO AWS \
             (ENDPOINT = localhost, REGION = useast1, \
              ASSUME ROLE ARN 'arn:aws:iam::123:role/mz', \
              ASSUME ROLE SESSION NAME = mysession)";
        let out = super::parse_connection_details(sql).expect("ok");
        let out = as_serde(out);
        assert_eq!(out["endpoint"], json!("localhost"));
        assert_eq!(out["region"], json!("useast1"));
        assert_eq!(out["assume_role_session_name"], json!("mysession"));
    }

    #[mz_ore::test]
    fn connection_aws_empty_endpoint_is_null() {
        // Planning coerces ENDPOINT = '' to None, so the packer wrote NULL. The
        // view must match, not report an empty string.
        let sql = "CREATE CONNECTION \"materialize\".\"public\".\"c\" TO AWS \
             (ENDPOINT = '', ASSUME ROLE ARN 'arn:aws:iam::123:role/mz')";
        let out = super::parse_connection_details(sql).expect("ok");
        assert_eq!(as_serde(out)["endpoint"], serde_json::Value::Null);
    }

    #[mz_ore::test]
    fn connection_other_type_returns_null_jsonb() {
        // A connection type without a detail view (postgres) yields null.
        let sql = "CREATE CONNECTION \"materialize\".\"public\".\"c\" TO POSTGRES \
             (HOST = 'db', DATABASE = 'postgres', USER = 'mz')";
        let out = super::parse_connection_details(sql).expect("ok");
        assert_eq!(as_serde(out), serde_json::Value::Null);
    }

    #[mz_ore::test]
    #[cfg_attr(miri, ignore)] // error: unsupported operation: can't call foreign function `rust_psm_stack_pointer` on OS `linux`
    fn connection_non_connection_returns_null_jsonb() {
        let sql = "CREATE VIEW v AS SELECT 1";
        let out = super::parse_connection_details(sql).expect("ok");
        assert_eq!(as_serde(out), serde_json::Value::Null);
    }

    // --- parse_catalog_create_sql envelope_type ------------------------------

    #[mz_ore::test]
    fn catalog_kafka_old_syntax_omitted_envelope_defaults_none() {
        // An old-syntax kafka source (carries EXPOSE PROGRESS AS) ingests into
        // its own relation, so an omitted ENVELOPE means the default NONE, which
        // the pre-MV packer reported as 'none'.
        let sql = "CREATE SOURCE \"materialize\".\"public\".\"k\" \
             IN CLUSTER [u42] \
             FROM KAFKA CONNECTION [u11 AS \"materialize\".\"public\".\"k_conn\"] \
             (TOPIC 'test') FORMAT TEXT \
             EXPOSE PROGRESS AS [u12 AS \"materialize\".\"public\".\"k_progress\"]";
        let out = super::parse_catalog_create_sql(sql).expect("ok");
        assert_eq!(as_serde(out)["envelope_type"], json!("none"));
    }

    #[mz_ore::test]
    fn catalog_kafka_old_syntax_explicit_envelope() {
        let sql = "CREATE SOURCE \"materialize\".\"public\".\"k\" \
             IN CLUSTER [u42] \
             FROM KAFKA CONNECTION [u11 AS \"materialize\".\"public\".\"k_conn\"] \
             (TOPIC 'test') FORMAT BYTES ENVELOPE UPSERT \
             EXPOSE PROGRESS AS [u12 AS \"materialize\".\"public\".\"k_progress\"]";
        let out = super::parse_catalog_create_sql(sql).expect("ok");
        assert_eq!(as_serde(out)["envelope_type"], json!("upsert"));
    }

    #[mz_ore::test]
    fn catalog_kafka_new_syntax_source_omits_envelope_type() {
        // A new-syntax kafka source has no progress subsource (no EXPOSE PROGRESS
        // AS). It ingests nothing itself. Envelopes live on the per-table exports,
        // so its own envelope_type stays absent (SQL NULL).
        let sql = "CREATE SOURCE \"materialize\".\"public\".\"k\" \
             IN CLUSTER [u42] \
             FROM KAFKA CONNECTION [u11 AS \"materialize\".\"public\".\"k_conn\"]";
        let out = super::parse_catalog_create_sql(sql).expect("ok");
        assert_eq!(as_serde(out).get("envelope_type"), None);
    }

    #[mz_ore::test]
    fn catalog_non_kafka_source_omits_envelope_type() {
        // Non-kafka sources carry no envelope, so envelope_type stays absent
        // (SQL NULL), not 'none'.
        let sql = "CREATE SOURCE \"materialize\".\"public\".\"lg\" \
             IN CLUSTER [u42] FROM LOAD GENERATOR COUNTER";
        let out = super::parse_catalog_create_sql(sql).expect("ok");
        assert_eq!(as_serde(out).get("envelope_type"), None);
    }

    // --- parse_catalog_create_sql, CreateSink arm ----------------------------

    const AVRO_FORMAT: &str = "FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION \
         [u12 AS \"materialize\".\"public\".\"csr_conn\"]";

    /// A persisted kafka-sink `create_sql`: resolved names, and a `TOPIC` that
    /// planning guarantees.
    fn kafka_sink_sql(key: Option<&str>, format: &str, envelope: &str) -> String {
        let key_clause = key.map(|k| format!(" KEY ({k})")).unwrap_or_default();
        format!(
            "CREATE SINK \"materialize\".\"public\".\"snk\" \
             IN CLUSTER [u42] \
             FROM [u1 AS \"materialize\".\"public\".\"t\"] \
             INTO KAFKA CONNECTION [u10 AS \"materialize\".\"public\".\"k_conn\"] \
             (TOPIC 'sink-topic'){key_clause} {format} ENVELOPE {envelope}"
        )
    }

    fn iceberg_sink_sql(mode: &str) -> String {
        format!(
            "CREATE SINK \"materialize\".\"public\".\"ice\" \
             IN CLUSTER [u42] \
             FROM [u1 AS \"materialize\".\"public\".\"t\"] \
             INTO ICEBERG CATALOG CONNECTION [u20 AS \"materialize\".\"public\".\"cat_conn\"] \
             (NAMESPACE 'ns', TABLE 'tbl') \
             USING AWS CONNECTION [u21 AS \"materialize\".\"public\".\"aws_conn\"] \
             MODE {mode}"
        )
    }

    #[mz_ore::test]
    fn sink_kafka_bare_format_without_key() {
        let sql = kafka_sink_sql(None, "FORMAT JSON", "DEBEZIUM");
        let out = super::parse_catalog_create_sql(&sql).expect("ok");
        assert_eq!(
            as_serde(out),
            json!({
                "type": "sink",
                "sink_type": "kafka",
                "cluster_id": "u42",
                "connection_id": "u10",
                "topic": "sink-topic",
                "envelope_type": "debezium",
                "format": "json",
                "value_format": "json",
            }),
        );
    }

    #[mz_ore::test]
    fn sink_kafka_bare_format_with_key_derives_key_format() {
        // A bare format applies to the key too once the sink has a KEY, which
        // is what makes the deprecated `format` column collapse to `avro`.
        let sql = kafka_sink_sql(Some("a"), AVRO_FORMAT, "UPSERT");
        let out = super::parse_catalog_create_sql(&sql).expect("ok");
        assert_eq!(
            as_serde(out),
            json!({
                "type": "sink",
                "sink_type": "kafka",
                "cluster_id": "u42",
                "connection_id": "u10",
                "topic": "sink-topic",
                "envelope_type": "upsert",
                "format": "avro",
                "key_format": "avro",
                "value_format": "avro",
            }),
        );
    }

    #[mz_ore::test]
    fn sink_kafka_bare_text_format_with_key_does_not_collapse() {
        // Only avro/avro and json/json collapse, so a keyed text sink reports
        // the composite form even though both halves are `text`.
        let sql = kafka_sink_sql(Some("a"), "FORMAT TEXT", "UPSERT");
        let out = super::parse_catalog_create_sql(&sql).expect("ok");
        let out = as_serde(out);
        assert_eq!(out["format"], json!("key-text-value-text"));
        assert_eq!(out["key_format"], json!("text"));
        assert_eq!(out["value_format"], json!("text"));
    }

    #[mz_ore::test]
    fn sink_kafka_key_value_json_collapses() {
        let sql = kafka_sink_sql(Some("a"), "KEY FORMAT JSON VALUE FORMAT JSON", "UPSERT");
        let out = as_serde(super::parse_catalog_create_sql(&sql).expect("ok"));
        assert_eq!(out["format"], json!("json"));
        assert_eq!(out["key_format"], json!("json"));
        assert_eq!(out["value_format"], json!("json"));
    }

    #[mz_ore::test]
    fn sink_kafka_key_value_mixed_is_composite() {
        let sql = kafka_sink_sql(Some("a"), "KEY FORMAT TEXT VALUE FORMAT BYTES", "UPSERT");
        let out = as_serde(super::parse_catalog_create_sql(&sql).expect("ok"));
        assert_eq!(out["format"], json!("key-text-value-bytes"));
        assert_eq!(out["key_format"], json!("text"));
        assert_eq!(out["value_format"], json!("bytes"));
    }

    #[mz_ore::test]
    fn sink_kafka_key_format_without_key_is_dropped() {
        // `kafka_sink_builder` ignores the key half of the format spec when the
        // sink has no KEY, so neither `key_format` nor the composite `format`
        // may reflect it.
        let sql = kafka_sink_sql(None, "KEY FORMAT JSON VALUE FORMAT TEXT", "DEBEZIUM");
        let out = as_serde(super::parse_catalog_create_sql(&sql).expect("ok"));
        assert_eq!(out["format"], json!("text"));
        assert_eq!(out["key_format"], serde_json::Value::Null);
        assert_eq!(out["value_format"], json!("text"));
    }

    #[mz_ore::test]
    fn sink_kafka_missing_topic_errors() {
        let sql = "CREATE SINK \"materialize\".\"public\".\"snk\" \
             IN CLUSTER [u42] \
             FROM [u1 AS \"materialize\".\"public\".\"t\"] \
             INTO KAFKA CONNECTION [u10 AS \"materialize\".\"public\".\"k_conn\"] \
             FORMAT JSON ENVELOPE DEBEZIUM";
        let err = super::parse_catalog_create_sql(sql).unwrap_err();
        assert!(
            matches!(err, EvalError::InvalidCatalogJson(msg) if msg.contains("missing TOPIC")),
            "wrong error variant/message"
        );
    }

    #[mz_ore::test]
    fn sink_iceberg_upsert_mode() {
        let sql = iceberg_sink_sql("UPSERT");
        let out = super::parse_catalog_create_sql(&sql).expect("ok");
        assert_eq!(
            as_serde(out),
            json!({
                "type": "sink",
                "sink_type": "iceberg",
                "cluster_id": "u42",
                // The catalog connection, never the AWS connection (u21).
                "connection_id": "u20",
                "namespace": "ns",
                "table": "tbl",
                "envelope_type": "upsert",
            }),
        );
    }

    #[mz_ore::test]
    fn sink_iceberg_append_mode() {
        let sql = iceberg_sink_sql("APPEND");
        let out = as_serde(super::parse_catalog_create_sql(&sql).expect("ok"));
        // `append` is reachable only through an iceberg sink's MODE.
        assert_eq!(out["envelope_type"], json!("append"));
    }

    #[mz_ore::test]
    fn sink_iceberg_missing_table_errors() {
        let sql = "CREATE SINK \"materialize\".\"public\".\"ice\" \
             IN CLUSTER [u42] \
             FROM [u1 AS \"materialize\".\"public\".\"t\"] \
             INTO ICEBERG CATALOG CONNECTION [u20 AS \"materialize\".\"public\".\"cat_conn\"] \
             (NAMESPACE 'ns') MODE UPSERT";
        let err = super::parse_catalog_create_sql(sql).unwrap_err();
        assert!(
            matches!(err, EvalError::InvalidCatalogJson(msg) if msg.contains("missing TABLE")),
            "wrong error variant/message"
        );
    }

    #[mz_ore::test]
    #[cfg_attr(miri, ignore)] // error: unsupported operation: can't call foreign function `rust_psm_stack_pointer` on OS `linux`
    fn sink_arm_leaves_other_item_types_alone() {
        let sql = "CREATE VIEW \"materialize\".\"public\".\"v\" AS SELECT 1";
        let out = super::parse_catalog_create_sql(sql).expect("ok");
        assert_eq!(
            as_serde(out),
            json!({ "type": "view", "definition": "SELECT 1;" })
        );
    }

    /// The `mz_foreign_keys` / `mz_foreign_key_columns` views read these fields,
    /// so the shape is a contract: two relation ids, and column pairs in the
    /// order the statement declared them.
    #[mz_ore::test]
    #[cfg_attr(miri, ignore)] // error: unsupported operation: can't call foreign function `rust_psm_stack_pointer` on OS `linux`
    fn foreign_key_arm_reports_both_sides_and_ordered_column_pairs() {
        let sql = "CREATE FOREIGN KEY \"fk\" \
                   ON [u1 AS \"materialize\".\"public\".\"o\"] (\"cid\", \"region\") \
                   REFERENCES [u2 AS \"materialize\".\"public\".\"c\"] (\"id\", \"region\") \
                   NOT ENFORCED";
        let out = super::parse_catalog_create_sql(sql).expect("ok");
        assert_eq!(
            as_serde(out),
            json!({
                "type": "foreign-key",
                "referencing_id": "u1",
                "referenced_id": "u2",
                "columns": [
                    { "referencing": "cid", "referenced": "id" },
                    { "referencing": "region", "referenced": "region" },
                ],
            })
        );
    }

    // --- parse_catalog_create_sql --------------------------------------------

    /// `type` for a `create_sql`, or the error message if parsing failed.
    fn item_type(sql: &str) -> Result<String, String> {
        match super::parse_catalog_create_sql(sql) {
            Ok(out) => match as_serde(out) {
                serde_json::Value::Object(mut m) => match m.remove("type") {
                    Some(serde_json::Value::String(s)) => Ok(s),
                    other => panic!("no string `type` key: {other:?}"),
                },
                other => panic!("not a JSON object: {other:?}"),
            },
            Err(EvalError::InvalidCatalogJson(msg)) => Err(msg.to_string()),
            Err(e) => panic!("unexpected error variant: {e:?}"),
        }
    }

    fn view_sql(query: &str) -> String {
        format!("CREATE VIEW \"materialize\".\"public\".\"v\" AS {query}")
    }

    /// `definition` for a `CREATE VIEW` whose query is `query`.
    fn view_definition(query: &str) -> String {
        match as_serde(super::parse_catalog_create_sql(&view_sql(query)).expect("ok")) {
            serde_json::Value::Object(mut m) => match m.remove("definition") {
                Some(serde_json::Value::String(s)) => s,
                other => panic!("no string `definition` key: {other:?}"),
            },
            other => panic!("not a JSON object: {other:?}"),
        }
    }

    /// `mz_tables` and `mz_views` select rows by
    /// `parse_catalog_create_sql(...)->>'type'`, and the function runs over
    /// every `Item` row in the catalog, so a statement kind that changes its
    /// reported type silently gains or loses rows in those relations. Pin the
    /// type of every kind the catalog can hold.
    #[mz_ore::test]
    #[cfg_attr(miri, ignore)] // error: unsupported operation: can't call foreign function `rust_psm_stack_pointer` on OS `linux`
    fn catalog_item_type_per_statement_kind() {
        let cases = [
            (
                "CREATE TABLE \"materialize\".\"public\".\"t\" (a int4)",
                "table",
            ),
            // A table created from a source, and a webhook table, are both
            // `table`, so both land in mz_tables.
            (
                "CREATE TABLE \"materialize\".\"public\".\"tbl\" \
                 FROM SOURCE [u1 AS \"materialize\".\"public\".\"src\"] \
                 (REFERENCE = \"topic\") FORMAT TEXT",
                "table",
            ),
            (
                "CREATE TABLE \"materialize\".\"public\".\"wht\" FROM WEBHOOK BODY FORMAT JSON",
                "table",
            ),
            (
                "CREATE VIEW \"materialize\".\"public\".\"v\" AS SELECT 1",
                "view",
            ),
            (
                "CREATE MATERIALIZED VIEW \"materialize\".\"public\".\"mv\" \
                 IN CLUSTER [u1] AS SELECT 1",
                "materialized-view",
            ),
            (
                "CREATE SOURCE \"materialize\".\"public\".\"lg\" \
                 IN CLUSTER [u1] FROM LOAD GENERATOR COUNTER",
                "source",
            ),
            (
                "CREATE SOURCE \"materialize\".\"public\".\"wh\" \
                 IN CLUSTER [u1] FROM WEBHOOK BODY FORMAT JSON",
                "source",
            ),
            (
                "CREATE SUBSOURCE \"materialize\".\"public\".\"sub\" (id int4) \
                 OF SOURCE [u1 AS \"materialize\".\"public\".\"src\"]",
                "subsource",
            ),
            (
                "CREATE SUBSOURCE \"materialize\".\"public\".\"progress\" (id int4) \
                 WITH (PROGRESS)",
                "subsource",
            ),
            (
                "CREATE SINK \"materialize\".\"public\".\"snk\" IN CLUSTER [u1] \
                 FROM [u1 AS \"materialize\".\"public\".\"t\"] \
                 INTO KAFKA CONNECTION [u2 AS \"materialize\".\"public\".\"c\"] \
                 (TOPIC 'tp') FORMAT JSON ENVELOPE DEBEZIUM",
                "sink",
            ),
            (
                "CREATE INDEX \"i\" IN CLUSTER [u1] \
                 ON [u1 AS \"materialize\".\"public\".\"t\"] (\"a\")",
                "index",
            ),
            (
                "CREATE FOREIGN KEY \"fk\" \
                 ON [u1 AS \"materialize\".\"public\".\"o\"] (\"cid\") \
                 REFERENCES [u2 AS \"materialize\".\"public\".\"c\"] (\"id\") NOT ENFORCED",
                "foreign-key",
            ),
            (
                "CREATE TYPE \"materialize\".\"public\".\"ty\" AS LIST (ELEMENT TYPE = int4)",
                "type",
            ),
            (
                "CREATE SECRET \"materialize\".\"public\".\"s\" AS 'x'",
                "secret",
            ),
            (
                "CREATE CONNECTION \"materialize\".\"public\".\"c\" \
                 TO KAFKA (BROKER 'b', SECURITY PROTOCOL PLAINTEXT)",
                "connection",
            ),
        ];
        for (sql, expected) in cases {
            assert_eq!(item_type(sql).as_deref(), Ok(expected), "for {sql}");
        }
    }

    /// `mz_views.definition` is produced here. It used to be produced by
    /// `pack_view_update` in the adapter, so the exact rendering is a
    /// compatibility surface: `pg_views.definition` reads it.
    #[mz_ore::test]
    #[cfg_attr(miri, ignore)] // error: unsupported operation: can't call foreign function `rust_psm_stack_pointer` on OS `linux`
    fn catalog_view_definition() {
        // Identifiers and function names come back fully quoted, literals
        // untouched, and PostgreSQL's trailing semicolon is appended.
        assert_eq!(view_definition("SELECT 1"), "SELECT 1;");
        assert_eq!(
            view_definition("WITH c AS (SELECT 1 AS a) SELECT a FROM c"),
            "WITH \"c\" AS (SELECT 1 AS \"a\") SELECT \"a\" FROM \"c\";"
        );
        assert_eq!(
            view_definition("SELECT 1 UNION ALL SELECT 2"),
            "SELECT 1 UNION ALL SELECT 2;"
        );
        assert_eq!(
            view_definition("SELECT (SELECT max(a) FROM [u1 AS \"materialize\".\"public\".\"t\"])"),
            "SELECT (SELECT \"max\"(\"a\") FROM [u1 AS \"materialize\".\"public\".\"t\"]);"
        );
        // Identifiers needing quotes, an embedded double quote, non-ASCII, an
        // embedded single quote in a literal, and ORDER BY all survive.
        assert_eq!(
            view_definition(
                "SELECT \"a b\", \"héllo\", \"q\"\"x\" \
                 FROM [u1 AS \"materialize\".\"public\".\"t\"] \
                 WHERE s = 'lit''eral' AND n = 42 ORDER BY 1"
            ),
            "SELECT \"a b\", \"héllo\", \"q\"\"x\" \
             FROM [u1 AS \"materialize\".\"public\".\"t\"] \
             WHERE \"s\" = 'lit''eral' AND \"n\" = 42 ORDER BY 1;"
        );
    }

    /// The rendering must be a fixed point: `pg_views` consumers re-issue
    /// `definition` as the body of a new view, so a second pass through the
    /// parser has to produce the identical string. The trailing `;` is part of
    /// what gets re-parsed.
    #[mz_ore::test]
    #[cfg_attr(miri, ignore)] // error: unsupported operation: can't call foreign function `rust_psm_stack_pointer` on OS `linux`
    fn catalog_view_definition_is_idempotent() {
        for query in [
            "SELECT 1",
            "WITH c AS (SELECT 1 AS a) SELECT a FROM c",
            "SELECT 1 UNION ALL SELECT 2",
            "SELECT \"a b\", \"q\"\"x\" FROM [u1 AS \"materialize\".\"public\".\"t\"] \
             WHERE s = 'lit''eral' ORDER BY 1",
        ] {
            let once = view_definition(query);
            assert_eq!(
                view_definition(&once),
                once,
                "not a fixed point for {query}"
            );
        }
    }

    /// `mz_tables.source_id` comes from this key. A table with no source must
    /// omit it entirely, so the MV's `->>'source_id'` yields SQL NULL.
    #[mz_ore::test]
    #[cfg_attr(miri, ignore)] // error: unsupported operation: can't call foreign function `rust_psm_stack_pointer` on OS `linux`
    fn catalog_table_source_id() {
        let from_source = as_serde(
            super::parse_catalog_create_sql(
                "CREATE TABLE \"materialize\".\"public\".\"tbl\" \
                 FROM SOURCE [u1 AS \"materialize\".\"public\".\"src\"] \
                 (REFERENCE = \"topic\") FORMAT TEXT",
            )
            .expect("ok"),
        );
        assert_eq!(from_source, json!({ "type": "table", "source_id": "u1" }));

        for sql in [
            "CREATE TABLE \"materialize\".\"public\".\"t\" (a int4)",
            "CREATE TABLE \"materialize\".\"public\".\"wht\" FROM WEBHOOK BODY FORMAT JSON",
        ] {
            assert_eq!(
                as_serde(super::parse_catalog_create_sql(sql).expect("ok")),
                json!({ "type": "table" }),
                "for {sql}"
            );
        }
    }

    /// Every error here is fatal to the whole of `mz_tables`/`mz_views`, not to
    /// one row: the MVs call this function inside their `WHERE` clause, so an
    /// item the parser rejects makes the relation unreadable for everyone.
    #[mz_ore::test]
    #[cfg_attr(miri, ignore)] // error: unsupported operation: can't call foreign function `rust_psm_stack_pointer` on OS `linux`
    fn catalog_create_sql_errors() {
        assert_eq!(
            item_type("this is not sql"),
            Err(
                "failed to parse create_sql: Expected a keyword at the beginning of a statement, \
                 found identifier \"this\""
                    .to_string()
            )
        );
        assert_eq!(
            item_type("CREATE TABLE t (a int4); CREATE TABLE u (b int4)"),
            Err("expected a single statement, found 2".to_string())
        );
        // A statement that is not a CREATE of a catalog item, e.g. if a future
        // change persists something else in an Item record.
        assert_eq!(
            item_type("SELECT 1"),
            Err("not a CREATE item statement".to_string())
        );
        // Catalog `create_sql` always names items by id. An unresolved name
        // means the record was written wrong.
        assert_eq!(
            item_type(
                "CREATE TABLE \"materialize\".\"public\".\"tbl\" \
                 FROM SOURCE src (REFERENCE = \"topic\")"
            ),
            Err("unresolved item name".to_string())
        );
    }
}

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
use mz_lowertest::MzReflect;
use mz_repr::adt::jsonb::{Jsonb, JsonbRef};
use mz_repr::adt::mz_acl_item::{AclMode, MzAclItem};
use mz_repr::adt::numeric::{self, Numeric, NumericMaxScale};
use mz_repr::role_id::RoleId;
use mz_repr::{ArrayRustType, Datum, Row, RowPacker, SqlColumnType, SqlScalarType, strconv};
use mz_sql_parser::ast::display::AstDisplay;
use mz_sql_parser::ast::{
    AstInfo, AvroSchema, ConnectionOption, ConnectionOptionName, CreateConnectionType,
    CreateSubsourceOptionName, Format, FormatSpecifier, KafkaSourceConfigOptionName,
    PgConfigOptionName, ProtobufSchema, RawClusterName, RawItemName, SourceEnvelope,
    SourceErrorPolicy, Value, WithOptionValue,
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
    Hash,
    MzReflect
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
            CreateView(_) => "view",
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
            CreateTable(_) | CreateTableFromSource(_) => "table",
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

                if let Some(envelope) = stmt.envelope {
                    use mz_sql_parser::ast::SourceEnvelope::*;
                    let envelope_type = match envelope {
                        None => "none",
                        Debezium => "debezium",
                        Upsert { .. } => "upsert",
                        CdcV2 => "materialize",
                    };
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
            CreateSink(_) => "sink",
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
            CreateType(_) => "type",
            _ => return Err("not a CREATE item statement".into()),
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
            let string_value = match opt.value {
                Some(WithOptionValue::Value(Value::String(s))) => Some(s),
                _ => None,
            };
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
/// `mz_kafka_connections`, `mz_ssh_tunnel_connections`, `mz_aws_connections`,
/// and `mz_aws_privatelink_connections` builtin views need. For everything
/// else (other connection types, non-connection statements) it returns jsonb
/// `null`, so callers filter on `IS NOT NULL` and gate on the connection type
/// separately (via `parse_catalog_create_sql(...)->>'connection_type'`, the way
/// `mz_connections` already does).
///
/// The shape per type:
///
/// ```json
/// // kafka
/// { "brokers": ["host:port", ...], "progress_topic": <text | null> }
/// // ssh-tunnel
/// { "public_key_1": "<text>", "public_key_2": "<text>" }
/// ```
///
/// `progress_topic` is null when the connection does not set an explicit
/// `PROGRESS TOPIC`. The default (`_materialize-progress-<env>-<conn_id>`) is
/// reconstructed by the view, not here, because it needs the environment id and
/// the connection's own id. Values derived only from environment context
/// (AWS principal, external id, trust policy, privatelink principal) are also
/// left to the view. This keeps the helper a pure function of the `create_sql`.
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
        values.iter().find_map(|o| match &o.value {
            Some(WithOptionValue::Value(Value::String(s))) if o.name == name => Some(s.clone()),
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
}

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
use mz_sql_parser::ast::{AstInfo, Format, FormatSpecifier, RawClusterName, RawItemName};
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
                        let Datum::Map(mode_dict) = val else {
                            return Err(format!("unexpected acl_mode: {val}"));
                        };
                        let (key, val) = mode_dict.iter().next().ok_or("empty acl_mode")?;
                        if key != "bitflags" {
                            return Err(format!("unexpected acl_mode field: {key}"));
                        }
                        let bits = jsonb_datum_to_u64(val)?;
                        let Some(mode) = AclMode::from_bits(bits) else {
                            return Err(format!("invalid acl_mode bitflags: {bits}"));
                        };
                        acl_mode = Some(mode);
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
            CreateContinualTask(_) => "continual-task",
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
                info.insert("source_type", json!("webhook"));
                if let Some(in_cluster) = stmt.in_cluster {
                    let cluster_id = get_cluster_id(in_cluster)?;
                    info.insert("cluster_id", json!(cluster_id));
                }

                "source"
            }
            CreateSubsource(_) => {
                info.insert("source_type", json!("subsource"));

                "subsource"
            }
            CreateSink(_) => "sink",
            CreateIndex(_) => "index",
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

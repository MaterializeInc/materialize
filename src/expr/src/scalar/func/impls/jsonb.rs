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

/// Decodes the externally-tagged `proto::audit_log_event_v1::Details` JSON
/// (e.g. `{"IdFullNameV1": {"id": "u1", "name": {...}}}`) into the audit-log
/// shape: the format the prior `pack_audit_log_update` populator wrote to
/// `mz_audit_events`. The helper covers the variants whose only proto/audit-log
/// divergence is `#[serde(flatten)]` or `StringWrapper`; for variants with
/// renamed fields (`external_type`/`type`) or enum-with-payload encodings
/// (`reason`, `scheduling_policies`), the output keeps the proto shape. See
/// the comment on `mz_audit_events` in `src/catalog/src/builtin/mz_catalog.rs`
/// for the user-visible consequences.
///
/// Specifically the helper applies three normalizations:
///   1. Strips the externally-tagged variant wrapper (e.g. drops the
///      `"IdFullNameV1"` key, returning the inner struct).
///   2. For variants whose `audit_log::*` struct has `#[serde(flatten)]` on
///      an inner field (the `(variant, field)` pairs in `FLATTENED_FIELDS`),
///      hoists the inner struct's fields into the outer object.
///   3. Within any value, collapses `{"inner": <v>}` to `<v>` recursively.
///      The audit-log crate uses plain `String` where the proto crate uses
///      `StringWrapper`.
///
/// New variants on `audit_log::EventDetails` that add a `#[serde(flatten)]`
/// field must add an entry to `FLATTENED_FIELDS`; otherwise the MV will
/// continue to emit the un-flattened shape for that variant.
#[sqlfunc]
fn parse_catalog_audit_log_details<'a>(a: JsonbRef<'a>) -> Result<Jsonb, EvalError> {
    /// `(variant_name, flatten_field_name, sub_variant_name)` triples mirroring
    /// the `#[serde(flatten)]` attributes on `audit_log::EventDetails` variants.
    /// `sub_variant_name` is the variant name to apply flattening recursively
    /// to the flatten target (used when the flattened struct is itself a struct
    /// with its own `#[serde(flatten)]`). Keep in sync with
    /// `src/audit-log/src/lib.rs`.
    const FLATTENED_FIELDS: &[(&str, &str, Option<&str>)] = &[
        ("IdFullNameV1", "name", None),
        ("CreateSourceSinkV1", "name", None),
        ("CreateSourceSinkV2", "name", None),
        ("CreateSourceSinkV3", "name", None),
        ("CreateSourceSinkV4", "name", None),
        ("CreateIndexV1", "name", None),
        ("CreateMaterializedViewV1", "name", None),
        ("AlterSourceSinkV1", "name", None),
        ("AlterSetClusterV1", "name", None),
        ("UpdateItemV1", "name", None),
        // `AlterApplyReplacementV1.target: IdFullNameV1`, which itself flattens
        // `name: FullNameV1`.
        ("AlterApplyReplacementV1", "target", Some("IdFullNameV1")),
    ];

    /// Returns true if `dict` structurally matches `audit_log::FullNameV1`:
    /// exactly three string-valued keys `database`, `schema`, `item`. Used by
    /// `push_unwrapped` to identify nested `name: FullNameV1` fields that
    /// `as_json` would have flattened.
    fn is_full_name_v1(dict: &mz_repr::DatumMap) -> bool {
        let mut saw_db = false;
        let mut saw_schema = false;
        let mut saw_item = false;
        let mut other = false;
        for (k, v) in dict.iter() {
            if !matches!(v, Datum::String(_)) {
                return false;
            }
            match k {
                "database" => saw_db = true,
                "schema" => saw_schema = true,
                "item" => saw_item = true,
                _ => other = true,
            }
        }
        saw_db && saw_schema && saw_item && !other
    }

    fn collect_flattened<'a>(
        dict: &mz_repr::DatumMap<'a>,
        variant: &str,
        out: &mut Vec<(&'a str, Datum<'a>)>,
    ) -> Result<(), String> {
        let entry = FLATTENED_FIELDS.iter().find(|(v, _, _)| *v == variant);
        let flatten_field = entry.map(|(_, f, _)| *f);
        let sub_variant = entry.and_then(|(_, _, s)| *s);
        let mut sub_target: Option<Datum<'a>> = None;
        for (k, v) in dict.iter() {
            if Some(k) == flatten_field {
                sub_target = Some(v);
            } else {
                out.push((k, v));
            }
        }
        if let Some(sub) = sub_target {
            let Datum::Map(sub_dict) = sub else {
                // Safe to unwrap: `sub_target` is only set when
                // `Some(k) == flatten_field`, which requires
                // `flatten_field.is_some()`.
                return Err(format!(
                    "expected '{}' field to be an object",
                    flatten_field.expect("set whenever sub_target is"),
                ));
            };
            // Recurse only when the flattened type itself has its own
            // `#[serde(flatten)]` (encoded via `sub_variant_name`). Otherwise
            // its fields are just merged in flat.
            collect_flattened(&sub_dict, sub_variant.unwrap_or(""), out)?;
        }
        Ok(())
    }

    /// Push a datum, performing the audit-log shape normalizations:
    /// * collapses `{"inner": <v>}` to `<v>` (proto `StringWrapper` →
    ///   plain `String`),
    /// * hoists the fields of an inner `FullNameV1`-shaped object (detected
    ///   structurally, see `is_full_name_v1`) into the parent. This mirrors
    ///   `#[serde(flatten)]` on `audit_log::IdFullNameV1.name`, which appears
    ///   in nested-by-value positions (e.g. `AlterApplyReplacementV1.replacement`)
    ///   that `collect_flattened` can't reach from the variant name alone.
    fn push_unwrapped(row: &mut RowPacker, datum: Datum) {
        match datum {
            Datum::Map(dict) => {
                // `StringWrapper`-shaped object: a single key named `inner`.
                let mut iter = dict.iter();
                if let (Some((k, v)), None) = (iter.next(), iter.next()) {
                    if k == "inner" {
                        push_unwrapped(row, v);
                        return;
                    }
                }
                // Gather entries; structurally identify any field whose value
                // is a `FullNameV1` and hoist its fields.
                let mut fields: Vec<(&str, Datum)> = Vec::new();
                for (k, v) in dict.iter() {
                    match v {
                        Datum::Map(sub) if is_full_name_v1(&sub) => {
                            for (sk, sv) in sub.iter() {
                                fields.push((sk, sv));
                            }
                        }
                        _ => fields.push((k, v)),
                    }
                }
                fields.sort_by_key(|(k, _)| *k);
                row.push_dict_with(|row| {
                    for (k, v) in &fields {
                        row.push(Datum::String(k));
                        push_unwrapped(row, *v);
                    }
                });
            }
            Datum::List(list) => {
                row.push_list_with(|row| {
                    for v in list.iter() {
                        push_unwrapped(row, v);
                    }
                });
            }
            other => row.push(other),
        }
    }

    let parse = || -> Result<Jsonb, String> {
        let Datum::Map(dict) = a.into_datum() else {
            return Err("expected object".into());
        };
        let mut iter = dict.iter();
        let (variant, inner) = iter
            .next()
            .ok_or_else(|| "empty details enum".to_string())?;
        if iter.next().is_some() {
            return Err("details enum had multiple keys".into());
        }
        let Datum::Map(inner_dict) = inner else {
            return Err(format!("expected inner object for variant {variant}"));
        };
        let mut fields: Vec<(&str, Datum)> = Vec::new();
        collect_flattened(&inner_dict, variant, &mut fields)?;
        // Datum::Map requires keys in ascending order.
        fields.sort_by_key(|(k, _)| *k);
        let mut row = Row::default();
        row.packer().push_dict_with(|row| {
            for (k, v) in &fields {
                row.push(Datum::String(k));
                push_unwrapped(row, *v);
            }
        });
        Ok(Jsonb::from_row(row))
    };

    parse().map_err(|e| EvalError::InvalidCatalogJson(e.into()))
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

#[cfg(test)]
mod tests {
    use super::*;

    /// Round-trip a JSON `input` through `parse_catalog_audit_log_details` and
    /// assert the resulting JSON parses equal to `expected`.
    fn check(input: &str, expected: &str) {
        let input: Jsonb = input.parse().expect("valid input JSONB");
        let actual = parse_catalog_audit_log_details(input.as_ref())
            .expect("helper succeeded")
            .to_string();
        let actual_value: serde_json::Value =
            serde_json::from_str(&actual).expect("valid output JSON");
        let expected_value: serde_json::Value =
            serde_json::from_str(expected).expect("valid expected JSON");
        assert_eq!(actual_value, expected_value);
    }

    /// Run `parse_catalog_audit_log_details` on `input` and assert it returns
    /// a `InvalidCatalogJson` error containing `expected_substr`.
    fn check_err(input: &str, expected_substr: &str) {
        let input: Jsonb = input.parse().expect("valid input JSONB");
        let err = parse_catalog_audit_log_details(input.as_ref()).expect_err("helper should error");
        let msg = format!("{err:?}");
        assert!(
            msg.contains(expected_substr),
            "error did not contain {expected_substr:?}: {msg}",
        );
    }

    /// Variant with no nested struct and no `#[serde(flatten)]`. The helper
    /// just strips the wrapper.
    #[mz_ore::test]
    fn variant_strip() {
        check(
            r#"{"IdNameV1": {"id": "u1", "name": "foo"}}"#,
            r#"{"id": "u1", "name": "foo"}"#,
        );
    }

    /// `IdFullNameV1` has `#[serde(flatten)] name: FullNameV1`. The helper
    /// must hoist `database`/`schema`/`item` to the top level.
    #[mz_ore::test]
    fn flatten_name() {
        check(
            r#"{"IdFullNameV1": {
                "id": "u1",
                "name": {"database": "materialize", "schema": "public", "item": "t"}
            }}"#,
            r#"{"id": "u1", "database": "materialize", "schema": "public", "item": "t"}"#,
        );
    }

    /// `AlterApplyReplacementV1` flattens `target: IdFullNameV1`, which itself
    /// flattens `name: FullNameV1`. Exercises the recursive sub-variant lookup.
    /// The non-flattened `replacement: IdFullNameV1` field still has its inner
    /// `name` hoisted by the structural `is_full_name_v1` detection in
    /// `push_unwrapped`.
    #[mz_ore::test]
    fn flatten_recursive_and_nested_full_name() {
        check(
            r#"{"AlterApplyReplacementV1": {
                "target": {
                    "id": "u1",
                    "name": {"database": "materialize", "schema": "public", "item": "mv"}
                },
                "replacement": {
                    "id": "u2",
                    "name": {"database": "materialize", "schema": "public", "item": "rp"}
                }
            }}"#,
            r#"{
                "id": "u1",
                "database": "materialize",
                "schema": "public",
                "item": "mv",
                "replacement": {
                    "id": "u2",
                    "database": "materialize",
                    "schema": "public",
                    "item": "rp"
                }
            }"#,
        );
    }

    /// `Option<StringWrapper>` fields serialize as `{"inner": "..."}` in the
    /// proto, where the audit-log crate uses a plain `String`. The helper must
    /// unwrap recursively (including on optional fields nested under flatten).
    #[mz_ore::test]
    fn string_wrapper_unwrap() {
        check(
            r#"{"AlterDefaultPrivilegeV1": {
                "role_id": "u1",
                "database_id": {"inner": "u2"},
                "schema_id": {"inner": "u3"},
                "grantee_id": "p",
                "privileges": "r"
            }}"#,
            r#"{
                "role_id": "u1",
                "database_id": "u2",
                "schema_id": "u3",
                "grantee_id": "p",
                "privileges": "r"
            }"#,
        );
    }

    /// Null `Option<StringWrapper>` fields are passed through as JSON null.
    #[mz_ore::test]
    fn null_option() {
        check(
            r#"{"AlterDefaultPrivilegeV1": {
                "role_id": "u1",
                "database_id": null,
                "schema_id": null,
                "grantee_id": "p",
                "privileges": "r"
            }}"#,
            r#"{
                "role_id": "u1",
                "database_id": null,
                "schema_id": null,
                "grantee_id": "p",
                "privileges": "r"
            }"#,
        );
    }

    /// `is_full_name_v1` requires all three of `database`, `schema`, `item` to
    /// be present; objects that look similar but aren't `FullNameV1` (e.g. an
    /// extra key, or a missing key) must NOT be flattened.
    #[mz_ore::test]
    fn not_full_name_not_flattened() {
        check(
            r#"{"IdNameV1": {
                "id": "u1",
                "name": {"database": "d", "schema": "s"}
            }}"#,
            r#"{
                "id": "u1",
                "name": {"database": "d", "schema": "s"}
            }"#,
        );
        check(
            r#"{"IdNameV1": {
                "id": "u1",
                "name": {"database": "d", "schema": "s", "item": "t", "extra": "x"}
            }}"#,
            r#"{
                "id": "u1",
                "name": {"database": "d", "schema": "s", "item": "t", "extra": "x"}
            }"#,
        );
    }

    /// Bad inputs: empty enum object, multiple keys, non-object.
    #[mz_ore::test]
    fn error_cases() {
        check_err(r#"{}"#, "empty details enum");
        check_err(
            r#"{"A": {"x": 1}, "B": {"y": 2}}"#,
            "details enum had multiple keys",
        );
        check_err(r#"["IdNameV1", {"id": "u1"}]"#, "expected object");
        check_err(r#"{"IdNameV1": "not an object"}"#, "expected inner object");
    }
}

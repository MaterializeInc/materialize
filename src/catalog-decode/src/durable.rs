// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Decoders for values in their durable catalog JSON encoding, the serde shape
//! of the `mz-catalog-protos` types.

use mz_repr::Datum;
use mz_repr::adt::jsonb::JsonbRef;
use mz_repr::adt::mz_acl_item::{AclMode, MzAclItem};
use mz_repr::adt::numeric;
use mz_repr::role_id::RoleId;

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
/// Shared decoder for [`privileges`] (which embeds the object as the
/// `acl_mode` field of each privilege) and [`acl_mode`] (which receives the
/// object at the top level).
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
pub fn catalog_id(a: JsonbRef<'_>) -> Result<String, String> {
    match a.into_datum() {
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
    }
}

/// Converts a catalog JSON-serialized privilege array into a list of `MzAclItem`s.
pub fn privileges(a: JsonbRef<'_>) -> Result<Vec<MzAclItem>, String> {
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

    match a.into_datum() {
        Datum::List(list) => {
            let mut result = Vec::new();
            for item in list.iter() {
                result.push(parse_one(item)?);
            }
            Ok(result)
        }
        _ => Err("expected array".to_string()),
    }
}

/// Converts a catalog JSON-serialized `AclMode` bitflags object
/// (e.g. `{"bitflags": 514}`) into an `AclMode`.
pub fn acl_mode(a: JsonbRef<'_>) -> Result<AclMode, String> {
    jsonb_datum_to_acl_mode(a.into_datum())
}

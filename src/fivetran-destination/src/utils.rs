// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::error::{OpError, OpErrorKind};
use crate::fivetran_sdk::{DataType, DecimalParams};

use mz_pgrepr::Type;

/// According to folks from Fivetran, checking if a column name is prefixed with a specific
/// string is enough to determine if it's a system column.
///
/// See: <https://materializeinc.slack.com/archives/C060KAR4802/p1706557379061899>
///
/// ```
/// use mz_fivetran_destination::utils;
///
/// // Needs to be prefixed with '_fivetran_' to be considered a system column.
/// assert!(!utils::is_system_column("timestamp"));
/// assert!(!utils::is_system_column("_fivetrantimestamp"));
/// assert!(!utils::is_system_column("my_fivetran_timestamp"));
///
/// assert!(utils::is_system_column("_fivetran_timestamp"));
/// ```
pub fn is_system_column(name: &str) -> bool {
    name.starts_with("_fivetran_")
}

/// Converts a type defined in the Fivetran SDK to the name of one that Materialize supports.
///
/// Errors if Materialize doesn't support the data type.
pub fn to_materialize_type(ty: DataType) -> Result<&'static str, OpError> {
    match ty {
        DataType::Boolean => Ok("boolean"),
        DataType::Short => Ok("smallint"),
        DataType::Int => Ok("integer"),
        DataType::Long => Ok("bigint"),
        DataType::Decimal => Ok("numeric"),
        DataType::Float => Ok("real"),
        DataType::Double => Ok("double precision"),
        DataType::NaiveDate => Ok("date"),
        DataType::NaiveDatetime => Ok("timestamp"),
        DataType::UtcDatetime => Ok("timestamptz"),
        DataType::Binary => Ok("bytea"),
        DataType::String => Ok("text"),
        DataType::Json => Ok("jsonb"),
        DataType::Unspecified | DataType::Xml => {
            let msg = format!("{} data type is unsupported", ty.as_str_name());
            Err(OpErrorKind::Unsupported(msg).into())
        }
    }
}

/// Converts a Postgres data type, to one supported by the Fivetran SDK.
pub fn to_fivetran_type(ty: Type) -> Result<(DataType, Option<DecimalParams>), OpError> {
    match ty {
        Type::Bool => Ok((DataType::Boolean, None)),
        Type::Int2 => Ok((DataType::Short, None)),
        Type::Int4 => Ok((DataType::Int, None)),
        Type::Int8 => Ok((DataType::Long, None)),
        Type::Numeric { constraints } => {
            let params = match constraints {
                None => None,
                Some(constraints) => {
                    let precision = u32::try_from(constraints.max_precision()).map_err(|_| {
                        OpErrorKind::InvariantViolated(format!(
                            "negative numeric precision: {}",
                            constraints.max_precision()
                        ))
                    })?;
                    let scale = u32::try_from(constraints.max_scale()).map_err(|_| {
                        OpErrorKind::InvariantViolated(format!(
                            "negative numeric scale: {}",
                            constraints.max_scale()
                        ))
                    })?;
                    Some(DecimalParams { precision, scale })
                }
            };
            Ok((DataType::Decimal, params))
        }
        Type::Float4 => Ok((DataType::Float, None)),
        Type::Float8 => Ok((DataType::Double, None)),
        Type::Date => Ok((DataType::NaiveDate, None)),
        Type::Timestamp { precision: _ } => Ok((DataType::NaiveDatetime, None)),
        Type::TimestampTz { precision: _ } => Ok((DataType::UtcDatetime, None)),
        Type::Bytea => Ok((DataType::Binary, None)),
        Type::Text => Ok((DataType::String, None)),
        Type::Jsonb => Ok((DataType::Json, None)),
        _ => {
            let msg = format!("no mapping to Fivetran data type for OID {}", ty.oid());
            Err(OpErrorKind::Unsupported(msg).into())
        }
    }
}

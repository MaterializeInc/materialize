// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! A macro to simplify handling of `WITH` options.

use std::collections::BTreeMap;

use anyhow::{anyhow, bail, Context};
use rusoto_core::Region;

use aws_util::aws;

use crate::ast::Value;

macro_rules! with_option_type {
    ($name:ident, String) => {
        if let Some(crate::ast::WithOptionValue::Value(crate::ast::Value::String(value))) = $name {
            value
        } else if let Some(crate::ast::WithOptionValue::ObjectName(name)) = $name {
            crate::ast::display::AstDisplay::to_ast_string(&name)
        } else {
            ::anyhow::bail!("expected String");
        }
    };
    ($name:ident, bool) => {
        if let Some(crate::ast::WithOptionValue::Value(crate::ast::Value::Boolean(value))) = $name {
            value
        } else if $name.is_none() {
            // Bools, if they have no '= value', are true.
            true
        } else {
            ::anyhow::bail!("expected bool");
        }
    };
    ($name:ident, Interval) => {
        if let Some(crate::ast::WithOptionValue::Value(Value::String(value))) = $name {
            ::repr::strconv::parse_interval(&value)?
        } else if let Some(crate::ast::WithOptionValue::Value(Value::Interval(interval))) = $name {
            ::repr::strconv::parse_interval(&interval.value)?
        } else {
            ::anyhow::bail!("expected Interval");
        }
    };
}

/// This macro accepts a struct definition and will generate it and a `try_from`
/// method that takes a `Vec<WithOption>` which will extract and type check
/// options based on the struct field names and types. Field names must match
/// exactly the lowercased option name. Supported types are:
///
/// - `String`: expects a SQL string (`WITH (name = "value")`) or identifier
///   (`WITH (name = text)`).
/// - `bool`: expects either a SQL bool (`WITH (name = true)`) or a valueless
///   option which will be interpreted as true: (`WITH (name)`.
/// - `Interval`: expects either a SQL interval or string that can be parsed as
///   an interval.
macro_rules! with_options {
  (struct $name:ident {
        $($field_name:ident: $field_type:ident,)*
    }) => {
        #[derive(Debug)]
        pub struct $name {
            pub $($field_name: Option<$field_type>,)*
        }

        impl ::std::convert::TryFrom<Vec<crate::ast::WithOption>> for $name {
            type Error = anyhow::Error;

            fn try_from(mut options: Vec<crate::ast::WithOption>) -> Result<Self, Self::Error> {
                let v = Self {
                    $($field_name: {
                        match options.iter().position(|opt| opt.key.as_str() == stringify!($field_name)) {
                            None => None,
                            Some(pos) => {
                                let value: Option<crate::ast::WithOptionValue> = options.swap_remove(pos).value;
                                let value: $field_type = with_option_type!(value, $field_type);
                                Some(value)
                            },
                        }
                    },
                    )*
                };
                if !options.is_empty() {
                    ::anyhow::bail!("unexpected options");
                }
                Ok(v)
            }
        }
    }
}

pub(crate) fn aws_connect_info(
    options: &mut BTreeMap<String, Value>,
    region: Option<String>,
) -> anyhow::Result<aws::ConnectInfo> {
    let mut extract = |key| match options.remove(key) {
        Some(Value::String(key)) => {
            if !key.is_empty() {
                Ok(Some(key))
            } else {
                Ok(None)
            }
        }
        Some(_) => bail!("{} must be a string", key),
        _ => Ok(None),
    };

    let region_raw = match region {
        Some(region) => region,
        None => extract("region")?.ok_or_else(|| anyhow!("region is required"))?,
    };

    let region = match region_raw.parse() {
        Ok(region) => {
            // ignore/drop the endpoint option if we're pointing at a valid,
            // non-custom AWS region. Endpoints are meaningless without custom
            // regions, and this makes writing tests that support both
            // LocalStack and real AWS much easier.
            let _ = extract("endpoint");
            region
        }
        Err(e) => {
            // Region's FromStr doesn't support parsing custom regions.
            // If a Kinesis stream's ARN indicates it exists in a custom
            // region, support it iff a valid endpoint for the stream
            // is also provided.
            match extract("endpoint").with_context(|| {
                format!("endpoint is required for custom regions: {:?}", region_raw)
            })? {
                Some(endpoint) => Region::Custom {
                    name: region_raw,
                    endpoint,
                },
                _ => bail!(
                    "Unable to parse AWS region: {}. If providing a custom \
                         region, an `endpoint` option must also be provided",
                    e
                ),
            }
        }
    };

    aws::ConnectInfo::new(
        region,
        extract("access_key_id")?,
        extract("secret_access_key")?,
        extract("token")?,
    )
}

#[cfg(test)]
mod test {
    use std::collections::BTreeMap;

    use super::*;
    use crate::ast::Value;

    #[test]
    fn with_options_errirs_if_endpoint_missing_for_invalid_region() {
        let mut map = BTreeMap::new();
        map.insert("region".to_string(), Value::String("nonsense".into()));
        assert!(aws_connect_info(&mut map, None).is_err());

        let mut map = BTreeMap::new();
        assert!(aws_connect_info(&mut map, Some("nonsense".into())).is_err());
    }

    #[test]
    fn with_options_allows_invalid_region_with_endpoint() {
        let mut map = BTreeMap::new();
        map.insert("region".to_string(), Value::String("nonsense".into()));
        map.insert("endpoint".to_string(), Value::String("endpoint".into()));
        assert!(aws_connect_info(&mut map, None).is_ok());

        let mut map = BTreeMap::new();
        map.insert("endpoint".to_string(), Value::String("endpoint".into()));
        assert!(aws_connect_info(&mut map, Some("nonsense".into())).is_ok());
    }

    #[test]
    fn with_options_ignores_endpoint_with_valid_region() {
        let mut map = BTreeMap::new();
        map.insert("region".to_string(), Value::String("us-east-1".into()));
        map.insert("endpoint".to_string(), Value::String("endpoint".into()));
        assert!(aws_connect_info(&mut map, None).is_ok());

        let mut map = BTreeMap::new();
        map.insert("endpoint".to_string(), Value::String("endpoint".into()));
        assert!(aws_connect_info(&mut map, Some("us-east-1".into())).is_ok());

        let mut map = BTreeMap::new();
        map.insert("region".to_string(), Value::String("us-east-1".into()));
        assert!(aws_connect_info(&mut map, None).is_ok());

        let mut map = BTreeMap::new();
        assert!(aws_connect_info(&mut map, Some("us-east-1".into())).is_ok());
    }
}

// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! A macro to simplify handling of `WITH` options.

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

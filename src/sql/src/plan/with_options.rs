// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Provides tooling to handle `WITH` options.

use anyhow::bail;
use serde::{Deserialize, Serialize};

use mz_repr::adt::interval::Interval;
use mz_repr::strconv;

use crate::ast::{AstInfo, IntervalValue, Value, WithOptionValue};

pub trait TryFromValue<T>: Sized {
    fn try_from_value(v: T) -> Result<Self, anyhow::Error>;
}

pub trait ImpliedValue: Sized {
    fn implied_value() -> Result<Self, anyhow::Error>;
}

impl TryFromValue<Value> for Interval {
    fn try_from_value(v: Value) -> Result<Self, anyhow::Error> {
        Ok(match v {
            Value::Interval(IntervalValue { value, .. })
            | Value::Number(value)
            | Value::String(value) => strconv::parse_interval(&value)?,
            _ => bail!("cannot use value as interval"),
        })
    }
}

impl ImpliedValue for Interval {
    fn implied_value() -> Result<Self, anyhow::Error> {
        bail!("must provide an interval value")
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Hash, Deserialize)]
pub struct OptionalInterval(pub Option<Interval>);

impl TryFromValue<Value> for OptionalInterval {
    fn try_from_value(v: Value) -> Result<Self, anyhow::Error> {
        let inner = match v {
            Value::Null => None,
            v => {
                let d = Interval::try_from_value(v)?;
                // An interval of 0 disables the setting.
                if d == Interval::default() {
                    None
                } else {
                    Some(d)
                }
            }
        };
        Ok(OptionalInterval(inner))
    }
}

impl ImpliedValue for OptionalInterval {
    fn implied_value() -> Result<Self, anyhow::Error> {
        bail!("must provide an interval value")
    }
}

impl TryFromValue<Value> for String {
    fn try_from_value(v: Value) -> Result<Self, anyhow::Error> {
        match v {
            Value::String(v) => Ok(v),
            _ => anyhow::bail!("cannot use value as string"),
        }
    }
}

impl ImpliedValue for String {
    fn implied_value() -> Result<Self, anyhow::Error> {
        bail!("must provide a string value")
    }
}

impl TryFromValue<Value> for bool {
    fn try_from_value(v: Value) -> Result<Self, anyhow::Error> {
        match v {
            Value::Boolean(v) => Ok(v),
            _ => anyhow::bail!("cannot use value as boolean"),
        }
    }
}

impl ImpliedValue for bool {
    fn implied_value() -> Result<Self, anyhow::Error> {
        Ok(true)
    }
}

impl TryFromValue<Value> for i32 {
    fn try_from_value(v: Value) -> Result<Self, anyhow::Error> {
        match v {
            Value::Number(v) => v
                .parse::<i32>()
                .map_err(|_| anyhow::anyhow!("invalid numeric value")),
            _ => anyhow::bail!("cannot use value as number"),
        }
    }
}

impl ImpliedValue for i32 {
    fn implied_value() -> Result<Self, anyhow::Error> {
        bail!("must provide an integer value")
    }
}

impl<V: TryFromValue<Value>> TryFromValue<Value> for Vec<V> {
    fn try_from_value(v: Value) -> Result<Self, anyhow::Error> {
        match v {
            Value::Array(a) => {
                let mut out = Vec::with_capacity(a.len());
                for i in a {
                    out.push(
                        V::try_from_value(i)
                            .map_err(|_| anyhow::anyhow!("cannot use value in array"))?,
                    )
                }
                Ok(out)
            }
            _ => anyhow::bail!("cannot use value as array"),
        }
    }
}

impl<V: TryFromValue<Value>> ImpliedValue for Vec<V> {
    fn implied_value() -> Result<Self, anyhow::Error> {
        bail!("must provide an array value")
    }
}

impl<V: TryFromValue<Value>, T: AstInfo> TryFromValue<WithOptionValue<T>> for V {
    fn try_from_value(v: WithOptionValue<T>) -> Result<Self, anyhow::Error> {
        match v {
            WithOptionValue::Value(v) => V::try_from_value(v),
            _ => bail!("incompatible value types"),
        }
    }
}

impl<T, V: TryFromValue<T> + ImpliedValue> TryFromValue<Option<T>> for V {
    fn try_from_value(v: Option<T>) -> Result<Self, anyhow::Error> {
        match v {
            Some(v) => V::try_from_value(v),
            None => V::implied_value(),
        }
    }
}

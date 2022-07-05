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
use mz_repr::GlobalId;
use mz_storage::client::connections::StringOrSecret;

use crate::ast::{AstInfo, IntervalValue, Value, WithOptionValue};
use crate::names::ResolvedObjectName;
use crate::plan::Aug;

pub trait TryFromValue<T>: Sized {
    fn try_from_value(v: T) -> Result<Self, anyhow::Error>;
}

pub trait ImpliedValue: Sized {
    fn implied_value() -> Result<Self, anyhow::Error>;
}

#[derive(Copy, Clone, Debug)]
pub struct Secret(GlobalId);

impl From<Secret> for GlobalId {
    fn from(secret: Secret) -> Self {
        secret.0
    }
}

impl TryFromValue<WithOptionValue<Aug>> for Secret {
    fn try_from_value(v: WithOptionValue<Aug>) -> Result<Self, anyhow::Error> {
        match StringOrSecret::try_from_value(v)? {
            StringOrSecret::Secret(id) => Ok(Secret(id)),
            _ => bail!("must provide a secret value"),
        }
    }
}

impl ImpliedValue for Secret {
    fn implied_value() -> Result<Self, anyhow::Error> {
        bail!("must provide a secret value")
    }
}

impl TryFromValue<WithOptionValue<Aug>> for StringOrSecret {
    fn try_from_value(v: WithOptionValue<Aug>) -> Result<Self, anyhow::Error> {
        Ok(match v {
            WithOptionValue::Secret(ResolvedObjectName::Object { id, .. }) => {
                StringOrSecret::Secret(id)
            }
            v => StringOrSecret::String(String::try_from_value(v)?),
        })
    }
}

impl ImpliedValue for StringOrSecret {
    fn implied_value() -> Result<Self, anyhow::Error> {
        bail!("must provide a string or secret value")
    }
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

impl From<Interval> for OptionalInterval {
    fn from(i: Interval) -> OptionalInterval {
        // An interval of 0 disables the setting.
        let inner = if i == Interval::default() {
            None
        } else {
            Some(i)
        };
        OptionalInterval(inner)
    }
}

impl TryFromValue<Value> for OptionalInterval {
    fn try_from_value(v: Value) -> Result<Self, anyhow::Error> {
        Ok(match v {
            Value::Null => OptionalInterval(None),
            v => Interval::try_from_value(v)?.into(),
        })
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

impl<T: AstInfo, V: TryFromValue<WithOptionValue<T>>> TryFromValue<WithOptionValue<T>>
    for Option<V>
{
    fn try_from_value(v: WithOptionValue<T>) -> Result<Self, anyhow::Error> {
        Ok(Some(V::try_from_value(v)?))
    }
}

impl<V: ImpliedValue> ImpliedValue for Option<V> {
    fn implied_value() -> Result<Self, anyhow::Error> {
        Ok(Some(V::implied_value()?))
    }
}

impl<V: TryFromValue<Value>, T: AstInfo + std::fmt::Debug> TryFromValue<WithOptionValue<T>> for V {
    fn try_from_value(v: WithOptionValue<T>) -> Result<Self, anyhow::Error> {
        match v {
            WithOptionValue::Value(v) => V::try_from_value(v),
            WithOptionValue::Ident(i) => V::try_from_value(Value::String(i.to_string())),
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

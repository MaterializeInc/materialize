// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Tooling to handle `WITH` options and other, similar constructs.

use anyhow::bail;
use serde::{Deserialize, Serialize};

use mz_repr::adt::interval::Interval;
use mz_repr::strconv;

use crate::ast::{AstInfo, IntervalValue, Value, WithOptionValue};

pub trait ProcessOption<T>: Sized {
    type Error;
    fn try_from_value(v: T) -> Result<Self, Self::Error>;
}

impl ProcessOption<Value> for Interval {
    type Error = anyhow::Error;

    fn try_from_value(v: Value) -> Result<Self, Self::Error> {
        Ok(match v {
            Value::Interval(IntervalValue { value, .. })
            | Value::Number(value)
            | Value::String(value) => strconv::parse_interval(&value)?,
            _ => bail!("cannot use value as interval"),
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Hash, Deserialize)]
pub struct OptionalInterval(pub Option<Interval>);

impl ProcessOption<Value> for OptionalInterval {
    type Error = anyhow::Error;

    fn try_from_value(v: Value) -> Result<Self, Self::Error> {
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

impl ProcessOption<Value> for String {
    type Error = anyhow::Error;

    fn try_from_value(v: Value) -> Result<Self, Self::Error> {
        match v {
            Value::String(v) => Ok(v),
            _ => anyhow::bail!("cannot use value as string"),
        }
    }
}

impl ProcessOption<Value> for bool {
    type Error = anyhow::Error;

    fn try_from_value(v: Value) -> Result<Self, Self::Error> {
        match v {
            Value::Boolean(v) => Ok(v),
            _ => anyhow::bail!("cannot use value as boolean"),
        }
    }
}

impl ProcessOption<Value> for i32 {
    type Error = anyhow::Error;
    fn try_from_value(v: Value) -> Result<Self, Self::Error> {
        match v {
            Value::Number(v) => v
                .parse::<i32>()
                .map_err(|_| anyhow::anyhow!("invalid numeric value")),
            _ => anyhow::bail!("cannot use value as number"),
        }
    }
}

impl<V: ProcessOption<Value, Error = anyhow::Error>> ProcessOption<Value> for Vec<V> {
    type Error = anyhow::Error;
    fn try_from_value(v: Value) -> Result<Self, Self::Error> {
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

impl<V: ProcessOption<Value, Error = anyhow::Error>, T: AstInfo> ProcessOption<WithOptionValue<T>>
    for V
{
    type Error = anyhow::Error;
    fn try_from_value(v: WithOptionValue<T>) -> Result<Self, Self::Error> {
        match v {
            WithOptionValue::Value(v) => V::try_from_value(v),
            _ => bail!("incompatible value types"),
        }
    }
}

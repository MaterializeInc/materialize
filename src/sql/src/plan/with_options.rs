// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Provides tooling to handle `WITH` options.

use mz_sql_parser::ast::KafkaBroker;
use mz_sql_parser::ast::ReplicaDefinition;
use serde::{Deserialize, Serialize};

use mz_repr::adt::interval::Interval;
use mz_repr::strconv;
use mz_repr::GlobalId;
use mz_storage_client::types::connections::StringOrSecret;

use crate::ast::{AstInfo, IntervalValue, Value, WithOptionValue};
use crate::names::{ResolvedDataType, ResolvedObjectName};
use crate::plan::{Aug, PlanError};

pub trait TryFromValue<T>: Sized {
    fn try_from_value(v: T) -> Result<Self, PlanError>;
    fn name() -> String;
}

pub trait ImpliedValue: Sized {
    fn implied_value() -> Result<Self, PlanError>;
}

#[derive(Copy, Clone, Debug)]
pub struct Secret(GlobalId);

impl From<Secret> for GlobalId {
    fn from(secret: Secret) -> Self {
        secret.0
    }
}

impl TryFromValue<WithOptionValue<Aug>> for Secret {
    fn try_from_value(v: WithOptionValue<Aug>) -> Result<Self, PlanError> {
        match StringOrSecret::try_from_value(v)? {
            StringOrSecret::Secret(id) => Ok(Secret(id)),
            _ => sql_bail!("must provide a secret value"),
        }
    }
    fn name() -> String {
        "secret".to_string()
    }
}

impl ImpliedValue for Secret {
    fn implied_value() -> Result<Self, PlanError> {
        sql_bail!("must provide a secret value")
    }
}

#[derive(Copy, Clone, Debug)]
pub struct Object(GlobalId);

impl From<Object> for GlobalId {
    fn from(obj: Object) -> Self {
        obj.0
    }
}

impl From<&Object> for GlobalId {
    fn from(obj: &Object) -> Self {
        obj.0
    }
}

impl TryFromValue<WithOptionValue<Aug>> for Object {
    fn try_from_value(v: WithOptionValue<Aug>) -> Result<Self, PlanError> {
        Ok(match v {
            WithOptionValue::Object(ResolvedObjectName::Object { id, .. }) => Object(id),
            _ => sql_bail!("must provide an object"),
        })
    }
    fn name() -> String {
        "object reference".to_string()
    }
}

impl ImpliedValue for Object {
    fn implied_value() -> Result<Self, PlanError> {
        sql_bail!("must provide an object")
    }
}

impl TryFromValue<WithOptionValue<Aug>> for ResolvedDataType {
    fn try_from_value(v: WithOptionValue<Aug>) -> Result<Self, PlanError> {
        Ok(match v {
            WithOptionValue::DataType(ty) => ty,
            _ => sql_bail!("must provide a data type"),
        })
    }
    fn name() -> String {
        "data type".to_string()
    }
}

impl ImpliedValue for ResolvedDataType {
    fn implied_value() -> Result<Self, PlanError> {
        sql_bail!("must provide a data type")
    }
}

impl TryFromValue<WithOptionValue<Aug>> for StringOrSecret {
    fn try_from_value(v: WithOptionValue<Aug>) -> Result<Self, PlanError> {
        Ok(match v {
            WithOptionValue::Secret(ResolvedObjectName::Object { id, .. }) => {
                StringOrSecret::Secret(id)
            }
            v => StringOrSecret::String(String::try_from_value(v)?),
        })
    }
    fn name() -> String {
        "string or secret".to_string()
    }
}

impl ImpliedValue for StringOrSecret {
    fn implied_value() -> Result<Self, PlanError> {
        sql_bail!("must provide a string or secret value")
    }
}

impl TryFromValue<Value> for Interval {
    fn try_from_value(v: Value) -> Result<Self, PlanError> {
        Ok(match v {
            Value::Interval(IntervalValue { value, .. })
            | Value::Number(value)
            | Value::String(value) => strconv::parse_interval(&value)?,
            _ => sql_bail!("cannot use value as interval"),
        })
    }
    fn name() -> String {
        "interval".to_string()
    }
}

impl ImpliedValue for Interval {
    fn implied_value() -> Result<Self, PlanError> {
        sql_bail!("must provide an interval value")
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
    fn try_from_value(v: Value) -> Result<Self, PlanError> {
        Ok(match v {
            Value::Null => OptionalInterval(None),
            v => Interval::try_from_value(v)?.into(),
        })
    }
    fn name() -> String {
        "optional interval".to_string()
    }
}

impl ImpliedValue for OptionalInterval {
    fn implied_value() -> Result<Self, PlanError> {
        sql_bail!("must provide an interval value")
    }
}

impl TryFromValue<Value> for String {
    fn try_from_value(v: Value) -> Result<Self, PlanError> {
        match v {
            Value::String(v) => Ok(v),
            _ => sql_bail!("cannot use value as string"),
        }
    }
    fn name() -> String {
        "text".to_string()
    }
}

impl ImpliedValue for String {
    fn implied_value() -> Result<Self, PlanError> {
        sql_bail!("must provide a string value")
    }
}

impl TryFromValue<Value> for bool {
    fn try_from_value(v: Value) -> Result<Self, PlanError> {
        match v {
            Value::Boolean(v) => Ok(v),
            _ => sql_bail!("cannot use value as boolean"),
        }
    }
    fn name() -> String {
        "bool".to_string()
    }
}

impl ImpliedValue for bool {
    fn implied_value() -> Result<Self, PlanError> {
        Ok(true)
    }
}

impl TryFromValue<Value> for f64 {
    fn try_from_value(v: Value) -> Result<Self, PlanError> {
        match v {
            Value::Number(v) => v
                .parse::<f64>()
                .map_err(|e| sql_err!("invalid numeric value: {e}")),
            _ => sql_bail!("cannot use value as number"),
        }
    }

    fn name() -> String {
        "float8".to_string()
    }
}

impl ImpliedValue for f64 {
    fn implied_value() -> Result<Self, PlanError> {
        sql_bail!("must provide a float value")
    }
}

impl TryFromValue<Value> for i32 {
    fn try_from_value(v: Value) -> Result<Self, PlanError> {
        match v {
            Value::Number(v) => v
                .parse::<i32>()
                .map_err(|e| sql_err!("invalid numeric value: {e}")),
            _ => sql_bail!("cannot use value as number"),
        }
    }
    fn name() -> String {
        "int".to_string()
    }
}

impl ImpliedValue for i32 {
    fn implied_value() -> Result<Self, PlanError> {
        sql_bail!("must provide an integer value")
    }
}

impl TryFromValue<Value> for i64 {
    fn try_from_value(v: Value) -> Result<Self, PlanError> {
        match v {
            Value::Number(v) => v
                .parse::<i64>()
                .map_err(|e| sql_err!("invalid numeric value: {e}")),
            _ => sql_bail!("cannot use value as number"),
        }
    }
    fn name() -> String {
        "int8".to_string()
    }
}

impl ImpliedValue for i64 {
    fn implied_value() -> Result<Self, PlanError> {
        sql_bail!("must provide an integer value")
    }
}

impl TryFromValue<Value> for u16 {
    fn try_from_value(v: Value) -> Result<Self, PlanError> {
        match v {
            Value::Number(v) => v
                .parse::<u16>()
                .map_err(|e| sql_err!("invalid numeric value: {e}")),
            _ => sql_bail!("cannot use value as number"),
        }
    }
    fn name() -> String {
        "uint2".to_string()
    }
}

impl ImpliedValue for u16 {
    fn implied_value() -> Result<Self, PlanError> {
        sql_bail!("must provide an integer value")
    }
}

impl TryFromValue<Value> for u64 {
    fn try_from_value(v: Value) -> Result<Self, PlanError> {
        match v {
            Value::Number(v) => v
                .parse::<u64>()
                .map_err(|e| sql_err!("invalid unsigned numeric value: {e}")),
            _ => sql_bail!("cannot use value as number"),
        }
    }
    fn name() -> String {
        "uint8".to_string()
    }
}

impl ImpliedValue for u64 {
    fn implied_value() -> Result<Self, PlanError> {
        sql_bail!("must provide an unsigned integer value")
    }
}

impl<T, V> TryFromValue<WithOptionValue<T>> for Vec<V>
where
    T: AstInfo,
    V: TryFromValue<WithOptionValue<T>>,
{
    fn try_from_value(v: WithOptionValue<T>) -> Result<Self, PlanError> {
        match v {
            WithOptionValue::Sequence(a) => {
                let mut out = Vec::with_capacity(a.len());
                for i in a {
                    out.push(
                        V::try_from_value(i)
                            .map_err(|_| anyhow::anyhow!("cannot use value in array"))?,
                    )
                }
                Ok(out)
            }
            _ => sql_bail!("cannot use value as array"),
        }
    }
    fn name() -> String {
        format!("array of {}", V::name())
    }
}

impl<V: TryFromValue<Value>> ImpliedValue for Vec<V> {
    fn implied_value() -> Result<Self, PlanError> {
        sql_bail!("must provide an array value")
    }
}

impl<T: AstInfo, V: TryFromValue<WithOptionValue<T>>> TryFromValue<WithOptionValue<T>>
    for Option<V>
{
    fn try_from_value(v: WithOptionValue<T>) -> Result<Self, PlanError> {
        Ok(Some(V::try_from_value(v)?))
    }

    fn name() -> String {
        format!("optional {}", V::name())
    }
}

impl<V: ImpliedValue> ImpliedValue for Option<V> {
    fn implied_value() -> Result<Self, PlanError> {
        Ok(Some(V::implied_value()?))
    }
}

impl<V: TryFromValue<Value>, T: AstInfo + std::fmt::Debug> TryFromValue<WithOptionValue<T>> for V {
    fn try_from_value(v: WithOptionValue<T>) -> Result<Self, PlanError> {
        match v {
            WithOptionValue::Value(v) => V::try_from_value(v),
            WithOptionValue::Ident(i) => V::try_from_value(Value::String(i.into_string())),
            WithOptionValue::Sequence(_)
            | WithOptionValue::Object(_)
            | WithOptionValue::Secret(_)
            | WithOptionValue::DataType(_)
            | WithOptionValue::ClusterReplicas(_)
            | WithOptionValue::ConnectionKafkaBroker(_) => sql_bail!(
                "incompatible value types: cannot convert {} to {}",
                match v {
                    WithOptionValue::Sequence(_) => "sequences",
                    WithOptionValue::Object(_) => "object references",
                    WithOptionValue::Secret(_) => "secrets",
                    WithOptionValue::DataType(_) => "data types",
                    WithOptionValue::ClusterReplicas(_) => "cluster replicas",
                    WithOptionValue::ConnectionKafkaBroker(_) => "connection kafka brokers",
                    _ => unreachable!(),
                },
                V::name()
            ),
        }
    }
    fn name() -> String {
        V::name()
    }
}

impl<T, V: TryFromValue<T> + ImpliedValue> TryFromValue<Option<T>> for V {
    fn try_from_value(v: Option<T>) -> Result<Self, PlanError> {
        match v {
            Some(v) => V::try_from_value(v),
            None => V::implied_value(),
        }
    }
    fn name() -> String {
        V::name()
    }
}

impl TryFromValue<WithOptionValue<Aug>> for Vec<ReplicaDefinition<Aug>> {
    fn try_from_value(v: WithOptionValue<Aug>) -> Result<Self, PlanError> {
        match v {
            WithOptionValue::ClusterReplicas(replicas) => Ok(replicas),
            _ => sql_bail!("cannot use value as cluster replicas"),
        }
    }
    fn name() -> String {
        "cluster replicas".to_string()
    }
}

impl ImpliedValue for Vec<ReplicaDefinition<Aug>> {
    fn implied_value() -> Result<Self, PlanError> {
        sql_bail!("must provide a set of cluster replicas")
    }
}

impl TryFromValue<WithOptionValue<Aug>> for Vec<KafkaBroker<Aug>> {
    fn try_from_value(v: WithOptionValue<Aug>) -> Result<Self, PlanError> {
        let mut out = vec![];
        match v {
            WithOptionValue::ConnectionKafkaBroker(broker) => {
                out.push(broker);
            }
            WithOptionValue::Sequence(values) => {
                for value in values {
                    out.extend(Self::try_from_value(value)?);
                }
            }
            _ => sql_bail!("cannot use value as a kafka broker"),
        }
        Ok(out)
    }
    fn name() -> String {
        "kafka broker".to_string()
    }
}

impl ImpliedValue for Vec<KafkaBroker<Aug>> {
    fn implied_value() -> Result<Self, PlanError> {
        sql_bail!("must provide a kafka broker")
    }
}

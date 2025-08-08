// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::any::Any;
use std::borrow::Cow;
use std::fmt::{self, Debug};
use std::str::FromStr;
use std::time::Duration;

use chrono::{DateTime, Utc};
use ipnet::IpNet;
use itertools::Itertools;
use mz_pgwire_common::Severity;
use mz_repr::adt::numeric::Numeric;
use mz_repr::adt::timestamp::CheckedTimestamp;
use mz_repr::strconv;
use mz_rocksdb_types::config::{CompactionStyle, CompressionType};
use mz_sql_parser::ast::{Ident, TransactionIsolationLevel};
use mz_tracing::{CloneableEnvFilter, SerializableDirective};
use serde::{Deserialize, Serialize};
use uncased::UncasedStr;

use super::VarInput;
use super::errors::VarParseError;

/// Defines a value that get stored as part of a System or Session variable.
///
/// This trait is partially object safe, see [`VarDefinition`] for more details.
///
/// [`VarDefinition`]: crate::session::vars::definitions::VarDefinition
pub trait Value: Any + AsAny + Debug + Send + Sync {
    fn type_name() -> Cow<'static, str>
    where
        Self: Sized;

    fn parse(input: VarInput) -> Result<Self, VarParseError>
    where
        Self: Sized;

    fn format(&self) -> String;

    fn box_clone(&self) -> Box<dyn Value>;

    /// Parse an instance of `Self` from [`VarInput`], returning it as a `Box<dyn Value>`.
    fn parse_dyn_value(input: VarInput) -> Result<Box<dyn Value>, VarParseError>
    where
        Self: Sized,
    {
        Self::parse(input).map(|val| {
            let dyn_val: Box<dyn Value> = Box::new(val);
            dyn_val
        })
    }
}

// Note(parkmycar): We have a blanket impl for `PartialEq` instead of requiring it as a trait
// bound because otherwise it's tricky to make `Value` object safe.
impl PartialEq for Box<dyn Value> {
    fn eq(&self, other: &Box<dyn Value>) -> bool {
        self.format().eq(&other.format())
    }
}
impl Eq for Box<dyn Value> {}

impl<'a> PartialEq for &'a dyn Value {
    fn eq(&self, other: &&dyn Value) -> bool {
        self.format().eq(&other.format())
    }
}
impl<'a> Eq for &'a dyn Value {}

/// Helper trait to cast a `&dyn T` to a `&dyn Any`.
///
/// In Rust all types that are `'static` implement [`std::any::Any`] and thus can be casted to a
/// `&dyn Any`. But once you create a trait object, the type is erased and thus so is the
/// implementation for [`Any`]. This trait essentially adds a types' [`Any`] impl to the vtable
/// created when casted to trait object, if [`AsAny`] is a supertrait.
///
/// See [`Value`] for an example of using [`AsAny`].
pub trait AsAny {
    fn as_any(&self) -> &dyn Any;
}

impl<T: Any> AsAny for T {
    fn as_any(&self) -> &dyn Any {
        self
    }
}

/// Helper method to extract a single value from any kind of [`VarInput`].
fn extract_single_value(input: VarInput<'_>) -> Result<&str, VarParseError> {
    match input {
        VarInput::Flat(value) => Ok(value),
        VarInput::SqlSet([value]) => Ok(value),
        VarInput::SqlSet(values) => Err(VarParseError::InvalidParameterValue {
            invalid_values: values.to_vec(),
            reason: "expects a single value".into(),
        }),
    }
}

impl<V: Value + Clone> Value for Option<V> {
    fn type_name() -> Cow<'static, str>
    where
        Self: Sized,
    {
        format!("optional {}", V::type_name()).into()
    }

    fn parse(input: VarInput) -> Result<Self, VarParseError>
    where
        Self: Sized,
    {
        let s = extract_single_value(input)?;
        match s {
            "" => Ok(None),
            _ => <V as Value>::parse(VarInput::Flat(s)).map(Some),
        }
    }

    fn box_clone(&self) -> Box<dyn Value> {
        Box::new(self.clone())
    }

    fn format(&self) -> String {
        match self {
            Some(s) => s.format(),
            None => "".to_string(),
        }
    }
}

impl Value for String {
    fn type_name() -> Cow<'static, str>
    where
        Self: Sized,
    {
        "string".into()
    }

    fn parse(input: VarInput<'_>) -> Result<Self, VarParseError>
    where
        Self: Sized,
    {
        let s = extract_single_value(input)?;
        Ok(s.to_string())
    }

    fn box_clone(&self) -> Box<dyn Value> {
        Box::new(self.clone())
    }

    fn format(&self) -> String {
        self.to_string()
    }
}

impl Value for Cow<'static, str> {
    fn type_name() -> Cow<'static, str>
    where
        Self: Sized,
    {
        "string".into()
    }

    fn parse(input: VarInput<'_>) -> Result<Self, VarParseError>
    where
        Self: Sized,
    {
        let s = extract_single_value(input)?;
        Ok(s.to_string().into())
    }

    fn box_clone(&self) -> Box<dyn Value> {
        Box::new(self.clone())
    }

    fn format(&self) -> String {
        self.to_string()
    }
}

impl Value for bool {
    fn type_name() -> Cow<'static, str>
    where
        Self: Sized,
    {
        "boolean".into()
    }

    fn parse(input: VarInput<'_>) -> Result<Self, VarParseError>
    where
        Self: Sized,
    {
        let s = extract_single_value(input)?;
        match s {
            "t" | "true" | "on" => Ok(true),
            "f" | "false" | "off" => Ok(false),
            _ => Err(VarParseError::InvalidParameterType),
        }
    }

    fn box_clone(&self) -> Box<dyn Value> {
        Box::new(self.clone())
    }

    fn format(&self) -> String {
        match self {
            true => "on".into(),
            false => "off".into(),
        }
    }
}

impl Value for Option<CheckedTimestamp<DateTime<Utc>>> {
    fn type_name() -> Cow<'static, str>
    where
        Self: Sized,
    {
        "timestamptz".into()
    }

    fn parse(input: VarInput<'_>) -> Result<Self, VarParseError>
    where
        Self: Sized,
    {
        let s = extract_single_value(input)?;
        strconv::parse_timestamptz(s)
            .map_err(|_| VarParseError::InvalidParameterType)
            .map(Some)
    }

    fn box_clone(&self) -> Box<dyn Value> {
        Box::new(self.clone())
    }

    fn format(&self) -> String {
        self.map(|t| t.to_string()).unwrap_or_default()
    }
}

impl Value for Numeric {
    fn type_name() -> Cow<'static, str>
    where
        Self: Sized,
    {
        "numeric".into()
    }

    fn parse(input: VarInput<'_>) -> Result<Self, VarParseError>
    where
        Self: Sized,
    {
        let s = extract_single_value(input)?;
        s.parse::<Numeric>()
            .map_err(|_| VarParseError::InvalidParameterType)
    }

    fn box_clone(&self) -> Box<dyn Value> {
        Box::new(self.clone())
    }

    fn format(&self) -> String {
        self.to_standard_notation_string()
    }
}

const SEC_TO_MIN: u64 = 60u64;
const SEC_TO_HOUR: u64 = 60u64 * 60;
const SEC_TO_DAY: u64 = 60u64 * 60 * 24;
const MICRO_TO_MILLI: u32 = 1000u32;

impl Value for Duration {
    fn type_name() -> Cow<'static, str>
    where
        Self: Sized,
    {
        "duration".into()
    }

    fn parse(input: VarInput<'_>) -> Result<Self, VarParseError>
    where
        Self: Sized,
    {
        let s = extract_single_value(input)?;
        let s = s.trim();
        // Take all numeric values from [0..]
        let split_pos = s
            .chars()
            .position(|p| !char::is_numeric(p))
            .unwrap_or_else(|| s.chars().count());

        // Error if the numeric values don't parse, i.e. there aren't any.
        let d = s[..split_pos]
            .parse::<u64>()
            .map_err(|_| VarParseError::InvalidParameterType)?;

        // We've already trimmed end
        let (f, m): (fn(u64) -> Duration, u64) = match s[split_pos..].trim_start() {
            "us" => (Duration::from_micros, 1),
            // Default unit is milliseconds
            "ms" | "" => (Duration::from_millis, 1),
            "s" => (Duration::from_secs, 1),
            "min" => (Duration::from_secs, SEC_TO_MIN),
            "h" => (Duration::from_secs, SEC_TO_HOUR),
            "d" => (Duration::from_secs, SEC_TO_DAY),
            o => {
                return Err(VarParseError::InvalidParameterValue {
                    invalid_values: vec![o.to_string()],
                    reason: "expected us, ms, s, min, h, or d but got {o:?}".to_string(),
                });
            }
        };

        let d = f(d
            .checked_mul(m)
            .ok_or_else(|| VarParseError::InvalidParameterValue {
                invalid_values: vec![s.to_string()],
                reason: "expected value to fit in u64".into(),
            })?);
        Ok(d)
    }

    fn box_clone(&self) -> Box<dyn Value> {
        Box::new(self.clone())
    }

    // The strategy for formatting these strings is to find the least
    // significant unit of time that can be printed as an integer––we know this
    // is always possible because the input can only be an integer of a single
    // unit of time.
    fn format(&self) -> String {
        let micros = self.subsec_micros();
        if micros > 0 {
            match micros {
                ms if ms != 0 && ms % MICRO_TO_MILLI == 0 => {
                    format!(
                        "{} ms",
                        self.as_secs() * 1000 + u64::from(ms / MICRO_TO_MILLI)
                    )
                }
                us => format!("{} us", self.as_secs() * 1_000_000 + u64::from(us)),
            }
        } else {
            match self.as_secs() {
                zero if zero == u64::MAX => "0".to_string(),
                d if d != 0 && d % SEC_TO_DAY == 0 => format!("{} d", d / SEC_TO_DAY),
                h if h != 0 && h % SEC_TO_HOUR == 0 => format!("{} h", h / SEC_TO_HOUR),
                m if m != 0 && m % SEC_TO_MIN == 0 => format!("{} min", m / SEC_TO_MIN),
                s => format!("{} s", s),
            }
        }
    }
}

impl Value for serde_json::Value {
    fn type_name() -> Cow<'static, str>
    where
        Self: Sized,
    {
        "jsonb".into()
    }

    fn parse(input: VarInput<'_>) -> Result<Self, VarParseError>
    where
        Self: Sized,
    {
        let s = extract_single_value(input)?;
        serde_json::from_str(s).map_err(|_| VarParseError::InvalidParameterType)
    }

    fn box_clone(&self) -> Box<dyn Value> {
        Box::new(self.clone())
    }

    fn format(&self) -> String {
        self.to_string()
    }
}

/// This style should actually be some more complex struct, but we only support this configuration
/// of it, so this is fine for the time being.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct DateStyle(pub [&'static str; 2]);

pub static DEFAULT_DATE_STYLE: DateStyle = DateStyle(["ISO", "MDY"]);

impl Value for DateStyle {
    fn type_name() -> Cow<'static, str>
    where
        Self: Sized,
    {
        "string list".into()
    }

    /// This impl is unlike most others because we have under-implemented its backing struct.
    fn parse(input: VarInput<'_>) -> Result<Self, VarParseError>
    where
        Self: Sized,
    {
        let input = match input {
            VarInput::Flat(v) => mz_sql_parser::parser::split_identifier_string(v)
                .map_err(|_| VarParseError::InvalidParameterType)?,
            // Unlike parsing `Vec<Ident>`, we further split each element.
            // This matches PostgreSQL.
            VarInput::SqlSet(values) => {
                let mut out = vec![];
                for v in values {
                    let idents = mz_sql_parser::parser::split_identifier_string(v)
                        .map_err(|_| VarParseError::InvalidParameterType)?;
                    out.extend(idents)
                }
                out
            }
        };

        for input in input {
            if !DEFAULT_DATE_STYLE
                .0
                .iter()
                .any(|valid| UncasedStr::new(valid) == &input)
            {
                return Err(VarParseError::FixedValueParameter);
            }
        }

        Ok(DEFAULT_DATE_STYLE.clone())
    }

    fn box_clone(&self) -> Box<dyn Value> {
        Box::new(self.clone())
    }

    fn format(&self) -> String {
        self.0.join(", ")
    }
}

impl Value for Vec<Ident> {
    fn type_name() -> Cow<'static, str>
    where
        Self: Sized,
    {
        "identifier list".into()
    }

    fn parse(input: VarInput<'_>) -> Result<Self, VarParseError>
    where
        Self: Sized,
    {
        let holder;
        let values = match input {
            VarInput::Flat(value) => {
                holder = mz_sql_parser::parser::split_identifier_string(value)
                    .map_err(|_| VarParseError::InvalidParameterType)?;
                &holder
            }
            // Unlike parsing `Vec<String>`, we do *not* further split each
            // element. This matches PostgreSQL.
            VarInput::SqlSet(values) => values,
        };
        let values = values
            .iter()
            .map(Ident::new)
            .collect::<Result<_, _>>()
            .map_err(|e| VarParseError::InvalidParameterValue {
                invalid_values: values.to_vec(),
                reason: e.to_string(),
            })?;
        Ok(values)
    }

    fn box_clone(&self) -> Box<dyn Value> {
        Box::new(self.clone())
    }

    fn format(&self) -> String {
        self.iter().map(|ident| ident.to_string()).join(", ")
    }
}

impl Value for Vec<IpNet> {
    fn type_name() -> Cow<'static, str>
    where
        Self: Sized,
    {
        "CIDR list".into()
    }

    fn parse(input: VarInput<'_>) -> Result<Self, VarParseError>
    where
        Self: Sized,
    {
        let values = input.to_vec();
        let values: Vec<IpNet> = values
            .iter()
            .flat_map(|i| i.split(','))
            .map(|d| IpNet::from_str(d.trim()))
            .collect::<Result<_, _>>()
            .map_err(|e| VarParseError::InvalidParameterValue {
                invalid_values: values,
                reason: e.to_string(),
            })?;
        Ok(values)
    }

    fn box_clone(&self) -> Box<dyn Value> {
        Box::new(self.clone())
    }

    fn format(&self) -> String {
        self.iter().map(|ident| ident.to_string()).join(", ")
    }
}

impl Value for Vec<SerializableDirective> {
    fn type_name() -> Cow<'static, str>
    where
        Self: Sized,
    {
        "directive list".into()
    }

    fn parse(input: VarInput<'_>) -> Result<Self, VarParseError>
    where
        Self: Sized,
    {
        let values = input.to_vec();
        let dirs: Result<_, _> = values
            .iter()
            .flat_map(|i| i.split(','))
            .map(|d| SerializableDirective::from_str(d.trim()))
            .collect();
        dirs.map_err(|e| VarParseError::InvalidParameterValue {
            invalid_values: values.to_vec(),
            reason: e.to_string(),
        })
    }

    fn box_clone(&self) -> Box<dyn Value> {
        Box::new(self.clone())
    }

    fn format(&self) -> String {
        self.iter().map(|d| d.to_string()).join(", ")
    }
}

// This unorthodox design lets us escape complex errors from value parsing.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Failpoints;

impl Value for Failpoints {
    fn type_name() -> Cow<'static, str>
    where
        Self: Sized,
    {
        "failpoints config".into()
    }

    fn parse(input: VarInput<'_>) -> Result<Self, VarParseError>
    where
        Self: Sized,
    {
        let values = input.to_vec();
        for mut cfg in values.iter().map(|v| v.trim().split(';')).flatten() {
            cfg = cfg.trim();
            if cfg.is_empty() {
                continue;
            }
            let mut splits = cfg.splitn(2, '=');
            let failpoint = splits
                .next()
                .ok_or_else(|| VarParseError::InvalidParameterValue {
                    invalid_values: input.to_vec(),
                    reason: "missing failpoint name".into(),
                })?;
            let action = splits
                .next()
                .ok_or_else(|| VarParseError::InvalidParameterValue {
                    invalid_values: input.to_vec(),
                    reason: "missing failpoint action".into(),
                })?;
            fail::cfg(failpoint, action).map_err(|e| VarParseError::InvalidParameterValue {
                invalid_values: input.to_vec(),
                reason: e.to_string(),
            })?;
        }

        Ok(Failpoints)
    }

    fn box_clone(&self) -> Box<dyn Value> {
        Box::new(self.clone())
    }

    fn format(&self) -> String {
        "<omitted>".to_string()
    }
}

/// Severity levels can used to be used to filter which messages get sent
/// to a client.
///
/// The ordering of severity levels used for client-level filtering differs from the
/// one used for server-side logging in two aspects: INFO messages are always sent,
/// and the LOG severity is considered as below NOTICE, while it is above ERROR for
/// server-side logs.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ClientSeverity {
    /// Sends only INFO, ERROR, FATAL and PANIC level messages.
    Error,
    /// Sends only WARNING, INFO, ERROR, FATAL and PANIC level messages.
    Warning,
    /// Sends only NOTICE, WARNING, INFO, ERROR, FATAL and PANIC level messages.
    Notice,
    /// Sends only LOG, NOTICE, WARNING, INFO, ERROR, FATAL and PANIC level messages.
    Log,
    /// Sends all messages to the client, since all DEBUG levels are treated as the same right now.
    Debug1,
    /// Sends all messages to the client, since all DEBUG levels are treated as the same right now.
    Debug2,
    /// Sends all messages to the client, since all DEBUG levels are treated as the same right now.
    Debug3,
    /// Sends all messages to the client, since all DEBUG levels are treated as the same right now.
    Debug4,
    /// Sends all messages to the client, since all DEBUG levels are treated as the same right now.
    Debug5,
    /// Sends only NOTICE, WARNING, INFO, ERROR, FATAL and PANIC level messages.
    /// Not listed as a valid value, but accepted by Postgres
    Info,
}

impl Serialize for ClientSeverity {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_str(self.as_str())
    }
}

impl ClientSeverity {
    fn as_str(&self) -> &'static str {
        match self {
            ClientSeverity::Error => "error",
            ClientSeverity::Warning => "warning",
            ClientSeverity::Notice => "notice",
            ClientSeverity::Info => "info",
            ClientSeverity::Log => "log",
            ClientSeverity::Debug1 => "debug1",
            ClientSeverity::Debug2 => "debug2",
            ClientSeverity::Debug3 => "debug3",
            ClientSeverity::Debug4 => "debug4",
            ClientSeverity::Debug5 => "debug5",
        }
    }

    fn valid_values() -> Vec<&'static str> {
        // INFO left intentionally out, to match Postgres
        vec![
            ClientSeverity::Debug5.as_str(),
            ClientSeverity::Debug4.as_str(),
            ClientSeverity::Debug3.as_str(),
            ClientSeverity::Debug2.as_str(),
            ClientSeverity::Debug1.as_str(),
            ClientSeverity::Log.as_str(),
            ClientSeverity::Notice.as_str(),
            ClientSeverity::Warning.as_str(),
            ClientSeverity::Error.as_str(),
        ]
    }

    /// Checks if a message of a given severity level should be sent to a client.
    ///
    /// The ordering of severity levels used for client-level filtering differs from the
    /// one used for server-side logging in two aspects: INFO messages are always sent,
    /// and the LOG severity is considered as below NOTICE, while it is above ERROR for
    /// server-side logs.
    ///
    /// Postgres only considers the session setting after the client authentication
    /// handshake is completed. Since this function is only called after client authentication
    /// is done, we are not treating this case right now, but be aware if refactoring it.
    pub fn should_output_to_client(&self, severity: &Severity) -> bool {
        match (self, severity) {
            // INFO messages are always sent
            (_, Severity::Info) => true,
            (ClientSeverity::Error, Severity::Error | Severity::Fatal | Severity::Panic) => true,
            (
                ClientSeverity::Warning,
                Severity::Error | Severity::Fatal | Severity::Panic | Severity::Warning,
            ) => true,
            (
                ClientSeverity::Notice,
                Severity::Error
                | Severity::Fatal
                | Severity::Panic
                | Severity::Warning
                | Severity::Notice,
            ) => true,
            (
                ClientSeverity::Info,
                Severity::Error
                | Severity::Fatal
                | Severity::Panic
                | Severity::Warning
                | Severity::Notice,
            ) => true,
            (
                ClientSeverity::Log,
                Severity::Error
                | Severity::Fatal
                | Severity::Panic
                | Severity::Warning
                | Severity::Notice
                | Severity::Log,
            ) => true,
            (
                ClientSeverity::Debug1
                | ClientSeverity::Debug2
                | ClientSeverity::Debug3
                | ClientSeverity::Debug4
                | ClientSeverity::Debug5,
                _,
            ) => true,

            (
                ClientSeverity::Error,
                Severity::Warning | Severity::Notice | Severity::Log | Severity::Debug,
            ) => false,
            (ClientSeverity::Warning, Severity::Notice | Severity::Log | Severity::Debug) => false,
            (ClientSeverity::Notice, Severity::Log | Severity::Debug) => false,
            (ClientSeverity::Info, Severity::Log | Severity::Debug) => false,
            (ClientSeverity::Log, Severity::Debug) => false,
        }
    }
}

impl Value for ClientSeverity {
    fn type_name() -> Cow<'static, str>
    where
        Self: Sized,
    {
        "string".into()
    }

    fn parse(input: VarInput<'_>) -> Result<Self, VarParseError>
    where
        Self: Sized,
    {
        let s = extract_single_value(input)?;
        let s = UncasedStr::new(s);

        if s == ClientSeverity::Error.as_str() {
            Ok(ClientSeverity::Error)
        } else if s == ClientSeverity::Warning.as_str() {
            Ok(ClientSeverity::Warning)
        } else if s == ClientSeverity::Notice.as_str() {
            Ok(ClientSeverity::Notice)
        } else if s == ClientSeverity::Info.as_str() {
            Ok(ClientSeverity::Info)
        } else if s == ClientSeverity::Log.as_str() {
            Ok(ClientSeverity::Log)
        } else if s == ClientSeverity::Debug1.as_str() {
            Ok(ClientSeverity::Debug1)
        // Postgres treats `debug` as an input as equivalent to `debug2`
        } else if s == ClientSeverity::Debug2.as_str() || s == "debug" {
            Ok(ClientSeverity::Debug2)
        } else if s == ClientSeverity::Debug3.as_str() {
            Ok(ClientSeverity::Debug3)
        } else if s == ClientSeverity::Debug4.as_str() {
            Ok(ClientSeverity::Debug4)
        } else if s == ClientSeverity::Debug5.as_str() {
            Ok(ClientSeverity::Debug5)
        } else {
            Err(VarParseError::ConstrainedParameter {
                invalid_values: input.to_vec(),
                valid_values: Some(ClientSeverity::valid_values()),
            })
        }
    }

    fn box_clone(&self) -> Box<dyn Value> {
        Box::new(self.clone())
    }

    fn format(&self) -> String {
        self.as_str().into()
    }
}

/// List of valid time zones.
///
/// Names are following the tz database, but only time zones equivalent
/// to UTC±00:00 are supported.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TimeZone {
    /// UTC
    UTC,
    /// GMT
    GMT,
    /// Fixed offset from UTC, currently only "+00:00" is supported.
    /// A string representation is kept here for compatibility with Postgres.
    FixedOffset(&'static str),
}

impl TimeZone {
    fn as_str(&self) -> &'static str {
        match self {
            TimeZone::UTC => "UTC",
            TimeZone::GMT => "GMT",
            TimeZone::FixedOffset(s) => s,
        }
    }
}

impl Value for TimeZone {
    fn type_name() -> Cow<'static, str>
    where
        Self: Sized,
    {
        // TODO(parkmycar): It seems like we should change this?
        "string".into()
    }

    fn parse(input: VarInput<'_>) -> Result<Self, VarParseError>
    where
        Self: Sized,
    {
        let s = extract_single_value(input)?;
        let s = UncasedStr::new(s);

        if s == TimeZone::UTC.as_str() {
            Ok(TimeZone::UTC)
        } else if s == TimeZone::GMT.as_str() {
            Ok(TimeZone::GMT)
        } else if s == "+00:00" {
            Ok(TimeZone::FixedOffset("+00:00"))
        } else {
            Err(VarParseError::ConstrainedParameter {
                invalid_values: input.to_vec(),
                valid_values: None,
            })
        }
    }

    fn box_clone(&self) -> Box<dyn Value> {
        Box::new(self.clone())
    }

    fn format(&self) -> String {
        self.as_str().into()
    }
}

/// List of valid isolation levels.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum IsolationLevel {
    ReadUncommitted,
    ReadCommitted,
    RepeatableRead,
    Serializable,
    /* TODO(jkosh44) Move this comment to user facing docs when this isolation level becomes available to users.
     * The Strong Session Serializable isolation level combines the Serializable isolation level
     * (https://jepsen.io/consistency/models/serializable) with the Sequential consistency model
     * (https://jepsen.io/consistency/models/sequential). See
     * http://dbmsmusings.blogspot.com/2019/06/correctness-anomalies-under.html and
     * https://cs.uwaterloo.ca/~kmsalem/pubs/DaudjeeICDE04.pdf. Operations within a single session
     * are linearizable, but operations across sessions are not linearizable.
     *
     * Operations in sessions that use Strong Session Serializable are not linearizable with
     * operations in sessions that use Strict Serializable. For example, consider the following
     * sequence of events in order:
     *
     *   1. Session s0 executes read at timestamp t0 under Strong Session Serializable.
     *   2. Session s1 executes read at timestamp t1 under Strict Serializable.
     *
     * If t0 > t1, then this is not considered a consistency violation. This matches with the
     * semantics of Serializable, which can execute queries arbitrarily in the future without
     * violating the consistency of Strict Serializable queries.
     *
     * All operations within a session that use Strong Session Serializable are only
     * linearizable within operations within the same session that also use Strong Session
     * Serializable. For example, consider the following sequence of events in order:
     *
     *   1. Session s0 executes read at timestamp t0 under Strong Session Serializable.
     *   2. Session s0 executes read at timestamp t1 under I.
     *
     * If I is Strong Session Serializable then t0 > t1 is guaranteed. If I is any other isolation
     * level then t0 < t1 is not considered a consistency violation. This matches the semantics of
     * Serializable, which can execute queries arbitrarily in the future without violating the
     * consistency of Strict Serializable queries within the same session.
     *
     * The items left TODO before this is considered ready for prod are:
     *
     * - Add more tests.
     * - Linearize writes to system tables under this isolation (most of these are the side effect
     *   of some DDL).
     */
    StrongSessionSerializable,
    StrictSerializable,
}

impl IsolationLevel {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::ReadUncommitted => "read uncommitted",
            Self::ReadCommitted => "read committed",
            Self::RepeatableRead => "repeatable read",
            Self::Serializable => "serializable",
            Self::StrongSessionSerializable => "strong session serializable",
            Self::StrictSerializable => "strict serializable",
        }
    }

    fn valid_values() -> Vec<&'static str> {
        vec![
            Self::ReadUncommitted.as_str(),
            Self::ReadCommitted.as_str(),
            Self::RepeatableRead.as_str(),
            Self::Serializable.as_str(),
            // TODO(jkosh44) Add StrongSessionSerializable when it becomes available to users.
            Self::StrictSerializable.as_str(),
        ]
    }
}

impl fmt::Display for IsolationLevel {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

impl Value for IsolationLevel {
    fn type_name() -> Cow<'static, str>
    where
        Self: Sized,
    {
        "string".into()
    }

    fn parse(input: VarInput<'_>) -> Result<Self, VarParseError>
    where
        Self: Sized,
    {
        let s = extract_single_value(input)?;
        let s = UncasedStr::new(s);

        // We don't have any optimizations for levels below Serializable,
        // so we upgrade them all to Serializable.
        if s == Self::ReadUncommitted.as_str()
            || s == Self::ReadCommitted.as_str()
            || s == Self::RepeatableRead.as_str()
            || s == Self::Serializable.as_str()
        {
            Ok(Self::Serializable)
        } else if s == Self::StrongSessionSerializable.as_str() {
            Ok(Self::StrongSessionSerializable)
        } else if s == Self::StrictSerializable.as_str() {
            Ok(Self::StrictSerializable)
        } else {
            Err(VarParseError::ConstrainedParameter {
                invalid_values: input.to_vec(),
                valid_values: Some(IsolationLevel::valid_values()),
            })
        }
    }

    fn box_clone(&self) -> Box<dyn Value> {
        Box::new(self.clone())
    }

    fn format(&self) -> String {
        self.as_str().into()
    }
}

impl From<TransactionIsolationLevel> for IsolationLevel {
    fn from(transaction_isolation_level: TransactionIsolationLevel) -> Self {
        match transaction_isolation_level {
            TransactionIsolationLevel::ReadUncommitted => Self::ReadUncommitted,
            TransactionIsolationLevel::ReadCommitted => Self::ReadCommitted,
            TransactionIsolationLevel::RepeatableRead => Self::RepeatableRead,
            TransactionIsolationLevel::Serializable => Self::Serializable,
            TransactionIsolationLevel::StrongSessionSerializable => Self::StrongSessionSerializable,
            TransactionIsolationLevel::StrictSerializable => Self::StrictSerializable,
        }
    }
}

impl Value for CloneableEnvFilter {
    fn type_name() -> Cow<'static, str>
    where
        Self: Sized,
    {
        "EnvFilter".into()
    }

    fn parse(input: VarInput<'_>) -> Result<Self, VarParseError>
    where
        Self: Sized,
    {
        let s = extract_single_value(input)?;
        CloneableEnvFilter::from_str(s).map_err(|e| VarParseError::InvalidParameterValue {
            invalid_values: vec![s.to_string()],
            reason: e.to_string(),
        })
    }

    fn box_clone(&self) -> Box<dyn Value> {
        Box::new(self.clone())
    }

    fn format(&self) -> String {
        self.to_string()
    }
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum ClientEncoding {
    Utf8,
}

impl ClientEncoding {
    fn as_str(&self) -> &'static str {
        match self {
            ClientEncoding::Utf8 => "UTF8",
        }
    }

    fn valid_values() -> Vec<&'static str> {
        vec![ClientEncoding::Utf8.as_str()]
    }
}

impl Value for ClientEncoding {
    fn type_name() -> Cow<'static, str>
    where
        Self: Sized,
    {
        "string".into()
    }

    fn parse(input: VarInput<'_>) -> Result<Self, VarParseError>
    where
        Self: Sized,
    {
        let s = extract_single_value(input)?;
        let s = UncasedStr::new(s);
        if s == Self::Utf8.as_str() {
            Ok(Self::Utf8)
        } else {
            Err(VarParseError::ConstrainedParameter {
                invalid_values: vec![s.to_string()],
                valid_values: Some(ClientEncoding::valid_values()),
            })
        }
    }

    fn box_clone(&self) -> Box<dyn Value> {
        Box::new(self.clone())
    }

    fn format(&self) -> String {
        self.as_str().to_string()
    }
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum IntervalStyle {
    Postgres,
}

impl IntervalStyle {
    fn as_str(&self) -> &'static str {
        match self {
            IntervalStyle::Postgres => "postgres",
        }
    }

    fn valid_values() -> Vec<&'static str> {
        vec![IntervalStyle::Postgres.as_str()]
    }
}

impl Value for IntervalStyle {
    fn type_name() -> Cow<'static, str>
    where
        Self: Sized,
    {
        "string".into()
    }

    fn parse(input: VarInput<'_>) -> Result<Self, VarParseError>
    where
        Self: Sized,
    {
        let s = extract_single_value(input)?;
        let s = UncasedStr::new(s);
        if s == Self::Postgres.as_str() {
            Ok(Self::Postgres)
        } else {
            Err(VarParseError::ConstrainedParameter {
                invalid_values: vec![s.to_string()],
                valid_values: Some(IntervalStyle::valid_values()),
            })
        }
    }

    fn box_clone(&self) -> Box<dyn Value> {
        Box::new(self.clone())
    }

    fn format(&self) -> String {
        self.as_str().to_string()
    }
}

/// Macro to implement [`Value`] for simpler types, i.e. ones that already implement `FromStr` and
/// `ToString`.
///
/// Note: Macros can be hot garbage, if at any point folks think this is too complicated please
/// feel free to refactor!
macro_rules! impl_value_for_simple {
    ($t: ty, $name: literal) => {
        impl Value for $t {
            fn type_name() -> Cow<'static, str>
            where
                Self: Sized,
            {
                $name.into()
            }

            fn parse(input: VarInput<'_>) -> Result<Self, VarParseError>
            where
                Self: Sized,
            {
                let s = extract_single_value(input)?;
                s.parse::<Self>()
                    .map_err(|_| VarParseError::InvalidParameterType)
            }

            fn box_clone(&self) -> Box<dyn Value> {
                Box::new(self.clone())
            }

            fn format(&self) -> String {
                self.to_string()
            }
        }
    };
}

impl_value_for_simple!(i32, "integer");
impl_value_for_simple!(u32, "unsigned integer");
impl_value_for_simple!(u64, "64-bit unsigned integer");
impl_value_for_simple!(usize, "unsigned integer");
impl_value_for_simple!(f64, "double-precision floating-point number");

impl_value_for_simple!(mz_repr::Timestamp, "mz-timestamp");
impl_value_for_simple!(mz_repr::bytes::ByteSize, "bytes");
impl_value_for_simple!(CompactionStyle, "rocksdb_compaction_style");
impl_value_for_simple!(CompressionType, "rocksdb_compression_type");

#[cfg(test)]
mod tests {
    use mz_ore::assert_err;

    use super::*;

    #[mz_ore::test]
    fn test_value_duration() {
        fn inner(t: &'static str, e: Duration, expected_format: Option<&'static str>) {
            let d = Duration::parse(VarInput::Flat(t)).expect("invalid duration");
            assert_eq!(d, e);
            let mut d_format = d.format();
            d_format.retain(|c| !c.is_whitespace());
            if let Some(expected) = expected_format {
                assert_eq!(d_format, expected);
            } else {
                assert_eq!(
                    t.chars().filter(|c| !c.is_whitespace()).collect::<String>(),
                    d_format
                )
            }
        }
        inner("1", Duration::from_millis(1), Some("1ms"));
        inner("0", Duration::from_secs(0), Some("0s"));
        inner("1ms", Duration::from_millis(1), None);
        inner("1000ms", Duration::from_millis(1000), Some("1s"));
        inner("1001ms", Duration::from_millis(1001), None);
        inner("1us", Duration::from_micros(1), None);
        inner("1000us", Duration::from_micros(1000), Some("1ms"));
        inner("1s", Duration::from_secs(1), None);
        inner("60s", Duration::from_secs(60), Some("1min"));
        inner("3600s", Duration::from_secs(3600), Some("1h"));
        inner("3660s", Duration::from_secs(3660), Some("61min"));
        inner("1min", Duration::from_secs(1 * SEC_TO_MIN), None);
        inner("60min", Duration::from_secs(60 * SEC_TO_MIN), Some("1h"));
        inner("1h", Duration::from_secs(1 * SEC_TO_HOUR), None);
        inner("24h", Duration::from_secs(24 * SEC_TO_HOUR), Some("1d"));
        inner("1d", Duration::from_secs(1 * SEC_TO_DAY), None);
        inner("2d", Duration::from_secs(2 * SEC_TO_DAY), None);
        inner("  1   s ", Duration::from_secs(1), None);
        inner("1s ", Duration::from_secs(1), None);
        inner("   1s", Duration::from_secs(1), None);
        inner("0d", Duration::from_secs(0), Some("0s"));
        inner(
            "18446744073709551615",
            Duration::from_millis(u64::MAX),
            Some("18446744073709551615ms"),
        );
        inner(
            "18446744073709551615 s",
            Duration::from_secs(u64::MAX),
            Some("0"),
        );

        fn errs(t: &'static str) {
            assert_err!(Duration::parse(VarInput::Flat(t)));
        }
        errs("1 m");
        errs("1 sec");
        errs("1 min 1 s");
        errs("1m1s");
        errs("1.1");
        errs("1.1 min");
        errs("-1 s");
        errs("");
        errs("   ");
        errs("x");
        errs("s");
        errs("18446744073709551615 min");
    }

    #[mz_ore::test]
    fn test_should_output_to_client() {
        #[rustfmt::skip]
        let test_cases = [
            (ClientSeverity::Debug1, vec![Severity::Debug, Severity::Log, Severity::Notice, Severity::Warning, Severity::Error, Severity::Fatal, Severity:: Panic, Severity::Info], true),
            (ClientSeverity::Debug2, vec![Severity::Debug, Severity::Log, Severity::Notice, Severity::Warning, Severity::Error, Severity::Fatal, Severity:: Panic, Severity::Info], true),
            (ClientSeverity::Debug3, vec![Severity::Debug, Severity::Log, Severity::Notice, Severity::Warning, Severity::Error, Severity::Fatal, Severity:: Panic, Severity::Info], true),
            (ClientSeverity::Debug4, vec![Severity::Debug, Severity::Log, Severity::Notice, Severity::Warning, Severity::Error, Severity::Fatal, Severity:: Panic, Severity::Info], true),
            (ClientSeverity::Debug5, vec![Severity::Debug, Severity::Log, Severity::Notice, Severity::Warning, Severity::Error, Severity::Fatal, Severity:: Panic, Severity::Info], true),
            (ClientSeverity::Log, vec![Severity::Notice, Severity::Warning, Severity::Error, Severity::Fatal, Severity:: Panic, Severity::Info], true),
            (ClientSeverity::Log, vec![Severity::Debug], false),
            (ClientSeverity::Info, vec![Severity::Notice, Severity::Warning, Severity::Error, Severity::Fatal, Severity:: Panic, Severity::Info], true),
            (ClientSeverity::Info, vec![Severity::Debug, Severity::Log], false),
            (ClientSeverity::Notice, vec![Severity::Notice, Severity::Warning, Severity::Error, Severity::Fatal, Severity:: Panic, Severity::Info], true),
            (ClientSeverity::Notice, vec![Severity::Debug, Severity::Log], false),
            (ClientSeverity::Warning, vec![Severity::Warning, Severity::Error, Severity::Fatal, Severity:: Panic, Severity::Info], true),
            (ClientSeverity::Warning, vec![Severity::Debug, Severity::Log, Severity::Notice], false),
            (ClientSeverity::Error, vec![Severity::Error, Severity::Fatal, Severity:: Panic, Severity::Info], true),
            (ClientSeverity::Error, vec![Severity::Debug, Severity::Log, Severity::Notice, Severity::Warning], false),
        ];

        for test_case in test_cases {
            run_test(test_case)
        }

        fn run_test(test_case: (ClientSeverity, Vec<Severity>, bool)) {
            let client_min_messages_setting = test_case.0;
            let expected = test_case.2;
            for message_severity in test_case.1 {
                assert!(
                    client_min_messages_setting.should_output_to_client(&message_severity)
                        == expected
                )
            }
        }
    }
}

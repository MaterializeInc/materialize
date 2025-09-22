// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Mz-specific library for interacting with `tracing`.

use serde::{Deserialize, Serializer, de};
use std::fmt::Formatter;
use std::str::FromStr;
use tracing_subscriber::EnvFilter;

pub mod params;

#[derive(Debug, Clone)]
struct ValidatedEnvFilterString(String);

/// Wraps [`EnvFilter`] to provide a [`Clone`] implementation.
pub struct CloneableEnvFilter {
    filter: EnvFilter,
    validated: ValidatedEnvFilterString,
}

impl AsRef<EnvFilter> for CloneableEnvFilter {
    fn as_ref(&self) -> &EnvFilter {
        &self.filter
    }
}

impl From<CloneableEnvFilter> for EnvFilter {
    fn from(value: CloneableEnvFilter) -> Self {
        value.filter
    }
}

impl PartialEq for CloneableEnvFilter {
    fn eq(&self, other: &Self) -> bool {
        format!("{}", self) == format!("{}", other)
    }
}

impl Eq for CloneableEnvFilter {}

impl Clone for CloneableEnvFilter {
    fn clone(&self) -> Self {
        // TODO: implement Clone on `EnvFilter` upstream
        Self {
            // While EnvFilter has the undocumented property of roundtripping through
            // its String format, it seems safer to always create a new EnvFilter from
            // the same validated input when cloning.
            filter: EnvFilter::from_str(&self.validated.0).expect("validated"),
            validated: self.validated.clone(),
        }
    }
}

impl FromStr for CloneableEnvFilter {
    type Err = tracing_subscriber::filter::ParseError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let filter: EnvFilter = s.parse()?;
        Ok(CloneableEnvFilter {
            filter,
            validated: ValidatedEnvFilterString(s.to_string()),
        })
    }
}

impl std::fmt::Display for CloneableEnvFilter {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.filter)
    }
}

impl std::fmt::Debug for CloneableEnvFilter {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.filter)
    }
}

impl serde::Serialize for CloneableEnvFilter {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&format!("{}", self))
    }
}

impl<'de> Deserialize<'de> for CloneableEnvFilter {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        Self::from_str(s.as_str()).map_err(|x| de::Error::custom(x.to_string()))
    }
}
use tracing_subscriber::filter::Directive;

/// Wraps [`Directive`] to provide a serde implementations.
#[derive(PartialEq, Eq, Clone, Debug)]
pub struct SerializableDirective(Directive);

impl From<SerializableDirective> for Directive {
    fn from(value: SerializableDirective) -> Self {
        value.0
    }
}

impl From<Directive> for SerializableDirective {
    fn from(value: Directive) -> Self {
        SerializableDirective(value)
    }
}

impl FromStr for SerializableDirective {
    type Err = tracing_subscriber::filter::ParseError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let dir: Directive = s.parse()?;
        Ok(SerializableDirective(dir))
    }
}

impl std::fmt::Display for SerializableDirective {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl serde::Serialize for SerializableDirective {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&format!("{}", self))
    }
}

impl<'de> Deserialize<'de> for SerializableDirective {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        Self::from_str(s.as_str()).map_err(|x| de::Error::custom(x.to_string()))
    }
}

#[cfg(test)]
mod test {
    use crate::{CloneableEnvFilter, SerializableDirective};
    use std::str::FromStr;

    // TODO(guswynn): we probably want to test round-tripping through the
    // `RustType` impl as well

    #[mz_ore::test]
    fn roundtrips() {
        let filter = CloneableEnvFilter::from_str(
            "abc=debug,def=trace,[123],foo,baz[bar{a=b}]=debug,[{13=37}]=trace,info",
        )
        .expect("valid");
        assert_eq!(
            format!("{}", filter),
            format!(
                "{}",
                CloneableEnvFilter::from_str(&format!("{}", filter)).expect("valid")
            )
        );
    }

    #[mz_ore::test]
    fn roundtrips_directive() {
        let dir = SerializableDirective::from_str("abc=debug").expect("valid");
        assert_eq!(
            format!("{}", dir),
            format!(
                "{}",
                SerializableDirective::from_str(&format!("{}", dir)).expect("valid")
            )
        );
    }
}

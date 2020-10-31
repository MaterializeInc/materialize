// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::borrow::Borrow;

use anyhow::bail;

/// A `Var` represents a configuration parameter of an arbitrary type.
pub trait Var {
    /// Returns the name of the configuration parameter.
    fn name(&self) -> &'static str;

    /// Constructs a string representation of the current value of the
    /// configuration parameter.
    fn value(&self) -> String;

    /// Returns a short sentence describing the purpose of the configuration
    /// parameter.
    fn description(&self) -> &'static str;
}

/// A `ServerVar` is the default value for a configuration parameter.
#[derive(Debug)]
pub struct ServerVar<V>
where
    V: ?Sized + 'static,
{
    pub name: unicase::Ascii<&'static str>,
    pub value: &'static V,
    pub description: &'static str,
}

impl<V> Var for ServerVar<V>
where
    V: Value + ?Sized + 'static,
{
    fn name(&self) -> &'static str {
        &self.name
    }

    fn value(&self) -> String {
        self.value.format()
    }

    fn description(&self) -> &'static str {
        self.description
    }
}


/// A `SessionVar` is the session value for a configuration parameter. If unset,
/// the server default is used instead.
#[derive(Debug)]
pub struct SessionVar<V>
where
    V: Value + ?Sized + 'static,
{
    value: Option<V::Owned>,
    parent: &'static ServerVar<V>,
}

impl<V> SessionVar<V>
where
    V: Value + ?Sized + 'static,
{
    pub fn new(parent: &'static ServerVar<V>) -> SessionVar<V> {
        SessionVar {
            value: None,
            parent,
        }
    }

    pub fn set(&mut self, s: &str) -> Result<(), anyhow::Error> {
        match V::parse(s) {
            Ok(v) => {
                self.value = Some(v);
                Ok(())
            }
            Err(()) => bail!("parameter {} requires a {} value", self.name(), V::TYPE_NAME),
        }
    }

    pub fn value(&self) -> &V {
        self.value
            .as_ref()
            .map(|v| v.borrow())
            .unwrap_or(self.parent.value)
    }
}

impl<V> Var for SessionVar<V>
where
    V: Value + ToOwned + ?Sized + 'static,
{
    fn name(&self) -> &'static str {
        &self.parent.name
    }

    fn value(&self) -> String {
        SessionVar::value(self).format()
    }

    fn description(&self) -> &'static str {
        self.parent.description
    }
}

/// A value that can be stored in a session variable.
pub trait Value: ToOwned {
    /// The name of the value type.
    const TYPE_NAME: &'static str;
    /// Parses a value of this type from a string.
    fn parse(s: &str) -> Result<Self::Owned, ()>;
    /// Formats this value as a string.
    fn format(&self) -> String;
}

impl Value for bool {
    const TYPE_NAME: &'static str = "boolean";

    fn parse(s: &str) -> Result<Self, ()> {
        match s {
            "t" | "true" | "on" => Ok(true),
            "f" | "false" | "off" => Ok(false),
            _ => Err(()),
        }
    }

    fn format(&self) -> String {
        match self {
            true => "on".into(),
            false => "off".into(),
        }
    }
}

impl Value for i32 {
    const TYPE_NAME: &'static str = "integer";

    fn parse(s: &str) -> Result<i32, ()> {
        s.parse().map_err(|_| ())
    }

    fn format(&self) -> String {
        self.to_string()
    }
}

impl Value for str {
    const TYPE_NAME: &'static str = "string";

    fn parse(s: &str) -> Result<String, ()> {
        Ok(s.to_owned())
    }

    fn format(&self) -> String {
        self.to_owned()
    }
}

impl Value for [&str] {
    const TYPE_NAME: &'static str = "string list";

    fn parse(_: &str) -> Result<Self::Owned, ()> {
        // Don't know how to parse string lists yet.
        Err(())
    }

    fn format(&self) -> String {
        self.join(", ")
    }
}

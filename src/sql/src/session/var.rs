// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

// NOTE(benesch): there is a lot of duplicative code in this file in order to
// avoid runtime type casting. If the approach gets hard to maintain, we can
// always write a macro.

use std::borrow::Borrow;

use failure::bail;

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

impl Var for ServerVar<&'static str> {
    fn name(&self) -> &'static str {
        &self.name
    }

    fn value(&self) -> String {
        self.value.to_owned()
    }

    fn description(&self) -> &'static str {
        self.description
    }
}

impl Var for ServerVar<&'static [&'static str]> {
    fn name(&self) -> &'static str {
        &self.name
    }

    fn value(&self) -> String {
        self.value.join(", ")
    }

    fn description(&self) -> &'static str {
        self.description
    }
}

/// A `ServerVar` is the default value for a configuration parameter.
#[derive(Debug)]
pub struct ServerVar<V> {
    pub name: unicase::Ascii<&'static str>,
    pub value: V,
    pub description: &'static str,
}

/// A `SessionVar` is the session value for a configuration parameter. If unset,
/// the server default is used instead.
#[derive(Debug)]
pub struct SessionVar<V>
where
    V: ToOwned + ?Sized + 'static,
{
    value: Option<V::Owned>,
    parent: &'static ServerVar<&'static V>,
}

impl<V> SessionVar<V>
where
    V: ToOwned + ?Sized + 'static,
{
    pub fn new(parent: &'static ServerVar<&'static V>) -> SessionVar<V> {
        SessionVar {
            value: None,
            parent,
        }
    }

    pub fn value(&self) -> &V {
        self.value
            .as_ref()
            .map(|v| v.borrow())
            .unwrap_or(self.parent.value)
    }
}

impl SessionVar<bool> {
    pub fn set(&mut self, value: &str) -> Result<(), failure::Error> {
        if value == "t" || value == "true" || value == "on" {
            self.value = Some(true)
        } else if value == "f" || value == "false" || value == "off" {
            self.value = Some(false);
        } else {
            bail!("parameter {} requires a boolean value", self.parent.name)
        }
        Ok(())
    }
}

impl Var for SessionVar<bool> {
    fn name(&self) -> &'static str {
        &self.parent.name
    }

    fn value(&self) -> String {
        SessionVar::value(self).to_string()
    }

    fn description(&self) -> &'static str {
        self.parent.description
    }
}

impl SessionVar<str> {
    pub fn set(&mut self, value: &str) -> Result<(), failure::Error> {
        self.value = Some(value.to_owned());
        Ok(())
    }
}

impl Var for SessionVar<str> {
    fn name(&self) -> &'static str {
        &self.parent.name
    }

    fn value(&self) -> String {
        SessionVar::value(self).to_owned()
    }

    fn description(&self) -> &'static str {
        self.parent.description
    }
}

impl SessionVar<i32> {
    pub fn set(&mut self, value: &str) -> Result<(), failure::Error> {
        match value.parse() {
            Ok(value) => {
                self.value = Some(value);
                Ok(())
            }
            Err(_) => bail!("parameter {} requires an integer value", self.parent.name),
        }
    }
}

impl Var for SessionVar<i32> {
    fn name(&self) -> &'static str {
        &self.parent.name
    }

    fn value(&self) -> String {
        SessionVar::value(self).to_string()
    }

    fn description(&self) -> &'static str {
        self.parent.description
    }
}

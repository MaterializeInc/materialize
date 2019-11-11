// Copyright 2019 Materialize, Inc. All rights reserved.
//
// This file is part of Materialize. Materialize may not be used or
// distributed without the express permission of Materialize, Inc.

//! Variables that can be set by users or the server
//!
//! Some of these can be set by the `SET` sql statement, see the top-level `session` docs
//! for more details.

// NOTE(benesch): there is a lot of duplicative code in this file in order to
// avoid runtime type casting. If the approach gets hard to maintain, we can
// always write a macro.

use std::borrow::Borrow;

use failure::bail;

pub(super) const APPLICATION_NAME: ServerVar<&'static str> = ServerVar {
    name: unicase::Ascii::new("application_name"),
    value: "",
    description: "Sets the application name to be reported in statistics and logs (PostgreSQL).",
};

pub(super) const CLIENT_ENCODING: ServerVar<&'static str> = ServerVar {
    name: unicase::Ascii::new("client_encoding"),
    value: "UTF8",
    description: "Sets the client's character set encoding (PostgreSQL).",
};

pub(super) const DATABASE: ServerVar<&'static str> = ServerVar {
    name: unicase::Ascii::new("database"),
    value: "default",
    description: "Sets the current database (CockroachDB).",
};

pub(super) const DATE_STYLE: ServerVar<&'static str> = ServerVar {
    // DateStyle has nonstandard capitalization for historical reasons.
    name: unicase::Ascii::new("DateStyle"),
    value: "ISO, MDY",
    description: "Sets the display format for date and time values (PostgreSQL).",
};

pub(super) const EXTRA_FLOAT_DIGITS: ServerVar<&i32> = ServerVar {
    name: unicase::Ascii::new("extra_float_digits"),
    value: &3,
    description: "Adjusts the number of digits displayed for floating-point values (PostgreSQL).",
};

pub(super) const SERVER_VERSION: ServerVar<&'static str> = ServerVar {
    name: unicase::Ascii::new("server_version"),
    // Pretend to be Postgres v9.5.0, which is also what CockroachDB pretends to
    // be. Too new and some clients will emit a "server too new" warning. Too
    // old and some clients will fall back to legacy code paths. v9.5.0
    // empirically seems to be a good compromise.
    value: "9.5.0",
    description: "Shows the server version (PostgreSQL).",
};

pub(super) const SQL_SAFE_UPDATES: ServerVar<&bool> = ServerVar {
    name: unicase::Ascii::new("sql_safe_updates"),
    value: &false,
    description: "Prohibits SQL statements that may be overly destructive (CockroachDB).",
};

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

/// A `ServerVar` is the default value for a configuration parameter.
#[derive(Debug)]
pub(super) struct ServerVar<V> {
    pub(super) name: unicase::Ascii<&'static str>,
    pub(super) value: V,
    pub(super) description: &'static str,
}

/// A `SessionVar` is the session value for a configuration parameter. If unset,
/// the server default is used instead.
#[derive(Debug)]
pub(super) struct SessionVar<V>
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
    pub(super) fn new(parent: &'static ServerVar<&'static V>) -> SessionVar<V> {
        SessionVar {
            value: None,
            parent,
        }
    }

    pub(super) fn value(&self) -> &V {
        self.value
            .as_ref()
            .map(|v| v.borrow())
            .unwrap_or(self.parent.value)
    }
}

impl SessionVar<bool> {
    pub(super) fn set(&mut self, value: &str) -> Result<(), failure::Error> {
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
    pub(super) fn set(&mut self, value: &str) -> Result<(), failure::Error> {
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
    pub(super) fn set(&mut self, value: &str) -> Result<(), failure::Error> {
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

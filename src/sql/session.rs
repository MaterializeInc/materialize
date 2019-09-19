// Copyright 2019 Materialize, Inc. All rights reserved.
//
// This file is part of Materialize. Materialize may not be used or
// distributed without the express permission of Materialize, Inc.

//! SQL configuration parameters and connection state.
//!
//! Materialize roughly follows the PostgreSQL configuration model, which works
//! as follows. There is a global set of named configuration parameters, like
//! `DateStyle` and `client_encoding`. These parameters can be set in several
//! places: in an on-disk configuration file (in Postgres, named
//! postgresql.conf), in command line arguments when the server is started, or
//! at runtime via the `ALTER SYSTEM` or `SET` statements. Parameters that are
//! set in a session take precedence over database defaults, which in turn take
//! precedence over command line arguments, which in turn take precedence over
//! settings in the on-disk configuration.
//!
//! The Materialize configuration hierarchy at the moment is much simpler.
//! Global defaults are hardcoded into the binary, and a select few parameters
//! can be overridden per session. The infrastructure has been designed with
//! an eye towards supporting additional layers to the hierarchy, however, as
//! should the need arise.
//!
//! The configuration parameters that exist are driven by compatibility with
//! PostgreSQL drivers that expect them, not because they are particularly
//! important.

#![forbid(missing_docs)]

use std::collections::HashMap;

use failure::bail;

// NOTE(benesch): there is a lot of duplicative code in this file in order to
// avoid runtime type casting. If the approach gets hard to maintain, we can
// always write a macro.

const CLIENT_ENCODING: ServerVar<&'static str> = ServerVar {
    name: unicase::Ascii::new("client_encoding"),
    value: "UTF8",
    description: "Sets the client's character set encoding (PostgreSQL).",
};

const DATABASE: ServerVar<&'static str> = ServerVar {
    name: unicase::Ascii::new("database"),
    value: "default",
    description: "Sets the current database (CockroachDB).",
};

const DATE_STYLE: ServerVar<&'static str> = ServerVar {
    // DateStyle has nonstandard capitalization for historical reasons.
    name: unicase::Ascii::new("DateStyle"),
    value: "ISO, MDY",
    description: "Sets the display format for date and time values (PostgreSQL).",
};

const SERVER_VERSION: ServerVar<&'static str> = ServerVar {
    name: unicase::Ascii::new("server_version"),
    value: concat!(env!("CARGO_PKG_VERSION"), "+materialized"),
    description: "Shows the server version (PostgreSQL).",
};

const SQL_SAFE_UPDATES: ServerVar<bool> = ServerVar {
    name: unicase::Ascii::new("sql_safe_updates"),
    value: false,
    description: "Prohibits SQL statements that may be overly destructive (CockroachDB).",
};

/// A `Session` holds SQL state that is attached to a session.
pub struct Session {
    client_encoding: ServerVar<&'static str>,
    database: ServerVar<&'static str>,
    date_style: ServerVar<&'static str>,
    server_version: ServerVar<&'static str>,
    sql_safe_updates: SessionVar<bool>,
    /// A map from statement names to SQL queries
    pub prepared_statements: HashMap<String, Prepared>,
    /// Portals associated with the current session
    ///
    /// Portals are primarily a way to retrieve the results for a query with all
    /// parameters bound.
    portals: HashMap<String, Portal>,
}

impl std::fmt::Debug for Session {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        f.debug_struct("Session")
            .field("client_encoding", &self.client_encoding)
            .field("database", &self.database)
            .field("date_style", &self.date_style)
            .field("server_version", &self.server_version)
            .field("sql_safe_updates", &self.sql_safe_updates)
            .field(
                "prepared_statements(count)",
                &self.prepared_statements.len(),
            )
            .field("portals", &self.portals.keys())
            .finish()
    }
}

impl std::default::Default for Session {
    /// Constructs a new `Session` with default values.
    fn default() -> Session {
        Session {
            client_encoding: CLIENT_ENCODING,
            database: DATABASE,
            date_style: DATE_STYLE,
            server_version: SERVER_VERSION,
            sql_safe_updates: SessionVar::new(&SQL_SAFE_UPDATES),
            prepared_statements: HashMap::new(),
            portals: HashMap::new(),
        }
    }
}

impl Session {
    /// Returns all configuration parameters and their current values for this
    /// session.
    pub fn vars(&self) -> Vec<&dyn Var> {
        vec![
            &self.client_encoding,
            &self.database,
            &self.date_style,
            &self.server_version,
            &self.sql_safe_updates,
        ]
    }

    /// Returns the configuration parameters (and their current values for this
    /// session) that are expected to be sent to the client when a new
    /// connection is established.
    pub fn startup_vars(&self) -> Vec<&dyn Var> {
        vec![
            &self.client_encoding,
            &self.date_style,
            &self.server_version,
        ]
    }

    /// Returns a [`Var`] representing the configuration parameter with the
    /// specified name.
    ///
    /// Configuration parameters are matched case insensitively. If no such
    /// configuration parameter exists, `get` returns an error.
    ///
    /// Note that if `name` is known at compile time, you should instead use the
    /// named accessor to access the variable with its true Rust type. For
    /// example, `self.get("sql_safe_updates").value()` returns the string
    /// `"true"` or `"false"`, while `self.sql_safe_updates()` returns a bool.
    pub fn get(&self, name: &str) -> Result<&dyn Var, failure::Error> {
        if name == CLIENT_ENCODING.name {
            Ok(&self.client_encoding)
        } else if name == DATABASE.name {
            Ok(&self.database)
        } else if name == DATE_STYLE.name {
            Ok(&self.date_style)
        } else if name == SERVER_VERSION.name {
            Ok(&self.server_version)
        } else if name == SQL_SAFE_UPDATES.name {
            Ok(&self.sql_safe_updates)
        } else {
            bail!("unknown parameter: {}", name)
        }
    }

    /// Sets the configuration parameter named `name` to the value represented
    /// by `value`.
    ///
    /// Like with [`Session::get`], configuration parameters are matched case
    /// insensitively. If `value` is not valid, as determined by the underlying
    /// configuration parameter, or if the named configuration parameter does
    /// not exist, an error is returned.
    pub fn set(&mut self, name: &str, value: &str) -> Result<(), failure::Error> {
        if name == CLIENT_ENCODING.name {
            bail!("parameter {} is read only", CLIENT_ENCODING.name);
        } else if name == DATABASE.name {
            bail!("parameter {} is read only", DATABASE.name);
        } else if name == DATE_STYLE.name {
            bail!("parameter {} is read only", DATE_STYLE.name);
        } else if name == SERVER_VERSION.name {
            bail!("parameter {} is read only", SERVER_VERSION.name);
        } else if name == SQL_SAFE_UPDATES.name {
            self.sql_safe_updates.set(value)
        } else {
            bail!("unknown parameter: {}", name)
        }
    }

    /// Returns the value of the `client_encoding` configuration parameter.
    pub fn client_encoding(&self) -> &'static str {
        self.client_encoding.value
    }

    /// Returns the value of the `DateStyle` configuration parameter.
    pub fn date_style(&self) -> &'static str {
        self.date_style.value
    }

    /// Returns the value of the `database` configuration parameter.
    pub fn database(&self) -> &'static str {
        self.database.value
    }

    /// Returns the value of the `server_version` configuration parameter.
    pub fn server_version(&self) -> &'static str {
        self.server_version.value
    }

    /// Returns the value of the `sql_safe_updates` configuration parameter.
    pub fn sql_safe_updates(&self) -> bool {
        *self.sql_safe_updates.value()
    }

    /// Ensure that the given portal exists
    ///
    /// **Errors** if the statement name has not be set
    pub fn set_portal(
        &mut self,
        portal_name: String,
        statement_name: String,
        return_field_formats: Vec<bool>,
    ) -> Result<(), failure::Error> {
        if self.prepared_statements.contains_key(&statement_name) {
            self.portals.insert(
                portal_name,
                Portal {
                    statement_name,
                    return_field_formats,
                },
            );
            Ok(())
        } else {
            failure::bail!(
                "statement does not exist for portal creation: \
                 statement={:?} portal={:?}",
                statement_name,
                portal_name
            );
        }
    }

    /// Retrieve a portal by name
    pub fn get_portal(&self, portal_name: &str) -> Option<&Portal> {
        self.portals.get(portal_name)
    }
}

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
struct ServerVar<V> {
    name: unicase::Ascii<&'static str>,
    value: V,
    description: &'static str,
}

/// A `SessionVar` is the session value for a configuration parameter. If unset,
/// the server default is used instead.
#[derive(Debug)]
struct SessionVar<V>
where
    V: 'static,
{
    value: Option<V>,
    parent: &'static ServerVar<V>,
}

impl<V> SessionVar<V>
where
    V: 'static,
{
    fn new(parent: &'static ServerVar<V>) -> SessionVar<V> {
        SessionVar {
            value: None,
            parent,
        }
    }

    fn value(&self) -> &V {
        self.value.as_ref().unwrap_or(&self.parent.value)
    }
}

impl SessionVar<bool> {
    fn set(&mut self, value: &str) -> Result<(), failure::Error> {
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

/// A prepared statement
#[derive(Debug)]
pub struct Prepared {
    pub raw_sql: String,
    pub parsed: crate::ParsedSelect,
}

#[derive(Debug)]
pub struct Portal {
    pub statement_name: String,
    /// A vec of "encoded" `materialize::pgwire::message::FieldFormat`s
    pub return_field_formats: Vec<bool>,
}

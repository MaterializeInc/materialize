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

use std::borrow::Borrow;
use std::collections::HashMap;
use std::fmt;

use failure::bail;

use repr::{RelationDesc, Row, ScalarType};

// NOTE(benesch): there is a lot of duplicative code in this file in order to
// avoid runtime type casting. If the approach gets hard to maintain, we can
// always write a macro.

const APPLICATION_NAME: ServerVar<&'static str> = ServerVar {
    name: unicase::Ascii::new("application_name"),
    value: "",
    description: "Sets the application name to be reported in statistics and logs (PostgreSQL).",
};

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

const EXTRA_FLOAT_DIGITS: ServerVar<&i32> = ServerVar {
    name: unicase::Ascii::new("extra_float_digits"),
    value: &3,
    description: "Adjusts the number of digits displayed for floating-point values (PostgreSQL).",
};

const SERVER_VERSION: ServerVar<&'static str> = ServerVar {
    name: unicase::Ascii::new("server_version"),
    // Pretend to be Postgres v9.5.0, which is also what CockroachDB pretends to
    // be. Too new and some clients will emit a "server too new" warning. Too
    // old and some clients will fall back to legacy code paths. v9.5.0
    // empirically seems to be a good compromise.
    value: "9.5.0",
    description: "Shows the server version (PostgreSQL).",
};

const SQL_SAFE_UPDATES: ServerVar<&bool> = ServerVar {
    name: unicase::Ascii::new("sql_safe_updates"),
    value: &false,
    description: "Prohibits SQL statements that may be overly destructive (CockroachDB).",
};

/// A `Session` holds SQL state that is attached to a session.
pub struct Session {
    application_name: SessionVar<str>,
    client_encoding: ServerVar<&'static str>,
    database: ServerVar<&'static str>,
    date_style: ServerVar<&'static str>,
    extra_float_digits: SessionVar<i32>,
    server_version: ServerVar<&'static str>,
    sql_safe_updates: SessionVar<bool>,
    /// A map from statement names to SQL queries
    prepared_statements: HashMap<String, PreparedStatement>,
    /// Portals associated with the current session
    ///
    /// Portals are primarily a way to retrieve the results for a query with all
    /// parameters bound.
    portals: HashMap<String, Portal>,
}

impl fmt::Debug for Session {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        f.debug_struct("Session")
            .field("application_name", &self.application_name())
            .field("client_encoding", &self.client_encoding())
            .field("database", &self.database())
            .field("date_style", &self.date_style())
            .field("extra_float_digits", &self.extra_float_digits())
            .field("server_version", &self.server_version())
            .field("sql_safe_updates", &self.sql_safe_updates())
            .field("prepared_statements", &self.prepared_statements.keys())
            .field("portals", &self.portals.keys())
            .finish()
    }
}

impl std::default::Default for Session {
    /// Constructs a new `Session` with default values.
    fn default() -> Session {
        Session {
            application_name: SessionVar::new(&APPLICATION_NAME),
            client_encoding: CLIENT_ENCODING,
            database: DATABASE,
            date_style: DATE_STYLE,
            extra_float_digits: SessionVar::new(&EXTRA_FLOAT_DIGITS),
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
            &self.application_name,
            &self.client_encoding,
            &self.database,
            &self.date_style,
            &self.extra_float_digits,
            &self.server_version,
            &self.sql_safe_updates,
        ]
    }

    /// Returns the configuration parameters (and their current values for this
    /// session) that are expected to be sent to the client when a new
    /// connection is established or when their value changes.
    pub fn notify_vars(&self) -> Vec<&dyn Var> {
        vec![
            &self.application_name,
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
        if name == APPLICATION_NAME.name {
            Ok(&self.application_name)
        } else if name == CLIENT_ENCODING.name {
            Ok(&self.client_encoding)
        } else if name == DATABASE.name {
            Ok(&self.database)
        } else if name == DATE_STYLE.name {
            Ok(&self.date_style)
        } else if name == EXTRA_FLOAT_DIGITS.name {
            Ok(&self.extra_float_digits)
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
        if name == APPLICATION_NAME.name {
            self.application_name.set(value)
        } else if name == CLIENT_ENCODING.name {
            bail!("parameter {} is read only", CLIENT_ENCODING.name);
        } else if name == DATABASE.name {
            bail!("parameter {} is read only", DATABASE.name);
        } else if name == DATE_STYLE.name {
            bail!("parameter {} is read only", DATE_STYLE.name);
        } else if name == EXTRA_FLOAT_DIGITS.name {
            self.extra_float_digits.set(value)
        } else if name == SERVER_VERSION.name {
            bail!("parameter {} is read only", SERVER_VERSION.name);
        } else if name == SQL_SAFE_UPDATES.name {
            self.sql_safe_updates.set(value)
        } else {
            bail!("unknown parameter: {}", name)
        }
    }

    /// Returns the value of the `application_name` configuration parameter.
    pub fn application_name(&self) -> &str {
        self.application_name.value()
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

    /// Returns the value of the `extra_float_digits` configuration parameter.
    pub fn extra_float_digits(&self) -> i32 {
        *self.extra_float_digits.value()
    }

    /// Returns the value of the `server_version` configuration parameter.
    pub fn server_version(&self) -> &'static str {
        self.server_version.value
    }

    /// Returns the value of the `sql_safe_updates` configuration parameter.
    pub fn sql_safe_updates(&self) -> bool {
        *self.sql_safe_updates.value()
    }

    /// Ensure that the given prepared statement is present in this session
    pub fn set_prepared_statement(&mut self, name: String, statement: PreparedStatement) {
        self.prepared_statements.insert(name, statement);
    }

    /// Removes the prepared statement associated with `name`. It is not an
    /// error if no such statement exists.
    pub fn remove_prepared_statement(&mut self, name: &str) {
        let _ = self.prepared_statements.remove(name);
    }

    /// Retrieve the prepared statement in this session associated with `name`
    pub fn get_prepared_statement(&self, name: &str) -> Option<&PreparedStatement> {
        self.prepared_statements.get(name)
    }

    /// Ensure that the given portal exists
    ///
    /// **Errors** if the statement name has not be set
    pub fn set_portal(
        &mut self,
        portal_name: String,
        statement_name: String,
        row: Row,
        return_field_formats: Vec<bool>,
    ) -> Result<(), failure::Error> {
        if self.prepared_statements.contains_key(&statement_name) {
            self.portals.insert(
                portal_name,
                Portal {
                    statement_name,
                    row,
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
    V: ToOwned + ?Sized + 'static,
{
    value: Option<V::Owned>,
    parent: &'static ServerVar<&'static V>,
}

impl<V> SessionVar<V>
where
    V: ToOwned + ?Sized + 'static,
{
    fn new(parent: &'static ServerVar<&'static V>) -> SessionVar<V> {
        SessionVar {
            value: None,
            parent,
        }
    }

    fn value(&self) -> &V {
        self.value
            .as_ref()
            .map(|v| v.borrow())
            .unwrap_or(self.parent.value)
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

impl SessionVar<str> {
    fn set(&mut self, value: &str) -> Result<(), failure::Error> {
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
    fn set(&mut self, value: &str) -> Result<(), failure::Error> {
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

/// A prepared statement.
#[derive(Debug)]
pub struct PreparedStatement {
    sql: Option<sqlparser::ast::Statement>,
    desc: Option<RelationDesc>,
    param_types: Vec<ScalarType>,
}

impl PreparedStatement {
    /// Constructs a new `PreparedStatement`.
    pub fn new(
        sql: Option<sqlparser::ast::Statement>,
        desc: Option<RelationDesc>,
        param_types: Vec<ScalarType>,
    ) -> PreparedStatement {
        PreparedStatement {
            sql,
            desc,
            param_types,
        }
    }

    /// Returns the raw SQL string associated with this prepared statement,
    /// if the prepared statement was not the empty query.
    pub fn sql(&self) -> Option<&sqlparser::ast::Statement> {
        self.sql.as_ref()
    }

    /// Returns the type of the rows that will be returned by this prepared
    /// statement, if this prepared statement will return rows at all.
    pub fn desc(&self) -> Option<&RelationDesc> {
        self.desc.as_ref()
    }

    /// Returns the types of any parameters in this prepared statement.
    pub fn param_types(&self) -> &[ScalarType] {
        &self.param_types
    }
}

/// A portal, used by clients to name the target of an Execute statement
#[derive(Debug)]
pub struct Portal {
    pub statement_name: String,
    /// A Row of Datum of parameter values to be used in the query.
    pub row: Row,
    /// A vec of "encoded" `materialize::pgwire::message::FieldFormat`s
    pub return_field_formats: Vec<bool>,
}

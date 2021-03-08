// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::borrow::Borrow;
use std::fmt;

use crate::error::CoordError;

const APPLICATION_NAME: ServerVar<str> = ServerVar {
    name: unicase::Ascii::new("application_name"),
    value: "",
    description: "Sets the application name to be reported in statistics and logs (PostgreSQL).",
};

const CLIENT_ENCODING: ServerVar<str> = ServerVar {
    name: unicase::Ascii::new("client_encoding"),
    value: "UTF8",
    description: "Sets the client's character set encoding (PostgreSQL).",
};

const DATABASE: ServerVar<str> = ServerVar {
    name: unicase::Ascii::new("database"),
    value: "materialize",
    description: "Sets the current database (CockroachDB).",
};

const DATE_STYLE: ServerVar<str> = ServerVar {
    // DateStyle has nonstandard capitalization for historical reasons.
    name: unicase::Ascii::new("DateStyle"),
    value: "ISO, MDY",
    description: "Sets the display format for date and time values (PostgreSQL).",
};

const EXTRA_FLOAT_DIGITS: ServerVar<i32> = ServerVar {
    name: unicase::Ascii::new("extra_float_digits"),
    value: &3,
    description: "Adjusts the number of digits displayed for floating-point values (PostgreSQL).",
};

const INTEGER_DATETIMES: ServerVar<bool> = ServerVar {
    name: unicase::Ascii::new("integer_datetimes"),
    value: &true,
    description: "Reports whether the server uses 64-bit-integer dates and times (PostgreSQL).",
};

const SEARCH_PATH: ServerVar<[&str]> = ServerVar {
    name: unicase::Ascii::new("search_path"),
    value: &["mz_catalog", "pg_catalog", "public", "mz_temp"],
    description:
        "Sets the schema search order for names that are not schema-qualified (PostgreSQL).",
};

const SERVER_VERSION: ServerVar<str> = ServerVar {
    name: unicase::Ascii::new("server_version"),
    // Pretend to be Postgres v9.5.0, which is also what CockroachDB pretends to
    // be. Too new and some clients will emit a "server too new" warning. Too
    // old and some clients will fall back to legacy code paths. v9.5.0
    // empirically seems to be a good compromise.
    value: "9.5.0",
    description: "Shows the server version (PostgreSQL).",
};

const SERVER_VERSION_NUM: ServerVar<i32> = ServerVar {
    name: unicase::Ascii::new("server_version_num"),
    // See the comment on `SERVER_VERSION`.
    value: &90500,
    description: "Shows the server version as an integer (PostgreSQL).",
};

const SQL_SAFE_UPDATES: ServerVar<bool> = ServerVar {
    name: unicase::Ascii::new("sql_safe_updates"),
    value: &false,
    description: "Prohibits SQL statements that may be overly destructive (CockroachDB).",
};

const STANDARD_CONFORMING_STRINGS: ServerVar<bool> = ServerVar {
    name: unicase::Ascii::new("standard_conforming_strings"),
    value: &true,
    description: "Causes '...' strings to treat backslashes literally (PostgreSQL).",
};

const TIMEZONE: ServerVar<str> = ServerVar {
    // TimeZone has nonstandard capitalization for historical reasons.
    name: unicase::Ascii::new("TimeZone"),
    value: "UTC",
    description: "Sets the time zone for displaying and interpreting time stamps (PostgreSQL).",
};

const TRANSACTION_ISOLATION: ServerVar<str> = ServerVar {
    name: unicase::Ascii::new("transaction_isolation"),
    value: "serializable",
    description: "Sets the current transaction's isolation level (PostgreSQL).",
};

/// Session variables.
///
/// Materialize roughly follows the PostgreSQL configuration model, which works
/// as follows. There is a global set of named configuration parameters, like
/// `DateStyle` and `client_encoding`. These parameters can be set in several
/// places: in an on-disk configuration file (in Postgres, named
/// postgresql.conf), in command line arguments when the server is started, or
/// at runtime via the `ALTER SYSTEM` or `SET` statements. Parameters that are
/// set in a session take precedence over database defaults, which in turn take
/// precedence over command line arguments, which in turn take precedence over
/// settings in the on-disk configuration.
///
/// The Materialize configuration hierarchy at the moment is much simpler.
/// Global defaults are hardcoded into the binary, and a select few parameters
/// can be overridden per session. The infrastructure has been designed with an
/// eye towards supporting additional layers to the hierarchy, however, should
/// the need arise.
///
/// The configuration parameters that exist are driven by compatibility with
/// PostgreSQL drivers that expect them, not because they are particularly
/// important.
#[derive(Debug)]
pub struct Vars {
    application_name: SessionVar<str>,
    client_encoding: ServerVar<str>,
    database: SessionVar<str>,
    date_style: ServerVar<str>,
    extra_float_digits: SessionVar<i32>,
    integer_datetimes: ServerVar<bool>,
    search_path: ServerVar<[&'static str]>,
    server_version: ServerVar<str>,
    server_version_num: ServerVar<i32>,
    sql_safe_updates: SessionVar<bool>,
    standard_conforming_strings: ServerVar<bool>,
    timezone: ServerVar<str>,
    transaction_isolation: ServerVar<str>,
}

impl Default for Vars {
    fn default() -> Vars {
        Vars {
            application_name: SessionVar::new(&APPLICATION_NAME),
            client_encoding: CLIENT_ENCODING,
            database: SessionVar::new(&DATABASE),
            date_style: DATE_STYLE,
            extra_float_digits: SessionVar::new(&EXTRA_FLOAT_DIGITS),
            integer_datetimes: INTEGER_DATETIMES,
            search_path: SEARCH_PATH,
            server_version: SERVER_VERSION,
            server_version_num: SERVER_VERSION_NUM,
            sql_safe_updates: SessionVar::new(&SQL_SAFE_UPDATES),
            standard_conforming_strings: STANDARD_CONFORMING_STRINGS,
            timezone: TIMEZONE,
            transaction_isolation: TRANSACTION_ISOLATION,
        }
    }
}

impl Vars {
    /// Returns an iterator over the configuration parameters and their current
    /// values for this session.
    pub fn iter(&self) -> impl Iterator<Item = &dyn Var> {
        vec![
            &self.application_name as &dyn Var,
            &self.client_encoding,
            &self.database,
            &self.date_style,
            &self.extra_float_digits,
            &self.integer_datetimes,
            &self.search_path,
            &self.server_version,
            &self.server_version_num,
            &self.sql_safe_updates,
            &self.standard_conforming_strings,
            &self.timezone,
            &self.transaction_isolation,
        ]
        .into_iter()
    }

    /// Returns an iterator over configuration parameters (and their current
    /// values for this session) that are expected to be sent to the client when
    /// a new connection is established or when their value changes.
    pub fn notify_set(&self) -> impl Iterator<Item = &dyn Var> {
        vec![
            &self.application_name as &dyn Var,
            &self.client_encoding,
            &self.date_style,
            &self.integer_datetimes,
            &self.server_version,
            &self.standard_conforming_strings,
        ]
        .into_iter()
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
    pub fn get(&self, name: &str) -> Result<&dyn Var, CoordError> {
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
        } else if name == INTEGER_DATETIMES.name {
            Ok(&self.integer_datetimes)
        } else if name == SEARCH_PATH.name {
            Ok(&self.search_path)
        } else if name == SERVER_VERSION.name {
            Ok(&self.server_version)
        } else if name == SERVER_VERSION_NUM.name {
            Ok(&self.server_version_num)
        } else if name == SQL_SAFE_UPDATES.name {
            Ok(&self.sql_safe_updates)
        } else if name == STANDARD_CONFORMING_STRINGS.name {
            Ok(&self.standard_conforming_strings)
        } else if name == TIMEZONE.name {
            Ok(&self.timezone)
        } else if name == TRANSACTION_ISOLATION.name {
            Ok(&self.transaction_isolation)
        } else {
            Err(CoordError::UnknownParameter(name.into()))
        }
    }

    /// Sets the configuration parameter named `name` to the value represented
    /// by `value`.
    ///
    /// Like with [`Vars::get`], configuration parameters are matched case
    /// insensitively. If `value` is not valid, as determined by the underlying
    /// configuration parameter, or if the named configuration parameter does
    /// not exist, an error is returned.
    pub fn set(&mut self, name: &str, value: &str) -> Result<(), CoordError> {
        if name == APPLICATION_NAME.name {
            self.application_name.set(value)
        } else if name == CLIENT_ENCODING.name {
            Err(CoordError::ReadOnlyParameter(&CLIENT_ENCODING))
        } else if name == DATABASE.name {
            self.database.set(value)
        } else if name == DATE_STYLE.name {
            for value in value.split(',') {
                let value = unicase::Ascii::new(value.trim());
                if value != "ISO" && value != "MDY" {
                    return Err(CoordError::ConstrainedParameter(&DATE_STYLE));
                }
            }
            Ok(())
        } else if name == EXTRA_FLOAT_DIGITS.name {
            self.extra_float_digits.set(value)
        } else if name == INTEGER_DATETIMES.name {
            Err(CoordError::ReadOnlyParameter(&INTEGER_DATETIMES))
        } else if name == SEARCH_PATH.name {
            Err(CoordError::ReadOnlyParameter(&SEARCH_PATH))
        } else if name == SERVER_VERSION.name {
            Err(CoordError::ReadOnlyParameter(&SERVER_VERSION))
        } else if name == SERVER_VERSION_NUM.name {
            Err(CoordError::ReadOnlyParameter(&SERVER_VERSION_NUM))
        } else if name == SQL_SAFE_UPDATES.name {
            self.sql_safe_updates.set(value)
        } else if name == STANDARD_CONFORMING_STRINGS.name {
            Err(CoordError::ReadOnlyParameter(&STANDARD_CONFORMING_STRINGS))
        } else if name == TIMEZONE.name {
            if unicase::Ascii::new(value) != TIMEZONE.value {
                return Err(CoordError::ConstrainedParameter(&TIMEZONE));
            } else {
                Ok(())
            }
        } else if name == TRANSACTION_ISOLATION.name {
            Err(CoordError::ReadOnlyParameter(&TRANSACTION_ISOLATION))
        } else {
            Err(CoordError::UnknownParameter(name.into()))
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
    pub fn database(&self) -> &str {
        self.database.value()
    }

    /// Returns the value of the `extra_float_digits` configuration parameter.
    pub fn extra_float_digits(&self) -> i32 {
        *self.extra_float_digits.value()
    }

    /// Returns the value of the `integer_datetimes` configuration parameter.
    pub fn integer_datetimes(&self) -> bool {
        *self.integer_datetimes.value
    }

    /// Returns the value of the `search_path` configuration parameter.
    pub fn search_path(&self) -> &'static [&'static str] {
        self.search_path.value
    }

    /// Returns the value of the `server_version` configuration parameter.
    pub fn server_version(&self) -> &'static str {
        self.server_version.value
    }

    /// Returns the value of the `server_version_num` configuration parameter.
    pub fn server_version_num(&self) -> i32 {
        *self.server_version_num.value
    }

    /// Returns the value of the `sql_safe_updates` configuration parameter.
    pub fn sql_safe_updates(&self) -> bool {
        *self.sql_safe_updates.value()
    }

    /// Returns the value of the `standard_conforming_strings` configuration
    /// parameter.
    pub fn standard_conforming_strings(&self) -> bool {
        *self.standard_conforming_strings.value
    }

    /// Returns the value of the `timezone` configuration parameter.
    pub fn timezone(&self) -> &'static str {
        self.timezone.value
    }

    /// Returns the value of the `transaction_isolation` configuration
    /// parameter.
    pub fn transaction_isolation(&self) -> &'static str {
        self.transaction_isolation.value
    }
}

/// A `Var` represents a configuration parameter of an arbitrary type.
pub trait Var: fmt::Debug {
    /// Returns the name of the configuration parameter.
    fn name(&self) -> &'static str;

    /// Constructs a string representation of the current value of the
    /// configuration parameter.
    fn value(&self) -> String;

    /// Returns a short sentence describing the purpose of the configuration
    /// parameter.
    fn description(&self) -> &'static str;

    /// Returns the name of the type of this variable.
    fn type_name(&self) -> &'static str;
}

/// A `ServerVar` is the default value for a configuration parameter.
#[derive(Debug)]
pub struct ServerVar<V>
where
    V: fmt::Debug + ?Sized + 'static,
{
    pub name: unicase::Ascii<&'static str>,
    pub value: &'static V,
    pub description: &'static str,
}

impl<V> Var for ServerVar<V>
where
    V: Value + fmt::Debug + ?Sized + 'static,
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

    fn type_name(&self) -> &'static str {
        V::TYPE_NAME
    }
}

/// A `SessionVar` is the session value for a configuration parameter. If unset,
/// the server default is used instead.
#[derive(Debug)]
pub struct SessionVar<V>
where
    V: Value + fmt::Debug + ?Sized + 'static,
{
    value: Option<V::Owned>,
    parent: &'static ServerVar<V>,
}

impl<V> SessionVar<V>
where
    V: Value + fmt::Debug + ?Sized + 'static,
{
    pub fn new(parent: &'static ServerVar<V>) -> SessionVar<V> {
        SessionVar {
            value: None,
            parent,
        }
    }

    pub fn set(&mut self, s: &str) -> Result<(), CoordError> {
        match V::parse(s) {
            Ok(v) => {
                self.value = Some(v);
                Ok(())
            }
            Err(()) => Err(CoordError::InvalidParameterType(self.parent)),
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
    V: Value + ToOwned + fmt::Debug + ?Sized + 'static,
    V::Owned: fmt::Debug,
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

    fn type_name(&self) -> &'static str {
        V::TYPE_NAME
    }
}

/// A value that can be stored in a session variable.
pub trait Value: ToOwned + Send + Sync {
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

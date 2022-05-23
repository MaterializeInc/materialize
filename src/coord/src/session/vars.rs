// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::borrow::Borrow;
use std::fmt;

use const_format::concatcp;
use lazy_static::lazy_static;
use uncased::UncasedStr;

use mz_ore::cast;
use mz_sql::DEFAULT_SCHEMA;

use crate::error::CoordError;
use crate::session::EndTransactionAction;

// We pretend to be Postgres v9.5.0, which is also what CockroachDB pretends to
// be. Too new and some clients will emit a "server too new" warning. Too old
// and some clients will fall back to legacy code paths. v9.5.0 empirically
// seems to be a good compromise.

/// The major version of PostgreSQL that Materialize claims to be.
pub const SERVER_MAJOR_VERSION: u8 = 9;

/// The minor version of PostgreSQL that Materialize claims to be.
pub const SERVER_MINOR_VERSION: u8 = 5;

/// The patch version of PostgreSQL that Materialize claims to be.
pub const SERVER_PATCH_VERSION: u8 = 0;

/// The name of the default database that Materialize uses.
pub const DEFAULT_DATABASE_NAME: &str = "materialize";

const APPLICATION_NAME: ServerVar<str> = ServerVar {
    name: UncasedStr::new("application_name"),
    value: "",
    description: "Sets the application name to be reported in statistics and logs (PostgreSQL).",
};

const CLIENT_ENCODING: ServerVar<str> = ServerVar {
    name: UncasedStr::new("client_encoding"),
    value: "UTF8",
    description: "Sets the client's character set encoding (PostgreSQL).",
};

const CLIENT_MIN_MESSAGES: ServerVar<ClientSeverity> = ServerVar {
    name: UncasedStr::new("client_min_messages"),
    value: &ClientSeverity::Notice,
    description: "Sets the message levels that are sent to the client (PostgreSQL).",
};

const CLUSTER: ServerVar<str> = ServerVar {
    name: UncasedStr::new("cluster"),
    value: "default",
    description: "Sets the current cluster (Materialize).",
};

const CLUSTER_REPLICA: ServerVar<Option<String>> = ServerVar {
    name: UncasedStr::new("cluster_replica"),
    value: &None,
    description: "Sets a target cluster replica for SELECT queries (Materialize).",
};

const DATABASE: ServerVar<str> = ServerVar {
    name: UncasedStr::new("database"),
    value: DEFAULT_DATABASE_NAME,
    description: "Sets the current database (CockroachDB).",
};

const DATE_STYLE: ServerVar<str> = ServerVar {
    // DateStyle has nonstandard capitalization for historical reasons.
    name: UncasedStr::new("DateStyle"),
    value: "ISO, MDY",
    description: "Sets the display format for date and time values (PostgreSQL).",
};

const EXTRA_FLOAT_DIGITS: ServerVar<i32> = ServerVar {
    name: UncasedStr::new("extra_float_digits"),
    value: &3,
    description: "Adjusts the number of digits displayed for floating-point values (PostgreSQL).",
};

const FAILPOINTS: ServerVar<str> = ServerVar {
    name: UncasedStr::new("failpoints"),
    value: "",
    description: "Allows failpoints to be dynamically activated.",
};

const INTEGER_DATETIMES: ServerVar<bool> = ServerVar {
    name: UncasedStr::new("integer_datetimes"),
    value: &true,
    description: "Reports whether the server uses 64-bit-integer dates and times (PostgreSQL).",
};

const INTERVAL_STYLE: ServerVar<str> = ServerVar {
    // IntervalStyle has nonstandard capitalization for historical reasons.
    name: UncasedStr::new("IntervalStyle"),
    value: "postgres",
    description: "Sets the display format for interval values (PostgreSQL).",
};

const QGM_OPTIMIZATIONS: ServerVar<bool> = ServerVar {
    name: UncasedStr::new("qgm_optimizations_experimental"),
    value: &false,
    description: "Enables optimizations based on a Query Graph Model (QGM) query representation.",
};

lazy_static! {
    static ref DEFAULT_SEARCH_PATH: [String; 1] = [DEFAULT_SCHEMA.to_owned(),];
    static ref SEARCH_PATH: ServerVar<[String]> = ServerVar {
        name: UncasedStr::new("search_path"),
        value: &*DEFAULT_SEARCH_PATH,
        description:
            "Sets the schema search order for names that are not schema-qualified (PostgreSQL).",
    };
}

const SERVER_VERSION: ServerVar<str> = ServerVar {
    name: UncasedStr::new("server_version"),
    value: concatcp!(
        SERVER_MAJOR_VERSION,
        ".",
        SERVER_MINOR_VERSION,
        ".",
        SERVER_PATCH_VERSION
    ),
    description: "Shows the server version (PostgreSQL).",
};

const SERVER_VERSION_NUM: ServerVar<i32> = ServerVar {
    name: UncasedStr::new("server_version_num"),
    value: &((cast::u8_to_i32(SERVER_MAJOR_VERSION) * 10_000)
        + (cast::u8_to_i32(SERVER_MINOR_VERSION) * 100)
        + cast::u8_to_i32(SERVER_PATCH_VERSION)),
    description: "Shows the server version as an integer (PostgreSQL).",
};

const SQL_SAFE_UPDATES: ServerVar<bool> = ServerVar {
    name: UncasedStr::new("sql_safe_updates"),
    value: &false,
    description: "Prohibits SQL statements that may be overly destructive (CockroachDB).",
};

const STANDARD_CONFORMING_STRINGS: ServerVar<bool> = ServerVar {
    name: UncasedStr::new("standard_conforming_strings"),
    value: &true,
    description: "Causes '...' strings to treat backslashes literally (PostgreSQL).",
};

const TIMEZONE: ServerVar<TimeZone> = ServerVar {
    // TimeZone has nonstandard capitalization for historical reasons.
    name: UncasedStr::new("TimeZone"),
    value: &TimeZone::UTC,
    description: "Sets the time zone for displaying and interpreting time stamps (PostgreSQL).",
};

const TRANSACTION_ISOLATION: ServerVar<str> = ServerVar {
    name: UncasedStr::new("transaction_isolation"),
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
/// settings in the on-disk configuration. Note that changing the value of
/// parameters obeys transaction semantics: if a transaction fails to commit,
/// any parameters that were changed in that transaction (i.e., via `SET`)
/// will be rolled back to their previous value.
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
    client_min_messages: SessionVar<ClientSeverity>,
    cluster: SessionVar<str>,
    cluster_replica: SessionVar<Option<String>>,
    database: SessionVar<str>,
    date_style: ServerVar<str>,
    extra_float_digits: SessionVar<i32>,
    failpoints: ServerVar<str>,
    integer_datetimes: ServerVar<bool>,
    interval_style: ServerVar<str>,
    qgm_optimizations: SessionVar<bool>,
    search_path: SessionVar<[String]>,
    server_version: ServerVar<str>,
    server_version_num: ServerVar<i32>,
    sql_safe_updates: SessionVar<bool>,
    standard_conforming_strings: ServerVar<bool>,
    timezone: SessionVar<TimeZone>,
    transaction_isolation: ServerVar<str>,
}

impl Default for Vars {
    fn default() -> Vars {
        Vars {
            application_name: SessionVar::new(&APPLICATION_NAME),
            client_encoding: CLIENT_ENCODING,
            client_min_messages: SessionVar::new(&CLIENT_MIN_MESSAGES),
            cluster: SessionVar::new(&CLUSTER),
            cluster_replica: SessionVar::new(&CLUSTER_REPLICA),
            database: SessionVar::new(&DATABASE),
            date_style: DATE_STYLE,
            extra_float_digits: SessionVar::new(&EXTRA_FLOAT_DIGITS),
            failpoints: FAILPOINTS,
            integer_datetimes: INTEGER_DATETIMES,
            interval_style: INTERVAL_STYLE,
            qgm_optimizations: SessionVar::new(&QGM_OPTIMIZATIONS),
            search_path: SessionVar::new(&SEARCH_PATH),
            server_version: SERVER_VERSION,
            server_version_num: SERVER_VERSION_NUM,
            sql_safe_updates: SessionVar::new(&SQL_SAFE_UPDATES),
            standard_conforming_strings: STANDARD_CONFORMING_STRINGS,
            timezone: SessionVar::new(&TIMEZONE),
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
            &self.client_min_messages,
            &self.cluster,
            &self.cluster_replica,
            &self.database,
            &self.date_style,
            &self.extra_float_digits,
            &self.failpoints,
            &self.integer_datetimes,
            &self.interval_style,
            &self.qgm_optimizations,
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
        } else if name == CLIENT_MIN_MESSAGES.name {
            Ok(&self.client_min_messages)
        } else if name == CLUSTER.name {
            Ok(&self.cluster)
        } else if name == CLUSTER_REPLICA.name {
            Ok(&self.cluster_replica)
        } else if name == DATABASE.name {
            Ok(&self.database)
        } else if name == DATE_STYLE.name {
            Ok(&self.date_style)
        } else if name == EXTRA_FLOAT_DIGITS.name {
            Ok(&self.extra_float_digits)
        } else if name == FAILPOINTS.name {
            Ok(&self.failpoints)
        } else if name == INTEGER_DATETIMES.name {
            Ok(&self.integer_datetimes)
        } else if name == INTERVAL_STYLE.name {
            Ok(&self.interval_style)
        } else if name == QGM_OPTIMIZATIONS.name {
            Ok(&self.qgm_optimizations)
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
    /// The new value may be either committed or rolled back by the next call to
    /// [`Vars::end_transaction`]. If `local` is true, the new value is always
    /// discarded by the next call to [`Vars::end_transaction`], even if the
    /// transaction is marked to commit.
    ///
    /// Like with [`Vars::get`], configuration parameters are matched case
    /// insensitively. If `value` is not valid, as determined by the underlying
    /// configuration parameter, or if the named configuration parameter does
    /// not exist, an error is returned.
    pub fn set(&mut self, name: &str, value: &str, local: bool) -> Result<(), CoordError> {
        if name == APPLICATION_NAME.name {
            self.application_name.set(value, local)
        } else if name == CLIENT_ENCODING.name {
            // Unfortunately, some orm's like Prisma set NAMES to UTF8, thats the only
            // value we support, so we let is through
            if UncasedStr::new(value) != CLIENT_ENCODING.value {
                return Err(CoordError::FixedValueParameter(&CLIENT_ENCODING));
            } else {
                Ok(())
            }
        } else if name == CLIENT_MIN_MESSAGES.name {
            if let Ok(_) = ClientSeverity::parse(value) {
                self.client_min_messages.set(value, local)
            } else {
                return Err(CoordError::ConstrainedParameter {
                    parameter: &CLIENT_MIN_MESSAGES,
                    value: value.into(),
                    valid_values: Some(ClientSeverity::valid_values()),
                });
            }
        } else if name == CLUSTER.name {
            self.cluster.set(value, local)
        } else if name == CLUSTER_REPLICA.name {
            self.cluster_replica.set(value, local)
        } else if name == DATABASE.name {
            self.database.set(value, local)
        } else if name == DATE_STYLE.name {
            for value in value.split(',') {
                let value = UncasedStr::new(value.trim());
                if value != "ISO" && value != "MDY" {
                    return Err(CoordError::FixedValueParameter(&DATE_STYLE));
                }
            }
            Ok(())
        } else if name == EXTRA_FLOAT_DIGITS.name {
            self.extra_float_digits.set(value, local)
        } else if name == FAILPOINTS.name {
            for mut cfg in value.trim().split(';') {
                cfg = cfg.trim();
                if cfg.is_empty() {
                    continue;
                }
                let mut splits = cfg.splitn(2, '=');
                let failpoint = splits
                    .next()
                    .ok_or_else(|| CoordError::InvalidParameterValue {
                        parameter: &FAILPOINTS,
                        value: value.into(),
                        reason: "missing failpoint name".into(),
                    })?;
                let action = splits
                    .next()
                    .ok_or_else(|| CoordError::InvalidParameterValue {
                        parameter: &FAILPOINTS,
                        value: value.into(),
                        reason: "missing failpoint action".into(),
                    })?;
                fail::cfg(failpoint, action).map_err(|e| CoordError::InvalidParameterValue {
                    parameter: &FAILPOINTS,
                    value: value.into(),
                    reason: e,
                })?;
            }
            Ok(())
        } else if name == INTEGER_DATETIMES.name {
            Err(CoordError::ReadOnlyParameter(&INTEGER_DATETIMES))
        } else if name == INTERVAL_STYLE.name {
            // Only `postgres` is supported right now
            if UncasedStr::new(value) != INTERVAL_STYLE.value {
                return Err(CoordError::FixedValueParameter(&INTERVAL_STYLE));
            } else {
                Ok(())
            }
        } else if name == QGM_OPTIMIZATIONS.name {
            self.qgm_optimizations.set(value, local)
        } else if name == SEARCH_PATH.name {
            self.search_path.set(value, local)
        } else if name == SERVER_VERSION.name {
            Err(CoordError::ReadOnlyParameter(&SERVER_VERSION))
        } else if name == SERVER_VERSION_NUM.name {
            Err(CoordError::ReadOnlyParameter(&SERVER_VERSION_NUM))
        } else if name == SQL_SAFE_UPDATES.name {
            self.sql_safe_updates.set(value, local)
        } else if name == STANDARD_CONFORMING_STRINGS.name {
            match bool::parse(value) {
                Ok(value) if value == *STANDARD_CONFORMING_STRINGS.value => Ok(()),
                Ok(_) => Err(CoordError::FixedValueParameter(
                    &STANDARD_CONFORMING_STRINGS,
                )),
                Err(()) => Err(CoordError::InvalidParameterType(
                    &STANDARD_CONFORMING_STRINGS,
                )),
            }
        } else if name == TIMEZONE.name {
            if let Ok(_) = TimeZone::parse(value) {
                self.timezone.set(value, local)
            } else {
                return Err(CoordError::ConstrainedParameter {
                    parameter: &TIMEZONE,
                    value: value.into(),
                    valid_values: None,
                });
            }
        } else if name == TRANSACTION_ISOLATION.name {
            Err(CoordError::ReadOnlyParameter(&TRANSACTION_ISOLATION))
        } else {
            Err(CoordError::UnknownParameter(name.into()))
        }
    }

    /// Commits or rolls back configuration parameter updates made via
    /// [`Vars::set`] since the last call to `end_transaction`.
    pub fn end_transaction(&mut self, action: EndTransactionAction) {
        // IMPORTANT: if you've added a new `SessionVar`, add a corresponding
        // call to `end_transaction` below.
        let Vars {
            application_name,
            client_encoding: _,
            client_min_messages,
            cluster,
            cluster_replica,
            database,
            date_style: _,
            extra_float_digits,
            failpoints: _,
            integer_datetimes: _,
            interval_style: _,
            qgm_optimizations,
            search_path,
            server_version: _,
            server_version_num: _,
            sql_safe_updates,
            standard_conforming_strings: _,
            timezone,
            transaction_isolation: _,
        } = self;
        application_name.end_transaction(action);
        client_min_messages.end_transaction(action);
        cluster.end_transaction(action);
        cluster_replica.end_transaction(action);
        database.end_transaction(action);
        extra_float_digits.end_transaction(action);
        qgm_optimizations.end_transaction(action);
        search_path.end_transaction(action);
        sql_safe_updates.end_transaction(action);
        timezone.end_transaction(action);
    }

    /// Returns the value of the `application_name` configuration parameter.
    pub fn application_name(&self) -> &str {
        self.application_name.value()
    }

    /// Returns the value of the `client_encoding` configuration parameter.
    pub fn client_encoding(&self) -> &'static str {
        self.client_encoding.value
    }

    /// Returns the value of the `client_min_messages` configuration parameter.
    pub fn client_min_messages(&self) -> &ClientSeverity {
        self.client_min_messages.value()
    }

    /// Returns the value of the `cluster` configuration parameter.
    pub fn cluster(&self) -> &str {
        self.cluster.value()
    }

    /// Returns the value of the `cluster_replica` configuration parameter.
    pub fn cluster_replica(&self) -> Option<&str> {
        self.cluster_replica.value().as_deref()
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

    /// Returns the value of the `intervalstyle` configuration parameter.
    pub fn intervalstyle(&self) -> &'static str {
        self.interval_style.value
    }

    /// Returns the value of the `qgm_optimizations` configuration parameter.
    pub fn qgm_optimizations(&self) -> bool {
        *self.qgm_optimizations.value()
    }

    /// Returns the value of the `search_path` configuration parameter.
    pub fn search_path(&self) -> Vec<&str> {
        self.search_path
            .value()
            .iter()
            .map(String::as_str)
            .collect()
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
    pub fn timezone(&self) -> &TimeZone {
        self.timezone.value()
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

    /// Indicates wither the [`Var`] is experimental.
    ///
    /// The default implementation determines this from the [`Var`] name, as
    /// experimental variable names should always end with "_experimental".
    fn experimental(&self) -> bool {
        self.name().ends_with("_experimental")
    }
}

/// A `ServerVar` is the default value for a configuration parameter.
#[derive(Debug)]
struct ServerVar<V>
where
    V: fmt::Debug + ?Sized + 'static,
{
    name: &'static UncasedStr,
    value: &'static V,
    description: &'static str,
}

impl<V> Var for ServerVar<V>
where
    V: Value + fmt::Debug + ?Sized + 'static,
{
    fn name(&self) -> &'static str {
        self.name.as_str()
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
struct SessionVar<V>
where
    V: Value + fmt::Debug + ?Sized + 'static,
{
    local_value: Option<V::Owned>,
    staged_value: Option<V::Owned>,
    session_value: Option<V::Owned>,
    parent: &'static ServerVar<V>,
}

impl<V> SessionVar<V>
where
    V: Value + fmt::Debug + ?Sized + 'static,
{
    fn new(parent: &'static ServerVar<V>) -> SessionVar<V> {
        SessionVar {
            local_value: None,
            staged_value: None,
            session_value: None,
            parent,
        }
    }

    fn set(&mut self, s: &str, local: bool) -> Result<(), CoordError> {
        match V::parse(s) {
            Ok(v) => {
                if local {
                    self.local_value = Some(v);
                } else {
                    self.local_value = None;
                    self.staged_value = Some(v);
                }
                Ok(())
            }
            Err(()) => Err(CoordError::InvalidParameterType(self.parent)),
        }
    }

    fn end_transaction(&mut self, action: EndTransactionAction) {
        self.local_value = None;
        match action {
            EndTransactionAction::Commit if self.staged_value.is_some() => {
                self.session_value = self.staged_value.take()
            }
            _ => self.staged_value = None,
        }
    }

    fn value(&self) -> &V {
        self.local_value
            .as_ref()
            .map(|v| v.borrow())
            .or_else(|| self.staged_value.as_ref().map(|v| v.borrow()))
            .or_else(|| self.session_value.as_ref().map(|v| v.borrow()))
            .unwrap_or(self.parent.value)
    }
}

impl<V> Var for SessionVar<V>
where
    V: Value + ToOwned + fmt::Debug + ?Sized + 'static,
    V::Owned: fmt::Debug,
{
    fn name(&self) -> &'static str {
        self.parent.name.as_str()
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

impl Value for [String] {
    const TYPE_NAME: &'static str = "string list";

    fn parse(s: &str) -> Result<Vec<String>, ()> {
        // Only supporting a single value for now, setting multiple values
        // requires a change in the parser.
        Ok(vec![s.to_owned()])
    }

    fn format(&self) -> String {
        self.join(", ")
    }
}

impl Value for Option<String> {
    const TYPE_NAME: &'static str = "optional string";

    fn parse(s: &str) -> Result<Self::Owned, ()> {
        // TODO(teskje): Remove this workaround of treating empty string
        // values as NULL once we have support for DEFAULT values (#12551).
        let parsed = match s {
            "" => None,
            _ => Some(s.into()),
        };
        Ok(parsed)
    }

    fn format(&self) -> String {
        match self {
            Some(s) => s.clone(),
            None => "".into(),
        }
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
}

impl Value for ClientSeverity {
    const TYPE_NAME: &'static str = "string";

    fn parse(s: &str) -> Result<Self::Owned, ()> {
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
            Err(())
        }
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
    /// Fixed offset from UTC, currently only "+00:00" is supported.
    /// A string representation is kept here for compatibility with Postgres.
    FixedOffset(&'static str),
}

impl TimeZone {
    fn as_str(&self) -> &'static str {
        match self {
            TimeZone::UTC => "UTC",
            TimeZone::FixedOffset(s) => s,
        }
    }
}

impl Value for TimeZone {
    const TYPE_NAME: &'static str = "string";

    fn parse(s: &str) -> Result<Self::Owned, ()> {
        let s = UncasedStr::new(s);

        if s == TimeZone::UTC.as_str() {
            Ok(TimeZone::UTC)
        } else if s == "+00:00" {
            Ok(TimeZone::FixedOffset("+00:00"))
        } else {
            Err(())
        }
    }

    fn format(&self) -> String {
        self.as_str().into()
    }
}

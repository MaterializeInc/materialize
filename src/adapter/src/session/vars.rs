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
use std::time::Duration;

use const_format::concatcp;
use once_cell::sync::Lazy;
use uncased::UncasedStr;

use mz_ore::cast;
use mz_sql::DEFAULT_SCHEMA;
use mz_sql_parser::ast::TransactionIsolationLevel;

use crate::error::AdapterError;
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

static DEFAULT_SEARCH_PATH: Lazy<[String; 1]> = Lazy::new(|| [DEFAULT_SCHEMA.to_owned()]);
static SEARCH_PATH: Lazy<ServerVar<[String]>> = Lazy::new(|| ServerVar {
    name: UncasedStr::new("search_path"),
    value: &*DEFAULT_SEARCH_PATH,
    description:
        "Sets the schema search order for names that are not schema-qualified (PostgreSQL).",
});

const STATEMENT_TIMEOUT: ServerVar<Duration> = ServerVar {
    name: UncasedStr::new("statement_timeout"),
    value: &Duration::from_secs(10),
    description:
        "Sets the maximum allowed duration of INSERT...SELECT, UPDATE, and DELETE operations.",
};

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

const TRANSACTION_ISOLATION: ServerVar<IsolationLevel> = ServerVar {
    name: UncasedStr::new("transaction_isolation"),
    value: &IsolationLevel::StrictSerializable,
    description: "Sets the current transaction's isolation level (PostgreSQL).",
};

const MAX_TABLES: ServerVar<u32> = ServerVar {
    name: UncasedStr::new("max_tables"),
    value: &25,
    description: "The maximum number of tables in the region, across all schemas (Materialize).",
};

const MAX_SOURCES: ServerVar<u32> = ServerVar {
    name: UncasedStr::new("max_sources"),
    value: &25,
    description: "The maximum number of sources in the region, across all schemas (Materialize).",
};

const MAX_SINKS: ServerVar<u32> = ServerVar {
    name: UncasedStr::new("max_sinks"),
    value: &25,
    description: "The maximum number of sinks in the region, across all schemas (Materialize).",
};

const MAX_MATERIALIZED_VIEWS: ServerVar<u32> = ServerVar {
    name: UncasedStr::new("max_materialized_views"),
    value: &100,
    description:
        "The maximum number of materialized views in the region, across all schemas (Materialize).",
};

const MAX_CLUSTERS: ServerVar<u32> = ServerVar {
    name: UncasedStr::new("max_clusters"),
    value: &10,
    description: "The maximum number of clusters in the region (Materialize).",
};

const MAX_REPLICAS_PER_CLUSTER: ServerVar<u32> = ServerVar {
    name: UncasedStr::new("max_replicas_per_cluster"),
    value: &5,
    description: "The maximum number of replicas of a single cluster (Materialize).",
};

const MAX_DATABASES: ServerVar<u32> = ServerVar {
    name: UncasedStr::new("max_databases"),
    value: &1000,
    description: "The maximum number of databases in the region (Materialize).",
};

const MAX_SCHEMAS_PER_DATABASE: ServerVar<u32> = ServerVar {
    name: UncasedStr::new("max_schemas_per_database"),
    value: &1000,
    description: "The maximum number of schemas in a database (Materialize).",
};

const MAX_OBJECTS_PER_SCHEMA: ServerVar<u32> = ServerVar {
    name: UncasedStr::new("max_objects_per_schema"),
    value: &1000,
    description: "The maximum number of objects in a schema (Materialize).",
};

const MAX_SECRETS: ServerVar<u32> = ServerVar {
    name: UncasedStr::new("max_secrets"),
    value: &100,
    description: "The maximum number of secrets in the region, across all schemas (Materialize).",
};

const MAX_ROLES: ServerVar<u32> = ServerVar {
    name: UncasedStr::new("max_roles"),
    value: &1000,
    description: "The maximum number of roles in the region (Materialize).",
};

// Cloud environmentd is configured with 4 GiB of RAM, so 1 GiB is a good heuristic for a single
// query.
// TODO(jkosh44) Eventually we want to be able to return arbitrary sized results.
const MAX_RESULT_SIZE: ServerVar<u32> = ServerVar {
    name: UncasedStr::new("max_result_size"),
    // 1 GiB
    value: &1_073_741_824,
    description: "The maximum size in bytes for a single query's result (Materialize).",
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
/// can be overridden per session. A select few parameters can be overridden on
/// disk.
///
/// The set of variables that can be overridden per session and the set of
/// variables that can be overridden on disk are currently disjoint. The
/// infrastructure has been designed with an eye towards merging these two sets
/// and supporting additional layers to the hierarchy, however, should the need arise.
///
/// The configuration parameters that exist are driven by compatibility with
/// PostgreSQL drivers that expect them, not because they are particularly
/// important.
#[derive(Debug)]
pub struct SessionVars {
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
    statement_timeout: SessionVar<Duration>,
    timezone: SessionVar<TimeZone>,
    transaction_isolation: SessionVar<IsolationLevel>,
}

impl Default for SessionVars {
    fn default() -> SessionVars {
        SessionVars {
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
            statement_timeout: SessionVar::new(&STATEMENT_TIMEOUT),
            timezone: SessionVar::new(&TIMEZONE),
            transaction_isolation: SessionVar::new(&TRANSACTION_ISOLATION),
        }
    }
}

impl SessionVars {
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
            &self.statement_timeout,
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
    pub fn get(&self, name: &str) -> Result<&dyn Var, AdapterError> {
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
        } else if name == STATEMENT_TIMEOUT.name {
            Ok(&self.statement_timeout)
        } else if name == TIMEZONE.name {
            Ok(&self.timezone)
        } else if name == TRANSACTION_ISOLATION.name {
            Ok(&self.transaction_isolation)
        } else {
            Err(AdapterError::UnknownParameter(name.into()))
        }
    }

    /// Sets the configuration parameter named `name` to the value represented
    /// by `value`.
    ///
    /// The new value may be either committed or rolled back by the next call to
    /// [`SessionVars::end_transaction`]. If `local` is true, the new value is always
    /// discarded by the next call to [`SessionVars::end_transaction`], even if the
    /// transaction is marked to commit.
    ///
    /// Like with [`SessionVars::get`], configuration parameters are matched case
    /// insensitively. If `value` is not valid, as determined by the underlying
    /// configuration parameter, or if the named configuration parameter does
    /// not exist, an error is returned.
    pub fn set(&mut self, name: &str, value: &str, local: bool) -> Result<(), AdapterError> {
        if name == APPLICATION_NAME.name {
            self.application_name.set(value, local)
        } else if name == CLIENT_ENCODING.name {
            // Unfortunately, some orm's like Prisma set NAMES to UTF8, thats the only
            // value we support, so we let is through
            if UncasedStr::new(value) != CLIENT_ENCODING.value {
                return Err(AdapterError::FixedValueParameter(&CLIENT_ENCODING));
            } else {
                Ok(())
            }
        } else if name == CLIENT_MIN_MESSAGES.name {
            if let Ok(_) = ClientSeverity::parse(value) {
                self.client_min_messages.set(value, local)
            } else {
                return Err(AdapterError::ConstrainedParameter {
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
                    return Err(AdapterError::FixedValueParameter(&DATE_STYLE));
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
                let failpoint =
                    splits
                        .next()
                        .ok_or_else(|| AdapterError::InvalidParameterValue {
                            parameter: &FAILPOINTS,
                            value: value.into(),
                            reason: "missing failpoint name".into(),
                        })?;
                let action = splits
                    .next()
                    .ok_or_else(|| AdapterError::InvalidParameterValue {
                        parameter: &FAILPOINTS,
                        value: value.into(),
                        reason: "missing failpoint action".into(),
                    })?;
                fail::cfg(failpoint, action).map_err(|e| AdapterError::InvalidParameterValue {
                    parameter: &FAILPOINTS,
                    value: value.into(),
                    reason: e,
                })?;
            }
            Ok(())
        } else if name == INTEGER_DATETIMES.name {
            Err(AdapterError::ReadOnlyParameter(&INTEGER_DATETIMES))
        } else if name == INTERVAL_STYLE.name {
            // Only `postgres` is supported right now
            if UncasedStr::new(value) != INTERVAL_STYLE.value {
                return Err(AdapterError::FixedValueParameter(&INTERVAL_STYLE));
            } else {
                Ok(())
            }
        } else if name == QGM_OPTIMIZATIONS.name {
            self.qgm_optimizations.set(value, local)
        } else if name == SEARCH_PATH.name {
            self.search_path.set(value, local)
        } else if name == SERVER_VERSION.name {
            Err(AdapterError::ReadOnlyParameter(&SERVER_VERSION))
        } else if name == SERVER_VERSION_NUM.name {
            Err(AdapterError::ReadOnlyParameter(&SERVER_VERSION_NUM))
        } else if name == SQL_SAFE_UPDATES.name {
            self.sql_safe_updates.set(value, local)
        } else if name == STANDARD_CONFORMING_STRINGS.name {
            match bool::parse(value) {
                Ok(value) if value == *STANDARD_CONFORMING_STRINGS.value => Ok(()),
                Ok(_) => Err(AdapterError::FixedValueParameter(
                    &STANDARD_CONFORMING_STRINGS,
                )),
                Err(()) => Err(AdapterError::InvalidParameterType(
                    &STANDARD_CONFORMING_STRINGS,
                )),
            }
        } else if name == STATEMENT_TIMEOUT.name {
            self.statement_timeout.set(value, local)
        } else if name == TIMEZONE.name {
            if let Ok(_) = TimeZone::parse(value) {
                self.timezone.set(value, local)
            } else {
                return Err(AdapterError::ConstrainedParameter {
                    parameter: &TIMEZONE,
                    value: value.into(),
                    valid_values: None,
                });
            }
        } else if name == TRANSACTION_ISOLATION.name {
            if let Ok(_) = IsolationLevel::parse(value) {
                self.transaction_isolation.set(value, local)
            } else {
                return Err(AdapterError::ConstrainedParameter {
                    parameter: &TRANSACTION_ISOLATION,
                    value: value.into(),
                    valid_values: Some(IsolationLevel::valid_values()),
                });
            }
        } else {
            Err(AdapterError::UnknownParameter(name.into()))
        }
    }

    /// Sets the configuration parameter named `name` to its default value.
    ///
    /// The new value may be either committed or rolled back by the next call to
    /// [`SessionVars::end_transaction`]. If `local` is true, the new value is always
    /// discarded by the next call to [`SessionVars::end_transaction`], even if the
    /// transaction is marked to commit.
    ///
    /// Like with [`SessionVars::get`], configuration parameters are matched case
    /// insensitively. If the named configuration parameter does not exist, an
    /// error is returned.
    pub fn reset(&mut self, name: &str, local: bool) -> Result<(), AdapterError> {
        if name == APPLICATION_NAME.name {
            self.application_name.reset(local);
        } else if name == CLIENT_MIN_MESSAGES.name {
            self.client_min_messages.reset(local);
        } else if name == CLUSTER.name {
            self.cluster.reset(local);
        } else if name == CLUSTER_REPLICA.name {
            self.cluster_replica.reset(local);
        } else if name == DATABASE.name {
            self.database.reset(local);
        } else if name == EXTRA_FLOAT_DIGITS.name {
            self.extra_float_digits.reset(local);
        } else if name == QGM_OPTIMIZATIONS.name {
            self.qgm_optimizations.reset(local);
        } else if name == SEARCH_PATH.name {
            self.search_path.reset(local);
        } else if name == SQL_SAFE_UPDATES.name {
            self.sql_safe_updates.reset(local);
        } else if name == TIMEZONE.name {
            self.timezone.reset(local);
        } else if name == CLIENT_ENCODING.name
            || name == DATE_STYLE.name
            || name == FAILPOINTS.name
            || name == INTEGER_DATETIMES.name
            || name == INTERVAL_STYLE.name
            || name == SERVER_VERSION.name
            || name == SERVER_VERSION_NUM.name
            || name == STANDARD_CONFORMING_STRINGS.name
            || name == TRANSACTION_ISOLATION.name
        {
            // fixed value
        } else {
            return Err(AdapterError::UnknownParameter(name.into()));
        }
        Ok(())
    }

    /// Commits or rolls back configuration parameter updates made via
    /// [`SessionVars::set`] since the last call to `end_transaction`.
    pub fn end_transaction(&mut self, action: EndTransactionAction) {
        // IMPORTANT: if you've added a new `SessionVar`, add a corresponding
        // call to `end_transaction` below.
        let SessionVars {
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
            statement_timeout: _,
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

    /// Returns the value of the `statement_timeout` configuration parameter.
    pub fn statement_timeout(&self) -> &Duration {
        self.statement_timeout.value()
    }

    /// Returns the value of the `timezone` configuration parameter.
    pub fn timezone(&self) -> &TimeZone {
        self.timezone.value()
    }

    /// Returns the value of the `transaction_isolation` configuration
    /// parameter.
    pub fn transaction_isolation(&self) -> &IsolationLevel {
        self.transaction_isolation.value()
    }
}

/// On disk variables.
///
/// See [`SessionVars`] for more details on the Materialize configuration model.
#[derive(Debug, Clone)]
pub struct SystemVars {
    max_tables: SystemVar<u32>,
    max_sources: SystemVar<u32>,
    max_sinks: SystemVar<u32>,
    max_materialized_views: SystemVar<u32>,
    max_clusters: SystemVar<u32>,
    max_replicas_per_cluster: SystemVar<u32>,
    max_databases: SystemVar<u32>,
    max_schemas_per_database: SystemVar<u32>,
    max_objects_per_schema: SystemVar<u32>,
    max_secrets: SystemVar<u32>,
    max_roles: SystemVar<u32>,
    max_result_size: SystemVar<u32>,
}

impl Default for SystemVars {
    fn default() -> Self {
        SystemVars {
            max_tables: SystemVar::new(&MAX_TABLES),
            max_sources: SystemVar::new(&MAX_SOURCES),
            max_sinks: SystemVar::new(&MAX_SINKS),
            max_materialized_views: SystemVar::new(&MAX_MATERIALIZED_VIEWS),
            max_clusters: SystemVar::new(&MAX_CLUSTERS),
            max_replicas_per_cluster: SystemVar::new(&MAX_REPLICAS_PER_CLUSTER),
            max_databases: SystemVar::new(&MAX_DATABASES),
            max_schemas_per_database: SystemVar::new(&MAX_SCHEMAS_PER_DATABASE),
            max_objects_per_schema: SystemVar::new(&MAX_OBJECTS_PER_SCHEMA),
            max_secrets: SystemVar::new(&MAX_SECRETS),
            max_roles: SystemVar::new(&MAX_ROLES),
            max_result_size: SystemVar::new(&MAX_RESULT_SIZE),
        }
    }
}

impl SystemVars {
    /// Returns an iterator over the configuration parameters and their current
    /// values on disk.
    pub fn iter(&self) -> impl Iterator<Item = &dyn Var> {
        vec![
            &self.max_tables as &dyn Var,
            &self.max_sources,
            &self.max_sinks,
            &self.max_materialized_views,
            &self.max_clusters,
            &self.max_replicas_per_cluster,
            &self.max_databases,
            &self.max_schemas_per_database,
            &self.max_objects_per_schema,
            &self.max_secrets,
            &self.max_roles,
            &self.max_result_size,
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
    /// example, `self.get("max_tables").value()` returns the string
    /// `"25"` or the current value, while `self.max_tables()` returns an i32.
    pub fn get(&self, name: &str) -> Result<&dyn Var, AdapterError> {
        if name == MAX_TABLES.name {
            Ok(&self.max_tables)
        } else if name == MAX_SOURCES.name {
            Ok(&self.max_sources)
        } else if name == MAX_SINKS.name {
            Ok(&self.max_sinks)
        } else if name == MAX_MATERIALIZED_VIEWS.name {
            Ok(&self.max_materialized_views)
        } else if name == MAX_CLUSTERS.name {
            Ok(&self.max_clusters)
        } else if name == MAX_REPLICAS_PER_CLUSTER.name {
            Ok(&self.max_replicas_per_cluster)
        } else if name == MAX_DATABASES.name {
            Ok(&self.max_databases)
        } else if name == MAX_SCHEMAS_PER_DATABASE.name {
            Ok(&self.max_schemas_per_database)
        } else if name == MAX_OBJECTS_PER_SCHEMA.name {
            Ok(&self.max_objects_per_schema)
        } else if name == MAX_SECRETS.name {
            Ok(&self.max_secrets)
        } else if name == MAX_ROLES.name {
            Ok(&self.max_roles)
        } else if name == MAX_RESULT_SIZE.name {
            Ok(&self.max_result_size)
        } else {
            Err(AdapterError::UnknownParameter(name.into()))
        }
    }

    /// Sets the configuration parameter named `name` to the value represented
    /// by `value`.
    ///
    /// Like with [`SystemVars::get`], configuration parameters are matched case
    /// insensitively. If `value` is not valid, as determined by the underlying
    /// configuration parameter, or if the named configuration parameter does
    /// not exist, an error is returned.
    pub fn set(&mut self, name: &str, value: &str) -> Result<(), AdapterError> {
        if name == MAX_TABLES.name {
            self.max_tables.set(value)
        } else if name == MAX_SOURCES.name {
            self.max_sources.set(value)
        } else if name == MAX_SINKS.name {
            self.max_sinks.set(value)
        } else if name == MAX_MATERIALIZED_VIEWS.name {
            self.max_materialized_views.set(value)
        } else if name == MAX_CLUSTERS.name {
            self.max_clusters.set(value)
        } else if name == MAX_REPLICAS_PER_CLUSTER.name {
            self.max_replicas_per_cluster.set(value)
        } else if name == MAX_DATABASES.name {
            self.max_databases.set(value)
        } else if name == MAX_SCHEMAS_PER_DATABASE.name {
            self.max_schemas_per_database.set(value)
        } else if name == MAX_OBJECTS_PER_SCHEMA.name {
            self.max_objects_per_schema.set(value)
        } else if name == MAX_SECRETS.name {
            self.max_secrets.set(value)
        } else if name == MAX_ROLES.name {
            self.max_roles.set(value)
        } else if name == MAX_RESULT_SIZE.name {
            self.max_result_size.set(value)
        } else {
            Err(AdapterError::UnknownParameter(name.into()))
        }
    }

    /// Sets the configuration parameter named `name` to its default value.
    ///
    /// Like with [`SystemVars::get`], configuration parameters are matched case
    /// insensitively. If the named configuration parameter does not exist, an
    /// error is returned.
    pub fn reset(&mut self, name: &str) -> Result<(), AdapterError> {
        if name == MAX_TABLES.name {
            self.max_tables.reset()
        } else if name == MAX_SOURCES.name {
            self.max_sources.reset()
        } else if name == MAX_SINKS.name {
            self.max_sinks.reset()
        } else if name == MAX_MATERIALIZED_VIEWS.name {
            self.max_materialized_views.reset()
        } else if name == MAX_CLUSTERS.name {
            self.max_clusters.reset()
        } else if name == MAX_REPLICAS_PER_CLUSTER.name {
            self.max_replicas_per_cluster.reset()
        } else if name == MAX_DATABASES.name {
            self.max_databases.reset()
        } else if name == MAX_SCHEMAS_PER_DATABASE.name {
            self.max_schemas_per_database.reset()
        } else if name == MAX_OBJECTS_PER_SCHEMA.name {
            self.max_objects_per_schema.reset()
        } else if name == MAX_SECRETS.name {
            self.max_secrets.reset()
        } else if name == MAX_ROLES.name {
            self.max_roles.reset()
        } else if name == MAX_RESULT_SIZE.name {
            self.max_result_size.reset()
        } else {
            return Err(AdapterError::UnknownParameter(name.into()));
        }
        Ok(())
    }

    /// Returns the value of the `max_tables` configuration parameter.
    pub fn max_tables(&self) -> u32 {
        *self.max_tables.value()
    }

    /// Returns the value of the `max_sources` configuration parameter.
    pub fn max_sources(&self) -> u32 {
        *self.max_sources.value()
    }

    /// Returns the value of the `max_sinks` configuration parameter.
    pub fn max_sinks(&self) -> u32 {
        *self.max_sinks.value()
    }

    /// Returns the value of the `max_materialized_views` configuration parameter.
    pub fn max_materialized_views(&self) -> u32 {
        *self.max_materialized_views.value()
    }

    /// Returns the value of the `max_clusters` configuration parameter.
    pub fn max_clusters(&self) -> u32 {
        *self.max_clusters.value()
    }

    /// Returns the value of the `max_replicas_per_cluster` configuration parameter.
    pub fn max_replicas_per_cluster(&self) -> u32 {
        *self.max_replicas_per_cluster.value()
    }

    /// Returns the value of the `max_databases` configuration parameter.
    pub fn max_databases(&self) -> u32 {
        *self.max_databases.value()
    }

    /// Returns the value of the `max_schemas_per_database` configuration parameter.
    pub fn max_schemas_per_database(&self) -> u32 {
        *self.max_schemas_per_database.value()
    }

    /// Returns the value of the `max_objects_per_schema` configuration parameter.
    pub fn max_objects_per_schema(&self) -> u32 {
        *self.max_objects_per_schema.value()
    }

    /// Returns the value of the `max_secrets` configuration parameter.
    pub fn max_secrets(&self) -> u32 {
        *self.max_secrets.value()
    }

    /// Returns the value of the `max_roles` configuration parameter.
    pub fn max_roles(&self) -> u32 {
        *self.max_roles.value()
    }

    /// Returns the value of the `max_result_size` configuration parameter.
    pub fn max_result_size(&self) -> u32 {
        *self.max_result_size.value()
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

/// A `SystemVar` is persisted on disck value for a configuration parameter. If unset,
/// the server default is used instead.
#[derive(Debug, Clone)]
struct SystemVar<V>
where
    V: Value + fmt::Debug + ?Sized + 'static,
    V::Owned: fmt::Debug,
{
    persisted_value: Option<V::Owned>,
    parent: &'static ServerVar<V>,
}

impl<V> SystemVar<V>
where
    V: Value + fmt::Debug + ?Sized + 'static,
    V::Owned: fmt::Debug,
{
    fn new(parent: &'static ServerVar<V>) -> SystemVar<V> {
        SystemVar {
            persisted_value: None,
            parent,
        }
    }

    fn set(&mut self, s: &str) -> Result<(), AdapterError> {
        match V::parse(s) {
            Ok(v) => {
                self.persisted_value = Some(v);
                Ok(())
            }
            Err(()) => Err(AdapterError::InvalidParameterType(self.parent)),
        }
    }

    fn reset(&mut self) {
        self.persisted_value = None;
    }

    fn value(&self) -> &V {
        self.persisted_value
            .as_ref()
            .map(|v| v.borrow())
            .unwrap_or(self.parent.value)
    }
}

impl<V> Var for SystemVar<V>
where
    V: Value + ToOwned + fmt::Debug + ?Sized + 'static,
    V::Owned: fmt::Debug,
{
    fn name(&self) -> &'static str {
        self.parent.name()
    }

    fn value(&self) -> String {
        SystemVar::value(self).format()
    }

    fn description(&self) -> &'static str {
        self.parent.description()
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
    default_value: &'static V,
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
            default_value: &parent.value,
            local_value: None,
            staged_value: None,
            session_value: None,
            parent,
        }
    }

    fn set(&mut self, s: &str, local: bool) -> Result<(), AdapterError> {
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
            Err(()) => Err(AdapterError::InvalidParameterType(self.parent)),
        }
    }

    fn reset(&mut self, local: bool) {
        let value = self.default_value.to_owned();
        if local {
            self.local_value = Some(value);
        } else {
            self.local_value = None;
            self.staged_value = Some(value);
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
        self.parent.name()
    }

    fn value(&self) -> String {
        SessionVar::value(self).format()
    }

    fn description(&self) -> &'static str {
        self.parent.description()
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

impl Value for u32 {
    const TYPE_NAME: &'static str = "unsigned integer";

    fn parse(s: &str) -> Result<u32, ()> {
        s.parse().map_err(|_| ())
    }

    fn format(&self) -> String {
        self.to_string()
    }
}

const SEC_TO_MIN: u64 = 60u64;
const SEC_TO_HOUR: u64 = 60u64 * 60;
const SEC_TO_DAY: u64 = 60u64 * 60 * 24;
const MICRO_TO_MILLI: u32 = 1000u32;

impl Value for Duration {
    const TYPE_NAME: &'static str = "duration";

    fn parse(s: &str) -> Result<Duration, ()> {
        let s = s.trim();
        // Take all numeric values from [0..]
        let split_pos = s
            .chars()
            .position(|p| !char::is_numeric(p))
            .unwrap_or_else(|| s.chars().count());

        // Error if the numeric values don't parse, i.e. there aren't any.
        let d = s[..split_pos].parse::<u64>().map_err(|_| ())?;

        // We've already trimmed end
        let (f, m): (fn(u64) -> Duration, u64) = match s[split_pos..].trim_start() {
            "us" => (Duration::from_micros, 1),
            // Default unit is milliseconds
            "ms" | "" => (Duration::from_millis, 1),
            "s" => (Duration::from_secs, 1),
            "min" => (Duration::from_secs, SEC_TO_MIN),
            "h" => (Duration::from_secs, SEC_TO_HOUR),
            "d" => (Duration::from_secs, SEC_TO_DAY),
            _ => return Err(()),
        };

        let d = if d == 0 {
            Duration::from_secs(u64::MAX)
        } else {
            f(d.checked_mul(m).ok_or(())?)
        };
        Ok(d)
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

#[test]
fn test_value_duration() {
    fn inner(t: &'static str, e: Duration, expected_format: Option<&'static str>) {
        let d = Duration::parse(t).unwrap();
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
    inner("0", Duration::from_secs(u64::MAX), None);
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
    inner("0s", Duration::from_secs(u64::MAX), Some("0"));
    inner("0d", Duration::from_secs(u64::MAX), Some("0"));
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
        assert!(Duration::parse(t).is_err());
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
        match s {
            "" => Ok(None),
            _ => Ok(Some(s.into())),
        }
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

/// List of valid isolation levels.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum IsolationLevel {
    ReadUncommitted,
    ReadCommitted,
    RepeatableRead,
    Serializable,
    StrictSerializable,
}

impl IsolationLevel {
    pub(super) fn as_str(&self) -> &'static str {
        match self {
            Self::ReadUncommitted => "read uncommitted",
            Self::ReadCommitted => "read committed",
            Self::RepeatableRead => "repeatable read",
            Self::Serializable => "serializable",
            Self::StrictSerializable => "strict serializable",
        }
    }

    fn valid_values() -> Vec<&'static str> {
        vec![
            Self::ReadUncommitted.as_str(),
            Self::ReadCommitted.as_str(),
            Self::RepeatableRead.as_str(),
            Self::Serializable.as_str(),
            Self::StrictSerializable.as_str(),
        ]
    }
}

impl Value for IsolationLevel {
    const TYPE_NAME: &'static str = "string";

    fn parse(s: &str) -> Result<Self::Owned, ()> {
        let s = UncasedStr::new(s);

        // We don't have any optimizations for levels below Serializable,
        // so we upgrade them all to Serializable.
        if s == Self::ReadUncommitted.as_str()
            || s == Self::ReadCommitted.as_str()
            || s == Self::RepeatableRead.as_str()
            || s == Self::Serializable.as_str()
        {
            Ok(Self::Serializable)
        } else if s == Self::StrictSerializable.as_str() {
            Ok(Self::StrictSerializable)
        } else {
            Err(())
        }
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
            TransactionIsolationLevel::StrictSerializable => Self::StrictSerializable,
        }
    }
}

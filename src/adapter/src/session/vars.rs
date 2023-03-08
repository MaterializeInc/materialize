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
use itertools::Itertools;
use once_cell::sync::Lazy;
use serde::Serialize;
use uncased::UncasedStr;

use mz_build_info::BuildInfo;
use mz_ore::cast;
use mz_persist_client::cfg::PersistConfig;
use mz_sql::ast::Ident;
use mz_sql::DEFAULT_SCHEMA;
use mz_sql_parser::ast::TransactionIsolationLevel;

use crate::catalog::SYSTEM_USER;
use crate::error::AdapterError;
use crate::session::{EndTransactionAction, User};

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
    internal: false,
    safe: true,
};

const CLIENT_ENCODING: ServerVar<str> = ServerVar {
    name: UncasedStr::new("client_encoding"),
    value: "UTF8",
    description: "Sets the client's character set encoding (PostgreSQL).",
    internal: false,
    safe: true,
};

const CLIENT_MIN_MESSAGES: ServerVar<ClientSeverity> = ServerVar {
    name: UncasedStr::new("client_min_messages"),
    value: &ClientSeverity::Notice,
    description: "Sets the message levels that are sent to the client (PostgreSQL).",
    internal: false,
    safe: true,
};
pub const CLUSTER_VAR_NAME: &UncasedStr = UncasedStr::new("cluster");

const CLUSTER: ServerVar<str> = ServerVar {
    name: CLUSTER_VAR_NAME,
    value: "default",
    description: "Sets the current cluster (Materialize).",
    internal: false,
    safe: true,
};

const CLUSTER_REPLICA: ServerVar<Option<String>> = ServerVar {
    name: UncasedStr::new("cluster_replica"),
    value: &None,
    description: "Sets a target cluster replica for SELECT queries (Materialize).",
    internal: false,
    safe: true,
};

pub const DATABASE_VAR_NAME: &UncasedStr = UncasedStr::new("database");

const DATABASE: ServerVar<str> = ServerVar {
    name: DATABASE_VAR_NAME,
    value: DEFAULT_DATABASE_NAME,
    description: "Sets the current database (CockroachDB).",
    internal: false,
    safe: true,
};

static DEFAULT_DATE_STYLE: Lazy<Vec<String>> = Lazy::new(|| vec!["ISO".into(), "MDY".into()]);
static DATE_STYLE: Lazy<ServerVar<Vec<String>>> = Lazy::new(|| ServerVar {
    // DateStyle has nonstandard capitalization for historical reasons.
    name: UncasedStr::new("DateStyle"),
    value: &*DEFAULT_DATE_STYLE,
    description: "Sets the display format for date and time values (PostgreSQL).",
    internal: false,
    safe: true,
});

const EXTRA_FLOAT_DIGITS: ServerVar<i32> = ServerVar {
    name: UncasedStr::new("extra_float_digits"),
    value: &3,
    description: "Adjusts the number of digits displayed for floating-point values (PostgreSQL).",
    internal: false,
    safe: true,
};

const FAILPOINTS: ServerVar<str> = ServerVar {
    name: UncasedStr::new("failpoints"),
    value: "",
    description: "Allows failpoints to be dynamically activated.",
    internal: false,
    safe: true,
};

const INTEGER_DATETIMES: ServerVar<bool> = ServerVar {
    name: UncasedStr::new("integer_datetimes"),
    value: &true,
    description: "Reports whether the server uses 64-bit-integer dates and times (PostgreSQL).",
    internal: false,
    safe: true,
};

const INTERVAL_STYLE: ServerVar<str> = ServerVar {
    // IntervalStyle has nonstandard capitalization for historical reasons.
    name: UncasedStr::new("IntervalStyle"),
    value: "postgres",
    description: "Sets the display format for interval values (PostgreSQL).",
    internal: false,
    safe: true,
};

const MZ_VERSION_NAME: &UncasedStr = UncasedStr::new("mz_version");

static DEFAULT_SEARCH_PATH: Lazy<Vec<Ident>> = Lazy::new(|| vec![Ident::new(DEFAULT_SCHEMA)]);
static SEARCH_PATH: Lazy<ServerVar<Vec<Ident>>> = Lazy::new(|| ServerVar {
    name: UncasedStr::new("search_path"),
    value: &*DEFAULT_SEARCH_PATH,
    description:
        "Sets the schema search order for names that are not schema-qualified (PostgreSQL).",
    internal: false,
    safe: true,
});

const STATEMENT_TIMEOUT: ServerVar<Duration> = ServerVar {
    name: UncasedStr::new("statement_timeout"),
    value: &Duration::from_secs(10),
    description:
        "Sets the maximum allowed duration of INSERT...SELECT, UPDATE, and DELETE operations.",
    internal: false,
    safe: true,
};

const IDLE_IN_TRANSACTION_SESSION_TIMEOUT: ServerVar<Duration> = ServerVar {
    name: UncasedStr::new("idle_in_transaction_session_timeout"),
    value: &Duration::from_secs(60 * 2),
    description:
        "Sets the maximum allowed duration that a session can sit idle in a transaction before \
         being terminated. A value of zero disables the timeout (PostgreSQL).",
    internal: false,
    safe: true,
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
    internal: false,
    safe: true,
};

const SERVER_VERSION_NUM: ServerVar<i32> = ServerVar {
    name: UncasedStr::new("server_version_num"),
    value: &((cast::u8_to_i32(SERVER_MAJOR_VERSION) * 10_000)
        + (cast::u8_to_i32(SERVER_MINOR_VERSION) * 100)
        + cast::u8_to_i32(SERVER_PATCH_VERSION)),
    description: "Shows the server version as an integer (PostgreSQL).",
    internal: false,
    safe: true,
};

const SQL_SAFE_UPDATES: ServerVar<bool> = ServerVar {
    name: UncasedStr::new("sql_safe_updates"),
    value: &false,
    description: "Prohibits SQL statements that may be overly destructive (CockroachDB).",
    internal: false,
    safe: true,
};

const STANDARD_CONFORMING_STRINGS: ServerVar<bool> = ServerVar {
    name: UncasedStr::new("standard_conforming_strings"),
    value: &true,
    description: "Causes '...' strings to treat backslashes literally (PostgreSQL).",
    internal: false,
    safe: true,
};

const TIMEZONE: ServerVar<TimeZone> = ServerVar {
    // TimeZone has nonstandard capitalization for historical reasons.
    name: UncasedStr::new("TimeZone"),
    value: &TimeZone::UTC,
    description: "Sets the time zone for displaying and interpreting time stamps (PostgreSQL).",
    internal: false,
    safe: true,
};

pub const TRANSACTION_ISOLATION_VAR_NAME: &UncasedStr = UncasedStr::new("transaction_isolation");
const TRANSACTION_ISOLATION: ServerVar<IsolationLevel> = ServerVar {
    name: TRANSACTION_ISOLATION_VAR_NAME,
    value: &IsolationLevel::StrictSerializable,
    description: "Sets the current transaction's isolation level (PostgreSQL).",
    internal: false,
    safe: true,
};

const MAX_AWS_PRIVATELINK_CONNECTIONS: ServerVar<u32> = ServerVar {
    name: UncasedStr::new("max_aws_privatelink_connections"),
    value: &0,
    description: "The maximum number of AWS PrivateLink connections in the region, across all schemas (Materialize).",
    internal: false,
    safe: true,
};

const MAX_TABLES: ServerVar<u32> = ServerVar {
    name: UncasedStr::new("max_tables"),
    value: &25,
    description: "The maximum number of tables in the region, across all schemas (Materialize).",
    internal: false,
    safe: true,
};

const MAX_SOURCES: ServerVar<u32> = ServerVar {
    name: UncasedStr::new("max_sources"),
    value: &25,
    description: "The maximum number of sources in the region, across all schemas (Materialize).",
    internal: false,
    safe: true,
};

const MAX_SINKS: ServerVar<u32> = ServerVar {
    name: UncasedStr::new("max_sinks"),
    value: &25,
    description: "The maximum number of sinks in the region, across all schemas (Materialize).",
    internal: false,
    safe: true,
};

const MAX_MATERIALIZED_VIEWS: ServerVar<u32> = ServerVar {
    name: UncasedStr::new("max_materialized_views"),
    value: &100,
    description:
        "The maximum number of materialized views in the region, across all schemas (Materialize).",
    internal: false,
    safe: true,
};

const MAX_CLUSTERS: ServerVar<u32> = ServerVar {
    name: UncasedStr::new("max_clusters"),
    value: &10,
    description: "The maximum number of clusters in the region (Materialize).",
    internal: false,
    safe: true,
};

const MAX_REPLICAS_PER_CLUSTER: ServerVar<u32> = ServerVar {
    name: UncasedStr::new("max_replicas_per_cluster"),
    value: &5,
    description: "The maximum number of replicas of a single cluster (Materialize).",
    internal: false,
    safe: true,
};

const MAX_DATABASES: ServerVar<u32> = ServerVar {
    name: UncasedStr::new("max_databases"),
    value: &1000,
    description: "The maximum number of databases in the region (Materialize).",
    internal: false,
    safe: true,
};

const MAX_SCHEMAS_PER_DATABASE: ServerVar<u32> = ServerVar {
    name: UncasedStr::new("max_schemas_per_database"),
    value: &1000,
    description: "The maximum number of schemas in a database (Materialize).",
    internal: false,
    safe: true,
};

const MAX_OBJECTS_PER_SCHEMA: ServerVar<u32> = ServerVar {
    name: UncasedStr::new("max_objects_per_schema"),
    value: &1000,
    description: "The maximum number of objects in a schema (Materialize).",
    internal: false,
    safe: true,
};

const MAX_SECRETS: ServerVar<u32> = ServerVar {
    name: UncasedStr::new("max_secrets"),
    value: &100,
    description: "The maximum number of secrets in the region, across all schemas (Materialize).",
    internal: false,
    safe: true,
};

const MAX_ROLES: ServerVar<u32> = ServerVar {
    name: UncasedStr::new("max_roles"),
    value: &1000,
    description: "The maximum number of roles in the region (Materialize).",
    internal: false,
    safe: true,
};

// Cloud environmentd is configured with 4 GiB of RAM, so 1 GiB is a good heuristic for a single
// query.
// TODO(jkosh44) Eventually we want to be able to return arbitrary sized results.
pub const MAX_RESULT_SIZE: ServerVar<u32> = ServerVar {
    name: UncasedStr::new("max_result_size"),
    // 1 GiB
    value: &1_073_741_824,
    description: "The maximum size in bytes for a single query's result (Materialize).",
    internal: false,
    safe: true,
};

/// The logical compaction window for builtin tables and sources that have the
/// `retained_metrics_relation` flag set.
///
/// The existence of this variable is a bit of a hack until we have a fully
/// general solution for controlling retention windows.
pub const METRICS_RETENTION: ServerVar<Duration> = ServerVar {
    name: UncasedStr::new("metrics_retention"),
    // 30 days
    value: &Duration::from_secs(30 * 24 * 60 * 60),
    description: "The time to retain cluster utilization metrics (Materialize).",
    internal: true,
    safe: true,
};

static DEFAULT_ALLOWED_CLUSTER_REPLICA_SIZES: Lazy<Vec<Ident>> = Lazy::new(Vec::new);
static ALLOWED_CLUSTER_REPLICA_SIZES: Lazy<ServerVar<Vec<Ident>>> = Lazy::new(|| ServerVar {
    name: UncasedStr::new("allowed_cluster_replica_sizes"),
    value: &DEFAULT_ALLOWED_CLUSTER_REPLICA_SIZES,
    description: "The allowed sizes when creating a new cluster replica (Materialize).",
    internal: false,
    safe: true,
});

/// Controls [`mz_persist_client::cfg::DynamicConfig::blob_target_size`].
const PERSIST_BLOB_TARGET_SIZE: ServerVar<usize> = ServerVar {
    name: UncasedStr::new("persist_blob_target_size"),
    value: &PersistConfig::DEFAULT_BLOB_TARGET_SIZE,
    description: "A target maximum size of persist blob payloads in bytes (Materialize).",
    internal: true,
    safe: true,
};

/// Controls [`mz_persist_client::cfg::DynamicConfig::compaction_minimum_timeout`].
const PERSIST_COMPACTION_MINIMUM_TIMEOUT: ServerVar<Duration> = ServerVar {
    name: UncasedStr::new("persist_compaction_minimum_timeout"),
    value: &PersistConfig::DEFAULT_COMPACTION_MINIMUM_TIMEOUT,
    description: "The minimum amount of time to allow a persist compaction request to run before \
                  timing it out (Materialize).",
    internal: true,
    safe: true,
};

/// Controls the connection timeout to Cockroach.
///
/// Used by persist as [`mz_persist_client::cfg::DynamicConfig::consensus_connect_timeout`].
const CRDB_CONNECT_TIMEOUT: ServerVar<Duration> = ServerVar {
    name: UncasedStr::new("crdb_connect_timeout"),
    value: &PersistConfig::DEFAULT_CRDB_CONNECT_TIMEOUT,
    description: "The time to connect to CockroachDB before timing out and retrying.",
    internal: true,
    safe: true,
};

/// The maximum number of in-flight bytes emitted by persist_sources feeding dataflows.
const DATAFLOW_MAX_INFLIGHT_BYTES: ServerVar<usize> = ServerVar {
    name: UncasedStr::new("dataflow_max_inflight_bytes"),
    value: &usize::MAX,
    description: "The maximum number of in-flight bytes emitted by persist_sources feeding \
                  dataflows (Materialize).",
    internal: true,
    safe: true,
};

/// Controls [`mz_persist_client::cfg::PersistConfig::sink_minimum_batch_updates`].
const PERSIST_SINK_MINIMUM_BATCH_UPDATES: ServerVar<usize> = ServerVar {
    name: UncasedStr::new("persist_sink_minimum_batch_updates"),
    value: &PersistConfig::DEFAULT_SINK_MINIMUM_BATCH_UPDATES,
    description: "In the compute persist sink, workers with less than the minimum number of updates \
                  will flush their records to single downstream worker to be batched up there... in \
                  the hopes of grouping our updates into fewer, larger batches.",
    internal: true,
    safe: true,
};

/// Boolean flag indicating that the remote configuration was synchronized at
/// least once with the persistent [SessionVars].
pub static CONFIG_HAS_SYNCED_ONCE: ServerVar<bool> = ServerVar {
    name: UncasedStr::new("config_has_synced_once"),
    value: &false,
    description: "Boolean flag indicating that the remote configuration was synchronized at least once (Materialize).",
    internal: true,
    safe: true,
};

/// Feature flag indicating whether `WITH MUTUALLY RECURSIVE` queries are enabled.
static ENABLE_WITH_MUTUALLY_RECURSIVE: ServerVar<bool> = ServerVar {
    name: UncasedStr::new("enable_with_mutually_recursive"),
    value: &false,
    description: "Feature flag indicating whether `WITH MUTUALLY RECURSIVE` queries are enabled (Materialize).",
    internal: true,
    safe: true,
};

/// Feature flag indicating whether real time recency is enabled.
static REAL_TIME_RECENCY: ServerVar<bool> = ServerVar {
    name: UncasedStr::new("real_time_recency"),
    value: &false,
    description: "Feature flag indicating whether real time recency is enabled (Materialize).",
    internal: true,
    safe: false,
};

static EMIT_TIMESTAMP_NOTICE: ServerVar<bool> = ServerVar {
    name: UncasedStr::new("emit_timestamp_notice"),
    value: &false,
    description:
        "Boolean flag indicating whether to send a NOTICE specifying query timestamps (Materialize).",
    internal: false,
    safe: true,
};

static EMIT_TRACE_ID_NOTICE: ServerVar<bool> = ServerVar {
    name: UncasedStr::new("emit_trace_id_notice"),
    value: &false,
    description:
        "Boolean flag indicating whether to send a NOTICE specifying the trace id when available (Materialize).",
    internal: false,
    safe: true,
};

static MOCK_AUDIT_EVENT_TIMESTAMP: ServerVar<Option<mz_repr::Timestamp>> = ServerVar {
    name: UncasedStr::new("mock_audit_event_timestamp"),
    value: &None,
    description: "Mocked timestamp to use for audit events for testing purposes",
    internal: true,
    safe: false,
};

const IS_SUPERUSER: ServerVar<bool> = ServerVar {
    name: UncasedStr::new("is_superuser"),
    value: &false,
    description: "Indicates whether the current session is a super user (PostgreSQL).",
    internal: false,
    safe: true,
};

/// Represents the input to a variable.
///
/// Each variable has different rules for how it handles each style of input.
/// This type allows us to defer interpretation of the input until the
/// variable-specific interpretation can be applied.
#[derive(Debug, Clone, Copy)]
pub enum VarInput<'a> {
    /// The input has been flattened into a single string.
    Flat(&'a str),
    /// The input comes from a SQL `SET` statement and is jumbled across
    /// multiple components.
    SqlSet(&'a [String]),
}

impl<'a> VarInput<'a> {
    /// Converts the variable input to an owned vector of strings.
    fn to_vec(&self) -> Vec<String> {
        match self {
            VarInput::Flat(v) => vec![v.to_string()],
            VarInput::SqlSet(values) => values.into_iter().map(|v| v.to_string()).collect(),
        }
    }
}

/// An owned version of [`VarInput`].
#[derive(Debug, Clone)]
pub enum OwnedVarInput {
    /// See [`VarInput::Flat`].
    Flat(String),
    /// See [`VarInput::SqlSet`].
    SqlSet(Vec<String>),
}

impl OwnedVarInput {
    /// Converts this owned variable input as a [`VarInput`].
    pub fn borrow(&self) -> VarInput {
        match self {
            OwnedVarInput::Flat(v) => VarInput::Flat(v),
            OwnedVarInput::SqlSet(v) => VarInput::SqlSet(v),
        }
    }
}

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
    build_info: &'static BuildInfo,
    client_encoding: ServerVar<str>,
    client_min_messages: SessionVar<ClientSeverity>,
    cluster: SessionVar<str>,
    cluster_replica: SessionVar<Option<String>>,
    database: SessionVar<str>,
    date_style: &'static ServerVar<Vec<String>>,
    extra_float_digits: SessionVar<i32>,
    failpoints: ServerVar<str>,
    integer_datetimes: ServerVar<bool>,
    interval_style: ServerVar<str>,
    search_path: SessionVar<Vec<Ident>>,
    server_version: ServerVar<str>,
    server_version_num: ServerVar<i32>,
    sql_safe_updates: SessionVar<bool>,
    standard_conforming_strings: ServerVar<bool>,
    statement_timeout: SessionVar<Duration>,
    idle_in_transaction_session_timeout: SessionVar<Duration>,
    timezone: SessionVar<TimeZone>,
    transaction_isolation: SessionVar<IsolationLevel>,
    real_time_recency: SessionVar<bool>,
    emit_timestamp_notice: SessionVar<bool>,
    emit_trace_id_notice: SessionVar<bool>,
    is_superuser: SessionVar<bool>,
}

impl SessionVars {
    /// Creates a new [`SessionVars`].
    pub fn new(build_info: &'static BuildInfo) -> SessionVars {
        SessionVars {
            application_name: SessionVar::new(&APPLICATION_NAME),
            build_info,
            client_encoding: CLIENT_ENCODING,
            client_min_messages: SessionVar::new(&CLIENT_MIN_MESSAGES),
            cluster: SessionVar::new(&CLUSTER),
            cluster_replica: SessionVar::new(&CLUSTER_REPLICA),
            database: SessionVar::new(&DATABASE),
            date_style: &DATE_STYLE,
            extra_float_digits: SessionVar::new(&EXTRA_FLOAT_DIGITS),
            failpoints: FAILPOINTS,
            integer_datetimes: INTEGER_DATETIMES,
            interval_style: INTERVAL_STYLE,
            search_path: SessionVar::new(&SEARCH_PATH),
            server_version: SERVER_VERSION,
            server_version_num: SERVER_VERSION_NUM,
            sql_safe_updates: SessionVar::new(&SQL_SAFE_UPDATES),
            standard_conforming_strings: STANDARD_CONFORMING_STRINGS,
            statement_timeout: SessionVar::new(&STATEMENT_TIMEOUT),
            idle_in_transaction_session_timeout: SessionVar::new(
                &IDLE_IN_TRANSACTION_SESSION_TIMEOUT,
            ),
            timezone: SessionVar::new(&TIMEZONE),
            transaction_isolation: SessionVar::new(&TRANSACTION_ISOLATION),
            real_time_recency: SessionVar::new(&REAL_TIME_RECENCY),
            emit_timestamp_notice: SessionVar::new(&EMIT_TIMESTAMP_NOTICE),
            emit_trace_id_notice: SessionVar::new(&EMIT_TRACE_ID_NOTICE),
            is_superuser: SessionVar::new(&IS_SUPERUSER),
        }
    }

    /// Returns an iterator over the configuration parameters and their current
    /// values for this session.
    pub fn iter(&self) -> impl Iterator<Item = &dyn Var> {
        let vars: [&dyn Var; 25] = [
            &self.application_name,
            self.build_info,
            &self.client_encoding,
            &self.client_min_messages,
            &self.cluster,
            &self.cluster_replica,
            &self.database,
            self.date_style,
            &self.extra_float_digits,
            &self.failpoints,
            &self.integer_datetimes,
            &self.interval_style,
            &self.search_path,
            &self.server_version,
            &self.server_version_num,
            &self.sql_safe_updates,
            &self.standard_conforming_strings,
            &self.statement_timeout,
            &self.idle_in_transaction_session_timeout,
            &self.timezone,
            &self.transaction_isolation,
            &self.real_time_recency,
            &self.emit_timestamp_notice,
            &self.emit_trace_id_notice,
            &self.is_superuser,
        ];
        vars.into_iter()
    }

    /// Returns an iterator over configuration parameters (and their current
    /// values for this session) that are expected to be sent to the client when
    /// a new connection is established or when their value changes.
    pub fn notify_set(&self) -> impl Iterator<Item = &dyn Var> {
        let vars: [&dyn Var; 9] = [
            &self.application_name,
            &self.client_encoding,
            self.date_style,
            &self.integer_datetimes,
            &self.server_version,
            &self.standard_conforming_strings,
            &self.timezone,
            &self.interval_style,
            // Including `mz_version` in the notify set is a Materialize
            // extension. Doing so allows applications to detect whether they
            // are talking to Materialize or PostgreSQL without an additional
            // network roundtrip. This is known to be safe because CockroachDB
            // has an analogous extension [0].
            // [0]: https://github.com/cockroachdb/cockroach/blob/369c4057a/pkg/sql/pgwire/conn.go#L1840
            self.build_info,
        ];
        vars.into_iter()
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
            Ok(self.date_style)
        } else if name == EXTRA_FLOAT_DIGITS.name {
            Ok(&self.extra_float_digits)
        } else if name == FAILPOINTS.name {
            Ok(&self.failpoints)
        } else if name == INTEGER_DATETIMES.name {
            Ok(&self.integer_datetimes)
        } else if name == INTERVAL_STYLE.name {
            Ok(&self.interval_style)
        } else if name == MZ_VERSION_NAME {
            Ok(self.build_info)
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
        } else if name == IDLE_IN_TRANSACTION_SESSION_TIMEOUT.name {
            Ok(&self.idle_in_transaction_session_timeout)
        } else if name == TIMEZONE.name {
            Ok(&self.timezone)
        } else if name == TRANSACTION_ISOLATION.name {
            Ok(&self.transaction_isolation)
        } else if name == REAL_TIME_RECENCY.name {
            Ok(&self.real_time_recency)
        } else if name == EMIT_TIMESTAMP_NOTICE.name {
            Ok(&self.emit_timestamp_notice)
        } else if name == EMIT_TRACE_ID_NOTICE.name {
            Ok(&self.emit_trace_id_notice)
        } else if name == IS_SUPERUSER.name {
            Ok(&self.is_superuser)
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
    pub fn set(&mut self, name: &str, input: VarInput, local: bool) -> Result<(), AdapterError> {
        if name == APPLICATION_NAME.name {
            self.application_name.set(input, local)
        } else if name == CLIENT_ENCODING.name {
            match extract_single_value(input) {
                Ok(value) if UncasedStr::new(value) == CLIENT_ENCODING.value => Ok(()),
                _ => Err(AdapterError::FixedValueParameter(&CLIENT_ENCODING)),
            }
        } else if name == CLIENT_MIN_MESSAGES.name {
            if let Ok(_) = ClientSeverity::parse(input) {
                self.client_min_messages.set(input, local)
            } else {
                return Err(AdapterError::ConstrainedParameter {
                    parameter: &CLIENT_MIN_MESSAGES,
                    values: input.to_vec(),
                    valid_values: Some(ClientSeverity::valid_values()),
                });
            }
        } else if name == CLUSTER.name {
            self.cluster.set(input, local)
        } else if name == CLUSTER_REPLICA.name {
            self.cluster_replica.set(input, local)
        } else if name == DATABASE.name {
            self.database.set(input, local)
        } else if name == DATE_STYLE.name {
            let Ok(values) = Vec::<String>::parse(input) else {
                return Err(AdapterError::FixedValueParameter(&*DATE_STYLE));
            };
            for value in values {
                let value = UncasedStr::new(value.trim());
                if value != "ISO" && value != "MDY" {
                    return Err(AdapterError::FixedValueParameter(&*DATE_STYLE));
                }
            }
            Ok(())
        } else if name == EXTRA_FLOAT_DIGITS.name {
            self.extra_float_digits.set(input, local)
        } else if name == FAILPOINTS.name {
            let values = input.to_vec();
            for mut cfg in values.iter().map(|v| v.trim().split(';')).flatten() {
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
                            values: input.to_vec(),
                            reason: "missing failpoint name".into(),
                        })?;
                let action = splits
                    .next()
                    .ok_or_else(|| AdapterError::InvalidParameterValue {
                        parameter: &FAILPOINTS,
                        values: input.to_vec(),
                        reason: "missing failpoint action".into(),
                    })?;
                fail::cfg(failpoint, action).map_err(|e| AdapterError::InvalidParameterValue {
                    parameter: &FAILPOINTS,
                    values: input.to_vec(),
                    reason: e,
                })?;
            }
            Ok(())
        } else if name == INTEGER_DATETIMES.name {
            Err(AdapterError::ReadOnlyParameter(&INTEGER_DATETIMES))
        } else if name == INTERVAL_STYLE.name {
            match extract_single_value(input) {
                Ok(value) if UncasedStr::new(value) == INTERVAL_STYLE.value => Ok(()),
                _ => Err(AdapterError::FixedValueParameter(&INTERVAL_STYLE)),
            }
        } else if name == SEARCH_PATH.name {
            self.search_path.set(input, local)
        } else if name == SERVER_VERSION.name {
            Err(AdapterError::ReadOnlyParameter(&SERVER_VERSION))
        } else if name == SERVER_VERSION_NUM.name {
            Err(AdapterError::ReadOnlyParameter(&SERVER_VERSION_NUM))
        } else if name == SQL_SAFE_UPDATES.name {
            self.sql_safe_updates.set(input, local)
        } else if name == STANDARD_CONFORMING_STRINGS.name {
            match bool::parse(input) {
                Ok(value) if value == *STANDARD_CONFORMING_STRINGS.value => Ok(()),
                Ok(_) => Err(AdapterError::FixedValueParameter(
                    &STANDARD_CONFORMING_STRINGS,
                )),
                Err(()) => Err(AdapterError::InvalidParameterType(
                    &STANDARD_CONFORMING_STRINGS,
                )),
            }
        } else if name == STATEMENT_TIMEOUT.name {
            self.statement_timeout.set(input, local)
        } else if name == IDLE_IN_TRANSACTION_SESSION_TIMEOUT.name {
            self.idle_in_transaction_session_timeout.set(input, local)
        } else if name == TIMEZONE.name {
            if let Ok(_) = TimeZone::parse(input) {
                self.timezone.set(input, local)
            } else {
                Err(AdapterError::ConstrainedParameter {
                    parameter: &TIMEZONE,
                    values: input.to_vec(),
                    valid_values: None,
                })
            }
        } else if name == TRANSACTION_ISOLATION.name {
            if let Ok(_) = IsolationLevel::parse(input) {
                self.transaction_isolation.set(input, local)
            } else {
                return Err(AdapterError::ConstrainedParameter {
                    parameter: &TRANSACTION_ISOLATION,
                    values: input.to_vec(),
                    valid_values: Some(IsolationLevel::valid_values()),
                });
            }
        } else if name == REAL_TIME_RECENCY.name {
            self.real_time_recency.set(input, local)
        } else if name == EMIT_TIMESTAMP_NOTICE.name {
            self.emit_timestamp_notice.set(input, local)
        } else if name == EMIT_TRACE_ID_NOTICE.name {
            self.emit_trace_id_notice.set(input, local)
        } else if name == IS_SUPERUSER.name {
            Err(AdapterError::FixedValueParameter(&IS_SUPERUSER))
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
        } else if name == SEARCH_PATH.name {
            self.search_path.reset(local);
        } else if name == SQL_SAFE_UPDATES.name {
            self.sql_safe_updates.reset(local);
        } else if name == STATEMENT_TIMEOUT.name {
            self.statement_timeout.reset(local);
        } else if name == IDLE_IN_TRANSACTION_SESSION_TIMEOUT.name {
            self.idle_in_transaction_session_timeout.reset(local);
        } else if name == TIMEZONE.name {
            self.timezone.reset(local);
        } else if name == TRANSACTION_ISOLATION.name {
            self.transaction_isolation.reset(local);
        } else if name == REAL_TIME_RECENCY.name {
            self.real_time_recency.reset(local);
        } else if name == EMIT_TIMESTAMP_NOTICE.name {
            self.emit_timestamp_notice.reset(local);
        } else if name == EMIT_TRACE_ID_NOTICE.name {
            self.emit_trace_id_notice.reset(local);
        } else if name == CLIENT_ENCODING.name
            || name == DATE_STYLE.name
            || name == FAILPOINTS.name
            || name == INTEGER_DATETIMES.name
            || name == INTERVAL_STYLE.name
            || name == SERVER_VERSION.name
            || name == SERVER_VERSION_NUM.name
            || name == STANDARD_CONFORMING_STRINGS.name
            || name == IS_SUPERUSER.name
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
            build_info: _,
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
            search_path,
            server_version: _,
            server_version_num: _,
            sql_safe_updates,
            standard_conforming_strings: _,
            statement_timeout,
            idle_in_transaction_session_timeout,
            timezone,
            transaction_isolation,
            real_time_recency,
            emit_timestamp_notice,
            emit_trace_id_notice,
            is_superuser: _,
        } = self;
        application_name.end_transaction(action);
        client_min_messages.end_transaction(action);
        cluster.end_transaction(action);
        cluster_replica.end_transaction(action);
        database.end_transaction(action);
        extra_float_digits.end_transaction(action);
        search_path.end_transaction(action);
        sql_safe_updates.end_transaction(action);
        statement_timeout.end_transaction(action);
        idle_in_transaction_session_timeout.end_transaction(action);
        timezone.end_transaction(action);
        transaction_isolation.end_transaction(action);
        real_time_recency.end_transaction(action);
        emit_timestamp_notice.end_transaction(action);
        emit_trace_id_notice.end_transaction(action);
    }

    /// Returns the value of the `application_name` configuration parameter.
    pub fn application_name(&self) -> &str {
        self.application_name.value()
    }

    /// Returns the build info.
    pub fn build_info(&self) -> &'static BuildInfo {
        self.build_info
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
    pub fn date_style(&self) -> &[String] {
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

    /// Returns the value of the `mz_version` configuration parameter.
    pub fn mz_version(&self) -> String {
        self.build_info.value()
    }

    /// Returns the value of the `search_path` configuration parameter.
    pub fn search_path(&self) -> &[Ident] {
        self.search_path.value()
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

    /// Returns the value of the `idle_in_transaction_session_timeout` configuration parameter.
    pub fn idle_in_transaction_session_timeout(&self) -> &Duration {
        self.idle_in_transaction_session_timeout.value()
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

    /// Returns the value of `real_time_recency` configuration parameter.
    pub fn real_time_recency(&self) -> bool {
        *self.real_time_recency.value()
    }

    /// Returns the value of `emit_timestamp_notice` configuration parameter.
    pub fn emit_timestamp_notice(&self) -> bool {
        *self.emit_timestamp_notice.value()
    }

    /// Returns the value of `emit_trace_id_notice` configuration parameter.
    pub fn emit_trace_id_notice(&self) -> bool {
        *self.emit_trace_id_notice.value()
    }

    /// Returns the value of `is_superuser` configuration parameter.
    pub fn is_superuser(&self) -> bool {
        *self.is_superuser.value()
    }

    pub(crate) fn set_cluster(&mut self, cluster: String) {
        self.cluster.session_value = Some(cluster);
    }

    pub(crate) fn set_superuser(&mut self, is_superuser: bool) {
        self.is_superuser.session_value = Some(is_superuser);
    }
}

/// On disk variables.
///
/// See [`SessionVars`] for more details on the Materialize configuration model.
#[derive(Debug, Clone)]
pub struct SystemVars {
    // internal bookkeeping
    config_has_synced_once: SystemVar<bool>,

    // limits
    max_aws_privatelink_connections: SystemVar<u32>,
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
    allowed_cluster_replica_sizes: SystemVar<Vec<Ident>>,

    // features
    enable_with_mutually_recursive: SystemVar<bool>,

    // persist configuration
    persist_blob_target_size: SystemVar<usize>,
    persist_compaction_minimum_timeout: SystemVar<Duration>,
    persist_sink_minimum_batch_updates: SystemVar<usize>,

    // crdb configuration
    crdb_connect_timeout: SystemVar<Duration>,

    // dataflow configuration
    dataflow_max_inflight_bytes: SystemVar<usize>,

    // misc
    metrics_retention: SystemVar<Duration>,

    // testing
    mock_audit_event_timestamp: SystemVar<Option<mz_repr::Timestamp>>,
}

impl Default for SystemVars {
    fn default() -> Self {
        SystemVars {
            config_has_synced_once: SystemVar::new(&CONFIG_HAS_SYNCED_ONCE),
            max_aws_privatelink_connections: SystemVar::new(&MAX_AWS_PRIVATELINK_CONNECTIONS),
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
            allowed_cluster_replica_sizes: SystemVar::new(&ALLOWED_CLUSTER_REPLICA_SIZES),
            persist_blob_target_size: SystemVar::new(&PERSIST_BLOB_TARGET_SIZE),
            persist_compaction_minimum_timeout: SystemVar::new(&PERSIST_COMPACTION_MINIMUM_TIMEOUT),
            crdb_connect_timeout: SystemVar::new(&CRDB_CONNECT_TIMEOUT),
            dataflow_max_inflight_bytes: SystemVar::new(&DATAFLOW_MAX_INFLIGHT_BYTES),
            persist_sink_minimum_batch_updates: SystemVar::new(&PERSIST_SINK_MINIMUM_BATCH_UPDATES),
            metrics_retention: SystemVar::new(&METRICS_RETENTION),
            mock_audit_event_timestamp: SystemVar::new(&MOCK_AUDIT_EVENT_TIMESTAMP),
            enable_with_mutually_recursive: SystemVar::new(&ENABLE_WITH_MUTUALLY_RECURSIVE),
        }
    }
}

impl SystemVars {
    /// Returns an iterator over the configuration parameters and their current
    /// values on disk.
    pub fn iter(&self) -> impl Iterator<Item = &dyn Var> {
        let vars: [&dyn Var; 22] = [
            &self.config_has_synced_once,
            &self.max_aws_privatelink_connections,
            &self.max_tables,
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
            &self.allowed_cluster_replica_sizes,
            &self.persist_blob_target_size,
            &self.persist_compaction_minimum_timeout,
            &self.crdb_connect_timeout,
            &self.dataflow_max_inflight_bytes,
            &self.persist_sink_minimum_batch_updates,
            &self.metrics_retention,
            &self.mock_audit_event_timestamp,
            &self.enable_with_mutually_recursive,
        ];
        vars.into_iter()
    }

    /// Returns an iterator over the configuration parameters and their current
    /// values on disk. Compared to [`SystemVars::iter`], this should omit vars
    /// that shouldn't be synced by [`crate::config::SystemParameterFrontend`].
    pub fn iter_synced(&self) -> impl Iterator<Item = &dyn Var> {
        self.iter()
            .filter(|v| v.name() != CONFIG_HAS_SYNCED_ONCE.name)
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
    ///
    /// # Errors
    ///
    /// The call will return an error:
    /// 1. If `name` does not refer to a valid [`SystemVars`] field.
    pub fn get(&self, name: &str) -> Result<&dyn Var, AdapterError> {
        if name == CONFIG_HAS_SYNCED_ONCE.name {
            Ok(&self.config_has_synced_once)
        } else if name == MAX_AWS_PRIVATELINK_CONNECTIONS.name {
            Ok(&self.max_aws_privatelink_connections)
        } else if name == MAX_TABLES.name {
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
        } else if name == ALLOWED_CLUSTER_REPLICA_SIZES.name {
            Ok(&self.allowed_cluster_replica_sizes)
        } else if name == PERSIST_BLOB_TARGET_SIZE.name {
            Ok(&self.persist_blob_target_size)
        } else if name == PERSIST_COMPACTION_MINIMUM_TIMEOUT.name {
            Ok(&self.persist_compaction_minimum_timeout)
        } else if name == CRDB_CONNECT_TIMEOUT.name {
            Ok(&self.crdb_connect_timeout)
        } else if name == DATAFLOW_MAX_INFLIGHT_BYTES.name {
            Ok(&self.dataflow_max_inflight_bytes)
        } else if name == PERSIST_SINK_MINIMUM_BATCH_UPDATES.name {
            Ok(&self.persist_sink_minimum_batch_updates)
        } else if name == METRICS_RETENTION.name {
            Ok(&self.metrics_retention)
        } else if name == MOCK_AUDIT_EVENT_TIMESTAMP.name {
            Ok(&self.mock_audit_event_timestamp)
        } else if name == ENABLE_WITH_MUTUALLY_RECURSIVE.name {
            Ok(&self.enable_with_mutually_recursive)
        } else {
            Err(AdapterError::UnknownParameter(name.into()))
        }
    }

    /// Check if the given `values` is the default value for the [`Var`]
    /// identified by `name`.
    ///
    /// # Errors
    ///
    /// The call will return an error:
    /// 1. If `name` does not refer to a valid [`SystemVars`] field.
    /// 2. If `values` does not represent a valid [`SystemVars`] value for
    ///    `name`.
    pub fn is_default(&self, name: &str, input: VarInput) -> Result<bool, AdapterError> {
        if name == CONFIG_HAS_SYNCED_ONCE.name {
            self.config_has_synced_once.is_default(input)
        } else if name == MAX_AWS_PRIVATELINK_CONNECTIONS.name {
            self.max_aws_privatelink_connections.is_default(input)
        } else if name == MAX_TABLES.name {
            self.max_tables.is_default(input)
        } else if name == MAX_SOURCES.name {
            self.max_sources.is_default(input)
        } else if name == MAX_SINKS.name {
            self.max_sinks.is_default(input)
        } else if name == MAX_MATERIALIZED_VIEWS.name {
            self.max_materialized_views.is_default(input)
        } else if name == MAX_CLUSTERS.name {
            self.max_clusters.is_default(input)
        } else if name == MAX_REPLICAS_PER_CLUSTER.name {
            self.max_replicas_per_cluster.is_default(input)
        } else if name == MAX_DATABASES.name {
            self.max_databases.is_default(input)
        } else if name == MAX_SCHEMAS_PER_DATABASE.name {
            self.max_schemas_per_database.is_default(input)
        } else if name == MAX_OBJECTS_PER_SCHEMA.name {
            self.max_objects_per_schema.is_default(input)
        } else if name == MAX_SECRETS.name {
            self.max_secrets.is_default(input)
        } else if name == MAX_ROLES.name {
            self.max_roles.is_default(input)
        } else if name == MAX_RESULT_SIZE.name {
            self.max_result_size.is_default(input)
        } else if name == ALLOWED_CLUSTER_REPLICA_SIZES.name {
            self.allowed_cluster_replica_sizes.is_default(input)
        } else if name == PERSIST_BLOB_TARGET_SIZE.name {
            self.persist_blob_target_size.is_default(input)
        } else if name == PERSIST_COMPACTION_MINIMUM_TIMEOUT.name {
            self.persist_compaction_minimum_timeout.is_default(input)
        } else if name == CRDB_CONNECT_TIMEOUT.name {
            self.crdb_connect_timeout.is_default(input)
        } else if name == DATAFLOW_MAX_INFLIGHT_BYTES.name {
            self.dataflow_max_inflight_bytes.is_default(input)
        } else if name == PERSIST_SINK_MINIMUM_BATCH_UPDATES.name {
            self.persist_sink_minimum_batch_updates.is_default(input)
        } else if name == METRICS_RETENTION.name {
            self.metrics_retention.is_default(input)
        } else if name == MOCK_AUDIT_EVENT_TIMESTAMP.name {
            self.mock_audit_event_timestamp.is_default(input)
        } else if name == ENABLE_WITH_MUTUALLY_RECURSIVE.name {
            self.enable_with_mutually_recursive.is_default(input)
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
    ///
    /// Return a `bool` value indicating whether the [`Var`] identified by
    /// `name` was modified by this call (it won't be if it already had the
    /// given `value`).
    ///
    /// # Errors
    ///
    /// The call will return an error:
    /// 1. If `name` does not refer to a valid [`SystemVars`] field.
    /// 2. If `value` does not represent a valid [`SystemVars`] value for
    ///    `name`.
    pub fn set(&mut self, name: &str, input: VarInput) -> Result<bool, AdapterError> {
        if name == CONFIG_HAS_SYNCED_ONCE.name {
            self.config_has_synced_once.set(input)
        } else if name == MAX_AWS_PRIVATELINK_CONNECTIONS.name {
            self.max_aws_privatelink_connections.set(input)
        } else if name == MAX_TABLES.name {
            self.max_tables.set(input)
        } else if name == MAX_SOURCES.name {
            self.max_sources.set(input)
        } else if name == MAX_SINKS.name {
            self.max_sinks.set(input)
        } else if name == MAX_MATERIALIZED_VIEWS.name {
            self.max_materialized_views.set(input)
        } else if name == MAX_CLUSTERS.name {
            self.max_clusters.set(input)
        } else if name == MAX_REPLICAS_PER_CLUSTER.name {
            self.max_replicas_per_cluster.set(input)
        } else if name == MAX_DATABASES.name {
            self.max_databases.set(input)
        } else if name == MAX_SCHEMAS_PER_DATABASE.name {
            self.max_schemas_per_database.set(input)
        } else if name == MAX_OBJECTS_PER_SCHEMA.name {
            self.max_objects_per_schema.set(input)
        } else if name == MAX_SECRETS.name {
            self.max_secrets.set(input)
        } else if name == MAX_ROLES.name {
            self.max_roles.set(input)
        } else if name == MAX_RESULT_SIZE.name {
            self.max_result_size.set(input)
        } else if name == ALLOWED_CLUSTER_REPLICA_SIZES.name {
            self.allowed_cluster_replica_sizes.set(input)
        } else if name == PERSIST_BLOB_TARGET_SIZE.name {
            self.persist_blob_target_size.set(input)
        } else if name == PERSIST_COMPACTION_MINIMUM_TIMEOUT.name {
            self.persist_compaction_minimum_timeout.set(input)
        } else if name == CRDB_CONNECT_TIMEOUT.name {
            self.crdb_connect_timeout.set(input)
        } else if name == DATAFLOW_MAX_INFLIGHT_BYTES.name {
            self.dataflow_max_inflight_bytes.set(input)
        } else if name == PERSIST_SINK_MINIMUM_BATCH_UPDATES.name {
            self.persist_sink_minimum_batch_updates.set(input)
        } else if name == METRICS_RETENTION.name {
            self.metrics_retention.set(input)
        } else if name == MOCK_AUDIT_EVENT_TIMESTAMP.name {
            self.mock_audit_event_timestamp.set(input)
        } else if name == ENABLE_WITH_MUTUALLY_RECURSIVE.name {
            self.enable_with_mutually_recursive.set(input)
        } else {
            Err(AdapterError::UnknownParameter(name.into()))
        }
    }

    /// Sets the configuration parameter named `name` to its default value.
    ///
    /// Like with [`SystemVars::get`], configuration parameters are matched case
    /// insensitively. If the named configuration parameter does not exist, an
    /// error is returned.
    ///
    /// Return a `bool` value indicating whether the [`Var`] identified by
    /// `name` was modified by this call (it won't be if was already reset).
    ///
    /// # Errors
    ///
    /// The call will return an error:
    /// 1. If `name` does not refer to a valid [`SystemVars`] field.
    pub fn reset(&mut self, name: &str) -> Result<bool, AdapterError> {
        if name == CONFIG_HAS_SYNCED_ONCE.name {
            Ok(self.config_has_synced_once.reset())
        } else if name == MAX_AWS_PRIVATELINK_CONNECTIONS.name {
            Ok(self.max_aws_privatelink_connections.reset())
        } else if name == MAX_TABLES.name {
            Ok(self.max_tables.reset())
        } else if name == MAX_SOURCES.name {
            Ok(self.max_sources.reset())
        } else if name == MAX_SINKS.name {
            Ok(self.max_sinks.reset())
        } else if name == MAX_MATERIALIZED_VIEWS.name {
            Ok(self.max_materialized_views.reset())
        } else if name == MAX_CLUSTERS.name {
            Ok(self.max_clusters.reset())
        } else if name == MAX_REPLICAS_PER_CLUSTER.name {
            Ok(self.max_replicas_per_cluster.reset())
        } else if name == MAX_DATABASES.name {
            Ok(self.max_databases.reset())
        } else if name == MAX_SCHEMAS_PER_DATABASE.name {
            Ok(self.max_schemas_per_database.reset())
        } else if name == MAX_OBJECTS_PER_SCHEMA.name {
            Ok(self.max_objects_per_schema.reset())
        } else if name == MAX_SECRETS.name {
            Ok(self.max_secrets.reset())
        } else if name == MAX_ROLES.name {
            Ok(self.max_roles.reset())
        } else if name == MAX_RESULT_SIZE.name {
            Ok(self.max_result_size.reset())
        } else if name == ALLOWED_CLUSTER_REPLICA_SIZES.name {
            Ok(self.allowed_cluster_replica_sizes.reset())
        } else if name == PERSIST_BLOB_TARGET_SIZE.name {
            Ok(self.persist_blob_target_size.reset())
        } else if name == PERSIST_COMPACTION_MINIMUM_TIMEOUT.name {
            Ok(self.persist_compaction_minimum_timeout.reset())
        } else if name == CRDB_CONNECT_TIMEOUT.name {
            Ok(self.crdb_connect_timeout.reset())
        } else if name == DATAFLOW_MAX_INFLIGHT_BYTES.name {
            Ok(self.dataflow_max_inflight_bytes.reset())
        } else if name == PERSIST_SINK_MINIMUM_BATCH_UPDATES.name {
            Ok(self.persist_sink_minimum_batch_updates.reset())
        } else if name == METRICS_RETENTION.name {
            Ok(self.metrics_retention.reset())
        } else if name == MOCK_AUDIT_EVENT_TIMESTAMP.name {
            Ok(self.mock_audit_event_timestamp.reset())
        } else if name == ENABLE_WITH_MUTUALLY_RECURSIVE.name {
            Ok(self.enable_with_mutually_recursive.reset())
        } else {
            Err(AdapterError::UnknownParameter(name.into()))
        }
    }

    /// Returns the `config_has_synced_once` configuration parameter.
    pub fn config_has_synced_once(&self) -> bool {
        *self.config_has_synced_once.value()
    }

    /// Returns the value of the `max_aws_privatelink_connections` configuration parameter.
    pub fn max_aws_privatelink_connections(&self) -> u32 {
        *self.max_aws_privatelink_connections.value()
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

    /// Returns the value of the `allowed_cluster_replica_sizes` configuration parameter.
    pub fn allowed_cluster_replica_sizes(&self) -> Vec<String> {
        self.allowed_cluster_replica_sizes
            .value()
            .into_iter()
            .map(|s| s.as_str().into())
            .collect()
    }

    /// Returns the `persist_blob_target_size` configuration parameter.
    pub fn persist_blob_target_size(&self) -> usize {
        *self.persist_blob_target_size.value()
    }

    /// Returns the `persist_compaction_minimum_timeout` configuration parameter.
    pub fn persist_compaction_minimum_timeout(&self) -> Duration {
        *self.persist_compaction_minimum_timeout.value()
    }

    /// Returns the `crdb_connect_timeout` configuration parameter.
    pub fn crdb_connect_timeout(&self) -> Duration {
        *self.crdb_connect_timeout.value()
    }

    /// Returns the `dataflow_max_inflight_bytes` configuration parameter.
    pub fn dataflow_max_inflight_bytes(&self) -> usize {
        *self.dataflow_max_inflight_bytes.value()
    }

    /// Returns the `persist_sink_minimum_batch_updates` configuration parameter.
    pub fn persist_sink_minimum_batch_updates(&self) -> usize {
        *self.persist_sink_minimum_batch_updates.value()
    }

    /// Returns the `metrics_retention` configuration parameter.
    pub fn metrics_retention(&self) -> Duration {
        *self.metrics_retention.value()
    }

    /// Returns the `mock_audit_event_timestamp` configuration parameter.
    pub fn mock_audit_event_timestamp(&self) -> Option<mz_repr::Timestamp> {
        *self.mock_audit_event_timestamp.value()
    }

    /// Returns the `enable_with_mutually_recursive` configuration parameter.
    pub fn enable_with_mutually_recursive(&self) -> bool {
        *self.enable_with_mutually_recursive.value()
    }

    /// Sets the `enable_with_mutually_recursive` configuration parameter.
    pub fn set_enable_with_mutually_recursive(&mut self, value: bool) -> bool {
        self.enable_with_mutually_recursive
            .set(VarInput::Flat(value.format().as_str()))
            .expect("valid parameter value")
    }
}

/// A `Var` represents a configuration parameter of an arbitrary type.
pub trait Var: fmt::Debug {
    /// Returns the name of the configuration parameter.
    fn name(&self) -> &'static str;

    /// Constructs a flattened string representation of the current value of the
    /// configuration parameter.
    ///
    /// The resulting string is guaranteed to be parsable if provided to
    /// `Value::parse` as a [`VarInput::Flat`].
    fn value(&self) -> String;

    /// Returns a short sentence describing the purpose of the configuration
    /// parameter.
    fn description(&self) -> &'static str;

    /// Returns the name of the type of this variable.
    fn type_name(&self) -> &'static str;

    /// Indicates wither the [`Var`] is visible for the given [`User`].
    ///
    /// Variables marked as `internal` are only visible for the
    /// [`crate::catalog::SYSTEM_USER`] user.
    fn visible(&self, user: &User) -> bool;

    /// Indicates wither the [`Var`] is only visible in unsafe mode.
    ///
    /// Variables marked as `safe` are visible outside of unsafe mode.
    fn safe(&self) -> bool;

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
pub struct ServerVar<V>
where
    V: fmt::Debug + ?Sized + 'static,
{
    name: &'static UncasedStr,
    value: &'static V,
    description: &'static str,
    internal: bool,
    safe: bool,
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

    fn visible(&self, user: &User) -> bool {
        !self.internal || user == &*SYSTEM_USER
    }

    fn safe(&self) -> bool {
        self.safe
    }
}

/// A `SystemVar` is persisted on disk value for a configuration parameter. If unset,
/// the server default is used instead.
#[derive(Debug, Clone)]
struct SystemVar<V>
where
    V: Value + fmt::Debug + ?Sized + 'static,
    V::Owned: fmt::Debug + PartialEq + Eq,
{
    persisted_value: Option<V::Owned>,
    parent: &'static ServerVar<V>,
}

impl<V> SystemVar<V>
where
    V: Value + fmt::Debug + PartialEq + Eq + ?Sized + 'static,
    V::Owned: fmt::Debug + PartialEq + Eq + PartialEq + Eq,
{
    fn new(parent: &'static ServerVar<V>) -> SystemVar<V> {
        SystemVar {
            persisted_value: None,
            parent,
        }
    }

    fn set(&mut self, input: VarInput) -> Result<bool, AdapterError> {
        match V::parse(input) {
            Ok(v) => {
                if self.persisted_value.as_ref() != Some(&v) {
                    self.persisted_value = Some(v);
                    Ok(true)
                } else {
                    Ok(false)
                }
            }
            Err(()) => Err(AdapterError::InvalidParameterType(self.parent)),
        }
    }

    fn reset(&mut self) -> bool {
        if self.persisted_value.as_ref() != None {
            self.persisted_value = None;
            true
        } else {
            false
        }
    }

    fn value(&self) -> &V {
        self.persisted_value
            .as_ref()
            .map(|v| v.borrow())
            .unwrap_or(self.parent.value)
    }

    fn is_default(&self, input: VarInput) -> Result<bool, AdapterError> {
        match V::parse(input) {
            Ok(v) => Ok(self.parent.value == v.borrow()),
            Err(()) => Err(AdapterError::InvalidParameterType(self.parent)),
        }
    }
}

impl<V> Var for SystemVar<V>
where
    V: Value + ToOwned + fmt::Debug + PartialEq + Eq + ?Sized + 'static,
    V::Owned: fmt::Debug + PartialEq + Eq + PartialEq + Eq,
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

    fn visible(&self, user: &User) -> bool {
        self.parent.visible(user)
    }

    fn safe(&self) -> bool {
        self.parent.safe()
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
            default_value: parent.value,
            local_value: None,
            staged_value: None,
            session_value: None,
            parent,
        }
    }

    fn set(&mut self, input: VarInput, local: bool) -> Result<(), AdapterError> {
        match V::parse(input) {
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

    fn visible(&self, user: &User) -> bool {
        self.parent.visible(user)
    }

    fn safe(&self) -> bool {
        self.parent.safe()
    }
}

impl Var for BuildInfo {
    fn name(&self) -> &'static str {
        "mz_version"
    }

    fn value(&self) -> String {
        self.human_version()
    }

    fn description(&self) -> &'static str {
        "Shows the Materialize server version (Materialize)."
    }

    fn type_name(&self) -> &'static str {
        str::TYPE_NAME
    }

    fn visible(&self, _: &User) -> bool {
        true
    }

    fn safe(&self) -> bool {
        true
    }
}

/// A value that can be stored in a session or server variable.
pub trait Value: ToOwned + Send + Sync {
    /// The name of the value type.
    const TYPE_NAME: &'static str;
    /// Parses a value of this type from a [`VarInput`].
    fn parse(input: VarInput) -> Result<Self::Owned, ()>;
    /// Formats this value as a flattened string.
    ///
    /// The resulting string is guaranteed to be parsable if provided to
    /// [`Value::parse`] as a [`VarInput::Flat`].
    fn format(&self) -> String;
}

fn extract_single_value(input: VarInput) -> Result<&str, ()> {
    match input {
        VarInput::Flat(value) => Ok(value),
        VarInput::SqlSet([value]) => Ok(value),
        _ => Err(()),
    }
}

impl Value for bool {
    const TYPE_NAME: &'static str = "boolean";

    fn parse(input: VarInput) -> Result<Self, ()> {
        let s = extract_single_value(input)?;
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

    fn parse(input: VarInput) -> Result<i32, ()> {
        let s = extract_single_value(input)?;
        s.parse().map_err(|_| ())
    }

    fn format(&self) -> String {
        self.to_string()
    }
}

impl Value for u32 {
    const TYPE_NAME: &'static str = "unsigned integer";

    fn parse(input: VarInput) -> Result<u32, ()> {
        let s = extract_single_value(input)?;
        s.parse().map_err(|_| ())
    }

    fn format(&self) -> String {
        self.to_string()
    }
}

impl Value for mz_repr::Timestamp {
    const TYPE_NAME: &'static str = "mz-timestamp";

    fn parse(input: VarInput) -> Result<mz_repr::Timestamp, ()> {
        let s = extract_single_value(input)?;
        s.parse().map_err(|_| ())
    }

    fn format(&self) -> String {
        self.to_string()
    }
}

impl Value for usize {
    const TYPE_NAME: &'static str = "unsigned integer";

    fn parse(input: VarInput) -> Result<usize, ()> {
        let s = extract_single_value(input)?;
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

    fn parse(input: VarInput) -> Result<Duration, ()> {
        let s = extract_single_value(input)?;
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
        assert!(Duration::parse(VarInput::Flat(t)).is_err());
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

    fn parse(input: VarInput) -> Result<String, ()> {
        let s = extract_single_value(input)?;
        Ok(s.to_owned())
    }

    fn format(&self) -> String {
        self.to_owned()
    }
}

impl Value for Vec<String> {
    const TYPE_NAME: &'static str = "string list";

    fn parse(input: VarInput) -> Result<Vec<String>, ()> {
        match input {
            VarInput::Flat(v) => mz_sql_parser::parser::split_identifier_string(v).map_err(|_| ()),
            // Unlike parsing `Vec<Ident>`, we further split each element.
            // This matches PostgreSQL.
            VarInput::SqlSet(values) => {
                let mut out = vec![];
                for v in values {
                    let idents =
                        mz_sql_parser::parser::split_identifier_string(v).map_err(|_| ())?;
                    out.extend(idents)
                }
                Ok(out)
            }
        }
    }

    fn format(&self) -> String {
        self.join(", ")
    }
}

impl Value for Vec<Ident> {
    const TYPE_NAME: &'static str = "identifier list";

    fn parse(input: VarInput) -> Result<Vec<Ident>, ()> {
        let holder;
        let values = match input {
            VarInput::Flat(value) => {
                holder = mz_sql_parser::parser::split_identifier_string(value).map_err(|_| ())?;
                &holder
            }
            // Unlike parsing `Vec<String>`, we do *not* further split each
            // element. This matches PostgreSQL.
            VarInput::SqlSet(values) => values,
        };
        Ok(values.iter().map(Ident::new).collect())
    }

    fn format(&self) -> String {
        self.iter().map(|ident| ident.to_string()).join(", ")
    }
}

impl Value for Option<String> {
    const TYPE_NAME: &'static str = "optional string";

    fn parse(input: VarInput) -> Result<Option<String>, ()> {
        let s = extract_single_value(input)?;
        match s {
            "" => Ok(None),
            _ => Ok(Some(s.to_string())),
        }
    }

    fn format(&self) -> String {
        match self {
            Some(s) => s.format(),
            None => "".into(),
        }
    }
}

impl Value for Option<mz_repr::Timestamp> {
    const TYPE_NAME: &'static str = "optional unsigned integer";

    fn parse(input: VarInput) -> Result<Option<mz_repr::Timestamp>, ()> {
        let s = extract_single_value(input)?;
        match s {
            "" => Ok(None),
            _ => <mz_repr::Timestamp as Value>::parse(VarInput::Flat(s)).map(Some),
        }
    }

    fn format(&self) -> String {
        match self {
            Some(s) => s.format(),
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
}

impl Value for ClientSeverity {
    const TYPE_NAME: &'static str = "string";

    fn parse(input: VarInput) -> Result<Self::Owned, ()> {
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

    fn parse(input: VarInput) -> Result<Self::Owned, ()> {
        let s = extract_single_value(input)?;
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
    pub fn as_str(&self) -> &'static str {
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

    fn parse(input: VarInput) -> Result<Self::Owned, ()> {
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

/// Returns whether the named variable is a compute configuration parameter.
pub(crate) fn is_compute_config_var(name: &str) -> bool {
    name == MAX_RESULT_SIZE.name()
        || name == DATAFLOW_MAX_INFLIGHT_BYTES.name()
        || is_persist_config_var(name)
}

/// Returns whether the named variable is a storage configuration parameter.
pub(crate) fn is_storage_config_var(name: &str) -> bool {
    is_persist_config_var(name)
}

/// Returns whether the named variable is a persist configuration parameter.
fn is_persist_config_var(name: &str) -> bool {
    name == PERSIST_BLOB_TARGET_SIZE.name()
        || name == PERSIST_COMPACTION_MINIMUM_TIMEOUT.name()
        || name == CRDB_CONNECT_TIMEOUT.name()
        || name == PERSIST_SINK_MINIMUM_BATCH_UPDATES.name()
}

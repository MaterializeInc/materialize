// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::borrow::Cow;
use std::str::FromStr;
use std::sync::Arc;
use std::sync::LazyLock;
use std::time::Duration;

use chrono::{DateTime, Utc};
use derivative::Derivative;
use mz_adapter_types::timestamp_oracle::{
    DEFAULT_PG_TIMESTAMP_ORACLE_CONNPOOL_MAX_SIZE, DEFAULT_PG_TIMESTAMP_ORACLE_CONNPOOL_MAX_WAIT,
    DEFAULT_PG_TIMESTAMP_ORACLE_CONNPOOL_TTL, DEFAULT_PG_TIMESTAMP_ORACLE_CONNPOOL_TTL_STAGGER,
};
use mz_ore::cast::{self, CastFrom};
use mz_repr::adt::numeric::Numeric;
use mz_repr::adt::timestamp::CheckedTimestamp;
use mz_repr::bytes::ByteSize;
use mz_repr::optimize::OptimizerFeatures;
use mz_sql_parser::ast::Ident;
use mz_sql_parser::ident;
use mz_storage_types::parameters::{
    DEFAULT_PG_SOURCE_CONNECT_TIMEOUT, DEFAULT_PG_SOURCE_TCP_CONFIGURE_SERVER,
    DEFAULT_PG_SOURCE_TCP_KEEPALIVES_IDLE, DEFAULT_PG_SOURCE_TCP_KEEPALIVES_INTERVAL,
    DEFAULT_PG_SOURCE_TCP_KEEPALIVES_RETRIES, DEFAULT_PG_SOURCE_TCP_USER_TIMEOUT,
    DEFAULT_PG_SOURCE_WAL_SENDER_TIMEOUT, STORAGE_MANAGED_COLLECTIONS_BATCH_DURATION_DEFAULT,
};
use mz_tracing::{CloneableEnvFilter, SerializableDirective};
use uncased::UncasedStr;

use crate::session::user::{User, SUPPORT_USER, SYSTEM_USER};
use crate::session::vars::constraints::{
    DomainConstraint, ValueConstraint, BYTESIZE_AT_LEAST_1MB, NUMERIC_BOUNDED_0_1_INCLUSIVE,
    NUMERIC_NON_NEGATIVE,
};
use crate::session::vars::errors::VarError;
use crate::session::vars::polyfill::{lazy_value, value, LazyValueFn};
use crate::session::vars::value::{
    ClientEncoding, ClientSeverity, Failpoints, IntervalStyle, IsolationLevel, TimeZone, Value,
    DEFAULT_DATE_STYLE,
};
use crate::session::vars::{FeatureFlag, Var, VarInput, VarParseError};
use crate::{DEFAULT_SCHEMA, WEBHOOK_CONCURRENCY_LIMIT};

/// Definition of a variable.
#[derive(Clone, Derivative)]
#[derivative(Debug)]
pub struct VarDefinition {
    /// Name of the variable, case-insensitive matching.
    pub name: &'static UncasedStr,
    /// Description of the variable.
    pub description: &'static str,
    /// Is the variable visible to users, when false only visible to system users.
    pub user_visible: bool,

    /// Default compiled in value for this variable.
    pub value: VarDefaultValue,
    /// Constraint that must be upheld for this variable to be valid.
    pub constraint: Option<ValueConstraint>,
    /// Optionally hides this variable if it's related to a feature flag being enabled.
    pub feature_flag: Option<&'static FeatureFlag>,

    /// Method to parse [`VarInput`] into a type that implements [`Value`].
    ///
    /// The reason `parse` exists as a function pointer is because we want to achieve two things:
    ///   1. `VarDefinition` has no generic parameters.
    ///   2. `Value::parse` returns an instance of `Self`.
    /// `VarDefinition` holds a `dyn Value`, but `Value::parse` is not object safe because it
    /// returns `Self`, so we can't call that method. We could change `Value::parse` to return a
    /// `Box<dyn Value>` making it object safe, but that creates a footgun where it's possible for
    /// `Value::parse` to return a type that isn't `Self`, e.g. `<String as Value>::parse` could
    /// return a `usize`!
    ///
    /// So to prevent making `VarDefinition` generic over some type `V: Value`, but also defining
    /// `Value::parse` as returning `Self`, we store a static function pointer to the `parse`
    /// implementation of our default value.
    #[derivative(Debug = "ignore")]
    parse: fn(VarInput) -> Result<Box<dyn Value>, VarParseError>,
    /// Returns a human readable name for the type of this variable. We store this as a static
    /// function pointer for the same reason as `parse`.
    #[derivative(Debug = "ignore")]
    type_name: fn() -> Cow<'static, str>,
}
static_assertions::assert_impl_all!(VarDefinition: Send, Sync);

impl VarDefinition {
    /// Create a new [`VarDefinition`] in a const context with a value known at compile time.
    pub const fn new<V: Value>(
        name: &'static str,
        value: &'static V,
        description: &'static str,
        user_visible: bool,
    ) -> Self {
        VarDefinition {
            name: UncasedStr::new(name),
            description,
            value: VarDefaultValue::Static(value),
            user_visible,
            parse: V::parse_dyn_value,
            type_name: V::type_name,
            constraint: None,
            feature_flag: None,
        }
    }

    /// Create a new [`VarDefinition`] in a const context with a lazily evaluated value.
    pub const fn new_lazy<V: Value, L: LazyValueFn<V>>(
        name: &'static str,
        _value: L,
        description: &'static str,
        user_visible: bool,
    ) -> Self {
        VarDefinition {
            name: UncasedStr::new(name),
            description,
            value: VarDefaultValue::Lazy(L::LAZY_VALUE_FN),
            user_visible,
            parse: V::parse_dyn_value,
            type_name: V::type_name,
            constraint: None,
            feature_flag: None,
        }
    }

    /// Create a new [`VarDefinition`] with a value known at runtime.
    pub fn new_runtime<V: Value>(
        name: &'static str,
        value: V,
        description: &'static str,
        user_visible: bool,
    ) -> Self {
        VarDefinition {
            name: UncasedStr::new(name),
            description,
            value: VarDefaultValue::Runtime(Arc::new(value)),
            user_visible,
            parse: V::parse_dyn_value,
            type_name: V::type_name,
            constraint: None,
            feature_flag: None,
        }
    }

    /// TODO(parkmycar): Refactor this method onto a `VarDefinitionBuilder` that would allow us to
    /// constrain `V` here to be the same `V` used in [`VarDefinition::new`].
    pub const fn with_constraint<V: Value, D: DomainConstraint<Value = V>>(
        mut self,
        constraint: &'static D,
    ) -> Self {
        self.constraint = Some(ValueConstraint::Domain(constraint));
        self
    }

    pub const fn fixed(mut self) -> Self {
        self.constraint = Some(ValueConstraint::Fixed);
        self
    }

    pub const fn read_only(mut self) -> Self {
        self.constraint = Some(ValueConstraint::ReadOnly);
        self
    }

    pub const fn with_feature_flag(mut self, feature_flag: &'static FeatureFlag) -> Self {
        self.feature_flag = Some(feature_flag);
        self
    }

    pub fn parse(&self, input: VarInput) -> Result<Box<dyn Value>, VarError> {
        (self.parse)(input).map_err(|err| err.into_var_error(self))
    }

    pub fn default_value(&self) -> &'_ dyn Value {
        self.value.value()
    }
}

impl Var for VarDefinition {
    fn name(&self) -> &'static str {
        self.name.as_str()
    }

    fn value(&self) -> String {
        self.default_value().format()
    }

    fn description(&self) -> &'static str {
        self.description
    }

    fn type_name(&self) -> Cow<'static, str> {
        (self.type_name)()
    }

    fn visible(
        &self,
        user: &User,
        system_vars: Option<&super::SystemVars>,
    ) -> Result<(), VarError> {
        if !self.user_visible && user != &*SYSTEM_USER && user != &*SUPPORT_USER {
            Err(VarError::UnknownParameter(self.name().to_string()))
        } else if self.name().starts_with("unsafe")
            && match system_vars {
                None => true,
                Some(system_vars) => !system_vars.allow_unsafe(),
            }
        {
            Err(VarError::RequiresUnsafeMode(self.name()))
        } else {
            if let Some(flag) = self.feature_flag {
                flag.enabled(system_vars, None, None)?;
            }

            Ok(())
        }
    }
}

/// The kinds of compiled in default values that can be used with [`VarDefinition`].
#[derive(Clone, Debug)]
pub enum VarDefaultValue {
    /// Static that can be evaluated at compile time.
    Static(&'static dyn Value),
    /// Lazy value that is defined at compile time, but created at runtime.
    Lazy(fn() -> &'static dyn Value),
    /// Value created at runtime. Note: This is generally an escape hatch.
    Runtime(Arc<dyn Value>),
}

impl VarDefaultValue {
    pub fn value(&self) -> &'_ dyn Value {
        match self {
            VarDefaultValue::Static(s) => *s,
            VarDefaultValue::Lazy(l) => (l)(),
            VarDefaultValue::Runtime(r) => r.as_ref(),
        }
    }
}

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

pub static APPLICATION_NAME: VarDefinition = VarDefinition::new(
    "application_name",
    value!(String; String::new()),
    "Sets the application name to be reported in statistics and logs (PostgreSQL).",
    true,
);

pub static CLIENT_ENCODING: VarDefinition = VarDefinition::new(
    "client_encoding",
    value!(ClientEncoding; ClientEncoding::Utf8),
    "Sets the client's character set encoding (PostgreSQL).",
    true,
);

pub static CLIENT_MIN_MESSAGES: VarDefinition = VarDefinition::new(
    "client_min_messages",
    value!(ClientSeverity; ClientSeverity::Notice),
    "Sets the message levels that are sent to the client (PostgreSQL).",
    true,
);

pub static CLUSTER: VarDefinition = VarDefinition::new_lazy(
    "cluster",
    lazy_value!(String; || "quickstart".to_string()),
    "Sets the current cluster (Materialize).",
    true,
);

pub static CLUSTER_REPLICA: VarDefinition = VarDefinition::new(
    "cluster_replica",
    value!(Option<String>; None),
    "Sets a target cluster replica for SELECT queries (Materialize).",
    true,
);

pub static CURRENT_OBJECT_MISSING_WARNINGS: VarDefinition = VarDefinition::new(
    "current_object_missing_warnings",
    value!(bool; true),
    "Whether to emit warnings when the current database, schema, or cluster is missing (Materialize).",
    true,
);

pub static DATABASE: VarDefinition = VarDefinition::new_lazy(
    "database",
    lazy_value!(String; || DEFAULT_DATABASE_NAME.to_string()),
    "Sets the current database (CockroachDB).",
    true,
);

pub static DATE_STYLE: VarDefinition = VarDefinition::new(
    // DateStyle has nonstandard capitalization for historical reasons.
    "DateStyle",
    &DEFAULT_DATE_STYLE,
    "Sets the display format for date and time values (PostgreSQL).",
    true,
);

pub static EXTRA_FLOAT_DIGITS: VarDefinition = VarDefinition::new(
    "extra_float_digits",
    value!(i32; 3),
    "Adjusts the number of digits displayed for floating-point values (PostgreSQL).",
    true,
);

pub static FAILPOINTS: VarDefinition = VarDefinition::new(
    "failpoints",
    value!(Failpoints; Failpoints),
    "Allows failpoints to be dynamically activated.",
    true,
);

pub static INTEGER_DATETIMES: VarDefinition = VarDefinition::new(
    "integer_datetimes",
    value!(bool; true),
    "Reports whether the server uses 64-bit-integer dates and times (PostgreSQL).",
    true,
)
.fixed();

pub static INTERVAL_STYLE: VarDefinition = VarDefinition::new(
    // IntervalStyle has nonstandard capitalization for historical reasons.
    "IntervalStyle",
    value!(IntervalStyle; IntervalStyle::Postgres),
    "Sets the display format for interval values (PostgreSQL).",
    true,
);

pub const MZ_VERSION_NAME: &UncasedStr = UncasedStr::new("mz_version");
pub const IS_SUPERUSER_NAME: &UncasedStr = UncasedStr::new("is_superuser");

// Schema can be used an alias for a search path with a single element.
pub const SCHEMA_ALIAS: &UncasedStr = UncasedStr::new("schema");
pub static SEARCH_PATH: VarDefinition = VarDefinition::new_lazy(
    "search_path",
    lazy_value!(Vec<Ident>; || vec![ident!(DEFAULT_SCHEMA)]),
    "Sets the schema search order for names that are not schema-qualified (PostgreSQL).",
    true,
);

pub static STATEMENT_TIMEOUT: VarDefinition = VarDefinition::new(
    "statement_timeout",
    value!(Duration; Duration::from_secs(10)),
    "Sets the maximum allowed duration of INSERT...SELECT, UPDATE, and DELETE operations. \
    If this value is specified without units, it is taken as milliseconds.",
    true,
);

pub static IDLE_IN_TRANSACTION_SESSION_TIMEOUT: VarDefinition = VarDefinition::new(
    "idle_in_transaction_session_timeout",
    value!(Duration; Duration::from_secs(60 * 2)),
    "Sets the maximum allowed duration that a session can sit idle in a transaction before \
    being terminated. If this value is specified without units, it is taken as milliseconds. \
    A value of zero disables the timeout (PostgreSQL).",
    true,
);

pub static SERVER_VERSION: VarDefinition = VarDefinition::new_lazy(
    "server_version",
    lazy_value!(String; || {
        format!("{SERVER_MAJOR_VERSION}.{SERVER_MINOR_VERSION}.{SERVER_PATCH_VERSION}")
    }),
    "Shows the PostgreSQL compatible server version (PostgreSQL).",
    true,
)
.read_only();

pub static SERVER_VERSION_NUM: VarDefinition = VarDefinition::new(
    "server_version_num",
    value!(i32; (cast::u8_to_i32(SERVER_MAJOR_VERSION) * 10_000)
        + (cast::u8_to_i32(SERVER_MINOR_VERSION) * 100)
        + cast::u8_to_i32(SERVER_PATCH_VERSION)),
    "Shows the PostgreSQL compatible server version as an integer (PostgreSQL).",
    true,
)
.read_only();

pub static SQL_SAFE_UPDATES: VarDefinition = VarDefinition::new(
    "sql_safe_updates",
    value!(bool; false),
    "Prohibits SQL statements that may be overly destructive (CockroachDB).",
    true,
);

pub static STANDARD_CONFORMING_STRINGS: VarDefinition = VarDefinition::new(
    "standard_conforming_strings",
    value!(bool; true),
    "Causes '...' strings to treat backslashes literally (PostgreSQL).",
    true,
)
.fixed();

pub static TIMEZONE: VarDefinition = VarDefinition::new(
    // TimeZone has nonstandard capitalization for historical reasons.
    "TimeZone",
    value!(TimeZone; TimeZone::UTC),
    "Sets the time zone for displaying and interpreting time stamps (PostgreSQL).",
    true,
);

pub const TRANSACTION_ISOLATION_VAR_NAME: &str = "transaction_isolation";
pub static TRANSACTION_ISOLATION: VarDefinition = VarDefinition::new(
    TRANSACTION_ISOLATION_VAR_NAME,
    value!(IsolationLevel; IsolationLevel::StrictSerializable),
    "Sets the current transaction's isolation level (PostgreSQL).",
    true,
);

pub static MAX_KAFKA_CONNECTIONS: VarDefinition = VarDefinition::new(
    "max_kafka_connections",
    value!(u32; 1000),
    "The maximum number of Kafka connections in the region, across all schemas (Materialize).",
    true,
);

pub static MAX_POSTGRES_CONNECTIONS: VarDefinition = VarDefinition::new(
    "max_postgres_connections",
    value!(u32; 1000),
    "The maximum number of PostgreSQL connections in the region, across all schemas (Materialize).",
    true,
);

pub static MAX_MYSQL_CONNECTIONS: VarDefinition = VarDefinition::new(
    "max_mysql_connections",
    value!(u32; 1000),
    "The maximum number of MySQL connections in the region, across all schemas (Materialize).",
    true,
);

pub static MAX_AWS_PRIVATELINK_CONNECTIONS: VarDefinition = VarDefinition::new(
    "max_aws_privatelink_connections",
    value!(u32; 0),
     "The maximum number of AWS PrivateLink connections in the region, across all schemas (Materialize).",
    true,
);

pub static MAX_TABLES: VarDefinition = VarDefinition::new(
    "max_tables",
    value!(u32; 25),
    "The maximum number of tables in the region, across all schemas (Materialize).",
    true,
);

pub static MAX_SOURCES: VarDefinition = VarDefinition::new(
    "max_sources",
    value!(u32; 25),
    "The maximum number of sources in the region, across all schemas (Materialize).",
    true,
);

pub static MAX_SINKS: VarDefinition = VarDefinition::new(
    "max_sinks",
    value!(u32; 25),
    "The maximum number of sinks in the region, across all schemas (Materialize).",
    true,
);

pub static MAX_MATERIALIZED_VIEWS: VarDefinition = VarDefinition::new(
    "max_materialized_views",
    value!(u32; 100),
    "The maximum number of materialized views in the region, across all schemas (Materialize).",
    true,
);

pub static MAX_CLUSTERS: VarDefinition = VarDefinition::new(
    "max_clusters",
    value!(u32; 10),
    "The maximum number of clusters in the region (Materialize).",
    true,
);

pub static MAX_REPLICAS_PER_CLUSTER: VarDefinition = VarDefinition::new(
    "max_replicas_per_cluster",
    value!(u32; 5),
    "The maximum number of replicas of a single cluster (Materialize).",
    true,
);

pub static MAX_CREDIT_CONSUMPTION_RATE: VarDefinition = VarDefinition::new_lazy(
    "max_credit_consumption_rate",
    lazy_value!(Numeric; || 1024.into()),
    "The maximum rate of credit consumption in a region. Credits are consumed based on the size of cluster replicas in use (Materialize).",
    true,
)
.with_constraint(&NUMERIC_NON_NEGATIVE);

pub static MAX_DATABASES: VarDefinition = VarDefinition::new(
    "max_databases",
    value!(u32; 1000),
    "The maximum number of databases in the region (Materialize).",
    true,
);

pub static MAX_SCHEMAS_PER_DATABASE: VarDefinition = VarDefinition::new(
    "max_schemas_per_database",
    value!(u32; 1000),
    "The maximum number of schemas in a database (Materialize).",
    true,
);

pub static MAX_OBJECTS_PER_SCHEMA: VarDefinition = VarDefinition::new(
    "max_objects_per_schema",
    value!(u32; 1000),
    "The maximum number of objects in a schema (Materialize).",
    true,
);

pub static MAX_SECRETS: VarDefinition = VarDefinition::new(
    "max_secrets",
    value!(u32; 100),
    "The maximum number of secrets in the region, across all schemas (Materialize).",
    true,
);

pub static MAX_ROLES: VarDefinition = VarDefinition::new(
    "max_roles",
    value!(u32; 1000),
    "The maximum number of roles in the region (Materialize).",
    true,
);

// Cloud environmentd is configured with 4 GiB of RAM, so 1 GiB is a good heuristic for a single
// query.
//
// We constrain this parameter to a minimum of 1MB, to avoid accidental usage of values that will
// interfer with queries executed by the system itself.
//
// TODO(jkosh44) Eventually we want to be able to return arbitrary sized results.
pub static MAX_RESULT_SIZE: VarDefinition = VarDefinition::new(
    "max_result_size",
    value!(ByteSize; ByteSize::gb(1)),
    "The maximum size in bytes for an internal query result (Materialize).",
    true,
)
.with_constraint(&BYTESIZE_AT_LEAST_1MB);

pub static MAX_QUERY_RESULT_SIZE: VarDefinition = VarDefinition::new(
    "max_query_result_size",
    value!(ByteSize; ByteSize::gb(1)),
    "The maximum size in bytes for a single query's result (Materialize).",
    true,
);

pub static MAX_COPY_FROM_SIZE: VarDefinition = VarDefinition::new(
    "max_copy_from_size",
    // 1 GiB, this limit is noted in the docs, if you change it make sure to update our docs.
    value!(u32; 1_073_741_824),
    "The maximum size in bytes we buffer for COPY FROM statements (Materialize).",
    true,
);

pub static MAX_IDENTIFIER_LENGTH: VarDefinition = VarDefinition::new(
    "max_identifier_length",
    value!(usize; mz_sql_lexer::lexer::MAX_IDENTIFIER_LENGTH),
    "The maximum length of object identifiers in bytes (PostgreSQL).",
    true,
);

pub static WELCOME_MESSAGE: VarDefinition = VarDefinition::new(
    "welcome_message",
    value!(bool; true),
    "Whether to send a notice with a welcome message after a successful connection (Materialize).",
    true,
);

/// The logical compaction window for builtin tables and sources that have the
/// `retained_metrics_relation` flag set.
///
/// The existence of this variable is a bit of a hack until we have a fully
/// general solution for controlling retention windows.
pub static METRICS_RETENTION: VarDefinition = VarDefinition::new(
    "metrics_retention",
    // 30 days
    value!(Duration; Duration::from_secs(30 * 24 * 60 * 60)),
    "The time to retain cluster utilization metrics (Materialize).",
    false,
);

pub static ALLOWED_CLUSTER_REPLICA_SIZES: VarDefinition = VarDefinition::new(
    "allowed_cluster_replica_sizes",
    value!(Vec<Ident>; Vec::new()),
    "The allowed sizes when creating a new cluster replica (Materialize).",
    true,
);

pub static PERSIST_FAST_PATH_LIMIT: VarDefinition = VarDefinition::new(
    "persist_fast_path_limit",
    value!(usize; 25),
    "An exclusive upper bound on the number of results we may return from a Persist fast-path peek; \
    queries that may return more results will follow the normal / slow path. \
    Setting this to 0 disables the feature.",
    false,
);

/// Controls `mz_adapter::coord::timestamp_oracle::postgres_oracle::DynamicConfig::pg_connection_pool_max_size`.
pub static PG_TIMESTAMP_ORACLE_CONNECTION_POOL_MAX_SIZE: VarDefinition = VarDefinition::new(
    "pg_timestamp_oracle_connection_pool_max_size",
    value!(usize; DEFAULT_PG_TIMESTAMP_ORACLE_CONNPOOL_MAX_SIZE),
    "Maximum size of the Postgres/CRDB connection pool, used by the Postgres/CRDB timestamp oracle.",
    false,
);

/// Controls `mz_adapter::coord::timestamp_oracle::postgres_oracle::DynamicConfig::pg_connection_pool_max_wait`.
pub static PG_TIMESTAMP_ORACLE_CONNECTION_POOL_MAX_WAIT: VarDefinition = VarDefinition::new(
    "pg_timestamp_oracle_connection_pool_max_wait",
    value!(Option<Duration>; Some(DEFAULT_PG_TIMESTAMP_ORACLE_CONNPOOL_MAX_WAIT)),
    "The maximum time to wait when attempting to obtain a connection from the Postgres/CRDB connection pool, used by the Postgres/CRDB timestamp oracle.",
    false,
);

/// Controls `mz_adapter::coord::timestamp_oracle::postgres_oracle::DynamicConfig::pg_connection_pool_ttl`.
pub static PG_TIMESTAMP_ORACLE_CONNECTION_POOL_TTL: VarDefinition = VarDefinition::new(
    "pg_timestamp_oracle_connection_pool_ttl",
    value!(Duration; DEFAULT_PG_TIMESTAMP_ORACLE_CONNPOOL_TTL),
    "The minimum TTL of a Consensus connection to Postgres/CRDB before it is proactively terminated",
    false,
);

/// Controls `mz_adapter::coord::timestamp_oracle::postgres_oracle::DynamicConfig::pg_connection_pool_ttl_stagger`.
pub static PG_TIMESTAMP_ORACLE_CONNECTION_POOL_TTL_STAGGER: VarDefinition = VarDefinition::new(
    "pg_timestamp_oracle_connection_pool_ttl_stagger",
    value!(Duration; DEFAULT_PG_TIMESTAMP_ORACLE_CONNPOOL_TTL_STAGGER),
    "The minimum time between TTLing Consensus connections to Postgres/CRDB.",
    false,
);

/// The default for the `DISK` option when creating managed clusters and cluster replicas.
pub static DISK_CLUSTER_REPLICAS_DEFAULT: VarDefinition = VarDefinition::new(
    "disk_cluster_replicas_default",
    value!(bool; false),
    "Whether the disk option for managed clusters and cluster replicas should be enabled by default.",
    false,
);

pub static UNSAFE_NEW_TRANSACTION_WALL_TIME: VarDefinition = VarDefinition::new(
    "unsafe_new_transaction_wall_time",
    value!(Option<CheckedTimestamp<DateTime<Utc>>>; None),
    "Sets the wall time for all new explicit or implicit transactions to control the value of `now()`. \
    If not set, uses the system's clock.",
    // This needs to be true because `user_visible: false` things are only modifiable by the mz_system
    // and mz_support users, and we want sqllogictest to have access with its user. Because the name
    // starts with "unsafe" it still won't be visible or changeable by users unless unsafe mode is
    // enabled.
    true,
);

/// Tuning for RocksDB used by `UPSERT` sources that takes effect on restart.
pub mod upsert_rocksdb {
    use super::*;
    use mz_rocksdb_types::config::{CompactionStyle, CompressionType};

    pub static UPSERT_ROCKSDB_COMPACTION_STYLE: VarDefinition = VarDefinition::new(
        "upsert_rocksdb_compaction_style",
        value!(CompactionStyle; mz_rocksdb_types::defaults::DEFAULT_COMPACTION_STYLE),
        "Tuning parameter for RocksDB as used in `UPSERT/DEBEZIUM` \
        sources. Described in the `mz_rocksdb_types::config` module. \
        Only takes effect on source restart (Materialize).",
        false,
    );

    pub static UPSERT_ROCKSDB_OPTIMIZE_COMPACTION_MEMTABLE_BUDGET: VarDefinition =
        VarDefinition::new(
            "upsert_rocksdb_optimize_compaction_memtable_budget",
            value!(usize; mz_rocksdb_types::defaults::DEFAULT_OPTIMIZE_COMPACTION_MEMTABLE_BUDGET),
            "Tuning parameter for RocksDB as used in `UPSERT/DEBEZIUM` \
        sources. Described in the `mz_rocksdb_types::config` module. \
        Only takes effect on source restart (Materialize).",
            false,
        );

    pub static UPSERT_ROCKSDB_LEVEL_COMPACTION_DYNAMIC_LEVEL_BYTES: VarDefinition =
        VarDefinition::new(
            "upsert_rocksdb_level_compaction_dynamic_level_bytes",
            value!(bool; mz_rocksdb_types::defaults::DEFAULT_LEVEL_COMPACTION_DYNAMIC_LEVEL_BYTES),
            "Tuning parameter for RocksDB as used in `UPSERT/DEBEZIUM` \
        sources. Described in the `mz_rocksdb_types::config` module. \
        Only takes effect on source restart (Materialize).",
            false,
        );

    pub static UPSERT_ROCKSDB_UNIVERSAL_COMPACTION_RATIO: VarDefinition = VarDefinition::new(
        "upsert_rocksdb_universal_compaction_ratio",
        value!(i32; mz_rocksdb_types::defaults::DEFAULT_UNIVERSAL_COMPACTION_RATIO),
        "Tuning parameter for RocksDB as used in `UPSERT/DEBEZIUM` \
        sources. Described in the `mz_rocksdb_types::config` module. \
        Only takes effect on source restart (Materialize).",
        false,
    );

    pub static UPSERT_ROCKSDB_PARALLELISM: VarDefinition = VarDefinition::new(
        "upsert_rocksdb_parallelism",
        value!(Option<i32>; mz_rocksdb_types::defaults::DEFAULT_PARALLELISM),
        "Tuning parameter for RocksDB as used in `UPSERT/DEBEZIUM` \
        sources. Described in the `mz_rocksdb_types::config` module. \
        Only takes effect on source restart (Materialize).",
        false,
    );

    pub static UPSERT_ROCKSDB_COMPRESSION_TYPE: VarDefinition = VarDefinition::new(
        "upsert_rocksdb_compression_type",
        value!(CompressionType; mz_rocksdb_types::defaults::DEFAULT_COMPRESSION_TYPE),
        "Tuning parameter for RocksDB as used in `UPSERT/DEBEZIUM` \
        sources. Described in the `mz_rocksdb_types::config` module. \
        Only takes effect on source restart (Materialize).",
        false,
    );

    pub static UPSERT_ROCKSDB_BOTTOMMOST_COMPRESSION_TYPE: VarDefinition = VarDefinition::new(
        "upsert_rocksdb_bottommost_compression_type",
        value!(CompressionType; mz_rocksdb_types::defaults::DEFAULT_BOTTOMMOST_COMPRESSION_TYPE),
        "Tuning parameter for RocksDB as used in `UPSERT/DEBEZIUM` \
        sources. Described in the `mz_rocksdb_types::config` module. \
        Only takes effect on source restart (Materialize).",
        false,
    );

    pub static UPSERT_ROCKSDB_BATCH_SIZE: VarDefinition = VarDefinition::new(
        "upsert_rocksdb_batch_size",
        value!(usize; mz_rocksdb_types::defaults::DEFAULT_BATCH_SIZE),
        "Tuning parameter for RocksDB as used in `UPSERT/DEBEZIUM` \
        sources. Described in the `mz_rocksdb_types::config` module. \
        Can be changed dynamically (Materialize).",
        false,
    );

    pub static UPSERT_ROCKSDB_RETRY_DURATION: VarDefinition = VarDefinition::new(
        "upsert_rocksdb_retry_duration",
        value!(Duration; mz_rocksdb_types::defaults::DEFAULT_RETRY_DURATION),
        "Tuning parameter for RocksDB as used in `UPSERT/DEBEZIUM` \
        sources. Described in the `mz_rocksdb_types::config` module. \
        Only takes effect on source restart (Materialize).",
        false,
    );

    /// Controls whether automatic spill to disk should be turned on when using `DISK`.
    pub static UPSERT_ROCKSDB_AUTO_SPILL_TO_DISK: VarDefinition = VarDefinition::new(
        "upsert_rocksdb_auto_spill_to_disk",
        value!(bool; false),
        "Controls whether automatic spill to disk should be turned on when using `DISK`",
        false,
    );

    /// The upsert in memory state size threshold after which it will spill to disk.
    /// The default is 85 MiB = 89128960 bytes
    pub static UPSERT_ROCKSDB_AUTO_SPILL_THRESHOLD_BYTES: VarDefinition = VarDefinition::new(
        "upsert_rocksdb_auto_spill_threshold_bytes",
        value!(usize; mz_rocksdb_types::defaults::DEFAULT_AUTO_SPILL_MEMORY_THRESHOLD),
        "The upsert in-memory state size threshold in bytes after which it will spill to disk",
        false,
    );

    pub static UPSERT_ROCKSDB_STATS_LOG_INTERVAL_SECONDS: VarDefinition = VarDefinition::new(
        "upsert_rocksdb_stats_log_interval_seconds",
        value!(u32; mz_rocksdb_types::defaults::DEFAULT_STATS_LOG_INTERVAL_S),
        "Tuning parameter for RocksDB as used in `UPSERT/DEBEZIUM` \
        sources. Described in the `mz_rocksdb_types::config` module. \
        Only takes effect on source restart (Materialize).",
        false,
    );

    pub static UPSERT_ROCKSDB_STATS_PERSIST_INTERVAL_SECONDS: VarDefinition = VarDefinition::new(
        "upsert_rocksdb_stats_persist_interval_seconds",
        value!(u32; mz_rocksdb_types::defaults::DEFAULT_STATS_PERSIST_INTERVAL_S),
        "Tuning parameter for RocksDB as used in `UPSERT/DEBEZIUM` \
        sources. Described in the `mz_rocksdb_types::config` module. \
        Only takes effect on source restart (Materialize).",
        false,
    );

    pub static UPSERT_ROCKSDB_POINT_LOOKUP_BLOCK_CACHE_SIZE_MB: VarDefinition = VarDefinition::new(
        "upsert_rocksdb_point_lookup_block_cache_size_mb",
        value!(Option<u32>; None),
        "Tuning parameter for RocksDB as used in `UPSERT/DEBEZIUM` \
        sources. Described in the `mz_rocksdb_types::config` module. \
        Only takes effect on source restart (Materialize).",
        false,
    );

    /// The number of times by which allocated buffers will be shrinked in upsert rocksdb.
    /// If value is 0, then no shrinking will occur.
    pub static UPSERT_ROCKSDB_SHRINK_ALLOCATED_BUFFERS_BY_RATIO: VarDefinition = VarDefinition::new(
        "upsert_rocksdb_shrink_allocated_buffers_by_ratio",
        value!(usize; mz_rocksdb_types::defaults::DEFAULT_SHRINK_BUFFERS_BY_RATIO),
        "The number of times by which allocated buffers will be shrinked in upsert rocksdb.",
        false,
    );

    /// Only used if `upsert_rocksdb_write_buffer_manager_memory_bytes` is also set
    /// and write buffer manager is enabled
    pub static UPSERT_ROCKSDB_WRITE_BUFFER_MANAGER_CLUSTER_MEMORY_FRACTION: VarDefinition =
        VarDefinition::new(
            "upsert_rocksdb_write_buffer_manager_cluster_memory_fraction",
            value!(Option<Numeric>; None),
            "Tuning parameter for RocksDB as used in `UPSERT/DEBEZIUM` \
        sources. Described in the `mz_rocksdb_types::config` module. \
        Only takes effect on source restart (Materialize).",
            false,
        );

    /// `upsert_rocksdb_write_buffer_manager_memory_bytes` needs to be set for write buffer manager to be
    /// used.
    pub static UPSERT_ROCKSDB_WRITE_BUFFER_MANAGER_MEMORY_BYTES: VarDefinition = VarDefinition::new(
        "upsert_rocksdb_write_buffer_manager_memory_bytes",
        value!(Option<usize>; None),
        "Tuning parameter for RocksDB as used in `UPSERT/DEBEZIUM` \
        sources. Described in the `mz_rocksdb_types::config` module. \
        Only takes effect on source restart (Materialize).",
        false,
    );

    pub static UPSERT_ROCKSDB_WRITE_BUFFER_MANAGER_ALLOW_STALL: VarDefinition = VarDefinition::new(
        "upsert_rocksdb_write_buffer_manager_allow_stall",
        value!(bool; false),
        "Tuning parameter for RocksDB as used in `UPSERT/DEBEZIUM` \
        sources. Described in the `mz_rocksdb_types::config` module. \
        Only takes effect on source restart (Materialize).",
        false,
    );
}

pub static LOGGING_FILTER: VarDefinition = VarDefinition::new_lazy(
    "log_filter",
    lazy_value!(CloneableEnvFilter; || CloneableEnvFilter::from_str("info").expect("valid EnvFilter")),
    "Sets the filter to apply to stderr logging.",
    false,
);

pub static OPENTELEMETRY_FILTER: VarDefinition = VarDefinition::new_lazy(
    "opentelemetry_filter",
    lazy_value!(CloneableEnvFilter; || CloneableEnvFilter::from_str("info").expect("valid EnvFilter")),
    "Sets the filter to apply to OpenTelemetry-backed distributed tracing.",
    false,
);

pub static LOGGING_FILTER_DEFAULTS: VarDefinition = VarDefinition::new_lazy(
    "log_filter_defaults",
    lazy_value!(Vec<SerializableDirective>; || {
        mz_ore::tracing::LOGGING_DEFAULTS
            .iter()
            .map(|d| d.clone().into())
            .collect()
    }),
    "Sets additional default directives to apply to stderr logging. \
        These apply to all variations of `log_filter`. Directives other than \
        `module=off` are likely incorrect.",
    false,
);

pub static OPENTELEMETRY_FILTER_DEFAULTS: VarDefinition = VarDefinition::new_lazy(
    "opentelemetry_filter_defaults",
    lazy_value!(Vec<SerializableDirective>; || {
        mz_ore::tracing::OPENTELEMETRY_DEFAULTS
            .iter()
            .map(|d| d.clone().into())
            .collect()
    }),
    "Sets additional default directives to apply to OpenTelemetry-backed \
        distributed tracing. \
        These apply to all variations of `opentelemetry_filter`. Directives other than \
        `module=off` are likely incorrect.",
    false,
);

pub static SENTRY_FILTERS: VarDefinition = VarDefinition::new_lazy(
    "sentry_filters",
    lazy_value!(Vec<SerializableDirective>; || {
        mz_ore::tracing::SENTRY_DEFAULTS
            .iter()
            .map(|d| d.clone().into())
            .collect()
    }),
    "Sets additional default directives to apply to sentry logging. \
        These apply on top of a default `info` directive. Directives other than \
        `module=off` are likely incorrect.",
    false,
);

pub static WEBHOOKS_SECRETS_CACHING_TTL_SECS: VarDefinition = VarDefinition::new_lazy(
    "webhooks_secrets_caching_ttl_secs",
    lazy_value!(usize; || {
        usize::cast_from(
            mz_secrets::cache::DEFAULT_TTL_SECS.load(std::sync::atomic::Ordering::Relaxed),
        )
    }),
    "Sets the time-to-live for values in the Webhooks secrets cache.",
    false,
);

pub static COORD_SLOW_MESSAGE_WARN_THRESHOLD: VarDefinition = VarDefinition::new(
    "coord_slow_message_warn_threshold",
    value!(Duration; Duration::from_secs(30)),
    "Sets the threshold at which we will error! for a coordinator message being slow.",
    false,
);

/// Controls the connect_timeout setting when connecting to PG via `mz_postgres_util`.
pub static PG_SOURCE_CONNECT_TIMEOUT: VarDefinition = VarDefinition::new(
    "pg_source_connect_timeout",
    value!(Duration; DEFAULT_PG_SOURCE_CONNECT_TIMEOUT),
    "Sets the timeout applied to socket-level connection attempts for PG \
    replication connections (Materialize).",
    false,
);

/// Sets the maximum number of TCP keepalive probes that will be sent before dropping a connection
/// when connecting to PG via `mz_postgres_util`.
pub static PG_SOURCE_TCP_KEEPALIVES_RETRIES: VarDefinition = VarDefinition::new(
    "pg_source_tcp_keepalives_retries",
    value!(u32; DEFAULT_PG_SOURCE_TCP_KEEPALIVES_RETRIES),
    "Sets the maximum number of TCP keepalive probes that will be sent before dropping \
    a connection when connecting to PG via `mz_postgres_util` (Materialize).",
    false,
);

/// Sets the amount of idle time before a keepalive packet is sent on the connection when connecting
/// to PG via `mz_postgres_util`.
pub static PG_SOURCE_TCP_KEEPALIVES_IDLE: VarDefinition = VarDefinition::new(
    "pg_source_tcp_keepalives_idle",
    value!(Duration; DEFAULT_PG_SOURCE_TCP_KEEPALIVES_IDLE),
    "Sets the amount of idle time before a keepalive packet is sent on the connection \
        when connecting to PG via `mz_postgres_util` (Materialize).",
    false,
);

/// Sets the time interval between TCP keepalive probes when connecting to PG via `mz_postgres_util`.
pub static PG_SOURCE_TCP_KEEPALIVES_INTERVAL: VarDefinition = VarDefinition::new(
    "pg_source_tcp_keepalives_interval",
    value!(Duration; DEFAULT_PG_SOURCE_TCP_KEEPALIVES_INTERVAL),
    "Sets the time interval between TCP keepalive probes when connecting to PG via \
        replication (Materialize).",
    false,
);

/// Sets the TCP user timeout when connecting to PG via `mz_postgres_util`.
pub static PG_SOURCE_TCP_USER_TIMEOUT: VarDefinition = VarDefinition::new(
    "pg_source_tcp_user_timeout",
    value!(Duration; DEFAULT_PG_SOURCE_TCP_USER_TIMEOUT),
    "Sets the TCP user timeout when connecting to PG via `mz_postgres_util` (Materialize).",
    false,
);

/// Sets whether to apply the TCP configuration parameters on the server when
/// connecting to PG via `mz_postgres_util`.
pub static PG_SOURCE_TCP_CONFIGURE_SERVER: VarDefinition = VarDefinition::new(
    "pg_source_tcp_configure_server",
    value!(bool; DEFAULT_PG_SOURCE_TCP_CONFIGURE_SERVER),
    "Sets whether to apply the TCP configuration parameters on the server when connecting to PG via `mz_postgres_util` (Materialize).",
    false,
);

/// Sets the `statement_timeout` value to use during the snapshotting phase of
/// PG sources.
pub static PG_SOURCE_SNAPSHOT_STATEMENT_TIMEOUT: VarDefinition = VarDefinition::new(
    "pg_source_snapshot_statement_timeout",
    value!(Duration; mz_postgres_util::DEFAULT_SNAPSHOT_STATEMENT_TIMEOUT),
    "Sets the `statement_timeout` value to use during the snapshotting phase of PG sources (Materialize)",
    false,
);

/// Sets the `wal_sender_timeout` value to use during the replication phase of
/// PG sources.
pub static PG_SOURCE_WAL_SENDER_TIMEOUT: VarDefinition = VarDefinition::new(
    "pg_source_wal_sender_timeout",
    value!(Option<Duration>; DEFAULT_PG_SOURCE_WAL_SENDER_TIMEOUT),
    "Sets the `wal_sender_timeout` value to use during the replication phase of PG sources (Materialize)",
    false,
);

/// Please see `PgSourceSnapshotConfig`.
pub static PG_SOURCE_SNAPSHOT_COLLECT_STRICT_COUNT: VarDefinition = VarDefinition::new(
    "pg_source_snapshot_collect_strict_count",
    value!(bool; mz_storage_types::parameters::PgSourceSnapshotConfig::new().collect_strict_count),
    "Please see <https://dev.materialize.com/api/rust-private\
        /mz_storage_types/parameters\
        /struct.PgSourceSnapshotConfig.html#structfield.collect_strict_count>",
    false,
);

/// Please see `PgSourceSnapshotConfig`.
pub static PG_SOURCE_SNAPSHOT_FALLBACK_TO_STRICT_COUNT: VarDefinition = VarDefinition::new(
    "pg_source_snapshot_fallback_to_strict_count",
    value!(bool; mz_storage_types::parameters::PgSourceSnapshotConfig::new().fallback_to_strict_count),
    "Please see <https://dev.materialize.com/api/rust-private\
        /mz_storage_types/parameters\
        /struct.PgSourceSnapshotConfig.html#structfield.fallback_to_strict_count>",
    false,
);

/// Please see `PgSourceSnapshotConfig`.
pub static PG_SOURCE_SNAPSHOT_WAIT_FOR_COUNT: VarDefinition = VarDefinition::new(
    "pg_source_snapshot_wait_for_count",
    value!(bool; mz_storage_types::parameters::PgSourceSnapshotConfig::new().wait_for_count),
    "Please see <https://dev.materialize.com/api/rust-private\
        /mz_storage_types/parameters\
        /struct.PgSourceSnapshotConfig.html#structfield.wait_for_count>",
    false,
);

/// Sets the time between TCP keepalive probes when connecting to MySQL via `mz_mysql_util`.
pub static MYSQL_SOURCE_TCP_KEEPALIVE: VarDefinition = VarDefinition::new(
    "mysql_source_tcp_keepalive",
    value!(Duration; mz_mysql_util::DEFAULT_TCP_KEEPALIVE),
    "Sets the time between TCP keepalive probes when connecting to MySQL",
    false,
);

/// Sets the `max_execution_time` value to use during the snapshotting phase of
/// MySQL sources.
pub static MYSQL_SOURCE_SNAPSHOT_MAX_EXECUTION_TIME: VarDefinition = VarDefinition::new(
    "mysql_source_snapshot_max_execution_time",
    value!(Duration; mz_mysql_util::DEFAULT_SNAPSHOT_MAX_EXECUTION_TIME),
    "Sets the `max_execution_time` value to use during the snapshotting phase of MySQL sources (Materialize)",
    false,
);

/// Sets the `lock_wait_timeout` value to use during the snapshotting phase of
/// MySQL sources.
pub static MYSQL_SOURCE_SNAPSHOT_LOCK_WAIT_TIMEOUT: VarDefinition = VarDefinition::new(
    "mysql_source_snapshot_lock_wait_timeout",
    value!(Duration; mz_mysql_util::DEFAULT_SNAPSHOT_LOCK_WAIT_TIMEOUT),
    "Sets the `lock_wait_timeout` value to use during the snapshotting phase of MySQL sources (Materialize)",
    false,
);

/// Controls the check interval for connections to SSH bastions via `mz_ssh_util`.
pub static SSH_CHECK_INTERVAL: VarDefinition = VarDefinition::new(
    "ssh_check_interval",
    value!(Duration; mz_ssh_util::tunnel::DEFAULT_CHECK_INTERVAL),
    "Controls the check interval for connections to SSH bastions via `mz_ssh_util`.",
    false,
);

/// Controls the connect timeout for connections to SSH bastions via `mz_ssh_util`.
pub static SSH_CONNECT_TIMEOUT: VarDefinition = VarDefinition::new(
    "ssh_connect_timeout",
    value!(Duration; mz_ssh_util::tunnel::DEFAULT_CONNECT_TIMEOUT),
    "Controls the connect timeout for connections to SSH bastions via `mz_ssh_util`.",
    false,
);

/// Controls the keepalive idle interval for connections to SSH bastions via `mz_ssh_util`.
pub static SSH_KEEPALIVES_IDLE: VarDefinition = VarDefinition::new(
    "ssh_keepalives_idle",
    value!(Duration; mz_ssh_util::tunnel::DEFAULT_KEEPALIVES_IDLE),
    "Controls the keepalive idle interval for connections to SSH bastions via `mz_ssh_util`.",
    false,
);

/// Enables `socket.keepalive.enable` for rdkafka client connections. Defaults to true.
pub static KAFKA_SOCKET_KEEPALIVE: VarDefinition = VarDefinition::new(
    "kafka_socket_keepalive",
    value!(bool; mz_kafka_util::client::DEFAULT_KEEPALIVE),
    "Enables `socket.keepalive.enable` for rdkafka client connections. Defaults to true.",
    false,
);

/// Controls `socket.timeout.ms` for rdkafka client connections. Defaults to the rdkafka default
/// (60000ms). Cannot be greater than 300000ms, more than 100ms greater than
/// `kafka_transaction_timeout`, or less than 10ms.
pub static KAFKA_SOCKET_TIMEOUT: VarDefinition = VarDefinition::new(
    "kafka_socket_timeout",
    value!(Duration; mz_kafka_util::client::DEFAULT_SOCKET_TIMEOUT),
    "Controls `socket.timeout.ms` for rdkafka \
        client connections. Defaults to the rdkafka default (60000ms). \
        Cannot be greater than 300000ms, more than 100ms greater than \
        `kafka_transaction_timeout`, or less than 10ms.",
    false,
);

/// Controls `transaction.timeout.ms` for rdkafka client connections. Defaults to the rdkafka default
/// (60000ms). Cannot be greater than `i32::MAX` or less than 1000ms.
pub static KAFKA_TRANSACTION_TIMEOUT: VarDefinition = VarDefinition::new(
    "kafka_transaction_timeout",
    value!(Duration; mz_kafka_util::client::DEFAULT_TRANSACTION_TIMEOUT),
    "Controls `transaction.timeout.ms` for rdkafka \
        client connections. Defaults to the rdkafka default (60000ms). \
        Cannot be greater than `i32::MAX` or less than 1000ms.",
    false,
);

/// Controls `socket.connection.setup.timeout.ms` for rdkafka client connections. Defaults to the rdkafka default
/// (30000ms). Cannot be greater than `i32::MAX` or less than 1000ms
pub static KAFKA_SOCKET_CONNECTION_SETUP_TIMEOUT: VarDefinition = VarDefinition::new(
    "kafka_socket_connection_setup_timeout",
    value!(Duration; mz_kafka_util::client::DEFAULT_SOCKET_CONNECTION_SETUP_TIMEOUT),
    "Controls `socket.connection.setup.timeout.ms` for rdkafka \
        client connections. Defaults to the rdkafka default (30000ms). \
        Cannot be greater than `i32::MAX` or less than 1000ms",
    false,
);

/// Controls the timeout when fetching kafka metadata. Defaults to 10s.
pub static KAFKA_FETCH_METADATA_TIMEOUT: VarDefinition = VarDefinition::new(
    "kafka_fetch_metadata_timeout",
    value!(Duration; mz_kafka_util::client::DEFAULT_FETCH_METADATA_TIMEOUT),
    "Controls the timeout when fetching kafka metadata. \
        Defaults to 10s.",
    false,
);

/// Controls the timeout when fetching kafka progress records. Defaults to 60s.
pub static KAFKA_PROGRESS_RECORD_FETCH_TIMEOUT: VarDefinition = VarDefinition::new(
    "kafka_progress_record_fetch_timeout",
    value!(Duration; mz_kafka_util::client::DEFAULT_PROGRESS_RECORD_FETCH_TIMEOUT),
    "Controls the timeout when fetching kafka progress records. \
        Defaults to 60s.",
    false,
);

/// The interval we will fetch metadata from, unless overridden by the source.
pub static KAFKA_DEFAULT_METADATA_FETCH_INTERVAL: VarDefinition = VarDefinition::new(
    "kafka_default_metadata_fetch_interval",
    value!(Duration; mz_kafka_util::client::DEFAULT_METADATA_FETCH_INTERVAL),
    "The interval we will fetch metadata from, unless overridden by the source. \
        Defaults to 60s.",
    false,
);

/// The maximum number of in-flight bytes emitted by persist_sources feeding _storage
/// dataflows_.
/// Currently defaults to 256MiB = 268435456 bytes
/// Note: Backpressure will only be turned on if disk is enabled based on
/// `storage_dataflow_max_inflight_bytes_disk_only` flag
pub static STORAGE_DATAFLOW_MAX_INFLIGHT_BYTES: VarDefinition = VarDefinition::new(
    "storage_dataflow_max_inflight_bytes",
    value!(Option<usize>; Some(256 * 1024 * 1024)),
    "The maximum number of in-flight bytes emitted by persist_sources feeding \
        storage dataflows. Defaults to backpressure enabled (Materialize).",
    false,
);

/// Configuration ratio to shrink unusef buffers in upsert by.
/// For eg: is 2 is set, then the buffers will be reduced by 2 i.e. halved.
/// Default is 0, which means shrinking is disabled.
pub static STORAGE_SHRINK_UPSERT_UNUSED_BUFFERS_BY_RATIO: VarDefinition = VarDefinition::new(
    "storage_shrink_upsert_unused_buffers_by_ratio",
    value!(usize; 0),
    "Configuration ratio to shrink unusef buffers in upsert by",
    false,
);

/// The fraction of the cluster replica size to be used as the maximum number of
/// in-flight bytes emitted by persist_sources feeding storage dataflows.
/// If not configured, the storage_dataflow_max_inflight_bytes value will be used.
/// For this value to be used storage_dataflow_max_inflight_bytes needs to be set.
pub static STORAGE_DATAFLOW_MAX_INFLIGHT_BYTES_TO_CLUSTER_SIZE_FRACTION: VarDefinition =
    VarDefinition::new_lazy(
        "storage_dataflow_max_inflight_bytes_to_cluster_size_fraction",
        lazy_value!(Option<Numeric>; || Some(0.0025.into())),
        "The fraction of the cluster replica size to be used as the maximum number of \
            in-flight bytes emitted by persist_sources feeding storage dataflows. \
            If not configured, the storage_dataflow_max_inflight_bytes value will be used.",
        false,
    );

pub static STORAGE_DATAFLOW_MAX_INFLIGHT_BYTES_DISK_ONLY: VarDefinition = VarDefinition::new(
    "storage_dataflow_max_inflight_bytes_disk_only",
    value!(bool; true),
    "Whether or not `storage_dataflow_max_inflight_bytes` applies only to \
        upsert dataflows using disks. Defaults to true (Materialize).",
    false,
);

/// The interval to submit statistics to `mz_source_statistics_per_worker` and `mz_sink_statistics_per_worker`.
pub static STORAGE_STATISTICS_INTERVAL: VarDefinition = VarDefinition::new(
    "storage_statistics_interval",
    value!(Duration; mz_storage_types::parameters::STATISTICS_INTERVAL_DEFAULT),
    "The interval to submit statistics to `mz_source_statistics_per_worker` \
        and `mz_sink_statistics` (Materialize).",
    false,
);

/// The interval to collect statistics for `mz_source_statistics_per_worker` and `mz_sink_statistics_per_worker` in
/// clusterd. Controls the accuracy of metrics.
pub static STORAGE_STATISTICS_COLLECTION_INTERVAL: VarDefinition = VarDefinition::new(
    "storage_statistics_collection_interval",
    value!(Duration; mz_storage_types::parameters::STATISTICS_COLLECTION_INTERVAL_DEFAULT),
    "The interval to collect statistics for `mz_source_statistics_per_worker` \
        and `mz_sink_statistics_per_worker` in clusterd. Controls the accuracy of metrics \
        (Materialize).",
    false,
);

pub static STORAGE_RECORD_SOURCE_SINK_NAMESPACED_ERRORS: VarDefinition = VarDefinition::new(
    "storage_record_source_sink_namespaced_errors",
    value!(bool; true),
    "Whether or not to record namespaced errors in the status history tables",
    false,
);

/// Boolean flag indicating whether to enable syncing from
/// LaunchDarkly. Can be turned off as an emergency measure to still
/// be able to alter parameters while LD is broken.
pub static ENABLE_LAUNCHDARKLY: VarDefinition = VarDefinition::new(
    "enable_launchdarkly",
    value!(bool; true),
    "Boolean flag indicating whether flag synchronization from LaunchDarkly should be enabled (Materialize).",
    false,
);

/// Feature flag indicating whether real time recency is enabled. Not that
/// unlike other feature flags, this is made available at the session level, so
/// is additionally gated by a feature flag.
pub static REAL_TIME_RECENCY: VarDefinition = VarDefinition::new(
    "real_time_recency",
    value!(bool; false),
    "Feature flag indicating whether real time recency is enabled (Materialize).",
    true,
)
.with_feature_flag(&ALLOW_REAL_TIME_RECENCY);

pub static REAL_TIME_RECENCY_TIMEOUT: VarDefinition = VarDefinition::new(
    "real_time_recency_timeout",
    value!(Duration; Duration::from_secs(10)),
    "Sets the maximum allowed duration of SELECTs that actively use real-time \
    recency, i.e. reach out to an external system to determine their most recencly exposed \
    data (Materialize).",
    true,
)
.with_feature_flag(&ALLOW_REAL_TIME_RECENCY);

pub static EMIT_PLAN_INSIGHTS_NOTICE: VarDefinition = VarDefinition::new(
    "emit_plan_insights_notice",
    value!(bool; false),
    "Boolean flag indicating whether to send a NOTICE with JSON-formatted plan insights before executing a SELECT statement (Materialize).",
    true,
);

pub static EMIT_TIMESTAMP_NOTICE: VarDefinition = VarDefinition::new(
    "emit_timestamp_notice",
    value!(bool; false),
    "Boolean flag indicating whether to send a NOTICE with timestamp explanations of queries (Materialize).",
    true,
);

pub static EMIT_TRACE_ID_NOTICE: VarDefinition = VarDefinition::new(
    "emit_trace_id_notice",
    value!(bool; false),
    "Boolean flag indicating whether to send a NOTICE specifying the trace id when available (Materialize).",
    true,
);

pub static UNSAFE_MOCK_AUDIT_EVENT_TIMESTAMP: VarDefinition = VarDefinition::new(
    "unsafe_mock_audit_event_timestamp",
    value!(Option<mz_repr::Timestamp>; None),
    "Mocked timestamp to use for audit events for testing purposes",
    false,
);

pub static ENABLE_RBAC_CHECKS: VarDefinition = VarDefinition::new(
    "enable_rbac_checks",
    value!(bool; true),
    "User facing global boolean flag indicating whether to apply RBAC checks before \
        executing statements (Materialize).",
    true,
);

pub static ENABLE_SESSION_RBAC_CHECKS: VarDefinition = VarDefinition::new(
    "enable_session_rbac_checks",
    // TODO(jkosh44) Once RBAC is complete, change this to `true`.
    value!(bool; false),
    "User facing session boolean flag indicating whether to apply RBAC checks before \
        executing statements (Materialize).",
    true,
);

pub static EMIT_INTROSPECTION_QUERY_NOTICE: VarDefinition = VarDefinition::new(
    "emit_introspection_query_notice",
    value!(bool; true),
    "Whether to print a notice when querying per-replica introspection sources.",
    true,
);

// TODO(mgree) change this to a SelectOption
pub static ENABLE_SESSION_CARDINALITY_ESTIMATES: VarDefinition = VarDefinition::new(
    "enable_session_cardinality_estimates",
    value!(bool; false),
    "Feature flag indicating whether to use cardinality estimates when optimizing queries; \
        does not affect EXPLAIN WITH(cardinality) (Materialize).",
    true,
)
.with_feature_flag(&ENABLE_CARDINALITY_ESTIMATES);

pub static OPTIMIZER_STATS_TIMEOUT: VarDefinition = VarDefinition::new(
    "optimizer_stats_timeout",
    value!(Duration; Duration::from_millis(250)),
    "Sets the timeout applied to the optimizer's statistics collection from storage; \
        applied to non-oneshot, i.e., long-lasting queries, like CREATE MATERIALIZED VIEW (Materialize).",
    false,
);

pub static OPTIMIZER_ONESHOT_STATS_TIMEOUT: VarDefinition = VarDefinition::new(
    "optimizer_oneshot_stats_timeout",
    value!(Duration; Duration::from_millis(20)),
    "Sets the timeout applied to the optimizer's statistics collection from storage; \
        applied to oneshot queries, like SELECT (Materialize).",
    false,
);

pub static PRIVATELINK_STATUS_UPDATE_QUOTA_PER_MINUTE: VarDefinition = VarDefinition::new(
    "privatelink_status_update_quota_per_minute",
    value!(u32; 20),
    "Sets the per-minute quota for privatelink vpc status updates to be written to \
        the storage-collection-backed system table. This value implies the total and burst quota per-minute.",
    false,
);

pub static STATEMENT_LOGGING_SAMPLE_RATE: VarDefinition = VarDefinition::new_lazy(
    "statement_logging_sample_rate",
    lazy_value!(Numeric; || 0.1.into()),
    "User-facing session variable indicating how many statement executions should be \
        logged, subject to constraint by the system variable `statement_logging_max_sample_rate` (Materialize).",
    true,
).with_constraint(&NUMERIC_BOUNDED_0_1_INCLUSIVE);

pub static ARRANGEMENT_EXERT_PROPORTIONALITY: VarDefinition = VarDefinition::new(
    "arrangement_exert_proportionality",
    value!(u32; 16),
    "Value that controls how much merge effort to exert on arrangements.",
    false,
);

pub static ENABLE_DEFAULT_CONNECTION_VALIDATION: VarDefinition = VarDefinition::new(
    "enable_default_connection_validation",
    value!(bool; true),
    "LD facing global boolean flag that allows turning default connection validation off for everyone (Materialize).",
    false,
);

pub static STATEMENT_LOGGING_MAX_DATA_CREDIT: VarDefinition = VarDefinition::new(
    "statement_logging_max_data_credit",
    value!(Option<usize>; None),
    // The idea is that during periods of low logging, tokens can accumulate up to this value,
    // and then be depleted during periods of high logging.
    "The maximum number of bytes that can be logged for statement logging in short burts, or NULL if unlimited (Materialize).",
    false,
);

pub static STATEMENT_LOGGING_TARGET_DATA_RATE: VarDefinition = VarDefinition::new(
    "statement_logging_target_data_rate",
    value!(Option<usize>; None),
    "The maximum sustained data rate of statement logging, in bytes per second, or NULL if unlimited (Materialize).",
    false,
);

pub static STATEMENT_LOGGING_MAX_SAMPLE_RATE: VarDefinition = VarDefinition::new_lazy(
    "statement_logging_max_sample_rate",
    lazy_value!(Numeric; || 0.0.into()),
    "The maximum rate at which statements may be logged. If this value is less than \
        that of `statement_logging_sample_rate`, the latter is ignored (Materialize).",
    true,
)
.with_constraint(&NUMERIC_BOUNDED_0_1_INCLUSIVE);

pub static STATEMENT_LOGGING_DEFAULT_SAMPLE_RATE: VarDefinition = VarDefinition::new_lazy(
    "statement_logging_default_sample_rate",
    lazy_value!(Numeric; || 0.0.into()),
    "The default value of `statement_logging_sample_rate` for new sessions (Materialize).",
    true,
)
.with_constraint(&NUMERIC_BOUNDED_0_1_INCLUSIVE);

pub static AUTO_ROUTE_CATALOG_QUERIES: VarDefinition = VarDefinition::new(
    "auto_route_catalog_queries",
    value!(bool; true),
    "Whether to force queries that depend only on system tables, to run on the mz_catalog_server cluster (Materialize).",
    true,
);

pub static MAX_CONNECTIONS: VarDefinition = VarDefinition::new(
    "max_connections",
    value!(u32; 5000),
    "The maximum number of concurrent connections (PostgreSQL).",
    true,
);

pub static SUPERUSER_RESERVED_CONNECTIONS: VarDefinition = VarDefinition::new(
    "superuser_reserved_connections",
    value!(u32; 3),
    "The number of connections that are reserved for superusers (PostgreSQL).",
    true,
);

/// Controls [`mz_storage_types::parameters::StorageParameters::keep_n_source_status_history_entries`].
pub static KEEP_N_SOURCE_STATUS_HISTORY_ENTRIES: VarDefinition = VarDefinition::new(
    "keep_n_source_status_history_entries",
    value!(usize; 5),
    "On reboot, truncate all but the last n entries per ID in the source_status_history collection (Materialize).",
    false,
);

/// Controls [`mz_storage_types::parameters::StorageParameters::keep_n_sink_status_history_entries`].
pub static KEEP_N_SINK_STATUS_HISTORY_ENTRIES: VarDefinition = VarDefinition::new(
    "keep_n_sink_status_history_entries",
    value!(usize; 5),
    "On reboot, truncate all but the last n entries per ID in the sink_status_history collection (Materialize).",
    false,
);

/// Controls [`mz_storage_types::parameters::StorageParameters::keep_n_privatelink_status_history_entries`].
pub static KEEP_N_PRIVATELINK_STATUS_HISTORY_ENTRIES: VarDefinition = VarDefinition::new(
    "keep_n_privatelink_status_history_entries",
    value!(usize; 5),
    "On reboot, truncate all but the last n entries per ID in the mz_aws_privatelink_connection_status_history \
        collection (Materialize).",
    false,
);

pub static ENABLE_STORAGE_SHARD_FINALIZATION: VarDefinition = VarDefinition::new(
    "enable_storage_shard_finalization",
    value!(bool; true),
    "Whether to allow the storage client to finalize shards (Materialize).",
    false,
);

pub static ENABLE_CONSOLIDATE_AFTER_UNION_NEGATE: VarDefinition = VarDefinition::new(
    "enable_consolidate_after_union_negate",
    value!(bool; true),
    "consolidation after Unions that have a Negated input (Materialize).",
    true,
);

pub static MIN_TIMESTAMP_INTERVAL: VarDefinition = VarDefinition::new(
    "min_timestamp_interval",
    value!(Duration; Duration::from_millis(1000)),
    "Minimum timestamp interval",
    false,
);

pub static MAX_TIMESTAMP_INTERVAL: VarDefinition = VarDefinition::new(
    "max_timestamp_interval",
    value!(Duration; Duration::from_millis(1000)),
    "Maximum timestamp interval",
    false,
);

pub static WEBHOOK_CONCURRENT_REQUEST_LIMIT: VarDefinition = VarDefinition::new(
    "webhook_concurrent_request_limit",
    value!(usize; WEBHOOK_CONCURRENCY_LIMIT),
    "Maximum number of concurrent requests for appending to a webhook source.",
    false,
);

pub static USER_STORAGE_MANAGED_COLLECTIONS_BATCH_DURATION: VarDefinition = VarDefinition::new(
    "user_storage_managed_collections_batch_duration",
    value!(Duration; STORAGE_MANAGED_COLLECTIONS_BATCH_DURATION_DEFAULT),
    "Duration which we'll wait to collect a batch of events for a webhook source.",
    false,
);

/// Configuration for gRPC client connections.
pub mod grpc_client {
    use super::*;

    pub static CONNECT_TIMEOUT: VarDefinition = VarDefinition::new(
        "grpc_client_connect_timeout",
        value!(Duration; Duration::from_secs(5)),
        "Timeout to apply to initial gRPC client connection establishment.",
        false,
    );

    pub static HTTP2_KEEP_ALIVE_INTERVAL: VarDefinition = VarDefinition::new(
        "grpc_client_http2_keep_alive_interval",
        value!(Duration; Duration::from_secs(3)),
        "Idle time to wait before sending HTTP/2 PINGs to maintain established gRPC client connections.",
        false,
    );

    pub static HTTP2_KEEP_ALIVE_TIMEOUT: VarDefinition = VarDefinition::new(
        "grpc_client_http2_keep_alive_timeout",
        value!(Duration; Duration::from_secs(5)),
        "Time to wait for HTTP/2 pong response before terminating a gRPC client connection.",
        false,
    );
}

/// Configuration for how cluster replicas are scheduled.
pub mod cluster_scheduling {
    use super::*;
    use mz_orchestrator::scheduling_config::*;

    pub static CLUSTER_MULTI_PROCESS_REPLICA_AZ_AFFINITY_WEIGHT: VarDefinition = VarDefinition::new(
        "cluster_multi_process_replica_az_affinity_weight",
        value!(Option<i32>; DEFAULT_POD_AZ_AFFINITY_WEIGHT),
        "Whether or not to add an availability zone affinity between instances of \
            multi-process replicas. Either an affinity weight or empty (off) (Materialize).",
        false,
    );

    pub static CLUSTER_SOFTEN_REPLICATION_ANTI_AFFINITY: VarDefinition = VarDefinition::new(
        "cluster_soften_replication_anti_affinity",
        value!(bool; DEFAULT_SOFTEN_REPLICATION_ANTI_AFFINITY),
        "Whether or not to turn the node-scope anti affinity between replicas \
            in the same cluster into a preference (Materialize).",
        false,
    );

    pub static CLUSTER_SOFTEN_REPLICATION_ANTI_AFFINITY_WEIGHT: VarDefinition = VarDefinition::new(
        "cluster_soften_replication_anti_affinity_weight",
        value!(i32; DEFAULT_SOFTEN_REPLICATION_ANTI_AFFINITY_WEIGHT),
        "The preference weight for `cluster_soften_replication_anti_affinity` (Materialize).",
        false,
    );

    pub static CLUSTER_ENABLE_TOPOLOGY_SPREAD: VarDefinition = VarDefinition::new(
        "cluster_enable_topology_spread",
        value!(bool; DEFAULT_TOPOLOGY_SPREAD_ENABLED),
        "Whether or not to add topology spread constraints among replicas in the same cluster (Materialize).",
        false,
    );

    pub static CLUSTER_TOPOLOGY_SPREAD_IGNORE_NON_SINGULAR_SCALE: VarDefinition = VarDefinition::new(
        "cluster_topology_spread_ignore_non_singular_scale",
        value!(bool; DEFAULT_TOPOLOGY_SPREAD_IGNORE_NON_SINGULAR_SCALE),
        "If true, ignore replicas with more than 1 process when adding topology spread constraints (Materialize).",
        false,
    );

    pub static CLUSTER_TOPOLOGY_SPREAD_MAX_SKEW: VarDefinition = VarDefinition::new(
        "cluster_topology_spread_max_skew",
        value!(i32; DEFAULT_TOPOLOGY_SPREAD_MAX_SKEW),
        "The `maxSkew` for replica topology spread constraints (Materialize).",
        false,
    );

    pub static CLUSTER_TOPOLOGY_SPREAD_SOFT: VarDefinition = VarDefinition::new(
        "cluster_topology_spread_soft",
        value!(bool; DEFAULT_TOPOLOGY_SPREAD_SOFT),
        "If true, soften the topology spread constraints for replicas (Materialize).",
        false,
    );

    pub static CLUSTER_SOFTEN_AZ_AFFINITY: VarDefinition = VarDefinition::new(
        "cluster_soften_az_affinity",
        value!(bool; DEFAULT_SOFTEN_AZ_AFFINITY),
        "Whether or not to turn the az-scope node affinity for replicas. \
            Note this could violate requests from the user (Materialize).",
        false,
    );

    pub static CLUSTER_SOFTEN_AZ_AFFINITY_WEIGHT: VarDefinition = VarDefinition::new(
        "cluster_soften_az_affinity_weight",
        value!(i32; DEFAULT_SOFTEN_AZ_AFFINITY_WEIGHT),
        "The preference weight for `cluster_soften_az_affinity` (Materialize).",
        false,
    );

    pub static CLUSTER_ALWAYS_USE_DISK: VarDefinition = VarDefinition::new(
        "cluster_always_use_disk",
        value!(bool; DEFAULT_ALWAYS_USE_DISK),
        "Always provisions a replica with disk, regardless of `DISK` DDL option.",
        false,
    );

    const DEFAULT_CLUSTER_ALTER_CHECK_READY_INTERVAL: Duration = Duration::from_secs(3);

    pub static CLUSTER_ALTER_CHECK_READY_INTERVAL: VarDefinition = VarDefinition::new(
        "cluster_alter_check_ready_interval",
        value!(Duration; DEFAULT_CLUSTER_ALTER_CHECK_READY_INTERVAL),
        "How often to poll readiness checks for cluster alter",
        false,
    );

    const DEFAULT_CHECK_SCHEDULING_POLICIES_INTERVAL: Duration = Duration::from_secs(3);

    pub static CLUSTER_CHECK_SCHEDULING_POLICIES_INTERVAL: VarDefinition = VarDefinition::new(
        "cluster_check_scheduling_policies_interval",
        value!(Duration; DEFAULT_CHECK_SCHEDULING_POLICIES_INTERVAL),
        "How often policies are invoked to automatically start/stop clusters, e.g., \
            for REFRESH EVERY materialized views.",
        false,
    );

    pub static CLUSTER_SECURITY_CONTEXT_ENABLED: VarDefinition = VarDefinition::new(
        "cluster_security_context_enabled",
        value!(bool; DEFAULT_SECURITY_CONTEXT_ENABLED),
        "Enables SecurityContext for clusterd instances, restricting capabilities to improve security.",
        false,
    );
}

/// Macro to simplify creating feature flags, i.e. boolean flags that we use to toggle the
/// availability of features.
///
/// The arguments to `feature_flags!` are:
/// - `$name`, which will be the name of the feature flag, in snake_case
/// - `$feature_desc`, a human-readable description of the feature
/// - `$value`, which if not provided, defaults to `false`
///
/// Note that not all `VarDefinition<bool>` are feature flags. Feature flags are for variables that:
/// - Belong to `SystemVars`, _not_ `SessionVars`
/// - Default to false and must be explicitly enabled, or default to `true` and can be explicitly disabled.
///
/// WARNING / CONTRACT: Syntax-related feature flags must always *enable* behavior. In other words,
/// setting a feature flag must make the system more permissive. For example, let's suppose we'd like
/// to gate deprecated upsert syntax behind a feature flag. In this case, do not add a feature flag
/// like `disable_deprecated_upsert_syntax`, as `disable_deprecated_upsert_syntax = on` would
/// _prevent_ the system from parsing the deprecated upsert syntax. Instead, use a feature flag
/// like `enable_deprecated_upsert_syntax`.
///
/// The hazard this protects against is related to reboots after feature flags have been disabled.
/// Say someone creates a Kinesis source while `enable_kinesis_sources = on`. Materialize will
/// commit this source to the system catalog. Then, suppose we discover a catastrophic bug in
/// Kinesis sources and set `enable_kinesis_sources` to `off`. This prevents users from creating
/// new Kinesis sources, but leaves the existing Kinesis sources in place. This is because
/// disabling a feature flag doesn't remove access to catalog objects created while the feature
/// flag was live. On the next reboot, Materialize will proceed to load the Kinesis source from the
/// catalog, reparsing and replanning the `CREATE SOURCE` definition and rechecking the
/// `enable_kinesis_sources` feature flag along the way. Even though the feature flag has been
/// switched to `off`, we need to temporarily re-enable it during parsing and planning to be able
/// to boot successfully.
///
/// Ensuring that all syntax-related feature flags *enable* behavior means that setting all such
/// feature flags to `on` during catalog boot has the desired effect.
macro_rules! feature_flags {
    // Match `$name, $feature_desc, $value`.
    (@inner
        // The feature flag name.
        name: $name:expr,
        // The feature flag description.
        desc: $desc:literal,
        // The feature flag default value.
        default: $value:expr,
    ) => {
        paste::paste!{
            // Note that the ServerVar is not directly exported; we expect these to be
            // accessible through their FeatureFlag variant.
            static [<$name:upper _VAR>]: VarDefinition = VarDefinition::new(
                stringify!($name),
                value!(bool; $value),
                concat!("Whether ", $desc, " is allowed (Materialize)."),
                false,
            );

            pub static [<$name:upper >]: FeatureFlag = FeatureFlag {
                flag: &[<$name:upper _VAR>],
                feature_desc: $desc,
            };
        }
    };
    ($({
        // The feature flag name.
        name: $name:expr,
        // The feature flag description.
        desc: $desc:literal,
        // The feature flag default value.
        default: $value:expr,
        // Should the feature be turned on during catalog rehydration when
        // parsing a catalog item.
        enable_for_item_parsing: $enable_for_item_parsing:expr,
    },)+) => {
        $(feature_flags! { @inner
            name: $name,
            desc: $desc,
            default: $value,
        })+

        paste::paste!{
            pub static FEATURE_FLAGS: &'static [&'static VarDefinition] = &[
                $(  & [<$name:upper _VAR>] , )+
            ];
        }

        paste::paste!{
            impl super::SystemVars {
                pub fn enable_all_feature_flags_by_default(&mut self) {
                    $(
                        self.set_default(stringify!($name), super::VarInput::Flat("on"))
                            .expect("setting default value must work");
                    )+
                }

                pub fn enable_for_item_parsing(&mut self) {
                    $(
                        if $enable_for_item_parsing {
                            self.set(stringify!($name), super::VarInput::Flat("on"))
                                .expect("setting default value must work");
                        }
                    )+
                }

                $(
                    pub fn [<$name:lower>](&self) -> bool {
                        *self.expect_value(&[<$name:upper _VAR>])
                    }
                )+
            }
        }
    }
}

feature_flags!(
    // Gates for other feature flags
    {
        name: allow_real_time_recency,
        desc: "real time recency",
        default: false,
        enable_for_item_parsing: true,
    },
    // Actual feature flags
    {
        name: enable_binary_date_bin,
        desc: "the binary version of date_bin function",
        default: false,
        enable_for_item_parsing: true,
    },
    {
        name: enable_create_sink_denylist_with_options,
        desc: "CREATE SINK with unsafe options",
        default: false,
        enable_for_item_parsing: true,
    },
    {
        name: enable_create_source_denylist_with_options,
        desc: "CREATE SOURCE with unsafe options",
        default: false,
        enable_for_item_parsing: true,
    },
    {
        name: enable_date_bin_hopping,
        desc: "the date_bin_hopping function",
        default: false,
        enable_for_item_parsing: true,
    },
    {
        name: enable_envelope_debezium_in_subscribe,
        desc: "`ENVELOPE DEBEZIUM (KEY (..))`",
        default: false,
        enable_for_item_parsing: true,
    },
    {
        name: enable_envelope_materialize,
        desc: "ENVELOPE MATERIALIZE",
        default: false,
        enable_for_item_parsing: true,
    },
    {
        name: enable_explain_pushdown,
        desc: "EXPLAIN FILTER PUSHDOWN",
        default: false,
        enable_for_item_parsing: true,
    },
    {
        name: enable_index_options,
        desc: "INDEX OPTIONS",
        default: false,
        enable_for_item_parsing: true,
    },
    {
        name: enable_list_length_max,
        desc: "the list_length_max function",
        default: false,
        enable_for_item_parsing: true,
    },
    {
        name: enable_list_n_layers,
        desc: "the list_n_layers function",
        default: false,
        enable_for_item_parsing: true,
    },
    {
        name: enable_list_remove,
        desc: "the list_remove function",
        default: false,
        enable_for_item_parsing: true,
    },
    {

        name: enable_logical_compaction_window,
        desc: "RETAIN HISTORY",
        default: false,
        enable_for_item_parsing: true,
    },
    {
        name: enable_primary_key_not_enforced,
        desc: "PRIMARY KEY NOT ENFORCED",
        default: false,
        enable_for_item_parsing: true,
    },
    {
        name: enable_multi_worker_storage_persist_sink,
        desc: "multi-worker storage persist sink",
        default: true,
        enable_for_item_parsing: true,
    },
    {
        name: enable_persist_streaming_snapshot_and_fetch,
        desc: "use the new streaming consolidate for snapshot_and_fetch",
        default: false,
        enable_for_item_parsing: true,
    },
    {
        name: enable_persist_streaming_compaction,
        desc: "use the new streaming consolidate for compaction",
        default: false,
        enable_for_item_parsing: true,
    },
    {
        name: enable_raise_statement,
        desc: "RAISE statement",
        default: false,
        enable_for_item_parsing: true,
    },
    {
        name: enable_repeat_row,
        desc: "the repeat_row function",
        default: false,
        enable_for_item_parsing: true,
    },
    {
        name: enable_table_check_constraint,
        desc: "CREATE TABLE with a check constraint",
        default: false,
        enable_for_item_parsing: true,
    },
    {
        name: enable_table_foreign_key,
        desc: "CREATE TABLE with a foreign key",
        default: false,
        enable_for_item_parsing: true,
    },
    {
        name: enable_table_keys,
        desc: "CREATE TABLE with a primary key or unique constraint",
        default: false,
        enable_for_item_parsing: true,
    },
    {
        name: enable_unorchestrated_cluster_replicas,
        desc: "unorchestrated cluster replicas",
        default: false,
        enable_for_item_parsing: true,
    },
    {
        name: enable_unstable_dependencies,
        desc: "depending on unstable objects",
        default: false,
        enable_for_item_parsing: true,
    },
    {
        name: enable_disk_cluster_replicas,
        desc: "`WITH (DISK)` for cluster replicas",
        default: true,
        enable_for_item_parsing: true,
    },
    {
        name: enable_within_timestamp_order_by_in_subscribe,
        desc: "`WITHIN TIMESTAMP ORDER BY ..`",
        default: false,
        enable_for_item_parsing: true,
    },
    {
        name: enable_cardinality_estimates,
        desc: "join planning with cardinality estimates",
        default: false,
        enable_for_item_parsing: true,
    },
    {
        name: enable_connection_validation_syntax,
        desc: "CREATE CONNECTION .. WITH (VALIDATE) and VALIDATE CONNECTION syntax",
        default: true,
        enable_for_item_parsing: true,
    },
    {
        name: enable_alter_set_cluster,
        desc: "ALTER ... SET CLUSTER syntax",
        default: false,
        enable_for_item_parsing: true,
    },
    {
        name: enable_unsafe_functions,
        desc: "executing potentially dangerous functions",
        default: false,
        enable_for_item_parsing: true,
    },
    {
        name: enable_managed_cluster_availability_zones,
        desc: "MANAGED, AVAILABILITY ZONES syntax",
        default: false,
        enable_for_item_parsing: true,
    },
    {
        name: statement_logging_use_reproducible_rng,
        desc: "statement logging with reproducible RNG",
        default: false,
        enable_for_item_parsing: true,
    },
    {
        name: enable_notices_for_index_already_exists,
        desc: "emitting notices for IndexAlreadyExists (doesn't affect EXPLAIN)",
        default: true,
        enable_for_item_parsing: true,
    },
    {
        name: enable_notices_for_index_too_wide_for_literal_constraints,
        desc: "emitting notices for IndexTooWideForLiteralConstraints (doesn't affect EXPLAIN)",
        default: false,
        enable_for_item_parsing: true,
    },
    {
        name: enable_notices_for_index_empty_key,
        desc: "emitting notices for indexes with an empty key (doesn't affect EXPLAIN)",
        default: true,
        enable_for_item_parsing: true,
    },
    {
        name: enable_comment,
        desc: "the COMMENT ON feature for objects",
        default: true,
        enable_for_item_parsing: true,
    },
    {
        name: enable_alter_swap,
        desc: "the ALTER SWAP feature for objects",
        default: true,
        enable_for_item_parsing: true,
    },
    {
        name: enable_new_outer_join_lowering,
        desc: "new outer join lowering",
        default: true,
        enable_for_item_parsing: false,
    },
    {
        name: enable_time_at_time_zone,
        desc: "use of AT TIME ZONE or timezone() with time type",
        default: false,
        enable_for_item_parsing: true,
    },
    {
        name: enable_load_generator_key_value,
        desc: "Create a LOAD GENERATOR KEY VALUE",
        default: false,
        enable_for_item_parsing: false,
    },
    {
        name: enable_expressions_in_limit_syntax,
        desc: "LIMIT <expr> syntax",
        default: false,
        enable_for_item_parsing: true,
    },
    {
        name: enable_mz_notices,
        desc: "Populate the contents of `mz_internal.mz_notices`",
        default: true,
        enable_for_item_parsing: false,
    },
    {
        name: enable_eager_delta_joins,
        desc:
            "eager delta joins",
        default: false,
        enable_for_item_parsing: false,
    },
    {
        name: enable_off_thread_optimization,
        desc: "use off-thread optimization in `CREATE` statements",
        default: true,
        enable_for_item_parsing: false,
    },
    {
        name: enable_refresh_every_mvs,
        desc: "REFRESH EVERY and REFRESH AT materialized views",
        default: false,
        enable_for_item_parsing: true,
    },
    {
        name: enable_cluster_schedule_refresh,
        desc: "`SCHEDULE = ON REFRESH` cluster option",
        default: false,
        enable_for_item_parsing: true,
    },
    {
        name: enable_reduce_mfp_fusion,
        desc: "fusion of MFPs in reductions",
        default: false,
        enable_for_item_parsing: false,
    },
    {
        name: enable_worker_core_affinity,
        desc: "set core affinity for replica worker threads",
        default: false,
        enable_for_item_parsing: false,
    },
    {
        name: enable_copy_to_expr,
        desc: "COPY ... TO 's3://...'",
        default: false,
        enable_for_item_parsing: false,
    },
    {
        name: enable_session_timelines,
        desc: "strong session serializable isolation levels",
        default: false,
        enable_for_item_parsing: false,
    },
    {
        name: enable_variadic_left_join_lowering,
        desc: "Enable joint HIR ⇒ MIR lowering of stacks of left joins",
        default: false,
        enable_for_item_parsing: false,
    },
    {
        name: enable_redacted_test_option,
        desc: "Enable useless option to test value redaction",
        default: false,
        enable_for_item_parsing: true,
    },
    {
        name: enable_letrec_fixpoint_analysis,
        desc: "Enable Lattice-based fixpoint iteration on LetRec nodes in the Analysis framework",
        default: true, // This is just a failsafe switch for the deployment of #25591.
        enable_for_item_parsing: false,
    },
    {
        name: enable_kafka_sink_headers,
        desc: "Enable the HEADERS option for Kafka sinks",
        default: false,
        enable_for_item_parsing: true,
    },
    {
        name: enable_unlimited_retain_history,
        desc: "Disable limits on RETAIN HISTORY (below 1s default, and 0 disables compaction).",
        default: false,
        enable_for_item_parsing: true,
    },
    {
        name: enable_envelope_upsert_inline_errors,
        desc: "The VALUE DECODING ERRORS = INLINE option on ENVELOPE UPSERT",
        default: false,
        enable_for_item_parsing: true,
    },
    {
        name: enable_outer_join_null_filter,
        desc: "Add an extra null filter to the semi-join part of outer join lowering",
        default: true,
        enable_for_item_parsing: false,
    },
    {
        name: enable_alter_table_add_column,
        desc: "Enable ALTER TABLE ... ADD COLUMN ...",
        default: false,
        enable_for_item_parsing: true,
    },
    {
        name: enable_graceful_cluster_reconfiguration,
        desc: "Enable graceful reconfiguration for alter cluster",
        default: false,
        enable_for_item_parsing: false,
    },
    {
        name: enable_aws_msk_iam_auth,
        desc: "Enable AWS MSK IAM authentication for Kafka connections",
        default: false,
        enable_for_item_parsing: true,
    },
    {
        name: enable_clock_load_generator,
        desc: "Enable the clock load generator",
        default: false,
        enable_for_item_parsing: true,
    },
    {
        name: enable_yugabyte_connection,
        desc: "Create a YUGABYTE connection",
        default: false,
        enable_for_item_parsing: false,
    },
    {
        name: enable_value_window_function_fusion,
        desc: "Enables the value window function fusion optimization",
        default: true,
        enable_for_item_parsing: false,
    },
);

impl From<&super::SystemVars> for OptimizerFeatures {
    fn from(vars: &super::SystemVars) -> Self {
        Self {
            enable_consolidate_after_union_negate: vars.enable_consolidate_after_union_negate(),
            enable_eager_delta_joins: vars.enable_eager_delta_joins(),
            enable_new_outer_join_lowering: vars.enable_new_outer_join_lowering(),
            enable_reduce_mfp_fusion: vars.enable_reduce_mfp_fusion(),
            enable_variadic_left_join_lowering: vars.enable_variadic_left_join_lowering(),
            enable_letrec_fixpoint_analysis: vars.enable_letrec_fixpoint_analysis(),
            enable_cardinality_estimates: vars.enable_cardinality_estimates(),
            enable_outer_join_null_filter: vars.enable_outer_join_null_filter(),
            enable_value_window_function_fusion: vars.enable_value_window_function_fusion(),
            persist_fast_path_limit: vars.persist_fast_path_limit(),
            reoptimize_imported_views: false,
        }
    }
}

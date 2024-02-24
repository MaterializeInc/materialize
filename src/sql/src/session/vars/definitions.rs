// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

// We pretend to be Postgres v9.5.0, which is also what CockroachDB pretends to
// be. Too new and some clients will emit a "server too new" warning. Too old
// and some clients will fall back to legacy code paths. v9.5.0 empirically
// seems to be a good compromise.

use std::clone::Clone;
use std::fmt::Debug;
use std::str::FromStr;
use std::string::ToString;
use std::time::Duration;

use chrono::{DateTime, Utc};
use mz_adapter_types::timestamp_oracle::{
    DEFAULT_PG_TIMESTAMP_ORACLE_CONNPOOL_MAX_SIZE, DEFAULT_PG_TIMESTAMP_ORACLE_CONNPOOL_MAX_WAIT,
    DEFAULT_PG_TIMESTAMP_ORACLE_CONNPOOL_TTL, DEFAULT_PG_TIMESTAMP_ORACLE_CONNPOOL_TTL_STAGGER,
};
use mz_ore::cast;
use mz_ore::cast::CastFrom;
use mz_repr::adt::numeric::Numeric;
use mz_repr::adt::timestamp::CheckedTimestamp;
use mz_repr::bytes::ByteSize;
use mz_sql_parser::ast::Ident;
use mz_sql_parser::ident;
use mz_storage_types::controller::PersistTxnTablesImpl;
use mz_tracing::{CloneableEnvFilter, SerializableDirective};
use once_cell::sync::Lazy;
use uncased::UncasedStr;

use crate::session::user::{User, SUPPORT_USER, SYSTEM_USER};
use crate::session::vars::errors::VarError;
use crate::session::vars::value::{
    CatalogKind, ClientEncoding, ClientSeverity, DateStyle, Failpoints, IntervalStyle,
    IsolationLevel, TimeZone, TimestampOracleImpl, DEFAULT_DATE_STYLE,
};
use crate::session::vars::{FeatureFlag, SystemVars, Value, Var, VarInput};
use crate::{DEFAULT_SCHEMA, WEBHOOK_CONCURRENCY_LIMIT};

/// A `ServerVar` is the default value for a configuration parameter.
#[derive(Debug)]
pub struct ServerVar<V>
where
    V: Debug + 'static,
{
    pub name: &'static UncasedStr,
    pub value: V,
    pub description: &'static str,
    pub internal: bool,
}

impl<V: Clone + Debug + 'static> Clone for ServerVar<V> {
    fn clone(&self) -> Self {
        Self {
            name: self.name,
            value: self.value.clone(),
            description: self.description,
            internal: self.internal,
        }
    }
}

impl<V> Var for ServerVar<V>
where
    V: Value + Debug + PartialEq + 'static,
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

    fn type_name(&self) -> String {
        V::type_name()
    }

    fn visible(&self, user: &User, system_vars: Option<&SystemVars>) -> Result<(), VarError> {
        if self.internal && user != &*SYSTEM_USER && user != &*SUPPORT_USER {
            Err(VarError::UnknownParameter(self.name().to_string()))
        } else if self.name().starts_with("unsafe")
            && match system_vars {
                None => true,
                Some(system_vars) => !system_vars.allow_unsafe,
            }
        {
            Err(VarError::RequiresUnsafeMode(self.name()))
        } else {
            Ok(())
        }
    }
}

/// The major version of PostgreSQL that Materialize claims to be.
pub const SERVER_MAJOR_VERSION: u8 = 9;

/// The minor version of PostgreSQL that Materialize claims to be.
pub const SERVER_MINOR_VERSION: u8 = 5;

/// The patch version of PostgreSQL that Materialize claims to be.
pub const SERVER_PATCH_VERSION: u8 = 0;

/// The name of the default database that Materialize uses.
pub const DEFAULT_DATABASE_NAME: &str = "materialize";

pub static APPLICATION_NAME: Lazy<ServerVar<String>> = Lazy::new(|| ServerVar {
    name: UncasedStr::new("application_name"),
    value: "".to_string(),
    description: "Sets the application name to be reported in statistics and logs (PostgreSQL).",
    internal: false,
});

pub static CLIENT_ENCODING: ServerVar<ClientEncoding> = ServerVar {
    name: UncasedStr::new("client_encoding"),
    value: ClientEncoding::Utf8,
    description: "Sets the client's character set encoding (PostgreSQL).",
    internal: false,
};

pub const CLIENT_MIN_MESSAGES: ServerVar<ClientSeverity> = ServerVar {
    name: UncasedStr::new("client_min_messages"),
    value: ClientSeverity::Notice,
    description: "Sets the message levels that are sent to the client (PostgreSQL).",
    internal: false,
};

pub const CLUSTER_VAR_NAME: &UncasedStr = UncasedStr::new("cluster");
pub static CLUSTER: Lazy<ServerVar<String>> = Lazy::new(|| ServerVar {
    name: CLUSTER_VAR_NAME,
    value: "quickstart".to_string(),
    description: "Sets the current cluster (Materialize).",
    internal: false,
});

pub const CLUSTER_REPLICA: ServerVar<Option<String>> = ServerVar {
    name: UncasedStr::new("cluster_replica"),
    value: None,
    description: "Sets a target cluster replica for SELECT queries (Materialize).",
    internal: false,
};

pub const DATABASE_VAR_NAME: &UncasedStr = UncasedStr::new("database");
pub static DATABASE: Lazy<ServerVar<String>> = Lazy::new(|| ServerVar {
    name: DATABASE_VAR_NAME,
    value: DEFAULT_DATABASE_NAME.to_string(),
    description: "Sets the current database (CockroachDB).",
    internal: false,
});

pub static DATE_STYLE: ServerVar<DateStyle> = ServerVar {
    // DateStyle has nonstandard capitalization for historical reasons.
    name: UncasedStr::new("DateStyle"),
    value: DEFAULT_DATE_STYLE,
    description: "Sets the display format for date and time values (PostgreSQL).",
    internal: false,
};

pub const EXTRA_FLOAT_DIGITS: ServerVar<i32> = ServerVar {
    name: UncasedStr::new("extra_float_digits"),
    value: 3,
    description: "Adjusts the number of digits displayed for floating-point values (PostgreSQL).",
    internal: false,
};

pub const FAILPOINTS: ServerVar<Failpoints> = ServerVar {
    name: UncasedStr::new("failpoints"),
    value: Failpoints,
    description: "Allows failpoints to be dynamically activated.",
    internal: false,
};

pub const INTEGER_DATETIMES: ServerVar<bool> = ServerVar {
    name: UncasedStr::new("integer_datetimes"),
    value: true,
    description: "Reports whether the server uses 64-bit-integer dates and times (PostgreSQL).",
    internal: false,
};

pub static INTERVAL_STYLE: ServerVar<IntervalStyle> = ServerVar {
    // IntervalStyle has nonstandard capitalization for historical reasons.
    name: UncasedStr::new("IntervalStyle"),
    value: IntervalStyle::Postgres,
    description: "Sets the display format for interval values (PostgreSQL).",
    internal: false,
};

pub const MZ_VERSION_NAME: &UncasedStr = UncasedStr::new("mz_version");
pub const IS_SUPERUSER_NAME: &UncasedStr = UncasedStr::new("is_superuser");

// Schema can be used an alias for a search path with a single element.
pub const SCHEMA_ALIAS: &UncasedStr = UncasedStr::new("schema");
pub static SEARCH_PATH: Lazy<ServerVar<Vec<Ident>>> = Lazy::new(|| ServerVar {
    name: UncasedStr::new("search_path"),
    value: vec![ident!(DEFAULT_SCHEMA)],
    description:
        "Sets the schema search order for names that are not schema-qualified (PostgreSQL).",
    internal: false,
});

pub const STATEMENT_TIMEOUT: ServerVar<Duration> = ServerVar {
    name: UncasedStr::new("statement_timeout"),
    value: Duration::from_secs(10),
    description:
        "Sets the maximum allowed duration of INSERT...SELECT, UPDATE, and DELETE operations. \
        If this value is specified without units, it is taken as milliseconds.",
    internal: false,
};

pub const IDLE_IN_TRANSACTION_SESSION_TIMEOUT: ServerVar<Duration> = ServerVar {
    name: UncasedStr::new("idle_in_transaction_session_timeout"),
    value: Duration::from_secs(60 * 2),
    description:
        "Sets the maximum allowed duration that a session can sit idle in a transaction before \
         being terminated. If this value is specified without units, it is taken as milliseconds. \
         A value of zero disables the timeout (PostgreSQL).",
    internal: false,
};

pub static SERVER_VERSION: Lazy<ServerVar<String>> = Lazy::new(|| ServerVar {
    name: UncasedStr::new("server_version"),
    value: format!(
        "{}.{}.{}",
        SERVER_MAJOR_VERSION, SERVER_MINOR_VERSION, SERVER_PATCH_VERSION
    ),
    description: "Shows the PostgreSQL compatible server version (PostgreSQL).",
    internal: false,
});

pub const SERVER_VERSION_NUM: ServerVar<i32> = ServerVar {
    name: UncasedStr::new("server_version_num"),
    value: ((cast::u8_to_i32(SERVER_MAJOR_VERSION) * 10_000)
        + (cast::u8_to_i32(SERVER_MINOR_VERSION) * 100)
        + cast::u8_to_i32(SERVER_PATCH_VERSION)),
    description: "Shows the PostgreSQL compatible server version as an integer (PostgreSQL).",
    internal: false,
};

pub const SQL_SAFE_UPDATES: ServerVar<bool> = ServerVar {
    name: UncasedStr::new("sql_safe_updates"),
    value: false,
    description: "Prohibits SQL statements that may be overly destructive (CockroachDB).",
    internal: false,
};

pub const STANDARD_CONFORMING_STRINGS: ServerVar<bool> = ServerVar {
    name: UncasedStr::new("standard_conforming_strings"),
    value: true,
    description: "Causes '...' strings to treat backslashes literally (PostgreSQL).",
    internal: false,
};

pub const TIMEZONE: ServerVar<TimeZone> = ServerVar {
    // TimeZone has nonstandard capitalization for historical reasons.
    name: UncasedStr::new("TimeZone"),
    value: TimeZone::UTC,
    description: "Sets the time zone for displaying and interpreting time stamps (PostgreSQL).",
    internal: false,
};

pub const TRANSACTION_ISOLATION_VAR_NAME: &UncasedStr = UncasedStr::new("transaction_isolation");
pub const TRANSACTION_ISOLATION: ServerVar<IsolationLevel> = ServerVar {
    name: TRANSACTION_ISOLATION_VAR_NAME,
    value: IsolationLevel::StrictSerializable,
    description: "Sets the current transaction's isolation level (PostgreSQL).",
    internal: false,
};

pub const MAX_KAFKA_CONNECTIONS: ServerVar<u32> = ServerVar {
    name: UncasedStr::new("max_kafka_connections"),
    value: 1000,
    description:
        "The maximum number of Kafka connections in the region, across all schemas (Materialize).",
    internal: false,
};

pub const MAX_POSTGRES_CONNECTIONS: ServerVar<u32> = ServerVar {
    name: UncasedStr::new("max_postgres_connections"),
    value: 1000,
    description: "The maximum number of PostgreSQL connections in the region, across all schemas (Materialize).",
    internal: false
};

pub const MAX_AWS_PRIVATELINK_CONNECTIONS: ServerVar<u32> = ServerVar {
    name: UncasedStr::new("max_aws_privatelink_connections"),
    value: 0,
    description: "The maximum number of AWS PrivateLink connections in the region, across all schemas (Materialize).",
    internal: false
};

pub const MAX_TABLES: ServerVar<u32> = ServerVar {
    name: UncasedStr::new("max_tables"),
    value: 25,
    description: "The maximum number of tables in the region, across all schemas (Materialize).",
    internal: false,
};

pub const MAX_SOURCES: ServerVar<u32> = ServerVar {
    name: UncasedStr::new("max_sources"),
    value: 25,
    description: "The maximum number of sources in the region, across all schemas (Materialize).",
    internal: false,
};

pub const MAX_SINKS: ServerVar<u32> = ServerVar {
    name: UncasedStr::new("max_sinks"),
    value: 25,
    description: "The maximum number of sinks in the region, across all schemas (Materialize).",
    internal: false,
};

pub const MAX_MATERIALIZED_VIEWS: ServerVar<u32> = ServerVar {
    name: UncasedStr::new("max_materialized_views"),
    value: 100,
    description:
        "The maximum number of materialized views in the region, across all schemas (Materialize).",
    internal: false,
};

pub const MAX_CLUSTERS: ServerVar<u32> = ServerVar {
    name: UncasedStr::new("max_clusters"),
    value: 10,
    description: "The maximum number of clusters in the region (Materialize).",
    internal: false,
};

pub const MAX_REPLICAS_PER_CLUSTER: ServerVar<u32> = ServerVar {
    name: UncasedStr::new("max_replicas_per_cluster"),
    value: 5,
    description: "The maximum number of replicas of a single cluster (Materialize).",
    internal: false,
};

pub static MAX_CREDIT_CONSUMPTION_RATE: Lazy<ServerVar<Numeric>> = Lazy::new(|| {
    ServerVar {
        name: UncasedStr::new("max_credit_consumption_rate"),
        value: 1024.into(),
        description: "The maximum rate of credit consumption in a region. Credits are consumed based on the size of cluster replicas in use (Materialize).",
        internal: false
    }
});

pub const MAX_DATABASES: ServerVar<u32> = ServerVar {
    name: UncasedStr::new("max_databases"),
    value: 1000,
    description: "The maximum number of databases in the region (Materialize).",
    internal: false,
};

pub const MAX_SCHEMAS_PER_DATABASE: ServerVar<u32> = ServerVar {
    name: UncasedStr::new("max_schemas_per_database"),
    value: 1000,
    description: "The maximum number of schemas in a database (Materialize).",
    internal: false,
};

pub const MAX_OBJECTS_PER_SCHEMA: ServerVar<u32> = ServerVar {
    name: UncasedStr::new("max_objects_per_schema"),
    value: 1000,
    description: "The maximum number of objects in a schema (Materialize).",
    internal: false,
};

pub const MAX_SECRETS: ServerVar<u32> = ServerVar {
    name: UncasedStr::new("max_secrets"),
    value: 100,
    description: "The maximum number of secrets in the region, across all schemas (Materialize).",
    internal: false,
};

pub const MAX_ROLES: ServerVar<u32> = ServerVar {
    name: UncasedStr::new("max_roles"),
    value: 1000,
    description: "The maximum number of roles in the region (Materialize).",
    internal: false,
};

// Cloud environmentd is configured with 4 GiB of RAM, so 1 GiB is a good heuristic for a single
// query.
// TODO(jkosh44) Eventually we want to be able to return arbitrary sized results.
pub const MAX_RESULT_SIZE: ServerVar<ByteSize> = ServerVar {
    name: UncasedStr::new("max_result_size"),
    value: ByteSize::gb(1),
    description: "The maximum size in bytes for an internal query result (Materialize).",
    internal: false,
};

pub const MAX_QUERY_RESULT_SIZE: ServerVar<ByteSize> = ServerVar {
    name: UncasedStr::new("max_query_result_size"),
    value: ByteSize::gb(1),
    description: "The maximum size in bytes for a single query's result (Materialize).",
    internal: false,
};

pub const MAX_COPY_FROM_SIZE: ServerVar<u32> = ServerVar {
    name: UncasedStr::new("max_copy_from_size"),
    // 1 GiB, this limit is noted in the docs, if you change it make sure to update our docs.
    value: 1_073_741_824,
    description: "The maximum size in bytes we buffer for COPY FROM statements (Materialize).",
    internal: false,
};

pub const MAX_IDENTIFIER_LENGTH: ServerVar<usize> = ServerVar {
    name: UncasedStr::new("max_identifier_length"),
    value: mz_sql_lexer::lexer::MAX_IDENTIFIER_LENGTH,
    description: "The maximum length of object identifiers in bytes (PostgreSQL).",
    internal: false,
};

pub const WELCOME_MESSAGE: ServerVar<bool> = ServerVar {
    name: UncasedStr::new("welcome_message"),
    value: true,
    description: "Whether to send a notice with a welcome message after a successful connection (Materialize).",
    internal: false,
};

/// The logical compaction window for builtin tables and sources that have the
/// `retained_metrics_relation` flag set.
///
/// The existence of this variable is a bit of a hack until we have a fully
/// general solution for controlling retention windows.
pub const METRICS_RETENTION: ServerVar<Duration> = ServerVar {
    name: UncasedStr::new("metrics_retention"),
    // 30 days
    value: Duration::from_secs(30 * 24 * 60 * 60),
    description: "The time to retain cluster utilization metrics (Materialize).",
    internal: true,
};

pub static ALLOWED_CLUSTER_REPLICA_SIZES: Lazy<ServerVar<Vec<Ident>>> = Lazy::new(|| ServerVar {
    name: UncasedStr::new("allowed_cluster_replica_sizes"),
    value: Vec::new(),
    description: "The allowed sizes when creating a new cluster replica (Materialize).",
    internal: false,
});

pub const PERSIST_FAST_PATH_LIMIT: ServerVar<usize> = ServerVar {
    name: UncasedStr::new("persist_fast_path_limit"),
    value: 0,
    description:
        "An exclusive upper bound on the number of results we may return from a Persist fast-path peek; \
        queries that may return more results will follow the normal / slow path. \
        Setting this to 0 disables the feature.",
    internal: true,
};

pub const PERSIST_TXN_TABLES: ServerVar<PersistTxnTablesImpl> = ServerVar {
    name: UncasedStr::new("persist_txn_tables"),
    value: PersistTxnTablesImpl::Eager,
    description: "\
        Whether to use the new persist-txn tables implementation or the legacy \
        one.

        Only takes effect on restart. Any changes will also cause clusterd \
        processes to restart.

        This value is also configurable via a Launch Darkly parameter of the \
        same name, but we keep the flag to make testing easier. If specified, \
        the flag takes precedence over the Launch Darkly param.",
    internal: true,
};

pub const TIMESTAMP_ORACLE_IMPL: ServerVar<TimestampOracleImpl> = ServerVar {
    name: UncasedStr::new("timestamp_oracle"),
    value: TimestampOracleImpl::Postgres,
    description: "Backing implementation of TimestampOracle.",
    internal: true,
};

pub const CATALOG_KIND_IMPL: ServerVar<Option<CatalogKind>> = ServerVar {
    name: UncasedStr::new("catalog_kind"),
    value: None,
    description: "Backing implementation of catalog.",
    internal: true,
};

/// Controls `mz_adapter::coord::timestamp_oracle::postgres_oracle::DynamicConfig::pg_connection_pool_max_size`.
pub const PG_TIMESTAMP_ORACLE_CONNECTION_POOL_MAX_SIZE: ServerVar<usize> = ServerVar {
    name: UncasedStr::new("pg_timestamp_oracle_connection_pool_max_size"),
    value: DEFAULT_PG_TIMESTAMP_ORACLE_CONNPOOL_MAX_SIZE,
    description: "Maximum size of the Postgres/CRDB connection pool, used by the Postgres/CRDB timestamp oracle.",
    internal: true,
};

/// Controls `mz_adapter::coord::timestamp_oracle::postgres_oracle::DynamicConfig::pg_connection_pool_max_wait`.
pub const PG_TIMESTAMP_ORACLE_CONNECTION_POOL_MAX_WAIT: ServerVar<Option<Duration>> = ServerVar {
    name: UncasedStr::new("pg_timestamp_oracle_connection_pool_max_wait"),
    value: Some(DEFAULT_PG_TIMESTAMP_ORACLE_CONNPOOL_MAX_WAIT),
    description: "The maximum time to wait when attempting to obtain a connection from the Postgres/CRDB connection pool, used by the Postgres/CRDB timestamp oracle.",
    internal: true,
};

/// Controls `mz_adapter::coord::timestamp_oracle::postgres_oracle::DynamicConfig::pg_connection_pool_ttl`.
pub const PG_TIMESTAMP_ORACLE_CONNECTION_POOL_TTL: ServerVar<Duration> = ServerVar {
    name: UncasedStr::new("pg_timestamp_oracle_connection_pool_ttl"),
    value: DEFAULT_PG_TIMESTAMP_ORACLE_CONNPOOL_TTL,
    description: "The minimum TTL of a Consensus connection to Postgres/CRDB before it is proactively terminated",
    internal: true,
};

/// Controls `mz_adapter::coord::timestamp_oracle::postgres_oracle::DynamicConfig::pg_connection_pool_ttl_stagger`.
pub const PG_TIMESTAMP_ORACLE_CONNECTION_POOL_TTL_STAGGER: ServerVar<Duration> = ServerVar {
    name: UncasedStr::new("pg_timestamp_oracle_connection_pool_ttl_stagger"),
    value: DEFAULT_PG_TIMESTAMP_ORACLE_CONNPOOL_TTL_STAGGER,
    description: "The minimum time between TTLing Consensus connections to Postgres/CRDB.",
    internal: true,
};

/// The default for the `DISK` option when creating managed clusters and cluster replicas.
pub const DISK_CLUSTER_REPLICAS_DEFAULT: ServerVar<bool> = ServerVar {
    name: UncasedStr::new("disk_cluster_replicas_default"),
    value: false,
    description: "Whether the disk option for managed clusters and cluster replicas should be enabled by default.",
    internal: true,
};

pub const UNSAFE_NEW_TRANSACTION_WALL_TIME: ServerVar<Option<CheckedTimestamp<DateTime<Utc>>>> = ServerVar {
    name: UncasedStr::new("unsafe_new_transaction_wall_time"),
    value: None,
    description:
        "Sets the wall time for all new explicit or implicit transactions to control the value of `now()`. \
        If not set, uses the system's clock.",
    // This needs to be false because `internal: true` things are only modifiable by the mz_system
    // and mz_support users, and we want sqllogictest to have access with its user. Because the name
    // starts with "unsafe" it still won't be visible or changeable by users unless unsafe mode is
    // enabled.
    internal: false,
};

/// Tuning for RocksDB used by `UPSERT` sources that takes effect on restart.
pub mod upsert_rocksdb {
    use super::*;
    use mz_rocksdb_types::config::{CompactionStyle, CompressionType};

    pub static UPSERT_ROCKSDB_COMPACTION_STYLE: ServerVar<CompactionStyle> = ServerVar {
        name: UncasedStr::new("upsert_rocksdb_compaction_style"),
        value: mz_rocksdb_types::defaults::DEFAULT_COMPACTION_STYLE,
        description: "Tuning parameter for RocksDB as used in `UPSERT/DEBEZIUM` \
                  sources. Described in the `mz_rocksdb_types::config` module. \
                  Only takes effect on source restart (Materialize).",
        internal: true,
    };
    pub const UPSERT_ROCKSDB_OPTIMIZE_COMPACTION_MEMTABLE_BUDGET: ServerVar<usize> = ServerVar {
        name: UncasedStr::new("upsert_rocksdb_optimize_compaction_memtable_budget"),
        value: mz_rocksdb_types::defaults::DEFAULT_OPTIMIZE_COMPACTION_MEMTABLE_BUDGET,
        description: "Tuning parameter for RocksDB as used in `UPSERT/DEBEZIUM` \
                  sources. Described in the `mz_rocksdb_types::config` module. \
                  Only takes effect on source restart (Materialize).",
        internal: true,
    };
    pub const UPSERT_ROCKSDB_LEVEL_COMPACTION_DYNAMIC_LEVEL_BYTES: ServerVar<bool> = ServerVar {
        name: UncasedStr::new("upsert_rocksdb_level_compaction_dynamic_level_bytes"),
        value: mz_rocksdb_types::defaults::DEFAULT_LEVEL_COMPACTION_DYNAMIC_LEVEL_BYTES,
        description: "Tuning parameter for RocksDB as used in `UPSERT/DEBEZIUM` \
                  sources. Described in the `mz_rocksdb_types::config` module. \
                  Only takes effect on source restart (Materialize).",
        internal: true,
    };
    pub const UPSERT_ROCKSDB_UNIVERSAL_COMPACTION_RATIO: ServerVar<i32> = ServerVar {
        name: UncasedStr::new("upsert_rocksdb_universal_compaction_ratio"),
        value: mz_rocksdb_types::defaults::DEFAULT_UNIVERSAL_COMPACTION_RATIO,
        description: "Tuning parameter for RocksDB as used in `UPSERT/DEBEZIUM` \
                  sources. Described in the `mz_rocksdb_types::config` module. \
                  Only takes effect on source restart (Materialize).",
        internal: true,
    };
    pub const UPSERT_ROCKSDB_PARALLELISM: ServerVar<Option<i32>> = ServerVar {
        name: UncasedStr::new("upsert_rocksdb_parallelism"),
        value: mz_rocksdb_types::defaults::DEFAULT_PARALLELISM,
        description: "Tuning parameter for RocksDB as used in `UPSERT/DEBEZIUM` \
                  sources. Described in the `mz_rocksdb_types::config` module. \
                  Only takes effect on source restart (Materialize).",
        internal: true,
    };
    pub static UPSERT_ROCKSDB_COMPRESSION_TYPE: ServerVar<CompressionType> = ServerVar {
        name: UncasedStr::new("upsert_rocksdb_compression_type"),
        value: mz_rocksdb_types::defaults::DEFAULT_COMPRESSION_TYPE,
        description: "Tuning parameter for RocksDB as used in `UPSERT/DEBEZIUM` \
                  sources. Described in the `mz_rocksdb_types::config` module. \
                  Only takes effect on source restart (Materialize).",
        internal: true,
    };
    pub static UPSERT_ROCKSDB_BOTTOMMOST_COMPRESSION_TYPE: ServerVar<CompressionType> = ServerVar {
        name: UncasedStr::new("upsert_rocksdb_bottommost_compression_type"),
        value: mz_rocksdb_types::defaults::DEFAULT_BOTTOMMOST_COMPRESSION_TYPE,
        description: "Tuning parameter for RocksDB as used in `UPSERT/DEBEZIUM` \
                  sources. Described in the `mz_rocksdb_types::config` module. \
                  Only takes effect on source restart (Materialize).",
        internal: true,
    };

    pub static UPSERT_ROCKSDB_BATCH_SIZE: ServerVar<usize> = ServerVar {
        name: UncasedStr::new("upsert_rocksdb_batch_size"),
        value: mz_rocksdb_types::defaults::DEFAULT_BATCH_SIZE,
        description: "Tuning parameter for RocksDB as used in `UPSERT/DEBEZIUM` \
                  sources. Described in the `mz_rocksdb_types::config` module. \
                  Can be changed dynamically (Materialize).",
        internal: true,
    };

    pub static UPSERT_ROCKSDB_RETRY_DURATION: ServerVar<Duration> = ServerVar {
        name: UncasedStr::new("upsert_rocksdb_retry_duration"),
        value: mz_rocksdb_types::defaults::DEFAULT_RETRY_DURATION,
        description: "Tuning parameter for RocksDB as used in `UPSERT/DEBEZIUM` \
                  sources. Described in the `mz_rocksdb_types::config` module. \
                  Only takes effect on source restart (Materialize).",
        internal: true,
    };

    /// Controls whether automatic spill to disk should be turned on when using `DISK`.
    pub const UPSERT_ROCKSDB_AUTO_SPILL_TO_DISK: ServerVar<bool> = ServerVar {
        name: UncasedStr::new("upsert_rocksdb_auto_spill_to_disk"),
        value: false,
        description:
            "Controls whether automatic spill to disk should be turned on when using `DISK`",
        internal: true,
    };

    /// The upsert in memory state size threshold after which it will spill to disk.
    /// The default is 85 MiB = 89128960 bytes
    pub const UPSERT_ROCKSDB_AUTO_SPILL_THRESHOLD_BYTES: ServerVar<usize> = ServerVar {
        name: UncasedStr::new("upsert_rocksdb_auto_spill_threshold_bytes"),
        value: mz_rocksdb_types::defaults::DEFAULT_AUTO_SPILL_MEMORY_THRESHOLD,
        description:
            "The upsert in-memory state size threshold in bytes after which it will spill to disk",
        internal: true,
    };

    pub static UPSERT_ROCKSDB_STATS_LOG_INTERVAL_SECONDS: ServerVar<u32> = ServerVar {
        name: UncasedStr::new("upsert_rocksdb_stats_log_interval_seconds"),
        value: mz_rocksdb_types::defaults::DEFAULT_STATS_LOG_INTERVAL_S,
        description: "Tuning parameter for RocksDB as used in `UPSERT/DEBEZIUM` \
                  sources. Described in the `mz_rocksdb_types::config` module. \
                  Only takes effect on source restart (Materialize).",
        internal: true,
    };
    pub static UPSERT_ROCKSDB_STATS_PERSIST_INTERVAL_SECONDS: ServerVar<u32> = ServerVar {
        name: UncasedStr::new("upsert_rocksdb_stats_persist_interval_seconds"),
        value: mz_rocksdb_types::defaults::DEFAULT_STATS_PERSIST_INTERVAL_S,
        description: "Tuning parameter for RocksDB as used in `UPSERT/DEBEZIUM` \
                  sources. Described in the `mz_rocksdb_types::config` module. \
                  Only takes effect on source restart (Materialize).",
        internal: true,
    };
    pub static UPSERT_ROCKSDB_POINT_LOOKUP_BLOCK_CACHE_SIZE_MB: ServerVar<Option<u32>> =
        ServerVar {
            name: UncasedStr::new("upsert_rocksdb_point_lookup_block_cache_size_mb"),
            value: None,
            description: "Tuning parameter for RocksDB as used in `UPSERT/DEBEZIUM` \
                  sources. Described in the `mz_rocksdb_types::config` module. \
                  Only takes effect on source restart (Materialize).",
            internal: true,
        };

    /// The number of times by which allocated buffers will be shrinked in upsert rocksdb.
    /// If value is 0, then no shrinking will occur.
    pub static UPSERT_ROCKSDB_SHRINK_ALLOCATED_BUFFERS_BY_RATIO: ServerVar<usize> = ServerVar {
        name: UncasedStr::new("upsert_rocksdb_shrink_allocated_buffers_by_ratio"),
        value: mz_rocksdb_types::defaults::DEFAULT_SHRINK_BUFFERS_BY_RATIO,
        description:
            "The number of times by which allocated buffers will be shrinked in upsert rocksdb.",
        internal: true,
    };
    /// Only used if `upsert_rocksdb_write_buffer_manager_memory_bytes` is also set
    /// and write buffer manager is enabled
    pub static UPSERT_ROCKSDB_WRITE_BUFFER_MANAGER_CLUSTER_MEMORY_FRACTION: Lazy<
        ServerVar<Option<Numeric>>,
    > = Lazy::new(|| ServerVar {
        name: UncasedStr::new("upsert_rocksdb_write_buffer_manager_cluster_memory_fraction"),
        value: None,
        description: "Tuning parameter for RocksDB as used in `UPSERT/DEBEZIUM` \
                  sources. Described in the `mz_rocksdb_types::config` module. \
                  Only takes effect on source restart (Materialize).",
        internal: true,
    });
    /// `upsert_rocksdb_write_buffer_manager_memory_bytes` needs to be set for write buffer manager to be
    /// used.
    pub static UPSERT_ROCKSDB_WRITE_BUFFER_MANAGER_MEMORY_BYTES: ServerVar<Option<usize>> =
        ServerVar {
            name: UncasedStr::new("upsert_rocksdb_write_buffer_manager_memory_bytes"),
            value: None,
            description: "Tuning parameter for RocksDB as used in `UPSERT/DEBEZIUM` \
                  sources. Described in the `mz_rocksdb_types::config` module. \
                  Only takes effect on source restart (Materialize).",
            internal: true,
        };
    pub static UPSERT_ROCKSDB_WRITE_BUFFER_MANAGER_ALLOW_STALL: ServerVar<bool> = ServerVar {
        name: UncasedStr::new("upsert_rocksdb_write_buffer_manager_allow_stall"),
        value: false,
        description: "Tuning parameter for RocksDB as used in `UPSERT/DEBEZIUM` \
                  sources. Described in the `mz_rocksdb_types::config` module. \
                  Only takes effect on source restart (Materialize).",
        internal: true,
    };
}

pub static LOGGING_FILTER: Lazy<ServerVar<CloneableEnvFilter>> = Lazy::new(|| ServerVar {
    name: UncasedStr::new("log_filter"),
    value: CloneableEnvFilter::from_str("info").expect("valid EnvFilter"),
    description: "Sets the filter to apply to stderr logging.",
    internal: true,
});

pub static OPENTELEMETRY_FILTER: Lazy<ServerVar<CloneableEnvFilter>> = Lazy::new(|| ServerVar {
    name: UncasedStr::new("opentelemetry_filter"),
    value: CloneableEnvFilter::from_str("off").expect("valid EnvFilter"),
    description: "Sets the filter to apply to OpenTelemetry-backed distributed tracing.",
    internal: true,
});

pub static LOGGING_FILTER_DEFAULTS: Lazy<ServerVar<Vec<SerializableDirective>>> =
    Lazy::new(|| ServerVar {
        name: UncasedStr::new("log_filter_defaults"),
        value: mz_ore::tracing::LOGGING_DEFAULTS
            .iter()
            .map(|d| d.clone().into())
            .collect(),
        description: "Sets additional default directives to apply to stderr logging. \
            These apply to all variations of `log_filter`. Directives other than \
            `module=off` are likely incorrect.",
        internal: true,
    });

pub static OPENTELEMETRY_FILTER_DEFAULTS: Lazy<ServerVar<Vec<SerializableDirective>>> =
    Lazy::new(|| ServerVar {
        name: UncasedStr::new("opentelemetry_filter_defaults"),
        value: mz_ore::tracing::OPENTELEMETRY_DEFAULTS
            .iter()
            .map(|d| d.clone().into())
            .collect(),
        description: "Sets additional default directives to apply to OpenTelemetry-backed \
            distributed tracing. \
            These apply to all variations of `opentelemetry_filter`. Directives other than \
            `module=off` are likely incorrect.",
        internal: true,
    });

pub static SENTRY_FILTERS: Lazy<ServerVar<Vec<SerializableDirective>>> = Lazy::new(|| ServerVar {
    name: UncasedStr::new("sentry_filters"),
    value: mz_ore::tracing::SENTRY_DEFAULTS
        .iter()
        .map(|d| d.clone().into())
        .collect(),
    description: "Sets additional default directives to apply to sentry logging. \
            These apply on top of a default `info` directive. Directives other than \
            `module=off` are likely incorrect.",
    internal: true,
});

pub static WEBHOOKS_SECRETS_CACHING_TTL_SECS: Lazy<ServerVar<usize>> = Lazy::new(|| ServerVar {
    name: UncasedStr::new("webhooks_secrets_caching_ttl_secs"),
    value: usize::cast_from(
        mz_secrets::cache::DEFAULT_TTL_SECS.load(std::sync::atomic::Ordering::Relaxed),
    ),
    description: "Sets the time-to-live for values in the Webhooks secrets cache.",
    internal: true,
});

pub const COORD_SLOW_MESSAGE_WARN_THRESHOLD: ServerVar<Duration> = ServerVar {
    name: UncasedStr::new("coord_slow_message_warn_threshold"),
    // Note(parkmycar): This value was chosen arbitrarily.
    value: Duration::from_secs(5),
    description: "Sets the threshold at which we will warn! for a coordinator message being slow.",
    internal: true,
};

/// Controls the connect_timeout setting when connecting to PG via `mz_postgres_util`.
pub const PG_SOURCE_CONNECT_TIMEOUT: ServerVar<Duration> = ServerVar {
    name: UncasedStr::new("pg_source_connect_timeout"),
    value: mz_postgres_util::DEFAULT_CONNECT_TIMEOUT,
    description: "Sets the timeout applied to socket-level connection attempts for PG \
    replication connections. (Materialize)",
    internal: true,
};

/// Sets the maximum number of TCP keepalive probes that will be sent before dropping a connection
/// when connecting to PG via `mz_postgres_util`.
pub const PG_SOURCE_KEEPALIVES_RETRIES: ServerVar<u32> = ServerVar {
    name: UncasedStr::new("pg_source_keepalives_retries"),
    value: mz_postgres_util::DEFAULT_KEEPALIVE_RETRIES,
    description:
        "Sets the maximum number of TCP keepalive probes that will be sent before dropping \
    a connection when connecting to PG via `mz_postgres_util`. (Materialize)",
    internal: true,
};

/// Sets the amount of idle time before a keepalive packet is sent on the connection when connecting
/// to PG via `mz_postgres_util`.
pub const PG_SOURCE_KEEPALIVES_IDLE: ServerVar<Duration> = ServerVar {
    name: UncasedStr::new("pg_source_keepalives_idle"),
    value: mz_postgres_util::DEFAULT_KEEPALIVE_IDLE,
    description:
        "Sets the amount of idle time before a keepalive packet is sent on the connection \
    when connecting to PG via `mz_postgres_util`. (Materialize)",
    internal: true,
};

/// Sets the time interval between TCP keepalive probes when connecting to PG via `mz_postgres_util`.
pub const PG_SOURCE_KEEPALIVES_INTERVAL: ServerVar<Duration> = ServerVar {
    name: UncasedStr::new("pg_source_keepalives_interval"),
    value: mz_postgres_util::DEFAULT_KEEPALIVE_INTERVAL,
    description: "Sets the time interval between TCP keepalive probes when connecting to PG via \
    replication. (Materialize)",
    internal: true,
};

/// Sets the TCP user timeout when connecting to PG via `mz_postgres_util`.
pub const PG_SOURCE_TCP_USER_TIMEOUT: ServerVar<Duration> = ServerVar {
    name: UncasedStr::new("pg_source_tcp_user_timeout"),
    value: mz_postgres_util::DEFAULT_TCP_USER_TIMEOUT,
    description:
        "Sets the TCP user timeout when connecting to PG via `mz_postgres_util`. (Materialize)",
    internal: true,
};

/// Sets the `statement_timeout` value to use during the snapshotting phase of
/// PG sources.
pub const PG_SOURCE_SNAPSHOT_STATEMENT_TIMEOUT: ServerVar<Duration> = ServerVar {
    name: UncasedStr::new("pg_source_snapshot_statement_timeout"),
    value: mz_postgres_util::DEFAULT_SNAPSHOT_STATEMENT_TIMEOUT,
    description: "Sets the `statement_timeout` value to use during the snapshotting phase of PG sources (Materialize)",
    internal: true,
};

/// Please see `PgSourceSnapshotConfig`.
pub const PG_SOURCE_SNAPSHOT_COLLECT_STRICT_COUNT: ServerVar<bool> = ServerVar {
    name: UncasedStr::new("pg_source_snapshot_collect_strict_count"),
    value: mz_storage_types::parameters::PgSourceSnapshotConfig::new().collect_strict_count,
    description: "Please see <https://dev.materialize.com/api/rust-private\
        /mz_storage_types/parameters\
        /struct.PgSourceSnapshotConfig.html#structfield.collect_strict_count>",
    internal: true,
};
/// Please see `PgSourceSnapshotConfig`.
pub const PG_SOURCE_SNAPSHOT_FALLBACK_TO_STRICT_COUNT: ServerVar<bool> = ServerVar {
    name: UncasedStr::new("pg_source_snapshot_fallback_to_strict_count"),
    value: mz_storage_types::parameters::PgSourceSnapshotConfig::new().fallback_to_strict_count,
    description: "Please see <https://dev.materialize.com/api/rust-private\
        /mz_storage_types/parameters\
        /struct.PgSourceSnapshotConfig.html#structfield.fallback_to_strict_count>",
    internal: true,
};
/// Please see `PgSourceSnapshotConfig`.
pub const PG_SOURCE_SNAPSHOT_WAIT_FOR_COUNT: ServerVar<bool> = ServerVar {
    name: UncasedStr::new("pg_source_snapshot_wait_for_count"),
    value: mz_storage_types::parameters::PgSourceSnapshotConfig::new().wait_for_count,
    description: "Please see <https://dev.materialize.com/api/rust-private\
        /mz_storage_types/parameters\
        /struct.PgSourceSnapshotConfig.html#structfield.wait_for_count>",
    internal: true,
};

/// Controls the check interval for connections to SSH bastions via `mz_ssh_util`.
pub const SSH_CHECK_INTERVAL: ServerVar<Duration> = ServerVar {
    name: UncasedStr::new("ssh_check_interval"),
    value: mz_ssh_util::tunnel::DEFAULT_CHECK_INTERVAL,
    description: "Controls the check interval for connections to SSH bastions via `mz_ssh_util`.",
    internal: true,
};

/// Controls the connect timeout for connections to SSH bastions via `mz_ssh_util`.
pub const SSH_CONNECT_TIMEOUT: ServerVar<Duration> = ServerVar {
    name: UncasedStr::new("ssh_connect_timeout"),
    value: mz_ssh_util::tunnel::DEFAULT_CONNECT_TIMEOUT,
    description: "Controls the connect timeout for connections to SSH bastions via `mz_ssh_util`.",
    internal: true,
};

/// Controls the keepalive idle interval for connections to SSH bastions via `mz_ssh_util`.
pub const SSH_KEEPALIVES_IDLE: ServerVar<Duration> = ServerVar {
    name: UncasedStr::new("ssh_keepalives_idle"),
    value: mz_ssh_util::tunnel::DEFAULT_KEEPALIVES_IDLE,
    description:
        "Controls the keepalive idle interval for connections to SSH bastions via `mz_ssh_util`.",
    internal: true,
};

/// Enables `socket.keepalive.enable` for rdkafka client connections. Defaults to true.
pub const KAFKA_SOCKET_KEEPALIVE: ServerVar<bool> = ServerVar {
    name: UncasedStr::new("kafka_socket_keepalive"),
    value: mz_kafka_util::client::DEFAULT_KEEPALIVE,
    description:
        "Enables `socket.keepalive.enable` for rdkafka client connections. Defaults to true.",
    internal: true,
};

/// Controls `socket.timeout.ms` for rdkafka client connections. Defaults to the rdkafka default
/// (60000ms). Cannot be greater than 300000ms, more than 100ms greater than
/// `kafka_transaction_timeout`, or less than 10ms.
pub const KAFKA_SOCKET_TIMEOUT: ServerVar<Duration> = ServerVar {
    name: UncasedStr::new("kafka_socket_timeout"),
    value: mz_kafka_util::client::DEFAULT_SOCKET_TIMEOUT,
    description: "Controls `socket.timeout.ms` for rdkafka \
        client connections. Defaults to the rdkafka default (60000ms). \
        Cannot be greater than 300000ms or more than 100ms greater than \
        Cannot be greater than 300000ms, more than 100ms greater than \
        `kafka_transaction_timeout`, or less than 10ms.",
    internal: true,
};

/// Controls `transaction.timeout.ms` for rdkafka client connections. Defaults to the rdkafka default
/// (60000ms). Cannot be greater than `i32::MAX` or less than 1000ms.
pub const KAFKA_TRANSACTION_TIMEOUT: ServerVar<Duration> = ServerVar {
    name: UncasedStr::new("kafka_transaction_timeout"),
    value: mz_kafka_util::client::DEFAULT_TRANSACTION_TIMEOUT,
    description: "Controls `transaction.timeout.ms` for rdkafka \
        client connections. Defaults to the rdkafka default (60000ms). \
        Cannot be greater than `i32::MAX` or less than 1000ms.",
    internal: true,
};

/// Controls `socket.connection.setup.timeout.ms` for rdkafka client connections. Defaults to the rdkafka default
/// (30000ms). Cannot be greater than `i32::MAX` or less than 1000ms
pub const KAFKA_SOCKET_CONNECTION_SETUP_TIMEOUT: ServerVar<Duration> = ServerVar {
    name: UncasedStr::new("kafka_socket_connection_setup_timeout"),
    value: mz_kafka_util::client::DEFAULT_SOCKET_CONNECTION_SETUP_TIMEOUT,
    description: "Controls `socket.connection.setup.timeout.ms` for rdkafka \
        client connections. Defaults to the rdkafka default (30000ms). \
        Cannot be greater than `i32::MAX` or less than 1000ms",
    internal: true,
};

/// Controls the timeout when fetching kafka metadata. Defaults to 10s.
pub const KAFKA_FETCH_METADATA_TIMEOUT: ServerVar<Duration> = ServerVar {
    name: UncasedStr::new("kafka_fetch_metadata_timeout"),
    value: mz_kafka_util::client::DEFAULT_FETCH_METADATA_TIMEOUT,
    description: "Controls the timeout when fetching kafka metadata. \
        Defaults to 10s.",
    internal: true,
};

/// Controls the timeout when fetching kafka progress records. Defaults to 60s.
pub const KAFKA_PROGRESS_RECORD_FETCH_TIMEOUT: ServerVar<Duration> = ServerVar {
    name: UncasedStr::new("kafka_progress_record_fetch_timeout"),
    value: mz_kafka_util::client::DEFAULT_PROGRESS_RECORD_FETCH_TIMEOUT,
    description: "Controls the timeout when fetching kafka progress records. \
        Defaults to 60s.",
    internal: true,
};

/// The interval we will fetch metadata from, unless overridden by the source.
pub const KAFKA_DEFAULT_METADATA_FETCH_INTERVAL: ServerVar<Duration> = ServerVar {
    name: UncasedStr::new("kafka_default_metadata_fetch_interval"),
    value: mz_kafka_util::client::DEFAULT_METADATA_FETCH_INTERVAL,
    description: "The interval we will fetch metadata from, unless overridden by the source. \
        Defaults to 60s.",
    internal: true,
};

/// The maximum number of in-flight bytes emitted by persist_sources feeding compute dataflows.
pub const COMPUTE_DATAFLOW_MAX_INFLIGHT_BYTES: ServerVar<Option<usize>> = ServerVar {
    name: UncasedStr::new("compute_dataflow_max_inflight_bytes"),
    value: None,
    description: "The maximum number of in-flight bytes emitted by persist_sources feeding \
                  compute dataflows (Materialize).",
    internal: true,
};

/// The maximum number of in-flight bytes emitted by persist_sources feeding _storage
/// dataflows_.
/// Currently defaults to 256MiB = 268435456 bytes
/// Note: Backpressure will only be turned on if disk is enabled based on
/// `storage_dataflow_max_inflight_bytes_disk_only` flag
pub const STORAGE_DATAFLOW_MAX_INFLIGHT_BYTES: ServerVar<Option<usize>> = ServerVar {
    name: UncasedStr::new("storage_dataflow_max_inflight_bytes"),
    value: Some(256 * 1024 * 1024),
    description: "The maximum number of in-flight bytes emitted by persist_sources feeding \
                  storage dataflows. Defaults to backpressure enabled (Materialize).",
    internal: true,
};

/// Whether or not to delay sources producing values in some scenarios
/// (namely, upsert) till after rehydration is finished.
pub const STORAGE_DATAFLOW_DELAY_SOURCES_PAST_REHYDRATION: ServerVar<bool> = ServerVar {
    name: UncasedStr::new("storage_dataflow_delay_sources_past_rehydration"),
    value: false,
    description: "Whether or not to delay sources producing values in some scenarios \
                  (namely, upsert) till after rehydration is finished",
    internal: true,
};

/// Configuration ratio to shrink unusef buffers in upsert by.
/// For eg: is 2 is set, then the buffers will be reduced by 2 i.e. halved.
/// Default is 0, which means shrinking is disabled.
pub const STORAGE_SHRINK_UPSERT_UNUSED_BUFFERS_BY_RATIO: ServerVar<usize> = ServerVar {
    name: UncasedStr::new("storage_shrink_upsert_unused_buffers_by_ratio"),
    value: 0,
    description: "Configuration ratio to shrink unusef buffers in upsert by",
    internal: true,
};

/// The fraction of the cluster replica size to be used as the maximum number of
/// in-flight bytes emitted by persist_sources feeding storage dataflows.
/// If not configured, the storage_dataflow_max_inflight_bytes value will be used.
/// For this value to be used storage_dataflow_max_inflight_bytes needs to be set.
pub static STORAGE_DATAFLOW_MAX_INFLIGHT_BYTES_TO_CLUSTER_SIZE_FRACTION: Lazy<
    ServerVar<Option<Numeric>>,
> = Lazy::new(|| ServerVar {
    name: UncasedStr::new("storage_dataflow_max_inflight_bytes_to_cluster_size_fraction"),
    value: Some(0.0025.into()),
    description: "The fraction of the cluster replica size to be used as the maximum number of \
    in-flight bytes emitted by persist_sources feeding storage dataflows. \
    If not configured, the storage_dataflow_max_inflight_bytes value will be used.",
    internal: true,
});

pub const STORAGE_DATAFLOW_MAX_INFLIGHT_BYTES_DISK_ONLY: ServerVar<bool> = ServerVar {
    name: UncasedStr::new("storage_dataflow_max_inflight_bytes_disk_only"),
    value: true,
    description: "Whether or not `storage_dataflow_max_inflight_bytes` applies only to \
        upsert dataflows using disks. Defaults to true (Materialize).",
    internal: true,
};

/// The interval to submit statistics to `mz_source_statistics_raw` and `mz_sink_statistics_raw`.
pub const STORAGE_STATISTICS_INTERVAL: ServerVar<Duration> = ServerVar {
    name: UncasedStr::new("storage_statistics_interval"),
    value: mz_storage_types::parameters::STATISTICS_INTERVAL_DEFAULT,
    description: "The interval to submit statistics to `mz_source_statistics_raw` \
        and `mz_sink_statistics` (Materialize).",
    internal: true,
};

/// The interval to collect statistics for `mz_source_statistics_raw` and `mz_sink_statistics_raw` in
/// clusterd. Controls the accuracy of metrics.
pub const STORAGE_STATISTICS_COLLECTION_INTERVAL: ServerVar<Duration> = ServerVar {
    name: UncasedStr::new("storage_statistics_collection_interval"),
    value: mz_storage_types::parameters::STATISTICS_COLLECTION_INTERVAL_DEFAULT,
    description: "The interval to collect statistics for `mz_source_statistics_raw` \
        and `mz_sink_statistics_raw` in clusterd. Controls the accuracy of metrics \
        (Materialize).",
    internal: true,
};

pub const STORAGE_RECORD_SOURCE_SINK_NAMESPACED_ERRORS: ServerVar<bool> = ServerVar {
    name: UncasedStr::new("storage_record_source_sink_namespaced_errors"),
    value: true,
    description: "Whether or not to record namespaced errors in the status history tables",
    internal: true,
};

/// Boolean flag indicating whether to enable syncing from
/// LaunchDarkly. Can be turned off as an emergency measure to still
/// be able to alter parameters while LD is broken.
pub static ENABLE_LAUNCHDARKLY: ServerVar<bool> = ServerVar {
    name: UncasedStr::new("enable_launchdarkly"),
    value: true,
    description: "Boolean flag indicating whether flag synchronization from LaunchDarkly should be enabled (Materialize).",
    internal: true
};

/// Feature flag indicating whether real time recency is enabled. Not that
/// unlike other feature flags, this is made available at the session level, so
/// is additionally gated by a feature flag.
pub static REAL_TIME_RECENCY: ServerVar<bool> = ServerVar {
    name: UncasedStr::new("real_time_recency"),
    value: false,
    description: "Feature flag indicating whether real time recency is enabled (Materialize).",
    internal: false,
};

pub static EMIT_TIMESTAMP_NOTICE: ServerVar<bool> = ServerVar {
    name: UncasedStr::new("emit_timestamp_notice"),
    value: false,
    description:
        "Boolean flag indicating whether to send a NOTICE with timestamp explanations of queries (Materialize).",
    internal: false
};

pub static EMIT_TRACE_ID_NOTICE: ServerVar<bool> = ServerVar {
    name: UncasedStr::new("emit_trace_id_notice"),
    value: false,
    description:
        "Boolean flag indicating whether to send a NOTICE specifying the trace id when available (Materialize).",
    internal: false
};

pub static UNSAFE_MOCK_AUDIT_EVENT_TIMESTAMP: ServerVar<Option<mz_repr::Timestamp>> = ServerVar {
    name: UncasedStr::new("unsafe_mock_audit_event_timestamp"),
    value: None,
    description: "Mocked timestamp to use for audit events for testing purposes",
    internal: true,
};

pub const ENABLE_RBAC_CHECKS: ServerVar<bool> = ServerVar {
    name: UncasedStr::new("enable_rbac_checks"),
    value: true,
    description: "User facing global boolean flag indicating whether to apply RBAC checks before \
    executing statements (Materialize).",
    internal: false,
};

pub const ENABLE_SESSION_RBAC_CHECKS: ServerVar<bool> = ServerVar {
    name: UncasedStr::new("enable_session_rbac_checks"),
    // TODO(jkosh44) Once RBAC is complete, change this to `true`.
    value: false,
    description: "User facing session boolean flag indicating whether to apply RBAC checks before \
    executing statements (Materialize).",
    internal: false,
};

pub const EMIT_INTROSPECTION_QUERY_NOTICE: ServerVar<bool> = ServerVar {
    name: UncasedStr::new("emit_introspection_query_notice"),
    value: true,
    description: "Whether to print a notice when querying per-replica introspection sources.",
    internal: false,
};

// TODO(mgree) change this to a SelectOption
pub const ENABLE_SESSION_CARDINALITY_ESTIMATES: ServerVar<bool> = ServerVar {
    name: UncasedStr::new("enable_session_cardinality_estimates"),
    value: false,
    description:
        "Feature flag indicating whether to use cardinality estimates when optimizing queries; \
    does not affect EXPLAIN WITH(cardinality) (Materialize).",
    internal: false,
};

pub const OPTIMIZER_STATS_TIMEOUT: ServerVar<Duration> = ServerVar {
    name: UncasedStr::new("optimizer_stats_timeout"),
    value: Duration::from_millis(250),
    description: "Sets the timeout applied to the optimizer's statistics collection from storage; \
    applied to non-oneshot, i.e., long-lasting queries, like CREATE MATERIALIZED VIEW (Materialize).",
    internal: true,
};

pub const OPTIMIZER_ONESHOT_STATS_TIMEOUT: ServerVar<Duration> = ServerVar {
    name: UncasedStr::new("optimizer_oneshot_stats_timeout"),
    value: Duration::from_millis(20),
    description: "Sets the timeout applied to the optimizer's statistics collection from storage; \
    applied to oneshot queries, like SELECT (Materialize).",
    internal: true,
};

pub const PRIVATELINK_STATUS_UPDATE_QUOTA_PER_MINUTE: ServerVar<u32> = ServerVar {
    name: UncasedStr::new("privatelink_status_update_quota_per_minute"),
    value: 20,
    description: "Sets the per-minute quota for privatelink vpc status updates to be written to \
    the storage-collection-backed system table. This value implies the total and burst quota per-minute.",
    internal: true,
};

pub static STATEMENT_LOGGING_SAMPLE_RATE: Lazy<ServerVar<Numeric>> = Lazy::new(|| {
    ServerVar {
    name: UncasedStr::new("statement_logging_sample_rate"),
    value: 0.1.into(),
    description: "User-facing session variable indicating how many statement executions should be \
    logged, subject to constraint by the system variable `statement_logging_max_sample_rate` (Materialize).",
    internal: false,
}
});

/// Whether compute rendering should use Materialize's custom linear join implementation rather
/// than the one from Differential Dataflow.
pub const ENABLE_MZ_JOIN_CORE: ServerVar<bool> = ServerVar {
    name: UncasedStr::new("enable_mz_join_core"),
    value: true,
    description:
        "Feature flag indicating whether compute rendering should use Materialize's custom linear \
         join implementation rather than the one from Differential Dataflow. (Materialize).",
    internal: true,
};

pub static DEFAULT_LINEAR_JOIN_YIELDING: Lazy<String> =
    Lazy::new(|| "work:1000000,time:100".into());
pub static LINEAR_JOIN_YIELDING: Lazy<ServerVar<String>> = Lazy::new(|| ServerVar {
    name: UncasedStr::new("linear_join_yielding"),
    value: DEFAULT_LINEAR_JOIN_YIELDING.clone(),
    description:
        "The yielding behavior compute rendering should apply for linear join operators. Either \
         'work:<amount>' or 'time:<milliseconds>' or 'work:<amount>,time:<milliseconds>'. Note \
         that omitting one of 'work' or 'time' will entirely disable join yielding by time or \
         work, respectively, rather than falling back to some default.",
    internal: true,
});

pub const DEFAULT_IDLE_ARRANGEMENT_MERGE_EFFORT: ServerVar<u32> = ServerVar {
    name: UncasedStr::new("default_idle_arrangement_merge_effort"),
    value: 0,
    description:
        "The default value to use for the `IDLE ARRANGEMENT MERGE EFFORT` cluster/replica option.",
    internal: true,
};

pub const DEFAULT_ARRANGEMENT_EXERT_PROPORTIONALITY: ServerVar<u32> = ServerVar {
    name: UncasedStr::new("default_arrangement_exert_proportionality"),
    value: 16,
    description:
        "The default value to use for the `ARRANGEMENT EXERT PROPORTIONALITY` cluster/replica option.",
    internal: true,
};

pub const ENABLE_DEFAULT_CONNECTION_VALIDATION: ServerVar<bool> = ServerVar {
    name: UncasedStr::new("enable_default_connection_validation"),
    value: true,
    description:
        "LD facing global boolean flag that allows turning default connection validation off for everyone (Materialize).",
    internal: true,
};

pub static STATEMENT_LOGGING_MAX_DATA_CREDIT: Lazy<ServerVar<Option<usize>>> = Lazy::new(|| {
    ServerVar {
    name: UncasedStr::new("statement_logging_max_data_credit"),
    value: None,
    // The idea is that during periods of low logging, tokens can accumulate up to this value,
    // and then be depleted during periods of high logging.
    description: "The maximum number of bytes that can be logged for statement logging in short burts, or NULL if unlimited (Materialize).",
    internal: true,
}
});

pub static STATEMENT_LOGGING_TARGET_DATA_RATE: Lazy<ServerVar<Option<usize>>> = Lazy::new(|| {
    ServerVar {
    name: UncasedStr::new("statement_logging_target_data_rate"),
    value: None,
    description: "The maximum sustained data rate of statement logging, in bytes per second, or NULL if unlimited (Materialize).",
    internal: true,
}
});

pub static STATEMENT_LOGGING_MAX_SAMPLE_RATE: Lazy<ServerVar<Numeric>> = Lazy::new(|| ServerVar {
    name: UncasedStr::new("statement_logging_max_sample_rate"),
    value: 0.0.into(),
    description: "The maximum rate at which statements may be logged. If this value is less than \
that of `statement_logging_sample_rate`, the latter is ignored (Materialize).",
    internal: false,
});

pub static STATEMENT_LOGGING_DEFAULT_SAMPLE_RATE: Lazy<ServerVar<Numeric>> =
    Lazy::new(|| ServerVar {
        name: UncasedStr::new("statement_logging_default_sample_rate"),
        value: 0.0.into(),
        description:
            "The default value of `statement_logging_sample_rate` for new sessions (Materialize).",
        internal: false,
    });

pub const AUTO_ROUTE_INTROSPECTION_QUERIES: ServerVar<bool> = ServerVar {
    name: UncasedStr::new("auto_route_introspection_queries"),
    value: true,
    description:
        "Whether to force queries that depend only on system tables, to run on the mz_introspection cluster (Materialize).",
    internal: false
};

pub const MAX_CONNECTIONS: ServerVar<u32> = ServerVar {
    name: UncasedStr::new("max_connections"),
    value: 1000,
    description: "The maximum number of concurrent connections (Materialize).",
    internal: false,
};

/// Controls [`mz_storage_types::parameters::StorageParameters::keep_n_source_status_history_entries`].
pub const KEEP_N_SOURCE_STATUS_HISTORY_ENTRIES: ServerVar<usize> = ServerVar {
    name: UncasedStr::new("keep_n_source_status_history_entries"),
    value: 5,
    description: "On reboot, truncate all but the last n entries per ID in the source_status_history collection (Materialize).",
    internal: true
};

/// Controls [`mz_storage_types::parameters::StorageParameters::keep_n_sink_status_history_entries`].
pub const KEEP_N_SINK_STATUS_HISTORY_ENTRIES: ServerVar<usize> = ServerVar {
    name: UncasedStr::new("keep_n_sink_status_history_entries"),
    value: 5,
    description: "On reboot, truncate all but the last n entries per ID in the sink_status_history collection (Materialize).",
    internal: true
};

/// Controls [`mz_storage_types::parameters::StorageParameters::keep_n_privatelink_status_history_entries`].
pub const KEEP_N_PRIVATELINK_STATUS_HISTORY_ENTRIES: ServerVar<usize> = ServerVar {
    name: UncasedStr::new("keep_n_privatelink_status_history_entries"),
    value: 5,
    description: "On reboot, truncate all but the last n entries per ID in the mz_aws_privatelink_connection_status_history \
    collection (Materialize).",
    internal: true
};

pub const ENABLE_STORAGE_SHARD_FINALIZATION: ServerVar<bool> = ServerVar {
    name: UncasedStr::new("enable_storage_shard_finalization"),
    value: true,
    description: "Whether to allow the storage client to finalize shards (Materialize).",
    internal: true,
};

pub const ENABLE_CONSOLIDATE_AFTER_UNION_NEGATE: ServerVar<bool> = ServerVar {
    name: UncasedStr::new("enable_consolidate_after_union_negate"),
    value: true,
    description: "consolidation after Unions that have a Negated input (Materialize).",
    internal: false,
};

pub const MIN_TIMESTAMP_INTERVAL: ServerVar<Duration> = ServerVar {
    name: UncasedStr::new("min_timestamp_interval"),
    value: Duration::from_millis(1000),
    description: "Minimum timestamp interval",
    internal: true,
};

pub const MAX_TIMESTAMP_INTERVAL: ServerVar<Duration> = ServerVar {
    name: UncasedStr::new("max_timestamp_interval"),
    value: Duration::from_millis(1000),
    description: "Maximum timestamp interval",
    internal: true,
};

pub const WEBHOOK_CONCURRENT_REQUEST_LIMIT: ServerVar<usize> = ServerVar {
    name: UncasedStr::new("webhook_concurrent_request_limit"),
    value: WEBHOOK_CONCURRENCY_LIMIT,
    description: "Maximum number of concurrent requests for appending to a webhook source.",
    internal: true,
};

pub const ENABLE_COLUMNATION_LGALLOC: ServerVar<bool> = ServerVar {
    name: UncasedStr::new("enable_columnation_lgalloc"),
    value: false,
    description: "Enable allocating regions from lgalloc",
    internal: true,
};

pub const ENABLE_STATEMENT_LIFECYCLE_LOGGING: ServerVar<bool> = ServerVar {
    name: UncasedStr::new("enable_statement_lifecycle_logging"),
    value: false,
    description:
        "Enable logging of statement lifecycle events in mz_internal.mz_statement_lifecycle_history",
    internal: true,
};

pub const ENABLE_DEPENDENCY_READ_HOLD_ASSERTS: ServerVar<bool> = ServerVar {
    name: UncasedStr::new("enable_dependency_read_hold_asserts"),
    value: true,
    description:
        "Whether to have the storage client check if a subsource's implied capability is less than \
        its write frontier. This should only be set to false in cases where customer envs cannot
        boot (Materialize).",
    internal: true,
};

/// Configuration for gRPC client connections.
pub mod grpc_client {
    use super::*;

    pub const CONNECT_TIMEOUT: ServerVar<Duration> = ServerVar {
        name: UncasedStr::new("grpc_client_connect_timeout"),
        value: Duration::from_secs(5),
        description: "Timeout to apply to initial gRPC client connection establishment.",
        internal: true,
    };
    pub const HTTP2_KEEP_ALIVE_INTERVAL: ServerVar<Duration> = ServerVar {
        name: UncasedStr::new("grpc_client_http2_keep_alive_interval"),
        value: Duration::from_secs(3),
        description: "Idle time to wait before sending HTTP/2 PINGs to maintain established gRPC client connections.",
        internal: true,
    };
    pub const HTTP2_KEEP_ALIVE_TIMEOUT: ServerVar<Duration> = ServerVar {
        name: UncasedStr::new("grpc_client_http2_keep_alive_timeout"),
        value: Duration::from_secs(5),
        description:
            "Time to wait for HTTP/2 pong response before terminating a gRPC client connection.",
        internal: true,
    };
}

/// Configuration for how cluster replicas are scheduled.
pub mod cluster_scheduling {
    use super::*;
    use mz_orchestrator::scheduling_config::*;

    pub const CLUSTER_MULTI_PROCESS_REPLICA_AZ_AFFINITY_WEIGHT: ServerVar<Option<i32>> =
        ServerVar {
            name: UncasedStr::new("cluster_multi_process_replica_az_affinity_weight"),
            value: DEFAULT_POD_AZ_AFFINITY_WEIGHT,
            description:
                "Whether or not to add an availability zone affinity between instances of \
            multi-process replicas. Either an affinity weight or empty (off) (Materialize).",
            internal: true,
        };

    pub const CLUSTER_SOFTEN_REPLICATION_ANTI_AFFINITY: ServerVar<bool> = ServerVar {
        name: UncasedStr::new("cluster_soften_replication_anti_affinity"),
        value: DEFAULT_SOFTEN_REPLICATION_ANTI_AFFINITY,
        description: "Whether or not to turn the node-scope anti affinity between replicas \
            in the same cluster into a preference (Materialize).",
        internal: true,
    };

    pub const CLUSTER_SOFTEN_REPLICATION_ANTI_AFFINITY_WEIGHT: ServerVar<i32> = ServerVar {
        name: UncasedStr::new("cluster_soften_replication_anti_affinity_weight"),
        value: DEFAULT_SOFTEN_REPLICATION_ANTI_AFFINITY_WEIGHT,
        description:
            "The preference weight for `cluster_soften_replication_anti_affinity` (Materialize).",
        internal: true,
    };

    pub const CLUSTER_ENABLE_TOPOLOGY_SPREAD: ServerVar<bool> = ServerVar {
        name: UncasedStr::new("cluster_enable_topology_spread"),
        value: DEFAULT_TOPOLOGY_SPREAD_ENABLED,
        description:
            "Whether or not to add topology spread constraints among replicas in the same cluster (Materialize).",
        internal: true,
    };

    pub const CLUSTER_TOPOLOGY_SPREAD_IGNORE_NON_SINGULAR_SCALE: ServerVar<bool> = ServerVar {
        name: UncasedStr::new("cluster_topology_spread_ignore_non_singular_scale"),
        value: DEFAULT_TOPOLOGY_SPREAD_IGNORE_NON_SINGULAR_SCALE,
        description:
            "If true, ignore replicas with more than 1 process when adding topology spread constraints (Materialize).",
        internal: true,
    };

    pub const CLUSTER_TOPOLOGY_SPREAD_MAX_SKEW: ServerVar<i32> = ServerVar {
        name: UncasedStr::new("cluster_topology_spread_max_skew"),
        value: DEFAULT_TOPOLOGY_SPREAD_MAX_SKEW,
        description: "The `maxSkew` for replica topology spread constraints (Materialize).",
        internal: true,
    };

    pub const CLUSTER_TOPOLOGY_SPREAD_SOFT: ServerVar<bool> = ServerVar {
        name: UncasedStr::new("cluster_topology_spread_soft"),
        value: DEFAULT_TOPOLOGY_SPREAD_SOFT,
        description: "If true, soften the topology spread constraints for replicas (Materialize).",
        internal: true,
    };

    pub const CLUSTER_SOFTEN_AZ_AFFINITY: ServerVar<bool> = ServerVar {
        name: UncasedStr::new("cluster_soften_az_affinity"),
        value: DEFAULT_SOFTEN_AZ_AFFINITY,
        description: "Whether or not to turn the az-scope node affinity for replicas. \
            Note this could violate requests from the user (Materialize).",
        internal: true,
    };

    pub const CLUSTER_SOFTEN_AZ_AFFINITY_WEIGHT: ServerVar<i32> = ServerVar {
        name: UncasedStr::new("cluster_soften_az_affinity_weight"),
        value: DEFAULT_SOFTEN_AZ_AFFINITY_WEIGHT,
        description: "The preference weight for `cluster_soften_az_affinity` (Materialize).",
        internal: true,
    };

    pub const CLUSTER_ALWAYS_USE_DISK: ServerVar<bool> = ServerVar {
        name: UncasedStr::new("cluster_always_use_disk"),
        value: DEFAULT_ALWAYS_USE_DISK,
        description: "Always provisions a replica with disk, regardless of `DISK` DDL option.",
        internal: true,
    };
}

/// Macro to simplify creating feature flags, i.e. boolean flags that we use to toggle the
/// availability of features.
///
/// The arguments to `feature_flags!` are:
/// - `$name`, which will be the name of the feature flag, in snake_case,
/// - `$feature_desc`, a human-readable description of the feature,
/// - `$value`, which if not provided, defaults to `false` and also defaults `$internal` to `true`.
/// - `$internal`, which if not provided, defaults to `true`. Requires `$value`.
///
/// Note that not all `ServerVar<bool>` are feature flags. Feature flags are for variables that:
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
    // Match `$name, $feature_desc, $value, $internal`.
    (@inner
        // The feature flag name.
        name: $name:expr,
        // The feature flag description.
        desc: $desc:literal,
        // The feature flag default value.
        default: $value:expr,
        // Should this feature be visible only internally.
        internal: $internal:expr,
    ) => {
        paste::paste!{
            // Note that the ServerVar is not directly exported; we expect these to be
            // accessible through their FeatureFlag variant.
            pub static [<$name:upper _VAR>]: ServerVar<bool> = ServerVar {
                name: UncasedStr::new(stringify!($name)),
                value: $value,
                description: concat!("Whether ", $desc, " is allowed (Materialize)."),
                internal: $internal
            };

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
        // Should this feature be visible only internally.
        internal: $internal:expr,
        // Should the feature be turned on during catalog rehydration when
        // parsing a catalog item.
        enable_for_item_parsing: $enable_for_item_parsing:expr,
    },)+) => {
        $(feature_flags! { @inner
            name: $name,
            desc: $desc,
            default: $value,
            internal: $internal,
        })+

        paste::paste!{
            impl SystemVars {
                pub fn with_feature_flags(self) -> Self
                {
                    self
                    $(
                        .with_var(&[<$name:upper _VAR>])
                    )+
                }

                pub fn enable_all_feature_flags_by_default(&mut self) {
                    $(
                        self.set_default(stringify!($name), VarInput::Flat("on"))
                            .expect("setting default value must work");
                    )+
                }

                pub fn enable_for_item_parsing(&mut self) {
                    $(
                        if $enable_for_item_parsing {
                            self.set(stringify!($name), VarInput::Flat("on"))
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
        internal: true,
        enable_for_item_parsing: true,
    },
    // Actual feature flags
    {
        name: enable_binary_date_bin,
        desc: "the binary version of date_bin function",
        default: false,
        internal: true,
        enable_for_item_parsing: true,
    },
    {
        name: enable_create_sink_denylist_with_options,
        desc: "CREATE SINK with unsafe options",
        default: false,
        internal: true,
        enable_for_item_parsing: true,
    },
    {
        name: enable_create_source_denylist_with_options,
        desc: "CREATE SOURCE with unsafe options",
        default: false,
        internal: true,
        enable_for_item_parsing: true,
    },
    {
        name: enable_date_bin_hopping,
        desc: "the date_bin_hopping function",
        default: false,
        internal: true,
        enable_for_item_parsing: true,
    },
    {
        name: enable_envelope_debezium_in_subscribe,
        desc: "`ENVELOPE DEBEZIUM (KEY (..))`",
        default: false,
        internal: true,
        enable_for_item_parsing: true,
    },
    {
        name: enable_envelope_materialize,
        desc: "ENVELOPE MATERIALIZE",
        default: false,
        internal: true,
        enable_for_item_parsing: true,
    },
    {
        name: enable_explain_pushdown,
        desc: "EXPLAIN FILTER PUSHDOWN",
        default: false,
        internal: true,
        enable_for_item_parsing: true,
    },
    {
        name: enable_index_options,
        desc: "INDEX OPTIONS",
        default: false,
        internal: true,
        enable_for_item_parsing: true,
    },
    {
        name: enable_list_length_max,
        desc: "the list_length_max function",
        default: false,
        internal: true,
        enable_for_item_parsing: true,
    },
    {
        name: enable_list_n_layers,
        desc: "the list_n_layers function",
        default: false,
        internal: true,
        enable_for_item_parsing: true,
    },
    {
        name: enable_list_remove,
        desc: "the list_remove function",
        default: false,
        internal: true,
        enable_for_item_parsing: true,
    },
    {

        name: enable_logical_compaction_window,
        desc: "RETAIN HISTORY",
        default: false,
        internal: true,
        enable_for_item_parsing: true,
    },
    {
        name: enable_primary_key_not_enforced,
        desc: "PRIMARY KEY NOT ENFORCED",
        default: false,
        internal: true,
        enable_for_item_parsing: true,
    },
    {
        name: enable_mfp_pushdown_explain,
        desc: "`filter_pushdown` explain",
        default: true,
        internal: true,
        enable_for_item_parsing: true,
    },
    {
        name: enable_multi_worker_storage_persist_sink,
        desc: "multi-worker storage persist sink",
        default: true,
        internal: true,
        enable_for_item_parsing: true,
    },
    {
        name: enable_persist_streaming_snapshot_and_fetch,
        desc: "use the new streaming consolidate for snapshot_and_fetch",
        default: false,
        internal: true,
        enable_for_item_parsing: true,
    },
    {
        name: enable_persist_streaming_compaction,
        desc: "use the new streaming consolidate for compaction",
        default: false,
        internal: true,
        enable_for_item_parsing: true,
    },
    {
        name: enable_raise_statement,
        desc: "RAISE statement",
        default: false,
        internal: true,
        enable_for_item_parsing: true,
    },
    {
        name: enable_repeat_row,
        desc: "the repeat_row function",
        default: false,
        internal: true,
        enable_for_item_parsing: true,
    },
    {
        name: enable_table_check_constraint,
        desc: "CREATE TABLE with a check constraint",
        default: false,
        internal: true,
        enable_for_item_parsing: true,
    },
    {
        name: enable_table_foreign_key,
        desc: "CREATE TABLE with a foreign key",
        default: false,
        internal: true,
        enable_for_item_parsing: true,
    },
    {
        name: enable_table_keys,
        desc: "CREATE TABLE with a primary key or unique constraint",
        default: false,
        internal: true,
        enable_for_item_parsing: true,
    },
    {
        name: enable_unorchestrated_cluster_replicas,
        desc: "unorchestrated cluster replicas",
        default: false,
        internal: true,
        enable_for_item_parsing: true,
    },
    {
        name: enable_unstable_dependencies,
        desc: "depending on unstable objects",
        default: false,
        internal: true,
        enable_for_item_parsing: true,
    },
    {
        name: enable_disk_cluster_replicas,
        desc: "`WITH (DISK)` for cluster replicas",
        default: true,
        internal: true,
        enable_for_item_parsing: true,
    },
    {
        name: enable_within_timestamp_order_by_in_subscribe,
        desc: "`WITHIN TIMESTAMP ORDER BY ..`",
        default: false,
        internal: true,
        enable_for_item_parsing: true,
    },
    {
        name: enable_cardinality_estimates,
        desc: "join planning with cardinality estimates",
        default: false,
        internal: true,
        enable_for_item_parsing: true,
    },
    {
        name: enable_connection_validation_syntax,
        desc: "CREATE CONNECTION .. WITH (VALIDATE) and VALIDATE CONNECTION syntax",
        default: true,
        internal: true,
        enable_for_item_parsing: true,
    },
    {
        name: enable_try_parse_monotonic_iso8601_timestamp,
        desc: "the try_parse_monotonic_iso8601_timestamp function",
        default: true,
        internal: true,
        enable_for_item_parsing: true,
    },
    {
        name: enable_alter_set_cluster,
        desc: "ALTER ... SET CLUSTER syntax",
        default: false,
        internal: true,
        enable_for_item_parsing: true,
    },
    {
        name: enable_unsafe_functions,
        desc: "executing potentially dangerous functions",
        default: false,
        internal: true,
        enable_for_item_parsing: true,
    },
    {
        name: enable_managed_cluster_availability_zones,
        desc: "MANAGED, AVAILABILITY ZONES syntax",
        default: false,
        internal: true,
        enable_for_item_parsing: true,
    },
    {
        name: statement_logging_use_reproducible_rng,
        desc: "statement logging with reproducible RNG",
        default: false,
        internal: true,
        enable_for_item_parsing: true,
    },
    {
        name: enable_notices_for_index_already_exists,
        desc: "emitting notices for IndexAlreadyExists (doesn't affect EXPLAIN)",
        default: true,
        internal: true,
        enable_for_item_parsing: true,
    },
    {
        name: enable_notices_for_index_too_wide_for_literal_constraints,
        desc: "emitting notices for IndexTooWideForLiteralConstraints (doesn't affect EXPLAIN)",
        default: false,
        internal: true,
        enable_for_item_parsing: true,
    },
    {
        name: enable_notices_for_index_empty_key,
        desc: "emitting notices for indexes with an empty key (doesn't affect EXPLAIN)",
        default: true,
        internal: true,
        enable_for_item_parsing: true,
    },
    {
        name: enable_explain_broken,
        desc: "EXPLAIN ... BROKEN <query> syntax",
        default: false,
        internal: true,
        enable_for_item_parsing: true,
    },
    {
        name: enable_jemalloc_profiling,
        desc: "jemalloc heap memory profiling",
        default: true,
        internal: true,
        enable_for_item_parsing: true,
    },
    {
        name: enable_comment,
        desc: "the COMMENT ON feature for objects",
        default: true,
        internal: false,
        enable_for_item_parsing: true,
    },
    {
        name: enable_sink_doc_on_option,
        desc: "DOC ON option for sinks",
        default: false,
        internal: false,
        enable_for_item_parsing: true,
    },
    {
        name: enable_assert_not_null,
        desc: "ASSERT NOT NULL for materialized views",
        default: false,
        internal: true,
        enable_for_item_parsing: true,
    },
    {
        name: enable_alter_swap,
        desc: "the ALTER SWAP feature for objects",
        default: true,
        internal: false,
        enable_for_item_parsing: true,
    },
    {
        name: enable_new_outer_join_lowering,
        desc: "new outer join lowering",
        default: true,
        internal: true,
        enable_for_item_parsing: false,
    },
    {
        name: enable_default_kafka_ssh_tunnel,
        desc: "the top-level SSH TUNNEL feature for kafka connections",
        default: true,
        internal: true,
        enable_for_item_parsing: true,
    },
    {
        name: enable_default_kafka_aws_private_link,
        desc: "the top-level Aws Privatelink feature for kafka connections",
        default: false,
        internal: true,
        enable_for_item_parsing: true,
    },
    {
        name: enable_time_at_time_zone,
        desc: "use of AT TIME ZONE or timezone() with time type",
        default: false,
        internal: true,
        enable_for_item_parsing: true,
    },
    {
        name: enable_aws_connection,
        desc: "CREATE CONNECTION ... TO AWS",
        default: false,
        internal: true,
        enable_for_item_parsing: false,
    },
    {
        name: enable_mysql_source,
        desc: "Create a MySQL connection or source",
        default: false,
        internal: true,
        enable_for_item_parsing: false,
    },
    {
        name: enable_expressions_in_limit_syntax,
        desc: "LIMIT <expr> syntax",
        default: false,
        internal: true,
        enable_for_item_parsing: true,
    },
    {
        name: enable_mz_notices,
        desc: "Populate the contents of `mz_internal.mz_notices`",
        default: true,
        internal: true,
        enable_for_item_parsing: false,
    },
    {
        name: enable_eager_delta_joins,
        desc:
            "eager delta joins",
        default: false,
        internal: true,
        enable_for_item_parsing: false,
    },
    {
        name: enable_off_thread_optimization,
        desc: "use off-thread optimization in `CREATE` statements",
        default: true,
        internal: true,
        enable_for_item_parsing: false,
    },
    {
        name: enable_refresh_every_mvs,
        desc: "REFRESH EVERY materialized views",
        default: false,
        internal: true,
        enable_for_item_parsing: true,
    },
    {
        name: enable_reduce_mfp_fusion,
        desc: "fusion of MFPs in reductions",
        default: false,
        internal: true,
        enable_for_item_parsing: false,
    },
    {
        name: enable_worker_core_affinity,
        desc: "set core affinity for replica worker threads",
        default: false,
        internal: true,
        enable_for_item_parsing: false,
    },
    {
        name: wait_catalog_consolidation_on_startup,
        desc: "When opening the Catalog, wait for consolidation to complete before returning",
        default: false,
        internal: true,
        enable_for_item_parsing: false,
    },
    {
        name: enable_copy_to_expr,
        desc: "COPY ... TO 's3://...'",
        default: false,
        internal: true,
        enable_for_item_parsing: false,
    },
    {
        name: enable_compute_aggressive_readhold_downgrades,
        desc: "let the compute controller aggressively downgrade read holds for sink dataflows",
        default: true,
        internal: true,
        enable_for_item_parsing: false,
    },
    {
        name: enable_session_timelines,
        desc: "strong session serializable isolation levels",
        default: false,
        internal: true,
        enable_for_item_parsing: false,
    },
    {
        name: enable_compute_operator_hydration_status_logging,
        desc: "log the hydration status of compute operators",
        default: true,
        internal: true,
        enable_for_item_parsing: false,
    },
);

// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Run-time configuration parameters
//!
//! ## Overview
//! Materialize roughly follows the PostgreSQL configuration model, which works
//! as follows. There is a global set of named configuration parameters, like
//! `DateStyle` and `client_encoding`. These parameters can be set in several
//! places: in an on-disk configuration file (in Postgres, named
//! postgresql.conf), in command line arguments when the server is started, or
//! at runtime via the `ALTER SYSTEM` or `SET` statements. Parameters that are
//! set in a session take precedence over database defaults, which in turn take
//! precedence over command line arguments, which in turn take precedence over
//! settings in the on-disk configuration. Note that changing the value of
//! parameters obeys transaction semantics: if a transaction fails to commit,
//! any parameters that were changed in that transaction (i.e., via `SET`) will
//! be rolled back to their previous value.
//!
//! The Materialize configuration hierarchy at the moment is much simpler.
//! Global defaults are hardcoded into the binary, and a select few parameters
//! can be overridden per session. A select few parameters can be overridden on
//! disk.
//!
//! The set of variables that can be overridden per session and the set of
//! variables that can be overridden on disk are currently disjoint. The
//! infrastructure has been designed with an eye towards merging these two sets
//! and supporting additional layers to the hierarchy, however, should the need
//! arise.
//!
//! The configuration parameters that exist are driven by compatibility with
//! PostgreSQL drivers that expect them, not because they are particularly
//! important.
//!
//! ## Structure
//! Thw most meaningful exports from this module are:
//!
//! - [`SessionVars`] represent per-session parameters, which each user can
//!   access independently of one another, and are accessed via `SET`.
//!
//!   The fields of [`SessionVars`] are either;
//!     - `SessionVar`, which is preferable and simply requires full support of
//!       the `SessionVar` impl for its embedded value type.
//!     - [`ServerVar`] for types that do not currently support everything
//!       required by `SessionVar`, e.g. they are fixed-value parameters.
//!
//!   In the fullness of time, all fields in [`SessionVars`] should be
//!   `SessionVar`.
//!
//! - [`SystemVars`] represent system-wide configuration settings and are
//!   accessed via `ALTER SYSTEM SET`.
//!
//!   All elements of [`SystemVars`] are `SystemVar`.
//!
//! Some [`ServerVar`] are also marked as a [`FeatureFlag`]; this is just a
//! wrapper to make working with a set of [`ServerVar`] easier, primarily from
//! within SQL planning, where we might want to check if a feature is enabled
//! before planning it.

use std::any::Any;
use std::borrow::Borrow;
use std::collections::BTreeMap;
use std::fmt::Debug;
use std::str::FromStr;
use std::string::ToString;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use itertools::Itertools;
use mz_build_info::BuildInfo;
use mz_ore::cast;
use mz_ore::cast::CastFrom;
use mz_ore::str::StrExt;
use mz_persist_client::cfg::PersistConfig;
use mz_repr::adt::numeric::Numeric;
use mz_sql_parser::ast::TransactionIsolationLevel;
use mz_tracing::CloneableEnvFilter;
use once_cell::sync::Lazy;
use serde::Serialize;
use uncased::UncasedStr;

use crate::ast::Ident;
use crate::session::user::{ExternalUserMetadata, User, SYSTEM_USER};
use crate::DEFAULT_SCHEMA;

/// The action to take during end_transaction.
///
/// This enum lives here because of convenience: it's more of an adapter
/// concept but [`SessionVars::end_transaction`] takes it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EndTransactionAction {
    /// Commit the transaction.
    Commit,
    /// Rollback the transaction.
    Rollback,
}

/// Errors that can occur when working with [`Var`]s
#[derive(Clone, Debug, Eq, PartialEq, thiserror::Error)]
pub enum VarError {
    /// The specified session parameter is constrained to a finite set of
    /// values.
    #[error(
        "invalid value for parameter {}: {}",
        parameter.name.quoted(),
        values.iter().map(|v| v.quoted()).join(",")
    )]
    ConstrainedParameter {
        parameter: VarErrParam,
        values: Vec<String>,
        valid_values: Option<Vec<&'static str>>,
    },
    /// The specified parameter is fixed to a single specific value.
    ///
    /// We allow setting the parameter to its fixed value for compatibility
    /// with PostgreSQL-based tools.
    #[error(
        "parameter {} can only be set to {}",
        .0.name.quoted(),
        .0.value.quoted(),
    )]
    FixedValueParameter(VarErrParam),
    /// The value for the specified parameter does not have the right type.
    #[error(
        "parameter {} requires a {} value",
        .0.name.quoted(),
        .0.type_name.quoted()
    )]
    InvalidParameterType(VarErrParam),
    /// The value of the specified parameter is incorrect.
    #[error(
        "parameter {} cannot have value {}: {}",
        parameter.name.quoted(),
        values
            .iter()
            .map(|v| v.quoted().to_string())
            .collect::<Vec<_>>()
            .join(","),
        reason,
    )]
    InvalidParameterValue {
        parameter: VarErrParam,
        values: Vec<String>,
        reason: String,
    },
    /// The specified session parameter is read only.
    #[error("parameter {} cannot be changed", .0.quoted())]
    ReadOnlyParameter(&'static str),
    /// The named parameter is unknown to the system.
    #[error("unrecognized configuration parameter {}", .0.quoted())]
    UnknownParameter(String),
    /// The specified session parameter is read only unless in unsafe mode.
    #[error("parameter {} can only be set in unsafe mode", .0.quoted())]
    RequiresUnsafeMode(&'static str),
    #[error("{} is not supported", .feature)]
    RequiresFeatureFlag {
        feature: String,
        detail: Option<String>,
        /// If we're running in unsafe mode and hit this error, we should surface the flag name that
        /// needs to be set to make the feature work.
        name_hint: Option<&'static UncasedStr>,
    },
}

impl VarError {
    pub fn detail(&self) -> Option<String> {
        match self {
            Self::RequiresFeatureFlag { detail, .. } => {
                match detail {
                    None => Some("The requested feature is typically meant only for internal development and testing of Materialize.".into()),
                    o => o.clone()
                }
            }
            _ => None,
        }
    }

    pub fn hint(&self) -> Option<String> {
        match self {
            VarError::ConstrainedParameter {
                valid_values: Some(valid_values),
                ..
            } => Some(format!("Available values: {}.", valid_values.join(", "))),
            VarError::RequiresFeatureFlag { name_hint, .. } => {
                name_hint.map(|name| format!("Enable with {name} flag"))
            }
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
/// We don't want to hold a static reference to a variable while erroring, so take an owned version
/// of the fields we want.
pub struct VarErrParam {
    name: &'static str,
    value: String,
    type_name: String,
}

impl<'a, V: Var + Send + Sync + ?Sized> From<&'a V> for VarErrParam {
    fn from(var: &'a V) -> VarErrParam {
        VarErrParam {
            name: var.name(),
            value: var.value(),
            type_name: var.type_name(),
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

pub static DEFAULT_APPLICATION_NAME: Lazy<String> = Lazy::new(|| "".to_string());
pub static APPLICATION_NAME: Lazy<ServerVar<String>> = Lazy::new(|| ServerVar {
    name: UncasedStr::new("application_name"),
    value: &DEFAULT_APPLICATION_NAME,
    description: "Sets the application name to be reported in statistics and logs (PostgreSQL).",
    internal: false,
});

pub static CLIENT_ENCODING: ServerVar<ClientEncoding> = ServerVar {
    name: UncasedStr::new("client_encoding"),
    value: &ClientEncoding::Utf8,
    description: "Sets the client's character set encoding (PostgreSQL).",
    internal: false,
};

const CLIENT_MIN_MESSAGES: ServerVar<ClientSeverity> = ServerVar {
    name: UncasedStr::new("client_min_messages"),
    value: &ClientSeverity::Notice,
    description: "Sets the message levels that are sent to the client (PostgreSQL).",
    internal: false,
};

pub const CLUSTER_VAR_NAME: &UncasedStr = UncasedStr::new("cluster");
pub static DEFAULT_CLUSTER: Lazy<String> = Lazy::new(|| "default".to_string());
pub static CLUSTER: Lazy<ServerVar<String>> = Lazy::new(|| ServerVar {
    name: CLUSTER_VAR_NAME,
    value: &DEFAULT_CLUSTER,
    description: "Sets the current cluster (Materialize).",
    internal: false,
});

const CLUSTER_REPLICA: ServerVar<Option<String>> = ServerVar {
    name: UncasedStr::new("cluster_replica"),
    value: &None,
    description: "Sets a target cluster replica for SELECT queries (Materialize).",
    internal: false,
};

pub const DATABASE_VAR_NAME: &UncasedStr = UncasedStr::new("database");
pub static DEFAULT_DATABASE: Lazy<String> = Lazy::new(|| DEFAULT_DATABASE_NAME.to_string());
pub static DATABASE: Lazy<ServerVar<String>> = Lazy::new(|| ServerVar {
    name: DATABASE_VAR_NAME,
    value: &DEFAULT_DATABASE,
    description: "Sets the current database (CockroachDB).",
    internal: false,
});

static DATE_STYLE: ServerVar<DateStyle> = ServerVar {
    // DateStyle has nonstandard capitalization for historical reasons.
    name: UncasedStr::new("DateStyle"),
    value: &DEFAULT_DATE_STYLE,
    description: "Sets the display format for date and time values (PostgreSQL).",
    internal: false,
};

const EXTRA_FLOAT_DIGITS: ServerVar<i32> = ServerVar {
    name: UncasedStr::new("extra_float_digits"),
    value: &3,
    description: "Adjusts the number of digits displayed for floating-point values (PostgreSQL).",
    internal: false,
};

const FAILPOINTS: ServerVar<Failpoints> = ServerVar {
    name: UncasedStr::new("failpoints"),
    value: &Failpoints,
    description: "Allows failpoints to be dynamically activated.",
    internal: false,
};

const INTEGER_DATETIMES: ServerVar<bool> = ServerVar {
    name: UncasedStr::new("integer_datetimes"),
    value: &true,
    description: "Reports whether the server uses 64-bit-integer dates and times (PostgreSQL).",
    internal: false,
};

pub static INTERVAL_STYLE: ServerVar<IntervalStyle> = ServerVar {
    // IntervalStyle has nonstandard capitalization for historical reasons.
    name: UncasedStr::new("IntervalStyle"),
    value: &IntervalStyle::Postgres,
    description: "Sets the display format for interval values (PostgreSQL).",
    internal: false,
};

const MZ_VERSION_NAME: &UncasedStr = UncasedStr::new("mz_version");
const IS_SUPERUSER_NAME: &UncasedStr = UncasedStr::new("is_superuser");

// Schema can be used an alias for a search path with a single element.
pub const SCHEMA_ALIAS: &UncasedStr = UncasedStr::new("schema");
static DEFAULT_SEARCH_PATH: Lazy<Vec<Ident>> = Lazy::new(|| vec![Ident::new(DEFAULT_SCHEMA)]);
static SEARCH_PATH: Lazy<ServerVar<Vec<Ident>>> = Lazy::new(|| ServerVar {
    name: UncasedStr::new("search_path"),
    value: &*DEFAULT_SEARCH_PATH,
    description:
        "Sets the schema search order for names that are not schema-qualified (PostgreSQL).",
    internal: false,
});

const STATEMENT_TIMEOUT: ServerVar<Duration> = ServerVar {
    name: UncasedStr::new("statement_timeout"),
    value: &Duration::from_secs(10),
    description:
        "Sets the maximum allowed duration of INSERT...SELECT, UPDATE, and DELETE operations. \
        If this value is specified without units, it is taken as milliseconds.",
    internal: false,
};

const IDLE_IN_TRANSACTION_SESSION_TIMEOUT: ServerVar<Duration> = ServerVar {
    name: UncasedStr::new("idle_in_transaction_session_timeout"),
    value: &Duration::from_secs(60 * 2),
    description:
        "Sets the maximum allowed duration that a session can sit idle in a transaction before \
         being terminated. If this value is specified without units, it is taken as milliseconds. \
         A value of zero disables the timeout (PostgreSQL).",
    internal: false,
};

pub static SERVER_VERSION_VALUE: Lazy<String> = Lazy::new(|| {
    format!(
        "{}.{}.{}",
        SERVER_MAJOR_VERSION, SERVER_MINOR_VERSION, SERVER_PATCH_VERSION
    )
});

pub static SERVER_VERSION: Lazy<ServerVar<String>> = Lazy::new(|| ServerVar {
    name: UncasedStr::new("server_version"),
    value: &SERVER_VERSION_VALUE,
    description: "Shows the PostgreSQL compatible server version (PostgreSQL).",
    internal: false,
});

const SERVER_VERSION_NUM: ServerVar<i32> = ServerVar {
    name: UncasedStr::new("server_version_num"),
    value: &((cast::u8_to_i32(SERVER_MAJOR_VERSION) * 10_000)
        + (cast::u8_to_i32(SERVER_MINOR_VERSION) * 100)
        + cast::u8_to_i32(SERVER_PATCH_VERSION)),
    description: "Shows the PostgreSQL compatible server version as an integer (PostgreSQL).",
    internal: false,
};

const SQL_SAFE_UPDATES: ServerVar<bool> = ServerVar {
    name: UncasedStr::new("sql_safe_updates"),
    value: &false,
    description: "Prohibits SQL statements that may be overly destructive (CockroachDB).",
    internal: false,
};

const STANDARD_CONFORMING_STRINGS: ServerVar<bool> = ServerVar {
    name: UncasedStr::new("standard_conforming_strings"),
    value: &true,
    description: "Causes '...' strings to treat backslashes literally (PostgreSQL).",
    internal: false,
};

const TIMEZONE: ServerVar<TimeZone> = ServerVar {
    // TimeZone has nonstandard capitalization for historical reasons.
    name: UncasedStr::new("TimeZone"),
    value: &TimeZone::UTC,
    description: "Sets the time zone for displaying and interpreting time stamps (PostgreSQL).",
    internal: false,
};

pub const TRANSACTION_ISOLATION_VAR_NAME: &UncasedStr = UncasedStr::new("transaction_isolation");
const TRANSACTION_ISOLATION: ServerVar<IsolationLevel> = ServerVar {
    name: TRANSACTION_ISOLATION_VAR_NAME,
    value: &IsolationLevel::StrictSerializable,
    description: "Sets the current transaction's isolation level (PostgreSQL).",
    internal: false,
};

pub const MAX_AWS_PRIVATELINK_CONNECTIONS: ServerVar<u32> = ServerVar {
    name: UncasedStr::new("max_aws_privatelink_connections"),
    value: &0,
    description: "The maximum number of AWS PrivateLink connections in the region, across all schemas (Materialize).",
    internal: false
};

pub const MAX_TABLES: ServerVar<u32> = ServerVar {
    name: UncasedStr::new("max_tables"),
    value: &25,
    description: "The maximum number of tables in the region, across all schemas (Materialize).",
    internal: false,
};

pub const MAX_SOURCES: ServerVar<u32> = ServerVar {
    name: UncasedStr::new("max_sources"),
    value: &25,
    description: "The maximum number of sources in the region, across all schemas (Materialize).",
    internal: false,
};

pub const MAX_SINKS: ServerVar<u32> = ServerVar {
    name: UncasedStr::new("max_sinks"),
    value: &25,
    description: "The maximum number of sinks in the region, across all schemas (Materialize).",
    internal: false,
};

pub const MAX_MATERIALIZED_VIEWS: ServerVar<u32> = ServerVar {
    name: UncasedStr::new("max_materialized_views"),
    value: &100,
    description:
        "The maximum number of materialized views in the region, across all schemas (Materialize).",
    internal: false,
};

pub const MAX_CLUSTERS: ServerVar<u32> = ServerVar {
    name: UncasedStr::new("max_clusters"),
    value: &10,
    description: "The maximum number of clusters in the region (Materialize).",
    internal: false,
};

pub const MAX_REPLICAS_PER_CLUSTER: ServerVar<u32> = ServerVar {
    name: UncasedStr::new("max_replicas_per_cluster"),
    value: &5,
    description: "The maximum number of replicas of a single cluster (Materialize).",
    internal: false,
};

static DEFAULT_MAX_CREDIT_CONSUMPTION_RATE: Lazy<Numeric> = Lazy::new(|| 1024.into());
pub static MAX_CREDIT_CONSUMPTION_RATE: Lazy<ServerVar<Numeric>> = Lazy::new(|| {
    ServerVar {
        name: UncasedStr::new("max_credit_consumption_rate"),
        value: &DEFAULT_MAX_CREDIT_CONSUMPTION_RATE,
        description: "The maximum rate of credit consumption in a region. Credits are consumed based on the size of cluster replicas in use (Materialize).",
        internal: false
    }
});

pub const MAX_DATABASES: ServerVar<u32> = ServerVar {
    name: UncasedStr::new("max_databases"),
    value: &1000,
    description: "The maximum number of databases in the region (Materialize).",
    internal: false,
};

pub const MAX_SCHEMAS_PER_DATABASE: ServerVar<u32> = ServerVar {
    name: UncasedStr::new("max_schemas_per_database"),
    value: &1000,
    description: "The maximum number of schemas in a database (Materialize).",
    internal: false,
};

pub const MAX_OBJECTS_PER_SCHEMA: ServerVar<u32> = ServerVar {
    name: UncasedStr::new("max_objects_per_schema"),
    value: &1000,
    description: "The maximum number of objects in a schema (Materialize).",
    internal: false,
};

pub const MAX_SECRETS: ServerVar<u32> = ServerVar {
    name: UncasedStr::new("max_secrets"),
    value: &100,
    description: "The maximum number of secrets in the region, across all schemas (Materialize).",
    internal: false,
};

pub const MAX_ROLES: ServerVar<u32> = ServerVar {
    name: UncasedStr::new("max_roles"),
    value: &1000,
    description: "The maximum number of roles in the region (Materialize).",
    internal: false,
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
};

static DEFAULT_ALLOWED_CLUSTER_REPLICA_SIZES: Lazy<Vec<Ident>> = Lazy::new(Vec::new);
static ALLOWED_CLUSTER_REPLICA_SIZES: Lazy<ServerVar<Vec<Ident>>> = Lazy::new(|| ServerVar {
    name: UncasedStr::new("allowed_cluster_replica_sizes"),
    value: &DEFAULT_ALLOWED_CLUSTER_REPLICA_SIZES,
    description: "The allowed sizes when creating a new cluster replica (Materialize).",
    internal: false,
});

/// Controls [`mz_persist_client::cfg::DynamicConfig::blob_target_size`].
const PERSIST_BLOB_TARGET_SIZE: ServerVar<usize> = ServerVar {
    name: UncasedStr::new("persist_blob_target_size"),
    value: &PersistConfig::DEFAULT_BLOB_TARGET_SIZE,
    description: "A target maximum size of persist blob payloads in bytes (Materialize).",
    internal: true,
};

/// Controls [`mz_persist_client::cfg::DynamicConfig::blob_cache_mem_limit_bytes`].
const PERSIST_BLOB_CACHE_MEM_LIMIT_BYTES: ServerVar<usize> = ServerVar {
    name: UncasedStr::new("persist_blob_cache_mem_limit_bytes"),
    value: &PersistConfig::DEFAULT_BLOB_CACHE_MEM_LIMIT_BYTES,
    description:
        "Capacity of in-mem blob cache in bytes. Only takes effect on restart (Materialize).",
    internal: true,
};

/// Controls [`mz_persist_client::cfg::DynamicConfig::compaction_minimum_timeout`].
const PERSIST_COMPACTION_MINIMUM_TIMEOUT: ServerVar<Duration> = ServerVar {
    name: UncasedStr::new("persist_compaction_minimum_timeout"),
    value: &PersistConfig::DEFAULT_COMPACTION_MINIMUM_TIMEOUT,
    description: "The minimum amount of time to allow a persist compaction request to run before \
                  timing it out (Materialize).",
    internal: true,
};

/// Controls [`mz_persist_client::cfg::DynamicConfig::consensus_connection_pool_ttl`].
const PERSIST_CONSENSUS_CONNECTION_POOL_TTL: ServerVar<Duration> = ServerVar {
    name: UncasedStr::new("persist_consensus_connection_pool_ttl"),
    value: &PersistConfig::DEFAULT_CONSENSUS_CONNPOOL_TTL,
    description: "The minimum TTL of a Consensus connection to Postgres/CRDB before it is proactively terminated",
    internal: true,
};

/// Controls [`mz_persist_client::cfg::DynamicConfig::consensus_connection_pool_ttl_stagger`].
const PERSIST_CONSENSUS_CONNECTION_POOL_TTL_STAGGER: ServerVar<Duration> = ServerVar {
    name: UncasedStr::new("persist_consensus_connection_pool_ttl_stagger"),
    value: &PersistConfig::DEFAULT_CONSENSUS_CONNPOOL_TTL_STAGGER,
    description: "The minimum time between TTLing Consensus connections to Postgres/CRDB.",
    internal: true,
};

/// Controls initial backoff of [`mz_persist_client::cfg::DynamicConfig::next_listen_batch_retry_params`].
const PERSIST_NEXT_LISTEN_BATCH_RETRYER_INITIAL_BACKOFF: ServerVar<Duration> = ServerVar {
    name: UncasedStr::new("persist_next_listen_batch_retryer_initial_backoff"),
    value: &PersistConfig::DEFAULT_NEXT_LISTEN_BATCH_RETRYER.initial_backoff,
    description: "The initial backoff when polling for new batches from a Listen or Subscribe.",
    internal: true,
};

/// Controls backoff multiplier of [`mz_persist_client::cfg::DynamicConfig::next_listen_batch_retry_params`].
const PERSIST_NEXT_LISTEN_BATCH_RETRYER_MULTIPLIER: ServerVar<u32> = ServerVar {
    name: UncasedStr::new("persist_next_listen_batch_retryer_multiplier"),
    value: &PersistConfig::DEFAULT_NEXT_LISTEN_BATCH_RETRYER.multiplier,
    description: "The backoff multiplier when polling for new batches from a Listen or Subscribe.",
    internal: true,
};

/// Controls backoff clamp of [`mz_persist_client::cfg::DynamicConfig::next_listen_batch_retry_params`].
const PERSIST_NEXT_LISTEN_BATCH_RETRYER_CLAMP: ServerVar<Duration> = ServerVar {
    name: UncasedStr::new("persist_next_listen_batch_retryer_clamp"),
    value: &PersistConfig::DEFAULT_NEXT_LISTEN_BATCH_RETRYER.clamp,
    description:
        "The backoff clamp duration when polling for new batches from a Listen or Subscribe.",
    internal: true,
};

/// The default for the `DISK` option when creating managed clusters and cluster replicas.
const DISK_CLUSTER_REPLICAS_DEFAULT: ServerVar<bool> = ServerVar {
    name: UncasedStr::new("disk_cluster_replicas_default"),
    value: &false,
    description: "Whether the disk option for managed clusters and cluster replicas should be enabled by default.",
    internal: true,
};

/// Tuning for RocksDB used by `UPSERT` sources that takes effect on restart.
mod upsert_rocksdb {
    use std::str::FromStr;

    use mz_rocksdb_types::config::{CompactionStyle, CompressionType};

    use super::*;

    impl Value for CompactionStyle {
        fn type_name() -> String {
            "rocksdb_compaction_style".to_string()
        }

        fn parse<'a>(
            param: &'a (dyn Var + Send + Sync),
            input: VarInput,
        ) -> Result<Self::Owned, VarError> {
            let s = extract_single_value(param, input)?;
            CompactionStyle::from_str(s).map_err(|_| VarError::InvalidParameterType(param.into()))
        }

        fn format(&self) -> String {
            self.to_string()
        }
    }

    impl Value for CompressionType {
        fn type_name() -> String {
            "rocksdb_compression_type".to_string()
        }

        fn parse<'a>(
            param: &'a (dyn Var + Send + Sync),
            input: VarInput,
        ) -> Result<Self::Owned, VarError> {
            let s = extract_single_value(param, input)?;
            CompressionType::from_str(s).map_err(|_| VarError::InvalidParameterType(param.into()))
        }

        fn format(&self) -> String {
            self.to_string()
        }
    }

    pub static UPSERT_ROCKSDB_COMPACTION_STYLE: ServerVar<CompactionStyle> = ServerVar {
        name: UncasedStr::new("upsert_rocksdb_compaction_style"),
        value: &mz_rocksdb_types::defaults::DEFAULT_COMPACTION_STYLE,
        description: "Tuning parameter for RocksDB as used in `UPSERT/DEBEZIUM` \
                  sources. Described in the `mz_rocksdb_types::config` module. \
                  Only takes effect on source restart (Materialize).",
        internal: true,
    };
    pub const UPSERT_ROCKSDB_OPTIMIZE_COMPACTION_MEMTABLE_BUDGET: ServerVar<usize> = ServerVar {
        name: UncasedStr::new("upsert_rocksdb_optimize_compaction_memtable_budget"),
        value: &mz_rocksdb_types::defaults::DEFAULT_OPTIMIZE_COMPACTION_MEMTABLE_BUDGET,
        description: "Tuning parameter for RocksDB as used in `UPSERT/DEBEZIUM` \
                  sources. Described in the `mz_rocksdb_types::config` module. \
                  Only takes effect on source restart (Materialize).",
        internal: true,
    };
    pub const UPSERT_ROCKSDB_LEVEL_COMPACTION_DYNAMIC_LEVEL_BYTES: ServerVar<bool> = ServerVar {
        name: UncasedStr::new("upsert_rocksdb_level_compaction_dynamic_level_bytes"),
        value: &mz_rocksdb_types::defaults::DEFAULT_LEVEL_COMPACTION_DYNAMIC_LEVEL_BYTES,
        description: "Tuning parameter for RocksDB as used in `UPSERT/DEBEZIUM` \
                  sources. Described in the `mz_rocksdb_types::config` module. \
                  Only takes effect on source restart (Materialize).",
        internal: true,
    };
    pub const UPSERT_ROCKSDB_UNIVERSAL_COMPACTION_RATIO: ServerVar<i32> = ServerVar {
        name: UncasedStr::new("upsert_rocksdb_universal_compaction_ratio"),
        value: &mz_rocksdb_types::defaults::DEFAULT_UNIVERSAL_COMPACTION_RATIO,
        description: "Tuning parameter for RocksDB as used in `UPSERT/DEBEZIUM` \
                  sources. Described in the `mz_rocksdb_types::config` module. \
                  Only takes effect on source restart (Materialize).",
        internal: true,
    };
    pub const UPSERT_ROCKSDB_PARALLELISM: ServerVar<Option<i32>> = ServerVar {
        name: UncasedStr::new("upsert_rocksdb_parallelism"),
        value: &mz_rocksdb_types::defaults::DEFAULT_PARALLELISM,
        description: "Tuning parameter for RocksDB as used in `UPSERT/DEBEZIUM` \
                  sources. Described in the `mz_rocksdb_types::config` module. \
                  Only takes effect on source restart (Materialize).",
        internal: true,
    };
    pub static UPSERT_ROCKSDB_COMPRESSION_TYPE: ServerVar<CompressionType> = ServerVar {
        name: UncasedStr::new("upsert_rocksdb_compression_type"),
        value: &mz_rocksdb_types::defaults::DEFAULT_COMPRESSION_TYPE,
        description: "Tuning parameter for RocksDB as used in `UPSERT/DEBEZIUM` \
                  sources. Described in the `mz_rocksdb_types::config` module. \
                  Only takes effect on source restart (Materialize).",
        internal: true,
    };
    pub static UPSERT_ROCKSDB_BOTTOMMOST_COMPRESSION_TYPE: ServerVar<CompressionType> = ServerVar {
        name: UncasedStr::new("upsert_rocksdb_bottommost_compression_type"),
        value: &mz_rocksdb_types::defaults::DEFAULT_BOTTOMMOST_COMPRESSION_TYPE,
        description: "Tuning parameter for RocksDB as used in `UPSERT/DEBEZIUM` \
                  sources. Described in the `mz_rocksdb_types::config` module. \
                  Only takes effect on source restart (Materialize).",
        internal: true,
    };

    pub static UPSERT_ROCKSDB_BATCH_SIZE: ServerVar<usize> = ServerVar {
        name: UncasedStr::new("upsert_rocksdb_batch_size"),
        value: &mz_rocksdb_types::defaults::DEFAULT_BATCH_SIZE,
        description: "Tuning parameter for RocksDB as used in `UPSERT/DEBEZIUM` \
                  sources. Described in the `mz_rocksdb_types::config` module. \
                  Can be changed dynamically (Materialize).",
        internal: true,
    };

    pub static UPSERT_ROCKSDB_RETRY_DURATION: ServerVar<Duration> = ServerVar {
        name: UncasedStr::new("upsert_rocksdb_retry_duration"),
        value: &mz_rocksdb_types::defaults::DEFAULT_RETRY_DURATION,
        description: "Tuning parameter for RocksDB as used in `UPSERT/DEBEZIUM` \
                  sources. Described in the `mz_rocksdb_types::config` module. \
                  Only takes effect on source restart (Materialize).",
        internal: true,
    };

    /// Controls whether automatic spill to disk should be turned on when using `DISK`.
    pub const UPSERT_ROCKSDB_AUTO_SPILL_TO_DISK: ServerVar<bool> = ServerVar {
        name: UncasedStr::new("upsert_rocksdb_auto_spill_to_disk"),
        value: &false,
        description:
            "Controls whether automatic spill to disk should be turned on when using `DISK`",
        internal: true,
    };

    /// The upsert in memory state size threshold after which it will spill to disk.
    /// The default is 256 MB = 268435456 bytes
    pub const UPSERT_ROCKSDB_AUTO_SPILL_THRESHOLD_BYTES: ServerVar<usize> = ServerVar {
        name: UncasedStr::new("upsert_rocksdb_auto_spill_threshold_bytes"),
        value: &mz_rocksdb_types::defaults::DEFAULT_AUTO_SPILL_MEMORY_THRESHOLD,
        description:
            "The upsert in-memory state size threshold in bytes after which it will spill to disk",
        internal: true,
    };
}

/// Controls the connect_timeout setting when connecting to PG via replication.
const PG_REPLICATION_CONNECT_TIMEOUT: ServerVar<Duration> = ServerVar {
    name: UncasedStr::new("pg_replication_connect_timeout"),
    value: &mz_postgres_util::DEFAULT_REPLICATION_CONNECT_TIMEOUT,
    description: "Sets the timeout applied to socket-level connection attempts for PG \
    replication connections. (Materialize)",
    internal: true,
};

static DEFAULT_LOGGING_FILTER: Lazy<CloneableEnvFilter> =
    Lazy::new(|| CloneableEnvFilter::from_str("info").expect("valid EnvFilter"));
static LOGGING_FILTER: Lazy<ServerVar<CloneableEnvFilter>> = Lazy::new(|| ServerVar {
    name: UncasedStr::new("log_filter"),
    value: &DEFAULT_LOGGING_FILTER,
    description: "Sets the filter to apply to stderr logging.",
    internal: true,
});

static DEFAULT_OPENTELEMETRY_FILTER: Lazy<CloneableEnvFilter> =
    Lazy::new(|| CloneableEnvFilter::from_str("off").expect("valid EnvFilter"));
static OPENTELEMETRY_FILTER: Lazy<ServerVar<CloneableEnvFilter>> = Lazy::new(|| ServerVar {
    name: UncasedStr::new("opentelemetry_filter"),
    value: &DEFAULT_OPENTELEMETRY_FILTER,
    description: "Sets the filter to apply to OpenTelemetry-backed distributed tracing.",
    internal: true,
});

/// Sets the maximum number of TCP keepalive probes that will be sent before dropping a connection
/// when connecting to PG via replication.
const PG_REPLICATION_KEEPALIVES_RETRIES: ServerVar<u32> = ServerVar {
    name: UncasedStr::new("pg_replication_keepalives_retries"),
    value: &mz_postgres_util::DEFAULT_REPLICATION_KEEPALIVE_RETRIES,
    description:
        "Sets the maximum number of TCP keepalive probes that will be sent before dropping \
    a connection when connecting to PG via replication. (Materialize)",
    internal: true,
};

/// Sets the amount of idle time before a keepalive packet is sent on the connection when connecting
/// to PG via replication.
const PG_REPLICATION_KEEPALIVES_IDLE: ServerVar<Duration> = ServerVar {
    name: UncasedStr::new("pg_replication_keepalives_idle"),
    value: &mz_postgres_util::DEFAULT_REPLICATION_KEEPALIVE_IDLE,
    description:
        "Sets the amount of idle time before a keepalive packet is sent on the connection \
    when connecting to PG via replication. (Materialize)",
    internal: true,
};

/// Sets the time interval between TCP keepalive probes when connecting to PG via replication.
const PG_REPLICATION_KEEPALIVES_INTERVAL: ServerVar<Duration> = ServerVar {
    name: UncasedStr::new("pg_replication_keepalives_interval"),
    value: &mz_postgres_util::DEFAULT_REPLICATION_KEEPALIVE_INTERVAL,
    description: "Sets the time interval between TCP keepalive probes when connecting to PG via \
    replication. (Materialize)",
    internal: true,
};

/// Sets the TCP user timeout when connecting to PG via replication.
const PG_REPLICATION_TCP_USER_TIMEOUT: ServerVar<Duration> = ServerVar {
    name: UncasedStr::new("pg_replication_tcp_user_timeout"),
    value: &mz_postgres_util::DEFAULT_REPLICATION_TCP_USER_TIMEOUT,
    description: "Sets the TCP user timeout when connecting to PG via replication. (Materialize)",
    internal: true,
};

/// Controls the connection timeout to Cockroach.
///
/// Used by persist as [`mz_persist_client::cfg::DynamicConfig::consensus_connect_timeout`].
const CRDB_CONNECT_TIMEOUT: ServerVar<Duration> = ServerVar {
    name: UncasedStr::new("crdb_connect_timeout"),
    value: &PersistConfig::DEFAULT_CRDB_CONNECT_TIMEOUT,
    description: "The time to connect to CockroachDB before timing out and retrying.",
    internal: true,
};

/// Controls the TCP user timeout to Cockroach.
///
/// Used by persist as [`mz_persist_client::cfg::DynamicConfig::consensus_tcp_user_timeout`].
const CRDB_TCP_USER_TIMEOUT: ServerVar<Duration> = ServerVar {
    name: UncasedStr::new("crdb_tcp_user_timeout"),
    value: &PersistConfig::DEFAULT_CRDB_TCP_USER_TIMEOUT,
    description:
        "The TCP timeout for connections to CockroachDB. Specifies the amount of time that \
        transmitted data may remain unacknowledged before the TCP connection is forcibly \
        closed.",
    internal: true,
};

/// The maximum number of in-flight bytes emitted by persist_sources feeding dataflows.
const DATAFLOW_MAX_INFLIGHT_BYTES: ServerVar<usize> = ServerVar {
    name: UncasedStr::new("dataflow_max_inflight_bytes"),
    value: &usize::MAX,
    description: "The maximum number of in-flight bytes emitted by persist_sources feeding \
                  dataflows (Materialize).",
    internal: true,
};

/// Controls [`mz_persist_client::cfg::PersistConfig::sink_minimum_batch_updates`].
const PERSIST_SINK_MINIMUM_BATCH_UPDATES: ServerVar<usize> = ServerVar {
    name: UncasedStr::new("persist_sink_minimum_batch_updates"),
    value: &PersistConfig::DEFAULT_SINK_MINIMUM_BATCH_UPDATES,
    description: "In the compute persist sink, workers with less than the minimum number of updates \
                  will flush their records to single downstream worker to be batched up there... in \
                  the hopes of grouping our updates into fewer, larger batches.",
    internal: true
};

/// Controls [`mz_persist_client::cfg::PersistConfig::storage_sink_minimum_batch_updates`].
const STORAGE_PERSIST_SINK_MINIMUM_BATCH_UPDATES: ServerVar<usize> = ServerVar {
    name: UncasedStr::new("storage_persist_sink_minimum_batch_updates"),
    // Reasonable default based on our experience in production.
    value: &1024,
    description: "In the storage persist sink, workers with less than the minimum number of updates \
                  will flush their records to single downstream worker to be batched up there... in \
                  the hopes of grouping our updates into fewer, larger batches.",
    internal: true
};

/// Controls [`mz_persist_client::cfg::DynamicConfig::stats_audit_percent`].
const PERSIST_STATS_AUDIT_PERCENT: ServerVar<usize> = ServerVar {
    name: UncasedStr::new("persist_stats_audit_percent"),
    value: &PersistConfig::DEFAULT_STATS_AUDIT_PERCENT,
    description: "Percent of filtered data to opt in to correctness auditing (Materialize).",
    internal: true,
};

/// Controls [`mz_persist_client::cfg::DynamicConfig::stats_collection_enabled`].
const PERSIST_STATS_COLLECTION_ENABLED: ServerVar<bool> = ServerVar {
    name: UncasedStr::new("persist_stats_collection_enabled"),
    value: &PersistConfig::DEFAULT_STATS_COLLECTION_ENABLED,
    description: "Whether to calculate and record statistics about the data stored in persist \
                  to be used at read time, see persist_stats_filter_enabled (Materialize).",
    internal: true,
};

/// Controls [`mz_persist_client::cfg::DynamicConfig::stats_filter_enabled`].
const PERSIST_STATS_FILTER_ENABLED: ServerVar<bool> = ServerVar {
    name: UncasedStr::new("persist_stats_filter_enabled"),
    value: &PersistConfig::DEFAULT_STATS_FILTER_ENABLED,
    description: "Whether to use recorded statistics about the data stored in persist \
                  to filter at read time, see persist_stats_collection_enabled (Materialize).",
    internal: true,
};

/// Controls [`mz_persist_client::cfg::DynamicConfig::pubsub_client_enabled`].
const PERSIST_PUBSUB_CLIENT_ENABLED: ServerVar<bool> = ServerVar {
    name: UncasedStr::new("persist_pubsub_client_enabled"),
    value: &PersistConfig::DEFAULT_PUBSUB_CLIENT_ENABLED,
    description: "Whether to connect to the Persist PubSub service.",
    internal: true,
};

/// Controls [`mz_persist_client::cfg::DynamicConfig::pubsub_push_diff_enabled`].
const PERSIST_PUBSUB_PUSH_DIFF_ENABLED: ServerVar<bool> = ServerVar {
    name: UncasedStr::new("persist_pubsub_push_diff_enabled"),
    value: &PersistConfig::DEFAULT_PUBSUB_PUSH_DIFF_ENABLED,
    description: "Whether to push state diffs to Persist PubSub.",
    internal: true,
};

/// Controls [`mz_persist_client::cfg::DynamicConfig::rollup_threshold`].
const PERSIST_ROLLUP_THRESHOLD: ServerVar<usize> = ServerVar {
    name: UncasedStr::new("persist_rollup_threshold"),
    value: &PersistConfig::DEFAULT_ROLLUP_THRESHOLD,
    description: "The number of seqnos between rollups.",
    internal: true,
};

/// Boolean flag indicating that the remote configuration was synchronized at
/// least once with the persistent [SessionVars].
pub static CONFIG_HAS_SYNCED_ONCE: ServerVar<bool> = ServerVar {
    name: UncasedStr::new("config_has_synced_once"),
    value: &false,
    description: "Boolean flag indicating that the remote configuration was synchronized at least once (Materialize).",
    internal: true
};

/// Boolean flag indicating whether to enable syncing from
/// LaunchDarkly. Can be turned off as an emergency measure to still
/// be able to alter parameters while LD is broken.
pub static ENABLE_LAUNCHDARKLY: ServerVar<bool> = ServerVar {
    name: UncasedStr::new("enable_launchdarkly"),
    value: &true,
    description: "Boolean flag indicating whether flag synchronization from LaunchDarkly should be enabled (Materialize).",
    internal: true
};

/// Feature flag indicating whether real time recency is enabled. Not that
/// unlike other feature flags, this is made available at the session level, so
/// is additionally gated by a feature flag.
static REAL_TIME_RECENCY: ServerVar<bool> = ServerVar {
    name: UncasedStr::new("real_time_recency"),
    value: &false,
    description: "Feature flag indicating whether real time recency is enabled (Materialize).",
    internal: false,
};

static EMIT_TIMESTAMP_NOTICE: ServerVar<bool> = ServerVar {
    name: UncasedStr::new("emit_timestamp_notice"),
    value: &false,
    description:
        "Boolean flag indicating whether to send a NOTICE with timestamp explanations of queries (Materialize).",
    internal: false
};

static EMIT_TRACE_ID_NOTICE: ServerVar<bool> = ServerVar {
    name: UncasedStr::new("emit_trace_id_notice"),
    value: &false,
    description:
        "Boolean flag indicating whether to send a NOTICE specifying the trace id when available (Materialize).",
    internal: false
};

static UNSAFE_MOCK_AUDIT_EVENT_TIMESTAMP: ServerVar<Option<mz_repr::Timestamp>> = ServerVar {
    name: UncasedStr::new("unsafe_mock_audit_event_timestamp"),
    value: &None,
    description: "Mocked timestamp to use for audit events for testing purposes",
    internal: true,
};

pub const ENABLE_LD_RBAC_CHECKS: ServerVar<bool> = ServerVar {
    name: UncasedStr::new("enable_ld_rbac_checks"),
    // TODO(jkosh44) Once RBAC is complete, change this to `true`.
    value: &false,
    description:
        "LD facing global boolean flag that allows turning RBAC off for everyone (Materialize).",
    internal: true,
};

pub const ENABLE_RBAC_CHECKS: ServerVar<bool> = ServerVar {
    name: UncasedStr::new("enable_rbac_checks"),
    // TODO(jkosh44) Once RBAC is complete, change this to `true`.
    value: &false,
    description: "User facing global boolean flag indicating whether to apply RBAC checks before \
    executing statements (Materialize).",
    internal: false,
};

pub const ENABLE_SESSION_RBAC_CHECKS: ServerVar<bool> = ServerVar {
    name: UncasedStr::new("enable_session_rbac_checks"),
    // TODO(jkosh44) Once RBAC is complete, change this to `true`.
    value: &false,
    description: "User facing session boolean flag indicating whether to apply RBAC checks before \
    executing statements (Materialize).",
    internal: false,
};

/// Whether compute rendering should use Materialize's custom linear join implementation rather
/// than the one from Differential Dataflow.
const ENABLE_MZ_JOIN_CORE: ServerVar<bool> = ServerVar {
    name: UncasedStr::new("enable_mz_join_core"),
    value: &true,
    description:
        "Feature flag indicating whether compute rendering should use Materialize's custom linear \
         join implementation rather than the one from Differential Dataflow. (Materialize).",
    internal: true,
};

pub const ENABLE_DEFAULT_CONNECTION_VALIDATION: ServerVar<bool> = ServerVar {
    name: UncasedStr::new("enable_default_connection_validation"),
    value: &true,
    description:
        "LD facing global boolean flag that allows turning default connection validation off for everyone (Materialize).",
    internal: true,
};

pub const AUTO_ROUTE_INTROSPECTION_QUERIES: ServerVar<bool> = ServerVar {
    name: UncasedStr::new("auto_route_introspection_queries"),
    value: &true,
    description:
        "Whether to force queries that depend only on system tables, to run on the mz_introspection cluster (Materialize).",
    internal: false
};

pub const MAX_CONNECTIONS: ServerVar<u32> = ServerVar {
    name: UncasedStr::new("max_connections"),
    value: &1000,
    description: "The maximum number of concurrent connections (Materialize).",
    internal: false,
};

/// Controls [`mz_storage_client::types::parameters::StorageParameters::keep_n_source_status_history_entries`].
const KEEP_N_SOURCE_STATUS_HISTORY_ENTRIES: ServerVar<usize> = ServerVar {
    name: UncasedStr::new("keep_n_source_status_history_entries"),
    value: &5,
    description: "On reboot, truncate all but the last n entries per ID in the source_status_history collection (Materialize).",
    internal: true
};

/// Controls [`mz_storage_client::types::parameters::StorageParameters::keep_n_sink_status_history_entries`].
const KEEP_N_SINK_STATUS_HISTORY_ENTRIES: ServerVar<usize> = ServerVar {
    name: UncasedStr::new("keep_n_sink_status_history_entries"),
    value: &5,
    description: "On reboot, truncate all but the last n entries per ID in the sink_status_history collection (Materialize).",
    internal: true
};

const ENABLE_STORAGE_SHARD_FINALIZATION: ServerVar<bool> = ServerVar {
    name: UncasedStr::new("enable_storage_shard_finalization"),
    value: &true,
    description: "Whether to allow the storage client to finalize shards (Materialize).",
    internal: true,
};

// Macro to simplify creating feature flags, i.e. boolean flags that we use to toggle the
// availability of features.
//
// Note that not all ServerVar<bool> are feature flags. Feature flags are for variables that:
// - Belong to `SystemVars`, _not_ `SessionVars`
// - Default to false and must be explicitly enabled
macro_rules! feature_flags {
    ($(($name:expr, $feature_desc:literal)),+ $(,)?) => {
        paste::paste!{
            $(
                // Note that the ServerVar is not directly exported; we expect these to be
                // accessible through their FeatureFlag variant.
                static [<$name:upper _VAR>]: ServerVar<bool> = ServerVar {
                    name: UncasedStr::new(stringify!($name)),
                    value: &false,
                    description: concat!("Whether ", $feature_desc, " is allowed (Materialize)."),
                    internal: true                };

                pub static [<$name:upper >]: FeatureFlag = FeatureFlag {
                    flag: &[<$name:upper _VAR>],
                    feature_desc: $feature_desc,
                };
            )+

            impl SystemVars {
                fn with_feature_flags(self) -> Self
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

                pub fn enable_all_feature_flags(&mut self) {
                    $(
                        self.set(stringify!($name), VarInput::Flat("on"))
                            .expect("setting default value must work");
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
    (allow_real_time_recency, "real time recency"),
    // Actual feature flags
    (
        enable_binary_date_bin,
        "the binary version of date_bin function"
    ),
    (
        enable_create_sink_denylist_with_options,
        "CREATE SINK with unsafe options"
    ),
    (
        enable_create_source_denylist_with_options,
        "CREATE SOURCE with unsafe options"
    ),
    (
        enable_create_source_from_testscript,
        "CREATE SOURCE ... FROM TEST SCRIPT"
    ),
    (enable_date_bin_hopping, "the date_bin_hopping function"),
    (
        enable_envelope_debezium_in_subscribe,
        "`ENVELOPE DEBEZIUM (KEY (..))`"
    ),
    (enable_envelope_materialize, "ENVELOPE MATERIALIZE"),
    (
        enable_envelope_upsert_in_subscribe,
        "`ENVELOPE UPSERT` can be used in `SUBSCRIBE`"
    ),
    (enable_index_options, "INDEX OPTIONS"),
    (
        enable_kafka_config_denylist_options,
        "Kafka sources with non-allowlisted options"
    ),
    (enable_list_length_max, "the list_length_max function"),
    (enable_list_n_layers, "the list_n_layers function"),
    (enable_list_remove, "the list_remove function"),
    (
        enable_logical_compaction_window,
        "LOGICAL COMPACTION WINDOW"
    ),
    (
        enable_monotonic_oneshot_selects,
        "monotonic evaluation of one-shot SELECT queries"
    ),
    (enable_primary_key_not_enforced, "PRIMARY KEY NOT ENFORCED"),
    (enable_mfp_pushdown_explain, "`filter_pushdown` explain"),
    (
        enable_multi_worker_storage_persist_sink,
        "multi-worker storage persist sink"
    ),
    (enable_raise_statement, "RAISE statement"),
    (enable_repeat_row, "the repeat_row function"),
    (
        enable_table_check_constraint,
        "CREATE TABLE with a check constraint"
    ),
    (enable_table_foreign_key, "CREATE TABLE with a foreign key"),
    (
        enable_table_keys,
        "CREATE TABLE with a primary key or unique constraint"
    ),
    (
        enable_unmanaged_cluster_replicas,
        "unmanaged cluster replicas"
    ),
    (
        enable_unstable_dependencies,
        "depending on unstable objects"
    ),
    (
        enable_disk_cluster_replicas,
        "`WITH (DISK)` for cluster replicas"
    ),
    (enable_with_mutually_recursive, "WITH MUTUALLY RECURSIVE"),
    (
        enable_within_timestamp_order_by_in_subscribe,
        "`WITHIN TIMESTAMP ORDER BY ..`"
    ),
    (enable_managed_clusters, "managed clusters"),
    (
        enable_connection_validation_syntax,
        "CREATE CONNECTION .. WITH (VALIDATE) and VALIDATE CONNECTION syntax"
    ),
    (
        enable_webhook_sources,
        "creating or pushing data to webhook sources"
    ),
);

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
/// See the `mz_sql::session` module documentation for more details on the
/// Materialize configuration model.
#[derive(Debug)]
pub struct SessionVars {
    vars: BTreeMap<&'static UncasedStr, Box<dyn SessionVarMut>>,
    // Inputs to computed variables.
    build_info: &'static BuildInfo,
    user: User,
}

impl SessionVars {
    pub fn new(build_info: &'static BuildInfo, user: User) -> SessionVars {
        let s = SessionVars {
            vars: BTreeMap::new(),
            build_info,
            user,
        };

        s.with_var(&APPLICATION_NAME)
            .with_var(&CLIENT_ENCODING)
            .with_var(&CLIENT_MIN_MESSAGES)
            .with_var(&CLUSTER)
            .with_var(&CLUSTER_REPLICA)
            .with_var(&DATABASE)
            .with_var(&DATE_STYLE)
            .with_var(&EXTRA_FLOAT_DIGITS)
            .with_var(&FAILPOINTS)
            .with_value_constrained_var(&INTEGER_DATETIMES, ValueConstraint::Fixed)
            .with_var(&INTERVAL_STYLE)
            .with_var(&SEARCH_PATH)
            .with_value_constrained_var(&SERVER_VERSION, ValueConstraint::ReadOnly)
            .with_value_constrained_var(&SERVER_VERSION_NUM, ValueConstraint::ReadOnly)
            .with_var(&SEARCH_PATH)
            .with_var(&SQL_SAFE_UPDATES)
            .with_value_constrained_var(&STANDARD_CONFORMING_STRINGS, ValueConstraint::Fixed)
            .with_var(&STATEMENT_TIMEOUT)
            .with_var(&IDLE_IN_TRANSACTION_SESSION_TIMEOUT)
            .with_var(&TIMEZONE)
            .with_var(&TRANSACTION_ISOLATION)
            .with_feature_gated_var(&REAL_TIME_RECENCY, &ALLOW_REAL_TIME_RECENCY)
            .with_var(&EMIT_TIMESTAMP_NOTICE)
            .with_var(&EMIT_TRACE_ID_NOTICE)
            .with_var(&AUTO_ROUTE_INTROSPECTION_QUERIES)
            .with_var(&ENABLE_SESSION_RBAC_CHECKS)
    }

    fn with_var<V>(mut self, var: &'static ServerVar<V>) -> Self
    where
        V: Value + Debug + PartialEq + Clone + 'static,
        V::Owned: Debug + PartialEq + Send + Clone + Sync,
    {
        self.vars.insert(var.name, Box::new(SessionVar::new(var)));
        self
    }

    fn with_value_constrained_var<V>(
        mut self,
        var: &'static ServerVar<V>,
        c: ValueConstraint<V>,
    ) -> Self
    where
        V: Value + Debug + PartialEq + Clone + 'static,
        V::Owned: Debug + PartialEq + Send + Clone + Sync,
    {
        self.vars.insert(
            var.name,
            Box::new(SessionVar::new(var).with_value_constraint(c)),
        );
        self
    }

    fn with_feature_gated_var<V>(
        mut self,
        var: &'static ServerVar<V>,
        flag: &'static FeatureFlag,
    ) -> Self
    where
        V: Value + Debug + PartialEq + Clone + 'static,
        V::Owned: Debug + PartialEq + Send + Clone + Sync,
    {
        self.vars.insert(
            var.name,
            Box::new(SessionVar::new(var).add_feature_flag(flag)),
        );
        self
    }

    fn expect_value<V>(&self, var: &ServerVar<V>) -> &V
    where
        V: Value + Debug + PartialEq + Clone + 'static,
        V::Owned: Debug + PartialEq + Send + Clone + Sync,
    {
        let var = self
            .vars
            .get(var.name)
            .expect("provided var should be in state");

        var.value_any()
            .downcast_ref()
            .expect("provided var type should matched stored var")
    }

    /// Returns an iterator over the configuration parameters and their current
    /// values for this session.
    ///
    /// Note that this function does not check that the access variable should
    /// be visible because of other settings or users. Before or after accessing
    /// this method, you should call `Var::visible`.
    pub fn iter(&self) -> impl Iterator<Item = &dyn Var> {
        #[allow(clippy::as_conversions)]
        self.vars
            .values()
            .map(|v| v.as_var())
            .chain([self.build_info as &dyn Var, &self.user].into_iter())
    }

    /// Returns an iterator over configuration parameters (and their current
    /// values for this session) that are expected to be sent to the client when
    /// a new connection is established or when their value changes.
    pub fn notify_set(&self) -> impl Iterator<Item = &dyn Var> {
        #[allow(clippy::as_conversions)]
        [
            &*APPLICATION_NAME as &dyn Var,
            &CLIENT_ENCODING,
            &DATE_STYLE,
            &INTEGER_DATETIMES,
            &*SERVER_VERSION,
            &STANDARD_CONFORMING_STRINGS,
            &TIMEZONE,
            &INTERVAL_STYLE,
            // Including `cluster`, `cluster_replica`, and `database` in the notify set is a
            // Materialize extension. Doing so allows users to more easily identify where their
            // queries will be executing, which is important to know when you consider the size of
            // a cluster, what indexes are present, etc.
            &*CLUSTER,
            &CLUSTER_REPLICA,
            &*DATABASE,
        ]
        .into_iter()
        .map(|p| self.get(None, p.name()).expect("SystemVars known to exist"))
        // Including `mz_version` in the notify set is a Materialize
        // extension. Doing so allows applications to detect whether they
        // are talking to Materialize or PostgreSQL without an additional
        // network roundtrip. This is known to be safe because CockroachDB
        // has an analogous extension [0].
        // [0]: https://github.com/cockroachdb/cockroach/blob/369c4057a/pkg/sql/pgwire/conn.go#L1840
        .chain(std::iter::once(self.build_info as &dyn Var))
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
    pub fn get(&self, system_vars: Option<&SystemVars>, name: &str) -> Result<&dyn Var, VarError> {
        let name = UncasedStr::new(name);
        if name == MZ_VERSION_NAME {
            Ok(self.build_info)
        } else if name == IS_SUPERUSER_NAME {
            Ok(&self.user)
        } else {
            self.vars
                .get(name)
                .map(|v| {
                    v.visible(&self.user, system_vars)?;
                    Ok(v.as_var())
                })
                .transpose()?
                .ok_or_else(|| VarError::UnknownParameter(name.to_string()))
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
    pub fn set(
        &mut self,
        system_vars: Option<&SystemVars>,
        name: &str,
        input: VarInput,
        local: bool,
    ) -> Result<(), VarError> {
        let name = UncasedStr::new(name);
        if name == MZ_VERSION_NAME {
            Err(VarError::ReadOnlyParameter(MZ_VERSION_NAME.as_str()))
        } else if name == IS_SUPERUSER_NAME {
            Err(VarError::ReadOnlyParameter(IS_SUPERUSER_NAME.as_str()))
        } else {
            self.vars
                .get_mut(name)
                .map(|v| {
                    v.visible(&self.user, system_vars)?;
                    v.set(input, local)
                })
                .transpose()?
                .ok_or_else(|| VarError::UnknownParameter(name.to_string()))
        }
    }

    /// Sets the configuration parameter named `name` to its default value.
    ///
    /// The new value may be either committed or rolled back by the next call to
    /// [`SessionVars::end_transaction`]. If `local` is true, the new value is
    /// always discarded by the next call to [`SessionVars::end_transaction`],
    /// even if the transaction is marked to commit.
    ///
    /// Like with [`SessionVars::get`], configuration parameters are matched
    /// case insensitively. If the named configuration parameter does not exist,
    /// an error is returned.
    ///
    /// If the variable does not exist or the user does not have the visibility
    /// requires, this function returns an error.
    pub fn reset(
        &mut self,
        system_vars: Option<&SystemVars>,
        name: &str,
        local: bool,
    ) -> Result<(), VarError> {
        let name = UncasedStr::new(name);
        if name == MZ_VERSION_NAME {
            Err(VarError::ReadOnlyParameter(MZ_VERSION_NAME.as_str()))
        } else if name == IS_SUPERUSER_NAME {
            Err(VarError::ReadOnlyParameter(IS_SUPERUSER_NAME.as_str()))
        } else {
            self.vars
                .get_mut(name)
                .map(|v| {
                    v.visible(&self.user, system_vars)?;
                    v.reset(local);
                    Ok(())
                })
                .transpose()?
                .ok_or_else(|| VarError::UnknownParameter(name.to_string()))
        }
    }

    /// Commits or rolls back configuration parameter updates made via
    /// [`SessionVars::set`] since the last call to `end_transaction`.
    ///
    /// Returns any session parameters that changed because the transaction ended.
    pub fn end_transaction(
        &mut self,
        action: EndTransactionAction,
    ) -> BTreeMap<&'static str, String> {
        let mut changed = BTreeMap::new();
        for var in self.vars.values_mut() {
            let before = var.value();
            var.end_transaction(action);
            let after = var.value();

            // Report the new value of the parameter.
            if before != after {
                changed.insert(var.name(), after);
            }
        }
        changed
    }

    /// Returns the value of the `application_name` configuration parameter.
    pub fn application_name(&self) -> &str {
        self.expect_value(&*APPLICATION_NAME).as_str()
    }

    /// Returns the build info.
    pub fn build_info(&self) -> &'static BuildInfo {
        self.build_info
    }

    /// Returns the value of the `client_encoding` configuration parameter.
    pub fn client_encoding(&self) -> &ClientEncoding {
        self.expect_value(&CLIENT_ENCODING)
    }

    /// Returns the value of the `client_min_messages` configuration parameter.
    pub fn client_min_messages(&self) -> &ClientSeverity {
        self.expect_value(&CLIENT_MIN_MESSAGES)
    }

    /// Returns the value of the `cluster` configuration parameter.
    pub fn cluster(&self) -> &str {
        self.expect_value(&*CLUSTER).as_str()
    }

    /// Returns the value of the `cluster_replica` configuration parameter.
    pub fn cluster_replica(&self) -> Option<&str> {
        self.expect_value(&CLUSTER_REPLICA).as_deref()
    }

    /// Returns the value of the `DateStyle` configuration parameter.
    pub fn date_style(&self) -> &[&str] {
        &self.expect_value(&DATE_STYLE).0
    }

    /// Returns the value of the `database` configuration parameter.
    pub fn database(&self) -> &str {
        self.expect_value(&*DATABASE).as_str()
    }

    /// Returns the value of the `extra_float_digits` configuration parameter.
    pub fn extra_float_digits(&self) -> i32 {
        *self.expect_value(&EXTRA_FLOAT_DIGITS)
    }

    /// Returns the value of the `integer_datetimes` configuration parameter.
    pub fn integer_datetimes(&self) -> bool {
        *self.expect_value(&INTEGER_DATETIMES)
    }

    /// Returns the value of the `intervalstyle` configuration parameter.
    pub fn intervalstyle(&self) -> &IntervalStyle {
        self.expect_value(&INTERVAL_STYLE)
    }

    /// Returns the value of the `mz_version` configuration parameter.
    pub fn mz_version(&self) -> String {
        self.build_info.value()
    }

    /// Returns the value of the `search_path` configuration parameter.
    pub fn search_path(&self) -> &[Ident] {
        self.expect_value(&*SEARCH_PATH).as_slice()
    }

    /// Returns the value of the `server_version` configuration parameter.
    pub fn server_version(&self) -> &str {
        self.expect_value(&*SERVER_VERSION).as_str()
    }

    /// Returns the value of the `server_version_num` configuration parameter.
    pub fn server_version_num(&self) -> i32 {
        *self.expect_value(&SERVER_VERSION_NUM)
    }

    /// Returns the value of the `sql_safe_updates` configuration parameter.
    pub fn sql_safe_updates(&self) -> bool {
        *self.expect_value(&SQL_SAFE_UPDATES)
    }

    /// Returns the value of the `standard_conforming_strings` configuration
    /// parameter.
    pub fn standard_conforming_strings(&self) -> bool {
        *self.expect_value(&STANDARD_CONFORMING_STRINGS)
    }

    /// Returns the value of the `statement_timeout` configuration parameter.
    pub fn statement_timeout(&self) -> &Duration {
        self.expect_value(&STATEMENT_TIMEOUT)
    }

    /// Returns the value of the `idle_in_transaction_session_timeout` configuration parameter.
    pub fn idle_in_transaction_session_timeout(&self) -> &Duration {
        self.expect_value(&IDLE_IN_TRANSACTION_SESSION_TIMEOUT)
    }

    /// Returns the value of the `timezone` configuration parameter.
    pub fn timezone(&self) -> &TimeZone {
        self.expect_value(&TIMEZONE)
    }

    /// Returns the value of the `transaction_isolation` configuration
    /// parameter.
    pub fn transaction_isolation(&self) -> &IsolationLevel {
        self.expect_value(&TRANSACTION_ISOLATION)
    }

    /// Returns the value of `real_time_recency` configuration parameter.
    pub fn real_time_recency(&self) -> bool {
        *self.expect_value(&REAL_TIME_RECENCY)
    }

    /// Returns the value of `emit_timestamp_notice` configuration parameter.
    pub fn emit_timestamp_notice(&self) -> bool {
        *self.expect_value(&EMIT_TIMESTAMP_NOTICE)
    }

    /// Returns the value of `emit_trace_id_notice` configuration parameter.
    pub fn emit_trace_id_notice(&self) -> bool {
        *self.expect_value(&EMIT_TRACE_ID_NOTICE)
    }

    /// Returns the value of `auto_route_introspection_queries` configuration parameter.
    pub fn auto_route_introspection_queries(&self) -> bool {
        *self.expect_value(&AUTO_ROUTE_INTROSPECTION_QUERIES)
    }

    /// Returns the value of `enable_session_rbac_checks` configuration parameter.
    pub fn enable_session_rbac_checks(&self) -> bool {
        *self.expect_value(&ENABLE_SESSION_RBAC_CHECKS)
    }

    /// Returns the value of `is_superuser` configuration parameter.
    pub fn is_superuser(&self) -> bool {
        self.user.is_superuser()
    }

    /// Returns the user associated with this `SessionVars` instance.
    pub fn user(&self) -> &User {
        &self.user
    }

    /// Sets the external metadata associated with the user.
    pub fn set_external_user_metadata(&mut self, metadata: ExternalUserMetadata) {
        self.user.external_metadata = Some(metadata);
    }

    pub fn set_cluster(&mut self, cluster: String) {
        self.set(None, CLUSTER.name(), VarInput::Flat(&cluster), false)
            .expect("setting cluster from string succeeds");
    }
}

#[derive(Debug, Copy, Clone)]
pub struct ConnectionCounter {
    pub current: u64,
    pub limit: u64,
}

impl ConnectionCounter {
    pub fn new(limit: u64) -> Self {
        ConnectionCounter { current: 0, limit }
    }
}

#[derive(Debug)]
pub enum ConnectionError {
    /// There were too many connections
    TooManyConnections { current: u64, limit: u64 },
}

#[derive(Debug)]
pub struct DropConnection {
    pub active_connection_count: Arc<Mutex<ConnectionCounter>>,
}

impl Drop for DropConnection {
    fn drop(&mut self) {
        let mut connections = self.active_connection_count.lock().expect("lock poisoned");
        assert_ne!(connections.current, 0);
        connections.current -= 1;
    }
}

impl DropConnection {
    pub fn new_connection(
        user: &User,
        active_connection_count: Arc<Mutex<ConnectionCounter>>,
    ) -> Result<Option<Self>, ConnectionError> {
        Ok(if user.limit_max_connections() {
            {
                let mut connections = active_connection_count.lock().expect("lock poisoned");
                if connections.current >= connections.limit {
                    return Err(ConnectionError::TooManyConnections {
                        current: connections.current,
                        limit: connections.limit,
                    });
                }
                connections.current += 1;
            }
            Some(DropConnection {
                active_connection_count,
            })
        } else {
            None
        })
    }
}

/// On disk variables.
///
/// See the `mz_sql::session` module documentation for more details on the
/// Materialize configuration model.
#[derive(Debug)]
pub struct SystemVars {
    /// Allows "unsafe" parameters to be set.
    allow_unsafe: bool,
    vars: BTreeMap<&'static UncasedStr, Box<dyn SystemVarMut>>,
    active_connection_count: Arc<Mutex<ConnectionCounter>>,
}

impl Clone for SystemVars {
    fn clone(&self) -> Self {
        SystemVars {
            allow_unsafe: self.allow_unsafe,
            vars: self.vars.iter().map(|(k, v)| (*k, v.clone_var())).collect(),
            active_connection_count: Arc::clone(&self.active_connection_count),
        }
    }
}

impl Default for SystemVars {
    fn default() -> Self {
        Self::new(Arc::new(Mutex::new(ConnectionCounter::new(0))))
    }
}

impl SystemVars {
    pub fn new(active_connection_count: Arc<Mutex<ConnectionCounter>>) -> Self {
        let vars = SystemVars {
            vars: Default::default(),
            active_connection_count,
            allow_unsafe: false,
        };

        let mut vars = vars
            .with_feature_flags()
            .with_var(&CONFIG_HAS_SYNCED_ONCE)
            .with_var(&MAX_AWS_PRIVATELINK_CONNECTIONS)
            .with_var(&MAX_TABLES)
            .with_var(&MAX_SOURCES)
            .with_var(&MAX_SINKS)
            .with_var(&MAX_MATERIALIZED_VIEWS)
            .with_var(&MAX_CLUSTERS)
            .with_var(&MAX_REPLICAS_PER_CLUSTER)
            .with_value_constrained_var(
                &MAX_CREDIT_CONSUMPTION_RATE,
                ValueConstraint::Domain(&NumericNonNegNonNan),
            )
            .with_var(&MAX_DATABASES)
            .with_var(&MAX_SCHEMAS_PER_DATABASE)
            .with_var(&MAX_OBJECTS_PER_SCHEMA)
            .with_var(&MAX_SECRETS)
            .with_var(&MAX_ROLES)
            .with_var(&MAX_RESULT_SIZE)
            .with_var(&ALLOWED_CLUSTER_REPLICA_SIZES)
            .with_var(&DISK_CLUSTER_REPLICAS_DEFAULT)
            .with_var(&upsert_rocksdb::UPSERT_ROCKSDB_AUTO_SPILL_TO_DISK)
            .with_var(&upsert_rocksdb::UPSERT_ROCKSDB_AUTO_SPILL_THRESHOLD_BYTES)
            .with_var(&upsert_rocksdb::UPSERT_ROCKSDB_COMPACTION_STYLE)
            .with_var(&upsert_rocksdb::UPSERT_ROCKSDB_OPTIMIZE_COMPACTION_MEMTABLE_BUDGET)
            .with_var(&upsert_rocksdb::UPSERT_ROCKSDB_LEVEL_COMPACTION_DYNAMIC_LEVEL_BYTES)
            .with_var(&upsert_rocksdb::UPSERT_ROCKSDB_UNIVERSAL_COMPACTION_RATIO)
            .with_var(&upsert_rocksdb::UPSERT_ROCKSDB_PARALLELISM)
            .with_var(&upsert_rocksdb::UPSERT_ROCKSDB_COMPRESSION_TYPE)
            .with_var(&upsert_rocksdb::UPSERT_ROCKSDB_BOTTOMMOST_COMPRESSION_TYPE)
            .with_var(&upsert_rocksdb::UPSERT_ROCKSDB_BATCH_SIZE)
            .with_var(&upsert_rocksdb::UPSERT_ROCKSDB_RETRY_DURATION)
            .with_var(&PERSIST_BLOB_TARGET_SIZE)
            .with_var(&PERSIST_BLOB_CACHE_MEM_LIMIT_BYTES)
            .with_var(&PERSIST_COMPACTION_MINIMUM_TIMEOUT)
            .with_var(&PERSIST_CONSENSUS_CONNECTION_POOL_TTL)
            .with_var(&PERSIST_CONSENSUS_CONNECTION_POOL_TTL_STAGGER)
            .with_var(&CRDB_CONNECT_TIMEOUT)
            .with_var(&CRDB_TCP_USER_TIMEOUT)
            .with_var(&DATAFLOW_MAX_INFLIGHT_BYTES)
            .with_var(&PERSIST_SINK_MINIMUM_BATCH_UPDATES)
            .with_var(&STORAGE_PERSIST_SINK_MINIMUM_BATCH_UPDATES)
            .with_var(&PERSIST_NEXT_LISTEN_BATCH_RETRYER_INITIAL_BACKOFF)
            .with_var(&PERSIST_NEXT_LISTEN_BATCH_RETRYER_MULTIPLIER)
            .with_var(&PERSIST_NEXT_LISTEN_BATCH_RETRYER_CLAMP)
            .with_var(&PERSIST_STATS_AUDIT_PERCENT)
            .with_var(&PERSIST_STATS_COLLECTION_ENABLED)
            .with_var(&PERSIST_STATS_FILTER_ENABLED)
            .with_var(&PERSIST_PUBSUB_CLIENT_ENABLED)
            .with_var(&PERSIST_PUBSUB_PUSH_DIFF_ENABLED)
            .with_var(&PERSIST_ROLLUP_THRESHOLD)
            .with_var(&METRICS_RETENTION)
            .with_var(&UNSAFE_MOCK_AUDIT_EVENT_TIMESTAMP)
            .with_var(&ENABLE_LD_RBAC_CHECKS)
            .with_var(&ENABLE_RBAC_CHECKS)
            .with_var(&PG_REPLICATION_CONNECT_TIMEOUT)
            .with_var(&PG_REPLICATION_KEEPALIVES_IDLE)
            .with_var(&PG_REPLICATION_KEEPALIVES_INTERVAL)
            .with_var(&PG_REPLICATION_KEEPALIVES_RETRIES)
            .with_var(&PG_REPLICATION_TCP_USER_TIMEOUT)
            .with_var(&ENABLE_LAUNCHDARKLY)
            .with_var(&MAX_CONNECTIONS)
            .with_var(&KEEP_N_SOURCE_STATUS_HISTORY_ENTRIES)
            .with_var(&KEEP_N_SINK_STATUS_HISTORY_ENTRIES)
            .with_var(&ENABLE_MZ_JOIN_CORE)
            .with_var(&ENABLE_STORAGE_SHARD_FINALIZATION)
            .with_var(&ENABLE_DEFAULT_CONNECTION_VALIDATION)
            .with_var(&LOGGING_FILTER)
            .with_var(&OPENTELEMETRY_FILTER);
        vars.refresh_internal_state();
        vars
    }

    fn with_var<V>(mut self, var: &'static ServerVar<V>) -> Self
    where
        V: Value + Debug + PartialEq + Clone + 'static,
        V::Owned: Debug + Send + Clone + Sync,
    {
        self.vars.insert(var.name, Box::new(SystemVar::new(var)));
        self
    }

    pub fn set_unsafe(mut self, allow_unsafe: bool) -> Self {
        self.allow_unsafe = allow_unsafe;
        self
    }

    pub fn allow_unsafe(&self) -> bool {
        self.allow_unsafe
    }

    fn with_value_constrained_var<V>(
        mut self,
        var: &'static ServerVar<V>,
        c: ValueConstraint<V>,
    ) -> Self
    where
        V: Value + Debug + PartialEq + Clone + 'static,
        V::Owned: Debug + Send + Clone + Sync,
    {
        self.vars.insert(
            var.name,
            Box::new(SystemVar::new(var).with_value_constraint(c)),
        );
        self
    }

    fn expect_value<V>(&self, var: &ServerVar<V>) -> &V
    where
        V: Value + Debug + PartialEq + Clone + 'static,
        V::Owned: Debug + Send + Clone + Sync,
    {
        let var = self
            .vars
            .get(var.name)
            .expect("provided var should be in state");

        var.value_any()
            .downcast_ref()
            .expect("provided var type should matched stored var")
    }

    /// Reset all the values to their defaults (preserving
    /// defaults from `VarMut::set_default).
    pub fn reset_all(&mut self) {
        for (_, var) in &mut self.vars {
            var.reset();
        }
    }

    /// Returns an iterator over the configuration parameters and their current
    /// values on disk.
    pub fn iter(&self) -> impl Iterator<Item = &dyn Var> {
        self.vars.values().map(|v| v.as_var())
    }

    /// Returns an iterator over the configuration parameters and their current
    /// values on disk. Compared to [`SystemVars::iter`], this should omit vars
    /// that shouldn't be synced by SystemParameterFrontend.
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
    /// Note that:
    /// - If `name` is known at compile time, you should instead use the named
    /// accessor to access the variable with its true Rust type. For example,
    /// `self.get("max_tables").value()` returns the string `"25"` or the
    /// current value, while `self.max_tables()` returns an i32.
    ///
    /// - This function does not check that the access variable should be
    /// visible because of other settings or users. Before or after accessing
    /// this method, you should call `Var::visible`.
    ///
    /// # Errors
    ///
    /// The call will return an error:
    /// 1. If `name` does not refer to a valid [`SystemVars`] field.
    pub fn get(&self, name: &str) -> Result<&dyn Var, VarError> {
        self.vars
            .get(UncasedStr::new(name))
            .map(|v| v.as_var())
            .ok_or_else(|| VarError::UnknownParameter(name.into()))
    }

    /// Check if the given `values` is the default value for the [`Var`]
    /// identified by `name`.
    ///
    /// Note that this function does not check that the access variable should
    /// be visible because of other settings or users. Before or after accessing
    /// this method, you should call `Var::visible`.
    ///
    /// # Errors
    ///
    /// The call will return an error:
    /// 1. If `name` does not refer to a valid [`SystemVars`] field.
    /// 2. If `values` does not represent a valid [`SystemVars`] value for
    ///    `name`.
    pub fn is_default(&self, name: &str, input: VarInput) -> Result<bool, VarError> {
        self.vars
            .get(UncasedStr::new(name))
            .ok_or_else(|| VarError::UnknownParameter(name.into()))
            .and_then(|v| v.is_default(input))
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
    /// Note that this function does not check that the access variable should
    /// be visible because of other settings or users. Before or after accessing
    /// this method, you should call `Var::visible`.
    ///
    /// # Errors
    ///
    /// The call will return an error:
    /// 1. If `name` does not refer to a valid [`SystemVars`] field.
    /// 2. If `value` does not represent a valid [`SystemVars`] value for
    ///    `name`.
    pub fn set(&mut self, name: &str, input: VarInput) -> Result<bool, VarError> {
        let result = self
            .vars
            .get_mut(UncasedStr::new(name))
            .ok_or_else(|| VarError::UnknownParameter(name.into()))
            .and_then(|v| v.set(input))?;
        self.propagate_var_change(name);
        Ok(result)
    }

    /// Set the default for this variable. This is the value this
    /// variable will be be `reset` to. If no default is set, the static default in the
    /// variable definition is used instead.
    ///
    /// Note that this function does not check that the access variable should
    /// be visible because of other settings or users. Before or after accessing
    /// this method, you should call `Var::visible`.
    pub fn set_default(&mut self, name: &str, input: VarInput) -> Result<(), VarError> {
        let result = self
            .vars
            .get_mut(UncasedStr::new(name))
            .ok_or_else(|| VarError::UnknownParameter(name.into()))
            .and_then(|v| v.set_default(input))?;
        self.propagate_var_change(name);
        Ok(result)
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
    /// Note that this function does not check that the access variable should
    /// be visible because of other settings or users. Before or after accessing
    /// this method, you should call `Var::visible`.
    ///
    /// # Errors
    ///
    /// The call will return an error:
    /// 1. If `name` does not refer to a valid [`SystemVars`] field.
    pub fn reset(&mut self, name: &str) -> Result<bool, VarError> {
        let result = self
            .vars
            .get_mut(UncasedStr::new(name))
            .ok_or_else(|| VarError::UnknownParameter(name.into()))
            .map(|v| v.reset())?;
        self.propagate_var_change(name);
        Ok(result)
    }

    /// Propagate a change to the parameter named `name` to our state.
    fn propagate_var_change(&mut self, name: &str) {
        if name == MAX_CONNECTIONS.name {
            self.active_connection_count
                .lock()
                .expect("lock poisoned")
                .limit = u64::cast_from(*self.expect_value(&MAX_CONNECTIONS));
        }
    }

    /// Make sure that the internal state matches the SystemVars. Generally
    /// only needed when initializing, `set`, `set_default`, and `reset`
    /// are responsible for keeping the internal state in sync with
    /// the affected SystemVars.
    fn refresh_internal_state(&mut self) {
        self.propagate_var_change(MAX_CONNECTIONS.name.as_str());
    }

    /// Returns the `config_has_synced_once` configuration parameter.
    pub fn config_has_synced_once(&self) -> bool {
        *self.expect_value(&CONFIG_HAS_SYNCED_ONCE)
    }

    /// Returns the value of the `max_aws_privatelink_connections` configuration parameter.
    pub fn max_aws_privatelink_connections(&self) -> u32 {
        *self.expect_value(&MAX_AWS_PRIVATELINK_CONNECTIONS)
    }

    /// Returns the value of the `max_tables` configuration parameter.
    pub fn max_tables(&self) -> u32 {
        *self.expect_value(&MAX_TABLES)
    }

    /// Returns the value of the `max_sources` configuration parameter.
    pub fn max_sources(&self) -> u32 {
        *self.expect_value(&MAX_SOURCES)
    }

    /// Returns the value of the `max_sinks` configuration parameter.
    pub fn max_sinks(&self) -> u32 {
        *self.expect_value(&MAX_SINKS)
    }

    /// Returns the value of the `max_materialized_views` configuration parameter.
    pub fn max_materialized_views(&self) -> u32 {
        *self.expect_value(&MAX_MATERIALIZED_VIEWS)
    }

    /// Returns the value of the `max_clusters` configuration parameter.
    pub fn max_clusters(&self) -> u32 {
        *self.expect_value(&MAX_CLUSTERS)
    }

    /// Returns the value of the `max_replicas_per_cluster` configuration parameter.
    pub fn max_replicas_per_cluster(&self) -> u32 {
        *self.expect_value(&MAX_REPLICAS_PER_CLUSTER)
    }

    /// Returns the value of the `max_credit_consumption_rate` configuration parameter.
    pub fn max_credit_consumption_rate(&self) -> Numeric {
        *self.expect_value(&MAX_CREDIT_CONSUMPTION_RATE)
    }

    /// Returns the value of the `max_databases` configuration parameter.
    pub fn max_databases(&self) -> u32 {
        *self.expect_value(&MAX_DATABASES)
    }

    /// Returns the value of the `max_schemas_per_database` configuration parameter.
    pub fn max_schemas_per_database(&self) -> u32 {
        *self.expect_value(&MAX_SCHEMAS_PER_DATABASE)
    }

    /// Returns the value of the `max_objects_per_schema` configuration parameter.
    pub fn max_objects_per_schema(&self) -> u32 {
        *self.expect_value(&MAX_OBJECTS_PER_SCHEMA)
    }

    /// Returns the value of the `max_secrets` configuration parameter.
    pub fn max_secrets(&self) -> u32 {
        *self.expect_value(&MAX_SECRETS)
    }

    /// Returns the value of the `max_roles` configuration parameter.
    pub fn max_roles(&self) -> u32 {
        *self.expect_value(&MAX_ROLES)
    }

    /// Returns the value of the `max_result_size` configuration parameter.
    pub fn max_result_size(&self) -> u32 {
        *self.expect_value(&MAX_RESULT_SIZE)
    }

    /// Returns the value of the `allowed_cluster_replica_sizes` configuration parameter.
    pub fn allowed_cluster_replica_sizes(&self) -> Vec<String> {
        self.expect_value(&ALLOWED_CLUSTER_REPLICA_SIZES)
            .into_iter()
            .map(|s| s.as_str().into())
            .collect()
    }

    /// Returns the `disk_cluster_replicas_default` configuration parameter.
    pub fn disk_cluster_replicas_default(&self) -> bool {
        *self.expect_value(&DISK_CLUSTER_REPLICAS_DEFAULT)
    }

    pub fn upsert_rocksdb_auto_spill_to_disk(&self) -> bool {
        *self.expect_value(&upsert_rocksdb::UPSERT_ROCKSDB_AUTO_SPILL_TO_DISK)
    }

    pub fn upsert_rocksdb_auto_spill_threshold_bytes(&self) -> usize {
        *self.expect_value(&upsert_rocksdb::UPSERT_ROCKSDB_AUTO_SPILL_THRESHOLD_BYTES)
    }

    pub fn upsert_rocksdb_compaction_style(&self) -> mz_rocksdb_types::config::CompactionStyle {
        *self.expect_value(&upsert_rocksdb::UPSERT_ROCKSDB_COMPACTION_STYLE)
    }

    pub fn upsert_rocksdb_optimize_compaction_memtable_budget(&self) -> usize {
        *self.expect_value(&upsert_rocksdb::UPSERT_ROCKSDB_OPTIMIZE_COMPACTION_MEMTABLE_BUDGET)
    }

    pub fn upsert_rocksdb_level_compaction_dynamic_level_bytes(&self) -> bool {
        *self.expect_value(&upsert_rocksdb::UPSERT_ROCKSDB_LEVEL_COMPACTION_DYNAMIC_LEVEL_BYTES)
    }

    pub fn upsert_rocksdb_universal_compaction_ratio(&self) -> i32 {
        *self.expect_value(&upsert_rocksdb::UPSERT_ROCKSDB_UNIVERSAL_COMPACTION_RATIO)
    }

    pub fn upsert_rocksdb_parallelism(&self) -> Option<i32> {
        *self.expect_value(&upsert_rocksdb::UPSERT_ROCKSDB_PARALLELISM)
    }

    pub fn upsert_rocksdb_compression_type(&self) -> mz_rocksdb_types::config::CompressionType {
        *self.expect_value(&upsert_rocksdb::UPSERT_ROCKSDB_COMPRESSION_TYPE)
    }

    pub fn upsert_rocksdb_bottommost_compression_type(
        &self,
    ) -> mz_rocksdb_types::config::CompressionType {
        *self.expect_value(&upsert_rocksdb::UPSERT_ROCKSDB_BOTTOMMOST_COMPRESSION_TYPE)
    }

    pub fn upsert_rocksdb_batch_size(&self) -> usize {
        *self.expect_value(&upsert_rocksdb::UPSERT_ROCKSDB_BATCH_SIZE)
    }

    pub fn upsert_rocksdb_retry_duration(&self) -> Duration {
        *self.expect_value(&upsert_rocksdb::UPSERT_ROCKSDB_RETRY_DURATION)
    }

    /// Returns the `persist_blob_target_size` configuration parameter.
    pub fn persist_blob_target_size(&self) -> usize {
        *self.expect_value(&PERSIST_BLOB_TARGET_SIZE)
    }

    /// Returns the `persist_blob_cache_mem_limit_bytes` configuration parameter.
    pub fn persist_blob_cache_mem_limit_bytes(&self) -> usize {
        *self.expect_value(&PERSIST_BLOB_CACHE_MEM_LIMIT_BYTES)
    }

    /// Returns the `persist_next_listen_batch_retryer_initial_backoff` configuration parameter.
    pub fn persist_next_listen_batch_retryer_initial_backoff(&self) -> Duration {
        *self.expect_value(&PERSIST_NEXT_LISTEN_BATCH_RETRYER_INITIAL_BACKOFF)
    }

    /// Returns the `persist_next_listen_batch_retryer_multiplier` configuration parameter.
    pub fn persist_next_listen_batch_retryer_multiplier(&self) -> u32 {
        *self.expect_value(&PERSIST_NEXT_LISTEN_BATCH_RETRYER_MULTIPLIER)
    }

    /// Returns the `persist_next_listen_batch_retryer_clamp` configuration parameter.
    pub fn persist_next_listen_batch_retryer_clamp(&self) -> Duration {
        *self.expect_value(&PERSIST_NEXT_LISTEN_BATCH_RETRYER_CLAMP)
    }

    /// Returns the `persist_compaction_minimum_timeout` configuration parameter.
    pub fn persist_compaction_minimum_timeout(&self) -> Duration {
        *self.expect_value(&PERSIST_COMPACTION_MINIMUM_TIMEOUT)
    }

    /// Returns the `persist_consensus_connection_pool_ttl` configuration parameter.
    pub fn persist_consensus_connection_pool_ttl(&self) -> Duration {
        *self.expect_value(&PERSIST_CONSENSUS_CONNECTION_POOL_TTL)
    }

    /// Returns the `persist_consensus_connection_pool_ttl_stagger` configuration parameter.
    pub fn persist_consensus_connection_pool_ttl_stagger(&self) -> Duration {
        *self.expect_value(&PERSIST_CONSENSUS_CONNECTION_POOL_TTL_STAGGER)
    }

    /// Returns the `pg_replication_connect_timeout` configuration parameter.
    pub fn pg_replication_connect_timeout(&self) -> Duration {
        *self.expect_value(&PG_REPLICATION_CONNECT_TIMEOUT)
    }

    /// Returns the `pg_replication_keepalives_retries` configuration parameter.
    pub fn pg_replication_keepalives_retries(&self) -> u32 {
        *self.expect_value(&PG_REPLICATION_KEEPALIVES_RETRIES)
    }

    /// Returns the `pg_replication_keepalives_idle` configuration parameter.
    pub fn pg_replication_keepalives_idle(&self) -> Duration {
        *self.expect_value(&PG_REPLICATION_KEEPALIVES_IDLE)
    }

    /// Returns the `pg_replication_keepalives_interval` configuration parameter.
    pub fn pg_replication_keepalives_interval(&self) -> Duration {
        *self.expect_value(&PG_REPLICATION_KEEPALIVES_INTERVAL)
    }

    /// Returns the `pg_replication_tcp_user_timeout` configuration parameter.
    pub fn pg_replication_tcp_user_timeout(&self) -> Duration {
        *self.expect_value(&PG_REPLICATION_TCP_USER_TIMEOUT)
    }

    /// Returns the `crdb_connect_timeout` configuration parameter.
    pub fn crdb_connect_timeout(&self) -> Duration {
        *self.expect_value(&CRDB_CONNECT_TIMEOUT)
    }

    /// Returns the `crdb_tcp_user_timeout` configuration parameter.
    pub fn crdb_tcp_user_timeout(&self) -> Duration {
        *self.expect_value(&CRDB_TCP_USER_TIMEOUT)
    }

    /// Returns the `dataflow_max_inflight_bytes` configuration parameter.
    pub fn dataflow_max_inflight_bytes(&self) -> usize {
        *self.expect_value(&DATAFLOW_MAX_INFLIGHT_BYTES)
    }

    /// Returns the `persist_sink_minimum_batch_updates` configuration parameter.
    pub fn persist_sink_minimum_batch_updates(&self) -> usize {
        *self.expect_value(&PERSIST_SINK_MINIMUM_BATCH_UPDATES)
    }

    /// Returns the `storage_persist_sink_minimum_batch_updates` configuration parameter.
    pub fn storage_persist_sink_minimum_batch_updates(&self) -> usize {
        *self.expect_value(&STORAGE_PERSIST_SINK_MINIMUM_BATCH_UPDATES)
    }

    /// Returns the `persist_stats_audit_percent` configuration parameter.
    pub fn persist_stats_audit_percent(&self) -> usize {
        *self.expect_value(&PERSIST_STATS_AUDIT_PERCENT)
    }

    /// Returns the `persist_stats_collection_enabled` configuration parameter.
    pub fn persist_stats_collection_enabled(&self) -> bool {
        *self.expect_value(&PERSIST_STATS_COLLECTION_ENABLED)
    }

    /// Returns the `persist_stats_filter_enabled` configuration parameter.
    pub fn persist_stats_filter_enabled(&self) -> bool {
        *self.expect_value(&PERSIST_STATS_FILTER_ENABLED)
    }

    /// Returns the `persist_pubsub_client_enabled` configuration parameter.
    pub fn persist_pubsub_client_enabled(&self) -> bool {
        *self.expect_value(&PERSIST_PUBSUB_CLIENT_ENABLED)
    }

    /// Returns the `persist_pubsub_push_diff_enabled` configuration parameter.
    pub fn persist_pubsub_push_diff_enabled(&self) -> bool {
        *self.expect_value(&PERSIST_PUBSUB_PUSH_DIFF_ENABLED)
    }

    /// Returns the `persist_rollup_threshold` configuration parameter.
    pub fn persist_rollup_threshold(&self) -> usize {
        *self.expect_value(&PERSIST_ROLLUP_THRESHOLD)
    }

    /// Returns the `metrics_retention` configuration parameter.
    pub fn metrics_retention(&self) -> Duration {
        *self.expect_value(&METRICS_RETENTION)
    }

    /// Returns the `unsafe_mock_audit_event_timestamp` configuration parameter.
    pub fn unsafe_mock_audit_event_timestamp(&self) -> Option<mz_repr::Timestamp> {
        *self.expect_value(&UNSAFE_MOCK_AUDIT_EVENT_TIMESTAMP)
    }

    /// Returns the `enable_ld_rbac_checks` configuration parameter.
    pub fn enable_ld_rbac_checks(&self) -> bool {
        *self.expect_value(&ENABLE_LD_RBAC_CHECKS)
    }

    /// Returns the `enable_rbac_checks` configuration parameter.
    pub fn enable_rbac_checks(&self) -> bool {
        *self.expect_value(&ENABLE_RBAC_CHECKS)
    }

    /// Returns the `max_connections` configuration parameter.
    pub fn max_connections(&self) -> u32 {
        *self.expect_value(&MAX_CONNECTIONS)
    }

    pub fn keep_n_source_status_history_entries(&self) -> usize {
        *self.expect_value(&KEEP_N_SOURCE_STATUS_HISTORY_ENTRIES)
    }

    pub fn keep_n_sink_status_history_entries(&self) -> usize {
        *self.expect_value(&KEEP_N_SINK_STATUS_HISTORY_ENTRIES)
    }

    /// Returns the `enable_mz_join_core` configuration parameter.
    pub fn enable_mz_join_core(&self) -> bool {
        *self.expect_value(&ENABLE_MZ_JOIN_CORE)
    }

    /// Returns the `enable_storage_shard_finalization` configuration parameter.
    pub fn enable_storage_shard_finalization(&self) -> bool {
        *self.expect_value(&ENABLE_STORAGE_SHARD_FINALIZATION)
    }

    /// Returns the `enable_default_connection_validation` configuration parameter.
    pub fn enable_default_connection_validation(&self) -> bool {
        *self.expect_value(&ENABLE_DEFAULT_CONNECTION_VALIDATION)
    }

    pub fn logging_filter(&self) -> CloneableEnvFilter {
        self.expect_value(&*LOGGING_FILTER).clone()
    }

    pub fn opentelemetry_filter(&self) -> CloneableEnvFilter {
        self.expect_value(&*OPENTELEMETRY_FILTER).clone()
    }
}

/// A `Var` represents a configuration parameter of an arbitrary type.
pub trait Var: Debug {
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
    fn type_name(&self) -> String;

    /// Indicates wither the [`Var`] is visible as a function of the `user` and `system_vars`.
    /// "Invisible" parameters return `VarErrors`.
    ///
    /// Variables marked as `internal` are only visible for the system user.
    fn visible(&self, user: &User, system_vars: Option<&SystemVars>) -> Result<(), VarError>;
}

/// A `Var` with additional methods for mutating the value, as well as
/// helpers that enable various operations in a `dyn` context.
pub trait SystemVarMut: Var + Send + Sync {
    /// Upcast to Var, for use with `dyn`.
    fn as_var(&self) -> &dyn Var;

    /// Return the value as `dyn Any`.
    fn value_any(&self) -> &(dyn Any + 'static);

    /// Clone, but object safe and specialized to `VarMut`.
    fn clone_var(&self) -> Box<dyn SystemVarMut>;

    /// Return whether or not `input` is equal to this var's default value,
    /// if there is one.
    fn is_default(&self, input: VarInput) -> Result<bool, VarError>;

    /// Parse the input and update the stored value to match.
    fn set(&mut self, input: VarInput) -> Result<bool, VarError>;

    /// Reset the stored value to the default.
    fn reset(&mut self) -> bool;

    /// Set the default for this variable. This is the value this
    /// variable will be be `reset` to.
    fn set_default(&mut self, input: VarInput) -> Result<(), VarError>;
}

/// A `ServerVar` is the default value for a configuration parameter.
#[derive(Debug)]
pub struct ServerVar<V>
where
    V: Debug + 'static,
{
    name: &'static UncasedStr,
    value: &'static V,
    description: &'static str,
    internal: bool,
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
        if self.internal && user != &*SYSTEM_USER {
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

/// A `SystemVar` is persisted on disk value for a configuration parameter. If unset,
/// the server default is used instead.
#[derive(Debug)]
struct SystemVar<V>
where
    V: Value + Debug + PartialEq + 'static,
    V::Owned: Debug + Clone + Send + Sync,
{
    persisted_value: Option<V::Owned>,
    dynamic_default: Option<V::Owned>,
    parent: &'static ServerVar<V>,
    constraints: Vec<ValueConstraint<V>>,
}

// The derived `Clone` implementation requires `V: Clone`, which is not needed.
impl<V> Clone for SystemVar<V>
where
    V: Value + Debug + PartialEq + 'static,
    V::Owned: Debug + Clone + Send + Sync,
{
    fn clone(&self) -> Self {
        SystemVar {
            persisted_value: self.persisted_value.clone(),
            dynamic_default: self.dynamic_default.clone(),
            parent: self.parent,
            constraints: self.constraints.clone(),
        }
    }
}

impl<V> SystemVar<V>
where
    V: Value + Debug + PartialEq + 'static,
    V::Owned: Debug + Clone + Send + Sync,
{
    fn new(parent: &'static ServerVar<V>) -> SystemVar<V> {
        SystemVar {
            persisted_value: None,
            dynamic_default: None,
            parent,
            constraints: vec![],
        }
    }

    fn with_value_constraint(mut self, c: ValueConstraint<V>) -> SystemVar<V> {
        assert!(
            !self
                .constraints
                .iter()
                .any(|c| matches!(c, ValueConstraint::ReadOnly | ValueConstraint::Fixed)),
            "fixed value and read only params do not support any other constraints"
        );
        self.constraints.push(c);
        self
    }

    fn check_constraints(&self, v: &V::Owned) -> Result<(), VarError> {
        let cur_v = self.value();
        for constraint in &self.constraints {
            constraint.check_constraint(self, cur_v, v)?;
        }

        Ok(())
    }

    fn persisted_value(&self) -> Option<&V> {
        self.persisted_value.as_ref().map(|v| v.borrow())
    }

    fn value(&self) -> &V {
        self.persisted_value
            .as_ref()
            .map(|v| v.borrow())
            .unwrap_or_else(|| {
                self.dynamic_default
                    .as_ref()
                    .map(|v| v.borrow())
                    .unwrap_or(self.parent.value)
            })
    }
}

impl<V> Var for SystemVar<V>
where
    V: Value + Debug + PartialEq + 'static,
    V::Owned: Debug + Clone + Send + Sync,
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

    fn type_name(&self) -> String {
        V::type_name()
    }

    fn visible(&self, user: &User, system_vars: Option<&SystemVars>) -> Result<(), VarError> {
        self.parent.visible(user, system_vars)
    }
}

impl<V> SystemVarMut for SystemVar<V>
where
    V: Value + Debug + PartialEq + 'static,
    V::Owned: Debug + Clone + Send + Sync,
{
    fn as_var(&self) -> &dyn Var {
        self
    }

    fn value_any(&self) -> &(dyn Any + 'static) {
        let value = SystemVar::value(self);
        value
    }

    fn clone_var(&self) -> Box<dyn SystemVarMut> {
        Box::new(self.clone())
    }

    fn is_default(&self, input: VarInput) -> Result<bool, VarError> {
        let v = V::parse(self, input)?;
        Ok(self.parent.value == v.borrow())
    }

    fn set(&mut self, input: VarInput) -> Result<bool, VarError> {
        let v = V::parse(self, input)?;

        self.check_constraints(&v)?;

        if self.persisted_value() != Some(v.borrow()) {
            self.persisted_value = Some(v);
            Ok(true)
        } else {
            Ok(false)
        }
    }

    fn reset(&mut self) -> bool {
        if self.persisted_value() != None {
            self.persisted_value = None;
            true
        } else {
            false
        }
    }

    fn set_default(&mut self, input: VarInput) -> Result<(), VarError> {
        let v = V::parse(self, input)?;
        self.dynamic_default = Some(v);
        Ok(())
    }
}

// Provides a wrapper to express that a particular `ServerVar` is meant to be used as a feature
/// flag.
#[derive(Debug)]
pub struct FeatureFlag {
    flag: &'static ServerVar<bool>,
    feature_desc: &'static str,
}

impl Var for FeatureFlag {
    fn name(&self) -> &'static str {
        self.flag.name()
    }

    fn value(&self) -> String {
        self.flag.value()
    }

    fn description(&self) -> &'static str {
        self.flag.description()
    }

    fn type_name(&self) -> String {
        self.flag.type_name()
    }

    fn visible(&self, user: &User, system_vars: Option<&SystemVars>) -> Result<(), VarError> {
        self.flag.visible(user, system_vars)
    }
}

impl FeatureFlag {
    pub fn enabled(
        &self,
        system_vars: Option<&SystemVars>,
        feature: Option<String>,
        detail: Option<String>,
    ) -> Result<(), VarError> {
        match system_vars {
            Some(system_vars) if *system_vars.expect_value(self.flag) => Ok(()),
            _ => Err(VarError::RequiresFeatureFlag {
                feature: feature.unwrap_or(self.feature_desc.to_string()),
                detail,
                name_hint: system_vars
                    .map(|s| {
                        if s.allow_unsafe {
                            Some(self.flag.name)
                        } else {
                            None
                        }
                    })
                    .flatten(),
            }),
        }
    }
}

/// A `SessionVar` is the session value for a configuration parameter. If unset,
/// the server default is used instead.
#[derive(Debug)]
struct SessionVar<V>
where
    V: Value + ToOwned + Debug + PartialEq + 'static,
{
    default_value: &'static V,
    local_value: Option<V::Owned>,
    staged_value: Option<V::Owned>,
    session_value: Option<V::Owned>,
    parent: &'static ServerVar<V>,
    feature_flag: Option<&'static FeatureFlag>,
    constraints: Vec<ValueConstraint<V>>,
}

#[derive(Debug)]
enum ValueConstraint<V>
where
    V: Value + ToOwned + Debug + PartialEq + 'static,
{
    Fixed,
    ReadOnly,
    // Arbitrary constraints over values.
    Domain(&'static dyn DomainConstraint<V>),
}

impl<V> ValueConstraint<V>
where
    V: Value + ToOwned + Debug + PartialEq + 'static,
{
    fn check_constraint(
        &self,
        var: &(dyn Var + Send + Sync),
        cur_value: &V,
        new_value: &V::Owned,
    ) -> Result<(), VarError> {
        match self {
            ValueConstraint::ReadOnly => return Err(VarError::ReadOnlyParameter(var.name())),
            ValueConstraint::Fixed => {
                if cur_value != new_value.borrow() {
                    return Err(VarError::FixedValueParameter(var.into()));
                }
            }
            ValueConstraint::Domain(check) => check.check(var, new_value)?,
        }

        Ok(())
    }
}

impl<V> Clone for ValueConstraint<V>
where
    V: Value + ToOwned + Debug + PartialEq + 'static,
{
    fn clone(&self) -> Self {
        match self {
            ValueConstraint::Fixed => ValueConstraint::Fixed,
            ValueConstraint::ReadOnly => ValueConstraint::ReadOnly,
            ValueConstraint::Domain(c) => ValueConstraint::Domain(*c),
        }
    }
}

trait DomainConstraint<V>: Debug + Send + Sync
where
    V: Value + Debug + PartialEq + 'static,
{
    // `self` is make a trait object
    fn check(&self, var: &(dyn Var + Send + Sync), v: &V::Owned) -> Result<(), VarError>;
}

impl<V> SessionVar<V>
where
    V: Value + ToOwned + Debug + PartialEq + 'static,
    V::Owned: Debug + Send + Sync,
{
    fn new(parent: &'static ServerVar<V>) -> SessionVar<V> {
        SessionVar {
            default_value: parent.value,
            local_value: None,
            staged_value: None,
            session_value: None,
            parent,
            feature_flag: None,
            constraints: vec![],
        }
    }

    fn add_feature_flag(mut self, flag: &'static FeatureFlag) -> Self {
        self.feature_flag = Some(flag);
        self
    }

    fn with_value_constraint(mut self, c: ValueConstraint<V>) -> SessionVar<V> {
        assert!(
            !self
                .constraints
                .iter()
                .any(|c| matches!(c, ValueConstraint::ReadOnly | ValueConstraint::Fixed)),
            "fixed value and read only params do not support any other constraints"
        );
        self.constraints.push(c);
        self
    }

    fn check_constraints(&self, v: &V::Owned) -> Result<(), VarError> {
        let cur_v = self.value();
        for constraint in &self.constraints {
            constraint.check_constraint(self, cur_v, v)?;
        }

        Ok(())
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
    V: Value + ToOwned + Debug + PartialEq + 'static,
    V::Owned: Debug + Send + Sync,
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

    fn type_name(&self) -> String {
        V::type_name()
    }

    fn visible(&self, user: &User, system_vars: Option<&SystemVars>) -> Result<(), VarError> {
        if let Some(flag) = self.feature_flag {
            flag.enabled(system_vars, None, None)?;
        }

        self.parent.visible(user, system_vars)
    }
}

/// A `Var` with additional methods for mutating the value, as well as
/// helpers that enable various operations in a `dyn` context.
pub trait SessionVarMut: Var + Send + Sync {
    /// Upcast to Var, for use with `dyn`.
    fn as_var(&self) -> &dyn Var;

    fn value_any(&self) -> &(dyn Any + 'static);

    /// Parse the input and update the stored value to match.
    fn set(&mut self, input: VarInput, local: bool) -> Result<(), VarError>;

    /// Reset the stored value to the default.
    fn reset(&mut self, local: bool);

    fn end_transaction(&mut self, action: EndTransactionAction);
}

impl<V> SessionVarMut for SessionVar<V>
where
    V: Value + Debug + PartialEq + 'static,
    V::Owned: Debug + Send + Sync + PartialEq,
{
    /// Upcast to Var, for use with `dyn`.
    fn as_var(&self) -> &dyn Var {
        self
    }

    fn value_any(&self) -> &(dyn Any + 'static) {
        let value = SessionVar::value(self);
        value
    }

    /// Parse the input and update the stored value to match.
    fn set(&mut self, input: VarInput, local: bool) -> Result<(), VarError> {
        let v = V::parse(self, input)?;

        self.check_constraints(&v)?;

        if local {
            self.local_value = Some(v);
        } else {
            self.local_value = None;
            self.staged_value = Some(v);
        }
        Ok(())
    }

    /// Reset the stored value to the default.
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
}

impl Var for BuildInfo {
    fn name(&self) -> &'static str {
        MZ_VERSION_NAME.as_str()
    }

    fn value(&self) -> String {
        self.human_version()
    }

    fn description(&self) -> &'static str {
        "Shows the Materialize server version (Materialize)."
    }

    fn type_name(&self) -> String {
        String::type_name()
    }

    fn visible(&self, _: &User, _: Option<&SystemVars>) -> Result<(), VarError> {
        Ok(())
    }
}

impl Var for User {
    fn name(&self) -> &'static str {
        IS_SUPERUSER_NAME.as_str()
    }

    fn value(&self) -> String {
        self.is_superuser().format()
    }

    fn description(&self) -> &'static str {
        "Reports whether the current session is a superuser (PostgreSQL)."
    }

    fn type_name(&self) -> String {
        bool::type_name()
    }

    fn visible(&self, _: &User, _: Option<&SystemVars>) -> Result<(), VarError> {
        Ok(())
    }
}

/// A value that can be stored in a session or server variable.
pub trait Value: ToOwned + Send + Sync {
    /// The name of the value type.
    fn type_name() -> String;

    /// Parses a value of this type from a [`VarInput`].
    fn parse<'a>(
        param: &'a (dyn Var + Send + Sync),
        input: VarInput,
    ) -> Result<Self::Owned, VarError>;
    /// Formats this value as a flattened string.
    ///
    /// The resulting string is guaranteed to be parsable if provided to
    /// [`Value::parse`] as a [`VarInput::Flat`].
    fn format(&self) -> String;
}

fn extract_single_value<'var, 'input: 'var>(
    param: &'var (dyn Var + Send + Sync),
    input: VarInput<'input>,
) -> Result<&'input str, VarError> {
    match input {
        VarInput::Flat(value) => Ok(value),
        VarInput::SqlSet([value]) => Ok(value),
        VarInput::SqlSet(values) => Err(VarError::InvalidParameterValue {
            parameter: param.into(),
            values: values.to_vec(),
            reason: "expects a single value".to_string(),
        }),
    }
}

impl Value for bool {
    fn type_name() -> String {
        "boolean".to_string()
    }

    fn parse<'a>(param: &'a (dyn Var + Send + Sync), input: VarInput) -> Result<Self, VarError> {
        let s = extract_single_value(param, input)?;
        match s {
            "t" | "true" | "on" => Ok(true),
            "f" | "false" | "off" => Ok(false),
            _ => Err(VarError::InvalidParameterType(param.into())),
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
    fn type_name() -> String {
        "integer".to_string()
    }

    fn parse<'a>(param: &'a (dyn Var + Send + Sync), input: VarInput) -> Result<i32, VarError> {
        let s = extract_single_value(param, input)?;
        s.parse()
            .map_err(|_| VarError::InvalidParameterType(param.into()))
    }

    fn format(&self) -> String {
        self.to_string()
    }
}

impl Value for u32 {
    fn type_name() -> String {
        "unsigned integer".to_string()
    }

    fn parse<'a>(param: &'a (dyn Var + Send + Sync), input: VarInput) -> Result<u32, VarError> {
        let s = extract_single_value(param, input)?;
        s.parse()
            .map_err(|_| VarError::InvalidParameterType(param.into()))
    }

    fn format(&self) -> String {
        self.to_string()
    }
}

impl Value for mz_repr::Timestamp {
    fn type_name() -> String {
        "mz-timestamp".to_string()
    }

    fn parse<'a>(
        param: &'a (dyn Var + Send + Sync),
        input: VarInput,
    ) -> Result<mz_repr::Timestamp, VarError> {
        let s = extract_single_value(param, input)?;
        s.parse()
            .map_err(|_| VarError::InvalidParameterType(param.into()))
    }

    fn format(&self) -> String {
        self.to_string()
    }
}

impl Value for usize {
    fn type_name() -> String {
        "unsigned integer".to_string()
    }

    fn parse<'a>(param: &'a (dyn Var + Send + Sync), input: VarInput) -> Result<usize, VarError> {
        let s = extract_single_value(param, input)?;
        s.parse()
            .map_err(|_| VarError::InvalidParameterType(param.into()))
    }

    fn format(&self) -> String {
        self.to_string()
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
struct NumericNonNegNonNan;

impl DomainConstraint<Numeric> for NumericNonNegNonNan {
    fn check(&self, var: &(dyn Var + Send + Sync), n: &Numeric) -> Result<(), VarError> {
        if n.is_nan() || n.is_negative() {
            Err(VarError::InvalidParameterValue {
                parameter: var.into(),
                values: vec![n.to_string()],
                reason: "only supports non-negative, non-NaN numeric values".to_string(),
            })
        } else {
            Ok(())
        }
    }
}

impl Value for Numeric {
    fn type_name() -> String {
        "numeric".to_string()
    }

    fn parse<'a>(
        param: &'a (dyn Var + Send + Sync),
        input: VarInput,
    ) -> Result<Self::Owned, VarError> {
        let s = extract_single_value(param, input)?;
        let n = s
            .parse()
            .map_err(|_| VarError::InvalidParameterType(param.into()))?;
        Ok(n)
    }

    fn format(&self) -> String {
        self.to_standard_notation_string()
    }
}

const SEC_TO_MIN: u64 = 60u64;
const SEC_TO_HOUR: u64 = 60u64 * 60;
const SEC_TO_DAY: u64 = 60u64 * 60 * 24;
const MICRO_TO_MILLI: u32 = 1000u32;

impl Value for Duration {
    fn type_name() -> String {
        "duration".to_string()
    }

    fn parse<'a>(
        param: &'a (dyn Var + Send + Sync),
        input: VarInput,
    ) -> Result<Duration, VarError> {
        let s = extract_single_value(param, input)?;
        let s = s.trim();
        // Take all numeric values from [0..]
        let split_pos = s
            .chars()
            .position(|p| !char::is_numeric(p))
            .unwrap_or_else(|| s.chars().count());

        // Error if the numeric values don't parse, i.e. there aren't any.
        let d = s[..split_pos]
            .parse::<u64>()
            .map_err(|_| VarError::InvalidParameterType(param.into()))?;

        // We've already trimmed end
        let (f, m): (fn(u64) -> Duration, u64) = match s[split_pos..].trim_start() {
            "us" => (Duration::from_micros, 1),
            // Default unit is milliseconds
            "ms" | "" => (Duration::from_millis, 1),
            "s" => (Duration::from_secs, 1),
            "min" => (Duration::from_secs, SEC_TO_MIN),
            "h" => (Duration::from_secs, SEC_TO_HOUR),
            "d" => (Duration::from_secs, SEC_TO_DAY),
            o => {
                return Err(VarError::InvalidParameterValue {
                    parameter: param.into(),
                    values: vec![s.to_string()],
                    reason: format!("expected us, ms, s, min, h, or d but got {:?}", o),
                })
            }
        };

        let d = if d == 0 {
            Duration::from_secs(u64::MAX)
        } else {
            f(d.checked_mul(m).ok_or(VarError::InvalidParameterValue {
                parameter: param.into(),
                values: vec![s.to_string()],
                reason: "expected value to fit in u64".to_string(),
            })?)
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

#[mz_ore::test]
fn test_value_duration() {
    fn inner(t: &'static str, e: Duration, expected_format: Option<&'static str>) {
        let d = Duration::parse(&STATEMENT_TIMEOUT, VarInput::Flat(t)).expect("invalid duration");
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
        assert!(Duration::parse(&STATEMENT_TIMEOUT, VarInput::Flat(t)).is_err());
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

impl Value for String {
    fn type_name() -> String {
        "string".to_string()
    }

    fn parse<'a>(param: &'a (dyn Var + Send + Sync), input: VarInput) -> Result<String, VarError> {
        let s = extract_single_value(param, input)?;
        Ok(s.to_owned())
    }

    fn format(&self) -> String {
        self.to_owned()
    }
}

/// This style should actually be some more complex struct, but we only support this configuration
/// of it, so this is fine for the time being.
#[derive(Debug, Clone, Eq, PartialEq)]
struct DateStyle([&'static str; 2]);

const DEFAULT_DATE_STYLE: DateStyle = DateStyle(["ISO", "MDY"]);

impl Value for DateStyle {
    fn type_name() -> String {
        "string list".to_string()
    }

    /// This impl is unlike most others because we have under-implemented its backing struct.
    fn parse<'a>(
        param: &'a (dyn Var + Send + Sync),
        input: VarInput,
    ) -> Result<DateStyle, VarError> {
        let input = match input {
            VarInput::Flat(v) => mz_sql_parser::parser::split_identifier_string(v)
                .map_err(|_| VarError::InvalidParameterType(param.into()))?,
            // Unlike parsing `Vec<Ident>`, we further split each element.
            // This matches PostgreSQL.
            VarInput::SqlSet(values) => {
                let mut out = vec![];
                for v in values {
                    let idents = mz_sql_parser::parser::split_identifier_string(v)
                        .map_err(|_| VarError::InvalidParameterType(param.into()))?;
                    out.extend(idents)
                }
                out
            }
        };

        for input in input {
            if !DEFAULT_DATE_STYLE
                .0
                .iter()
                .any(|valid| UncasedStr::new(valid) == &input)
            {
                return Err(VarError::FixedValueParameter((&DATE_STYLE).into()));
            }
        }

        Ok(DEFAULT_DATE_STYLE.clone())
    }

    fn format(&self) -> String {
        self.0.join(", ")
    }
}

impl Value for Vec<Ident> {
    fn type_name() -> String {
        "identifier list".to_string()
    }

    fn parse<'a>(
        param: &'a (dyn Var + Send + Sync),
        input: VarInput,
    ) -> Result<Vec<Ident>, VarError> {
        let holder;
        let values = match input {
            VarInput::Flat(value) => {
                holder = mz_sql_parser::parser::split_identifier_string(value)
                    .map_err(|_| VarError::InvalidParameterType(param.into()))?;
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

// Implement `Value` for `Option<V>` for any owned `V`.
impl<V> Value for Option<V>
where
    V: Value + Clone + ToOwned<Owned = V>,
{
    fn type_name() -> String {
        format!("optional {}", V::type_name())
    }

    fn parse<'a>(
        param: &'a (dyn Var + Send + Sync),
        input: VarInput,
    ) -> Result<Option<V>, VarError> {
        let s = extract_single_value(param, input)?;
        match s {
            "" => Ok(None),
            _ => <V as Value>::parse(param, VarInput::Flat(s)).map(Some),
        }
    }

    fn format(&self) -> String {
        match self {
            Some(s) => s.format(),
            None => "".into(),
        }
    }
}

// This unorthodox design lets us escape complex errors from value parsing.
#[derive(Clone, Debug, Eq, PartialEq)]
struct Failpoints;

impl Value for Failpoints {
    fn type_name() -> String {
        "failpoints config".to_string()
    }

    fn parse<'a>(
        param: &'a (dyn Var + Send + Sync),
        input: VarInput,
    ) -> Result<Failpoints, VarError> {
        let values = input.to_vec();
        for mut cfg in values.iter().map(|v| v.trim().split(';')).flatten() {
            cfg = cfg.trim();
            if cfg.is_empty() {
                continue;
            }
            let mut splits = cfg.splitn(2, '=');
            let failpoint = splits
                .next()
                .ok_or_else(|| VarError::InvalidParameterValue {
                    parameter: param.into(),
                    values: input.to_vec(),
                    reason: "missing failpoint name".into(),
                })?;
            let action = splits
                .next()
                .ok_or_else(|| VarError::InvalidParameterValue {
                    parameter: param.into(),
                    values: input.to_vec(),
                    reason: "missing failpoint action".into(),
                })?;
            fail::cfg(failpoint, action).map_err(|e| VarError::InvalidParameterValue {
                parameter: param.into(),
                values: input.to_vec(),
                reason: e,
            })?;
        }

        Ok(Failpoints)
    }

    fn format(&self) -> String {
        "<omitted>".to_string()
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
    fn type_name() -> String {
        "string".to_string()
    }

    fn parse<'a>(
        param: &'a (dyn Var + Send + Sync),
        input: VarInput,
    ) -> Result<Self::Owned, VarError> {
        let s = extract_single_value(param, input)?;
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
            Err(VarError::ConstrainedParameter {
                parameter: param.into(),
                values: input.to_vec(),
                valid_values: Some(ClientSeverity::valid_values()),
            })
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
    fn type_name() -> String {
        "string".to_string()
    }

    fn parse<'a>(
        param: &'a (dyn Var + Send + Sync),
        input: VarInput,
    ) -> Result<Self::Owned, VarError> {
        let s = extract_single_value(param, input)?;
        let s = UncasedStr::new(s);

        if s == TimeZone::UTC.as_str() {
            Ok(TimeZone::UTC)
        } else if s == "+00:00" {
            Ok(TimeZone::FixedOffset("+00:00"))
        } else {
            Err(VarError::ConstrainedParameter {
                parameter: (&TIMEZONE).into(),
                values: input.to_vec(),
                valid_values: None,
            })
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
    fn type_name() -> String {
        "string".to_string()
    }

    fn parse<'a>(
        param: &'a (dyn Var + Send + Sync),
        input: VarInput,
    ) -> Result<Self::Owned, VarError> {
        let s = extract_single_value(param, input)?;
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
            Err(VarError::ConstrainedParameter {
                parameter: (&TRANSACTION_ISOLATION).into(),
                values: input.to_vec(),
                valid_values: Some(IsolationLevel::valid_values()),
            })
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

impl Value for CloneableEnvFilter {
    fn type_name() -> String {
        "EnvFilter".to_string()
    }

    fn parse<'a>(
        param: &'a (dyn Var + Send + Sync),
        input: VarInput,
    ) -> Result<Self::Owned, VarError> {
        let s = extract_single_value(param, input)?;
        CloneableEnvFilter::from_str(s).map_err(|e| VarError::InvalidParameterValue {
            parameter: param.into(),
            values: vec![s.to_string()],
            reason: format!("{}", e),
        })
    }

    fn format(&self) -> String {
        format!("{}", self)
    }
}

pub fn is_tracing_var(name: &str) -> bool {
    name == LOGGING_FILTER.name() || name == OPENTELEMETRY_FILTER.name()
}

/// Returns whether the named variable is a compute configuration parameter.
pub fn is_compute_config_var(name: &str) -> bool {
    name == MAX_RESULT_SIZE.name()
        || name == DATAFLOW_MAX_INFLIGHT_BYTES.name()
        || name == ENABLE_MZ_JOIN_CORE.name()
        || is_persist_config_var(name)
        || is_tracing_var(name)
}

/// Returns whether the named variable is a storage configuration parameter.
pub fn is_storage_config_var(name: &str) -> bool {
    name == PG_REPLICATION_CONNECT_TIMEOUT.name()
        || name == PG_REPLICATION_KEEPALIVES_IDLE.name()
        || name == PG_REPLICATION_KEEPALIVES_INTERVAL.name()
        || name == PG_REPLICATION_KEEPALIVES_RETRIES.name()
        || name == PG_REPLICATION_TCP_USER_TIMEOUT.name()
        || is_upsert_rocksdb_config_var(name)
        || is_persist_config_var(name)
        || is_tracing_var(name)
}

fn is_upsert_rocksdb_config_var(name: &str) -> bool {
    name == upsert_rocksdb::UPSERT_ROCKSDB_COMPACTION_STYLE.name()
        || name == upsert_rocksdb::UPSERT_ROCKSDB_OPTIMIZE_COMPACTION_MEMTABLE_BUDGET.name()
        || name == upsert_rocksdb::UPSERT_ROCKSDB_LEVEL_COMPACTION_DYNAMIC_LEVEL_BYTES.name()
        || name == upsert_rocksdb::UPSERT_ROCKSDB_UNIVERSAL_COMPACTION_RATIO.name()
        || name == upsert_rocksdb::UPSERT_ROCKSDB_PARALLELISM.name()
        || name == upsert_rocksdb::UPSERT_ROCKSDB_COMPRESSION_TYPE.name()
        || name == upsert_rocksdb::UPSERT_ROCKSDB_BOTTOMMOST_COMPRESSION_TYPE.name()
        || name == upsert_rocksdb::UPSERT_ROCKSDB_BATCH_SIZE.name()
}

/// Returns whether the named variable is a persist configuration parameter.
fn is_persist_config_var(name: &str) -> bool {
    name == PERSIST_BLOB_TARGET_SIZE.name()
        || name == PERSIST_BLOB_CACHE_MEM_LIMIT_BYTES.name()
        || name == PERSIST_COMPACTION_MINIMUM_TIMEOUT.name()
        || name == PERSIST_CONSENSUS_CONNECTION_POOL_TTL.name()
        || name == PERSIST_CONSENSUS_CONNECTION_POOL_TTL_STAGGER.name()
        || name == CRDB_CONNECT_TIMEOUT.name()
        || name == CRDB_TCP_USER_TIMEOUT.name()
        || name == PERSIST_SINK_MINIMUM_BATCH_UPDATES.name()
        || name == STORAGE_PERSIST_SINK_MINIMUM_BATCH_UPDATES.name()
        || name == PERSIST_NEXT_LISTEN_BATCH_RETRYER_INITIAL_BACKOFF.name()
        || name == PERSIST_NEXT_LISTEN_BATCH_RETRYER_MULTIPLIER.name()
        || name == PERSIST_NEXT_LISTEN_BATCH_RETRYER_CLAMP.name()
        || name == PERSIST_STATS_AUDIT_PERCENT.name()
        || name == PERSIST_STATS_COLLECTION_ENABLED.name()
        || name == PERSIST_STATS_FILTER_ENABLED.name()
        || name == PERSIST_PUBSUB_CLIENT_ENABLED.name()
        || name == PERSIST_PUBSUB_PUSH_DIFF_ENABLED.name()
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum ClientEncoding {
    Utf8,
}

impl ClientEncoding {
    fn as_str(&self) -> &'static str {
        match self {
            ClientEncoding::Utf8 => "UTF8",
        }
    }

    fn valid_values() -> Vec<&'static str> {
        vec![ClientEncoding::Utf8.as_str()]
    }
}

impl Value for ClientEncoding {
    fn type_name() -> String {
        "string".to_string()
    }

    fn parse<'a>(
        param: &'a (dyn Var + Send + Sync),
        input: VarInput,
    ) -> Result<Self::Owned, VarError> {
        let s = extract_single_value(param, input)?;
        let s = UncasedStr::new(s);
        if s == Self::Utf8.as_str() {
            Ok(Self::Utf8)
        } else {
            Err(VarError::ConstrainedParameter {
                parameter: (&CLIENT_ENCODING).into(),
                values: vec![s.to_string()],
                valid_values: Some(ClientEncoding::valid_values()),
            })
        }
    }

    fn format(&self) -> String {
        self.as_str().to_string()
    }
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum IntervalStyle {
    Postgres,
}

impl IntervalStyle {
    fn as_str(&self) -> &'static str {
        match self {
            IntervalStyle::Postgres => "postgres",
        }
    }

    fn valid_values() -> Vec<&'static str> {
        vec![IntervalStyle::Postgres.as_str()]
    }
}

impl Value for IntervalStyle {
    fn type_name() -> String {
        "string".to_string()
    }

    fn parse<'a>(
        param: &'a (dyn Var + Send + Sync),
        input: VarInput,
    ) -> Result<Self::Owned, VarError> {
        let s = extract_single_value(param, input)?;
        let s = UncasedStr::new(s);
        if s == Self::Postgres.as_str() {
            Ok(Self::Postgres)
        } else {
            Err(VarError::ConstrainedParameter {
                parameter: (&INTERVAL_STYLE).into(),
                values: vec![s.to_string()],
                valid_values: Some(IntervalStyle::valid_values()),
            })
        }
    }

    fn format(&self) -> String {
        self.as_str().to_string()
    }
}

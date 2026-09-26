// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::time::Duration;

use mz_adapter_types::dyncfgs::PG_TIMESTAMP_ORACLE_STATEMENT_TIMEOUT;
use mz_orchestrator::scheduling_config::{ServiceSchedulingConfig, ServiceTopologySpreadConfig};
use mz_ore::cast::CastFrom;
use mz_sql::session::vars::SystemVars;

use mz_timestamp_oracle::postgres_oracle::TimestampOracleParameters;

pub use mz_catalog::compute_config::{compute_config, grpc_client_config, tracing_config};

pub use mz_catalog::storage_config::storage_config;

pub fn caching_config(config: &SystemVars) -> mz_secrets::CachingPolicy {
    let ttl_secs = config.webhooks_secrets_caching_ttl_secs();
    mz_secrets::CachingPolicy {
        enabled: ttl_secs > 0,
        ttl: Duration::from_secs(u64::cast_from(ttl_secs)),
    }
}

pub fn timestamp_oracle_config(config: &SystemVars) -> TimestampOracleParameters {
    TimestampOracleParameters {
        pg_connection_pool_max_size: Some(config.pg_timestamp_oracle_connection_pool_max_size()),
        pg_connection_pool_max_wait: Some(config.pg_timestamp_oracle_connection_pool_max_wait()),
        pg_connection_pool_ttl: Some(config.pg_timestamp_oracle_connection_pool_ttl()),
        pg_connection_pool_ttl_stagger: Some(
            config.pg_timestamp_oracle_connection_pool_ttl_stagger(),
        ),
        // We use a shared set of crdb flags for the basics, but the above flags
        // for the connection pool are specific to the postgres/crdb timestamp
        // oracle.
        pg_connection_pool_connect_timeout: Some(config.crdb_connect_timeout()),
        pg_connection_pool_tcp_user_timeout: Some(config.crdb_tcp_user_timeout()),
        pg_connection_pool_keepalives_idle: Some(config.crdb_keepalives_idle()),
        pg_connection_pool_keepalives_interval: Some(config.crdb_keepalives_interval()),
        pg_connection_pool_keepalives_retries: Some(config.crdb_keepalives_retries()),
        pg_statement_timeout: Some(PG_TIMESTAMP_ORACLE_STATEMENT_TIMEOUT.get(config.dyncfgs())),
    }
}

pub fn orchestrator_scheduling_config(config: &SystemVars) -> ServiceSchedulingConfig {
    ServiceSchedulingConfig {
        multi_pod_az_affinity_weight: config.cluster_multi_process_replica_az_affinity_weight(),
        soften_replication_anti_affinity: config.cluster_soften_replication_anti_affinity(),
        soften_replication_anti_affinity_weight: config
            .cluster_soften_replication_anti_affinity_weight(),
        topology_spread: ServiceTopologySpreadConfig {
            enabled: config.cluster_enable_topology_spread(),
            ignore_non_singular_scale: config.cluster_topology_spread_ignore_non_singular_scale(),
            max_skew: config.cluster_topology_spread_max_skew(),
            min_domains: config.cluster_topology_spread_set_min_domains(),
            soft: config.cluster_topology_spread_soft(),
        },
        soften_az_affinity: config.cluster_soften_az_affinity(),
        soften_az_affinity_weight: config.cluster_soften_az_affinity_weight(),
        security_context_enabled: config.cluster_security_context_enabled(),
    }
}

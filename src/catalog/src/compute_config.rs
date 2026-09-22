// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Pure derivation of compute runtime configuration from the native catalog.

use std::collections::BTreeMap;
use std::time::Duration;

use mz_compute_client::logging::LoggingConfig;
use mz_compute_client::protocol::command::{ComputeParameters, InstanceConfig};
use mz_compute_types::dyncfgs::{
    COMPUTE_REPLICA_EXPIRATION_OFFSET, ENABLE_ARRANGEMENT_DICTIONARY_COMPRESSION_ALPHA,
    ENABLE_COMPUTE_REPLICA_EXPIRATION,
};
use mz_controller_types::{ClusterId, ReplicaId};
use mz_dyncfg::ConfigUpdates;
use mz_persist_types::PersistLocation;
use mz_service::params::GrpcClientParameters;
use mz_sql::session::vars::SystemVars;
use mz_tracing::params::TracingParameters;

use crate::catalog::Catalog;

/// Return the current compute configuration, derived from the system configuration.
pub fn compute_config(config: &SystemVars) -> ComputeParameters {
    ComputeParameters {
        workload_class: None,
        max_result_size: Some(config.max_result_size()),
        tracing: tracing_config(config),
        grpc_client: grpc_client_config(config),
        dyncfg_updates: config.dyncfg_updates(),
    }
}

pub fn tracing_config(config: &SystemVars) -> TracingParameters {
    TracingParameters {
        log_filter: Some(config.logging_filter()),
        opentelemetry_filter: Some(config.opentelemetry_filter()),
        log_filter_defaults: config.logging_filter_defaults(),
        opentelemetry_filter_defaults: config.opentelemetry_filter_defaults(),
        sentry_filters: config.sentry_filters(),
    }
}

pub fn grpc_client_config(config: &SystemVars) -> GrpcClientParameters {
    GrpcClientParameters {
        connect_timeout: Some(config.grpc_connect_timeout()),
        http2_keep_alive_interval: Some(config.grpc_client_http2_keep_alive_interval()),
        http2_keep_alive_timeout: Some(config.grpc_client_http2_keep_alive_timeout()),
    }
}

/// Returns sparse replica-local dyncfg overrides, grouped by cluster.
///
/// Only existing replicas with a valid override are included. Non-dyncfg
/// parameters and values that fail to parse are skipped.
pub fn replica_dyncfg_overrides(
    catalog: &Catalog,
) -> BTreeMap<ClusterId, BTreeMap<ReplicaId, ConfigUpdates>> {
    let mut overrides: BTreeMap<ClusterId, BTreeMap<ReplicaId, ConfigUpdates>> = BTreeMap::new();
    for cluster in catalog.clusters() {
        for replica in cluster.replicas() {
            let updates = replica_dyncfg_override(catalog, replica.replica_id);
            if !updates.updates.is_empty() {
                overrides
                    .entry(cluster.id)
                    .or_default()
                    .insert(replica.replica_id, updates);
            }
        }
    }
    overrides
}

pub(crate) fn replica_dyncfg_override(catalog: &Catalog, replica_id: ReplicaId) -> ConfigUpdates {
    let mut updates = ConfigUpdates::default();
    let Some(values) = catalog
        .state()
        .scoped_system_parameters()
        .replica
        .get(&replica_id)
    else {
        return updates;
    };
    let dyncfgs = catalog.system_config().dyncfgs();
    for (name, value) in values {
        let Some(entry) = dyncfgs.entry(name) else {
            // A non-dyncfg parameter has no per-replica realization.
            continue;
        };
        match entry.parse_val(value) {
            Ok(val) => updates.add_dynamic(name, val),
            Err(e) => tracing::warn!(%name, %value, "cannot parse scoped override: {e}"),
        }
    }
    updates
}

/// Returns the full current configuration for an existing replica.
///
/// Every dyncfg has an environment value beneath the replica override, so
/// removing an override resets it on the next configuration update. An absent
/// workload class likewise explicitly clears the replica's workload class.
///
/// Panics if the cluster or replica does not exist in this catalog.
pub fn replica_compute_config(
    catalog: &Catalog,
    cluster_id: ClusterId,
    replica_id: ReplicaId,
) -> ComputeParameters {
    catalog.get_cluster_replica(cluster_id, replica_id);
    let cluster = catalog.get_cluster(cluster_id);
    let mut config = compute_config(catalog.system_config());
    config.workload_class = Some(cluster.config.workload_class.clone());
    config
        .dyncfg_updates
        .extend(replica_dyncfg_override(catalog, replica_id));
    config
}

/// Returns creation-time configuration for an existing replica.
///
/// Call once when initializing a replica incarnation, not for live configuration
/// updates. Logging, compression, and expiration are fixed at creation. The peek
/// stash location is supplied by the environment, rather than stored in the catalog.
///
/// Panics if the cluster or replica does not exist in this catalog.
pub fn replica_instance_config(
    catalog: &Catalog,
    cluster_id: ClusterId,
    replica_id: ReplicaId,
    peek_stash_persist_location: PersistLocation,
) -> InstanceConfig {
    let cluster = catalog.get_cluster(cluster_id);
    let replica = catalog.get_cluster_replica(cluster_id, replica_id);
    let compute = &replica.config.compute;
    let (enable_logging, interval) = match compute.logging.interval {
        Some(interval) => (true, interval),
        None => (false, Duration::from_secs(1)),
    };

    let system_config = catalog.system_config();
    let dyncfgs = system_config.dyncfgs();
    let mut initial_config = compute_config(system_config).dyncfg_updates;
    // Read committed values without mutating the process's shared ConfigSet.
    // The expiration kill switch is environment-wide, not replica-scoped.
    let enable_expiration =
        ENABLE_COMPUTE_REPLICA_EXPIRATION.get_with_overrides(dyncfgs, Some(&initial_config));
    initial_config.extend(replica_dyncfg_override(catalog, replica_id));
    let expiration_offset =
        COMPUTE_REPLICA_EXPIRATION_OFFSET.get_with_overrides(dyncfgs, Some(&initial_config));
    let arrangement_dictionary_compression = ENABLE_ARRANGEMENT_DICTIONARY_COMPRESSION_ALPHA
        .get_with_overrides(dyncfgs, Some(&initial_config))
        && compute.arrangement_compression;

    InstanceConfig {
        logging: LoggingConfig {
            interval,
            enable_logging,
            log_logging: compute.logging.log_logging,
            index_logs: cluster.log_indexes.clone(),
        },
        expiration_offset: (enable_expiration && !expiration_offset.is_zero())
            .then_some(expiration_offset),
        peek_stash_persist_location,
        arrangement_dictionary_compression,
        initial_config,
    }
}

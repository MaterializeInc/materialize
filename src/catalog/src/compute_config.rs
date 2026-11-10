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
        persist_state_version: None,
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
/// The caller supplies the Persist target installed by catalog admission.
///
/// Panics if the cluster or replica does not exist in this catalog.
pub fn replica_compute_config(
    catalog: &Catalog,
    cluster_id: ClusterId,
    replica_id: ReplicaId,
    persist_state_version: semver::Version,
) -> ComputeParameters {
    catalog.get_cluster_replica(cluster_id, replica_id);
    let cluster = catalog.get_cluster(cluster_id);
    let mut config = compute_config(catalog.system_config());
    config.persist_state_version = Some(persist_state_version);
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
/// The caller supplies the Persist target installed by catalog admission.
///
/// Panics if the cluster or replica does not exist in this catalog.
pub fn replica_instance_config(
    catalog: &Catalog,
    cluster_id: ClusterId,
    replica_id: ReplicaId,
    peek_stash_persist_location: PersistLocation,
    persist_state_version: semver::Version,
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
        persist_state_version: Some(persist_state_version),
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

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use mz_dyncfg::{ConfigVal, ParameterScope};
    use mz_sql::session::vars::{OwnedVarInput, VarInput};

    use super::*;
    use crate::catalog::{Op, ReplicaCreateDropReason};
    use crate::config::{ScopedParameters, ScopedParametersScope};

    #[mz_ore::test(tokio::test)]
    #[cfg_attr(miri, ignore)] // unsupported operation: can't call foreign function `TLS_client_method`
    async fn replica_compute_config_applies_and_removes_replica_override() {
        Catalog::with_debug(|mut catalog| async move {
            let replica = catalog
                .user_cluster_replicas()
                .next()
                .expect("bootstrap user replica")
                .clone();
            let ts = catalog.current_upper().await;
            let peer_id = catalog
                .allocate_user_replica_ids(1, ts)
                .await
                .expect("allocate peer")[0];
            let name = ENABLE_ARRANGEMENT_DICTIONARY_COMPRESSION_ALPHA.name();
            let ts = catalog.current_upper().await;
            catalog
                .transact(
                    None,
                    None,
                    ts,
                    None,
                    vec![
                        Op::CreateClusterReplica {
                            cluster_id: replica.cluster_id,
                            replica_id: peer_id,
                            name: "compute_config_peer".into(),
                            config: replica.config.clone(),
                            owner_id: replica.owner_id,
                            reason: ReplicaCreateDropReason::Manual,
                        },
                        Op::UpdateSystemConfiguration {
                            name: name.into(),
                            value: OwnedVarInput::Flat("false".into()),
                        },
                        Op::UpdateSystemConfiguration {
                            name: "max_result_size".into(),
                            value: OwnedVarInput::Flat("2MB".into()),
                        },
                    ],
                )
                .await
                .expect("create peer and commit environment configuration");

            let environment = compute_config(catalog.system_config());
            assert_eq!(environment.max_result_size, Some(2 * 1024 * 1024));
            assert_eq!(
                environment.dyncfg_updates.updates.get(name),
                Some(&ConfigVal::Bool(false)),
            );
            catalog.system_config().sync_dyncfgs();
            let shared = catalog.system_config().dyncfgs().clone();
            assert_eq!(shared.entry(name).unwrap().scope(), ParameterScope::Replica);
            let shared_values = || {
                shared
                    .entries()
                    .map(|entry| (entry.name(), entry.val()))
                    .collect::<BTreeMap<_, _>>()
            };
            let before = shared_values();
            let persist_state_version = semver::Version::new(1, 2, 3);
            let assert_config = |catalog: &Catalog, replica_id, overridden, workload_class| {
                let actual = replica_compute_config(
                    catalog,
                    replica.cluster_id,
                    replica_id,
                    persist_state_version.clone(),
                );
                let mut expected = environment.clone();
                expected.persist_state_version = Some(persist_state_version.clone());
                expected.workload_class = Some(workload_class);
                expected
                    .dyncfg_updates
                    .add(&ENABLE_ARRANGEMENT_DICTIONARY_COMPRESSION_ALPHA, overridden);
                assert_eq!(actual, expected);
                assert_eq!(shared_values(), before, "derivation mutated shared dyncfgs");
            };
            assert_config(&catalog, replica.replica_id, false, None);
            assert_config(&catalog, peer_id, false, None);

            // Scoped transactions expect prevalidated values, as in the scoped
            // configuration evaluator. Only replica-scoped parameters belong here.
            let override_value = catalog
                .system_config()
                .parse(name, VarInput::Flat("true"))
                .expect("valid replica override")
                .format();
            for (value, workload_class) in [
                (Some(override_value), Some("analytics".to_string())),
                (None, None),
            ] {
                let overridden = value.is_some();
                let mut scoped = ScopedParameters::default();
                if let Some(value) = value {
                    scoped.replica.insert(
                        replica.replica_id,
                        BTreeMap::from([(name.to_string(), value)]),
                    );
                }
                let cluster = catalog.get_cluster(replica.cluster_id);
                let mut config = cluster.config.clone();
                config.workload_class = workload_class.clone();
                let ops = vec![
                    Op::UpdateClusterConfig {
                        id: cluster.id,
                        name: cluster.name.clone(),
                        config,
                        reconfiguration_audit: None,
                        burst_audit: None,
                    },
                    Op::UpdateScopedSystemParameters {
                        scoped,
                        prune_scope: ScopedParametersScope {
                            clusters: BTreeSet::new(),
                            replicas: BTreeSet::from([replica.replica_id]),
                        },
                    },
                ];
                let ts = catalog.current_upper().await;
                catalog
                    .transact(None, None, ts, None, ops)
                    .await
                    .expect("commit replica override and workload class change");

                assert_config(
                    &catalog,
                    replica.replica_id,
                    overridden,
                    workload_class.clone(),
                );
                assert_config(&catalog, peer_id, false, workload_class);
                assert_eq!(compute_config(catalog.system_config()), environment);
            }

            catalog.expire().await;
        })
        .await
    }

    #[mz_ore::test(tokio::test)]
    #[cfg_attr(miri, ignore)] // unsupported operation: can't call foreign function `TLS_client_method`
    async fn replica_instance_config_derives_creation_settings_from_committed_catalog() {
        Catalog::with_debug(|mut catalog| async move {
            let template = catalog
                .user_cluster_replicas()
                .next()
                .expect("bootstrap user replica")
                .clone();
            let ts = catalog.current_upper().await;
            let ids = catalog
                .allocate_user_replica_ids(2, ts)
                .await
                .expect("allocate replicas");
            let mut ops = Vec::new();
            for (id, enabled) in [(ids[0], true), (ids[1], false)] {
                let mut config = template.config.clone();
                config.compute.logging.interval = enabled.then_some(Duration::from_millis(250));
                config.compute.logging.log_logging = enabled;
                config.compute.arrangement_compression = enabled;
                ops.push(Op::CreateClusterReplica {
                    cluster_id: template.cluster_id,
                    replica_id: id,
                    name: format!("instance_config_{id}"),
                    config,
                    owner_id: template.owner_id,
                    reason: ReplicaCreateDropReason::Manual,
                });
            }
            let compression = ENABLE_ARRANGEMENT_DICTIONARY_COMPRESSION_ALPHA.name();
            let expiration = COMPUTE_REPLICA_EXPIRATION_OFFSET.name();
            for (name, value) in [(compression, "false"), (expiration, "60s")] {
                ops.push(Op::UpdateSystemConfiguration {
                    name: name.into(),
                    value: OwnedVarInput::Flat(value.into()),
                });
            }
            let ts = catalog.current_upper().await;
            catalog
                .transact(None, None, ts, None, ops)
                .await
                .expect("commit replicas and environment configuration");

            let shared = catalog.system_config().dyncfgs().clone();
            for name in [compression, expiration] {
                assert_eq!(shared.entry(name).unwrap().scope(), ParameterScope::Replica);
            }
            let shared_values = || {
                shared
                    .entries()
                    .map(|entry| (entry.name(), entry.val()))
                    .collect::<BTreeMap<_, _>>()
            };
            let persist_state_version = semver::Version::new(1, 2, 3);
            let stash = PersistLocation::new_in_mem();

            // Each derivation models a new incarnation. Live application and
            // retention of these creation-time fields belong to the replica runtime.
            for (enable_expiration, offset, environment_compression, expected_expiration) in [
                ("true", "120s", "false", Some(Duration::from_secs(120))),
                ("false", "120s", "true", None),
                ("true", "0s", "true", None),
            ] {
                let values = [(compression, "true"), (expiration, offset)]
                    .into_iter()
                    .map(|(name, value)| {
                        let value = catalog
                            .system_config()
                            .parse(name, VarInput::Flat(value))
                            .expect("valid replica override")
                            .format();
                        (name.to_string(), value)
                    })
                    .collect();
                let mut scoped = ScopedParameters::default();
                scoped.replica.insert(ids[0], values);
                let ts = catalog.current_upper().await;
                catalog
                    .transact(
                        None,
                        None,
                        ts,
                        None,
                        vec![
                            Op::UpdateSystemConfiguration {
                                name: ENABLE_COMPUTE_REPLICA_EXPIRATION.name().into(),
                                value: OwnedVarInput::Flat(enable_expiration.into()),
                            },
                            Op::UpdateSystemConfiguration {
                                name: compression.into(),
                                value: OwnedVarInput::Flat(environment_compression.into()),
                            },
                            Op::UpdateScopedSystemParameters {
                                scoped,
                                prune_scope: ScopedParametersScope {
                                    clusters: BTreeSet::new(),
                                    replicas: BTreeSet::from([ids[0]]),
                                },
                            },
                        ],
                    )
                    .await
                    .expect("commit creation-time configuration");
                let before = shared_values();
                for (id, enabled) in [(ids[0], true), (ids[1], false)] {
                    let config = replica_instance_config(
                        &catalog,
                        template.cluster_id,
                        id,
                        stash.clone(),
                        persist_state_version.clone(),
                    );
                    assert_eq!(
                        config.persist_state_version,
                        Some(persist_state_version.clone())
                    );
                    assert_eq!(config.peek_stash_persist_location, stash);
                    assert_eq!(config.logging.enable_logging, enabled);
                    assert_eq!(config.logging.log_logging, enabled);
                    assert_eq!(
                        config.logging.interval,
                        if enabled {
                            Duration::from_millis(250)
                        } else {
                            Duration::from_secs(1)
                        },
                    );
                    assert_eq!(
                        config.logging.index_logs,
                        catalog.get_cluster(template.cluster_id).log_indexes
                    );
                    assert_eq!(config.arrangement_dictionary_compression, enabled);
                    assert_eq!(
                        config.expiration_offset,
                        if enabled {
                            expected_expiration
                        } else {
                            (enable_expiration == "true").then_some(Duration::from_secs(60))
                        },
                    );
                    let mut expected = compute_config(catalog.system_config()).dyncfg_updates;
                    if enabled {
                        expected.add(&ENABLE_ARRANGEMENT_DICTIONARY_COMPRESSION_ALPHA, true);
                        expected.add(
                            &COMPUTE_REPLICA_EXPIRATION_OFFSET,
                            if offset == "0s" {
                                Duration::ZERO
                            } else {
                                Duration::from_secs(120)
                            },
                        );
                    }
                    assert_eq!(config.initial_config, expected);
                    assert_eq!(shared_values(), before, "derivation mutated shared dyncfgs");
                }
            }

            catalog.expire().await;
        })
        .await
    }
}

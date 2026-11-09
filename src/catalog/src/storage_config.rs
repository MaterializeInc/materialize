// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Pure derivation of storage configuration and descriptions from the native catalog.

use mz_controller_types::{ClusterId, ReplicaId};
use mz_ore::error::ErrorExt;
use mz_repr::{CatalogItemId, Timestamp};
use mz_sql::session::vars::SystemVars;
use mz_storage_types::connections::inline::IntoInlineConnection;
use mz_storage_types::parameters::{
    PgSourceSnapshotConfig, StorageMaxInflightBytesConfig, StorageParameters,
};
use mz_storage_types::sinks::StorageSinkDesc;
use mz_storage_types::sources::{IngestionDescription, SourceExport};
use timely::progress::Antichain;

use crate::catalog::{Catalog, CatalogState};
use crate::compute_config::{grpc_client_config, tracing_config};
use crate::memory::objects::{DataSourceDesc, Sink};

/// Return the current storage configuration, derived from the system configuration.
pub fn storage_config(config: &SystemVars) -> StorageParameters {
    StorageParameters {
        persist_state_version: None,
        pg_source_connect_timeout: Some(config.pg_source_connect_timeout()),
        pg_source_tcp_keepalives_retries: Some(config.pg_source_tcp_keepalives_retries()),
        pg_source_tcp_keepalives_idle: Some(config.pg_source_tcp_keepalives_idle()),
        pg_source_tcp_keepalives_interval: Some(config.pg_source_tcp_keepalives_interval()),
        pg_source_tcp_user_timeout: Some(config.pg_source_tcp_user_timeout()),
        pg_source_tcp_configure_server: config.pg_source_tcp_configure_server(),
        pg_source_snapshot_statement_timeout: config.pg_source_snapshot_statement_timeout(),
        pg_source_wal_sender_timeout: config.pg_source_wal_sender_timeout(),
        mysql_source_timeouts: mz_mysql_util::TimeoutConfig::build(
            config.mysql_source_snapshot_max_execution_time(),
            config.mysql_source_snapshot_lock_wait_timeout(),
            config.mysql_source_snapshot_wait_timeout(),
            config.mysql_source_tcp_keepalive(),
            config.mysql_source_connect_timeout(),
        ),
        keep_n_source_status_history_entries: config.keep_n_source_status_history_entries(),
        keep_n_sink_status_history_entries: config.keep_n_sink_status_history_entries(),
        keep_n_privatelink_status_history_entries: config
            .keep_n_privatelink_status_history_entries(),
        replica_status_history_retention_window: config.replica_status_history_retention_window(),
        upsert_rocksdb_tuning_config: {
            match mz_rocksdb_types::RocksDBTuningParameters::from_parameters(
                config.upsert_rocksdb_compaction_style(),
                config.upsert_rocksdb_optimize_compaction_memtable_budget(),
                config.upsert_rocksdb_level_compaction_dynamic_level_bytes(),
                config.upsert_rocksdb_universal_compaction_ratio(),
                config.upsert_rocksdb_parallelism(),
                config.upsert_rocksdb_compression_type(),
                config.upsert_rocksdb_bottommost_compression_type(),
                config.upsert_rocksdb_batch_size(),
                config.upsert_rocksdb_retry_duration(),
                config.upsert_rocksdb_stats_log_interval_seconds(),
                config.upsert_rocksdb_stats_persist_interval_seconds(),
                config.upsert_rocksdb_point_lookup_block_cache_size_mb(),
                config.upsert_rocksdb_shrink_allocated_buffers_by_ratio(),
                config.upsert_rocksdb_write_buffer_manager_memory_bytes(),
                config
                    .upsert_rocksdb_write_buffer_manager_cluster_memory_fraction()
                    .and_then(|d| match d.try_into() {
                        Err(e) => {
                            tracing::error!(
                                "Couldn't convert upsert_rocksdb_write_buffer_manager_cluster_memory_fraction {:?} to f64, so defaulting to `None`: {e:?}",
                                config
                                    .upsert_rocksdb_write_buffer_manager_cluster_memory_fraction()
                            );
                            None
                        }
                        Ok(o) => Some(o),
                    }),
                config.upsert_rocksdb_write_buffer_manager_allow_stall(),
            ) {
                Ok(u) => u,
                Err(e) => {
                    tracing::warn!(
                        "Failed to deserialize upsert_rocksdb parameters \
                            into a `RocksDBTuningParameters`, \
                            failing back to reasonable defaults: {}",
                        e.display_with_causes()
                    );
                    mz_rocksdb_types::RocksDBTuningParameters::default()
                }
            }
        },
        finalize_shards: config.enable_storage_shard_finalization(),
        tracing: tracing_config(config),
        storage_dataflow_max_inflight_bytes_config: StorageMaxInflightBytesConfig {
            max_inflight_bytes_default: config.storage_dataflow_max_inflight_bytes(),
            // Interpret the `Numeric` as a float here, we don't need perfect
            // precision for a percentage. Unfortunately `Decimal` makes us handle errors.
            max_inflight_bytes_cluster_size_fraction: config
                .storage_dataflow_max_inflight_bytes_to_cluster_size_fraction()
                .and_then(|d| match d.try_into() {
                    Err(e) => {
                        tracing::error!(
                            "Couldn't convert {:?} to f64, so defaulting to `None`: {e:?}",
                            config.storage_dataflow_max_inflight_bytes_to_cluster_size_fraction()
                        );
                        None
                    }
                    Ok(o) => Some(o),
                }),
            disk_only: config.storage_dataflow_max_inflight_bytes_disk_only(),
        },
        grpc_client: grpc_client_config(config),
        shrink_upsert_unused_buffers_by_ratio: config
            .storage_shrink_upsert_unused_buffers_by_ratio(),
        record_namespaced_errors: config.storage_record_source_sink_namespaced_errors(),
        ssh_timeout_config: mz_ssh_util::tunnel::SshTimeoutConfig {
            check_interval: config.ssh_check_interval(),
            connect_timeout: config.ssh_connect_timeout(),
            keepalives_idle: config.ssh_keepalives_idle(),
        },
        kafka_timeout_config: mz_kafka_util::client::TimeoutConfig::build(
            config.kafka_socket_keepalive(),
            config.kafka_socket_timeout(),
            config.kafka_transaction_timeout(),
            config.kafka_socket_connection_setup_timeout(),
            config.kafka_fetch_metadata_timeout(),
            config.kafka_progress_record_fetch_timeout(),
        ),
        statistics_interval: config.storage_statistics_interval(),
        statistics_collection_interval: config.storage_statistics_collection_interval(),
        pg_snapshot_config: PgSourceSnapshotConfig {
            collect_strict_count: config.pg_source_snapshot_collect_strict_count(),
        },
        user_storage_managed_collections_batch_duration: config
            .user_storage_managed_collections_batch_duration(),
        dyncfg_updates: config.dyncfg_updates(),
    }
}

/// Returns the full current storage configuration for an existing replica.
///
/// Every dyncfg has its committed environment value beneath the replica override,
/// so removing an override resets it on the next configuration update. This does
/// not mutate the process's shared dyncfg ConfigSet.
/// The caller supplies the Persist target installed by catalog admission.
///
/// Panics if the cluster or replica does not exist in this catalog.
pub fn replica_storage_config(
    catalog: &Catalog,
    cluster_id: ClusterId,
    replica_id: ReplicaId,
    persist_state_version: semver::Version,
) -> StorageParameters {
    catalog.get_cluster_replica(cluster_id, replica_id);
    let mut config = storage_config(catalog.system_config());
    config.persist_state_version = Some(persist_state_version);
    config
        .dyncfg_updates
        .extend(crate::compute_config::replica_dyncfg_override(
            catalog, replica_id,
        ));
    config
}

impl CatalogState {
    /// Derives an ingestion and its committed exports, with connections inlined.
    ///
    /// Returns `None` for entries that are not ingestion sources. Both source and
    /// table exports use their latest global IDs. Old-syntax ingestions include
    /// their primary export and use their separate progress collection.
    ///
    /// The caller owns storage metadata enrichment, including prefix schemas and
    /// grants. This does not register collections or acquire read capabilities.
    /// Panics if `item_id` or a referenced catalog entry does not exist.
    pub fn ingestion_description(
        &self,
        item_id: CatalogItemId,
    ) -> Option<IngestionDescription<()>> {
        let entry = self.get_entry(&item_id);
        let source = entry.source()?;
        let mut ingestion = match &source.data_source {
            DataSourceDesc::Ingestion { desc, cluster_id } => IngestionDescription::new(
                desc.clone().into_inline_connection(self),
                *cluster_id,
                entry.latest_global_id(),
            ),
            DataSourceDesc::OldSyntaxIngestion {
                desc,
                cluster_id,
                progress_subsource,
                data_config,
                details,
            } => {
                let mut ingestion = IngestionDescription::new(
                    desc.clone().into_inline_connection(self),
                    *cluster_id,
                    self.get_entry(progress_subsource).latest_global_id(),
                );
                ingestion.source_exports.insert(
                    source.global_id,
                    SourceExport {
                        storage_metadata: (),
                        data_config: data_config.clone().into_inline_connection(self),
                        details: details.clone(),
                    },
                );
                ingestion
            }
            DataSourceDesc::IngestionExport { .. }
            | DataSourceDesc::Progress
            | DataSourceDesc::Webhook { .. }
            | DataSourceDesc::Introspection(_)
            | DataSourceDesc::Catalog => return None,
        };

        for dependent_id in entry.used_by() {
            let dependent = self.get_entry(dependent_id);
            let Some((ingestion_id, _, details, data_config)) = dependent.source_export_details()
            else {
                continue;
            };
            if ingestion_id != item_id {
                continue;
            }
            ingestion.source_exports.insert(
                dependent.latest_global_id(),
                SourceExport {
                    storage_metadata: (),
                    data_config: data_config.clone().into_inline_connection(self),
                    details: details.clone(),
                },
            );
        }
        Some(ingestion)
    }

    /// Derives a sink description and its cluster from a committed sink.
    ///
    /// The caller supplies the executable `as_of` and owns read capabilities and
    /// storage metadata enrichment. In particular, a catalog snapshot alone does
    /// not establish that the sink's input remains readable at a frontier.
    /// Panics if the input or a referenced connection is missing, or if the input
    /// has no relation description.
    pub fn storage_sink_description(
        &self,
        sink: &Sink,
        as_of: Antichain<Timestamp>,
    ) -> (StorageSinkDesc<()>, ClusterId) {
        let from_entry = self.get_entry_by_global_id(&sink.from);
        let desc = StorageSinkDesc {
            from: sink.from,
            from_desc: from_entry
                .relation_desc()
                .expect("sinks can only be built on items with descs")
                .into_owned(),
            connection: sink.connection.clone().into_inline_connection(self),
            envelope: sink.envelope,
            as_of,
            with_snapshot: sink.with_snapshot,
            version: sink.version,
            from_storage_metadata: (),
            to_storage_metadata: (),
            commit_interval: sink.commit_interval,
        };
        (desc, sink.cluster_id)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, BTreeSet};

    use mz_dyncfg::{ConfigVal, ParameterScope};
    use mz_sql::session::vars::{OwnedVarInput, VarInput};
    use mz_storage_types::dyncfgs::ENABLE_UPSERT_PAGED_SPILL;

    use super::*;
    use crate::catalog::{Op, ReplicaCreateDropReason};
    use crate::config::{ScopedParameters, ScopedParametersScope};

    #[mz_ore::test(tokio::test)]
    #[cfg_attr(miri, ignore)] // unsupported operation: can't call foreign function `TLS_client_method`
    async fn replica_storage_config_applies_and_removes_replica_override() {
        Catalog::with_debug(|mut catalog| async move {
            let replica = catalog
                .user_cluster_replicas()
                .next()
                .expect("bootstrap user replica")
                .clone();
            let ts = catalog.current_upper().await;
            let other_replica_id = catalog
                .allocate_user_replica_ids(1, ts)
                .await
                .expect("allocate second replica")[0];
            let name = ENABLE_UPSERT_PAGED_SPILL.name();
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
                            replica_id: other_replica_id,
                            name: "storage_config_peer".into(),
                            config: replica.config.clone(),
                            owner_id: replica.owner_id,
                            reason: ReplicaCreateDropReason::Manual,
                        },
                        Op::UpdateSystemConfiguration {
                            name: name.into(),
                            value: OwnedVarInput::Flat("true".into()),
                        },
                    ],
                )
                .await
                .expect("create peer and commit non-default environment value");

            assert_eq!(
                storage_config(catalog.system_config())
                    .dyncfg_updates
                    .updates
                    .get(name),
                Some(&ConfigVal::Bool(true)),
            );

            // Model environment-wide installation. ConfigSet clones share their
            // values, so deriving a replica must not apply its override here.
            catalog.system_config().sync_dyncfgs();
            let shared = catalog.system_config().dyncfgs().clone();
            assert!(ENABLE_UPSERT_PAGED_SPILL.get(&shared));
            assert_eq!(shared.entry(name).unwrap().scope(), ParameterScope::Replica);
            let shared_values = || {
                shared
                    .entries()
                    .map(|entry| (entry.name(), entry.val()))
                    .collect::<BTreeMap<_, _>>()
            };
            let before = shared_values();
            let persist_state_version = semver::Version::new(1, 2, 3);
            let assert_config = |catalog: &Catalog, replica_id, expected| {
                let config = replica_storage_config(
                    catalog,
                    replica.cluster_id,
                    replica_id,
                    persist_state_version.clone(),
                );
                assert_eq!(
                    config.dyncfg_updates.updates.get(name),
                    Some(&ConfigVal::Bool(expected)),
                );
                assert_eq!(
                    config.persist_state_version,
                    Some(persist_state_version.clone())
                );
                assert_eq!(shared_values(), before, "derivation mutated shared dyncfgs");
            };
            assert_config(&catalog, replica.replica_id, true);
            assert_config(&catalog, other_replica_id, true);

            // Scoped transactions consume validated values rather than parsing
            // them themselves. Use the same SystemVars parser as environment config.
            let override_value = catalog
                .system_config()
                .parse(name, VarInput::Flat("false"))
                .expect("valid replica override")
                .format();
            for (value, expected) in [(Some(override_value), false), (None, true)] {
                let mut scoped = ScopedParameters::default();
                if let Some(value) = value {
                    scoped.replica.insert(
                        replica.replica_id,
                        BTreeMap::from([(name.to_string(), value)]),
                    );
                }
                let ts = catalog.current_upper().await;
                catalog
                    .transact(
                        None,
                        None,
                        ts,
                        None,
                        vec![Op::UpdateScopedSystemParameters {
                            scoped,
                            prune_scope: ScopedParametersScope {
                                clusters: BTreeSet::new(),
                                replicas: BTreeSet::from([replica.replica_id]),
                            },
                        }],
                    )
                    .await
                    .expect("commit replica override change");

                assert_config(&catalog, replica.replica_id, expected);
                assert_config(&catalog, other_replica_id, true);
            }

            catalog.expire().await;
        })
        .await
    }
}

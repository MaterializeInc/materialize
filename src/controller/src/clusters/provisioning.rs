// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Replica launch specifications, independent of storage/compute controller state.

use std::collections::BTreeMap;
use std::path::Path;

use bytesize::ByteSize;
use mz_cluster_client::client::TimelyConfig;
use mz_controller_types::clusters::{ClusterRole, ManagedReplicaLocation};
use mz_controller_types::dyncfgs::{
    ARRANGEMENT_EXERT_PROPORTIONALITY, ENABLE_TIMELY_ZERO_COPY, ENABLE_TIMELY_ZERO_COPY_LGALLOC,
    ENABLE_UNIFIED_CLUSTER, TIMELY_ZERO_COPY_LIMIT,
};
use mz_controller_types::{ClusterId, ReplicaId};
use mz_dyncfg::{ConfigSet, ConfigUpdates};
use mz_orchestrator::{
    DiskLimit, LabelSelectionLogic, LabelSelector, MemoryLimit, ServiceConfig, ServicePort,
};
use mz_ore::url::SensitiveUrl;
use mz_persist_client::PersistLocation;
use mz_secrets_cli::SecretsReaderCliArgs;
use mz_storage_types::connections::ConnectionContext;

/// Deployment configuration for constructing replica launch specifications.
///
/// Building a specification does not authorize or provision a process. The caller
/// must establish committed membership before submitting it to an orchestrator.
/// Launch arguments contain credentials and are not an inspection API.
pub struct ReplicaProvisioningConfig<'a> {
    /// Deployment owning these replica realizations.
    pub deploy_generation: u64,
    /// Environment and external connection identity passed to replicas.
    pub connection_context: &'a ConnectionContext,
    /// Shared relay endpoint.
    pub persist_pubsub_url: &'a str,
    /// Shared catalog location for native catalog-following replicas.
    pub catalog_persist_location: Option<&'a PersistLocation>,
    /// Shared catalog timestamp oracle.
    pub catalog_timestamp_oracle_url: Option<&'a SensitiveUrl>,
    /// Explicit fixture clock for timestamp allocation and policy checks.
    pub timestamp_oracle_clock_file: Option<&'a Path>,
    /// Serialized catalog reconstruction inputs.
    pub catalog_follower_config: Option<&'a str>,
    /// Secrets-reader launch arguments.
    pub secrets_args: &'a SecretsReaderCliArgs,
    /// Environment-wide process configuration.
    pub dyncfg: &'a ConfigSet,
    /// Overrides for the replica being provisioned.
    pub replica_dyncfg_overrides: Option<&'a ConfigUpdates>,
    /// Executable or container image for replicas.
    pub clusterd_image: &'a str,
    /// Optional Kubernetes initialization image.
    pub init_container_image: Option<&'a str>,
}

impl ReplicaProvisioningConfig<'_> {
    /// Builds a backend-independent specification from committed replica intent.
    pub fn service_config(
        &self,
        cluster_id: ClusterId,
        replica_id: ReplicaId,
        cluster_name: String,
        replica_name: String,
        role: ClusterRole,
        location: ManagedReplicaLocation,
        enable_worker_core_affinity: bool,
    ) -> Result<ServiceConfig, anyhow::Error> {
        let role_label = match role {
            ClusterRole::SystemCritical => "system-critical",
            ClusterRole::System => "system",
            ClusterRole::User => "user",
        };
        let environment_id = self.connection_context.environment_id.clone();
        let aws_external_id_prefix = self.connection_context.aws_external_id_prefix.clone();
        let aws_connection_role_arn = self.connection_context.aws_connection_role_arn.clone();
        let persist_pubsub_url = self.persist_pubsub_url.to_owned();
        let catalog_persist_location = self.catalog_persist_location.cloned();
        let catalog_follower_config = self.catalog_follower_config.map(str::to_owned);
        let catalog_timestamp_oracle_url = self.catalog_timestamp_oracle_url.cloned();
        let timestamp_oracle_clock_file = self.timestamp_oracle_clock_file.map(Path::to_path_buf);
        if catalog_persist_location.is_some() && catalog_timestamp_oracle_url.is_none() {
            anyhow::bail!("catalog timestamp oracle URL must be set before provisioning replicas");
        }
        if catalog_persist_location.is_some() && catalog_follower_config.is_none() {
            anyhow::bail!("catalog follower config must be set before provisioning replicas");
        }
        let deploy_generation = self.deploy_generation;
        let secrets_args = self.secrets_args.to_flags();

        // These configure the replica's process rather than environmentd's, so
        // they are `ParameterScope::Replica` and must be read through this
        // replica's scoped overrides. They are baked into the process
        // configuration at provisioning time, so a later change to either the
        // environment-wide value or the override reaches the replica only when
        // it is next provisioned.
        let overrides = self.replica_dyncfg_overrides;
        // Storage and compute arrangements share one maintenance policy, so a
        // unified replica runs both kinds of arrangement under the same reach.
        let arrangement_exert_proportionality =
            ARRANGEMENT_EXERT_PROPORTIONALITY.get_with_overrides(self.dyncfg, overrides);
        let storage_proto_timely_config = TimelyConfig {
            arrangement_exert_proportionality,
            ..Default::default()
        };
        let compute_proto_timely_config = TimelyConfig {
            arrangement_exert_proportionality,
            enable_zero_copy: ENABLE_TIMELY_ZERO_COPY.get_with_overrides(self.dyncfg, overrides),
            enable_zero_copy_lgalloc: ENABLE_TIMELY_ZERO_COPY_LGALLOC
                .get_with_overrides(self.dyncfg, overrides),
            zero_copy_limit: TIMELY_ZERO_COPY_LIMIT.get_with_overrides(self.dyncfg, overrides),
            ..Default::default()
        };
        let unified_cluster = ENABLE_UNIFIED_CLUSTER.get_with_overrides(self.dyncfg, overrides);

        let mut disk_limit = location.allocation.disk_limit;
        let memory_limit = location.allocation.memory_limit;
        let mut memory_request = None;

        if location.allocation.swap_enabled {
            // The disk limit we specify in the service config decides whether or not the replica
            // gets a scratch disk attached. We want to avoid attaching disks to swap replicas, so
            // make sure to set the disk limit accordingly.
            disk_limit = Some(DiskLimit::ZERO);

            // We want to keep the memory request equal to the memory limit, to avoid
            // over-provisioning and ensure replicas have predictable performance. However, to
            // enable swap, Kubernetes currently requires that request and limit are different.
            memory_request = memory_limit.map(|MemoryLimit(limit)| {
                let request = ByteSize::b(limit.as_u64() - 1);
                MemoryLimit(request)
            });
        }

        Ok(ServiceConfig {
            app_name: "clusterd".into(),
            image: self.clusterd_image.to_owned(),
            init_container_image: self.init_container_image.map(str::to_owned),
            args: Box::new(move |assigned| {
                let storage_timely_config = TimelyConfig {
                    workers: location.allocation.workers.get(),
                    addresses: assigned.peer_addresses("storage"),
                    ..storage_proto_timely_config
                };
                let compute_timely_config = TimelyConfig {
                    workers: location.allocation.workers.get(),
                    addresses: assigned.peer_addresses("compute"),
                    ..compute_proto_timely_config
                };

                let mut args = vec![
                    format!(
                        "--storage-controller-listen-addr={}",
                        assigned.listen_addrs["storagectl"]
                    ),
                    format!(
                        "--compute-controller-listen-addr={}",
                        assigned.listen_addrs["computectl"]
                    ),
                    format!(
                        "--internal-http-listen-addr={}",
                        assigned.listen_addrs["internal-http"]
                    ),
                    format!("--opentelemetry-resource=cluster_id={}", cluster_id),
                    format!("--opentelemetry-resource=replica_id={}", replica_id),
                    format!("--persist-pubsub-url={}", persist_pubsub_url),
                    format!("--environment-id={}", environment_id),
                    format!(
                        "--storage-timely-config={}",
                        storage_timely_config.to_string(),
                    ),
                    format!(
                        "--compute-timely-config={}",
                        compute_timely_config.to_string(),
                    ),
                ];
                if let Some(location) = &catalog_persist_location {
                    args.extend([
                        format!("--catalog-cluster-id={cluster_id}"),
                        format!("--catalog-replica-id={replica_id}"),
                        format!("--catalog-deploy-generation={deploy_generation}"),
                        format!(
                            "--catalog-timestamp-oracle-url={}",
                            catalog_timestamp_oracle_url
                                .as_ref()
                                .expect("checked before provisioning")
                                .to_string_unredacted()
                        ),
                        format!(
                            "--catalog-config={}",
                            catalog_follower_config
                                .as_ref()
                                .expect("checked before provisioning")
                        ),
                        format!(
                            "--catalog-persist-blob-url={}",
                            location.blob_uri.to_string_unredacted()
                        ),
                        format!(
                            "--catalog-persist-consensus-url={}",
                            location.consensus_uri.to_string_unredacted()
                        ),
                    ]);
                    if let Some(path) = &timestamp_oracle_clock_file {
                        args.push(format!("--timestamp-oracle-clock-file={}", path.display()));
                    }
                }
                if let Some(aws_external_id_prefix) = &aws_external_id_prefix {
                    args.push(format!(
                        "--aws-external-id-prefix={}",
                        aws_external_id_prefix
                    ));
                }
                if let Some(aws_connection_role_arn) = &aws_connection_role_arn {
                    args.push(format!(
                        "--aws-connection-role-arn={}",
                        aws_connection_role_arn
                    ));
                }
                if let Some(memory_limit) = location.allocation.memory_limit {
                    args.push(format!(
                        "--announce-memory-limit={}",
                        memory_limit.0.as_u64()
                    ));
                }
                if location.allocation.cpu_exclusive && enable_worker_core_affinity {
                    args.push("--worker-core-affinity".into());
                }
                if unified_cluster {
                    args.push("--unified-cluster".into());
                }
                if location.allocation.is_cc {
                    args.push("--is-cc".into());
                }

                // If swap is enabled, make the replica limit its own heap usage based on the
                // configured memory and disk limits.
                if location.allocation.swap_enabled
                    && let Some(memory_limit) = location.allocation.memory_limit
                    && let Some(disk_limit) = location.allocation.disk_limit
                    // Currently, the way for replica sizes to request unlimited swap is to
                    // specify a `disk_limit` of 0. Ideally we'd change this to make them
                    // specify no disk limit instead, but for now we need to special-case here.
                    && disk_limit != DiskLimit::ZERO
                {
                    let heap_limit = memory_limit.0 + disk_limit.0;
                    args.push(format!("--heap-limit={}", heap_limit.as_u64()));
                }

                args.extend(secrets_args.clone());
                args
            }),
            ports: vec![
                ServicePort {
                    name: "storagectl".into(),
                    port_hint: 2100,
                    requires_tcp: false,
                },
                // To simplify the changes to tests, the port
                // chosen here is _after_ the compute ones.
                // TODO(petrosagg): fix the numerical ordering here
                ServicePort {
                    name: "storage".into(),
                    port_hint: 2103,
                    requires_tcp: false,
                },
                ServicePort {
                    name: "computectl".into(),
                    port_hint: 2101,
                    requires_tcp: false,
                },
                ServicePort {
                    name: "compute".into(),
                    port_hint: 2102,
                    requires_tcp: false,
                },
                ServicePort {
                    name: "internal-http".into(),
                    port_hint: 6878,
                    requires_tcp: false,
                },
            ],
            cpu_limit: location.allocation.cpu_limit,
            cpu_request: location.allocation.cpu_request,
            memory_limit,
            memory_request,
            scale: location.allocation.scale,
            labels: BTreeMap::from([
                ("replica-id".into(), replica_id.to_string()),
                ("cluster-id".into(), cluster_id.to_string()),
                ("generation".into(), self.deploy_generation.to_string()),
                ("type".into(), "cluster".into()),
                ("replica-role".into(), role_label.into()),
                ("workers".into(), location.allocation.workers.to_string()),
                (
                    "size".into(),
                    location
                        .size
                        .to_string()
                        .replace("=", "-")
                        .replace(",", "_"),
                ),
            ]),
            annotations: BTreeMap::from([
                (
                    "replica-name".into(),
                    format!("{cluster_name}.{replica_name}"),
                ),
                ("cluster-name".into(), cluster_name),
            ]),
            // An empty list means no AZ constraint; a non-empty one pins
            // placement to those zones.
            availability_zones: Some(location.availability_zones).filter(|azs| !azs.is_empty()),
            // This provides the orchestrator with some label selectors that
            // are used to constraint the scheduling of replicas, based on
            // its internal configuration.
            //
            // Selectors include `generation` so that scheduling constraints
            // (anti-affinity, topology spread) only consider pods of the same
            // deploy generation. Otherwise, during a generation rollout, the
            // new-generation pods would be constrained by the placement of
            // old-generation pods that are about to be torn down, which can
            // prevent the new pods from scheduling (e.g., when only one AZ
            // has capacity but it is already occupied by an old-generation
            // pod).
            other_replicas_selector: vec![
                LabelSelector {
                    label_name: "cluster-id".to_string(),
                    logic: LabelSelectionLogic::Eq {
                        value: cluster_id.to_string(),
                    },
                },
                // Select other replicas (but not oneself)
                LabelSelector {
                    label_name: "replica-id".into(),
                    logic: LabelSelectionLogic::NotEq {
                        value: replica_id.to_string(),
                    },
                },
                LabelSelector {
                    label_name: "generation".into(),
                    logic: LabelSelectionLogic::Eq {
                        value: self.deploy_generation.to_string(),
                    },
                },
            ],
            replicas_selector: vec![
                LabelSelector {
                    label_name: "cluster-id".to_string(),
                    // Select ALL replicas.
                    logic: LabelSelectionLogic::Eq {
                        value: cluster_id.to_string(),
                    },
                },
                LabelSelector {
                    label_name: "generation".into(),
                    logic: LabelSelectionLogic::Eq {
                        value: self.deploy_generation.to_string(),
                    },
                },
            ],
            disk_limit,
            node_selector: location.allocation.selectors,
        })
    }
}

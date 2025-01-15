// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::{collections::BTreeMap, time::Duration};

use anyhow::bail;
use k8s_openapi::{
    api::{
        apps::v1::{StatefulSet, StatefulSetSpec, StatefulSetUpdateStrategy},
        core::v1::{
            Capabilities, Container, ContainerPort, EnvVar, EnvVarSource, EphemeralVolumeSource,
            PersistentVolumeClaimSpec, PersistentVolumeClaimTemplate, Pod, PodSecurityContext,
            PodSpec, PodTemplateSpec, Probe, SeccompProfile, SecretKeySelector, SecretVolumeSource,
            SecurityContext, Service, ServiceAccount, ServicePort, ServiceSpec, TCPSocketAction,
            Toleration, Volume, VolumeMount, VolumeResourceRequirements,
        },
        networking::v1::{
            IPBlock, NetworkPolicy, NetworkPolicyEgressRule, NetworkPolicyIngressRule,
            NetworkPolicyPeer, NetworkPolicyPort, NetworkPolicySpec,
        },
        rbac::v1::{PolicyRule, Role, RoleBinding, RoleRef, Subject},
    },
    apimachinery::pkg::{apis::meta::v1::LabelSelector, util::intstr::IntOrString},
};
use kube::{api::ObjectMeta, runtime::controller::Action, Api, Client, ResourceExt};
use maplit::btreemap;
use rand::{thread_rng, Rng};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tracing::trace;

use super::matching_image_from_environmentd_image_ref;
use crate::controller::materialize::tls::{create_certificate, issuer_ref_defined};
use crate::k8s::{apply_resource, delete_resource, get_resource};
use mz_cloud_provider::CloudProvider;
use mz_cloud_resources::crd::gen::cert_manager::certificates::Certificate;
use mz_cloud_resources::crd::materialize::v1alpha1::Materialize;
use mz_orchestrator_tracing::TracingCliArgs;
use mz_ore::instrument;

/// Describes the status of a deployment.
///
/// This is a simplified representation of `DeploymentState`, suitable for
/// announcement to the external orchestrator.
#[derive(Debug, Serialize, Deserialize, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum DeploymentStatus {
    /// This deployment is not the leader. It is initializing and is not yet
    /// ready to become the leader.
    Initializing,
    /// This deployment is not the leader, but it is ready to become the leader.
    ReadyToPromote,
    /// This deployment is in the process of becoming the leader.
    Promoting,
    /// This deployment is the leader.
    IsLeader,
}

#[derive(Debug, Serialize)]
pub struct Resources {
    pub generation: u64,
    pub environmentd_network_policies: Vec<NetworkPolicy>,
    pub service_account: Box<ServiceAccount>,
    pub role: Box<Role>,
    pub role_binding: Box<RoleBinding>,
    pub public_service: Box<Service>,
    pub generation_service: Box<Service>,
    pub persist_pubsub_service: Box<Service>,
    pub environmentd_certificate: Box<Option<Certificate>>,
    pub environmentd_statefulset: Box<StatefulSet>,
}

impl Resources {
    pub fn new(
        config: &super::MaterializeControllerArgs,
        tracing: &TracingCliArgs,
        orchestratord_namespace: &str,
        mz: &Materialize,
        generation: u64,
    ) -> Self {
        let environmentd_network_policies =
            create_environmentd_network_policies(config, mz, orchestratord_namespace);

        let service_account = Box::new(create_service_account_object(config, mz));
        let role = Box::new(create_role_object(mz));
        let role_binding = Box::new(create_role_binding_object(mz));
        let public_service = Box::new(create_public_service_object(config, mz, generation));
        let generation_service = Box::new(create_generation_service_object(config, mz, generation));
        let persist_pubsub_service =
            Box::new(create_persist_pubsub_service(config, mz, generation));
        let environmentd_certificate = Box::new(create_environmentd_certificate(config, mz));
        let environmentd_statefulset = Box::new(create_environmentd_statefulset_object(
            config, tracing, mz, generation,
        ));

        Self {
            generation,
            environmentd_network_policies,
            service_account,
            role,
            role_binding,
            public_service,
            generation_service,
            persist_pubsub_service,
            environmentd_certificate,
            environmentd_statefulset,
        }
    }

    #[instrument]
    pub async fn apply(
        &self,
        client: &Client,
        args: &super::MaterializeControllerArgs,
        increment_generation: bool,
        namespace: &str,
    ) -> Result<Option<Action>, anyhow::Error> {
        let environmentd_network_policy_api: Api<NetworkPolicy> =
            Api::namespaced(client.clone(), namespace);
        let service_api: Api<Service> = Api::namespaced(client.clone(), namespace);
        let service_account_api: Api<ServiceAccount> = Api::namespaced(client.clone(), namespace);
        let role_api: Api<Role> = Api::namespaced(client.clone(), namespace);
        let role_binding_api: Api<RoleBinding> = Api::namespaced(client.clone(), namespace);
        let statefulset_api: Api<StatefulSet> = Api::namespaced(client.clone(), namespace);
        let pod_api: Api<Pod> = Api::namespaced(client.clone(), namespace);
        let certificate_api: Api<Certificate> = Api::namespaced(client.clone(), namespace);

        for policy in &self.environmentd_network_policies {
            trace!("applying network policy {}", policy.name_unchecked());
            apply_resource(&environmentd_network_policy_api, policy).await?;
        }

        trace!("applying environmentd service account");
        apply_resource(&service_account_api, &*self.service_account).await?;

        trace!("applying environmentd role");
        apply_resource(&role_api, &*self.role).await?;

        trace!("applying environmentd role binding");
        apply_resource(&role_binding_api, &*self.role_binding).await?;

        trace!("applying environmentd per-generation service");
        apply_resource(&service_api, &*self.generation_service).await?;

        trace!("creating persist pubsub service");
        apply_resource(&service_api, &*self.persist_pubsub_service).await?;

        if let Some(certificate) = &*self.environmentd_certificate {
            trace!("creating new environmentd certificate");
            apply_resource(&certificate_api, certificate).await?;
        }

        trace!("creating new environmentd statefulset");
        apply_resource(&statefulset_api, &*self.environmentd_statefulset).await?;

        // until we have full zero downtime upgrades, we have a tradeoff: if
        // we use the graceful upgrade mechanism, we minimize environmentd
        // unavailability but require a full clusterd rehydration every time,
        // and if we use the in-place upgrade mechanism, we cause a few
        // minutes of environmentd downtime, but as long as the environmentd
        // version didn't change, the existing clusterds will remain running
        // and won't need to rehydrate. during a version bump, the tradeoff
        // here is obvious (we need to rehydrate either way, so minimizing
        // environmentd downtime in the meantime is strictly better), but if
        // we need to force a rollout some other time (for instance, to
        // increase the environmentd memory request, or something like that),
        // it is often better to accept the environmentd unavailability in
        // order to get the environment as a whole back to a working state
        // sooner. once clusterd rehydration gets moved ahead of the leader
        // promotion step, this will no longer make a difference and we can
        // remove the extra codepath.
        if increment_generation {
            let retry_action = Action::requeue(Duration::from_secs(thread_rng().gen_range(5..10)));

            let statefulset = get_resource(
                &statefulset_api,
                &self.environmentd_statefulset.name_unchecked(),
            )
            .await?;
            if statefulset
                .and_then(|statefulset| statefulset.status)
                .and_then(|status| status.ready_replicas)
                .unwrap_or(0)
                == 0
            {
                trace!("environmentd statefulset is not ready yet...");
                return Ok(Some(retry_action));
            }

            let environmentd_url =
                environmentd_internal_http_address(args, namespace, &*self.generation_service);

            let http_client = reqwest::Client::builder()
                .timeout(std::time::Duration::from_secs(10))
                .build()
                .unwrap();
            let status_url =
                reqwest::Url::parse(&format!("http://{}/api/leader/status", environmentd_url))
                    .unwrap();

            match http_client.get(status_url.clone()).send().await {
                Ok(response) => {
                    let response: BTreeMap<String, DeploymentStatus> = response.json().await?;
                    if response["status"] == DeploymentStatus::Initializing {
                        trace!("environmentd is still initializing, retrying...");
                        return Ok(Some(retry_action));
                    } else {
                        trace!("environmentd is ready");
                    }
                }
                Err(e) => {
                    trace!("failed to connect to environmentd, retrying... ({e})");
                    return Ok(Some(retry_action));
                }
            }

            let promote_url =
                reqwest::Url::parse(&format!("http://{}/api/leader/promote", environmentd_url))
                    .unwrap();

            // !!!!!!!!!!!!!!!!!!!!!!!!!!! WARNING !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
            // It is absolutely critical that this promotion is done last!
            //
            // If there are any failures in this method, the error handler in
            // the caller will attempt to revert and delete the new environmentd.
            // After promotion, the new environmentd is active, so that would
            // cause an outage!
            // !!!!!!!!!!!!!!!!!!!!!!!!!!! WARNING !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
            trace!("promoting new environmentd to leader");
            let response = http_client.post(promote_url).send().await?;
            let response: BecomeLeaderResponse = response.json().await?;
            if let BecomeLeaderResult::Failure { message } = response.result {
                bail!("failed to promote new environmentd: {message}");
            }

            // A successful POST to the promotion endpoint only indicates
            // that the promotion process was kicked off. It does not
            // guarantee that the environment will be successfully promoted
            // (e.g., if the environment crashes immediately after responding
            // to the request, but before executing the takeover, the
            // promotion will be lost).
            //
            // To guarantee the environment has been promoted successfully,
            // we must wait to see at least one `IsLeader` status returned
            // from the environment.

            match http_client.get(status_url.clone()).send().await {
                Ok(response) => {
                    let response: BTreeMap<String, DeploymentStatus> = response.json().await?;
                    if response["status"] != DeploymentStatus::IsLeader {
                        trace!(
                            "environmentd is still promoting (status: {:?}), retrying...",
                            response["status"]
                        );
                        return Ok(Some(retry_action));
                    } else {
                        trace!("environmentd is ready");
                    }
                }
                Err(e) => {
                    trace!("failed to connect to environmentd, retrying... ({e})");
                    return Ok(Some(retry_action));
                }
            }
        } else {
            trace!("restarting environmentd pod to pick up statefulset changes");
            delete_resource(
                &pod_api,
                &statefulset_pod_name(&*self.environmentd_statefulset, 0),
            )
            .await?;
        }

        Ok(None)
    }

    #[instrument]
    pub async fn promote_services(
        &self,
        client: &Client,
        namespace: &str,
    ) -> Result<(), anyhow::Error> {
        let service_api: Api<Service> = Api::namespaced(client.clone(), namespace);

        trace!("applying environmentd public service");
        apply_resource(&service_api, &*self.public_service).await?;

        Ok(())
    }

    #[instrument]
    pub async fn teardown_generation(
        &self,
        client: &Client,
        mz: &Materialize,
        generation: u64,
    ) -> Result<(), anyhow::Error> {
        let service_api: Api<Service> = Api::namespaced(client.clone(), &mz.namespace());
        let statefulset_api: Api<StatefulSet> = Api::namespaced(client.clone(), &mz.namespace());

        trace!("deleting environmentd statefulset for generation {generation}");
        delete_resource(
            &statefulset_api,
            &mz.environmentd_statefulset_name(generation),
        )
        .await?;

        trace!("deleting persist pubsub service for generation {generation}");
        delete_resource(&service_api, &mz.persist_pubsub_service_name(generation)).await?;

        trace!("deleting environmentd per-generation service for generation {generation}");
        delete_resource(
            &service_api,
            &mz.environmentd_generation_service_name(generation),
        )
        .await?;

        Ok(())
    }

    // ideally we would just be able to hash the objects directly, but the
    // generated kubernetes objects don't implement the Hash trait
    pub fn generate_hash(&self) -> String {
        let mut hasher = Sha256::new();
        hasher.update(&serde_json::to_string(self).unwrap());
        format!("{:x}", hasher.finalize())
    }
}

fn create_environmentd_network_policies(
    config: &super::MaterializeControllerArgs,
    mz: &Materialize,
    orchestratord_namespace: &str,
) -> Vec<NetworkPolicy> {
    let mut network_policies = Vec::new();
    if config.network_policies.internal_enabled {
        let environmentd_label_selector = LabelSelector {
            match_labels: Some(
                mz.default_labels()
                    .into_iter()
                    .chain([(
                        "materialize.cloud/app".to_owned(),
                        mz.environmentd_app_name(),
                    )])
                    .collect(),
            ),
            ..Default::default()
        };
        let orchestratord_label_selector = LabelSelector {
            match_labels: Some(
                config
                    .orchestratord_pod_selector_labels
                    .iter()
                    .cloned()
                    .map(|kv| (kv.key, kv.value))
                    .collect(),
            ),
            ..Default::default()
        };
        // TODO (Alex) filter to just clusterd and environmentd,
        // once we get a consistent set of labels for both.
        let all_pods_label_selector = LabelSelector {
            match_labels: Some(mz.default_labels()),
            ..Default::default()
        };
        network_policies.extend([
            // Allow all clusterd/environmentd traffic (between pods in the
            // same environment)
            NetworkPolicy {
                metadata: mz
                    .managed_resource_meta(mz.name_prefixed("allow-all-within-environment")),
                spec: Some(NetworkPolicySpec {
                    egress: Some(vec![NetworkPolicyEgressRule {
                        to: Some(vec![NetworkPolicyPeer {
                            pod_selector: Some(all_pods_label_selector.clone()),
                            ..Default::default()
                        }]),
                        ..Default::default()
                    }]),
                    ingress: Some(vec![NetworkPolicyIngressRule {
                        from: Some(vec![NetworkPolicyPeer {
                            pod_selector: Some(all_pods_label_selector.clone()),
                            ..Default::default()
                        }]),
                        ..Default::default()
                    }]),
                    pod_selector: all_pods_label_selector.clone(),
                    policy_types: Some(vec!["Ingress".to_owned(), "Egress".to_owned()]),
                    ..Default::default()
                }),
            },
            // Allow traffic from orchestratord to environmentd in order to hit
            // the promotion endpoints during upgrades
            NetworkPolicy {
                metadata: mz.managed_resource_meta(mz.name_prefixed("allow-orchestratord")),
                spec: Some(NetworkPolicySpec {
                    ingress: Some(vec![NetworkPolicyIngressRule {
                        from: Some(vec![NetworkPolicyPeer {
                            namespace_selector: Some(LabelSelector {
                                match_labels: Some(btreemap! {
                                    "kubernetes.io/metadata.name".into()
                                        => orchestratord_namespace.into(),
                                }),
                                ..Default::default()
                            }),
                            pod_selector: Some(orchestratord_label_selector),
                            ..Default::default()
                        }]),
                        ports: Some(vec![NetworkPolicyPort {
                            port: Some(IntOrString::Int(config.environmentd_internal_http_port)),
                            protocol: Some("TCP".to_string()),
                            ..Default::default()
                        }]),
                        ..Default::default()
                    }]),
                    pod_selector: environmentd_label_selector,
                    policy_types: Some(vec!["Ingress".to_owned()]),
                    ..Default::default()
                }),
            },
        ]);
    }
    if config.network_policies.ingress_enabled {
        let mut ingress_label_selector = mz.default_labels();
        ingress_label_selector.insert("materialize.cloud/app".to_owned(), mz.balancerd_app_name());
        network_policies.extend([NetworkPolicy {
            metadata: mz.managed_resource_meta(mz.name_prefixed("sql-and-http-ingress")),
            spec: Some(NetworkPolicySpec {
                ingress: Some(vec![NetworkPolicyIngressRule {
                    from: Some(
                        config
                            .network_policies
                            .ingress_cidrs
                            .iter()
                            .map(|cidr| NetworkPolicyPeer {
                                ip_block: Some(IPBlock {
                                    cidr: cidr.to_owned(),
                                    except: None,
                                }),
                                ..Default::default()
                            })
                            .collect(),
                    ),
                    ports: Some(vec![
                        NetworkPolicyPort {
                            port: Some(IntOrString::Int(config.environmentd_http_port)),
                            protocol: Some("TCP".to_string()),
                            ..Default::default()
                        },
                        NetworkPolicyPort {
                            port: Some(IntOrString::Int(config.environmentd_sql_port)),
                            protocol: Some("TCP".to_string()),
                            ..Default::default()
                        },
                    ]),
                    ..Default::default()
                }]),
                pod_selector: LabelSelector {
                    match_expressions: None,
                    match_labels: Some(ingress_label_selector),
                },
                policy_types: Some(vec!["Ingress".to_owned()]),
                ..Default::default()
            }),
        }]);
    }
    if config.network_policies.egress_enabled {
        network_policies.extend([NetworkPolicy {
            metadata: mz.managed_resource_meta(mz.name_prefixed("sources-and-sinks-egress")),
            spec: Some(NetworkPolicySpec {
                egress: Some(vec![NetworkPolicyEgressRule {
                    to: Some(
                        config
                            .network_policies
                            .egress_cidrs
                            .iter()
                            .map(|cidr| NetworkPolicyPeer {
                                ip_block: Some(IPBlock {
                                    cidr: cidr.to_owned(),
                                    except: None,
                                }),
                                ..Default::default()
                            })
                            .collect(),
                    ),
                    ..Default::default()
                }]),
                pod_selector: LabelSelector {
                    match_expressions: None,
                    match_labels: Some(mz.default_labels()),
                },
                policy_types: Some(vec!["Egress".to_owned()]),
                ..Default::default()
            }),
        }]);
    }
    network_policies
}

fn create_service_account_object(
    config: &super::MaterializeControllerArgs,
    mz: &Materialize,
) -> ServiceAccount {
    let annotations = if config.cloud_provider == CloudProvider::Aws {
        let role_arn = mz
            .spec
            .environmentd_iam_role_arn
            .as_deref()
            .or(config.aws_info.environmentd_iam_role_arn.as_deref())
            .unwrap()
            .to_string();
        Some(btreemap! {
            "eks.amazonaws.com/role-arn".to_string() => role_arn
        })
    } else {
        None
    };
    ServiceAccount {
        metadata: ObjectMeta {
            annotations,
            ..mz.managed_resource_meta(mz.service_account_name())
        },
        ..Default::default()
    }
}

fn create_role_object(mz: &Materialize) -> Role {
    Role {
        metadata: mz.managed_resource_meta(mz.role_name()),
        rules: Some(vec![
            PolicyRule {
                api_groups: Some(vec!["apps".to_string()]),
                resources: Some(vec!["statefulsets".to_string()]),
                verbs: vec![
                    "get".to_string(),
                    "list".to_string(),
                    "watch".to_string(),
                    "create".to_string(),
                    "update".to_string(),
                    "patch".to_string(),
                    "delete".to_string(),
                ],
                ..Default::default()
            },
            PolicyRule {
                api_groups: Some(vec!["".to_string()]),
                resources: Some(vec![
                    "persistentvolumeclaims".to_string(),
                    "pods".to_string(),
                    "secrets".to_string(),
                    "services".to_string(),
                ]),
                verbs: vec![
                    "get".to_string(),
                    "list".to_string(),
                    "watch".to_string(),
                    "create".to_string(),
                    "update".to_string(),
                    "patch".to_string(),
                    "delete".to_string(),
                ],
                ..Default::default()
            },
            PolicyRule {
                api_groups: Some(vec!["".to_string()]),
                resources: Some(vec!["configmaps".to_string()]),
                verbs: vec!["get".to_string()],
                ..Default::default()
            },
            PolicyRule {
                api_groups: Some(vec!["materialize.cloud".to_string()]),
                resources: Some(vec!["vpcendpoints".to_string()]),
                verbs: vec![
                    "get".to_string(),
                    "list".to_string(),
                    "watch".to_string(),
                    "create".to_string(),
                    "update".to_string(),
                    "patch".to_string(),
                    "delete".to_string(),
                ],
                ..Default::default()
            },
            PolicyRule {
                api_groups: Some(vec!["metrics.k8s.io".to_string()]),
                resources: Some(vec!["pods".to_string()]),
                verbs: vec!["get".to_string(), "list".to_string()],
                ..Default::default()
            },
            PolicyRule {
                api_groups: Some(vec!["custom.metrics.k8s.io".to_string()]),
                resources: Some(vec![
                    "persistentvolumeclaims/kubelet_volume_stats_used_bytes".to_string(),
                    "persistentvolumeclaims/kubelet_volume_stats_capacity_bytes".to_string(),
                ]),
                verbs: vec!["get".to_string()],
                ..Default::default()
            },
        ]),
    }
}

fn create_role_binding_object(mz: &Materialize) -> RoleBinding {
    RoleBinding {
        metadata: mz.managed_resource_meta(mz.role_binding_name()),
        role_ref: RoleRef {
            api_group: "".to_string(),
            kind: "Role".to_string(),
            name: mz.role_name(),
        },
        subjects: Some(vec![Subject {
            api_group: Some("".to_string()),
            kind: "ServiceAccount".to_string(),
            name: mz.service_account_name(),
            namespace: Some(mz.namespace()),
        }]),
    }
}

fn create_public_service_object(
    config: &super::MaterializeControllerArgs,
    mz: &Materialize,
    generation: u64,
) -> Service {
    create_base_service_object(config, mz, generation, &mz.environmentd_service_name())
}

fn create_generation_service_object(
    config: &super::MaterializeControllerArgs,
    mz: &Materialize,
    generation: u64,
) -> Service {
    create_base_service_object(
        config,
        mz,
        generation,
        &mz.environmentd_generation_service_name(generation),
    )
}

fn create_base_service_object(
    config: &super::MaterializeControllerArgs,
    mz: &Materialize,
    generation: u64,
    service_name: &str,
) -> Service {
    let ports = vec![
        ServicePort {
            port: config.environmentd_sql_port,
            protocol: Some("TCP".to_string()),
            name: Some("sql".to_string()),
            ..Default::default()
        },
        ServicePort {
            port: config.environmentd_http_port,
            protocol: Some("TCP".to_string()),
            name: Some("https".to_string()),
            ..Default::default()
        },
        ServicePort {
            port: config.environmentd_internal_sql_port,
            protocol: Some("TCP".to_string()),
            name: Some("internal-sql".to_string()),
            ..Default::default()
        },
        ServicePort {
            port: config.environmentd_internal_http_port,
            protocol: Some("TCP".to_string()),
            name: Some("internal-http".to_string()),
            ..Default::default()
        },
    ];

    let selector = btreemap! {"materialize.cloud/name".to_string() => mz.environmentd_statefulset_name(generation)};

    let spec = ServiceSpec {
        type_: Some("ClusterIP".to_string()),
        cluster_ip: Some("None".to_string()),
        selector: Some(selector),
        ports: Some(ports),
        ..Default::default()
    };

    Service {
        metadata: mz.managed_resource_meta(service_name.to_string()),
        spec: Some(spec),
        status: None,
    }
}

fn create_persist_pubsub_service(
    config: &super::MaterializeControllerArgs,
    mz: &Materialize,
    generation: u64,
) -> Service {
    Service {
        metadata: mz.managed_resource_meta(mz.persist_pubsub_service_name(generation)),
        spec: Some(ServiceSpec {
            type_: Some("ClusterIP".to_string()),
            cluster_ip: Some("None".to_string()),
            selector: Some(btreemap! {
                "materialize.cloud/name".to_string() => mz.environmentd_statefulset_name(generation),
            }),
            ports: Some(vec![ServicePort {
                name: Some("grpc".to_string()),
                protocol: Some("TCP".to_string()),
                port: config.environmentd_internal_persist_pubsub_port,
                ..Default::default()
            }]),
            ..Default::default()
        }),
        status: None,
    }
}

fn create_environmentd_certificate(
    config: &super::MaterializeControllerArgs,
    mz: &Materialize,
) -> Option<Certificate> {
    create_certificate(
        config.default_certificate_specs.internal.clone(),
        mz,
        mz.spec.internal_certificate_spec.clone(),
        mz.environmentd_certificate_name(),
        mz.environmentd_certificate_secret_name(),
        Some(vec![
            mz.environmentd_service_name(),
            mz.environmentd_service_internal_fqdn(),
        ]),
    )
}

fn create_environmentd_statefulset_object(
    config: &super::MaterializeControllerArgs,
    tracing: &TracingCliArgs,
    mz: &Materialize,
    generation: u64,
) -> StatefulSet {
    // IMPORTANT: Only pass secrets via environment variables. All other
    // parameters should be passed as command line arguments, possibly gated
    // with a `meets_minimum_version` call. This ensures typos cause
    // `environmentd` to fail to start, rather than silently ignoring the
    // configuration.
    //
    // When passing a secret, use a `SecretKeySelector` to forward a secret into
    // the pod. Do *not* hardcode a secret as the value directly, as doing so
    // will leak the secret to anyone with permission to describe the pod.
    let mut env = vec![
        EnvVar {
            name: "MZ_METADATA_BACKEND_URL".to_string(),
            value_from: Some(EnvVarSource {
                secret_key_ref: Some(SecretKeySelector {
                    name: Some(mz.backend_secret_name()),
                    key: "metadata_backend_url".to_string(),
                    optional: Some(false),
                }),
                ..Default::default()
            }),
            ..Default::default()
        },
        EnvVar {
            name: "MZ_PERSIST_BLOB_URL".to_string(),
            value_from: Some(EnvVarSource {
                secret_key_ref: Some(SecretKeySelector {
                    name: Some(mz.backend_secret_name()),
                    key: "persist_backend_url".to_string(),
                    optional: Some(false),
                }),
                ..Default::default()
            }),
            ..Default::default()
        },
    ];

    env.push(EnvVar {
        name: "AWS_REGION".to_string(),
        value: Some(config.region.clone()),
        ..Default::default()
    });

    env.extend(mz.spec.environmentd_extra_env.iter().flatten().cloned());

    let mut args = vec![];

    if let Some(helm_chart_version) = &config.helm_chart_version {
        args.push(format!("--helm-chart-version={helm_chart_version}"));
    }

    // Add environment ID argument.
    args.push(format!(
        "--environment-id={}",
        mz.environment_id(&config.cloud_provider.to_string(), &config.region)
    ));

    // Add clusterd image argument based on environmentd tag.
    args.push(format!(
        "--clusterd-image={}",
        matching_image_from_environmentd_image_ref(
            &mz.spec.environmentd_image_ref,
            "clusterd",
            None
        )
    ));

    // Add cluster and storage host size arguments.
    args.extend(
        [
            config
                .environmentd_cluster_replica_sizes
                .as_ref()
                .map(|sizes| format!("--cluster-replica-sizes={sizes}")),
            config
                .bootstrap_default_cluster_replica_size
                .as_ref()
                .map(|size| format!("--bootstrap-default-cluster-replica-size={size}")),
            config
                .bootstrap_builtin_system_cluster_replica_size
                .as_ref()
                .map(|size| format!("--bootstrap-builtin-system-cluster-replica-size={size}")),
            config
                .bootstrap_builtin_probe_cluster_replica_size
                .as_ref()
                .map(|size| format!("--bootstrap-builtin-probe-cluster-replica-size={size}")),
            config
                .bootstrap_builtin_support_cluster_replica_size
                .as_ref()
                .map(|size| format!("--bootstrap-builtin-support-cluster-replica-size={size}")),
            config
                .bootstrap_builtin_catalog_server_cluster_replica_size
                .as_ref()
                .map(|size| {
                    format!("--bootstrap-builtin-catalog-server-cluster-replica-size={size}")
                }),
            config
                .bootstrap_builtin_analytics_cluster_replica_size
                .as_ref()
                .map(|size| format!("--bootstrap-builtin-analytics-cluster-replica-size={size}")),
        ]
        .into_iter()
        .flatten(),
    );

    // Add networking arguments.
    args.extend([
        format!("--sql-listen-addr=0.0.0.0:{}", config.environmentd_sql_port),
        format!(
            "--http-listen-addr=0.0.0.0:{}",
            config.environmentd_http_port
        ),
        format!(
            "--internal-sql-listen-addr=0.0.0.0:{}",
            config.environmentd_internal_sql_port
        ),
        format!(
            "--internal-http-listen-addr=0.0.0.0:{}",
            config.environmentd_internal_http_port
        ),
    ]);

    args.extend(
        config
            .environmentd_allowed_origins
            .iter()
            .map(|origin| format!("--cors-allowed-origin={}", origin.to_str().unwrap())),
    );

    args.push(format!(
        "--secrets-controller={}",
        config.secrets_controller
    ));

    if let Some(cluster_replica_sizes) = &config.environmentd_cluster_replica_sizes {
        if let Ok(cluster_replica_sizes) =
            serde_json::from_str::<BTreeMap<String, serde_json::Value>>(cluster_replica_sizes)
        {
            let cluster_replica_sizes: Vec<_> =
                cluster_replica_sizes.keys().map(|s| s.as_str()).collect();
            args.push(format!(
                "--system-parameter-default=allowed_cluster_replica_sizes='{}'",
                cluster_replica_sizes.join("', '")
            ));
        }
    }
    if !config.cloud_provider.is_cloud() {
        args.push("--system-parameter-default=cluster_enable_topology_spread=false".into());
    }

    if config.enable_internal_statement_logging {
        args.push("--system-parameter-default=enable_internal_statement_logging=true".into());
    }
    if config.disable_authentication {
        args.push("--system-parameter-default=enable_rbac_checks=false".into());
    }

    // Add persist arguments.

    // Configure the Persist Isolated Runtime to use one less thread than the total available.
    args.push("--persist-isolated-runtime-threads=-1".to_string());

    // Add AWS arguments.
    if config.cloud_provider == CloudProvider::Aws {
        if let Some(azs) = config.aws_info.environmentd_availability_zones.as_ref() {
            for az in azs {
                args.push(format!("--availability-zone={az}"));
            }
        }

        if let Some(environmentd_connection_role_arn) = mz
            .spec
            .environmentd_connection_role_arn
            .as_deref()
            .or(config.aws_info.environmentd_connection_role_arn.as_deref())
        {
            args.push(format!(
                "--aws-connection-role-arn={}",
                environmentd_connection_role_arn
            ));
        }
        if let Some(account_id) = &config.aws_info.aws_account_id {
            args.push(format!("--aws-account-id={account_id}"));
        }

        args.extend([format!(
            "--aws-secrets-controller-tags=Environment={}",
            mz.name_unchecked()
        )]);
        args.extend_from_slice(&config.aws_info.aws_secrets_controller_tags);
    }

    // Add Kubernetes arguments.
    args.extend([
        "--orchestrator=kubernetes".into(),
        format!(
            "--orchestrator-kubernetes-service-account={}",
            &mz.service_account_name()
        ),
        format!(
            "--orchestrator-kubernetes-image-pull-policy={}",
            config.image_pull_policy.as_kebab_case_str(),
        ),
    ]);
    for selector in &config.clusterd_node_selector {
        args.push(format!(
            "--orchestrator-kubernetes-service-node-selector={}={}",
            selector.key, selector.value,
        ));
    }
    if let Some(scheduler_name) = &config.scheduler_name {
        args.push(format!(
            "--orchestrator-kubernetes-scheduler-name={}",
            scheduler_name
        ));
    }
    for (key, val) in mz.default_labels() {
        args.push(format!(
            "--orchestrator-kubernetes-service-label={key}={val}"
        ));
    }
    if let Some(status) = &mz.status {
        args.push(format!(
            "--orchestrator-kubernetes-name-prefix=mz{}-",
            status.resource_id
        ));
    }

    // Add logging and tracing arguments.
    args.extend(["--log-format=json".into()]);
    if let Some(endpoint) = &tracing.opentelemetry_endpoint {
        args.extend([
            format!("--opentelemetry-endpoint={}", endpoint),
            format!(
                "--opentelemetry-resource=environment_name={}",
                mz.name_unchecked()
            ),
        ]);
    }

    if let Some(segment_api_key) = &config.segment_api_key {
        args.push(format!("--segment-api-key={}", segment_api_key));
        if config.segment_client_side {
            args.push("--segment-client-side".into());
        }
    }

    let mut volumes = Vec::new();
    let mut volume_mounts = Vec::new();
    if issuer_ref_defined(
        &config.default_certificate_specs.internal,
        &mz.spec.internal_certificate_spec,
    ) {
        volumes.push(Volume {
            name: "certificate".to_owned(),
            secret: Some(SecretVolumeSource {
                default_mode: Some(0o400),
                secret_name: Some(mz.environmentd_certificate_secret_name()),
                items: None,
                optional: Some(false),
            }),
            ..Default::default()
        });
        volume_mounts.push(VolumeMount {
            name: "certificate".to_owned(),
            mount_path: "/etc/materialized".to_owned(),
            read_only: Some(true),
            ..Default::default()
        });
        args.extend([
            "--tls-mode=require".into(),
            "--tls-cert=/etc/materialized/tls.crt".into(),
            "--tls-key=/etc/materialized/tls.key".into(),
        ]);
    } else {
        args.push("--tls-mode=disable".to_string());
    }
    if let Some(ephemeral_volume_class) = &config.ephemeral_volume_class {
        args.extend([
            format!(
                "--orchestrator-kubernetes-ephemeral-volume-class={}",
                ephemeral_volume_class
            ),
            "--scratch-directory=/scratch".to_string(),
        ]);
        volumes.push(Volume {
            name: "scratch".to_string(),
            ephemeral: Some(EphemeralVolumeSource {
                volume_claim_template: Some(PersistentVolumeClaimTemplate {
                    spec: PersistentVolumeClaimSpec {
                        access_modes: Some(vec!["ReadWriteOnce".to_string()]),
                        storage_class_name: Some(ephemeral_volume_class.to_string()),
                        resources: Some(VolumeResourceRequirements {
                            requests: Some(BTreeMap::from([(
                                "storage".to_string(),
                                mz.environmentd_scratch_volume_storage_requirement(),
                            )])),
                            ..Default::default()
                        }),
                        ..Default::default()
                    },
                    ..Default::default()
                }),
                ..Default::default()
            }),
            ..Default::default()
        });
        volume_mounts.push(VolumeMount {
            name: "scratch".to_string(),
            mount_path: "/scratch".to_string(),
            ..Default::default()
        });
    }
    // The `materialize` user used by clusterd always has gid 999.
    args.push("--orchestrator-kubernetes-service-fs-group=999".to_string());

    // Add Sentry arguments.
    if let Some(sentry_dsn) = &tracing.sentry_dsn {
        args.push(format!("--sentry-dsn={}", sentry_dsn));
        if let Some(sentry_environment) = &tracing.sentry_environment {
            args.push(format!("--sentry-environment={}", sentry_environment));
        }
        args.push(format!("--sentry-tag=region={}", config.region));
    }

    // Add Persist PubSub arguments
    args.push(format!(
        "--persist-pubsub-url=http://{}:{}",
        mz.persist_pubsub_service_name(generation),
        config.environmentd_internal_persist_pubsub_port,
    ));
    args.push(format!(
        "--internal-persist-pubsub-listen-addr=0.0.0.0:{}",
        config.environmentd_internal_persist_pubsub_port
    ));

    args.push(format!("--deploy-generation={}", generation));

    // Add URL for internal user impersonation endpoint
    args.push(format!(
        "--internal-console-redirect-url={}",
        &config.internal_console_proxy_url,
    ));

    if !config.collect_pod_metrics {
        args.push("--orchestrator-kubernetes-disable-pod-metrics-collection".into());
    }
    if config.enable_prometheus_scrape_annotations {
        args.push("--orchestrator-kubernetes-enable-prometheus-scrape-annotations".into());
    }

    // Add user-specified extra arguments.
    if let Some(extra_args) = &mz.spec.environmentd_extra_args {
        args.extend(extra_args.iter().cloned());
    }

    let probe = Probe {
        initial_delay_seconds: Some(1),
        failure_threshold: Some(12),
        tcp_socket: Some(TCPSocketAction {
            host: None,
            port: IntOrString::Int(config.environmentd_sql_port),
        }),
        ..Default::default()
    };

    let security_context = if config.enable_security_context {
        // Since we want to adhere to the most restrictive security context, all
        // of these fields have to be set how they are.
        // See https://kubernetes.io/docs/concepts/security/pod-security-standards/#restricted
        Some(SecurityContext {
            run_as_non_root: Some(true),
            capabilities: Some(Capabilities {
                drop: Some(vec!["ALL".to_string()]),
                ..Default::default()
            }),
            seccomp_profile: Some(SeccompProfile {
                type_: "RuntimeDefault".to_string(),
                ..Default::default()
            }),
            allow_privilege_escalation: Some(false),
            ..Default::default()
        })
    } else {
        None
    };

    let ports = vec![
        ContainerPort {
            container_port: config.environmentd_sql_port,
            name: Some("sql".to_owned()),
            ..Default::default()
        },
        ContainerPort {
            container_port: config.environmentd_internal_sql_port,
            name: Some("internal-sql".to_owned()),
            ..Default::default()
        },
        ContainerPort {
            container_port: config.environmentd_http_port,
            name: Some("http".to_owned()),
            ..Default::default()
        },
        ContainerPort {
            container_port: config.environmentd_internal_http_port,
            name: Some("internal-http".to_owned()),
            ..Default::default()
        },
        ContainerPort {
            container_port: config.environmentd_internal_persist_pubsub_port,
            name: Some("persist-pubsub".to_owned()),
            ..Default::default()
        },
    ];

    let container = Container {
        name: "environmentd".to_owned(),
        image: Some(mz.spec.environmentd_image_ref.to_owned()),
        image_pull_policy: Some(config.image_pull_policy.to_string()),
        ports: Some(ports),
        args: Some(args),
        env: Some(env),
        volume_mounts: Some(volume_mounts),
        liveness_probe: Some(probe.clone()),
        readiness_probe: Some(probe),
        resources: mz.spec.environmentd_resource_requirements.clone(),
        security_context: security_context.clone(),
        ..Default::default()
    };

    let mut pod_template_labels = mz.default_labels();
    pod_template_labels.insert(
        "materialize.cloud/name".to_owned(),
        mz.environmentd_statefulset_name(generation),
    );
    pod_template_labels.insert(
        "materialize.cloud/app".to_owned(),
        mz.environmentd_app_name(),
    );
    pod_template_labels.insert("app".to_owned(), "environmentd".to_string());

    let mut pod_template_annotations = btreemap! {
        // We can re-enable eviction once we have HA
        "cluster-autoscaler.kubernetes.io/safe-to-evict".to_owned() => "false".to_string(),

        // Prevents old (< 0.30) and new versions of karpenter from evicting database pods
        "karpenter.sh/do-not-evict".to_owned() => "true".to_string(),
        "karpenter.sh/do-not-disrupt".to_owned() => "true".to_string(),
        "materialize.cloud/generation".to_owned() => generation.to_string(),
    };
    if config.enable_prometheus_scrape_annotations {
        pod_template_annotations.insert("prometheus.io/scrape".to_owned(), "true".to_string());
        pod_template_annotations.insert(
            "prometheus.io/port".to_owned(),
            config.environmentd_internal_http_port.to_string(),
        );
        pod_template_annotations.insert("prometheus.io/path".to_owned(), "/metrics".to_string());
        pod_template_annotations.insert("prometheus.io/scheme".to_owned(), "http".to_string());
        pod_template_annotations.insert(
            "materialize.prometheus.io/mz_usage_path".to_owned(),
            "/metrics/mz_usage".to_string(),
        );
        pod_template_annotations.insert(
            "materialize.prometheus.io/mz_frontier_path".to_owned(),
            "/metrics/mz_frontier".to_string(),
        );
        pod_template_annotations.insert(
            "materialize.prometheus.io/mz_compute_path".to_owned(),
            "/metrics/mz_compute".to_string(),
        );
        pod_template_annotations.insert(
            "materialize.prometheus.io/mz_storage_path".to_owned(),
            "/metrics/mz_storage".to_string(),
        );
    }

    let tolerations = Some(vec![
        // When the node becomes `NotReady` it indicates there is a problem with the node,
        // By default kubernetes waits 300s (5 minutes) before doing anything in this case,
        // But we want to limit this to 30s for faster recovery
        Toleration {
            effect: Some("NoExecute".into()),
            key: Some("node.kubernetes.io/not-ready".into()),
            operator: Some("Exists".into()),
            toleration_seconds: Some(30),
            value: None,
        },
        Toleration {
            effect: Some("NoExecute".into()),
            key: Some("node.kubernetes.io/unreachable".into()),
            operator: Some("Exists".into()),
            toleration_seconds: Some(30),
            value: None,
        },
    ]);

    let pod_template_spec = PodTemplateSpec {
        // not using managed_resource_meta because the pod should be owned
        // by the statefulset, not the materialize instance
        metadata: Some(ObjectMeta {
            labels: Some(pod_template_labels),
            annotations: Some(pod_template_annotations), // This is inserted into later, do not delete.
            ..Default::default()
        }),
        spec: Some(PodSpec {
            containers: vec![container],
            node_selector: Some(
                config
                    .environmentd_node_selector
                    .iter()
                    .map(|selector| (selector.key.clone(), selector.value.clone()))
                    .collect(),
            ),
            scheduler_name: config.scheduler_name.clone(),
            service_account_name: Some(mz.service_account_name()),
            volumes: Some(volumes),
            security_context: Some(PodSecurityContext {
                fs_group: Some(999),
                run_as_user: Some(999),
                run_as_group: Some(999),
                ..Default::default()
            }),
            tolerations,
            // This (apparently) has the side effect of automatically starting a new pod
            // when the previous pod is currently terminating. This side steps the statefulset fencing
            // but allows for quicker recovery from a failed node
            // The Kubernetes documentation strongly advises against this
            // setting, as StatefulSets attempt to provide "at most once"
            // semantics [0]-- that is, the guarantee that for a given pod in a
            // StatefulSet there is *at most* one pod with that identity running
            // in the cluster
            //
            // Materialize, however, has been carefully designed to *not* rely
            // on this guarantee. (In fact, we do not believe that correct
            // distributed systems can meaningfully rely on Kubernetes's
            // guarantee--network packets from a pod can be arbitrarily delayed,
            // long past that pod's termination.) Running two `environmentd`
            // processes is safe: the newer process will safely and correctly
            // fence out the older process via CockroachDB. In the future,
            // we'll support running multiple `environmentd` processes in
            // parallel for high availability.
            //
            // [0]: https://kubernetes.io/docs/tasks/run-application/force-delete-stateful-set-pod/#statefulset-considerations
            termination_grace_period_seconds: Some(0),
            ..Default::default()
        }),
    };

    let mut match_labels = BTreeMap::new();
    match_labels.insert(
        "materialize.cloud/name".to_owned(),
        mz.environmentd_statefulset_name(generation),
    );

    let statefulset_spec = StatefulSetSpec {
        replicas: Some(1),
        template: pod_template_spec,
        update_strategy: Some(StatefulSetUpdateStrategy {
            rolling_update: None,
            type_: Some("OnDelete".to_owned()),
        }),
        service_name: mz.environmentd_service_name(),
        selector: LabelSelector {
            match_expressions: None,
            match_labels: Some(match_labels),
        },
        ..Default::default()
    };

    StatefulSet {
        metadata: ObjectMeta {
            annotations: Some(btreemap! {
                "materialize.cloud/generation".to_owned() => generation.to_string(),
                "materialize.cloud/force".to_owned() => mz.spec.force_rollout.to_string(),
            }),
            ..mz.managed_resource_meta(mz.environmentd_statefulset_name(generation))
        },
        spec: Some(statefulset_spec),
        status: None,
    }
}

// see materialize/src/environmentd/src/http.rs
#[derive(Debug, Deserialize, PartialEq, Eq)]
struct BecomeLeaderResponse {
    result: BecomeLeaderResult,
}

#[derive(Debug, Deserialize, PartialEq, Eq)]
enum BecomeLeaderResult {
    Success,
    Failure { message: String },
}

fn environmentd_internal_http_address(
    args: &super::MaterializeControllerArgs,
    namespace: &str,
    generation_service: &Service,
) -> String {
    let host = if let Some(host_override) = &args.environmentd_internal_http_host_override {
        host_override.to_string()
    } else {
        format!(
            "{}.{}.svc.cluster.local",
            generation_service.name_unchecked(),
            namespace,
        )
    };
    format!("{}:{}", host, args.environmentd_internal_http_port)
}

fn statefulset_pod_name(statefulset: &StatefulSet, idx: u64) -> String {
    format!("{}-{}", statefulset.name_unchecked(), idx)
}

// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::{
    collections::BTreeMap,
    net::{IpAddr, Ipv4Addr, SocketAddr},
    sync::LazyLock,
    time::Duration,
};

use k8s_openapi::{
    api::{
        apps::v1::{StatefulSet, StatefulSetSpec},
        core::v1::{
            Capabilities, ConfigMap, ConfigMapVolumeSource, Container, ContainerPort, EnvVar,
            EnvVarSource, KeyToPath, PodSecurityContext, PodSpec, PodTemplateSpec, Probe,
            SeccompProfile, Secret, SecretKeySelector, SecretVolumeSource, SecurityContext,
            Service, ServiceAccount, ServicePort, ServiceSpec, TCPSocketAction, Toleration, Volume,
            VolumeMount,
        },
        networking::v1::{
            IPBlock, NetworkPolicy, NetworkPolicyEgressRule, NetworkPolicyIngressRule,
            NetworkPolicyPeer, NetworkPolicyPort, NetworkPolicySpec,
        },
        rbac::v1::{PolicyRule, Role, RoleBinding, RoleRef, Subject},
    },
    apimachinery::pkg::{apis::meta::v1::LabelSelector, util::intstr::IntOrString},
};
use kube::{Api, Client, ResourceExt, api::ObjectMeta, runtime::controller::Action};
use maplit::btreemap;
use mz_server_core::listeners::{
    AllowedRoles, AuthenticatorKind, BaseListenerConfig, HttpListenerConfig, HttpRoutesEnabled,
    ListenersConfig, SqlListenerConfig,
};
use reqwest::{Client as HttpClient, StatusCode};
use semver::{BuildMetadata, Prerelease, Version};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tracing::{error, trace, warn};

use super::Error;
use super::matching_image_from_environmentd_image_ref;
use crate::k8s::{apply_resource, delete_resource, get_resource};
use crate::tls::{create_certificate, issuer_ref_defined};
use mz_cloud_provider::CloudProvider;
use mz_cloud_resources::crd::materialize::v1alpha1::Materialize;
use mz_cloud_resources::crd::{
    ManagedResource,
    generated::cert_manager::certificates::{Certificate, CertificatePrivateKeyAlgorithm},
};
use mz_ore::instrument;

static V140_DEV0: LazyLock<Version> = LazyLock::new(|| Version {
    major: 0,
    minor: 140,
    patch: 0,
    pre: Prerelease::new("dev.0").expect("dev.0 is valid prerelease"),
    build: BuildMetadata::new("").expect("empty string is valid buildmetadata"),
});
const V143: Version = Version::new(0, 143, 0);
const V144: Version = Version::new(0, 144, 0);
static V147_DEV0: LazyLock<Version> = LazyLock::new(|| Version {
    major: 0,
    minor: 147,
    patch: 0,
    pre: Prerelease::new("dev.0").expect("dev.0 is valid prerelease"),
    build: BuildMetadata::new("").expect("empty string is valid buildmetadata"),
});
const V153: Version = Version::new(0, 153, 0);
static V154_DEV0: LazyLock<Version> = LazyLock::new(|| Version {
    major: 0,
    minor: 154,
    patch: 0,
    pre: Prerelease::new("").expect("dev.0 is valid prerelease"),
    build: BuildMetadata::new("").expect("empty string is valid buildmetadata"),
});
pub const V161: Version = Version::new(0, 161, 0);

static V26_1_0: LazyLock<Version> = LazyLock::new(|| Version {
    major: 26,
    minor: 1,
    patch: 0,
    pre: Prerelease::new("dev.0").expect("dev.0 is valid prerelease"),
    build: BuildMetadata::new("").expect("empty string is valid buildmetadata"),
});

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

#[derive(Debug, Serialize, Deserialize, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct GetLeaderStatusResponse {
    status: DeploymentStatus,
}

#[derive(Deserialize, Serialize)]
pub struct LoginCredentials {
    username: String,
    // TODO Password type?
    password: String,
}

#[derive(Debug, Serialize)]
pub struct ConnectionInfo {
    pub environmentd_url: String,
    pub mz_system_secret_name: Option<String>,
    pub listeners_configmap: ConfigMap,
}

#[derive(Debug, Serialize)]
pub struct Resources {
    pub generation: u64,
    pub environmentd_network_policies: Vec<NetworkPolicy>,
    pub service_account: Box<Option<ServiceAccount>>,
    pub role: Box<Role>,
    pub role_binding: Box<RoleBinding>,
    pub public_service: Box<Service>,
    pub generation_service: Box<Service>,
    pub persist_pubsub_service: Box<Service>,
    pub environmentd_certificate: Box<Option<Certificate>>,
    pub environmentd_statefulset: Box<StatefulSet>,
    pub connection_info: Box<ConnectionInfo>,
}

impl Resources {
    pub fn new(config: &super::Config, mz: &Materialize, generation: u64) -> Self {
        let environmentd_network_policies = create_environmentd_network_policies(config, mz);

        let service_account = Box::new(create_service_account_object(config, mz));
        let role = Box::new(create_role_object(mz));
        let role_binding = Box::new(create_role_binding_object(mz));
        let public_service = Box::new(create_public_service_object(config, mz, generation));
        let generation_service = Box::new(create_generation_service_object(config, mz, generation));
        let persist_pubsub_service =
            Box::new(create_persist_pubsub_service(config, mz, generation));
        let environmentd_certificate = Box::new(create_environmentd_certificate(config, mz));
        let environmentd_statefulset = Box::new(create_environmentd_statefulset_object(
            config, mz, generation,
        ));
        let connection_info = Box::new(create_connection_info(config, mz, generation));

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
            connection_info,
        }
    }

    #[instrument]
    pub async fn apply(
        &self,
        client: &Client,
        force_promote: bool,
        namespace: &str,
    ) -> Result<Option<Action>, Error> {
        let environmentd_network_policy_api: Api<NetworkPolicy> =
            Api::namespaced(client.clone(), namespace);
        let service_api: Api<Service> = Api::namespaced(client.clone(), namespace);
        let service_account_api: Api<ServiceAccount> = Api::namespaced(client.clone(), namespace);
        let role_api: Api<Role> = Api::namespaced(client.clone(), namespace);
        let role_binding_api: Api<RoleBinding> = Api::namespaced(client.clone(), namespace);
        let statefulset_api: Api<StatefulSet> = Api::namespaced(client.clone(), namespace);
        let certificate_api: Api<Certificate> = Api::namespaced(client.clone(), namespace);
        let configmap_api: Api<ConfigMap> = Api::namespaced(client.clone(), namespace);

        for policy in &self.environmentd_network_policies {
            trace!("applying network policy {}", policy.name_unchecked());
            apply_resource(&environmentd_network_policy_api, policy).await?;
        }

        if let Some(service_account) = &*self.service_account {
            trace!("applying environmentd service account");
            apply_resource(&service_account_api, service_account).await?;
        }

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

        trace!("applying listeners configmap");
        apply_resource(&configmap_api, &self.connection_info.listeners_configmap).await?;

        trace!("creating new environmentd statefulset");
        apply_resource(&statefulset_api, &*self.environmentd_statefulset).await?;

        let retry_action = Action::requeue(Duration::from_secs(rand::random_range(5..10)));

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

        let Some(http_client) = self.get_http_client(client.clone(), namespace).await else {
            return Ok(Some(retry_action));
        };
        let status_url = reqwest::Url::parse(&format!(
            "{}/api/leader/status",
            self.connection_info.environmentd_url,
        ))
        .unwrap();

        match http_client.get(status_url.clone()).send().await {
            Ok(response) => {
                let response: GetLeaderStatusResponse = match response.error_for_status() {
                    Ok(response) => response.json().await?,
                    Err(e) => {
                        trace!("failed to get status of environmentd, retrying... ({e})");
                        return Ok(Some(retry_action));
                    }
                };
                if force_promote {
                    trace!("skipping cluster catchup");
                    let skip_catchup_url = reqwest::Url::parse(&format!(
                        "{}/api/leader/skip-catchup",
                        self.connection_info.environmentd_url,
                    ))
                    .unwrap();
                    let response = http_client.post(skip_catchup_url).send().await?;
                    if response.status() == StatusCode::BAD_REQUEST {
                        let err: SkipCatchupError = response.json().await?;
                        return Err(
                            anyhow::anyhow!("failed to skip catchup: {}", err.message).into()
                        );
                    }
                } else {
                    match response.status {
                        DeploymentStatus::Initializing => {
                            trace!("environmentd is still initializing, retrying...");
                            return Ok(Some(retry_action));
                        }
                        DeploymentStatus::ReadyToPromote
                        | DeploymentStatus::Promoting
                        | DeploymentStatus::IsLeader => trace!("environmentd is ready"),
                    }
                }
            }
            Err(e) => {
                trace!("failed to connect to environmentd, retrying... ({e})");
                return Ok(Some(retry_action));
            }
        }

        Ok(None)
    }

    #[instrument]
    async fn get_http_client(&self, client: Client, namespace: &str) -> Option<HttpClient> {
        let secret_api: Api<Secret> = Api::namespaced(client.clone(), namespace);
        Some(match &self.connection_info.mz_system_secret_name {
            Some(mz_system_secret_name) => {
                let http_client = reqwest::Client::builder()
                    .timeout(std::time::Duration::from_secs(10))
                    .cookie_store(true)
                    // TODO add_root_certificate instead
                    .danger_accept_invalid_certs(true)
                    .build()
                    .unwrap();

                let secret = secret_api
                    .get(mz_system_secret_name)
                    .await
                    .map_err(|e| {
                        error!("Failed to get backend secret: {:?}", e);
                        e
                    })
                    .ok()?;
                if let Some(data) = secret.data {
                    if let Some(password) = data.get("external_login_password_mz_system").cloned() {
                        let password = String::from_utf8_lossy(&password.0).to_string();
                        let login_url = reqwest::Url::parse(&format!(
                            "{}/api/login",
                            self.connection_info.environmentd_url,
                        ))
                        .unwrap();
                        match http_client
                            .post(login_url)
                            .body(
                                serde_json::to_string(&LoginCredentials {
                                    username: "mz_system".to_owned(),
                                    password,
                                })
                                .expect(
                                    "Serializing a simple struct with utf8 strings doesn't fail.",
                                ),
                            )
                            .header("Content-Type", "application/json")
                            .send()
                            .await
                        {
                            Ok(response) => {
                                if let Err(e) = response.error_for_status() {
                                    trace!("failed to login to environmentd, retrying... ({e})");
                                    return None;
                                }
                            }
                            Err(e) => {
                                trace!("failed to connect to environmentd, retrying... ({e})");
                                return None;
                            }
                        };
                    }
                };
                http_client
            }
            None => reqwest::Client::builder()
                .timeout(std::time::Duration::from_secs(10))
                .build()
                .unwrap(),
        })
    }

    #[instrument]
    pub async fn promote_services(
        &self,
        client: &Client,
        namespace: &str,
    ) -> Result<Option<Action>, Error> {
        let service_api: Api<Service> = Api::namespaced(client.clone(), namespace);
        let retry_action = Action::requeue(Duration::from_secs(rand::random_range(5..10)));

        let promote_url = reqwest::Url::parse(&format!(
            "{}/api/leader/promote",
            self.connection_info.environmentd_url,
        ))
        .unwrap();

        let Some(http_client) = self.get_http_client(client.clone(), namespace).await else {
            return Ok(Some(retry_action));
        };

        trace!("promoting new environmentd to leader");
        let response = http_client.post(promote_url).send().await?;
        let response: BecomeLeaderResponse = response.error_for_status()?.json().await?;
        if let BecomeLeaderResult::Failure { message } = response.result {
            return Err(Error::Anyhow(anyhow::anyhow!(
                "failed to promote new environmentd: {message}"
            )));
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

        let status_url = reqwest::Url::parse(&format!(
            "{}/api/leader/status",
            self.connection_info.environmentd_url,
        ))
        .unwrap();
        match http_client.get(status_url.clone()).send().await {
            Ok(response) => {
                let response: GetLeaderStatusResponse = response.json().await?;
                if response.status != DeploymentStatus::IsLeader {
                    trace!(
                        "environmentd is still promoting (status: {:?}), retrying...",
                        response.status
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

        trace!("applying environmentd public service");
        apply_resource(&service_api, &*self.public_service).await?;

        Ok(None)
    }

    #[instrument]
    pub async fn teardown_generation(
        &self,
        client: &Client,
        mz: &Materialize,
        generation: u64,
    ) -> Result<(), anyhow::Error> {
        let configmap_api: Api<ConfigMap> = Api::namespaced(client.clone(), &mz.namespace());
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

        trace!("deleting listeners configmap for generation {generation}");
        delete_resource(&configmap_api, &mz.listeners_configmap_name(generation)).await?;

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
    config: &super::Config,
    mz: &Materialize,
) -> Vec<NetworkPolicy> {
    let mut network_policies = Vec::new();
    if config.network_policies_internal_enabled {
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
                    pod_selector: Some(all_pods_label_selector.clone()),
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
                                        => config.orchestratord_namespace.clone(),
                                }),
                                ..Default::default()
                            }),
                            pod_selector: Some(orchestratord_label_selector),
                            ..Default::default()
                        }]),
                        ports: Some(vec![
                            NetworkPolicyPort {
                                port: Some(IntOrString::Int(config.environmentd_http_port.into())),
                                protocol: Some("TCP".to_string()),
                                ..Default::default()
                            },
                            NetworkPolicyPort {
                                port: Some(IntOrString::Int(
                                    config.environmentd_internal_http_port.into(),
                                )),
                                protocol: Some("TCP".to_string()),
                                ..Default::default()
                            },
                        ]),
                        ..Default::default()
                    }]),
                    pod_selector: Some(environmentd_label_selector),
                    policy_types: Some(vec!["Ingress".to_owned()]),
                    ..Default::default()
                }),
            },
        ]);
    }
    if config.network_policies_ingress_enabled {
        let mut ingress_label_selector = mz.default_labels();
        ingress_label_selector.insert("materialize.cloud/app".to_owned(), mz.balancerd_app_name());
        network_policies.extend([NetworkPolicy {
            metadata: mz.managed_resource_meta(mz.name_prefixed("sql-and-http-ingress")),
            spec: Some(NetworkPolicySpec {
                ingress: Some(vec![NetworkPolicyIngressRule {
                    from: Some(
                        config
                            .network_policies_ingress_cidrs
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
                            port: Some(IntOrString::Int(config.environmentd_http_port.into())),
                            protocol: Some("TCP".to_string()),
                            ..Default::default()
                        },
                        NetworkPolicyPort {
                            port: Some(IntOrString::Int(config.environmentd_sql_port.into())),
                            protocol: Some("TCP".to_string()),
                            ..Default::default()
                        },
                    ]),
                    ..Default::default()
                }]),
                pod_selector: Some(LabelSelector {
                    match_expressions: None,
                    match_labels: Some(ingress_label_selector),
                }),
                policy_types: Some(vec!["Ingress".to_owned()]),
                ..Default::default()
            }),
        }]);
    }
    if config.network_policies_egress_enabled {
        network_policies.extend([NetworkPolicy {
            metadata: mz.managed_resource_meta(mz.name_prefixed("sources-and-sinks-egress")),
            spec: Some(NetworkPolicySpec {
                egress: Some(vec![NetworkPolicyEgressRule {
                    to: Some(
                        config
                            .network_policies_egress_cidrs
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
                pod_selector: Some(LabelSelector {
                    match_expressions: None,
                    match_labels: Some(mz.default_labels()),
                }),
                policy_types: Some(vec!["Egress".to_owned()]),
                ..Default::default()
            }),
        }]);
    }
    network_policies
}

fn create_service_account_object(
    config: &super::Config,
    mz: &Materialize,
) -> Option<ServiceAccount> {
    if mz.create_service_account() {
        let mut annotations: BTreeMap<String, String> = mz
            .spec
            .service_account_annotations
            .clone()
            .unwrap_or_default();
        if let (CloudProvider::Aws, Some(role_arn)) = (
            config.cloud_provider,
            mz.spec
                .environmentd_iam_role_arn
                .as_deref()
                .or(config.environmentd_iam_role_arn.as_deref()),
        ) {
            warn!(
                "Use of Materialize.spec.environmentd_iam_role_arn is deprecated. Please set \"eks.amazonaws.com/role-arn\" in Materialize.spec.service_account_annotations instead."
            );
            annotations.insert(
                "eks.amazonaws.com/role-arn".to_string(),
                role_arn.to_string(),
            );
        };

        let mut labels = mz.default_labels();
        labels.extend(mz.spec.service_account_labels.clone().unwrap_or_default());

        Some(ServiceAccount {
            metadata: ObjectMeta {
                annotations: Some(annotations),
                labels: Some(labels),
                ..mz.managed_resource_meta(mz.service_account_name())
            },
            ..Default::default()
        })
    } else {
        None
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
    config: &super::Config,
    mz: &Materialize,
    generation: u64,
) -> Service {
    create_base_service_object(config, mz, generation, &mz.environmentd_service_name())
}

fn create_generation_service_object(
    config: &super::Config,
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
    config: &super::Config,
    mz: &Materialize,
    generation: u64,
    service_name: &str,
) -> Service {
    let ports = vec![
        ServicePort {
            port: config.environmentd_sql_port.into(),
            protocol: Some("TCP".to_string()),
            name: Some("sql".to_string()),
            ..Default::default()
        },
        ServicePort {
            port: config.environmentd_http_port.into(),
            protocol: Some("TCP".to_string()),
            name: Some("https".to_string()),
            ..Default::default()
        },
        ServicePort {
            port: config.environmentd_internal_sql_port.into(),
            protocol: Some("TCP".to_string()),
            name: Some("internal-sql".to_string()),
            ..Default::default()
        },
        ServicePort {
            port: config.environmentd_internal_http_port.into(),
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
    config: &super::Config,
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
                port: config.environmentd_internal_persist_pubsub_port.into(),
                ..Default::default()
            }]),
            ..Default::default()
        }),
        status: None,
    }
}

fn create_environmentd_certificate(
    config: &super::Config,
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
        CertificatePrivateKeyAlgorithm::Ed25519,
        None,
    )
}

fn create_environmentd_statefulset_object(
    config: &super::Config,
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
                    name: mz.backend_secret_name(),
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
                    name: mz.backend_secret_name(),
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
                .map(|size| format!("--bootstrap-builtin-catalog-server-cluster-replica-size={size}")),
            config
                .bootstrap_builtin_analytics_cluster_replica_size
                .as_ref()
                .map(|size| format!("--bootstrap-builtin-analytics-cluster-replica-size={size}")),
            config
                .bootstrap_builtin_system_cluster_replication_factor
                .as_ref()
                .map(|replication_factor| {
                    format!("--bootstrap-builtin-system-cluster-replication-factor={replication_factor}")
                }),
            config
                .bootstrap_builtin_probe_cluster_replication_factor
                .as_ref()
                .map(|replication_factor| format!("--bootstrap-builtin-probe-cluster-replication-factor={replication_factor}")),
            config
                .bootstrap_builtin_support_cluster_replication_factor
                .as_ref()
                .map(|replication_factor| format!("--bootstrap-builtin-support-cluster-replication-factor={replication_factor}")),
            config
                .bootstrap_builtin_analytics_cluster_replication_factor
                .as_ref()
                .map(|replication_factor| format!("--bootstrap-builtin-analytics-cluster-replication-factor={replication_factor}")),
        ]
        .into_iter()
        .flatten(),
    );

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

    if config.disable_statement_logging {
        args.push("--system-parameter-default=statement_logging_max_sample_rate=0".into());
    }

    if !mz.spec.enable_rbac {
        args.push("--system-parameter-default=enable_rbac_checks=false".into());
    }

    // Add persist arguments.

    // Configure the Persist Isolated Runtime to use one less thread than the total available.
    args.push("--persist-isolated-runtime-threads=-1".to_string());

    // Add AWS arguments.
    if config.cloud_provider == CloudProvider::Aws {
        if let Some(azs) = config.environmentd_availability_zones.as_ref() {
            for az in azs {
                args.push(format!("--availability-zone={az}"));
            }
        }

        if let Some(environmentd_connection_role_arn) = mz
            .spec
            .environmentd_connection_role_arn
            .as_deref()
            .or(config.environmentd_connection_role_arn.as_deref())
        {
            args.push(format!(
                "--aws-connection-role-arn={}",
                environmentd_connection_role_arn
            ));
        }
        if let Some(account_id) = &config.aws_account_id {
            args.push(format!("--aws-account-id={account_id}"));
        }

        args.extend([format!(
            "--aws-secrets-controller-tags=Environment={}",
            mz.name_unchecked()
        )]);
        args.extend_from_slice(&config.aws_secrets_controller_tags);
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
    if mz.meets_minimum_version(&V144) {
        if let Some(affinity) = &config.clusterd_affinity {
            let affinity = serde_json::to_string(affinity).unwrap();
            args.push(format!(
                "--orchestrator-kubernetes-service-affinity={affinity}"
            ))
        }
        if let Some(tolerations) = &config.clusterd_tolerations {
            let tolerations = serde_json::to_string(tolerations).unwrap();
            args.push(format!(
                "--orchestrator-kubernetes-service-tolerations={tolerations}"
            ))
        }
    }
    if let Some(scheduler_name) = &config.scheduler_name {
        args.push(format!(
            "--orchestrator-kubernetes-scheduler-name={}",
            scheduler_name
        ));
    }
    if mz.meets_minimum_version(&V154_DEV0) {
        args.extend(
            mz.spec
                .pod_annotations
                .as_ref()
                .map(|annotations| annotations.iter())
                .unwrap_or_default()
                .map(|(key, val)| {
                    format!("--orchestrator-kubernetes-service-annotation={key}={val}")
                }),
        );
    }
    args.extend(
        mz.default_labels()
            .iter()
            .chain(
                mz.spec
                    .pod_labels
                    .as_ref()
                    .map(|labels| labels.iter())
                    .unwrap_or_default(),
            )
            .map(|(key, val)| format!("--orchestrator-kubernetes-service-label={key}={val}")),
    );
    if let Some(status) = &mz.status {
        args.push(format!(
            "--orchestrator-kubernetes-name-prefix=mz{}-",
            status.resource_id
        ));
    }

    // Add logging and tracing arguments.
    args.extend(["--log-format=json".into()]);
    if let Some(endpoint) = &config.tracing.opentelemetry_endpoint {
        args.push(format!("--opentelemetry-endpoint={}", endpoint));
    }
    // --opentelemetry-resource also configures sentry tags
    args.extend([
        format!(
            "--opentelemetry-resource=organization_id={}",
            mz.spec.environment_id
        ),
        format!(
            "--opentelemetry-resource=environment_name={}",
            mz.name_unchecked()
        ),
    ]);

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
        args.push(format!(
            "--orchestrator-kubernetes-ephemeral-volume-class={}",
            ephemeral_volume_class
        ));
    }
    // The `materialize` user used by clusterd always has gid 999.
    args.push("--orchestrator-kubernetes-service-fs-group=999".to_string());

    // Add system_param configmap
    // This feature was enabled in 0.163 but did not have testing until after 0.164.
    // 0.165 should work with anything greater than 0.164 including v26 and v25.
    if mz.meets_minimum_version(&V26_1_0) {
        if let Some(ref name) = mz.spec.system_parameter_configmap_name {
            volumes.push(Volume {
                name: "system-params".to_string(),
                config_map: Some(ConfigMapVolumeSource {
                    default_mode: Some(0o400),
                    name: name.to_owned(),
                    items: None,
                    optional: Some(true),
                }),
                ..Default::default()
            });
            volume_mounts.push(VolumeMount {
                name: "system-params".to_string(),
                // The user must write to the `system-params.json` entry in the config map
                mount_path: "/system-params".to_owned(),
                read_only: Some(true),
                ..Default::default()
            });
            args.push("--config-sync-file-path=/system-params/system-params.json".to_string());
            args.push("--config-sync-loop-interval=1s".to_string());
        }
    }

    // Add Sentry arguments.
    if let Some(sentry_dsn) = &config.tracing.sentry_dsn {
        args.push(format!("--sentry-dsn={}", sentry_dsn));
        if let Some(sentry_environment) = &config.tracing.sentry_environment {
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

    // the --disable-license-key-checks environmentd flag only existed
    // between these versions
    if config.disable_license_key_checks {
        if mz.meets_minimum_version(&V143) && !mz.meets_minimum_version(&V153) {
            args.push("--disable-license-key-checks".into());
        }
    }

    // as of version 0.153, the ability to disable license key checks was
    // removed, so we should always set up license keys in that case
    if (mz.meets_minimum_version(&V140_DEV0) && !config.disable_license_key_checks)
        || mz.meets_minimum_version(&V153)
    {
        volume_mounts.push(VolumeMount {
            name: "license-key".to_string(),
            mount_path: "/license_key".to_string(),
            ..Default::default()
        });
        volumes.push(Volume {
            name: "license-key".to_string(),
            secret: Some(SecretVolumeSource {
                default_mode: Some(256),
                optional: Some(false),
                secret_name: Some(mz.backend_secret_name()),
                items: Some(vec![KeyToPath {
                    key: "license_key".to_string(),
                    path: "license_key".to_string(),
                    ..Default::default()
                }]),
                ..Default::default()
            }),
            ..Default::default()
        });
        env.push(EnvVar {
            name: "MZ_LICENSE_KEY".to_string(),
            value: Some("/license_key/license_key".to_string()),
            ..Default::default()
        });
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
            port: IntOrString::Int(config.environmentd_sql_port.into()),
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
            container_port: config.environmentd_sql_port.into(),
            name: Some("sql".to_owned()),
            ..Default::default()
        },
        ContainerPort {
            container_port: config.environmentd_internal_sql_port.into(),
            name: Some("internal-sql".to_owned()),
            ..Default::default()
        },
        ContainerPort {
            container_port: config.environmentd_http_port.into(),
            name: Some("http".to_owned()),
            ..Default::default()
        },
        ContainerPort {
            container_port: config.environmentd_internal_http_port.into(),
            name: Some("internal-http".to_owned()),
            ..Default::default()
        },
        ContainerPort {
            container_port: config.environmentd_internal_persist_pubsub_port.into(),
            name: Some("persist-pubsub".to_owned()),
            ..Default::default()
        },
    ];

    // Add networking arguments.
    if mz.meets_minimum_version(&V147_DEV0) {
        volume_mounts.push(VolumeMount {
            name: "listeners-configmap".to_string(),
            mount_path: "/listeners".to_string(),
            ..Default::default()
        });
        volumes.push(Volume {
            name: "listeners-configmap".to_string(),
            config_map: Some(ConfigMapVolumeSource {
                name: mz.listeners_configmap_name(generation),
                default_mode: Some(256),
                optional: Some(false),
                items: Some(vec![KeyToPath {
                    key: "listeners.json".to_string(),
                    path: "listeners.json".to_string(),
                    ..Default::default()
                }]),
            }),
            ..Default::default()
        });
        args.push("--listeners-config-path=/listeners/listeners.json".to_owned());
        if matches!(
            mz.spec.authenticator_kind,
            AuthenticatorKind::Password | AuthenticatorKind::Sasl
        ) {
            args.push("--system-parameter-default=enable_password_auth=true".into());
            env.push(EnvVar {
                name: "MZ_EXTERNAL_LOGIN_PASSWORD_MZ_SYSTEM".to_string(),
                value_from: Some(EnvVarSource {
                    secret_key_ref: Some(SecretKeySelector {
                        name: mz.backend_secret_name(),
                        key: "external_login_password_mz_system".to_string(),
                        optional: Some(false),
                    }),
                    ..Default::default()
                }),
                ..Default::default()
            })
        }
    } else {
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
    }

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
        resources: mz
            .spec
            .environmentd_resource_requirements
            .clone()
            .or_else(|| config.environmentd_default_resources.clone()),
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
    pod_template_labels.extend(
        mz.spec
            .pod_labels
            .as_ref()
            .map(|labels| labels.iter())
            .unwrap_or_default()
            .map(|(key, value)| (key.clone(), value.clone())),
    );

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
    pod_template_annotations.extend(
        mz.spec
            .pod_annotations
            .as_ref()
            .map(|annotations| annotations.iter())
            .unwrap_or_default()
            .map(|(key, value)| (key.clone(), value.clone())),
    );

    let mut tolerations = vec![
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
    ];
    if let Some(user_tolerations) = &config.environmentd_tolerations {
        tolerations.extend(user_tolerations.iter().cloned());
    }
    let tolerations = Some(tolerations);

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
            affinity: config.environmentd_affinity.clone(),
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
        service_name: Some(mz.environmentd_service_name()),
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

fn create_connection_info(
    config: &super::Config,
    mz: &Materialize,
    generation: u64,
) -> ConnectionInfo {
    let external_enable_tls = issuer_ref_defined(
        &config.default_certificate_specs.internal,
        &mz.spec.internal_certificate_spec,
    );
    let authenticator_kind = mz.spec.authenticator_kind;

    let mut listeners_config = ListenersConfig {
        sql: btreemap! {
            "external".to_owned() => SqlListenerConfig{
                addr: SocketAddr::new(IpAddr::V4(Ipv4Addr::new(0,0,0,0)), config.environmentd_sql_port),
                authenticator_kind,
                allowed_roles: AllowedRoles::Normal,
                enable_tls: external_enable_tls,
            },
            "internal".to_owned() => SqlListenerConfig{
                addr: SocketAddr::new(IpAddr::V4(Ipv4Addr::new(0,0,0,0)), config.environmentd_internal_sql_port),
                authenticator_kind: AuthenticatorKind::None,
                // Should this just be Internal?
                allowed_roles: AllowedRoles::NormalAndInternal,
                enable_tls: false,
            },
        },
        http: btreemap! {
            "external".to_owned() => HttpListenerConfig{
                base: BaseListenerConfig {
                    addr: SocketAddr::new(IpAddr::V4(Ipv4Addr::new(0,0,0,0)), config.environmentd_http_port),
                    // SASL authentication is only supported for SQL (PostgreSQL wire protocol).
                    // HTTP listeners must use Password authentication when SASL is enabled.
                    // This is validated at environmentd startup via ListenerConfig::validate().
                    authenticator_kind: if authenticator_kind == AuthenticatorKind::Sasl {
                        AuthenticatorKind::Password
                    } else {
                        authenticator_kind
                    },
                    allowed_roles: AllowedRoles::Normal,
                    enable_tls: external_enable_tls,
                },
                routes: HttpRoutesEnabled{
                    base: true,
                    webhook: true,
                    internal: false,
                    metrics: false,
                    profiling: false,
                }
            },
            "internal".to_owned() => HttpListenerConfig{
                base: BaseListenerConfig {
                    addr: SocketAddr::new(IpAddr::V4(Ipv4Addr::new(0,0,0,0)), config.environmentd_internal_http_port),
                    authenticator_kind: AuthenticatorKind::None,
                    // Should this just be Internal?
                    allowed_roles: AllowedRoles::NormalAndInternal,
                    enable_tls: false,
                },
                routes: HttpRoutesEnabled{
                    base: true,
                    webhook: true,
                    internal: true,
                    metrics: true,
                    profiling: true,
                }
            },
        },
    };

    if matches!(
        authenticator_kind,
        AuthenticatorKind::Password | AuthenticatorKind::Sasl
    ) {
        listeners_config.sql.remove("internal");
        listeners_config.http.remove("internal");

        listeners_config.sql.get_mut("external").map(|listener| {
            listener.allowed_roles = AllowedRoles::NormalAndInternal;
            listener
        });
        listeners_config.http.get_mut("external").map(|listener| {
            listener.base.allowed_roles = AllowedRoles::NormalAndInternal;
            listener.routes.internal = true;
            listener.routes.profiling = true;
            listener
        });

        listeners_config.http.insert(
            "metrics".to_owned(),
            HttpListenerConfig {
                base: BaseListenerConfig {
                    addr: SocketAddr::new(
                        IpAddr::V4(Ipv4Addr::new(0, 0, 0, 0)),
                        config.environmentd_internal_http_port,
                    ),
                    authenticator_kind: AuthenticatorKind::None,
                    allowed_roles: AllowedRoles::NormalAndInternal,
                    enable_tls: false,
                },
                routes: HttpRoutesEnabled {
                    base: false,
                    webhook: false,
                    internal: false,
                    metrics: true,
                    profiling: false,
                },
            },
        );
    }

    let listeners_json = serde_json::to_string(&listeners_config).expect("known valid");
    let listeners_configmap = ConfigMap {
        binary_data: None,
        data: Some(btreemap! {
            "listeners.json".to_owned() => listeners_json,
        }),
        immutable: None,
        metadata: ObjectMeta {
            annotations: Some(btreemap! {
                "materialize.cloud/generation".to_owned() => generation.to_string(),
            }),
            ..mz.managed_resource_meta(mz.listeners_configmap_name(generation))
        },
    };

    let (scheme, leader_api_port, mz_system_secret_name) = match authenticator_kind {
        AuthenticatorKind::Password | AuthenticatorKind::Sasl => {
            let scheme = if external_enable_tls { "https" } else { "http" };
            (
                scheme,
                config.environmentd_http_port,
                Some(mz.spec.backend_secret_name.clone()),
            )
        }
        _ => ("http", config.environmentd_internal_http_port, None),
    };
    let environmentd_url = format!(
        "{}://{}.{}.svc.cluster.local:{}",
        scheme,
        mz.environmentd_generation_service_name(generation),
        mz.namespace(),
        leader_api_port,
    );
    ConnectionInfo {
        environmentd_url,
        listeners_configmap,
        mz_system_secret_name,
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

#[derive(Debug, Deserialize, PartialEq, Eq)]
struct SkipCatchupError {
    message: String,
}

// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::BTreeMap;

use k8s_openapi::{
    api::{
        core::v1::ServiceAccount,
        networking::v1::{
            IPBlock, NetworkPolicy, NetworkPolicyEgressRule, NetworkPolicyIngressRule,
            NetworkPolicyPeer, NetworkPolicyPort, NetworkPolicySpec,
        },
        rbac::v1::{PolicyRule, Role, RoleBinding, RoleRef, Subject},
    },
    apimachinery::pkg::{apis::meta::v1::LabelSelector, util::intstr::IntOrString},
};
use kube::{
    Api, Client, Resource, ResourceExt,
    api::{ObjectMeta, PostParams},
    runtime::controller::Action,
};
use maplit::btreemap;
use mz_cloud_provider::CloudProvider;
use tracing::{trace, warn};

use crate::{
    Error,
    k8s::apply_resource,
    tls::{DefaultCertificateSpecs, create_certificate},
};
use mz_cloud_resources::crd::{
    ManagedResource,
    environment::v1alpha1::Environment,
    generated::cert_manager::certificates::{Certificate, CertificatePrivateKeyAlgorithm},
};
use mz_ore::{cli::KeyValueArg, instrument};

pub struct Config {
    pub cloud_provider: CloudProvider,

    pub environmentd_iam_role_arn: Option<String>,

    pub orchestratord_pod_selector_labels: Vec<KeyValueArg<String, String>>,
    pub network_policies_internal_enabled: bool,
    pub network_policies_ingress_enabled: bool,
    pub network_policies_ingress_cidrs: Vec<String>,
    pub network_policies_egress_enabled: bool,
    pub network_policies_egress_cidrs: Vec<String>,

    pub environmentd_sql_port: u16,
    pub environmentd_http_port: u16,
    pub environmentd_internal_http_port: u16,

    pub default_certificate_specs: DefaultCertificateSpecs,

    pub orchestratord_namespace: String,
}

pub struct Context {
    config: Config,
}

impl Context {
    pub fn new(config: Config) -> Self {
        Self { config }
    }

    fn create_network_policies(&self, environment: &Environment) -> Vec<NetworkPolicy> {
        let mut network_policies = Vec::new();
        if self.config.network_policies_internal_enabled {
            let environmentd_label_selector = LabelSelector {
                match_labels: Some(
                    environment
                        .default_labels()
                        .into_iter()
                        .chain([("materialize.cloud/app".to_owned(), environment.app_name())])
                        .collect(),
                ),
                ..Default::default()
            };
            let orchestratord_label_selector = LabelSelector {
                match_labels: Some(
                    self.config
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
                // TODO: can't use default_labels() here because it needs to be
                // consistent between balancer and materialize resources, and
                // materialize resources have additional labels - we should
                // figure out something better here (probably balancers should
                // install their own network policies)
                match_labels: Some(
                    [(
                        "materialize.cloud/mz-resource-id".to_owned(),
                        environment.resource_id().to_owned(),
                    )]
                    .into(),
                ),
                ..Default::default()
            };
            network_policies.extend([
                // Allow all clusterd/environmentd traffic (between pods in the
                // same environment)
                NetworkPolicy {
                    metadata: environment.managed_resource_meta(
                        environment.name_prefixed("allow-all-within-environment"),
                    ),
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
                    metadata: environment
                        .managed_resource_meta(environment.name_prefixed("allow-orchestratord")),
                    spec: Some(NetworkPolicySpec {
                        ingress: Some(vec![NetworkPolicyIngressRule {
                            from: Some(vec![NetworkPolicyPeer {
                                namespace_selector: Some(LabelSelector {
                                    match_labels: Some(btreemap! {
                                        "kubernetes.io/metadata.name".into()
                                            => self.config.orchestratord_namespace.clone(),
                                    }),
                                    ..Default::default()
                                }),
                                pod_selector: Some(orchestratord_label_selector),
                                ..Default::default()
                            }]),
                            ports: Some(vec![
                                NetworkPolicyPort {
                                    port: Some(IntOrString::Int(
                                        self.config.environmentd_http_port.into(),
                                    )),
                                    protocol: Some("TCP".to_string()),
                                    ..Default::default()
                                },
                                NetworkPolicyPort {
                                    port: Some(IntOrString::Int(
                                        self.config.environmentd_internal_http_port.into(),
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
        if self.config.network_policies_ingress_enabled {
            let mut ingress_label_selector = environment.default_labels();
            ingress_label_selector.insert(
                "materialize.cloud/app".to_owned(),
                environment.balancerd_app_name(),
            );
            network_policies.extend([NetworkPolicy {
                metadata: environment
                    .managed_resource_meta(environment.name_prefixed("sql-and-http-ingress")),
                spec: Some(NetworkPolicySpec {
                    ingress: Some(vec![NetworkPolicyIngressRule {
                        from: Some(
                            self.config
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
                                port: Some(IntOrString::Int(
                                    self.config.environmentd_http_port.into(),
                                )),
                                protocol: Some("TCP".to_string()),
                                ..Default::default()
                            },
                            NetworkPolicyPort {
                                port: Some(IntOrString::Int(
                                    self.config.environmentd_sql_port.into(),
                                )),
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
        if self.config.network_policies_egress_enabled {
            network_policies.extend([NetworkPolicy {
                metadata: environment
                    .managed_resource_meta(environment.name_prefixed("sources-and-sinks-egress")),
                spec: Some(NetworkPolicySpec {
                    egress: Some(vec![NetworkPolicyEgressRule {
                        to: Some(
                            self.config
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
                        match_labels: Some(environment.default_labels()),
                    }),
                    policy_types: Some(vec!["Egress".to_owned()]),
                    ..Default::default()
                }),
            }]);
        }
        network_policies
    }

    fn create_service_account(&self, environment: &Environment) -> Option<ServiceAccount> {
        if environment.create_service_account() {
            let mut annotations: BTreeMap<String, String> = environment
                .spec
                .service_account_annotations
                .clone()
                .unwrap_or_default();
            if let (CloudProvider::Aws, Some(role_arn)) = (
                self.config.cloud_provider,
                environment
                    .spec
                    .environmentd_iam_role_arn
                    .as_deref()
                    .or(self.config.environmentd_iam_role_arn.as_deref()),
            ) {
                warn!(
                    "Use of Materialize.spec.environmentd_iam_role_arn is deprecated. Please set \"eks.amazonaws.com/role-arn\" in Materialize.spec.service_account_annotations instead."
                );
                annotations.insert(
                    "eks.amazonaws.com/role-arn".to_string(),
                    role_arn.to_string(),
                );
            };

            let mut labels = environment.default_labels();
            labels.extend(
                environment
                    .spec
                    .service_account_labels
                    .clone()
                    .unwrap_or_default(),
            );

            Some(ServiceAccount {
                metadata: ObjectMeta {
                    annotations: Some(annotations),
                    labels: Some(labels),
                    ..environment.managed_resource_meta(environment.service_account_name())
                },
                ..Default::default()
            })
        } else {
            None
        }
    }

    fn create_role(&self, environment: &Environment) -> Role {
        Role {
            metadata: environment.managed_resource_meta(environment.role_name()),
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

    fn create_role_binding(&self, environment: &Environment) -> RoleBinding {
        RoleBinding {
            metadata: environment.managed_resource_meta(environment.role_binding_name()),
            role_ref: RoleRef {
                api_group: "".to_string(),
                kind: "Role".to_string(),
                name: environment.role_name(),
            },
            subjects: Some(vec![Subject {
                api_group: Some("".to_string()),
                kind: "ServiceAccount".to_string(),
                name: environment.service_account_name(),
                namespace: Some(environment.namespace()),
            }]),
        }
    }

    fn create_certificate(&self, environment: &Environment) -> Option<Certificate> {
        create_certificate(
            self.config.default_certificate_specs.internal.clone(),
            environment,
            environment.spec.internal_certificate_spec.clone(),
            environment.certificate_name(),
            environment.certificate_secret_name(),
            Some(vec![
                environment.service_name(),
                environment.service_internal_fqdn(),
            ]),
            CertificatePrivateKeyAlgorithm::Ed25519,
            None,
        )
    }
}

#[async_trait::async_trait]
impl k8s_controller::Context for Context {
    type Resource = Environment;
    type Error = Error;

    #[instrument(fields())]
    async fn apply(
        &self,
        client: Client,
        environment: &Self::Resource,
    ) -> Result<Option<Action>, Self::Error> {
        if environment.status.is_none() {
            let environment_api: Api<Environment> = Api::namespaced(
                client.clone(),
                &environment.meta().namespace.clone().unwrap(),
            );
            let mut new_environment = environment.clone();
            new_environment.status = Some(environment.status());
            environment_api
                .replace_status(
                    &environment.name_unchecked(),
                    &PostParams::default(),
                    serde_json::to_vec(&new_environment).unwrap(),
                )
                .await?;
            // Updating the status should trigger a reconciliation
            // which will include a status this time.
            return Ok(None);
        }

        let namespace = environment.namespace();
        let network_policy_api: Api<NetworkPolicy> = Api::namespaced(client.clone(), &namespace);
        let service_account_api: Api<ServiceAccount> = Api::namespaced(client.clone(), &namespace);
        let role_api: Api<Role> = Api::namespaced(client.clone(), &namespace);
        let role_binding_api: Api<RoleBinding> = Api::namespaced(client.clone(), &namespace);
        let certificate_api: Api<Certificate> = Api::namespaced(client.clone(), &namespace);

        for policy in self.create_network_policies(environment) {
            trace!("applying network policy {}", policy.name_unchecked());
            apply_resource(&network_policy_api, &policy).await?;
        }

        if let Some(service_account) = self.create_service_account(environment) {
            trace!("applying environmentd service account");
            apply_resource(&service_account_api, &service_account).await?;
        }

        trace!("applying environmentd role");
        apply_resource(&role_api, &self.create_role(environment)).await?;

        trace!("applying environmentd role binding");
        apply_resource(&role_binding_api, &self.create_role_binding(environment)).await?;

        if let Some(certificate) = self.create_certificate(environment) {
            trace!("creating new environmentd certificate");
            apply_resource(&certificate_api, &certificate).await?;
        }

        Ok(None)
    }
}

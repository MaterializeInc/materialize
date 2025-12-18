// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use k8s_openapi::{
    api::{
        apps::v1::{Deployment, DeploymentSpec},
        core::v1::{
            Affinity, Capabilities, ConfigMap, ConfigMapVolumeSource, Container, ContainerPort,
            EnvVar, HTTPGetAction, KeyToPath, PodSecurityContext, PodSpec, PodTemplateSpec, Probe,
            ResourceRequirements, SeccompProfile, SecretVolumeSource, SecurityContext, Service,
            ServicePort, ServiceSpec, Toleration, Volume, VolumeMount,
        },
        networking::v1::{
            IPBlock, NetworkPolicy, NetworkPolicyIngressRule, NetworkPolicyPeer, NetworkPolicyPort,
            NetworkPolicySpec,
        },
    },
    apimachinery::pkg::{
        apis::meta::v1::{Condition, LabelSelector, Time},
        util::intstr::IntOrString,
    },
};
use kube::{
    Api, Client, Resource, ResourceExt,
    api::{ObjectMeta, PostParams},
    runtime::{
        controller::Action,
        reflector::{ObjectRef, Store},
    },
};
use maplit::btreemap;
use serde::Serialize;
use tracing::trace;

use crate::{
    Error,
    k8s::{apply_resource, make_reflector},
    tls::{DefaultCertificateSpecs, create_certificate, issuer_ref_defined},
};
use mz_cloud_resources::crd::{
    ManagedResource,
    console::v1alpha1::{Console, HttpConnectionScheme},
    generated::cert_manager::certificates::{Certificate, CertificatePrivateKeyAlgorithm},
};
use mz_orchestrator_kubernetes::KubernetesImagePullPolicy;
use mz_ore::{cli::KeyValueArg, instrument};
use mz_server_core::listeners::AuthenticatorKind;

pub struct Config {
    pub enable_security_context: bool,
    pub enable_prometheus_scrape_annotations: bool,

    pub image_pull_policy: KubernetesImagePullPolicy,
    pub scheduler_name: Option<String>,
    pub console_node_selector: Vec<KeyValueArg<String, String>>,
    pub console_affinity: Option<Affinity>,
    pub console_tolerations: Option<Vec<Toleration>>,
    pub console_default_resources: Option<ResourceRequirements>,
    pub network_policies_ingress_enabled: bool,
    pub network_policies_ingress_cidrs: Vec<String>,

    pub default_certificate_specs: DefaultCertificateSpecs,

    pub console_http_port: u16,
    pub balancerd_http_port: u16,
}

#[derive(Serialize)]
struct AppConfig {
    version: String,
    auth: AppConfigAuth,
}

#[derive(Serialize)]
struct AppConfigAuth {
    mode: AuthenticatorKind,
}

pub struct Context {
    config: Config,
    deployments: Store<Deployment>,
}

impl Context {
    pub async fn new(config: Config, client: Client) -> Self {
        Self {
            config,
            deployments: make_reflector(client).await,
        }
    }

    async fn sync_deployment_status(
        &self,
        client: &Client,
        console: &Console,
    ) -> Result<(), kube::Error> {
        let namespace = console.namespace().unwrap();
        let console_api: Api<Console> = Api::namespaced(client.clone(), &namespace);

        let Some(deployment) = self
            .deployments
            .get(&ObjectRef::new(&console.deployment_name()).within(&namespace))
        else {
            return Ok(());
        };

        let Some(deployment_conditions) = &deployment
            .status
            .as_ref()
            .and_then(|status| status.conditions.as_ref())
        else {
            // if the deployment doesn't have any conditions set yet, there
            // is nothing to sync
            return Ok(());
        };

        let ready = deployment_conditions
            .iter()
            .any(|condition| condition.type_ == "Available" && condition.status == "True");
        let ready_str = if ready { "True" } else { "False" };

        let mut status = console.status.clone().unwrap();
        if status
            .conditions
            .iter()
            .any(|condition| condition.type_ == "Ready" && condition.status == ready_str)
        {
            // if the deployment status is already set correctly, we don't
            // need to set it again (this prevents us from getting stuck in
            // a reconcile loop)
            return Ok(());
        }

        status.conditions = vec![Condition {
            type_: "Ready".to_string(),
            status: ready_str.to_string(),
            last_transition_time: Time(chrono::offset::Utc::now()),
            message: format!(
                "console deployment is{} ready",
                if ready { "" } else { " not" }
            ),
            observed_generation: None,
            reason: "DeploymentStatus".to_string(),
        }];
        let mut new_console = console.clone();
        new_console.status = Some(status);

        console_api
            .replace_status(
                &console.name_unchecked(),
                &PostParams::default(),
                serde_json::to_vec(&new_console).unwrap(),
            )
            .await?;

        Ok(())
    }

    fn create_network_policies(&self, console: &Console) -> Vec<NetworkPolicy> {
        let mut network_policies = Vec::new();
        if self.config.network_policies_ingress_enabled {
            let console_label_selector = LabelSelector {
                match_labels: Some(
                    console
                        .default_labels()
                        .into_iter()
                        .chain([("materialize.cloud/app".to_owned(), console.app_name())])
                        .collect(),
                ),
                ..Default::default()
            };
            network_policies.extend([NetworkPolicy {
                metadata: console.managed_resource_meta(console.name_prefixed("console-ingress")),
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
                        ports: Some(vec![NetworkPolicyPort {
                            port: Some(IntOrString::Int(self.config.console_http_port.into())),
                            protocol: Some("TCP".to_string()),
                            ..Default::default()
                        }]),
                        ..Default::default()
                    }]),
                    pod_selector: Some(console_label_selector),
                    policy_types: Some(vec!["Ingress".to_owned()]),
                    ..Default::default()
                }),
            }]);
        }
        network_policies
    }

    fn create_console_external_certificate(&self, console: &Console) -> Option<Certificate> {
        create_certificate(
            self.config
                .default_certificate_specs
                .console_external
                .clone(),
            console,
            console.spec.external_certificate_spec.clone(),
            console.external_certificate_name(),
            console.external_certificate_secret_name(),
            None,
            CertificatePrivateKeyAlgorithm::Rsa,
            Some(4096),
        )
    }

    fn create_console_app_configmap_object(&self, console: &Console) -> ConfigMap {
        let version: String = console
            .spec
            .console_image_ref
            .rsplitn(2, ':')
            .next()
            .expect("at least one chunk, even if empty")
            .to_owned();
        let app_config_json = serde_json::to_string(&AppConfig {
            version,
            auth: AppConfigAuth {
                mode: console.spec.authenticator_kind,
            },
        })
        .expect("known valid");
        ConfigMap {
            binary_data: None,
            data: Some(btreemap! {
                "app-config.json".to_owned() => app_config_json,
            }),
            immutable: None,
            metadata: console.managed_resource_meta(console.configmap_name()),
        }
    }

    fn create_console_deployment_object(&self, console: &Console) -> Deployment {
        let mut pod_template_labels = console.default_labels();
        pod_template_labels.insert(
            "materialize.cloud/name".to_owned(),
            console.deployment_name(),
        );
        pod_template_labels.insert("app".to_owned(), "console".to_string());
        pod_template_labels.insert("materialize.cloud/app".to_owned(), console.app_name());

        let ports = vec![ContainerPort {
            container_port: self.config.console_http_port.into(),
            name: Some("http".into()),
            protocol: Some("TCP".into()),
            ..Default::default()
        }];

        let scheme = match console.spec.balancerd.scheme {
            HttpConnectionScheme::Http => "http",
            HttpConnectionScheme::Https => "https",
        };
        let mut env = vec![EnvVar {
            name: "MZ_ENDPOINT".to_string(),
            value: Some(format!(
                "{}://{}.{}.svc.cluster.local:{}",
                scheme,
                console.spec.balancerd.service_name,
                console.spec.balancerd.namespace,
                self.config.balancerd_http_port,
            )),
            ..Default::default()
        }];
        let mut volumes = vec![Volume {
            name: "app-config".to_string(),
            config_map: Some(ConfigMapVolumeSource {
                name: console.configmap_name(),
                default_mode: Some(256),
                optional: Some(false),
                items: Some(vec![KeyToPath {
                    key: "app-config.json".to_string(),
                    path: "app-config.json".to_string(),
                    ..Default::default()
                }]),
            }),
            ..Default::default()
        }];
        let mut volume_mounts = vec![VolumeMount {
            name: "app-config".to_string(),
            mount_path: "/usr/share/nginx/html/app-config".to_string(),
            ..Default::default()
        }];

        let scheme = if issuer_ref_defined(
            &self.config.default_certificate_specs.console_external,
            &console.spec.external_certificate_spec,
        ) {
            volumes.push(Volume {
                name: "external-certificate".to_owned(),
                secret: Some(SecretVolumeSource {
                    default_mode: Some(0o400),
                    secret_name: Some(console.external_certificate_secret_name()),
                    items: None,
                    optional: Some(false),
                }),
                ..Default::default()
            });
            volume_mounts.push(VolumeMount {
                name: "external-certificate".to_owned(),
                mount_path: "/nginx/tls".to_owned(),
                read_only: Some(true),
                ..Default::default()
            });
            env.push(EnvVar {
                name: "MZ_NGINX_LISTENER_CONFIG".to_string(),
                value: Some(format!(
                    "listen {} ssl;
ssl_certificate /nginx/tls/tls.crt;
ssl_certificate_key /nginx/tls/tls.key;",
                    self.config.console_http_port
                )),
                ..Default::default()
            });
            Some("HTTPS".to_owned())
        } else {
            env.push(EnvVar {
                name: "MZ_NGINX_LISTENER_CONFIG".to_string(),
                value: Some(format!("listen {};", self.config.console_http_port)),
                ..Default::default()
            });
            Some("HTTP".to_owned())
        };

        let probe = Probe {
            http_get: Some(HTTPGetAction {
                path: Some("/".to_string()),
                port: IntOrString::Int(self.config.console_http_port.into()),
                scheme,
                ..Default::default()
            }),
            ..Default::default()
        };

        let security_context = if self.config.enable_security_context {
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

        let container = Container {
            name: "console".to_owned(),
            image: Some(console.spec.console_image_ref.clone()),
            image_pull_policy: Some(self.config.image_pull_policy.to_string()),
            ports: Some(ports),
            env: Some(env),
            startup_probe: Some(Probe {
                period_seconds: Some(1),
                failure_threshold: Some(10),
                ..probe.clone()
            }),
            readiness_probe: Some(Probe {
                period_seconds: Some(30),
                failure_threshold: Some(1),
                ..probe.clone()
            }),
            liveness_probe: Some(Probe {
                period_seconds: Some(30),
                ..probe.clone()
            }),
            resources: console
                .spec
                .resource_requirements
                .clone()
                .or_else(|| self.config.console_default_resources.clone()),
            security_context,
            volume_mounts: Some(volume_mounts),
            ..Default::default()
        };

        let deployment_spec = DeploymentSpec {
            replicas: Some(console.replicas()),
            selector: LabelSelector {
                match_labels: Some(pod_template_labels.clone()),
                ..Default::default()
            },
            template: PodTemplateSpec {
                // not using managed_resource_meta because the pod should be
                // owned by the deployment, not the materialize instance
                metadata: Some(ObjectMeta {
                    labels: Some(pod_template_labels),
                    ..Default::default()
                }),
                spec: Some(PodSpec {
                    containers: vec![container],
                    node_selector: Some(
                        self.config
                            .console_node_selector
                            .iter()
                            .map(|selector| (selector.key.clone(), selector.value.clone()))
                            .collect(),
                    ),
                    affinity: self.config.console_affinity.clone(),
                    tolerations: self.config.console_tolerations.clone(),
                    scheduler_name: self.config.scheduler_name.clone(),
                    volumes: Some(volumes),
                    security_context: Some(PodSecurityContext {
                        fs_group: Some(101),
                        ..Default::default()
                    }),
                    ..Default::default()
                }),
            },
            ..Default::default()
        };

        Deployment {
            metadata: ObjectMeta {
                ..console.managed_resource_meta(console.deployment_name())
            },
            spec: Some(deployment_spec),
            status: None,
        }
    }

    fn create_console_service_object(&self, console: &Console) -> Service {
        let selector =
            btreemap! {"materialize.cloud/name".to_string() => console.deployment_name()};

        let ports = vec![ServicePort {
            name: Some("http".to_string()),
            protocol: Some("TCP".to_string()),
            port: self.config.console_http_port.into(),
            target_port: Some(IntOrString::Int(self.config.console_http_port.into())),
            ..Default::default()
        }];

        let spec = ServiceSpec {
            type_: Some("ClusterIP".to_string()),
            cluster_ip: Some("None".to_string()),
            selector: Some(selector),
            ports: Some(ports),
            ..Default::default()
        };

        Service {
            metadata: console.managed_resource_meta(console.service_name()),
            spec: Some(spec),
            status: None,
        }
    }
}

#[async_trait::async_trait]
impl k8s_controller::Context for Context {
    type Resource = Console;
    type Error = Error;

    #[instrument(fields())]
    async fn apply(
        &self,
        client: Client,
        console: &Self::Resource,
    ) -> Result<Option<Action>, Self::Error> {
        if console.status.is_none() {
            let console_api: Api<Console> =
                Api::namespaced(client.clone(), &console.meta().namespace.clone().unwrap());
            let mut new_console = console.clone();
            new_console.status = Some(console.status());
            console_api
                .replace_status(
                    &console.name_unchecked(),
                    &PostParams::default(),
                    serde_json::to_vec(&new_console).unwrap(),
                )
                .await?;
            // Updating the status should trigger a reconciliation
            // which will include a status this time.
            return Ok(None);
        }

        let namespace = console.namespace().unwrap();
        let network_policy_api: Api<NetworkPolicy> = Api::namespaced(client.clone(), &namespace);
        let configmap_api: Api<ConfigMap> = Api::namespaced(client.clone(), &namespace);
        let deployment_api: Api<Deployment> = Api::namespaced(client.clone(), &namespace);
        let service_api: Api<Service> = Api::namespaced(client.clone(), &namespace);
        let certificate_api: Api<Certificate> = Api::namespaced(client.clone(), &namespace);

        trace!("creating new network policies");
        let network_policies = self.create_network_policies(console);
        for network_policy in &network_policies {
            apply_resource(&network_policy_api, network_policy).await?;
        }

        trace!("creating new console configmap");
        let console_configmap = self.create_console_app_configmap_object(console);
        apply_resource(&configmap_api, &console_configmap).await?;

        trace!("creating new console deployment");
        let console_deployment = self.create_console_deployment_object(console);
        apply_resource(&deployment_api, &console_deployment).await?;

        trace!("creating new console service");
        let console_service = self.create_console_service_object(console);
        apply_resource(&service_api, &console_service).await?;

        let console_external_certificate = self.create_console_external_certificate(console);
        if let Some(certificate) = &console_external_certificate {
            trace!("creating new console external certificate");
            apply_resource(&certificate_api, certificate).await?;
        }

        self.sync_deployment_status(&client, console).await?;

        Ok(None)
    }
}

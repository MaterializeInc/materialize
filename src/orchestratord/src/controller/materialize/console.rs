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
            Capabilities, ConfigMap, ConfigMapVolumeSource, Container, ContainerPort, EnvVar,
            HTTPGetAction, KeyToPath, PodSecurityContext, PodSpec, PodTemplateSpec, Probe,
            SeccompProfile, SecretVolumeSource, SecurityContext, Service, ServicePort, ServiceSpec,
            Volume, VolumeMount,
        },
        networking::v1::{
            IPBlock, NetworkPolicy, NetworkPolicyIngressRule, NetworkPolicyPeer, NetworkPolicyPort,
            NetworkPolicySpec,
        },
    },
    apimachinery::pkg::{apis::meta::v1::LabelSelector, util::intstr::IntOrString},
};
use kube::{Api, Client, ResourceExt, api::ObjectMeta, runtime::controller::Action};
use maplit::btreemap;
use mz_server_core::listeners::AuthenticatorKind;
use serde::Serialize;
use tracing::trace;

use crate::{
    controller::materialize::tls::{create_certificate, issuer_ref_defined},
    k8s::{apply_resource, delete_resource},
};
use mz_cloud_resources::crd::{
    generated::cert_manager::certificates::Certificate, materialize::v1alpha1::Materialize,
};

pub struct Resources {
    network_policies: Vec<NetworkPolicy>,
    console_configmap: Box<ConfigMap>,
    console_deployment: Box<Deployment>,
    console_service: Box<Service>,
    console_external_certificate: Box<Option<Certificate>>,
}

impl Resources {
    pub fn new(
        config: &super::MaterializeControllerArgs,
        mz: &Materialize,
        console_image_ref: &str,
    ) -> Self {
        let network_policies = create_network_policies(config, mz);
        let console_configmap =
            Box::new(create_console_app_configmap_object(mz, console_image_ref));
        let console_deployment = Box::new(create_console_deployment_object(
            config,
            mz,
            console_image_ref,
        ));
        let console_service = Box::new(create_console_service_object(config, mz));
        let console_external_certificate =
            Box::new(create_console_external_certificate(config, mz));
        Self {
            network_policies,
            console_configmap,
            console_deployment,
            console_service,
            console_external_certificate,
        }
    }

    pub async fn apply(
        &self,
        client: &Client,
        namespace: &str,
    ) -> Result<Option<Action>, anyhow::Error> {
        let network_policy_api: Api<NetworkPolicy> = Api::namespaced(client.clone(), namespace);
        let configmap_api: Api<ConfigMap> = Api::namespaced(client.clone(), namespace);
        let deployment_api: Api<Deployment> = Api::namespaced(client.clone(), namespace);
        let service_api: Api<Service> = Api::namespaced(client.clone(), namespace);
        let certificate_api: Api<Certificate> = Api::namespaced(client.clone(), namespace);

        trace!("creating new network policies");
        for network_policy in &self.network_policies {
            apply_resource(&network_policy_api, network_policy).await?;
        }

        trace!("creating new console configmap");
        apply_resource(&configmap_api, &self.console_configmap).await?;

        trace!("creating new console deployment");
        apply_resource(&deployment_api, &self.console_deployment).await?;

        trace!("creating new console service");
        apply_resource(&service_api, &self.console_service).await?;

        if let Some(certificate) = &*self.console_external_certificate {
            trace!("creating new console external certificate");
            apply_resource(&certificate_api, certificate).await?;
        }

        Ok(None)
    }

    pub async fn cleanup(
        &self,
        client: &Client,
        namespace: &str,
    ) -> Result<Option<Action>, anyhow::Error> {
        let network_policy_api: Api<NetworkPolicy> = Api::namespaced(client.clone(), namespace);
        let configmap_api: Api<ConfigMap> = Api::namespaced(client.clone(), namespace);
        let deployment_api: Api<Deployment> = Api::namespaced(client.clone(), namespace);
        let service_api: Api<Service> = Api::namespaced(client.clone(), namespace);
        let certificate_api: Api<Certificate> = Api::namespaced(client.clone(), namespace);

        if let Some(certificate) = &*self.console_external_certificate {
            trace!("deleting console external certificate");
            delete_resource(&certificate_api, &certificate.name_unchecked()).await?;
        }

        trace!("deleting console service");
        delete_resource(&service_api, &self.console_service.name_unchecked()).await?;

        trace!("deleting console deployment");
        delete_resource(&deployment_api, &self.console_deployment.name_unchecked()).await?;

        trace!("deleting console configmap");
        delete_resource(&configmap_api, &self.console_configmap.name_unchecked()).await?;

        trace!("deleting network policies");
        for network_policy in &self.network_policies {
            delete_resource(&network_policy_api, &network_policy.name_unchecked()).await?;
        }

        Ok(None)
    }
}

fn create_network_policies(
    config: &super::MaterializeControllerArgs,
    mz: &Materialize,
) -> Vec<NetworkPolicy> {
    let mut network_policies = Vec::new();
    if config.network_policies.ingress_enabled {
        let console_label_selector = LabelSelector {
            match_labels: Some(
                mz.default_labels()
                    .into_iter()
                    .chain([("materialize.cloud/app".to_owned(), mz.console_app_name())])
                    .collect(),
            ),
            ..Default::default()
        };
        network_policies.extend([NetworkPolicy {
            metadata: mz.managed_resource_meta(mz.name_prefixed("console-ingress")),
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
                    ports: Some(vec![NetworkPolicyPort {
                        port: Some(IntOrString::Int(config.console_http_port.into())),
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

fn create_console_external_certificate(
    config: &super::MaterializeControllerArgs,
    mz: &Materialize,
) -> Option<Certificate> {
    create_certificate(
        config.default_certificate_specs.console_external.clone(),
        mz,
        mz.spec.console_external_certificate_spec.clone(),
        mz.console_external_certificate_name(),
        mz.console_external_certificate_secret_name(),
        None,
    )
}

#[derive(Serialize)]
struct ConsoleAppConfig {
    version: String,
    auth: ConsoleAppConfigAuth,
}

#[derive(Serialize)]
struct ConsoleAppConfigAuth {
    mode: AuthenticatorKind,
}

fn create_console_app_configmap_object(mz: &Materialize, console_image_ref: &str) -> ConfigMap {
    let version: String = console_image_ref
        .rsplitn(2, ':')
        .next()
        .expect("at least one chunk, even if empty")
        .to_owned();
    let app_config_json = serde_json::to_string(&ConsoleAppConfig {
        version,
        auth: ConsoleAppConfigAuth {
            mode: mz.spec.authenticator_kind,
        },
    })
    .expect("known valid");
    ConfigMap {
        binary_data: None,
        data: Some(btreemap! {
            "app-config.json".to_owned() => app_config_json,
        }),
        immutable: None,
        metadata: mz.managed_resource_meta(mz.console_configmap_name()),
    }
}

fn create_console_deployment_object(
    config: &super::MaterializeControllerArgs,
    mz: &Materialize,
    console_image_ref: &str,
) -> Deployment {
    let mut pod_template_labels = mz.default_labels();
    pod_template_labels.insert(
        "materialize.cloud/name".to_owned(),
        mz.console_deployment_name(),
    );
    pod_template_labels.insert("app".to_owned(), "console".to_string());
    pod_template_labels.insert("materialize.cloud/app".to_owned(), mz.console_app_name());

    let ports = vec![ContainerPort {
        container_port: config.console_http_port.into(),
        name: Some("http".into()),
        protocol: Some("TCP".into()),
        ..Default::default()
    }];

    let scheme = if issuer_ref_defined(
        &config.default_certificate_specs.balancerd_external,
        &mz.spec.balancerd_external_certificate_spec,
    ) {
        "https"
    } else {
        "http"
    };
    let mut env = vec![EnvVar {
        name: "MZ_ENDPOINT".to_string(),
        value: Some(format!(
            "{}://{}.{}.svc.cluster.local:{}",
            scheme,
            mz.balancerd_service_name(),
            mz.namespace(),
            config.balancerd_http_port,
        )),
        ..Default::default()
    }];
    let mut volumes = vec![Volume {
        name: "app-config".to_string(),
        config_map: Some(ConfigMapVolumeSource {
            name: mz.console_configmap_name(),
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
        &config.default_certificate_specs.console_external,
        &mz.spec.console_external_certificate_spec,
    ) {
        volumes.push(Volume {
            name: "external-certificate".to_owned(),
            secret: Some(SecretVolumeSource {
                default_mode: Some(0o400),
                secret_name: Some(mz.console_external_certificate_secret_name()),
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
                config.console_http_port
            )),
            ..Default::default()
        });
        Some("HTTPS".to_owned())
    } else {
        env.push(EnvVar {
            name: "MZ_NGINX_LISTENER_CONFIG".to_string(),
            value: Some(format!("listen {};", config.console_http_port)),
            ..Default::default()
        });
        Some("HTTP".to_owned())
    };

    let probe = Probe {
        http_get: Some(HTTPGetAction {
            path: Some("/".to_string()),
            port: IntOrString::Int(config.console_http_port.into()),
            scheme,
            ..Default::default()
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

    let container = Container {
        name: "console".to_owned(),
        image: Some(console_image_ref.to_string()),
        image_pull_policy: Some(config.image_pull_policy.to_string()),
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
        resources: mz.spec.console_resource_requirements.clone(),
        security_context,
        volume_mounts: Some(volume_mounts),
        ..Default::default()
    };

    let deployment_spec = DeploymentSpec {
        replicas: Some(2),
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
                    config
                        .console_node_selector
                        .iter()
                        .map(|selector| (selector.key.clone(), selector.value.clone()))
                        .collect(),
                ),
                affinity: config.console_affinity.clone(),
                tolerations: config.console_tolerations.clone(),
                scheduler_name: config.scheduler_name.clone(),
                service_account_name: Some(mz.service_account_name()),
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
            ..mz.managed_resource_meta(mz.console_deployment_name())
        },
        spec: Some(deployment_spec),
        status: None,
    }
}

fn create_console_service_object(
    config: &super::MaterializeControllerArgs,
    mz: &Materialize,
) -> Service {
    let selector = btreemap! {"materialize.cloud/name".to_string() => mz.console_deployment_name()};

    let ports = vec![ServicePort {
        name: Some("http".to_string()),
        protocol: Some("TCP".to_string()),
        port: config.console_http_port.into(),
        target_port: Some(IntOrString::Int(config.console_http_port.into())),
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
        metadata: mz.managed_resource_meta(mz.console_service_name()),
        spec: Some(spec),
        status: None,
    }
}

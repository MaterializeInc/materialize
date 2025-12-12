// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use anyhow::bail;
use k8s_openapi::{
    api::{
        apps::v1::{Deployment, DeploymentSpec, DeploymentStrategy, RollingUpdateDeployment},
        core::v1::{
            Affinity, Capabilities, Container, ContainerPort, HTTPGetAction, PodSecurityContext,
            PodSpec, PodTemplateSpec, Probe, ResourceRequirements, SeccompProfile,
            SecretVolumeSource, SecurityContext, Service, ServicePort, ServiceSpec, Toleration,
            Volume, VolumeMount,
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
use tracing::trace;

use crate::{
    Error,
    k8s::{apply_resource, make_reflector},
    tls::{DefaultCertificateSpecs, create_certificate, issuer_ref_defined},
};
use mz_cloud_resources::crd::{
    ManagedResource,
    balancer::v1alpha1::{Balancer, Routing},
    generated::cert_manager::certificates::{Certificate, CertificatePrivateKeyAlgorithm},
};
use mz_orchestrator_kubernetes::KubernetesImagePullPolicy;
use mz_ore::{cli::KeyValueArg, instrument};

pub struct Config {
    pub enable_security_context: bool,
    pub enable_prometheus_scrape_annotations: bool,

    pub image_pull_policy: KubernetesImagePullPolicy,
    pub scheduler_name: Option<String>,
    pub balancerd_node_selector: Vec<KeyValueArg<String, String>>,
    pub balancerd_affinity: Option<Affinity>,
    pub balancerd_tolerations: Option<Vec<Toleration>>,
    pub balancerd_default_resources: Option<ResourceRequirements>,

    pub default_certificate_specs: DefaultCertificateSpecs,

    pub environmentd_sql_port: u16,
    pub environmentd_http_port: u16,
    pub balancerd_sql_port: u16,
    pub balancerd_http_port: u16,
    pub balancerd_internal_http_port: u16,
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
        balancer: &Balancer,
    ) -> Result<(), kube::Error> {
        let namespace = balancer.namespace().unwrap();
        let balancer_api: Api<Balancer> = Api::namespaced(client.clone(), &namespace);

        let Some(deployment) = self
            .deployments
            .get(&ObjectRef::new(&balancer.deployment_name()).within(&namespace))
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

        let mut status = balancer.status.clone().unwrap();
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
                "balancerd deployment is{} ready",
                if ready { "" } else { " not" }
            ),
            observed_generation: None,
            reason: "DeploymentStatus".to_string(),
        }];
        let mut new_balancer = balancer.clone();
        new_balancer.status = Some(status);

        balancer_api
            .replace_status(
                &balancer.name_unchecked(),
                &PostParams::default(),
                serde_json::to_vec(&new_balancer).unwrap(),
            )
            .await?;

        Ok(())
    }

    fn create_external_certificate_object(&self, balancer: &Balancer) -> Option<Certificate> {
        create_certificate(
            self.config
                .default_certificate_specs
                .balancerd_external
                .clone(),
            balancer,
            balancer.spec.external_certificate_spec.clone(),
            balancer.external_certificate_name(),
            balancer.external_certificate_secret_name(),
            None,
            CertificatePrivateKeyAlgorithm::Rsa,
            Some(4096),
        )
    }

    fn create_deployment_object(&self, balancer: &Balancer) -> anyhow::Result<Deployment> {
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

        let pod_template_annotations = if self.config.enable_prometheus_scrape_annotations {
            Some(btreemap! {
                "prometheus.io/scrape".to_owned() => "true".to_string(),
                "prometheus.io/port".to_owned() => self.config.balancerd_internal_http_port.to_string(),
                "prometheus.io/path".to_owned() => "/metrics".to_string(),
                "prometheus.io/scheme".to_owned() => "http".to_string(),
            })
        } else {
            None
        };
        let mut pod_template_labels = balancer.default_labels();
        pod_template_labels.insert(
            "materialize.cloud/name".to_owned(),
            balancer.deployment_name(),
        );
        pod_template_labels.insert("app".to_owned(), "balancerd".to_string());
        pod_template_labels.insert("materialize.cloud/app".to_owned(), balancer.app_name());

        let ports = vec![
            ContainerPort {
                container_port: self.config.balancerd_sql_port.into(),
                name: Some("pgwire".into()),
                protocol: Some("TCP".into()),
                ..Default::default()
            },
            ContainerPort {
                container_port: self.config.balancerd_http_port.into(),
                name: Some("http".into()),
                protocol: Some("TCP".into()),
                ..Default::default()
            },
            ContainerPort {
                container_port: self.config.balancerd_internal_http_port.into(),
                name: Some("internal-http".into()),
                protocol: Some("TCP".into()),
                ..Default::default()
            },
        ];

        let mut args = vec![
            "service".to_string(),
            format!(
                "--pgwire-listen-addr=0.0.0.0:{}",
                self.config.balancerd_sql_port
            ),
            format!(
                "--https-listen-addr=0.0.0.0:{}",
                self.config.balancerd_http_port
            ),
            format!(
                "--internal-http-listen-addr=0.0.0.0:{}",
                self.config.balancerd_internal_http_port
            ),
        ];
        match balancer.routing()? {
            Routing::Static(static_routing_config) => {
                args.extend([
                    format!(
                        "--https-resolver-template={}.{}.svc.cluster.local:{}",
                        static_routing_config.environmentd_service_name,
                        static_routing_config.environmentd_namespace,
                        self.config.environmentd_http_port
                    ),
                    format!(
                        "--static-resolver-addr={}.{}.svc.cluster.local:{}",
                        static_routing_config.environmentd_service_name,
                        static_routing_config.environmentd_namespace,
                        self.config.environmentd_sql_port
                    ),
                ]);
            }
            Routing::Frontegg(_frontegg_routing_config) => {
                bail!("frontegg routing is not yet implemented");
            }
        }

        if issuer_ref_defined(
            &self.config.default_certificate_specs.internal,
            &balancer.spec.internal_certificate_spec,
        ) {
            args.push("--internal-tls".to_owned())
        }

        let mut volumes = Vec::new();
        let mut volume_mounts = Vec::new();
        if issuer_ref_defined(
            &self.config.default_certificate_specs.balancerd_external,
            &balancer.spec.external_certificate_spec,
        ) {
            volumes.push(Volume {
                name: "external-certificate".to_owned(),
                secret: Some(SecretVolumeSource {
                    default_mode: Some(0o400),
                    secret_name: Some(balancer.external_certificate_secret_name()),
                    items: None,
                    optional: Some(false),
                }),
                ..Default::default()
            });
            volume_mounts.push(VolumeMount {
                name: "external-certificate".to_owned(),
                mount_path: "/etc/external_tls".to_owned(),
                read_only: Some(true),
                ..Default::default()
            });
            args.extend([
                "--tls-mode=require".into(),
                "--tls-cert=/etc/external_tls/tls.crt".into(),
                "--tls-key=/etc/external_tls/tls.key".into(),
            ]);
        } else {
            args.push("--tls-mode=disable".to_string());
        }

        let startup_probe = Probe {
            http_get: Some(HTTPGetAction {
                port: IntOrString::Int(self.config.balancerd_internal_http_port.into()),
                path: Some("/api/readyz".into()),
                ..Default::default()
            }),
            failure_threshold: Some(20),
            initial_delay_seconds: Some(3),
            period_seconds: Some(3),
            success_threshold: Some(1),
            timeout_seconds: Some(1),
            ..Default::default()
        };
        let readiness_probe = Probe {
            http_get: Some(HTTPGetAction {
                port: IntOrString::Int(self.config.balancerd_internal_http_port.into()),
                path: Some("/api/readyz".into()),
                ..Default::default()
            }),
            failure_threshold: Some(3),
            period_seconds: Some(10),
            success_threshold: Some(1),
            timeout_seconds: Some(1),
            ..Default::default()
        };
        let liveness_probe = Probe {
            http_get: Some(HTTPGetAction {
                port: IntOrString::Int(self.config.balancerd_internal_http_port.into()),
                path: Some("/api/livez".into()),
                ..Default::default()
            }),
            failure_threshold: Some(3),
            initial_delay_seconds: Some(8),
            period_seconds: Some(10),
            success_threshold: Some(1),
            timeout_seconds: Some(1),
            ..Default::default()
        };

        let container = Container {
            name: "balancerd".to_owned(),
            image: Some(balancer.spec.balancerd_image_ref.clone()),
            image_pull_policy: Some(self.config.image_pull_policy.to_string()),
            ports: Some(ports),
            args: Some(args),
            startup_probe: Some(startup_probe),
            readiness_probe: Some(readiness_probe),
            liveness_probe: Some(liveness_probe),
            resources: balancer
                .spec
                .resource_requirements
                .clone()
                .or_else(|| self.config.balancerd_default_resources.clone()),
            security_context: security_context.clone(),
            volume_mounts: Some(volume_mounts),
            ..Default::default()
        };

        let deployment_spec = DeploymentSpec {
            replicas: Some(balancer.replicas()),
            selector: LabelSelector {
                match_labels: Some(pod_template_labels.clone()),
                ..Default::default()
            },
            strategy: Some(DeploymentStrategy {
                rolling_update: Some(RollingUpdateDeployment {
                    // Allow a complete set of new pods at once, to minimize the
                    // chances of a new connection going to a pod that will be
                    // immediately drained
                    max_surge: Some(IntOrString::String("100%".into())),
                    ..Default::default()
                }),
                ..Default::default()
            }),
            template: PodTemplateSpec {
                // not using managed_resource_meta because the pod should be
                // owned by the deployment, not the materialize instance
                metadata: Some(ObjectMeta {
                    annotations: pod_template_annotations,
                    labels: Some(pod_template_labels),
                    ..Default::default()
                }),
                spec: Some(PodSpec {
                    containers: vec![container],
                    node_selector: Some(
                        self.config
                            .balancerd_node_selector
                            .iter()
                            .map(|selector| (selector.key.clone(), selector.value.clone()))
                            .collect(),
                    ),
                    affinity: self.config.balancerd_affinity.clone(),
                    tolerations: self.config.balancerd_tolerations.clone(),
                    security_context: Some(PodSecurityContext {
                        fs_group: Some(999),
                        run_as_user: Some(999),
                        run_as_group: Some(999),
                        ..Default::default()
                    }),
                    scheduler_name: self.config.scheduler_name.clone(),
                    volumes: Some(volumes),
                    ..Default::default()
                }),
            },
            ..Default::default()
        };

        Ok(Deployment {
            metadata: balancer.managed_resource_meta(balancer.deployment_name()),
            spec: Some(deployment_spec),
            status: None,
        })
    }

    fn create_service_object(&self, balancer: &Balancer) -> Service {
        let selector =
            btreemap! {"materialize.cloud/name".to_string() => balancer.deployment_name()};

        let ports = vec![
            ServicePort {
                name: Some("http".to_string()),
                protocol: Some("TCP".to_string()),
                port: self.config.balancerd_http_port.into(),
                target_port: Some(IntOrString::Int(self.config.balancerd_http_port.into())),
                ..Default::default()
            },
            ServicePort {
                name: Some("pgwire".to_string()),
                protocol: Some("TCP".to_string()),
                port: self.config.balancerd_sql_port.into(),
                target_port: Some(IntOrString::Int(self.config.balancerd_sql_port.into())),
                ..Default::default()
            },
        ];

        let spec = ServiceSpec {
            type_: Some("ClusterIP".to_string()),
            cluster_ip: Some("None".to_string()),
            selector: Some(selector),
            ports: Some(ports),
            ..Default::default()
        };

        Service {
            metadata: balancer.managed_resource_meta(balancer.service_name()),
            spec: Some(spec),
            status: None,
        }
    }
}

#[async_trait::async_trait]
impl k8s_controller::Context for Context {
    type Resource = Balancer;
    type Error = Error;

    #[instrument(fields())]
    async fn apply(
        &self,
        client: Client,
        balancer: &Self::Resource,
    ) -> Result<Option<Action>, Self::Error> {
        if balancer.status.is_none() {
            let balancer_api: Api<Balancer> =
                Api::namespaced(client.clone(), &balancer.meta().namespace.clone().unwrap());
            let mut new_balancer = balancer.clone();
            new_balancer.status = Some(balancer.status());
            balancer_api
                .replace_status(
                    &balancer.name_unchecked(),
                    &PostParams::default(),
                    serde_json::to_vec(&new_balancer).unwrap(),
                )
                .await?;
            // Updating the status should trigger a reconciliation
            // which will include a status this time.
            return Ok(None);
        }

        let namespace = balancer.namespace().unwrap();
        let certificate_api: Api<Certificate> = Api::namespaced(client.clone(), &namespace);
        let deployment_api: Api<Deployment> = Api::namespaced(client.clone(), &namespace);
        let service_api: Api<Service> = Api::namespaced(client.clone(), &namespace);

        if let Some(external_certificate) = self.create_external_certificate_object(balancer) {
            trace!("creating new balancerd external certificate");
            apply_resource(&certificate_api, &external_certificate).await?;
        }

        let deployment = self.create_deployment_object(balancer)?;
        trace!("creating new balancerd deployment");
        apply_resource(&deployment_api, &deployment).await?;

        let service = self.create_service_object(balancer);
        trace!("creating new balancerd service");
        apply_resource(&service_api, &service).await?;

        self.sync_deployment_status(&client, balancer).await?;

        Ok(None)
    }
}

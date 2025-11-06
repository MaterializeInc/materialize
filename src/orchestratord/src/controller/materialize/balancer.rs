// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::time::Duration;

use k8s_openapi::{
    api::{
        apps::v1::{Deployment, DeploymentSpec, DeploymentStrategy, RollingUpdateDeployment},
        core::v1::{
            Capabilities, Container, ContainerPort, HTTPGetAction, PodSecurityContext, PodSpec,
            PodTemplateSpec, Probe, SeccompProfile, SecretVolumeSource, SecurityContext, Service,
            ServicePort, ServiceSpec, Volume, VolumeMount,
        },
    },
    apimachinery::pkg::{apis::meta::v1::LabelSelector, util::intstr::IntOrString},
};
use kube::{Api, Client, ResourceExt, api::ObjectMeta, runtime::controller::Action};
use maplit::btreemap;
use tracing::trace;

use crate::{
    controller::materialize::{
        Error, matching_image_from_environmentd_image_ref,
        tls::{create_certificate, issuer_ref_defined},
    },
    k8s::{apply_resource, delete_resource, get_resource},
};
use mz_cloud_resources::crd::{
    generated::cert_manager::certificates::{Certificate, CertificatePrivateKeyAlgorithm},
    materialize::v1alpha1::Materialize,
};
use mz_ore::instrument;

pub struct Resources {
    balancerd_external_certificate: Box<Option<Certificate>>,
    balancerd_deployment: Box<Deployment>,
    balancerd_service: Box<Service>,
}

impl Resources {
    pub fn new(config: &super::MaterializeControllerArgs, mz: &Materialize) -> Self {
        let balancerd_external_certificate =
            Box::new(create_balancerd_external_certificate(config, mz));
        let balancerd_deployment = Box::new(create_balancerd_deployment_object(config, mz));
        let balancerd_service = Box::new(create_balancerd_service_object(config, mz));

        Self {
            balancerd_external_certificate,
            balancerd_deployment,
            balancerd_service,
        }
    }

    #[instrument]
    pub async fn apply(&self, client: &Client, namespace: &str) -> Result<Option<Action>, Error> {
        let certificate_api: Api<Certificate> = Api::namespaced(client.clone(), namespace);
        let deployment_api: Api<Deployment> = Api::namespaced(client.clone(), namespace);
        let service_api: Api<Service> = Api::namespaced(client.clone(), namespace);

        if let Some(certificate) = &*self.balancerd_external_certificate {
            trace!("creating new balancerd external certificate");
            apply_resource(&certificate_api, certificate).await?;
        }

        trace!("creating new balancerd deployment");
        apply_resource(&deployment_api, &*self.balancerd_deployment).await?;

        trace!("creating new balancerd service");
        apply_resource(&service_api, &*self.balancerd_service).await?;

        if let Some(deployment) =
            get_resource(&deployment_api, &self.balancerd_deployment.name_unchecked()).await?
        {
            for condition in deployment
                .status
                .as_ref()
                .and_then(|status| status.conditions.as_deref())
                .unwrap_or(&[])
            {
                if condition.type_ == "Available" && condition.status == "True" {
                    return Ok(None);
                }
            }
        }

        Ok(Some(Action::requeue(Duration::from_secs(1))))
    }

    #[instrument]
    pub async fn cleanup(
        &self,
        client: &Client,
        namespace: &str,
    ) -> Result<Option<Action>, anyhow::Error> {
        let certificate_api: Api<Certificate> = Api::namespaced(client.clone(), namespace);
        let deployment_api: Api<Deployment> = Api::namespaced(client.clone(), namespace);
        let service_api: Api<Service> = Api::namespaced(client.clone(), namespace);

        trace!("deleting balancerd service");
        delete_resource(&service_api, &self.balancerd_service.name_unchecked()).await?;

        trace!("deleting balancerd deployment");
        delete_resource(&deployment_api, &self.balancerd_deployment.name_unchecked()).await?;

        if let Some(certificate) = &*self.balancerd_external_certificate {
            trace!("deleting balancerd external certificate");
            delete_resource(&certificate_api, &certificate.name_unchecked()).await?;
        }

        Ok(None)
    }
}

fn create_balancerd_external_certificate(
    config: &super::MaterializeControllerArgs,
    mz: &Materialize,
) -> Option<Certificate> {
    create_certificate(
        config.default_certificate_specs.balancerd_external.clone(),
        mz,
        mz.spec.balancerd_external_certificate_spec.clone(),
        mz.balancerd_external_certificate_name(),
        mz.balancerd_external_certificate_secret_name(),
        None,
        CertificatePrivateKeyAlgorithm::Rsa,
        Some(4096),
    )
}

fn create_balancerd_deployment_object(
    config: &super::MaterializeControllerArgs,
    mz: &Materialize,
) -> Deployment {
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

    let pod_template_annotations = if config.enable_prometheus_scrape_annotations {
        Some(btreemap! {
            "prometheus.io/scrape".to_owned() => "true".to_string(),
            "prometheus.io/port".to_owned() => config.balancerd_internal_http_port.to_string(),
            "prometheus.io/path".to_owned() => "/metrics".to_string(),
            "prometheus.io/scheme".to_owned() => "http".to_string(),
        })
    } else {
        None
    };
    let mut pod_template_labels = mz.default_labels();
    pod_template_labels.insert(
        "materialize.cloud/name".to_owned(),
        mz.balancerd_deployment_name(),
    );
    pod_template_labels.insert("app".to_owned(), "balancerd".to_string());
    pod_template_labels.insert("materialize.cloud/app".to_owned(), mz.balancerd_app_name());

    let ports = vec![
        ContainerPort {
            container_port: config.balancerd_sql_port.into(),
            name: Some("pgwire".into()),
            protocol: Some("TCP".into()),
            ..Default::default()
        },
        ContainerPort {
            container_port: config.balancerd_http_port.into(),
            name: Some("http".into()),
            protocol: Some("TCP".into()),
            ..Default::default()
        },
        ContainerPort {
            container_port: config.balancerd_internal_http_port.into(),
            name: Some("internal-http".into()),
            protocol: Some("TCP".into()),
            ..Default::default()
        },
    ];

    let mut args = vec![
        "service".to_string(),
        format!("--pgwire-listen-addr=0.0.0.0:{}", config.balancerd_sql_port),
        format!("--https-listen-addr=0.0.0.0:{}", config.balancerd_http_port),
        format!(
            "--internal-http-listen-addr=0.0.0.0:{}",
            config.balancerd_internal_http_port
        ),
        format!(
            "--https-resolver-template={}.{}.svc.cluster.local:{}",
            mz.environmentd_service_name(),
            mz.namespace(),
            config.environmentd_http_port
        ),
        format!(
            "--static-resolver-addr={}.{}.svc.cluster.local:{}",
            mz.environmentd_service_name(),
            mz.namespace(),
            config.environmentd_sql_port
        ),
    ];

    if issuer_ref_defined(
        &config.default_certificate_specs.internal,
        &mz.spec.internal_certificate_spec,
    ) {
        args.push("--internal-tls".to_owned())
    }

    let mut volumes = Vec::new();
    let mut volume_mounts = Vec::new();
    if issuer_ref_defined(
        &config.default_certificate_specs.balancerd_external,
        &mz.spec.balancerd_external_certificate_spec,
    ) {
        volumes.push(Volume {
            name: "external-certificate".to_owned(),
            secret: Some(SecretVolumeSource {
                default_mode: Some(0o400),
                secret_name: Some(mz.balancerd_external_certificate_secret_name()),
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
            port: IntOrString::Int(config.balancerd_internal_http_port.into()),
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
            port: IntOrString::Int(config.balancerd_internal_http_port.into()),
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
            port: IntOrString::Int(config.balancerd_internal_http_port.into()),
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
        image: Some(matching_image_from_environmentd_image_ref(
            &mz.spec.environmentd_image_ref,
            "balancerd",
            None,
        )),
        image_pull_policy: Some(config.image_pull_policy.to_string()),
        ports: Some(ports),
        args: Some(args),
        startup_probe: Some(startup_probe),
        readiness_probe: Some(readiness_probe),
        liveness_probe: Some(liveness_probe),
        resources: mz
            .spec
            .balancerd_resource_requirements
            .clone()
            .or_else(|| config.balancerd_default_resources.clone()),
        security_context: security_context.clone(),
        volume_mounts: Some(volume_mounts),
        ..Default::default()
    };

    let deployment_spec = DeploymentSpec {
        replicas: Some(mz.balancerd_replicas()),
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
                    config
                        .balancerd_node_selector
                        .iter()
                        .map(|selector| (selector.key.clone(), selector.value.clone()))
                        .collect(),
                ),
                affinity: config.balancerd_affinity.clone(),
                tolerations: config.balancerd_tolerations.clone(),
                security_context: Some(PodSecurityContext {
                    fs_group: Some(999),
                    run_as_user: Some(999),
                    run_as_group: Some(999),
                    ..Default::default()
                }),
                scheduler_name: config.scheduler_name.clone(),
                service_account_name: Some(mz.service_account_name()),
                volumes: Some(volumes),
                ..Default::default()
            }),
        },
        ..Default::default()
    };

    Deployment {
        metadata: ObjectMeta {
            ..mz.managed_resource_meta(mz.balancerd_deployment_name())
        },
        spec: Some(deployment_spec),
        status: None,
    }
}

fn create_balancerd_service_object(
    config: &super::MaterializeControllerArgs,
    mz: &Materialize,
) -> Service {
    let selector =
        btreemap! {"materialize.cloud/name".to_string() => mz.balancerd_deployment_name()};

    let ports = vec![
        ServicePort {
            name: Some("http".to_string()),
            protocol: Some("TCP".to_string()),
            port: config.balancerd_http_port.into(),
            target_port: Some(IntOrString::Int(config.balancerd_http_port.into())),
            ..Default::default()
        },
        ServicePort {
            name: Some("pgwire".to_string()),
            protocol: Some("TCP".to_string()),
            port: config.balancerd_sql_port.into(),
            target_port: Some(IntOrString::Int(config.balancerd_sql_port.into())),
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
        metadata: mz.managed_resource_meta(mz.balancerd_service_name()),
        spec: Some(spec),
        status: None,
    }
}

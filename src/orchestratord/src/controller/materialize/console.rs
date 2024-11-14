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
            Capabilities, Container, ContainerPort, EnvVar, HTTPGetAction, PodSpec,
            PodTemplateSpec, Probe, SeccompProfile, SecurityContext, Service, ServicePort,
            ServiceSpec,
        },
    },
    apimachinery::pkg::{apis::meta::v1::LabelSelector, util::intstr::IntOrString},
};
use kube::{api::ObjectMeta, Api, Client};
use maplit::btreemap;

use crate::k8s::apply_resource;
use mz_cloud_resources::crd::materialize::v1alpha1::Materialize;

// corresponds to the `EXPOSE` line in the console dockerfile
const CONSOLE_IMAGE_HTTP_PORT: i32 = 8080;

pub struct Resources {
    console_deployment: Box<Deployment>,
    console_service: Box<Service>,
}

impl Resources {
    pub fn new(config: &super::Args, mz: &Materialize, console_image_ref: &str) -> Self {
        let console_deployment = Box::new(create_console_deployment_object(
            config,
            mz,
            console_image_ref,
        ));
        let console_service = Box::new(create_console_service_object(config, mz));
        Self {
            console_deployment,
            console_service,
        }
    }

    pub async fn apply(&self, client: &Client, namespace: &str) -> Result<(), anyhow::Error> {
        let deployment_api: Api<Deployment> = Api::namespaced(client.clone(), namespace);
        let service_api: Api<Service> = Api::namespaced(client.clone(), namespace);

        apply_resource(&deployment_api, &self.console_deployment).await?;
        apply_resource(&service_api, &self.console_service).await?;

        Ok(())
    }
}

fn create_console_deployment_object(
    config: &super::Args,
    mz: &Materialize,
    console_image_ref: &str,
) -> Deployment {
    let mut pod_template_labels = mz.default_labels();
    pod_template_labels.insert(
        "materialize.cloud/name".to_owned(),
        mz.console_deployment_name(),
    );
    pod_template_labels.insert("app".to_owned(), "console".to_string());
    pod_template_labels.insert(
        "materialize.cloud/app".to_owned(),
        mz.console_service_name(),
    );

    let ports = vec![ContainerPort {
        container_port: CONSOLE_IMAGE_HTTP_PORT,
        name: Some("http".into()),
        protocol: Some("TCP".into()),
        ..Default::default()
    }];

    let probe = Probe {
        http_get: Some(HTTPGetAction {
            path: Some("/".to_string()),
            port: IntOrString::Int(CONSOLE_IMAGE_HTTP_PORT),
            ..Default::default()
        }),
        ..Default::default()
    };

    let env = vec![EnvVar {
        name: "MZ_ENDPOINT".to_string(),
        value: Some(format!(
            "http://{}.{}.svc.cluster.local:{}",
            // TODO: this should talk to balancerd eventually, but for now we
            // need to bypass auth which requires using the internal port
            mz.environmentd_service_name(),
            mz.namespace(),
            config.environmentd_internal_http_port,
        )),
        ..Default::default()
    }];

    if config.enable_tls {
        unimplemented!();
    } else {
        // currently the docker image just doesn't implement tls
    }

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
                scheduler_name: config.scheduler_name.clone(),
                service_account_name: Some(mz.service_account_name()),
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

fn create_console_service_object(config: &super::Args, mz: &Materialize) -> Service {
    let selector = btreemap! {"materialize.cloud/name".to_string() => mz.console_deployment_name()};

    let ports = vec![ServicePort {
        name: Some("http".to_string()),
        protocol: Some("TCP".to_string()),
        port: config.console_http_port,
        target_port: Some(IntOrString::Int(CONSOLE_IMAGE_HTTP_PORT)),
        ..Default::default()
    }];

    let spec = if config.local_development {
        ServiceSpec {
            type_: Some("NodePort".to_string()),
            selector: Some(selector),
            ports: Some(ports),
            external_traffic_policy: Some("Local".to_string()),
            ..Default::default()
        }
    } else {
        ServiceSpec {
            type_: Some("ClusterIP".to_string()),
            cluster_ip: Some("None".to_string()),
            selector: Some(selector),
            ports: Some(ports),
            ..Default::default()
        }
    };

    Service {
        metadata: mz.managed_resource_meta(mz.console_service_name()),
        spec: Some(spec),
        status: None,
    }
}

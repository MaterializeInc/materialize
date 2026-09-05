// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License in the LICENSE file at the
// root of this repository, or online at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Discovery of the processes a Materialize instance runs, via the kube API.
//!
//! environmentd is reached through its stable service, which the operator
//! points at the active generation's pod and which is the only name the
//! internal TLS certificate covers. clusterd processes are reached by pod IP:
//! a replica with `scale > 1` is one service fronting several pods, each of
//! which must be scraped individually, and clusterd's internal HTTP listener
//! has neither TLS nor authentication, so the pod IP is fine.

use std::collections::BTreeMap;

use anyhow::{Context, Result};
use k8s_openapi::api::apps::v1::StatefulSet;
use k8s_openapi::api::core::v1::{Pod, Service, ServicePort};
use kube::api::ListParams;
use kube::{Api, Client};
use tracing::warn;

const RESOURCE_ID_LABEL: &str = "materialize.cloud/mz-resource-id";
const ORGANIZATION_NAME_LABEL: &str = "materialize.cloud/organization-name";

#[derive(Debug, Clone)]
pub struct ServiceInfo {
    pub service_name: String,
    pub service_ports: Vec<ServicePort>,
    pub namespace: String,
    /// The service's pod selector, used to enumerate the pods it fronts. A
    /// scaled replica's service selects more than one pod.
    pub selector: BTreeMap<String, String>,
}

impl ServiceInfo {
    /// The in-cluster DNS name of the service.
    pub fn fqdn(&self) -> String {
        format!("{}.{}.svc.cluster.local", self.service_name, self.namespace)
    }
}

/// A pod fronted by a service, with the address to reach it at directly.
#[derive(Debug, Clone)]
pub struct PodInfo {
    pub name: String,
    /// Unset while the pod has not been scheduled or has no IP assigned yet.
    pub ip: Option<String>,
}

/// Returns the environmentd service of the instance.
///
/// The operator creates a stable service and one per generation, all carrying
/// the instance labels. The stable one is preferred: it follows the active
/// leader across rollouts and is the name the internal certificate is issued
/// for. It is recognised by its name, `mz<resource-id>-environmentd`, derived
/// from the resource id label the services carry.
pub async fn find_environmentd_service(
    client: &Client,
    k8s_namespace: &String,
    mz_instance_name: &String,
) -> Result<ServiceInfo> {
    let services_api: Api<Service> = Api::namespaced(client.clone(), k8s_namespace);

    let label_filter = format!("{RESOURCE_ID_LABEL},{ORGANIZATION_NAME_LABEL}={mz_instance_name}");

    let services = services_api
        .list(&ListParams::default().labels(&label_filter))
        .await
        .with_context(|| format!("Failed to list services in namespace {}", k8s_namespace))?;

    let mut candidates: Vec<(bool, ServiceInfo)> = services
        .iter()
        .filter_map(|service| {
            let service_name = service.metadata.name.clone()?;
            let spec = service.spec.as_ref()?;
            if !service_name.to_lowercase().contains("environmentd") {
                return None;
            }
            let ports = spec.ports.clone()?;
            let is_stable = service
                .metadata
                .labels
                .as_ref()
                .and_then(|labels| labels.get(RESOURCE_ID_LABEL))
                .is_some_and(|resource_id| service_name == format!("mz{resource_id}-environmentd"));
            Some((
                is_stable,
                ServiceInfo {
                    service_name,
                    service_ports: ports,
                    namespace: k8s_namespace.clone(),
                    selector: spec.selector.clone().unwrap_or_default(),
                },
            ))
        })
        .collect();

    // Stable service first, then by name so the choice is deterministic.
    candidates.sort_by(|(a_stable, a), (b_stable, b)| {
        b_stable
            .cmp(a_stable)
            .then_with(|| a.service_name.cmp(&b.service_name))
    });

    candidates
        .into_iter()
        .next()
        .map(|(_, service)| service)
        .ok_or_else(|| anyhow::anyhow!("Could not find environmentd service"))
}

/// Returns the clusterd services owned by the instance's environmentd.
pub async fn find_cluster_services(
    client: &Client,
    k8s_namespace: &String,
    mz_instance_name: &String,
) -> Result<Vec<ServiceInfo>> {
    let services: Api<Service> = Api::namespaced(client.clone(), k8s_namespace);
    let services = services
        .list(&ListParams::default())
        .await
        .with_context(|| format!("Failed to list services in namespace {}", k8s_namespace))?;

    let statefulsets_api: Api<StatefulSet> = Api::namespaced(client.clone(), k8s_namespace);

    let organization_name_filter = format!("{ORGANIZATION_NAME_LABEL}={mz_instance_name}");

    let statefulsets = statefulsets_api
        .list(&ListParams::default().labels(&organization_name_filter))
        .await
        .with_context(|| format!("Failed to list statefulsets in namespace {}", k8s_namespace))?;

    let cluster_services: Vec<ServiceInfo> = services
        .iter()
        .filter_map(|service| {
            let name = service.metadata.name.clone()?;
            let spec = service.spec.clone()?;
            let selector = spec.selector?;
            let ports = spec.ports?;

            // Check if this is a cluster service
            if selector.get("environmentd.materialize.cloud/namespace")? != "cluster" {
                return None;
            }

            // Check if the owner reference points to environmentd StatefulSet in the same mz instance
            let envd_statefulset_reference_name = service
                .metadata
                .owner_references
                .as_ref()?
                .iter()
                //  There should only be one StatefulSet reference to environmentd
                .find(|owner_reference| owner_reference.kind == "StatefulSet")?
                .name
                .clone();

            if !statefulsets
                .iter()
                .filter_map(|statefulset| statefulset.metadata.name.clone())
                .any(|name| name == envd_statefulset_reference_name)
            {
                return None;
            }

            Some(ServiceInfo {
                service_name: name,
                service_ports: ports,
                namespace: k8s_namespace.clone(),
                selector,
            })
        })
        .collect();

    if !cluster_services.is_empty() {
        return Ok(cluster_services);
    }

    Err(anyhow::anyhow!("Could not find cluster services"))
}

/// Lists the pods a service fronts, matched by its `selector`, sorted by name
/// so output is deterministic across runs.
pub async fn find_service_pods(
    client: &Client,
    k8s_namespace: &str,
    selector: &BTreeMap<String, String>,
) -> Result<Vec<PodInfo>> {
    // An empty selector would match every pod in the namespace, which is never
    // what a caller means. Treat it as "no pods".
    if selector.is_empty() {
        return Ok(Vec::new());
    }

    let label_filter = selector
        .iter()
        .map(|(k, v)| format!("{k}={v}"))
        .collect::<Vec<_>>()
        .join(",");

    let pods_api: Api<Pod> = Api::namespaced(client.clone(), k8s_namespace);
    let pods = pods_api
        .list(&ListParams::default().labels(&label_filter))
        .await
        .with_context(|| format!("Failed to list pods in namespace {}", k8s_namespace))?;

    let mut pod_infos: Vec<PodInfo> = pods
        .iter()
        .filter_map(|pod| {
            Some(PodInfo {
                name: pod.metadata.name.clone()?,
                ip: pod.status.as_ref().and_then(|status| status.pod_ip.clone()),
            })
        })
        .collect();
    pod_infos.sort_by(|a, b| a.name.cmp(&b.name));
    Ok(pod_infos)
}

/// Which binary a target runs, which decides the endpoint paths and which
/// listener serves them.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ServiceType {
    Clusterd,
    Environmentd,
}

/// One process to scrape over HTTP, reachable directly at `host`.
#[derive(Debug, Clone)]
pub struct HttpTarget {
    /// Names the output files, so it is the pod name wherever one is known.
    pub name: String,
    /// Host part of the authority: the service FQDN for environmentd, the pod
    /// IP for clusterd.
    pub host: String,
    /// Ports are taken from the fronting service. We assume the service port
    /// equals the pod's container port for these HTTP endpoints, which holds
    /// for clusterd and environmentd.
    pub service_ports: Vec<ServicePort>,
    pub service_type: ServiceType,
}

/// Everything the collector reaches for one snapshot.
#[derive(Debug, Clone)]
pub struct Targets {
    pub environmentd: ServiceInfo,
    pub http_targets: Vec<HttpTarget>,
}

/// Discovers the instance's environmentd service and every clusterd pod.
///
/// Fails only when environmentd cannot be found, since nothing else can be
/// collected without it. Missing cluster services and unreachable pods are
/// logged and skipped so a partially healthy instance still yields data.
pub async fn discover(
    client: &Client,
    k8s_namespace: &String,
    mz_instance_name: &String,
) -> Result<Targets> {
    let environmentd = find_environmentd_service(client, k8s_namespace, mz_instance_name)
        .await
        .context("Failed to find environmentd service")?;

    let mut http_targets = Vec::new();

    // The stable service fronts exactly the active leader, so a single pod is
    // expected. The output is named after that pod when there is one, to match
    // the pod logs; otherwise after the service.
    let environmentd_pods = find_service_pods(client, k8s_namespace, &environmentd.selector)
        .await
        .unwrap_or_else(|e| {
            warn!("Failed to list environmentd pods: {:#}", e);
            Vec::new()
        });
    let environmentd_name = match environmentd_pods.as_slice() {
        [pod] => pod.name.clone(),
        _ => environmentd.service_name.clone(),
    };
    http_targets.push(HttpTarget {
        name: environmentd_name,
        host: environmentd.fqdn(),
        service_ports: environmentd.service_ports.clone(),
        service_type: ServiceType::Environmentd,
    });

    let cluster_services =
        match find_cluster_services(client, k8s_namespace, mz_instance_name).await {
            Ok(services) => services,
            Err(e) => {
                warn!("Failed to find cluster services: {:#}", e);
                Vec::new()
            }
        };
    for service in &cluster_services {
        let pods = match find_service_pods(client, k8s_namespace, &service.selector).await {
            Ok(pods) => pods,
            Err(e) => {
                warn!(
                    "Failed to list pods for service {}: {:#}",
                    service.service_name, e
                );
                continue;
            }
        };
        if pods.is_empty() {
            warn!(
                "Found no pods for service {}, skipping",
                service.service_name
            );
        }
        for pod in pods {
            let Some(ip) = pod.ip else {
                warn!("Pod {} has no IP yet, skipping", pod.name);
                continue;
            };
            http_targets.push(HttpTarget {
                name: pod.name,
                host: ip,
                service_ports: service.service_ports.clone(),
                service_type: ServiceType::Clusterd,
            });
        }
    }

    Ok(Targets {
        environmentd,
        http_targets,
    })
}

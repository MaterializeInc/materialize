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

//! Dumps k8s resources to files.

use std::fmt::Debug;
use std::fs::{File, create_dir_all};
use std::future::Future;
use std::io::Write;
use std::path::PathBuf;
use std::pin::Pin;
use std::time::Duration;

use futures::future::join_all;
use k8s_openapi::NamespaceResourceScope;
use k8s_openapi::api::admissionregistration::v1::{
    MutatingWebhookConfiguration, ValidatingWebhookConfiguration,
};
use k8s_openapi::api::apps::v1::{DaemonSet, Deployment, ReplicaSet, StatefulSet};
use k8s_openapi::api::core::v1::{
    ConfigMap, Event, Node, PersistentVolume, PersistentVolumeClaim, Pod, Service, ServiceAccount,
};
use k8s_openapi::api::networking::v1::NetworkPolicy;
use k8s_openapi::api::rbac::v1::{Role, RoleBinding};
use k8s_openapi::api::storage::v1::StorageClass;
use k8s_openapi::apiextensions_apiserver::pkg::apis::apiextensions::v1::CustomResourceDefinition;
use k8s_openapi::jiff::Timestamp;
use kube::api::{ListParams, LogParams};
use kube::{Api, Client};
use mz_cloud_resources::crd::generated::cert_manager::certificates::Certificate;
use mz_cloud_resources::crd::materialize::v1alpha1::Materialize;

use serde::{Serialize, de::DeserializeOwned};
use tracing::{info, warn};

use crate::describe::{DescribeResource, describe};
use crate::{ContainerDumper, DumpConfig};

struct K8sResourceDumper<'n, K> {
    config: &'n DumpConfig,
    api: Api<K>,
    namespace: Option<String>,
    resource_type: String,
    /// The instant relative ages in `describe.txt` are computed against.
    now: Timestamp,
}

impl<'n, K> K8sResourceDumper<'n, K>
where
    K: DescribeResource + Clone + Debug + Serialize + DeserializeOwned,
{
    fn cluster(config: &'n DumpConfig, client: Client, now: Timestamp) -> Self {
        Self {
            config,
            api: Api::<K>::all(client),
            namespace: None,
            resource_type: K::plural(&()).into_owned(),
            now,
        }
    }

    fn namespaced(config: &'n DumpConfig, client: Client, namespace: String, now: Timestamp) -> Self
    where
        K: kube::Resource<Scope = NamespaceResourceScope>,
    {
        Self {
            config,
            api: Api::<K>::namespaced(client, namespace.as_str()),
            namespace: Some(namespace),
            resource_type: K::plural(&()).into_owned(),
            now,
        }
    }

    /// Writes one YAML file per object plus a `describe.txt` covering all of
    /// them. `events` is the namespace's Event list, from which each object's
    /// events are joined; cluster-scoped kinds pass an empty slice.
    async fn _dump(&self, events: &[Event]) -> Result<(), anyhow::Error> {
        let object_list = self.api.list(&ListParams::default()).await?;

        if object_list.items.is_empty() {
            let mut err_msg = format!("No {} found", self.resource_type);
            if let Some(namespace) = &self.namespace {
                err_msg = format!("{} for namespace {}", err_msg, namespace);
            }
            warn!("{}", err_msg);
            return Ok(());
        }
        let file_path = format_resource_path(
            self.config.base_path.clone(),
            self.resource_type.as_str(),
            self.namespace.as_ref(),
        );
        create_dir_all(&file_path)?;

        let mut described = Vec::with_capacity(object_list.items.len());
        for (i, item) in object_list.items.iter().enumerate() {
            let file_name = file_path.join(format!(
                "{}.yaml",
                item.meta()
                    .name
                    .clone()
                    .unwrap_or_else(|| format!("unknown_{}", i))
            ));
            let mut file = File::create(&file_name)?;

            serde_yaml::to_writer(&mut file, &item)?;

            info!("Exported {}", file_name.display());

            described.push(describe(item, events, self.now));
        }

        let describe_file_name = file_path.join("describe.txt");
        let mut file = File::create(&describe_file_name)?;
        file.write_all(described.join("\n\n").as_bytes())?;
        info!("Exported {}", describe_file_name.display());

        Ok(())
    }

    async fn dump(&self, events: &[Event]) {
        if let Err(e) = self._dump(events).await {
            warn!("Failed to write k8s {}: {}", self.resource_type, e);
        }
    }
}

pub struct K8sDumper<'n> {
    config: &'n DumpConfig,
    /// The kubernetes client to use.
    client: Client,
    /// The k8s namespace to dump.
    k8s_namespace: String,
    /// A list of additional k8s namespaces to dump.
    k8s_additional_namespaces: Option<Vec<String>>,
    /// The instant every relative age in this dump is computed against, so
    /// objects dumped seconds apart read consistently.
    now: Timestamp,
    /// When set, only pod log lines from this long before now are dumped.
    logs_since: Option<Duration>,
}

impl<'n> K8sDumper<'n> {
    pub fn new(
        config: &'n DumpConfig,
        client: Client,
        k8s_namespace: String,
        k8s_additional_namespaces: Option<Vec<String>>,
    ) -> Self {
        Self {
            config,
            client,
            k8s_namespace,
            k8s_additional_namespaces,
            now: Timestamp::now(),
            logs_since: None,
        }
    }

    /// Bounds the pod logs to the trailing `window`. A collector taking
    /// snapshots on an interval uses this so each snapshot carries only the
    /// logs since the previous one, rather than the full logs every time.
    pub fn with_logs_since(mut self, window: Duration) -> Self {
        self.logs_since = Some(window);
        self
    }

    /// Write cluster-level k8s resources to a yaml file per resource.
    ///
    /// Events for cluster-scoped objects live in namespaces the collector is
    /// not granted, so their describe output carries no Events section.
    async fn dump_cluster_resources(&self) {
        let no_events: &[Event] = &[];
        macro_rules! dump_cluster {
            ($($kind:ty),* $(,)?) => {
                $(
                    K8sResourceDumper::<$kind>::cluster(self.config, self.client.clone(), self.now)
                        .dump(no_events)
                        .await;
                )*
            };
        }
        dump_cluster!(
            Node,
            StorageClass,
            PersistentVolume,
            MutatingWebhookConfiguration,
            ValidatingWebhookConfiguration,
            CustomResourceDefinition,
        );
    }

    async fn _dump_k8s_pod_logs(&self, namespace: &String) -> Result<(), anyhow::Error> {
        let file_path =
            format_resource_path(self.config.base_path.clone(), "logs", Some(namespace));
        create_dir_all(&file_path)?;

        let pods: Api<Pod> = Api::<Pod>::namespaced(self.client.clone(), namespace);
        let pod_list = pods.list(&ListParams::default()).await?;

        for (i, pod) in pod_list.items.iter().enumerate() {
            let pod_name = pod
                .metadata
                .name
                .clone()
                .unwrap_or_else(|| format!("unknown_{}", i));
            async fn export_pod_logs(
                pods: &Api<Pod>,
                pod_name: &str,
                file_path: &PathBuf,
                is_previous: bool,
                since_seconds: Option<i64>,
            ) -> Result<(), anyhow::Error> {
                let suffix = if is_previous { "previous" } else { "current" };
                let file_name = file_path.join(format!("{}.{}.log", pod_name, suffix));

                let logs = pods
                    .logs(
                        pod_name,
                        &LogParams {
                            previous: is_previous,
                            timestamps: true,
                            since_seconds,
                            ..Default::default()
                        },
                    )
                    .await?;

                if logs.is_empty() {
                    warn!("No {} logs found for pod {}", suffix, pod_name);
                    return Ok(());
                }

                let mut file = File::create(&file_name)?;
                file.write_all(logs.as_bytes())?;
                info!("Exported {}", file_name.display());

                Ok(())
            }

            let since_seconds = self
                .logs_since
                .map(|window| i64::try_from(window.as_secs()).unwrap_or(i64::MAX));
            if let Err(e) = export_pod_logs(&pods, &pod_name, &file_path, true, since_seconds).await
            {
                match e.downcast_ref::<kube::Error>() {
                    Some(kube::Error::Api(e)) if e.code == 400 => {
                        warn!("No previous logs available for pod {}", pod_name);
                    }
                    _ => {
                        warn!(
                            "Failed to export previous logs for pod {}: {}",
                            &pod_name, e
                        );
                    }
                }
            }

            if let Err(e) =
                export_pod_logs(&pods, &pod_name, &file_path, false, since_seconds).await
            {
                warn!("Failed to export current logs for pod {}: {}", &pod_name, e);
            }
        }
        Ok(())
    }

    /// Write k8s pod logs to a yaml file per pod.
    async fn dump_k8s_pod_logs(&self, namespace: &String) {
        if let Err(e) = self._dump_k8s_pod_logs(namespace).await {
            warn!("Failed to dump k8s pod logs: {}", e);
        }
    }

    /// Write namespace-level k8s resources to a yaml file per resource.
    pub async fn dump_namespaced_resources(&self, namespace: String) {
        // Fetched once up front so every kind's describe output can join its
        // events without another round trip per kind.
        let events = match Api::<Event>::namespaced(self.client.clone(), &namespace)
            .list(&ListParams::default())
            .await
        {
            Ok(list) => list.items,
            Err(e) => {
                warn!(
                    "Failed to list events in namespace {}, describe output will have none: {}",
                    namespace, e
                );
                Vec::new()
            }
        };
        let events = events.as_slice();

        macro_rules! dump_namespaced {
            ($($kind:ty),* $(,)?) => {
                $(
                    K8sResourceDumper::<$kind>::namespaced(
                        self.config,
                        self.client.clone(),
                        namespace.clone(),
                        self.now,
                    )
                    .dump(events)
                    .await;
                )*
            };
        }
        dump_namespaced!(
            Pod,
            Service,
            Deployment,
            StatefulSet,
            ReplicaSet,
            DaemonSet,
            NetworkPolicy,
            Event,
            Materialize,
            Role,
            RoleBinding,
            ConfigMap,
            PersistentVolumeClaim,
            ServiceAccount,
            Certificate,
        );

        self.dump_k8s_pod_logs(&namespace).await;
    }
}

impl<'n> ContainerDumper for K8sDumper<'n> {
    async fn dump_container_resources(&self) {
        let mut futs: Vec<Pin<Box<dyn Future<Output = ()>>>> = vec![];

        let k8s_namespaces_iter = std::iter::once(&self.k8s_namespace)
            .chain(self.k8s_additional_namespaces.iter().flatten());

        for namespace in k8s_namespaces_iter {
            futs.push(Box::pin(self.dump_namespaced_resources(namespace.clone())));
        }
        futs.push(Box::pin(self.dump_cluster_resources()));

        join_all(futs).await;
    }
}

fn format_resource_path(
    base_path: PathBuf,
    resource_type: &str,
    namespace: Option<&String>,
) -> PathBuf {
    let mut path = base_path.join(resource_type);

    if let Some(namespace) = namespace {
        path = path.join(namespace);
    }
    path
}

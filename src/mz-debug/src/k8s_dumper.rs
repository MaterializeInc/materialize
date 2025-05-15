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

use futures::future::join_all;
use k8s_openapi::NamespaceResourceScope;
use k8s_openapi::api::admissionregistration::v1::{
    MutatingWebhookConfiguration, ValidatingWebhookConfiguration,
};
use k8s_openapi::api::apps::v1::{DaemonSet, Deployment, ReplicaSet, StatefulSet};
use k8s_openapi::api::core::v1::{
    ConfigMap, Event, Node, PersistentVolume, PersistentVolumeClaim, Pod, Secret, Service,
    ServiceAccount,
};
use k8s_openapi::api::networking::v1::NetworkPolicy;
use k8s_openapi::api::rbac::v1::{Role, RoleBinding};
use k8s_openapi::api::storage::v1::StorageClass;
use k8s_openapi::apiextensions_apiserver::pkg::apis::apiextensions::v1::CustomResourceDefinition;
use kube::api::{ListParams, LogParams};
use kube::{Api, Client};
use mz_cloud_resources::crd::generated::cert_manager::certificates::Certificate;
use mz_cloud_resources::crd::materialize::v1alpha1::Materialize;

use serde::{Serialize, de::DeserializeOwned};
use tracing::{info, warn};

use crate::{ContainerDumper, Context};

struct K8sResourceDumper<'n, K> {
    context: &'n Context,
    api: Api<K>,
    namespace: Option<String>,
    resource_type: String,
    dump_secret_values: bool,
}

impl<'n, K> K8sResourceDumper<'n, K>
where
    K: kube::Resource<DynamicType = ()> + Clone + Debug + Serialize + DeserializeOwned,
{
    fn cluster(context: &'n Context, client: Client, dump_secret_values: bool) -> Self {
        Self {
            context,
            api: Api::<K>::all(client),
            namespace: None,
            resource_type: K::plural(&()).into_owned(),
            dump_secret_values,
        }
    }

    fn namespaced(
        context: &'n Context,
        client: Client,
        namespace: String,
        dump_secret_values: bool,
    ) -> Self
    where
        K: kube::Resource<Scope = NamespaceResourceScope>,
    {
        Self {
            context,
            api: Api::<K>::namespaced(client, namespace.as_str()),
            namespace: Some(namespace),
            resource_type: K::plural(&()).into_owned(),
            dump_secret_values,
        }
    }

    async fn _dump(&self) -> Result<(), anyhow::Error> {
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
            self.context.base_path.clone(),
            self.resource_type.as_str(),
            self.namespace.as_ref(),
        );
        create_dir_all(&file_path)?;

        for (i, item) in object_list.items.iter().enumerate() {
            let file_name = file_path.join(format!(
                "{}.yaml",
                &item
                    .meta()
                    .name
                    .clone()
                    .unwrap_or_else(|| format!("unknown_{}", i))
            ));
            let mut file = File::create(&file_name)?;

            // If the resource is a secret, we hide its values by default.
            if self.resource_type == "secrets" && !self.dump_secret_values {
                serde_yaml::to_writer(&mut file, item.meta())?;
            } else {
                serde_yaml::to_writer(&mut file, &item)?;
            }

            info!("Exported {}", file_name.display());
        }

        Ok(())
    }

    async fn dump(&self) {
        if let Err(e) = self._dump().await {
            warn!("Failed to write k8s {}: {}", self.resource_type, e);
        }
    }
}

pub struct K8sDumper<'n> {
    context: &'n Context,
    /// The kubernetes client to use.
    client: Client,
    /// A list of namespaces to dump.
    k8s_namespaces: Vec<String>,
    /// The kubernetes context to use.
    k8s_context: Option<String>,
    /// If true, the tool will dump the values of secrets in the Kubernetes cluster.
    k8s_dump_secret_values: bool,
}

impl<'n> K8sDumper<'n> {
    pub fn new(
        context: &'n Context,
        client: Client,
        k8s_namespaces: Vec<String>,
        k8s_context: Option<String>,
        k8s_dump_secret_values: bool,
    ) -> Self {
        Self {
            context,
            client,
            k8s_namespaces,
            k8s_context,
            k8s_dump_secret_values,
        }
    }

    async fn _dump_kubectl_describe<K>(
        &self,
        namespace: Option<&String>,
    ) -> Result<(), anyhow::Error>
    where
        K: kube::Resource<DynamicType = ()>,
    {
        let resource_type = K::plural(&()).into_owned();
        let mut args = vec!["describe", &resource_type];
        if let Some(namespace) = namespace {
            args.extend(["-n", namespace]);
        } else {
            args.push("--all-namespaces");
        }

        if let Some(k8s_context) = &self.k8s_context {
            args.extend(["--context", k8s_context]);
        }

        let output = tokio::process::Command::new("kubectl")
            .args(args)
            .stderr(std::process::Stdio::null()) // Silence stderr
            .output()
            .await?;

        if !output.status.success() {
            return Err(anyhow::anyhow!(
                "{}",
                String::from_utf8_lossy(&output.stderr)
            ));
        }

        if output.stdout.is_empty() {
            let mut err_msg = format!("Describe: No {} found", resource_type);
            if let Some(namespace) = namespace {
                err_msg = format!("{} for namespace {}", err_msg, namespace);
            }
            warn!("{}", err_msg);
            return Ok(());
        }

        let file_path = format_resource_path(
            self.context.base_path.clone(),
            resource_type.as_str(),
            namespace,
        );
        let file_name = file_path.join("describe.txt");
        create_dir_all(&file_path)?;
        let mut file = File::create(&file_name)?;
        file.write_all(&output.stdout)?;

        info!("Exported {}", file_name.display());

        Ok(())
    }

    async fn dump_kubectl_describe<K>(&self, namespace: Option<&String>)
    where
        K: kube::Resource<DynamicType = ()>,
    {
        if let Err(e) = self._dump_kubectl_describe::<K>(namespace).await {
            warn!(
                "Failed to dump kubectl describe for {}: {}",
                K::plural(&()).into_owned(),
                e
            );
        }
    }

    /// Write cluster-level k8s resources to a yaml file per resource.
    async fn dump_cluster_resources(&self) {
        K8sResourceDumper::<Node>::cluster(
            self.context,
            self.client.clone(),
            self.k8s_dump_secret_values,
        )
        .dump()
        .await;

        K8sResourceDumper::<StorageClass>::cluster(
            self.context,
            self.client.clone(),
            self.k8s_dump_secret_values,
        )
        .dump()
        .await;

        K8sResourceDumper::<PersistentVolume>::cluster(
            self.context,
            self.client.clone(),
            self.k8s_dump_secret_values,
        )
        .dump()
        .await;

        K8sResourceDumper::<MutatingWebhookConfiguration>::cluster(
            self.context,
            self.client.clone(),
            self.k8s_dump_secret_values,
        )
        .dump()
        .await;

        K8sResourceDumper::<ValidatingWebhookConfiguration>::cluster(
            self.context,
            self.client.clone(),
            self.k8s_dump_secret_values,
        )
        .dump()
        .await;
        K8sResourceDumper::<DaemonSet>::cluster(
            self.context,
            self.client.clone(),
            self.k8s_dump_secret_values,
        )
        .dump()
        .await;
        K8sResourceDumper::<CustomResourceDefinition>::cluster(
            self.context,
            self.client.clone(),
            self.k8s_dump_secret_values,
        )
        .dump()
        .await;
    }

    async fn _dump_k8s_pod_logs(&self, namespace: &String) -> Result<(), anyhow::Error> {
        let file_path =
            format_resource_path(self.context.base_path.clone(), "logs", Some(namespace));
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
            ) -> Result<(), anyhow::Error> {
                let suffix = if is_previous { "previous" } else { "current" };
                let file_name = file_path.join(format!("{}.{}.log", pod_name, suffix));

                let logs = pods
                    .logs(
                        pod_name,
                        &LogParams {
                            previous: is_previous,
                            timestamps: true,
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

            if let Err(e) = export_pod_logs(&pods, &pod_name, &file_path, true).await {
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

            if let Err(e) = export_pod_logs(&pods, &pod_name, &file_path, false).await {
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
        K8sResourceDumper::<Pod>::namespaced(
            self.context,
            self.client.clone(),
            namespace.clone(),
            self.k8s_dump_secret_values,
        )
        .dump()
        .await;
        K8sResourceDumper::<Service>::namespaced(
            self.context,
            self.client.clone(),
            namespace.clone(),
            self.k8s_dump_secret_values,
        )
        .dump()
        .await;
        K8sResourceDumper::<Deployment>::namespaced(
            self.context,
            self.client.clone(),
            namespace.clone(),
            self.k8s_dump_secret_values,
        )
        .dump()
        .await;
        K8sResourceDumper::<StatefulSet>::namespaced(
            self.context,
            self.client.clone(),
            namespace.clone(),
            self.k8s_dump_secret_values,
        )
        .dump()
        .await;
        K8sResourceDumper::<ReplicaSet>::namespaced(
            self.context,
            self.client.clone(),
            namespace.clone(),
            self.k8s_dump_secret_values,
        )
        .dump()
        .await;
        K8sResourceDumper::<NetworkPolicy>::namespaced(
            self.context,
            self.client.clone(),
            namespace.clone(),
            self.k8s_dump_secret_values,
        )
        .dump()
        .await;
        K8sResourceDumper::<Event>::namespaced(
            self.context,
            self.client.clone(),
            namespace.clone(),
            self.k8s_dump_secret_values,
        )
        .dump()
        .await;
        K8sResourceDumper::<Materialize>::namespaced(
            self.context,
            self.client.clone(),
            namespace.clone(),
            self.k8s_dump_secret_values,
        )
        .dump()
        .await;
        K8sResourceDumper::<Role>::namespaced(
            self.context,
            self.client.clone(),
            namespace.clone(),
            self.k8s_dump_secret_values,
        )
        .dump()
        .await;
        K8sResourceDumper::<RoleBinding>::namespaced(
            self.context,
            self.client.clone(),
            namespace.clone(),
            self.k8s_dump_secret_values,
        )
        .dump()
        .await;
        K8sResourceDumper::<ConfigMap>::namespaced(
            self.context,
            self.client.clone(),
            namespace.clone(),
            self.k8s_dump_secret_values,
        )
        .dump()
        .await;
        K8sResourceDumper::<Secret>::namespaced(
            self.context,
            self.client.clone(),
            namespace.clone(),
            self.k8s_dump_secret_values,
        )
        .dump()
        .await;
        K8sResourceDumper::<PersistentVolumeClaim>::namespaced(
            self.context,
            self.client.clone(),
            namespace.clone(),
            self.k8s_dump_secret_values,
        )
        .dump()
        .await;
        K8sResourceDumper::<ServiceAccount>::namespaced(
            self.context,
            self.client.clone(),
            namespace.clone(),
            self.k8s_dump_secret_values,
        )
        .dump()
        .await;

        K8sResourceDumper::<Certificate>::namespaced(
            self.context,
            self.client.clone(),
            namespace.clone(),
            self.k8s_dump_secret_values,
        )
        .dump()
        .await;

        self.dump_k8s_pod_logs(&namespace).await;
    }
}

impl<'n> ContainerDumper for K8sDumper<'n> {
    async fn dump_container_resources(&self) {
        let mut futs: Vec<Pin<Box<dyn Future<Output = ()>>>> = vec![];

        for namespace in &self.k8s_namespaces {
            futs.push(Box::pin(self.dump_kubectl_describe::<Pod>(Some(namespace))));
            futs.push(Box::pin(
                self.dump_kubectl_describe::<Service>(Some(namespace)),
            ));
            futs.push(Box::pin(
                self.dump_kubectl_describe::<Deployment>(Some(namespace)),
            ));
            futs.push(Box::pin(
                self.dump_kubectl_describe::<StatefulSet>(Some(namespace)),
            ));
            futs.push(Box::pin(
                self.dump_kubectl_describe::<ReplicaSet>(Some(namespace)),
            ));
            futs.push(Box::pin(
                self.dump_kubectl_describe::<NetworkPolicy>(Some(namespace)),
            ));
            futs.push(Box::pin(
                self.dump_kubectl_describe::<Event>(Some(namespace)),
            ));
            futs.push(Box::pin(
                self.dump_kubectl_describe::<Materialize>(Some(namespace)),
            ));
            futs.push(Box::pin(
                self.dump_kubectl_describe::<Role>(Some(namespace)),
            ));
            futs.push(Box::pin(
                self.dump_kubectl_describe::<RoleBinding>(Some(namespace)),
            ));
            futs.push(Box::pin(
                self.dump_kubectl_describe::<ConfigMap>(Some(namespace)),
            ));
            futs.push(Box::pin(
                self.dump_kubectl_describe::<Secret>(Some(namespace)),
            ));
            futs.push(Box::pin(
                self.dump_kubectl_describe::<PersistentVolumeClaim>(Some(namespace)),
            ));
            futs.push(Box::pin(
                self.dump_kubectl_describe::<ServiceAccount>(Some(namespace)),
            ));
            futs.push(Box::pin(
                self.dump_kubectl_describe::<Certificate>(Some(namespace)),
            ));
        }

        futs.push(Box::pin(self.dump_kubectl_describe::<Node>(None)));
        futs.push(Box::pin(self.dump_kubectl_describe::<DaemonSet>(None)));
        futs.push(Box::pin(self.dump_kubectl_describe::<StorageClass>(None)));
        futs.push(Box::pin(
            self.dump_kubectl_describe::<PersistentVolume>(None),
        ));
        futs.push(Box::pin(
            self.dump_kubectl_describe::<MutatingWebhookConfiguration>(None),
        ));
        futs.push(Box::pin(
            self.dump_kubectl_describe::<ValidatingWebhookConfiguration>(None),
        ));
        futs.push(Box::pin(
            self.dump_kubectl_describe::<CustomResourceDefinition>(None),
        ));

        for namespace in self.k8s_namespaces.clone() {
            futs.push(Box::pin(self.dump_namespaced_resources(namespace)));
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

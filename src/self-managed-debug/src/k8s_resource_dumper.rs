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
use std::fs::{create_dir_all, File};
use std::io::Write;
use std::path::{PathBuf, MAIN_SEPARATOR};

use chrono::{DateTime, Utc};
use k8s_openapi::api::apps::v1::{Deployment, ReplicaSet, StatefulSet};
use k8s_openapi::api::core::v1::{Event, Node, Pod, Service};
use k8s_openapi::api::networking::v1::NetworkPolicy;
use k8s_openapi::{ListableResource, NamespaceResourceScope};
use kube::api::{ListParams, LogParams};
use kube::{Api, Client};
use mz_ore::task::JoinHandle;
use serde::{de::DeserializeOwned, Serialize};

use crate::Context;

pub struct K8sResourceDumper<'n, K> {
    context: &'n Context,
    api: Api<K>,
    namespace: Option<String>,
}

impl<'n, K> K8sResourceDumper<'n, K>
where
    K: kube::Resource<DynamicType = ()>
        + ListableResource
        + Clone
        + Debug
        + Serialize
        + DeserializeOwned,
{
    fn cluster(context: &'n Context, client: Client) -> Self {
        Self {
            context,
            api: Api::<K>::all(client),
            namespace: None,
        }
    }

    fn namespaced(context: &'n Context, client: Client, namespace: String) -> Self
    where
        K: kube::Resource<Scope = NamespaceResourceScope>,
    {
        Self {
            context,
            api: Api::<K>::namespaced(client, namespace.as_str()),
            namespace: Some(namespace),
        }
    }

    async fn _dump(&self) -> Result<(), anyhow::Error> {
        let object_list = self.api.list(&ListParams::default()).await?;
        let resource_type = K::URL_PATH_SEGMENT;

        if object_list.items.is_empty() {
            let mut err_msg = format!("No {} found", resource_type);
            if let Some(namespace) = &self.namespace {
                err_msg = format!("{} for namespace {}", err_msg, namespace);
            }
            println!("{}", err_msg);
            return Ok(());
        }
        let file_path = format_resource_path(
            self.context.start_time,
            resource_type,
            self.namespace.as_ref(),
        );
        create_dir_all(&file_path)?;

        for (i, item) in object_list.items.iter().enumerate() {
            let file_name = file_path.join(format!(
                "{}.yaml",
                &item.meta().name.clone().unwrap_or(format!("unknown_{}", i))
            ));
            let mut file = File::create(&file_name)?;
            serde_yaml::to_writer(&mut file, &item)?;
            println!("Exporting {}", file_name.display());
        }

        Ok(())
    }

    async fn dump(&self) {
        if let Err(e) = self._dump().await {
            eprintln!("Failed to write k8s {}: {}", K::URL_PATH_SEGMENT, e);
        }
    }
}

/// Write k8s pod logs to a yaml file per pod.
async fn _dump_k8s_pod_logs(
    context: &Context,
    client: Client,
    namespace: &String,
) -> Result<(), anyhow::Error> {
    let file_path = format_resource_path(context.start_time, "logs", Some(namespace));
    create_dir_all(&file_path)?;

    let pods: Api<Pod> = Api::<Pod>::namespaced(client.clone(), namespace);
    let pod_list = pods.list(&ListParams::default()).await?;

    for (i, pod) in pod_list.items.iter().enumerate() {
        let pod_name = pod
            .metadata
            .name
            .clone()
            .unwrap_or(format!("unknown_{}", i));
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
                eprintln!("No {} logs found for pod {}", suffix, pod_name);
                return Ok(());
            }

            let mut file = File::create(&file_name)?;
            file.write_all(logs.as_bytes())?;
            println!("Exported {}", file_name.display());

            Ok(())
        }

        if let Err(e) = export_pod_logs(&pods, &pod_name, &file_path, true).await {
            let print_error = || {
                eprintln!(
                    "Failed to export previous logs for pod {}: {}",
                    &pod_name, e
                );
            };

            if let Some(kube::Error::Api(e)) = e.downcast_ref::<kube::Error>() {
                if e.code == 400 {
                    eprintln!("No previous logs available for pod {}", pod_name);
                } else {
                    print_error();
                }
            } else {
                print_error();
            }
        }

        if let Err(e) = export_pod_logs(&pods, &pod_name, &file_path, false).await {
            eprintln!("Failed to export current logs for pod {}: {}", &pod_name, e);
        }
    }
    Ok(())
}

/// Write k8s pod logs to a yaml file per pod.
async fn dump_k8s_pod_logs(context: &Context, client: Client, namespace: &String) {
    if let Err(e) = _dump_k8s_pod_logs(context, client, namespace).await {
        eprintln!("Failed to dump k8s pod logs: {}", e);
    }
}

/// Write namespace-level k8s resources to a yaml file per resource.
pub async fn dump_namespaced_resources(context: &Context, client: &Client, namespace: String) {
    K8sResourceDumper::<Pod>::namespaced(context, client.clone(), namespace.clone())
        .dump()
        .await;
    K8sResourceDumper::<Service>::namespaced(context, client.clone(), namespace.clone())
        .dump()
        .await;
    K8sResourceDumper::<Deployment>::namespaced(context, client.clone(), namespace.clone())
        .dump()
        .await;
    K8sResourceDumper::<StatefulSet>::namespaced(context, client.clone(), namespace.clone())
        .dump()
        .await;
    K8sResourceDumper::<ReplicaSet>::namespaced(context, client.clone(), namespace.clone())
        .dump()
        .await;
    K8sResourceDumper::<NetworkPolicy>::namespaced(context, client.clone(), namespace.clone())
        .dump()
        .await;
    K8sResourceDumper::<Event>::namespaced(context, client.clone(), namespace.clone())
        .dump()
        .await;

    dump_k8s_pod_logs(context, client.clone(), &namespace).await;
}

/// Write cluster-level k8s resources to a yaml file per resource.
pub async fn dump_cluster_resources(context: &Context, client: &Client) {
    K8sResourceDumper::<Node>::cluster(context, client.clone())
        .dump()
        .await;
}

/// Runs `kubectl describe` for a given resource type K and writes the output to a file.
async fn dump_kubectl_describe<K>(
    context: &Context,
    namespace: Option<&String>,
) -> Result<(), anyhow::Error>
where
    K: ListableResource,
{
    let resource_type = K::URL_PATH_SEGMENT;
    let mut args = vec!["describe", &resource_type];
    if let Some(namespace) = namespace {
        args.extend(["-n", namespace]);
    }

    if let Some(k8s_context) = &context.args.k8s_context {
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
        eprintln!("{}", err_msg);
        return Ok(());
    }

    let file_path = format_resource_path(context.start_time, resource_type, namespace);
    let file_name = file_path.join("describe.txt");
    create_dir_all(&file_path)?;
    let mut file = File::create(&file_name)?;
    file.write_all(&output.stdout)?;

    println!("Exported {}", file_name.display());

    Ok(())
}

/// Spawns a new task to run `kubectl describe` for a given resource type K and writes the output to a file.
pub fn spawn_dump_kubectl_describe_process<K>(
    context: Context,
    namespace: Option<String>,
) -> JoinHandle<()>
where
    K: ListableResource,
{
    mz_ore::task::spawn(|| "dump-kubectl-describe", async move {
        if let Err(e) = dump_kubectl_describe::<K>(&context, namespace.as_ref()).await {
            eprintln!(
                "Failed to dump kubectl describe for {}: {}",
                K::URL_PATH_SEGMENT,
                e
            );
        }
    })
}

fn format_resource_path(
    date_time: DateTime<Utc>,
    resource_type: &str,
    namespace: Option<&String>,
) -> PathBuf {
    let mut path = PathBuf::from(format!(
        "mz-debug{}{}{}{}",
        MAIN_SEPARATOR,
        date_time.format("%Y-%m-%dT%H:%MZ"),
        MAIN_SEPARATOR,
        resource_type,
    ));
    if let Some(namespace) = namespace {
        path = path.join(namespace);
    }
    path
}

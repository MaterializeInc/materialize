// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Reconciles a `MaterializeDebug` into an in-cluster debug collector.

use std::sync::LazyLock;
use std::time::Duration;

use k8s_controller::TraceMetadata;
use k8s_openapi::{
    api::{
        apps::v1::{Deployment, DeploymentSpec, DeploymentStrategy},
        core::v1::{
            Capabilities, Container, ContainerPort, EmptyDirVolumeSource, EnvVar, EnvVarSource,
            HTTPGetAction, PodSecurityContext, PodSpec, PodTemplateSpec, Probe,
            ResourceRequirements, SeccompProfile, SecretKeySelector, SecurityContext, Service,
            ServiceAccount, ServicePort, ServiceSpec, Volume, VolumeMount,
        },
        rbac::v1::{ClusterRoleBinding, RoleBinding, RoleRef, Subject},
    },
    apimachinery::pkg::{
        apis::meta::v1::{Condition, LabelSelector, OwnerReference, Time},
        util::intstr::IntOrString,
    },
    jiff::Timestamp,
};
use kube::{
    Api, Client, Resource, ResourceExt,
    api::{ObjectMeta, Patch, PatchParams, PostParams},
    runtime::controller::Action,
};
use maplit::btreemap;
use semver::{BuildMetadata, Prerelease, Version};
use tracing::{trace, warn};

use crate::{
    Error,
    k8s::{apply_resource, delete_resource, get_resource, recommended_k8s_labels},
    matching_image_from_environmentd_image_ref,
};
use mz_cloud_resources::crd::{
    ManagedResource, materialize::v1alpha1::Materialize,
    materialize_debug::v1alpha1::MaterializeDebug,
};
use mz_orchestrator_kubernetes::KubernetesImagePullPolicy;
use mz_ore::instrument;
use mz_server_core::listeners::AuthenticatorKind;

/// The first release whose `mz-debug` image has the `collector` subcommand.
/// Instances on older versions get no collector, since the image the
/// operator would derive for them cannot run one.
pub static DEBUG_COLLECTOR_MIN_VERSION: LazyLock<Version> = LazyLock::new(|| Version {
    major: 26,
    minor: 40,
    patch: 0,
    pre: Prerelease::new("dev.0").expect("dev.0 is valid prerelease"),
    build: BuildMetadata::new("").expect("empty string is valid buildmetadata"),
});

/// The chart-provided ClusterRole with the collector's namespaced permissions,
/// granted per namespace through a RoleBinding.
pub const COLLECTOR_CLUSTER_ROLE: &str = "materialize-debug-collector";
/// The chart-provided ClusterRole with the collector's cluster-scoped
/// permissions, granted through a ClusterRoleBinding.
pub const COLLECTOR_CLUSTER_SCOPED_CLUSTER_ROLE: &str = "materialize-debug-collector-cluster";

/// The key in the instance's backend secret holding mz_system's password.
const MZ_SYSTEM_PASSWORD_SECRET_KEY: &str = "external_login_password_mz_system";
/// Where the collector keeps its snapshot buffer inside the pod.
const SNAPSHOT_DIR: &str = "/var/lib/mz-debug";

/// The Materialize a debug resource points at is not watched, so its drift
/// (image, authenticator kind, backend secret) is picked up on this cadence.
const RESYNC_INTERVAL: Duration = Duration::from_secs(300);
/// How soon to look again for a Materialize that does not exist yet or has
/// not been assigned a resource id.
const MISSING_MATERIALIZE_RETRY: Duration = Duration::from_secs(60);

#[derive(Clone)]
pub struct Config {
    pub enable_security_context: bool,
    pub image_pull_policy: KubernetesImagePullPolicy,
    pub scheduler_name: Option<String>,
    pub default_resources: Option<ResourceRequirements>,
    pub collector_http_port: u16,
}

pub struct Context {
    config: Config,
}

/// How the collector authenticates to the instance.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum AuthMode {
    None,
    Password,
}

impl Context {
    pub fn new(config: Config) -> Self {
        Self { config }
    }

    /// Sets the Ready condition, skipping the write when it already reads as
    /// requested so a reconcile triggered by our own status update does not
    /// trigger another.
    async fn set_ready_condition(
        &self,
        client: &Client,
        debug: &MaterializeDebug,
        ready: bool,
        reason: &str,
        message: String,
    ) -> Result<(), Error> {
        let status_str = if ready { "True" } else { "False" };
        let mut status = debug.status();
        if status.conditions.iter().any(|condition| {
            condition.type_ == "Ready"
                && condition.status == status_str
                && condition.reason == reason
                && condition.message == message
        }) {
            return Ok(());
        }
        status.conditions = vec![Condition {
            type_: "Ready".to_string(),
            status: status_str.to_string(),
            last_transition_time: Time(Timestamp::now()),
            message,
            observed_generation: debug.meta().generation,
            reason: reason.to_string(),
        }];
        let mut new_debug = debug.clone();
        new_debug.status = Some(status);
        Api::<MaterializeDebug>::namespaced(client.clone(), &debug.namespace())
            .replace_status(&debug.name_unchecked(), &PostParams::default(), &new_debug)
            .await?;
        Ok(())
    }

    async fn sync_deployment_status(
        &self,
        client: &Client,
        debug: &MaterializeDebug,
    ) -> Result<(), Error> {
        let deployment_api: Api<Deployment> = Api::namespaced(client.clone(), &debug.namespace());
        let Some(deployment) = get_resource(&deployment_api, &debug.deployment_name()).await?
        else {
            return Ok(());
        };
        let Some(conditions) = deployment
            .status
            .as_ref()
            .and_then(|status| status.conditions.as_ref())
        else {
            // The deployment has no conditions yet, so there is nothing to
            // report.
            return Ok(());
        };
        let ready = conditions
            .iter()
            .any(|condition| condition.type_ == "Available" && condition.status == "True");
        self.set_ready_condition(
            client,
            debug,
            ready,
            "DeploymentStatus",
            format!(
                "debug collector deployment is{} ready",
                if ready { "" } else { " not" }
            ),
        )
        .await
    }

    /// Makes the Materialize the owner of the debug resource, so deleting the
    /// instance takes its collector with it. Resources the operator creates
    /// already carry this; manually created ones acquire it here.
    async fn ensure_owner_reference(
        &self,
        client: &Client,
        debug: &MaterializeDebug,
        mz: &Materialize,
    ) -> Result<(), Error> {
        let Some(mz_uid) = mz.uid() else {
            return Ok(());
        };
        let mut owner_references = debug.owner_references().to_vec();
        if owner_references.iter().any(|owner| owner.uid == mz_uid) {
            return Ok(());
        }
        owner_references.push(OwnerReference {
            api_version: Materialize::api_version(&()).to_string(),
            kind: Materialize::kind(&()).to_string(),
            name: mz.name_unchecked(),
            uid: mz_uid,
            block_owner_deletion: Some(true),
            ..Default::default()
        });
        // A merge patch rather than server-side apply: the operator applies
        // the whole resource under its field manager when it creates debug
        // resources itself, and a second apply from that manager restricted
        // to the owner references would drop everything else it manages.
        Api::<MaterializeDebug>::namespaced(client.clone(), &debug.namespace())
            .patch_metadata(
                &debug.name_unchecked(),
                &PatchParams::default(),
                &Patch::Merge(serde_json::json!({
                    "metadata": { "ownerReferences": owner_references }
                })),
            )
            .await?;
        Ok(())
    }

    fn create_service_account_object(&self, debug: &MaterializeDebug) -> ServiceAccount {
        ServiceAccount {
            metadata: debug.managed_resource_meta(debug.service_account_name()),
            automount_service_account_token: Some(true),
            ..Default::default()
        }
    }

    /// The RoleBinding granting the collector's namespaced permissions in
    /// `namespace`. Only the one in the debug resource's own namespace can
    /// be owned by it; the others are cleaned up by the finalizer.
    fn create_role_binding_object(&self, debug: &MaterializeDebug, namespace: &str) -> RoleBinding {
        let metadata = if namespace == debug.namespace() {
            debug.managed_resource_meta(debug.role_binding_name(namespace))
        } else {
            ObjectMeta {
                namespace: Some(namespace.to_owned()),
                name: Some(debug.role_binding_name(namespace)),
                labels: Some(self.labels(debug)),
                ..Default::default()
            }
        };
        RoleBinding {
            metadata,
            role_ref: RoleRef {
                api_group: "rbac.authorization.k8s.io".to_string(),
                kind: "ClusterRole".to_string(),
                name: COLLECTOR_CLUSTER_ROLE.to_string(),
            },
            subjects: Some(vec![self.subject(debug)]),
        }
    }

    fn create_cluster_role_binding_object(&self, debug: &MaterializeDebug) -> ClusterRoleBinding {
        ClusterRoleBinding {
            metadata: ObjectMeta {
                name: Some(debug.cluster_role_binding_name()),
                labels: Some(self.labels(debug)),
                ..Default::default()
            },
            role_ref: RoleRef {
                api_group: "rbac.authorization.k8s.io".to_string(),
                kind: "ClusterRole".to_string(),
                name: COLLECTOR_CLUSTER_SCOPED_CLUSTER_ROLE.to_string(),
            },
            subjects: Some(vec![self.subject(debug)]),
        }
    }

    fn subject(&self, debug: &MaterializeDebug) -> Subject {
        Subject {
            api_group: Some("".to_string()),
            kind: "ServiceAccount".to_string(),
            name: debug.service_account_name(),
            namespace: Some(debug.namespace()),
        }
    }

    /// The labels `managed_resource_meta` would set, for objects that cannot
    /// carry the owner reference it also sets.
    fn labels(&self, debug: &MaterializeDebug) -> std::collections::BTreeMap<String, String> {
        let mut labels = debug.default_labels();
        labels.extend(mz_cloud_resources::crd::recommended_k8s_labels(
            ManagedResource::app_name(debug),
        ));
        labels
    }

    fn create_deployment_object(
        &self,
        debug: &MaterializeDebug,
        mz: &Materialize,
        image: String,
        auth_mode: AuthMode,
    ) -> Deployment {
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

        let mut pod_template_labels = debug.default_labels();
        pod_template_labels.insert("materialize.cloud/name".to_owned(), debug.deployment_name());
        let match_labels = pod_template_labels.clone();
        pod_template_labels.extend(recommended_k8s_labels(debug.app_name()));
        if let Some(pod_labels) = &debug.spec.pod_labels {
            pod_template_labels.extend(pod_labels.clone());
        }

        let collect = &debug.spec.collect;
        let mut args = vec![
            "collector".to_string(),
            format!("--k8s-namespace={}", debug.namespace()),
            format!("--mz-instance-name={}", debug.spec.materialize_name),
            format!("--listen-addr=0.0.0.0:{}", self.config.collector_http_port),
            format!("--snapshot-dir={SNAPSHOT_DIR}"),
            format!(
                "--snapshot-interval={}s",
                debug.snapshot_interval().as_secs()
            ),
            format!("--retained-snapshots={}", debug.retained_snapshots()),
            format!(
                "--buffer-size-limit-bytes={}",
                debug.buffer_size_limit_bytes()
            ),
            format!(
                "--auth-mode={}",
                match auth_mode {
                    AuthMode::None => "none",
                    AuthMode::Password => "password",
                }
            ),
            format!("--dump-k8s={}", collect.k8s),
            format!("--dump-system-catalog={}", collect.system_catalog),
            format!("--dump-heap-profiles={}", collect.heap_profiles),
            format!("--dump-prometheus-metrics={}", collect.prometheus_metrics),
            format!("--dump-cpu-profiles={}", collect.cpu_profiles),
            format!(
                "--cpu-profile-duration-seconds={}",
                collect.cpu_profile_duration_seconds
            ),
        ];
        for namespace in debug.spec.additional_namespaces.iter().flatten() {
            if *namespace != debug.namespace() {
                args.push(format!("--additional-k8s-namespace={namespace}"));
            }
        }

        let mut env = Vec::new();
        if auth_mode == AuthMode::Password {
            env.push(EnvVar {
                name: "MZ_USERNAME".to_string(),
                value: Some("mz_system".to_string()),
                ..Default::default()
            });
            env.push(EnvVar {
                name: "MZ_PASSWORD".to_string(),
                value_from: Some(EnvVarSource {
                    secret_key_ref: Some(SecretKeySelector {
                        name: mz.backend_secret_name(),
                        key: MZ_SYSTEM_PASSWORD_SECRET_KEY.to_string(),
                        optional: Some(false),
                    }),
                    ..Default::default()
                }),
                ..Default::default()
            });
        }

        let readiness_probe = Probe {
            http_get: Some(HTTPGetAction {
                port: IntOrString::Int(self.config.collector_http_port.into()),
                path: Some("/api/readyz".into()),
                ..Default::default()
            }),
            failure_threshold: Some(3),
            period_seconds: Some(10),
            success_threshold: Some(1),
            timeout_seconds: Some(1),
            ..Default::default()
        };

        let container = Container {
            name: "debug-collector".to_owned(),
            image: Some(image),
            image_pull_policy: Some(self.config.image_pull_policy.to_string()),
            ports: Some(vec![ContainerPort {
                container_port: self.config.collector_http_port.into(),
                name: Some("http".into()),
                protocol: Some("TCP".into()),
                ..Default::default()
            }]),
            args: Some(args),
            env: Some(env),
            readiness_probe: Some(readiness_probe),
            resources: debug
                .spec
                .resource_requirements
                .clone()
                .or_else(|| self.config.default_resources.clone()),
            security_context,
            volume_mounts: Some(vec![VolumeMount {
                name: "snapshots".to_owned(),
                mount_path: SNAPSHOT_DIR.to_owned(),
                ..Default::default()
            }]),
            ..Default::default()
        };

        let deployment_spec = DeploymentSpec {
            replicas: Some(1),
            selector: LabelSelector {
                match_labels: Some(match_labels),
                ..Default::default()
            },
            // Two collectors running at once would double the profiling load
            // on the instance and split the snapshot history between two
            // buffers, so the old pod goes before the new one starts.
            strategy: Some(DeploymentStrategy {
                type_: Some("Recreate".to_string()),
                ..Default::default()
            }),
            template: PodTemplateSpec {
                // not using managed_resource_meta because the pod should be
                // owned by the deployment, not the debug resource
                metadata: Some(ObjectMeta {
                    annotations: debug.spec.pod_annotations.clone(),
                    labels: Some(pod_template_labels),
                    ..Default::default()
                }),
                spec: Some(PodSpec {
                    containers: vec![container],
                    service_account_name: Some(debug.service_account_name()),
                    security_context: Some(PodSecurityContext {
                        fs_group: Some(999),
                        run_as_user: Some(999),
                        run_as_group: Some(999),
                        ..Default::default()
                    }),
                    scheduler_name: self.config.scheduler_name.clone(),
                    volumes: Some(vec![Volume {
                        name: "snapshots".to_owned(),
                        empty_dir: Some(EmptyDirVolumeSource {
                            size_limit: Some(debug.buffer_volume_size_limit()),
                            ..Default::default()
                        }),
                        ..Default::default()
                    }]),
                    ..Default::default()
                }),
            },
            ..Default::default()
        };

        Deployment {
            metadata: debug.managed_resource_meta(debug.deployment_name()),
            spec: Some(deployment_spec),
            status: None,
        }
    }

    fn create_service_object(&self, debug: &MaterializeDebug) -> Service {
        Service {
            metadata: debug.managed_resource_meta(debug.service_name()),
            spec: Some(ServiceSpec {
                type_: Some("ClusterIP".to_string()),
                selector: Some(
                    btreemap! {"materialize.cloud/name".to_string() => debug.deployment_name()},
                ),
                ports: Some(vec![ServicePort {
                    name: Some("http".to_string()),
                    protocol: Some("TCP".to_string()),
                    port: self.config.collector_http_port.into(),
                    target_port: Some(IntOrString::Int(self.config.collector_http_port.into())),
                    ..Default::default()
                }]),
                ..Default::default()
            }),
            status: None,
        }
    }

    /// Removes the objects that cannot be owned by the debug resource.
    async fn delete_unowned_children(
        &self,
        client: &Client,
        debug: &MaterializeDebug,
    ) -> Result<(), Error> {
        delete_resource(
            &Api::<ClusterRoleBinding>::all(client.clone()),
            &debug.cluster_role_binding_name(),
        )
        .await?;
        for namespace in debug.collected_namespaces() {
            if namespace == debug.namespace() {
                continue;
            }
            delete_resource(
                &Api::<RoleBinding>::namespaced(client.clone(), &namespace),
                &debug.role_binding_name(&namespace),
            )
            .await?;
        }
        Ok(())
    }
}

#[async_trait::async_trait]
impl k8s_controller::Context for Context {
    type Resource = MaterializeDebug;
    type Error = Error;

    const FINALIZER_NAME: Option<&'static str> =
        Some("orchestratord.materialize.cloud/materialize-mz_debug");

    #[instrument(fields(materialize_debug=mz_debug.name_unchecked()))]
    async fn apply(
        &self,
        client: Client,
        mz_debug: &Self::Resource,
        _metadata: &mut TraceMetadata,
    ) -> Result<Option<Action>, Self::Error> {
        let namespace = mz_debug.namespace();
        if mz_debug.status.is_none() {
            let debug_api: Api<MaterializeDebug> = Api::namespaced(client.clone(), &namespace);
            let mut new_debug = mz_debug.clone();
            new_debug.status = Some(mz_debug.status());
            debug_api
                .replace_status(
                    &mz_debug.name_unchecked(),
                    &PostParams::default(),
                    &new_debug,
                )
                .await?;
            // Updating the status should trigger a reconciliation
            // which will include a status this time.
            return Ok(None);
        }

        let mz_api: Api<Materialize> = Api::namespaced(client.clone(), &namespace);
        let Some(mz) = get_resource(&mz_api, &mz_debug.spec.materialize_name).await? else {
            self.set_ready_condition(
                &client,
                mz_debug,
                false,
                "MaterializeNotFound",
                format!(
                    "Materialize {}/{} does not exist",
                    namespace, mz_debug.spec.materialize_name
                ),
            )
            .await?;
            return Ok(Some(Action::requeue(MISSING_MATERIALIZE_RETRY)));
        };
        if mz
            .status
            .as_ref()
            .is_none_or(|status| status.resource_id.is_empty())
        {
            trace!("materialize has no resource id yet");
            return Ok(Some(Action::requeue(MISSING_MATERIALIZE_RETRY)));
        }
        if !mz.meets_minimum_version(&DEBUG_COLLECTOR_MIN_VERSION) {
            self.set_ready_condition(
                &client,
                mz_debug,
                false,
                "UnsupportedVersion",
                format!(
                    "Materialize {} runs {}, which predates the mz_debug collector (needs v{})",
                    mz.name_unchecked(),
                    mz.spec.environmentd_image_ref,
                    *DEBUG_COLLECTOR_MIN_VERSION
                ),
            )
            .await?;
            return Ok(Some(Action::requeue(RESYNC_INTERVAL)));
        }
        let auth_mode = match mz.spec.authenticator_kind {
            AuthenticatorKind::None => AuthMode::None,
            AuthenticatorKind::Password | AuthenticatorKind::Sasl | AuthenticatorKind::Oidc => {
                AuthMode::Password
            }
            AuthenticatorKind::Frontegg => {
                self.set_ready_condition(
                    &client,
                    mz_debug,
                    false,
                    "UnsupportedAuthenticatorKind",
                    "the mz_debug collector cannot authenticate to an instance using Frontegg"
                        .to_string(),
                )
                .await?;
                return Ok(Some(Action::requeue(RESYNC_INTERVAL)));
            }
        };

        self.ensure_owner_reference(&client, mz_debug, &mz).await?;

        let image = mz_debug
            .spec
            .collector_image_ref
            .clone()
            .unwrap_or_else(|| {
                matching_image_from_environmentd_image_ref(
                    mz.active_environmentd_image_ref(),
                    "mz-mz_debug",
                    None,
                )
            });

        trace!("creating mz_debug collector service account");
        apply_resource(
            &Api::<ServiceAccount>::namespaced(client.clone(), &namespace),
            &self.create_service_account_object(mz_debug),
        )
        .await?;

        for collected_namespace in mz_debug.collected_namespaces() {
            trace!(
                "creating mz_debug collector role binding in {}",
                collected_namespace
            );
            if let Err(e) = apply_resource(
                &Api::<RoleBinding>::namespaced(client.clone(), &collected_namespace),
                &self.create_role_binding_object(mz_debug, &collected_namespace),
            )
            .await
            {
                // An additional namespace that does not exist, or that the
                // operator may not write to, must not hold up collection of
                // the instance's own namespace.
                if collected_namespace == namespace {
                    return Err(e.into());
                }
                warn!(
                    "failed to create mz_debug collector role binding in {}: {}",
                    collected_namespace, e
                );
            }
        }

        trace!("creating mz_debug collector cluster role binding");
        apply_resource(
            &Api::<ClusterRoleBinding>::all(client.clone()),
            &self.create_cluster_role_binding_object(mz_debug),
        )
        .await?;

        trace!("creating mz_debug collector deployment");
        apply_resource(
            &Api::<Deployment>::namespaced(client.clone(), &namespace),
            &self.create_deployment_object(mz_debug, &mz, image, auth_mode),
        )
        .await?;

        trace!("creating mz_debug collector service");
        apply_resource(
            &Api::<Service>::namespaced(client.clone(), &namespace),
            &self.create_service_object(mz_debug),
        )
        .await?;

        self.sync_deployment_status(&client, mz_debug).await?;

        Ok(Some(Action::requeue(RESYNC_INTERVAL)))
    }

    #[instrument(fields(materialize_debug=mz_debug.name_unchecked()))]
    async fn cleanup(
        &self,
        client: Client,
        mz_debug: &Self::Resource,
        _metadata: &mut TraceMetadata,
    ) -> Result<Option<Action>, Self::Error> {
        // A resource deleted before its status was ever written has no
        // resource id, and so no children named after one.
        if mz_debug.status.is_none() {
            return Ok(None);
        }
        self.delete_unowned_children(&client, mz_debug).await?;
        Ok(None)
    }
}

// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Status API and embedded UI for orchestratord.
//!
//! Serves a JSON API under `/api` describing the Materialize, Balancer, and
//! Console resources the operator manages, plus a small set of intentional
//! mutations (request/promote rollouts, edit the system parameter ConfigMap).
//! The UI at `/` is a single self-contained HTML file.
//!
//! There is no authentication. The server is only reachable over the
//! operator's ClusterIP service (or a port-forward), and the design doc
//! (doc/developer/design/20260724_orchestratord_api_ui.md) explicitly scopes
//! auth out of this version, which is why the mutation surface is a small
//! fixed set of operations rather than a generic write path.

use std::collections::BTreeMap;
use std::sync::Arc;

use axum::{
    Json, Router,
    extract::{Path, State},
    response::{Html, IntoResponse, Response},
    routing::{get, post, put},
};
use http::StatusCode;
use k8s_openapi::api::{
    apps::v1::{Deployment, StatefulSet},
    core::v1::{ConfigMap, Event, Pod, ResourceRequirements, Service},
};
use k8s_openapi::jiff::Timestamp;
use kube::{
    Api, Client, Resource, ResourceExt,
    api::{ListParams, ObjectMeta, Patch, PatchParams, PostParams},
};
use serde::{Deserialize, Deserializer, Serialize};
use uuid::Uuid;

use mz_cloud_resources::crd::{
    balancer::v1alpha1::Balancer,
    console::v1alpha1::Console,
    materialize::{MaterializeRolloutStrategy, RolloutRequestTimeout, v1alpha1::Materialize},
};

use crate::controller::materialize::generation::V26_1_0;

/// The key inside the system parameter ConfigMap that environmentd reads.
/// Must match the mount in `controller::materialize::generation`.
const SYSTEM_PARAMS_KEY: &str = "system-params.json";

/// Static facts about this operator instance, surfaced at `/api/info`.
#[derive(Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Info {
    pub version: String,
    pub cloud_provider: String,
    pub region: String,
    pub helm_chart_version: Option<String>,
    pub create_balancers: bool,
    pub create_console: bool,
}

pub struct Context {
    client: Client,
    info: Info,
}

impl Context {
    pub fn new(client: Client, info: Info) -> Self {
        Self { client, info }
    }
}

pub fn router(context: Arc<Context>) -> Router {
    Router::new()
        .route("/", get(get_ui))
        .route("/api/health", get(get_health))
        .route("/api/info", get(get_info))
        .route("/api/materializes", get(list_materializes))
        .route("/api/materializes/{namespace}/{name}", get(get_materialize))
        .route(
            "/api/materializes/{namespace}/{name}/rollout",
            post(post_rollout),
        )
        .route(
            "/api/materializes/{namespace}/{name}/promote",
            post(post_promote),
        )
        .route(
            "/api/materializes/{namespace}/{name}/system-params",
            get(get_system_params).put(put_system_params),
        )
        .route(
            "/api/materializes/{namespace}/{name}/config",
            put(put_config),
        )
        .route("/api/balancers", get(list_balancers))
        .route("/api/consoles", get(list_consoles))
        .with_state(context)
}

struct ApiError {
    status: StatusCode,
    message: String,
}

impl ApiError {
    fn new(status: StatusCode, message: impl Into<String>) -> Self {
        Self {
            status,
            message: message.into(),
        }
    }

    fn bad_request(message: impl Into<String>) -> Self {
        Self::new(StatusCode::BAD_REQUEST, message)
    }

    fn conflict(message: impl Into<String>) -> Self {
        Self::new(StatusCode::CONFLICT, message)
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        (
            self.status,
            Json(serde_json::json!({ "error": self.message })),
        )
            .into_response()
    }
}

impl From<kube::Error> for ApiError {
    fn from(e: kube::Error) -> Self {
        let status = match &e {
            kube::Error::Api(response) => {
                StatusCode::from_u16(response.code).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR)
            }
            _ => StatusCode::INTERNAL_SERVER_ERROR,
        };
        Self::new(status, e.to_string())
    }
}

async fn get_ui() -> Html<&'static str> {
    Html(include_str!("api/ui.html"))
}

async fn get_health(
    State(context): State<Arc<Context>>,
) -> Result<Json<serde_json::Value>, ApiError> {
    let mz_api: Api<Materialize> = Api::all(context.client.clone());
    mz_api.list(&ListParams::default().limit(1)).await?;
    Ok(Json(serde_json::json!({ "status": "ok" })))
}

async fn get_info(State(context): State<Arc<Context>>) -> Json<Info> {
    Json(context.info.clone())
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct ConditionView {
    #[serde(rename = "type")]
    type_: String,
    status: String,
    reason: String,
    message: String,
    last_transition_time: String,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct MaterializeSummary {
    namespace: String,
    name: String,
    resource_id: String,
    environment_id: String,
    environmentd_image_ref: String,
    running_image_ref: Option<String>,
    active_generation: u64,
    rollout_strategy: String,
    rollout_requested: bool,
    up_to_date: Option<ConditionView>,
    created_at: Option<String>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct WorkloadView {
    kind: String,
    name: String,
    image: Option<String>,
    ready_replicas: i32,
    replicas: i32,
    generation: Option<String>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct PodView {
    name: String,
    app: Option<String>,
    phase: Option<String>,
    ready: bool,
    restarts: i32,
    image: Option<String>,
    node: Option<String>,
    start_time: Option<String>,
    generation: Option<String>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct ServiceView {
    name: String,
    type_: Option<String>,
    cluster_ip: Option<String>,
    ports: Vec<String>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct EventView {
    #[serde(rename = "type")]
    type_: Option<String>,
    reason: Option<String>,
    message: Option<String>,
    object: String,
    count: i32,
    last_timestamp: Option<String>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct SpecView {
    backend_secret_name: String,
    authenticator_kind: String,
    system_parameter_configmap_name: Option<String>,
    balancerd_replicas: i32,
    console_replicas: i32,
    rollout_request_timeout: String,
    request_rollout: String,
    force_promote: String,
    force_rollout: String,
    enable_rbac: bool,
    /// Current per-component overrides, so an editor can prefill them. `None`
    /// means the component falls back to the operator's configured defaults.
    environmentd_resources: Option<ResourceRequirements>,
    balancerd_resources: Option<ResourceRequirements>,
    console_resources: Option<ResourceRequirements>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct CrView {
    namespace: String,
    name: String,
    image_ref: String,
    replicas: Option<i32>,
    ready: Option<ConditionView>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct MaterializeDetail {
    summary: MaterializeSummary,
    spec: SpecView,
    conditions: Vec<ConditionView>,
    statefulsets: Vec<WorkloadView>,
    deployments: Vec<WorkloadView>,
    services: Vec<ServiceView>,
    pods: Vec<PodView>,
    events: Vec<EventView>,
}

fn enum_string<T: Serialize>(value: &T) -> String {
    match serde_json::to_value(value) {
        Ok(serde_json::Value::String(s)) => s,
        Ok(other) => other.to_string(),
        Err(_) => String::new(),
    }
}

fn condition_views(mz: &Materialize) -> Vec<ConditionView> {
    mz.status
        .as_ref()
        .map(|status| {
            status
                .conditions
                .iter()
                .map(|c| ConditionView {
                    type_: c.type_.clone(),
                    status: c.status.clone(),
                    reason: c.reason.clone(),
                    message: c.message.clone(),
                    last_transition_time: c.last_transition_time.0.to_string(),
                })
                .collect()
        })
        .unwrap_or_default()
}

fn summarize(mz: &Materialize) -> MaterializeSummary {
    let status = mz.status.as_ref();
    MaterializeSummary {
        namespace: mz.namespace(),
        name: mz.name_unchecked(),
        resource_id: status.map(|s| s.resource_id.clone()).unwrap_or_default(),
        environment_id: mz.spec.environment_id.to_string(),
        environmentd_image_ref: mz.spec.environmentd_image_ref.clone(),
        running_image_ref: status
            .and_then(|s| s.last_completed_rollout_environmentd_image_ref.clone()),
        active_generation: status.map(|s| s.active_generation).unwrap_or(0),
        rollout_strategy: enum_string(&mz.spec.rollout_strategy),
        rollout_requested: mz.rollout_requested(),
        up_to_date: condition_views(mz)
            .into_iter()
            .find(|c| c.type_ == "UpToDate"),
        created_at: mz
            .meta()
            .creation_timestamp
            .as_ref()
            .map(|t| t.0.to_string()),
    }
}

async fn list_materializes(
    State(context): State<Arc<Context>>,
) -> Result<Json<Vec<MaterializeSummary>>, ApiError> {
    let mz_api: Api<Materialize> = Api::all(context.client.clone());
    let mzs = mz_api.list(&ListParams::default()).await?;
    let mut summaries: Vec<_> = mzs.items.iter().map(summarize).collect();
    summaries.sort_by(|a, b| (&a.namespace, &a.name).cmp(&(&b.namespace, &b.name)));
    Ok(Json(summaries))
}

fn workload_view_sts(sts: &StatefulSet) -> WorkloadView {
    WorkloadView {
        kind: "StatefulSet".into(),
        name: sts.name_unchecked(),
        image: sts
            .spec
            .as_ref()
            .and_then(|s| s.template.spec.as_ref())
            .and_then(|s| s.containers.first())
            .and_then(|c| c.image.clone()),
        ready_replicas: sts
            .status
            .as_ref()
            .and_then(|s| s.ready_replicas)
            .unwrap_or(0),
        replicas: sts.spec.as_ref().and_then(|s| s.replicas).unwrap_or(0),
        generation: sts
            .annotations()
            .get("materialize.cloud/generation")
            .cloned(),
    }
}

fn workload_view_deployment(deployment: &Deployment) -> WorkloadView {
    WorkloadView {
        kind: "Deployment".into(),
        name: deployment.name_unchecked(),
        image: deployment
            .spec
            .as_ref()
            .and_then(|s| s.template.spec.as_ref())
            .and_then(|s| s.containers.first())
            .and_then(|c| c.image.clone()),
        ready_replicas: deployment
            .status
            .as_ref()
            .and_then(|s| s.ready_replicas)
            .unwrap_or(0),
        replicas: deployment
            .spec
            .as_ref()
            .and_then(|s| s.replicas)
            .unwrap_or(0),
        generation: None,
    }
}

fn pod_view(pod: &Pod) -> PodView {
    let status = pod.status.as_ref();
    PodView {
        name: pod.name_unchecked(),
        app: pod
            .labels()
            .get("materialize.cloud/app")
            .or_else(|| pod.labels().get("app.kubernetes.io/name"))
            .cloned(),
        phase: status.and_then(|s| s.phase.clone()),
        ready: status
            .and_then(|s| s.conditions.as_ref())
            .map(|conditions| {
                conditions
                    .iter()
                    .any(|c| c.type_ == "Ready" && c.status == "True")
            })
            .unwrap_or(false),
        restarts: status
            .and_then(|s| s.container_statuses.as_ref())
            .map(|statuses| statuses.iter().map(|s| s.restart_count).sum())
            .unwrap_or(0),
        image: pod
            .spec
            .as_ref()
            .and_then(|s| s.containers.first())
            .and_then(|c| c.image.clone()),
        node: pod.spec.as_ref().and_then(|s| s.node_name.clone()),
        start_time: status
            .and_then(|s| s.start_time.as_ref())
            .map(|t| t.0.to_string()),
        generation: pod
            .annotations()
            .get("materialize.cloud/generation")
            .cloned(),
    }
}

fn service_view(service: &Service) -> ServiceView {
    let spec = service.spec.as_ref();
    ServiceView {
        name: service.name_unchecked(),
        type_: spec.and_then(|s| s.type_.clone()),
        cluster_ip: spec.and_then(|s| s.cluster_ip.clone()),
        ports: spec
            .and_then(|s| s.ports.as_ref())
            .map(|ports| {
                ports
                    .iter()
                    .map(|p| {
                        format!(
                            "{}:{}",
                            p.name.clone().unwrap_or_else(|| "-".into()),
                            p.port
                        )
                    })
                    .collect()
            })
            .unwrap_or_default(),
    }
}

async fn get_mz(context: &Context, namespace: &str, name: &str) -> Result<Materialize, ApiError> {
    let mz_api: Api<Materialize> = Api::namespaced(context.client.clone(), namespace);
    Ok(mz_api.get(name).await?)
}

async fn get_materialize(
    State(context): State<Arc<Context>>,
    Path((namespace, name)): Path<(String, String)>,
) -> Result<Json<MaterializeDetail>, ApiError> {
    let mz = get_mz(&context, &namespace, &name).await?;
    let summary = summarize(&mz);
    let resource_id = summary.resource_id.clone();

    let by_resource_id =
        ListParams::default().labels(&format!("materialize.cloud/mz-resource-id={}", resource_id));
    // Clusterd pods are created by environmentd's own orchestrator, not by
    // orchestratord, and don't carry the mz-resource-id label. They do carry
    // the recommended part-of label and live in the same namespace, so we
    // list by that and attribute per-pod below.
    let part_of = ListParams::default().labels("app.kubernetes.io/part-of=materialize");

    let sts_api: Api<StatefulSet> = Api::namespaced(context.client.clone(), &namespace);
    let deployment_api: Api<Deployment> = Api::namespaced(context.client.clone(), &namespace);
    let service_api: Api<Service> = Api::namespaced(context.client.clone(), &namespace);
    let pod_api: Api<Pod> = Api::namespaced(context.client.clone(), &namespace);
    let event_api: Api<Event> = Api::namespaced(context.client.clone(), &namespace);

    let all_events = ListParams::default();
    let (statefulsets, deployments, services, pods, events) = futures::try_join!(
        sts_api.list(&by_resource_id),
        deployment_api.list(&by_resource_id),
        service_api.list(&by_resource_id),
        pod_api.list(&part_of),
        event_api.list(&all_events),
    )?;

    let name_prefix = format!("mz{}-", resource_id);
    let pods: Vec<_> = pods
        .items
        .iter()
        .filter(|pod| {
            pod.labels()
                .get("materialize.cloud/mz-resource-id")
                .map(|id| id == &resource_id)
                // clusterd pods: no resource id label, attribute by namespace
                .unwrap_or_else(|| {
                    pod.labels()
                        .contains_key("environmentd.materialize.cloud/service-id")
                })
        })
        .map(pod_view)
        .collect();

    let mut events: Vec<_> = events
        .items
        .iter()
        .filter(|event| {
            let object_name = event.involved_object.name.as_deref().unwrap_or("");
            object_name == name || object_name.starts_with(&name_prefix)
        })
        .map(|event| EventView {
            type_: event.type_.clone(),
            reason: event.reason.clone(),
            message: event.message.clone(),
            object: format!(
                "{}/{}",
                event.involved_object.kind.as_deref().unwrap_or("-"),
                event.involved_object.name.as_deref().unwrap_or("-"),
            ),
            count: event.count.unwrap_or(1),
            last_timestamp: event.last_timestamp.as_ref().map(|t| t.0.to_string()),
        })
        .collect();
    events.sort_by(|a, b| b.last_timestamp.cmp(&a.last_timestamp));
    events.truncate(50);

    Ok(Json(MaterializeDetail {
        spec: SpecView {
            backend_secret_name: mz.spec.backend_secret_name.clone(),
            authenticator_kind: enum_string(&mz.spec.authenticator_kind),
            system_parameter_configmap_name: mz.spec.system_parameter_configmap_name.clone(),
            balancerd_replicas: mz.balancerd_replicas(),
            console_replicas: mz.console_replicas(),
            rollout_request_timeout: mz.spec.rollout_request_timeout.0.clone(),
            request_rollout: mz.spec.request_rollout.to_string(),
            force_promote: mz.spec.force_promote.clone(),
            force_rollout: mz.spec.force_rollout.to_string(),
            enable_rbac: mz.spec.enable_rbac,
            environmentd_resources: mz.spec.environmentd_resource_requirements.clone(),
            balancerd_resources: mz.spec.balancerd_resource_requirements.clone(),
            console_resources: mz.spec.console_resource_requirements.clone(),
        },
        conditions: condition_views(&mz),
        statefulsets: statefulsets.iter().map(workload_view_sts).collect(),
        deployments: deployments.iter().map(workload_view_deployment).collect(),
        services: services.iter().map(service_view).collect(),
        pods,
        events,
        summary,
    }))
}

#[derive(Deserialize, Default)]
#[serde(rename_all = "camelCase", default)]
struct RolloutRequest {
    force_rollout: bool,
    force_promote: bool,
}

async fn post_rollout(
    State(context): State<Arc<Context>>,
    Path((namespace, name)): Path<(String, String)>,
    body: Option<Json<RolloutRequest>>,
) -> Result<Json<serde_json::Value>, ApiError> {
    let request = body.map(|Json(r)| r).unwrap_or_default();
    let mz_api: Api<Materialize> = Api::namespaced(context.client.clone(), &namespace);
    let mut mz = mz_api.get(&name).await?;

    let rollout_id = Uuid::new_v4();
    mz.spec.request_rollout = rollout_id;
    if request.force_rollout {
        mz.spec.force_rollout = rollout_id;
    }
    if request.force_promote {
        mz.spec.force_promote = rollout_id.hyphenated().to_string();
    }
    // replace (not apply) so a concurrent spec edit fails with 409 instead of
    // being clobbered
    mz_api.replace(&name, &PostParams::default(), &mz).await?;
    Ok(Json(
        serde_json::json!({ "requestRollout": rollout_id.to_string() }),
    ))
}

async fn post_promote(
    State(context): State<Arc<Context>>,
    Path((namespace, name)): Path<(String, String)>,
) -> Result<Json<serde_json::Value>, ApiError> {
    let mz_api: Api<Materialize> = Api::namespaced(context.client.clone(), &namespace);
    let mut mz = mz_api.get(&name).await?;
    if !mz.rollout_requested() {
        return Err(ApiError::conflict(
            "no rollout is pending, nothing to promote",
        ));
    }
    mz.set_force_promote();
    mz_api.replace(&name, &PostParams::default(), &mz).await?;
    Ok(Json(
        serde_json::json!({ "forcePromote": mz.spec.force_promote }),
    ))
}

/// The system parameter ConfigMap is mounted into `environmentd`, which re-reads
/// it on its config sync loop and applies changes through `ALTER SYSTEM`. Edits
/// therefore take effect on a running environment without a rollout, bounded by
/// how fast the kubelet refreshes the mounted file.
///
/// Two things break that, and both are reported here rather than assumed:
/// wiring the ConfigMap into the spec for the first time changes the
/// StatefulSet and so needs a rollout before the sync loop exists, and a
/// `environmentd` older than [`V26_1_0`] never gets the mount at all.
/// The editable slice of the Materialize spec, as one request.
///
/// Every field is optional and only the ones present are changed, so the UI can
/// save a single row without restating the rest of the configuration.
#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct ConfigRequest {
    environmentd_image_ref: Option<String>,
    rollout_strategy: Option<MaterializeRolloutStrategy>,
    rollout_request_timeout: Option<String>,
    balancerd_replicas: Option<i32>,
    console_replicas: Option<i32>,
    enable_rbac: Option<bool>,
    #[serde(default)]
    resources: ResourcesRequest,
    /// Request the rollout in the same write, so a change that needs one can be
    /// applied without a second call.
    #[serde(default)]
    request_rollout: bool,
}

/// Distinguishes an absent field from an explicit `null`, so a request can
/// clear one component's overrides without disturbing the others.
fn deserialize_some<'de, T, D>(deserializer: D) -> Result<Option<T>, D::Error>
where
    T: Deserialize<'de>,
    D: Deserializer<'de>,
{
    T::deserialize(deserializer).map(Some)
}

/// Per-component resource overrides. An omitted component is left alone; a
/// component given as `null` has its override cleared, falling back to the
/// operator's configured defaults.
#[derive(Deserialize, Default)]
#[serde(rename_all = "camelCase")]
struct ResourcesRequest {
    #[serde(default, deserialize_with = "deserialize_some")]
    environmentd: Option<Option<ResourceRequirements>>,
    #[serde(default, deserialize_with = "deserialize_some")]
    balancerd: Option<Option<ResourceRequirements>>,
    #[serde(default, deserialize_with = "deserialize_some")]
    console: Option<Option<ResourceRequirements>>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct ConfigResponse {
    /// Fields this write actually changed, by the same names the request uses.
    updated: Vec<String>,
    /// True when a changed field is part of the environmentd StatefulSet, which
    /// is hashed into the Materialize status: the image, the RBAC flag, and
    /// environmentd's own resources only reach pods through a new generation.
    /// Rollout strategy, the rollout timeout, replica counts, and balancerd and
    /// console resources take effect without one.
    rollout_required: bool,
    /// Set when this write also requested the rollout.
    request_rollout: Option<String>,
}

/// Fields whose change only reaches environmentd through a new generation.
const ROLLOUT_BOUND_FIELDS: &[&str] = &[
    "environmentdImageRef",
    "enableRbac",
    "resources.environmentd",
];

async fn put_config(
    State(context): State<Arc<Context>>,
    Path((namespace, name)): Path<(String, String)>,
    Json(request): Json<ConfigRequest>,
) -> Result<Json<ConfigResponse>, ApiError> {
    let mz_api: Api<Materialize> = Api::namespaced(context.client.clone(), &namespace);
    let mut mz = mz_api.get(&name).await?;
    let mut updated = Vec::new();

    if let Some(image_ref) = request.environmentd_image_ref {
        let image_ref = image_ref.trim().to_owned();
        if image_ref.is_empty() {
            return Err(ApiError::bad_request(
                "environmentd image ref cannot be empty",
            ));
        }
        if image_ref != mz.spec.environmentd_image_ref {
            // Check the proposed image against the same upgrade window the
            // controller enforces, by asking a candidate copy rather than
            // reimplementing the rule. Otherwise the write would succeed and
            // only surface as FailedDeploy once it tried to roll.
            let mut candidate = mz.clone();
            candidate.spec.environmentd_image_ref = image_ref.clone();
            if !candidate.within_upgrade_window() {
                let running = mz
                    .status
                    .as_ref()
                    .and_then(|status| status.last_completed_rollout_environmentd_image_ref.clone())
                    .unwrap_or_else(|| mz.spec.environmentd_image_ref.clone());
                return Err(ApiError::bad_request(format!(
                    "refusing to move from {running} to {image_ref}: more than one major version \
                     from the last successful rollout, or a downgrade. Upgrade to an intermediate \
                     version first."
                )));
            }
            mz.spec.environmentd_image_ref = image_ref;
            updated.push("environmentdImageRef".to_owned());
        }
    }

    if let Some(strategy) = request.rollout_strategy {
        if mz.spec.rollout_strategy != strategy {
            mz.spec.rollout_strategy = strategy;
            updated.push("rolloutStrategy".to_owned());
        }
    }

    if let Some(timeout) = request.rollout_request_timeout {
        let timeout = timeout.trim().to_owned();
        // The CRD silently falls back to the default on an unparseable value,
        // so reject it here instead of accepting a setting that will not apply.
        humantime::parse_duration(&timeout).map_err(|e| {
            ApiError::bad_request(format!("invalid rollout timeout {timeout:?}: {e}"))
        })?;
        if mz.spec.rollout_request_timeout.0 != timeout {
            mz.spec.rollout_request_timeout = RolloutRequestTimeout(timeout);
            updated.push("rolloutRequestTimeout".to_owned());
        }
    }

    if let Some(replicas) = request.balancerd_replicas {
        if replicas < 0 {
            return Err(ApiError::bad_request(
                "balancerd replicas cannot be negative",
            ));
        }
        if mz.spec.balancerd_replicas != Some(replicas) {
            mz.spec.balancerd_replicas = Some(replicas);
            updated.push("balancerdReplicas".to_owned());
        }
    }

    if let Some(replicas) = request.console_replicas {
        if replicas < 0 {
            return Err(ApiError::bad_request("console replicas cannot be negative"));
        }
        if mz.spec.console_replicas != Some(replicas) {
            mz.spec.console_replicas = Some(replicas);
            updated.push("consoleReplicas".to_owned());
        }
    }

    if let Some(enable_rbac) = request.enable_rbac {
        if mz.spec.enable_rbac != enable_rbac {
            mz.spec.enable_rbac = enable_rbac;
            updated.push("enableRbac".to_owned());
        }
    }

    if let Some(resources) = request.resources.environmentd {
        if mz.spec.environmentd_resource_requirements != resources {
            mz.spec.environmentd_resource_requirements = resources;
            updated.push("resources.environmentd".to_owned());
        }
    }
    if let Some(resources) = request.resources.balancerd {
        if mz.spec.balancerd_resource_requirements != resources {
            mz.spec.balancerd_resource_requirements = resources;
            updated.push("resources.balancerd".to_owned());
        }
    }
    if let Some(resources) = request.resources.console {
        if mz.spec.console_resource_requirements != resources {
            mz.spec.console_resource_requirements = resources;
            updated.push("resources.console".to_owned());
        }
    }

    let rollout_required = updated
        .iter()
        .any(|field| ROLLOUT_BOUND_FIELDS.contains(&field.as_str()));

    // Only bump the rollout when something changed, so a no-op save does not
    // start one.
    let rollout_id = (request.request_rollout && !updated.is_empty()).then(Uuid::new_v4);
    if let Some(rollout_id) = rollout_id {
        mz.spec.request_rollout = rollout_id;
    }
    if !updated.is_empty() {
        mz_api.replace(&name, &PostParams::default(), &mz).await?;
    }

    Ok(Json(ConfigResponse {
        updated,
        rollout_required,
        request_rollout: rollout_id.map(|id| id.to_string()),
    }))
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct SystemParamsResponse {
    configmap_name: Option<String>,
    params: Option<serde_json::Value>,
    /// Whether this environment's `environmentd` is new enough to be given the
    /// mount and the sync loop.
    sync_supported: bool,
    /// Whether edits to the ConfigMap reach `environmentd` without a rollout.
    live_reload: bool,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct SystemParamsWriteResponse {
    configmap_name: String,
    /// True only when this write had to add `systemParameterConfigmapName` to
    /// the spec. That changes the StatefulSet, so the sync loop only starts
    /// after the next rollout. Subsequent edits are picked up live.
    rollout_required: bool,
    sync_supported: bool,
    /// How many `environmentd` pods were nudged to re-read the ConfigMap. See
    /// [`refresh_mounted_configmap`].
    pods_refreshed: usize,
}

/// Annotation written to `environmentd` pods to force the kubelet to re-project
/// the system parameter ConfigMap.
const REFRESHED_AT_ANNOTATION: &str = "materialize.cloud/system-params-refreshed-at";

/// Nudge the pods that mount the system parameter ConfigMap so they observe a
/// write promptly.
///
/// A ConfigMap update does not reach a mounted volume immediately: the kubelet
/// refreshes projected volumes on its own sync period, so `environmentd` can
/// keep reading the old file for up to that period even though its own sync
/// loop runs every second. Writing an annotation to the *pod* triggers a pod
/// sync, which re-projects the volume right away.
///
/// This deliberately annotates the live pods rather than the pod template. The
/// template is part of the StatefulSet, and the StatefulSet is hashed into the
/// Materialize status, so changing it would create a new generation and roll
/// the environment out on every save. Patching the pod object changes neither
/// the StatefulSet nor the container, so nothing restarts.
///
/// Best effort: the ConfigMap write has already succeeded by this point, so a
/// failure here only means the change lands on the kubelet's own schedule.
/// Returns the number of pods successfully annotated.
async fn refresh_mounted_configmap(client: &Client, mz: &Materialize, namespace: &str) -> usize {
    let Some(status) = mz.status.as_ref() else {
        return 0;
    };
    let pod_api: Api<Pod> = Api::namespaced(client.clone(), namespace);
    // Only environmentd mounts the system parameter ConfigMap; clusterd does not.
    let selector = format!(
        "materialize.cloud/mz-resource-id={},materialize.cloud/app=environmentd",
        status.resource_id,
    );
    let pods = match pod_api.list(&ListParams::default().labels(&selector)).await {
        Ok(pods) => pods,
        Err(e) => {
            tracing::warn!("could not list environmentd pods to refresh system parameters: {e}");
            return 0;
        }
    };

    let patch = serde_json::json!({
        "metadata": { "annotations": { REFRESHED_AT_ANNOTATION: Timestamp::now().to_string() } },
    });
    let mut refreshed = 0;
    for pod in &pods.items {
        let name = pod.name_unchecked();
        match pod_api
            .patch(&name, &PatchParams::default(), &Patch::Merge(&patch))
            .await
        {
            Ok(_) => refreshed += 1,
            Err(e) => tracing::warn!("could not refresh system parameters on pod {name}: {e}"),
        }
    }
    refreshed
}

async fn get_system_params(
    State(context): State<Arc<Context>>,
    Path((namespace, name)): Path<(String, String)>,
) -> Result<Json<SystemParamsResponse>, ApiError> {
    let mz = get_mz(&context, &namespace, &name).await?;
    let sync_supported = mz.meets_minimum_version(&V26_1_0);
    let Some(configmap_name) = mz.spec.system_parameter_configmap_name.clone() else {
        return Ok(Json(SystemParamsResponse {
            configmap_name: None,
            params: None,
            sync_supported,
            live_reload: false,
        }));
    };
    let configmap_api: Api<ConfigMap> = Api::namespaced(context.client.clone(), &namespace);
    let params = match configmap_api.get_opt(&configmap_name).await? {
        Some(configmap) => configmap
            .data
            .as_ref()
            .and_then(|data| data.get(SYSTEM_PARAMS_KEY))
            .map(|raw| {
                serde_json::from_str(raw).map_err(|e| {
                    ApiError::new(
                        StatusCode::INTERNAL_SERVER_ERROR,
                        format!("ConfigMap {configmap_name} contains invalid JSON: {e}"),
                    )
                })
            })
            .transpose()?,
        None => None,
    };
    Ok(Json(SystemParamsResponse {
        configmap_name: Some(configmap_name),
        params,
        sync_supported,
        // The mount and the sync loop are generated from this spec field, so
        // once it is set the running generation reloads edits on its own.
        live_reload: sync_supported,
    }))
}

async fn put_system_params(
    State(context): State<Arc<Context>>,
    Path((namespace, name)): Path<(String, String)>,
    Json(params): Json<serde_json::Value>,
) -> Result<Json<SystemParamsWriteResponse>, ApiError> {
    if !params.is_object() {
        return Err(ApiError::bad_request(
            "system parameters must be a JSON object",
        ));
    }

    let mz_api: Api<Materialize> = Api::namespaced(context.client.clone(), &namespace);
    let mut mz = mz_api.get(&name).await?;
    let sync_supported = mz.meets_minimum_version(&V26_1_0);
    let configmap_name = mz
        .spec
        .system_parameter_configmap_name
        .clone()
        .unwrap_or_else(|| format!("{name}-system-params"));

    let configmap_api: Api<ConfigMap> = Api::namespaced(context.client.clone(), &namespace);
    let raw = serde_json::to_string_pretty(&params).expect("value round-trips");
    match configmap_api.get_opt(&configmap_name).await? {
        Some(mut configmap) => {
            configmap
                .data
                .get_or_insert_with(BTreeMap::new)
                .insert(SYSTEM_PARAMS_KEY.into(), raw);
            configmap_api
                .replace(&configmap_name, &PostParams::default(), &configmap)
                .await?;
        }
        None => {
            let configmap = ConfigMap {
                metadata: ObjectMeta {
                    name: Some(configmap_name.clone()),
                    namespace: Some(namespace.clone()),
                    ..Default::default()
                },
                data: Some(BTreeMap::from([(SYSTEM_PARAMS_KEY.into(), raw)])),
                ..Default::default()
            };
            configmap_api
                .create(&PostParams::default(), &configmap)
                .await?;
        }
    }

    // Point the spec at the ConfigMap after it exists, so the spec never
    // references a missing ConfigMap. This is also what adds the volume mount
    // and the config sync arguments to the StatefulSet, which is why only this
    // first write needs a rollout.
    let wired_configmap = mz.spec.system_parameter_configmap_name.is_none();
    if wired_configmap {
        mz.spec.system_parameter_configmap_name = Some(configmap_name.clone());
        mz_api.replace(&name, &PostParams::default(), &mz).await?;
    }

    // Only worth nudging when the running pods actually mount the ConfigMap.
    // When this write is the one that wired it up, they do not yet.
    let pods_refreshed = if sync_supported && !wired_configmap {
        refresh_mounted_configmap(&context.client, &mz, &namespace).await
    } else {
        0
    };

    Ok(Json(SystemParamsWriteResponse {
        configmap_name,
        rollout_required: wired_configmap,
        sync_supported,
        pods_refreshed,
    }))
}

async fn list_balancers(
    State(context): State<Arc<Context>>,
) -> Result<Json<Vec<CrView>>, ApiError> {
    let api: Api<Balancer> = Api::all(context.client.clone());
    let balancers = api.list(&ListParams::default()).await?;
    Ok(Json(
        balancers
            .items
            .iter()
            .map(|balancer| CrView {
                namespace: balancer.namespace(),
                name: balancer.name_unchecked(),
                image_ref: balancer.spec.balancerd_image_ref.clone(),
                replicas: balancer.spec.replicas,
                ready: ready_condition(balancer.status.as_ref().map(|s| s.conditions.as_slice())),
            })
            .collect(),
    ))
}

async fn list_consoles(State(context): State<Arc<Context>>) -> Result<Json<Vec<CrView>>, ApiError> {
    let api: Api<Console> = Api::all(context.client.clone());
    let consoles = api.list(&ListParams::default()).await?;
    Ok(Json(
        consoles
            .items
            .iter()
            .map(|console| CrView {
                namespace: console.namespace(),
                name: console.name_unchecked(),
                image_ref: console.spec.console_image_ref.clone(),
                replicas: console.spec.replicas,
                ready: ready_condition(console.status.as_ref().map(|s| s.conditions.as_slice())),
            })
            .collect(),
    ))
}

fn ready_condition(
    conditions: Option<&[k8s_openapi::apimachinery::pkg::apis::meta::v1::Condition]>,
) -> Option<ConditionView> {
    conditions?
        .iter()
        .find(|c| c.type_ == "Ready")
        .map(|c| ConditionView {
            type_: c.type_.clone(),
            status: c.status.clone(),
            reason: c.reason.clone(),
            message: c.message.clone(),
            last_transition_time: c.last_transition_time.0.to_string(),
        })
}

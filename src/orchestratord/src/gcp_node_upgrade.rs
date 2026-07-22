// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Triggers rollouts of Materialize instances when GKE upgrades the node
//! pools they are running on.
//!
//! GKE automatically upgrades node pools (e.g. to roll out new node images),
//! and this cannot be disabled. With the blue-green upgrade strategy, GKE
//! first creates replacement (green) nodes, then cordons all of the existing
//! (blue) nodes, then drains them in batches (respecting pod disruption
//! budgets), and finally deletes them after a soak period of up to seven
//! days. Left alone, this would evict or force-delete environmentd and
//! clusterd pods, causing an outage.
//!
//! This module instead moves the pods with the standard graceful rollout
//! machinery before GKE gets around to deleting the nodes:
//!
//!   * A Pub/Sub subscriber listens for GKE cluster notifications
//!     (`UpgradeEvent`s) and *arms* a node pool when it starts upgrading.
//!     Since notifications can be missed (e.g. while orchestratord is
//!     restarting), the GKE API is additionally polled at startup and
//!     periodically thereafter, arming any watched pool with an upgrade in
//!     progress.
//!
//!   * While a pool is armed, its blue-green upgrade phase is polled from
//!     the GKE API. Once the phase reports that *all* blue nodes have been
//!     cordoned (`DRAINING_BLUE_POOL` or later), each Materialize instance
//!     with environmentd or clusterd pods on the cordoned nodes gets a
//!     forced rollout, triggered by setting the
//!     `materialize.cloud/force-rollout` annotation on the v1 Materialize
//!     resource. The new generation of pods can only be scheduled onto the
//!     green nodes (the blue nodes are unschedulable), and the old
//!     generation is torn down gracefully once the new one is ready.
//!
//! Arming on upgrade notifications rather than triggering on any cordon
//! avoids spurious (and expensive) rollouts when a node is cordoned for
//! reasons that don't mean the node is going away, e.g. an administrator
//! debugging a node. Waiting for the cordoning phase to complete before
//! triggering ensures the new generation cannot be scheduled onto a blue
//! node that simply hadn't been cordoned *yet* and will still be drained.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use anyhow::{Context as _, bail};
use k8s_openapi::api::core::v1::{Node, Pod};
use kube::{
    Api, Client,
    api::{ListParams, Patch, PatchParams},
};
use serde::Deserialize;
use serde_json::json;
use tracing::{debug, info, warn};
use uuid::Uuid;

use crate::k8s::get_resource;
use mz_cloud_resources::crd::materialize::{FORCE_ROLLOUT_ANNOTATION, v1::Materialize};

/// The node label GKE uses to record which node pool a node belongs to.
const GKE_NODE_POOL_LABEL: &str = "cloud.google.com/gke-nodepool";

/// Label selector matching the pods which must be moved before their node
/// goes away: environmentd and clusterd pods, which are moved between
/// generations by the rollout machinery. Balancerd and console pods are
/// stateless deployments and can be drained normally.
const DATA_PLANE_POD_SELECTOR: &str = "app.kubernetes.io/name in (environmentd,clusterd),materialize.cloud/organization-name,materialize.cloud/organization-namespace";

/// Safety valve: disarm pools which have been armed for longer than this.
/// GKE caps the total soak time of a blue-green upgrade at seven days, so a
/// pool armed for longer than this is not going to see any more drains from
/// the upgrade that armed it.
const MAX_ARMED_DURATION: Duration = Duration::from_secs(14 * 24 * 60 * 60);

#[derive(Debug, Clone)]
pub struct Config {
    /// The Pub/Sub subscription receiving GKE cluster notifications, in
    /// `projects/{project}/subscriptions/{subscription}` form.
    pub notification_subscription: String,
    /// The name of the GKE cluster this orchestratord is running in.
    pub cluster_name: String,
    /// The location (region or zone) of the GKE cluster.
    pub cluster_location: String,
    /// The node pools to watch. When empty, all node pools are watched.
    pub watched_node_pools: Vec<String>,
    /// How often to check armed node pools for progress.
    pub scan_interval: Duration,
    /// How often to poll the GKE API for upgrades in progress, to catch
    /// missed notifications.
    pub gke_poll_interval: Duration,
    /// Minimum time between consecutive rollout triggers for the same
    /// instance.
    ///
    /// A rollout in progress already suppresses re-triggering, but the
    /// in-progress signal is only visible once the instance's status has
    /// been updated by the reconcile loop; the cooldown covers that gap.
    pub trigger_cooldown: Duration,
}

impl Config {
    pub fn new(
        notification_subscription: String,
        cluster_name: String,
        cluster_location: String,
        watched_node_pools: Vec<String>,
    ) -> Result<Self, anyhow::Error> {
        let parts: Vec<_> = notification_subscription.split('/').collect();
        if !matches!(&*parts, ["projects", p, "subscriptions", s] if !p.is_empty() && !s.is_empty())
        {
            bail!(
                "invalid Pub/Sub subscription {notification_subscription:?}: expected projects/{{project}}/subscriptions/{{subscription}}"
            );
        }
        Ok(Self {
            notification_subscription,
            cluster_name,
            cluster_location,
            watched_node_pools,
            scan_interval: Duration::from_secs(60),
            gke_poll_interval: Duration::from_secs(60 * 60),
            trigger_cooldown: Duration::from_secs(300),
        })
    }

    fn project(&self) -> &str {
        self.notification_subscription
            .split('/')
            .nth(1)
            .expect("validated in Config::new")
    }

    fn watches_pool(&self, pool: &str) -> bool {
        self.watched_node_pools.is_empty() || self.watched_node_pools.iter().any(|p| p == pool)
    }

    fn node_pool_url(&self, pool: &str) -> String {
        format!(
            "https://container.googleapis.com/v1beta1/projects/{}/locations/{}/clusters/{}/nodePools/{}",
            self.project(),
            self.cluster_location,
            self.cluster_name,
            pool,
        )
    }
}

/// The blue-green upgrade phase of a node pool, from the GKE API.
///
/// `WAITING_TO_DRAIN_BLUE_POOL` (the wait window of autoscaled blue-green
/// upgrades, between cordoning and draining) is only reported by the
/// `v1beta1` API, which is why this module talks to that version.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
enum BlueGreenPhase {
    PhaseUnspecified,
    UpdateStarted,
    CreatingGreenPool,
    CordoningBluePool,
    WaitingToDrainBluePool,
    DrainingBluePool,
    NodePoolSoaking,
    DeletingBluePool,
    RollbackStarted,
    /// A phase this version of orchestratord doesn't know about.
    #[serde(other)]
    Unknown,
}

impl BlueGreenPhase {
    /// Whether every blue node is guaranteed to have been cordoned, meaning
    /// a new generation of pods cannot land on a node which is about to be
    /// drained.
    ///
    /// `WAITING_TO_DRAIN_BLUE_POOL` is the ideal trigger window: all blue
    /// nodes are cordoned but GKE won't start draining them until the
    /// configured wait (up to 7 days) elapses.
    fn blue_pool_fully_cordoned(&self) -> bool {
        matches!(
            self,
            Self::WaitingToDrainBluePool
                | Self::DrainingBluePool
                | Self::NodePoolSoaking
                | Self::DeletingBluePool
        )
    }
}

#[derive(Debug, Default)]
struct ArmedPools {
    pools: BTreeMap<String, ArmedPool>,
}

#[derive(Debug)]
struct ArmedPool {
    armed_at: Instant,
}

impl ArmedPools {
    fn arm(&mut self, pool: &str, reason: &str) {
        self.pools.entry(pool.to_owned()).or_insert_with(|| {
            info!(pool, reason, "arming node pool");
            ArmedPool {
                armed_at: Instant::now(),
            }
        });
    }
}

/// Runs the GCP node upgrade watcher forever. Errors are logged and retried.
pub async fn run(client: Client, config: Config) {
    info!(
        subscription = config.notification_subscription,
        cluster_name = config.cluster_name,
        cluster_location = config.cluster_location,
        watched_node_pools = ?config.watched_node_pools,
        "starting GCP node upgrade watcher",
    );

    let armed = Arc::new(Mutex::new(ArmedPools::default()));

    let gcp = Arc::new(GcpApiClient::new().await);

    mz_ore::task::spawn(|| "gcp node upgrade notification subscriber", {
        let config = config.clone();
        let armed = Arc::clone(&armed);
        let gcp = Arc::clone(&gcp);
        async move { subscriber_loop(gcp, config, armed).await }
    });

    let mut scan_interval = tokio::time::interval(config.scan_interval);
    scan_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    let mut last_gke_poll: Option<Instant> = None;
    let mut last_triggered = BTreeMap::new();
    loop {
        scan_interval.tick().await;

        // Poll the GKE API for upgrades in progress, to catch notifications
        // that were missed (e.g. published while orchestratord was down).
        // The first poll happens immediately at startup.
        if last_gke_poll.is_none_or(|at| at.elapsed() >= config.gke_poll_interval) {
            match poll_gke_for_upgrades(&gcp, &config).await {
                Ok(upgrading_pools) => {
                    last_gke_poll = Some(Instant::now());
                    let mut armed = armed.lock().expect("poisoned");
                    for pool in upgrading_pools {
                        armed.arm(&pool, "GKE API reports an upgrade in progress");
                    }
                }
                Err(e) => {
                    warn!("failed to poll GKE for node pool upgrades: {e:#}");
                }
            }
        }

        let pools: Vec<String> = {
            let armed = armed.lock().expect("poisoned");
            armed.pools.keys().cloned().collect()
        };
        for pool in pools {
            match check_armed_pool(&client, &gcp, &config, &pool, &mut last_triggered).await {
                Ok(PoolCheckOutcome::StillUpgrading) => {}
                Ok(PoolCheckOutcome::Done(reason)) => {
                    info!(pool, reason, "disarming node pool");
                    armed.lock().expect("poisoned").pools.remove(&pool);
                }
                Err(e) => {
                    warn!(pool, "failed to check armed node pool: {e:#}");
                    let armed_at = armed
                        .lock()
                        .expect("poisoned")
                        .pools
                        .get(&pool)
                        .map(|state| state.armed_at);
                    if armed_at.is_some_and(|at| at.elapsed() > MAX_ARMED_DURATION) {
                        warn!(
                            pool,
                            "disarming node pool: armed for longer than {MAX_ARMED_DURATION:?}"
                        );
                        armed.lock().expect("poisoned").pools.remove(&pool);
                    }
                }
            }
        }
    }
}

enum PoolCheckOutcome {
    StillUpgrading,
    Done(&'static str),
}

/// Checks the upgrade progress of a single armed node pool, triggering
/// rollouts for affected instances once the blue pool is fully cordoned.
async fn check_armed_pool(
    client: &Client,
    gcp: &GcpApiClient,
    config: &Config,
    pool: &str,
    last_triggered: &mut BTreeMap<(String, String), Instant>,
) -> Result<PoolCheckOutcome, anyhow::Error> {
    #[derive(Deserialize)]
    #[serde(rename_all = "camelCase")]
    struct NodePool {
        update_info: Option<UpdateInfo>,
    }
    #[derive(Deserialize)]
    #[serde(rename_all = "camelCase")]
    struct UpdateInfo {
        blue_green_info: Option<BlueGreenInfo>,
    }
    #[derive(Deserialize)]
    #[serde(rename_all = "camelCase")]
    struct BlueGreenInfo {
        phase: Option<BlueGreenPhase>,
    }

    let node_pool: NodePool = serde_json::from_value(gcp.get(&config.node_pool_url(pool)).await?)
        .context("parsing GKE nodePool response")?;

    let Some(phase) = node_pool
        .update_info
        .and_then(|info| info.blue_green_info)
        .and_then(|info| info.phase)
    else {
        // No blue-green upgrade is in progress (any longer). Either the
        // upgrade completed, or this pool doesn't use the blue-green
        // strategy, in which case we can't protect it: nodes are drained
        // immediately as they're replaced, so there is no window in which
        // to move the pods gracefully.
        return Ok(PoolCheckOutcome::Done(
            "no blue-green upgrade in progress; if this pool was upgraded with the \
             surge strategy, its pods were NOT protected: configure the pool to use \
             blue-green upgrades",
        ));
    };

    debug!(pool, ?phase, "checked armed node pool");
    if phase == BlueGreenPhase::RollbackStarted {
        // GKE is restoring the blue pool; the nodes are being uncordoned,
        // so there is nothing to move away from. Stay armed in case the
        // rollback is followed by another upgrade attempt (the phase goes
        // back to an earlier value in that case).
        return Ok(PoolCheckOutcome::StillUpgrading);
    }
    if !phase.blue_pool_fully_cordoned() {
        // Blue nodes may still be schedulable; triggering a rollout now
        // could schedule the new generation onto a blue node that just
        // hasn't been cordoned yet. Wait for the cordoning phase to
        // complete.
        return Ok(PoolCheckOutcome::StillUpgrading);
    }

    // All blue nodes are cordoned. Find Materialize instances with data
    // plane pods on them and trigger rollouts.
    let node_api: Api<Node> = Api::all(client.clone());
    let nodes = node_api
        .list(&ListParams::default().labels(&format!("{GKE_NODE_POOL_LABEL}={pool}")))
        .await?;
    let cordoned_nodes: BTreeSet<String> = nodes
        .items
        .into_iter()
        .filter(|node| {
            node.spec
                .as_ref()
                .and_then(|spec| spec.unschedulable)
                .unwrap_or(false)
        })
        .filter_map(|node| node.metadata.name)
        .collect();
    if cordoned_nodes.is_empty() {
        return Ok(PoolCheckOutcome::StillUpgrading);
    }
    debug!(
        pool,
        ?cordoned_nodes,
        "found cordoned nodes in armed node pool"
    );

    // Pods which are already terminating are ignored: the old generation's
    // pods are deleted asynchronously after a rollout completes and must not
    // re-trigger another rollout.
    let pod_api: Api<Pod> = Api::all(client.clone());
    let pods = pod_api
        .list(&ListParams::default().labels(DATA_PLANE_POD_SELECTOR))
        .await?;
    let mut affected_instances: BTreeMap<(String, String), BTreeSet<String>> = BTreeMap::new();
    for pod in pods.items {
        if pod.metadata.deletion_timestamp.is_some() {
            continue;
        }
        let Some(node_name) = pod.spec.as_ref().and_then(|spec| spec.node_name.clone()) else {
            continue;
        };
        if !cordoned_nodes.contains(&node_name) {
            continue;
        }
        let Some(labels) = &pod.metadata.labels else {
            continue;
        };
        let (Some(namespace), Some(name)) = (
            labels.get("materialize.cloud/organization-namespace"),
            labels.get("materialize.cloud/organization-name"),
        ) else {
            continue;
        };
        affected_instances
            .entry((namespace.clone(), name.clone()))
            .or_default()
            .insert(node_name);
    }

    for ((namespace, name), nodes) in affected_instances {
        if let Err(e) =
            maybe_trigger_rollout(client, config, last_triggered, &namespace, &name, &nodes).await
        {
            warn!(
                namespace,
                name, "failed to trigger rollout for instance on cordoned nodes: {e:#}"
            );
        }
    }

    Ok(PoolCheckOutcome::StillUpgrading)
}

/// Triggers a forced rollout of the given Materialize instance, unless one
/// is already in progress or was triggered very recently.
async fn maybe_trigger_rollout(
    client: &Client,
    config: &Config,
    last_triggered: &mut BTreeMap<(String, String), Instant>,
    namespace: &str,
    name: &str,
    nodes: &BTreeSet<String>,
) -> Result<(), anyhow::Error> {
    let key = (namespace.to_owned(), name.to_owned());
    if let Some(triggered_at) = last_triggered.get(&key) {
        if triggered_at.elapsed() < config.trigger_cooldown {
            debug!(
                namespace,
                name, "skipping rollout trigger: instance is in the trigger cooldown period"
            );
            return Ok(());
        }
    }

    let mz_api: Api<Materialize> = Api::namespaced(client.clone(), namespace);
    let Some(mz) = get_resource(&mz_api, name).await? else {
        warn!(
            namespace,
            name, "pods on cordoned nodes belong to a Materialize instance which no longer exists"
        );
        return Ok(());
    };
    if mz.rollout_requested() {
        debug!(
            namespace,
            name, "skipping rollout trigger: a rollout is already in progress"
        );
        return Ok(());
    }

    let force_rollout = Uuid::new_v4();
    info!(
        namespace,
        name,
        %force_rollout,
        ?nodes,
        "triggering rollout: instance has pods on nodes which are being upgraded away",
    );
    // Trigger via the force-rollout annotation rather than a spec field:
    // spec fields are typically managed by infrastructure-as-code tools
    // (e.g. Terraform), which would fight over any value we write there.
    // The annotation feeds into the v1 rollout hash, and this patch goes
    // through the v1 endpoint, so the conversion webhook re-derives the
    // stored `requestRollout` from the new hash when the patch is applied.
    mz_api
        .patch(
            name,
            &PatchParams::default(),
            &Patch::Merge(json!({
                "metadata": {
                    "annotations": {
                        FORCE_ROLLOUT_ANNOTATION: force_rollout,
                    }
                }
            })),
        )
        .await?;
    last_triggered.insert(key, Instant::now());
    Ok(())
}

/// A minimal client for the GCP REST APIs used here (Pub/Sub and GKE),
/// authenticating via Application Default Credentials (in particular, GKE
/// workload identity).
struct GcpApiClient {
    http: reqwest::Client,
    auth: Arc<dyn gcp_auth::TokenProvider>,
}

impl GcpApiClient {
    async fn new() -> Self {
        let auth = loop {
            match gcp_auth::provider().await {
                Ok(auth) => break auth,
                Err(e) => {
                    warn!("failed to initialize GCP credentials, retrying: {e:#}");
                    tokio::time::sleep(Duration::from_secs(10)).await;
                }
            }
        };
        let http = reqwest::Client::builder()
            .timeout(Duration::from_secs(120))
            .build()
            .expect("valid client config");
        Self { http, auth }
    }

    async fn post(
        &self,
        url: &str,
        body: serde_json::Value,
    ) -> Result<serde_json::Value, anyhow::Error> {
        let token = self
            .auth
            .token(&["https://www.googleapis.com/auth/cloud-platform"])
            .await
            .context("fetching GCP auth token")?;
        let response = self
            .http
            .post(url)
            .bearer_auth(token.as_str())
            .json(&body)
            .send()
            .await?;
        let status = response.status();
        if !status.is_success() {
            let body = response.text().await.unwrap_or_default();
            bail!("{url} returned {status}: {body}");
        }
        Ok(response.json().await?)
    }

    async fn get(&self, url: &str) -> Result<serde_json::Value, anyhow::Error> {
        let token = self
            .auth
            .token(&["https://www.googleapis.com/auth/cloud-platform"])
            .await
            .context("fetching GCP auth token")?;
        let response = self
            .http
            .get(url)
            .bearer_auth(token.as_str())
            .send()
            .await?;
        let status = response.status();
        if !status.is_success() {
            let body = response.text().await.unwrap_or_default();
            bail!("{url} returned {status}: {body}");
        }
        Ok(response.json().await?)
    }
}

/// Returns the watched node pools which the GKE API reports as having an
/// upgrade in progress.
async fn poll_gke_for_upgrades(
    gcp: &GcpApiClient,
    config: &Config,
) -> Result<Vec<String>, anyhow::Error> {
    #[derive(Deserialize)]
    #[serde(rename_all = "camelCase")]
    struct NodePoolsResponse {
        #[serde(default)]
        node_pools: Vec<NodePool>,
    }
    #[derive(Deserialize)]
    #[serde(rename_all = "camelCase")]
    struct NodePool {
        name: String,
        #[serde(default)]
        status: String,
        update_info: Option<UpdateInfo>,
    }
    #[derive(Deserialize)]
    #[serde(rename_all = "camelCase")]
    struct UpdateInfo {
        blue_green_info: Option<serde_json::Value>,
    }

    let url = format!(
        "https://container.googleapis.com/v1beta1/projects/{}/locations/{}/clusters/{}/nodePools",
        config.project(),
        config.cluster_location,
        config.cluster_name,
    );
    let response: NodePoolsResponse =
        serde_json::from_value(gcp.get(&url).await?).context("parsing GKE nodePools response")?;

    Ok(response
        .node_pools
        .into_iter()
        .filter(|pool| config.watches_pool(&pool.name))
        .filter(|pool| {
            // RECONCILING covers upgrades in general; blueGreenInfo is
            // present for the whole lifetime of a blue-green upgrade,
            // including the soak phase (when the pool status may have
            // returned to RUNNING but cordoned blue nodes still exist).
            pool.status == "RECONCILING"
                || pool
                    .update_info
                    .as_ref()
                    .is_some_and(|info| info.blue_green_info.is_some())
        })
        .map(|pool| pool.name)
        .collect())
}

/// Pulls GKE cluster notifications from the Pub/Sub subscription forever,
/// arming node pools when they start upgrading.
async fn subscriber_loop(gcp: Arc<GcpApiClient>, config: Config, armed: Arc<Mutex<ArmedPools>>) {
    let base_url = format!(
        "https://pubsub.googleapis.com/v1/{}",
        config.notification_subscription
    );
    loop {
        let response = match gcp
            .post(&format!("{base_url}:pull"), json!({"maxMessages": 100}))
            .await
        {
            Ok(response) => response,
            Err(e) => {
                warn!("failed to pull GKE cluster notifications: {e:#}");
                tokio::time::sleep(Duration::from_secs(30)).await;
                continue;
            }
        };

        #[derive(Deserialize)]
        #[serde(rename_all = "camelCase")]
        struct PullResponse {
            #[serde(default)]
            received_messages: Vec<ReceivedMessage>,
        }
        #[derive(Deserialize)]
        #[serde(rename_all = "camelCase")]
        struct ReceivedMessage {
            ack_id: String,
            message: Option<PubsubMessage>,
        }
        #[derive(Deserialize)]
        #[serde(rename_all = "camelCase")]
        struct PubsubMessage {
            #[serde(default)]
            attributes: BTreeMap<String, String>,
        }

        let response: PullResponse = match serde_json::from_value(response) {
            Ok(response) => response,
            Err(e) => {
                warn!("failed to parse Pub/Sub pull response: {e:#}");
                tokio::time::sleep(Duration::from_secs(30)).await;
                continue;
            }
        };
        if response.received_messages.is_empty() {
            // An empty response indicates the (long) poll timed out without
            // any messages arriving; pull again immediately.
            continue;
        }

        let mut ack_ids = Vec::new();
        for received in response.received_messages {
            ack_ids.push(received.ack_id);
            let Some(message) = received.message else {
                continue;
            };
            if let Some(pool) = upgrading_node_pool(&config, &message.attributes) {
                let mut armed = armed.lock().expect("poisoned");
                armed.arm(&pool, "received a GKE node pool UpgradeEvent notification");
            }
        }

        if let Err(e) = gcp
            .post(
                &format!("{base_url}:acknowledge"),
                json!({"ackIds": ack_ids}),
            )
            .await
        {
            // The messages will be redelivered; arming is idempotent.
            warn!("failed to acknowledge GKE cluster notifications: {e:#}");
        }
    }
}

/// If the given Pub/Sub message attributes describe an `UpgradeEvent` for a
/// watched node pool of our cluster, returns the node pool name.
fn upgrading_node_pool(config: &Config, attributes: &BTreeMap<String, String>) -> Option<String> {
    #[derive(Deserialize)]
    #[serde(rename_all = "camelCase")]
    struct UpgradeEvent {
        #[serde(default)]
        resource_type: String,
        #[serde(default)]
        resource: String,
    }

    if attributes.get("type_url").map(String::as_str)
        != Some("type.googleapis.com/google.container.v1beta1.UpgradeEvent")
    {
        return None;
    }
    // The topic is created per-cluster by our terraform modules, but nothing
    // prevents other clusters from sharing it, so check that the event is
    // for our cluster.
    if attributes.get("cluster_name") != Some(&config.cluster_name)
        || attributes.get("cluster_location") != Some(&config.cluster_location)
    {
        return None;
    }
    let event: UpgradeEvent = match serde_json::from_str(attributes.get("payload")?) {
        Ok(event) => event,
        Err(e) => {
            warn!("failed to parse UpgradeEvent payload: {e:#}");
            return None;
        }
    };
    if event.resource_type != "NODE_POOL" {
        return None;
    }
    // `resource` is of the form
    // projects/{project}/locations/{location}/clusters/{cluster}/nodePools/{pool}.
    let pool = match event.resource.split('/').collect::<Vec<_>>()[..] {
        [_, _, _, _, _, _, "nodePools", pool] => pool.to_owned(),
        _ => {
            warn!(
                resource = event.resource,
                "unexpected resource format in UpgradeEvent"
            );
            return None;
        }
    };
    if !config.watches_pool(&pool) {
        debug!(pool, "ignoring UpgradeEvent for unwatched node pool");
        return None;
    }
    Some(pool)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_config() -> Config {
        Config::new(
            "projects/my-project/subscriptions/my-sub".into(),
            "my-cluster".into(),
            "us-central1".into(),
            vec!["materialize".into()],
        )
        .unwrap()
    }

    fn upgrade_event_attributes(
        cluster_name: &str,
        cluster_location: &str,
        resource_type: &str,
        resource: &str,
    ) -> BTreeMap<String, String> {
        BTreeMap::from_iter([
            (
                "type_url".to_owned(),
                "type.googleapis.com/google.container.v1beta1.UpgradeEvent".to_owned(),
            ),
            ("cluster_name".to_owned(), cluster_name.to_owned()),
            ("cluster_location".to_owned(), cluster_location.to_owned()),
            ("project_id".to_owned(), "1234567890".to_owned()),
            (
                "payload".to_owned(),
                serde_json::to_string(&json!({
                    "resourceType": resource_type,
                    "operation": "operation-1234",
                    "operationStartTime": "2026-07-22T00:00:00Z",
                    "currentVersion": "1.32.1-gke.1",
                    "targetVersion": "1.33.1-gke.1",
                    "resource": resource,
                }))
                .unwrap(),
            ),
        ])
    }

    #[mz_ore::test]
    fn test_config_validation() {
        assert!(
            Config::new("my-sub".into(), "c".into(), "l".into(), vec![]).is_err(),
            "bare subscription names are rejected"
        );
        assert!(
            Config::new(
                "projects//subscriptions/my-sub".into(),
                "c".into(),
                "l".into(),
                vec![]
            )
            .is_err(),
            "empty project is rejected"
        );
        let config = test_config();
        assert_eq!(config.project(), "my-project");
        assert_eq!(
            config.node_pool_url("materialize"),
            "https://container.googleapis.com/v1beta1/projects/my-project/locations/us-central1/clusters/my-cluster/nodePools/materialize",
        );
    }

    #[mz_ore::test]
    fn test_blue_green_phase_parsing() {
        for (json, expected) in [
            ("\"DRAINING_BLUE_POOL\"", BlueGreenPhase::DrainingBluePool),
            ("\"NODE_POOL_SOAKING\"", BlueGreenPhase::NodePoolSoaking),
            ("\"CORDONING_BLUE_POOL\"", BlueGreenPhase::CordoningBluePool),
            (
                "\"WAITING_TO_DRAIN_BLUE_POOL\"",
                BlueGreenPhase::WaitingToDrainBluePool,
            ),
            // Phases from future API versions must not fail parsing.
            ("\"SOME_FUTURE_PHASE\"", BlueGreenPhase::Unknown),
        ] {
            let phase: BlueGreenPhase = serde_json::from_str(json).unwrap();
            assert_eq!(phase, expected, "json: {json}");
        }
    }

    #[mz_ore::test]
    fn test_blue_pool_fully_cordoned() {
        for (phase, expected) in [
            (BlueGreenPhase::UpdateStarted, false),
            (BlueGreenPhase::CreatingGreenPool, false),
            // Cordoning is in progress but not necessarily complete: some
            // blue nodes may still accept pods.
            (BlueGreenPhase::CordoningBluePool, false),
            (BlueGreenPhase::WaitingToDrainBluePool, true),
            (BlueGreenPhase::DrainingBluePool, true),
            (BlueGreenPhase::NodePoolSoaking, true),
            (BlueGreenPhase::DeletingBluePool, true),
            (BlueGreenPhase::RollbackStarted, false),
            (BlueGreenPhase::Unknown, false),
        ] {
            assert_eq!(
                phase.blue_pool_fully_cordoned(),
                expected,
                "phase: {phase:?}"
            );
        }
    }

    #[mz_ore::test]
    fn test_upgrading_node_pool() {
        let config = test_config();
        let pool_resource =
            "projects/my-project/locations/us-central1/clusters/my-cluster/nodePools/materialize";

        // The happy path: a node pool upgrade event for a watched pool.
        assert_eq!(
            upgrading_node_pool(
                &config,
                &upgrade_event_attributes("my-cluster", "us-central1", "NODE_POOL", pool_resource),
            ),
            Some("materialize".to_owned()),
        );

        // Events for other clusters, other resource types, and unwatched
        // pools are ignored.
        assert_eq!(
            upgrading_node_pool(
                &config,
                &upgrade_event_attributes(
                    "other-cluster",
                    "us-central1",
                    "NODE_POOL",
                    pool_resource
                ),
            ),
            None,
        );
        assert_eq!(
            upgrading_node_pool(
                &config,
                &upgrade_event_attributes("my-cluster", "europe-west1", "NODE_POOL", pool_resource),
            ),
            None,
        );
        assert_eq!(
            upgrading_node_pool(
                &config,
                &upgrade_event_attributes(
                    "my-cluster",
                    "us-central1",
                    "MASTER",
                    "projects/my-project/locations/us-central1/clusters/my-cluster",
                ),
            ),
            None,
        );
        assert_eq!(
            upgrading_node_pool(
                &config,
                &upgrade_event_attributes(
                    "my-cluster",
                    "us-central1",
                    "NODE_POOL",
                    "projects/my-project/locations/us-central1/clusters/my-cluster/nodePools/generic",
                ),
            ),
            None,
        );

        // Non-UpgradeEvent notifications are ignored.
        let mut attributes =
            upgrade_event_attributes("my-cluster", "us-central1", "NODE_POOL", pool_resource);
        attributes.insert(
            "type_url".to_owned(),
            "type.googleapis.com/google.container.v1beta1.SecurityBulletinEvent".to_owned(),
        );
        assert_eq!(upgrading_node_pool(&config, &attributes), None);

        // A config with no watched pools watches everything.
        let config = Config::new(
            "projects/my-project/subscriptions/my-sub".into(),
            "my-cluster".into(),
            "us-central1".into(),
            vec![],
        )
        .unwrap();
        assert_eq!(
            upgrading_node_pool(
                &config,
                &upgrade_event_attributes("my-cluster", "us-central1", "NODE_POOL", pool_resource),
            ),
            Some("materialize".to_owned()),
        );
    }
}

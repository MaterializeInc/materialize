---
source: src/orchestratord/src/gcp_node_upgrade.rs
revision: 8dad2dbf43
---

# mz-orchestratord::gcp_node_upgrade

Watches for GKE node pool upgrades and triggers graceful rollouts of Materialize instances before GKE drains the nodes they are running on.

## Problem

GKE automatically upgrades node pools using a blue-green strategy: it creates replacement (green) nodes, cordons the old (blue) nodes, drains them in batches, and deletes them after a soak period. Without intervention this would evict or force-delete `environmentd` and `clusterd` pods. This module moves those pods via Materialize's standard rollout machinery before GKE reaches them.

## Approach

Two concurrent loops run under `run`, each spawned as a separate task so neither can block the other:

**`subscriber_loop`** — pulls GKE cluster notifications from a Pub/Sub subscription. When a message carries an `UpgradeEvent` for a watched node pool, the pool is *armed* (added to `ArmedPools`). Because Pub/Sub messages can be missed while `orchestratord` is down, the subscriber loop alone is not sufficient.

**`scan_loop`** — runs on a configurable interval. At startup and periodically thereafter it also polls the GKE API directly for upgrades in progress (`poll_gke_for_upgrades`), arming any pools not yet armed. For each armed pool, `check_armed_pool` fetches the pool's `updateInfo.blueGreenInfo.phase` from the GKE v1beta1 API. Once the phase reaches `WAITING_TO_DRAIN_BLUE_POOL` or later (all blue nodes are guaranteed to be cordoned), `maybe_trigger_rollout` is called for each Materialize instance whose `environmentd` or `clusterd` pods are scheduled on a cordoned node. A rollout is triggered by patching the `materialize.cloud/force-rollout` annotation on the `v1/Materialize` resource; this feeds the rollout hash without touching spec fields managed by external tools.

A pool is disarmed once `check_armed_pool` finds no blue-green upgrade in progress (upgrade completed or not using blue-green), or after `MAX_ARMED_DURATION` (14 days) as a safety valve.

## Key types

**`Config`** — runtime parameters: Pub/Sub subscription name, GKE cluster name and location, watched node pool names, scan interval, GKE poll interval, and trigger cooldown. Validated at construction; `Config::new` rejects bare subscription names.

**`BlueGreenPhase`** — the blue-green upgrade phase enum from the GKE API. `blue_pool_fully_cordoned()` returns `true` for `WAITING_TO_DRAIN_BLUE_POOL`, `DRAINING_BLUE_POOL`, `NODE_POOL_SOAKING`, and `DELETING_BLUE_POOL`. Unknown future phases (caught by `#[serde(other)]`) return `false`.

**`GcpApiClient`** — minimal GCP REST client authenticating via Application Default Credentials (GKE workload identity). Wraps `reqwest` with a 120-second timeout. Provides `get` and `post` methods that attach bearer tokens and return parsed JSON.

**`ArmedPools`** — shared state tracking which node pools are currently armed and when they were armed.

## Rollout trigger cooldown

`maybe_trigger_rollout` skips a trigger if a rollout is already in progress (visible in the `Materialize` resource status) or if a trigger was issued for the same instance within `trigger_cooldown` (default 5 minutes). The cooldown covers the window between a trigger being issued and the reconciler updating the instance status.

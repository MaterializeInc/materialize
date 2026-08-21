---
title: "GKE node pool upgrades"
description: "Configure graceful Materialize rollouts when GKE upgrades the node pools underneath it."
menu:
  main:
    parent: "deployment-guidelines"
    weight: 50
---

GKE upgrades node pools automatically, for example to roll out new node
images. This cannot be disabled, only delayed with maintenance windows and
exclusions. An upgrade eventually drains the nodes Materialize runs on, and
without coordination that means `environmentd` and `clusterd` pods are
evicted, causing an outage until they reschedule and rehydrate.

The Materialize operator (v26.36.0 and later) can watch for GKE node pool
upgrades and move the pods with the normal rollout machinery before GKE
drains anything. This gives the same minimal-downtime behavior as any other
Materialize rollout instead of an eviction.

{{< note >}}

If you deploy with the [Materialize Terraform
modules](https://github.com/MaterializeInc/materialize-terraform-self-managed)
(v9.0.0 and later), this is configured for you and no action is needed. The
rest of this page describes the setup for deployments that do not use those
modules.

{{< /note >}}

## How it works

The trigger relies on the [blue-green node upgrade
strategy](https://cloud.google.com/kubernetes-engine/docs/concepts/node-pool-upgrade-strategies#blue-green-upgrade-strategy),
where GKE creates a replacement (green) pool, cordons all of the original
(blue) nodes, waits, then drains the blue nodes in batches and finally
deletes them after a soak period. The operator uses that wait window:

1. **Arm.** A GKE `UpgradeEvent` cluster notification, pulled from a Pub/Sub
   subscription, arms a watched node pool. The GKE API is also polled at
   startup and hourly, so a notification missed while the operator was
   restarting does not lose the upgrade.

2. **Gate.** The armed pool's blue-green upgrade phase is polled until it
   reports `WAITING_TO_DRAIN_BLUE_POOL` or later, meaning every blue node has
   been cordoned. Triggering earlier risks scheduling the new generation of
   pods onto a blue node that simply had not been cordoned yet.

3. **Trigger.** Each Materialize instance with `environmentd` or `clusterd`
   pods on the cordoned nodes gets a forced rollout, by way of the
   `materialize.cloud/force-rollout` annotation on the `v1` Materialize
   resource. The new generation can only schedule onto the green nodes, and
   the old generation is torn down gracefully once the new one is ready.
   Instances that already have a rollout in progress are skipped.

Arming on upgrade notifications rather than on any cordon avoids expensive
spurious rollouts when a node is cordoned for reasons that do not mean it is
going away, such as an administrator debugging it.

## Requirements

- Materialize operator and Helm chart v26.36.0 or later.
- The `v1` Materialize CRD (`operator.args.installV1CRD=true`), since
  rollouts are triggered through it. See [Adopting the v1
  CRD](/self-managed-deployments/upgrading/adopting-the-v1-crd/).
- `operator.cloudProvider.type=gcp`.
- [Workload Identity
  Federation](https://cloud.google.com/kubernetes-engine/docs/how-to/workload-identity)
  enabled on the cluster, for the operator's access to the Pub/Sub and GKE
  APIs. The node pool the operator runs on also needs the `GKE_METADATA`
  workload metadata mode, otherwise its pods cannot reach the metadata server
  to fetch credentials.
- A GKE control plane on 1.34.0-gke.2201000 or later, for the autoscaled
  blue-green rollout policy.
- Cluster autoscaling enabled on the node pools running Materialize. GKE
  requires it for the autoscaled rollout policy, which relies on the
  autoscaler to grow the replacement pool.

## Setup

Throughout, replace `CLUSTER_NAME`, `CONTROL_PLANE_LOCATION` (the cluster's
region or zone), `PROJECT_ID`, `NODE_POOL_NAME`, and `OPERATOR_NAMESPACE`
with your own values.

### 1. Put the Materialize node pools on autoscaled blue-green upgrades

The autoscaled rollout policy creates the green pool empty and lets the
cluster autoscaler scale it up as pods move over, so you do not pay for a
duplicate pool for the whole upgrade.

```bash
gcloud container node-pools update NODE_POOL_NAME \
  --cluster=CLUSTER_NAME \
  --location=CONTROL_PLANE_LOCATION \
  --enable-blue-green-upgrade \
  --autoscaled-rollout-policy=wait-for-drain-duration=259200s
```

`wait-for-drain-duration` is how long GKE waits after cordoning the blue
nodes before it starts draining them. This is the window the operator has to
complete its rollouts, so size it against how long a rollout of your largest
instance takes. `259200s` (3 days) is the GKE default and 7 days is the
maximum.

Upgrade settings apply in place, without replacing the pool.

### 2. Publish upgrade notifications to Pub/Sub

Create a topic for the cluster's notifications, and a **pull** subscription
for the operator:

```bash
gcloud pubsub topics create gke-upgrade-notifications --project=PROJECT_ID

gcloud pubsub subscriptions create orchestratord-upgrade-notifications \
  --project=PROJECT_ID \
  --topic=gke-upgrade-notifications \
  --message-retention-duration=86400s \
  --expiration-period=never
```

`--expiration-period=never` matters: node pool upgrades can be weeks apart,
and a subscription that expires from inactivity stops delivering
notifications. Message retention only needs to cover an operator restart,
since the hourly GKE API poll catches anything that expires.

Then point the cluster at the topic, filtered to upgrade events:

```bash
gcloud container clusters update CLUSTER_NAME \
  --location=CONTROL_PLANE_LOCATION \
  --notification-config=pubsub=ENABLED,pubsub-topic=projects/PROJECT_ID/topics/gke-upgrade-notifications,filter="UpgradeEvent"
```

If the topic lives in a different project than the cluster, grant the GKE
service agent
(`service-PROJECT_NUMBER@container-engine-robot.iam.gserviceaccount.com`)
`roles/pubsub.viewer` and `roles/pubsub.publisher` on the topic.

### 3. Grant the operator access to Pub/Sub and the GKE API

Create a GCP service account, grant it the two roles the trigger needs, and
link it to the operator's Kubernetes service account through workload
identity:

```bash
gcloud iam service-accounts create orchestratord --project=PROJECT_ID

SA="orchestratord@PROJECT_ID.iam.gserviceaccount.com"

# Pull the cluster notifications.
gcloud pubsub subscriptions add-iam-policy-binding \
  orchestratord-upgrade-notifications \
  --project=PROJECT_ID \
  --role=roles/pubsub.subscriber \
  --member="serviceAccount:$SA"

# Read node pool upgrade state.
gcloud projects add-iam-policy-binding PROJECT_ID \
  --role=roles/container.clusterViewer \
  --member="serviceAccount:$SA"

# Let the operator's Kubernetes service account impersonate it.
gcloud iam service-accounts add-iam-policy-binding "$SA" \
  --project=PROJECT_ID \
  --role=roles/iam.workloadIdentityUser \
  --member="serviceAccount:PROJECT_ID.svc.id.goog[OPERATOR_NAMESPACE/orchestratord]"
```

The member above uses `orchestratord`, the default
`serviceAccount.name` of the Helm chart. Use your own value if you have
overridden it.

The Kubernetes-side permissions (reading nodes and pods, patching Materialize
resources) are part of the chart's RBAC and need no extra configuration.

### 4. Configure the Helm chart

```yaml
serviceAccount:
  annotations:
    iam.gke.io/gcp-service-account: orchestratord@PROJECT_ID.iam.gserviceaccount.com

operator:
  args:
    installV1CRD: true
  cloudProvider:
    type: gcp
    providers:
      gcp:
        enabled: true
        nodeUpgradeRolloutTrigger:
          enabled: true
          notificationSubscription: "projects/PROJECT_ID/subscriptions/orchestratord-upgrade-notifications"
          clusterName: "CLUSTER_NAME"
          clusterLocation: "CONTROL_PLANE_LOCATION"
          # Empty watches every node pool in the cluster.
          watchedNodePools:
            - "NODE_POOL_NAME"
```

Restrict `watchedNodePools` to the pools that run Materialize workloads.
Watching pools that never host `environmentd` or `clusterd` pools costs
nothing but adds noise.

### 5. Allow egress to the GKE metadata server

If you restrict the operator's egress with network policies, allow it to
reach the metadata server. Workload identity credentials are fetched over
plain HTTP on `169.254.169.254:80`, and under [GKE Dataplane
V2](https://cloud.google.com/kubernetes-engine/docs/concepts/dataplane-v2)
the metadata server also answers on `169.254.169.252:988`, which is the
destination policy is enforced against after DNAT. Allow both, otherwise the
trigger cannot authenticate and logs `no available authentication method
found`.

Plain HTTP is not a concern here. Both addresses are link-local and served by
the `gke-metadata-server` agent running on the pod's own node, so credentials
never travel over the network.

```yaml
apiVersion: networking.k8s.io/v1
kind: NetworkPolicy
metadata:
  name: allow-metadata-server-egress
  namespace: OPERATOR_NAMESPACE
spec:
  podSelector:
    matchLabels:
      app.kubernetes.io/name: materialize-operator
  policyTypes:
    - Egress
  egress:
    - to:
        - ipBlock:
            cidr: 169.254.169.254/32
      ports:
        - protocol: TCP
          port: 80
    - to:
        - ipBlock:
            cidr: 169.254.169.252/32
      ports:
        - protocol: TCP
          port: 988
```

## Verify

Nothing happens until GKE next upgrades a watched pool, so the check after
setup is that the operator started the watcher and authenticated. It logs
`starting GCP node upgrade watcher` at startup, `arming node pool` when it
picks up an upgrade, and `triggering rollout` when it acts on one. A failure
to authenticate is retried and logged as `failed to initialize GCP
credentials`:

```bash
kubectl logs -n OPERATOR_NAMESPACE -l app.kubernetes.io/name=materialize-operator \
  | grep -iE "node upgrade watcher|node pool|triggering rollout|GCP credentials"
```

The polling itself is only logged at debug level, so on a healthy cluster
with no upgrade in flight the startup line is the only output.

During an upgrade, the triggered rollouts are visible on the Materialize
resources and behave like any other rollout:

```bash
kubectl get materialize <instance-name> \
  -n <materialize-instance-namespace> \
  -o jsonpath='{.metadata.annotations.materialize\.cloud/force-rollout}'
kubectl get pods -n <materialize-instance-namespace> -o wide
```

See [Rollout
behavior](/self-managed-deployments/deployment-guidelines/resize-node-pools/#rollout-behavior)
for what to expect. The default `WaitUntilReady` strategy runs both
generations at once, so the green pool needs headroom for the new generation
on top of the old one. With the autoscaled rollout policy the cluster
autoscaler provides it, subject to the pool's `--max-nodes`.

## Limitations

- Only `environmentd` and `clusterd` pods are moved. `balancerd`, the
  console, and other pods are ordinary deployments and stay on the cordoned
  blue nodes until GKE drains them.
- There are no pod disruption budgets for `environmentd` and `clusterd`. A
  pool left on the default `SURGE` upgrade strategy, or an upgrade whose
  wait window elapses before the rollouts finish, will still evict pods.
- Rollouts are triggered through the `v1` Materialize CRD only.
- Instances using the `ManuallyPromote` rollout strategy are not protected
  unless someone promotes the new generation within the wait window. The
  triggered rollout brings the new generation up on the green nodes, but the
  serving generation stays on the cordoned blue nodes until it is promoted.
  An unpromoted rollout is cancelled once it exceeds `rolloutRequestTimeout`
  (24 hours by default), leaving the instance back on the blue nodes, and the
  trigger then requests another rollout, repeating until the upgrade
  finishes. `ImmediatelyPromoteCausingDowntime` moves the pods to the green
  nodes, but with the downtime that strategy always incurs. `WaitUntilReady`
  is the only strategy this feature makes an upgrade transparent under.

## See also

- [GCP deployment
  guidelines](/self-managed-deployments/deployment-guidelines/gcp-deployment-guidelines/)
- [Resize node
  pools](/self-managed-deployments/deployment-guidelines/resize-node-pools/)
- [Operator configuration](/self-managed-deployments/operator-configuration/)

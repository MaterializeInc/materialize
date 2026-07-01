---
title: "Upgrading"
description: "Upgrading Self-Managed Materialize."
disable_list: true
menu:
  main:
    parent: "sm-deployments"
    weight: 30
    identifier: "upgrading"
---

Materialize releases new Self-Managed versions per the schedule outlined in [Release schedule](/releases/schedule/#self-managed-release-schedule).

## General rules for upgrading

{{< include-from-yaml data="self_managed/upgrades"
name="upgrades-general-rules" >}}

{{< note >}}

{{< include-from-yaml data="self_managed/upgrades"
name="upgrade-major-version-restriction" >}}

{{< /note >}}


## Upgrade guides

The following upgrade guides are available as examples:

{{% include-from-yaml data="self_managed/upgrades"
name="upgrade-landing-guides-helm" %}}

{{% include-from-yaml data="self_managed/upgrades"
name="upgrade-landing-guides-unified" %}}

{{% include-from-yaml data="self_managed/upgrades"
name="upgrade-landing-guides-legacy" %}}

## Upgrading the Helm Chart and Materialize Operator

{{< important >}}

When upgrading Materialize, always upgrade the Helm Chart and Materialize
Operator first.

{{</ important >}}

### Update the Helm Chart repository

To update your Materialize Helm Chart repository:

```shell
helm repo update materialize
```

View the available chart versions:

```shell
helm search repo materialize/materialize-operator --versions
```

### Upgrade your Materialize Operator

The Materialize Kubernetes Operator is deployed via Helm and can be updated
through standard `helm upgrade` command:

{{% include-syntax file="self_managed/upgrades"
example="syntax-helm-upgrade-operator" %}}

You can use `helm list` to find your release name. For example, if your Operator
is running in the namespace `materialize`, run `helm list`:

```shell
helm list -n materialize
```

Retrieve the name associated with the `materialize-operator` **CHART**; for
example, `my-demo` in the following helm list:

```none
NAME    	  NAMESPACE  	REVISION	UPDATED                             	STATUS  	CHART                                          APP VERSION
my-demo	materialize	1      2025-12-08 11:39:50.185976 -0500 EST	deployed	materialize-operator-v26.1.0    v26.1.0
```

Then, to upgrade:

```shell
helm upgrade -n materialize my-demo materialize/operator \
  -f my-values.yaml \
  --version {{< self-managed/versions/get-latest-version >}}
```

## Upgrading Materialize Instances

After upgrading the operator, upgrade each Materialize instance to the
operator's **App Version** by updating its `environmentdImageRef`. For
step-by-step instructions, use the [upgrade guide](#upgrade-guides) for your
environment (Kind, AWS, Azure, or GCP). This section explains how instance
rollouts work.

## Rollout configuration

Materialize supports two CRD API versions: `v1alpha1` (default) and `v1`
(available starting in v26.30).

How you trigger a rollout depends on the CRD API version of your instances.

{{< tip >}}
To migrate a `v1alpha1` instance to `v1`, see
[Adopting the v1 CRD](/self-managed-deployments/upgrading/adopting-the-v1-crd/).
{{< /tip >}}

{{< tabs >}}
{{< tab "v1alpha1" >}}

Specify a new `UUID` value for `requestRollout` to roll out changes to the
Materialize instance.

{{< note >}}
`requestRollout` without the `forceRollout` field only rolls out if changes exist
to the Materialize instance. To roll out even if there are no changes to the
instance, use it with `forceRollout`.
{{< /note >}}

```shell
# Only rolls out if there are changes
kubectl patch materialize <instance-name> \
  -n <materialize-instance-namespace> \
  --type='merge' \
  -p "{\"spec\": {\"requestRollout\": \"$(uuidgen)\"}}"
```

To force a rollout even when there are no other changes, set a new `forceRollout`
UUID alongside `requestRollout`:

```shell
kubectl patch materialize <instance-name> \
  -n <materialize-instance-namespace> \
  --type='merge' \
  -p "{\"spec\": {\"requestRollout\": \"$(uuidgen)\", \"forceRollout\": \"$(uuidgen)\"}}"
```

{{< /tab >}}
{{< tab "v1" >}}

With v1, updating the spec automatically triggers a rollout and there is no
`requestRollout` field. For details on the underlying mechanism, see [How it
works](/self-managed-deployments/upgrading/adopting-the-v1-crd/#how-the-switchover-works).

To trigger a rollout even when there are no other changes to the instance,
specify a new `UUID` value for `forceRollout`. That is:

- Set it in your manifest:

  ```yaml
  spec:
    forceRollout: <new-uuid>  # e.g. the output of `uuidgen`
  ```

- Then reapply.

  ```shell
  kubectl apply -f materialize.yaml
  ```

{{< /tab >}}
{{< /tabs >}}

## Rollout strategies

Rollout strategies control how Materialize transitions from the current
generation to a new generation during an upgrade. The behavior follows your
`rolloutStrategy` setting.

### *WaitUntilReady* — ***Default***

`WaitUntilReady` creates a new generation of pods and automatically promotes them
as soon as they catch up to the old generation and become `ReadyToPromote`.
Because both generations run simultaneously until the promotion, this strategy
temporarily doubles the required resources to run Materialize.

{{< note >}}
While the new generation waits to become `ReadyToPromote`, it runs in a
read-only, un-promoted state and holds back compaction. To prevent it from
sitting in this state indefinitely (which can cause incident-inducing load when
it is eventually promoted), the rollout is bounded by the `rolloutRequestTimeout`
field in the Materialize spec, which defaults to `24h`.

If the new generation does not become `ReadyToPromote` within
`rolloutRequestTimeout`, the operator cancels the rollout: the new generation is
torn down and the previously-active generation continues serving. You can then
trigger a new rollout (in v1, set a new `forceRollout`; in v1alpha1, set a new
`requestRollout`).
{{< /note >}}

### *ImmediatelyPromoteCausingDowntime*

{{< warning >}} Using the `ImmediatelyPromoteCausingDowntime` rollout flag will cause downtime. {{< /warning >}}

`ImmediatelyPromoteCausingDowntime` tears down the prior generation and
immediately promotes the new generation without waiting for it to hydrate. This
causes downtime until the new generation has hydrated. However, it does not
require additional resources.

### *ManuallyPromote*

`ManuallyPromote` allows you to choose when to promote the new generation. This
means you can time the promotion for periods when load is low, minimizing the
impact of potential downtime for any clients connected to Materialize. This
strategy temporarily doubles the required resources to run Materialize.

To minimize downtime, wait until the new generation has fully hydrated and caught
up to the prior generation before promoting. To check hydration status, inspect
the `UpToDate` condition in the Materialize resource status. When hydration
completes, the condition will be `ReadyToPromote`.

To promote, update the `forcePromote` field to match the current rollout
identifier (in v1, the `status.requestedRolloutHash`; in v1alpha1, the
`requestRollout` UUID in the spec). If you need to promote before hydration
completes, you can set `forcePromote` immediately, but clients may experience
downtime.

{{< warning >}} Leaving a new generation unpromoted for over 6 hours may cause downtime. {{< /warning >}}

**Do not leave new generations unpromoted indefinitely**. They should either be
promoted or canceled. New generations open a read hold on the metadata database
that prevents compaction. This hold is only released when the generation is
promoted or canceled. If left open too long, promoting or canceling can trigger a
spike in deletion load on the metadata database, potentially causing downtime. It
is not recommended to leave generations unpromoted for over 6 hours.

### *inPlaceRollout* — ***Deprecated*** (v1alpha1 only)

The setting is ignored.

## Verifying the upgrade

After initiating the rollout, you can monitor the status field of the Materialize
custom resource to check on the upgrade.

```shell
# Watch the status of your Materialize environment
kubectl get materialize -n materialize-environment -w

# Check the logs of the operator
kubectl logs -l app.kubernetes.io/name=materialize-operator -n materialize
```

## Cancelling the upgrade

You may want to cancel an in-progress rollout if the upgrade has failed (for
example, new pods are not healthy). Before cancelling, verify that the upgrade has
not already completed by checking that the deploy generation (found via
`status.activeGeneration`) is still the one from before the upgrade. Once an
upgrade has already happened, you cannot revert using this method.

{{< tabs >}}
{{< tab "v1alpha1" >}}

To cancel an in-progress rollout and revert to the last completed rollout state,
revert both `requestRollout` and `environmentdImageRef` back to the values from
the last completed rollout. Reverting `environmentdImageRef` alongside
`requestRollout` keeps the spec aligned with what is actually running, so a later
rollout doesn't accidentally pick up the previously attempted upgrade image.

First, retrieve the last completed rollout request ID and the matching
environmentd image ref from your Materialize CR:

```shell
kubectl get materialize <instance-name> -n materialize-environment \
  -o jsonpath='{.status.lastCompletedRolloutRequest} {.status.lastCompletedRolloutEnvironmentdImageRef}'
```

Then set both fields back to these values in a single patch:

```shell
kubectl patch materialize <instance-name> \
  -n materialize-environment \
  --type='merge' \
  -p "{\"spec\": {\"requestRollout\": \"<lastCompletedRolloutRequest-value>\", \"environmentdImageRef\": \"<lastCompletedRolloutEnvironmentdImageRef-value>\"}}"
```

{{< /tab >}}
{{< tab "v1" >}}

To cancel an in-progress rollout and revert to the last completed rollout state,
reapply the Materialize resource with the spec it had before the rollout (notably
the previous `environmentdImageRef`). Because v1 derives the rollout from the
spec hash, restoring the previous spec produces the previous hash and returns the
instance to the last completed state.

```shell
kubectl apply -f previous_materialize_configuration.yaml
```

{{< /tab >}}
{{< /tabs >}}

## See also

- [Version-specific upgrade
  notes](/self-managed-deployments/upgrading/version-notes/)

- [Adopting the v1
  CRD](/self-managed-deployments/upgrading/adopting-the-v1-crd/)

- [Materialize Operator
  Configuration](/self-managed-deployments/operator-configuration/)

- [Materialize CRD Field
  Descriptions](/self-managed-deployments/materialize-crd-field-descriptions/)

- [Troubleshooting](/self-managed-deployments/troubleshooting/)

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

**After** you have upgraded your Materialize Operator, upgrade your Materialize
instance(s) to the **APP Version** of the Operator. To find the version of your
currently deployed Materialize Operator:

```shell
helm list -n materialize
```

You will use the returned **App Version** for the updated `environmentdImageRef`
value. Specifically, for your Materialize instance(s), set
`environmentdImageRef` value to use the new version:

```
spec:
  environmentdImageRef: docker.io/materialize/environmentd:<app_version>
```

To minimize unexpected downtime and avoid connection drops at critical
periods for your application, the upgrade process involves two steps:

- First, stage the changes (update the `environmentdImageRef` with the new
  version) to the Materialize custom resource. The Operator watches for changes
  but does not automatically roll out the changes.

- Second, roll out the changes by specifying a new UUID for `requestRollout`.

### Stage the Materialize instance version change

To stage the Materialize instances version upgrade, update the
`environmentdImageRef` field in the Materialize custom resource spec to the
compatible version of your currently deployed Materialize Operator.

To stage, but **not** rollout, the Materialize instance version upgrade, you can
use the `kubectl patch` command; for example, if the **App Version** is {{< self-managed/versions/get-latest-version >}}:

```shell
kubectl patch materialize <instance-name> \
  -n <materialize-instance-namespace> \
  --type='merge' \
  -p "{\"spec\": {\"environmentdImageRef\": \"docker.io/materialize/environmentd:{{< self-managed/versions/get-latest-version >}}\"}}"
```

{{< note >}}
Until you specify a new `requestRollout`, the Operator watches for updates but
does not roll out the changes.
{{< /note >}}


### Applying the changes via `requestRollout`

To apply chang Materialize instance upgrade, you must update the `requestRollout` field in the Materialize custom resource spec to a new UUID.
Be sure to consult the [Rollout Configurations](#rollout-configuration) to ensure you've selected the correct rollout behavior.
```shell
# Then trigger the rollout with a new UUID
kubectl patch materialize <instance-name> \
  -n <materialize-instance-namespace> \
  --type='merge' \
  -p "{\"spec\": {\"requestRollout\": \"$(uuidgen)\"}}"
```

### Staging and applying in a single command

Although separating the staging and rollout of the changes into two steps can
minimize unexpected downtime and avoid connection drops at critical periods, you
can, if preferred, combine both operations in a single command

```shell
kubectl patch materialize <instance-name> \
  -n materialize-environment \
  --type='merge' \
  -p "{\"spec\": {\"environmentdImageRef\": \"docker.io/materialize/environmentd:{{< self-managed/versions/get-latest-version >}}\", \"requestRollout\": \"$(uuidgen)\"}}"
```

#### Using YAML Definition

Alternatively, you can update your Materialize custom resource definition directly:

```yaml
apiVersion: materialize.cloud/v1alpha1
kind: Materialize
metadata:
  name: 12345678-1234-1234-1234-123456789012
  namespace: materialize-environment
spec:
  environmentdImageRef: materialize/environmentd:{{< self-managed/versions/get-latest-version >}} # Update version as needed
  requestRollout: 22222222-2222-2222-2222-222222222222    # Use a new UUID
  forceRollout: 33333333-3333-3333-3333-333333333333      # Optional: for forced rollouts
  inPlaceRollout: false                                   # In Place rollout is deprecated and ignored. Please use rolloutStrategy
  rolloutStrategy: WaitUntilReady                         # The mechanism to use when rolling out the new version. Can be WaitUntilReady or ImmediatelyPromoteCausingDowntime
  backendSecretName: materialize-backend
```

Apply the updated definition:

```shell
kubectl apply -f materialize.yaml
```

## Rollout Configuration

### `requestRollout`

Specify a new `UUID` value for the `requestRollout` to roll out the changes to
the Materialize instance.

{{< note >}}

`requestRollout` without the `forcedRollout` field only rolls out if changes
exist to the Materialize instance. To roll out even if there are no changes to
the instance, use with `forcedRollouts`.

{{< /note >}}

```shell
# Only rolls out if there are changes
kubectl patch materialize <instance-name> \
  -n <materialize-instance-namespace> \
  --type='merge' \
  -p "{\"spec\": {\"requestRollout\": \"$(uuidgen)\"}}"
```
#### `requestRollout` with `forcedRollouts`

Specify a new `UUID` value for `forcedRollout` to roll out even when there are
no changes to the instance. Use `forcedRollout` with `requestRollout`.

```shell
kubectl patch materialize <instance-name> \
  -n materialize-environment \
  --type='merge' \
  -p "{\"spec\": {\"requestRollout\": \"$(uuidgen)\", \"forceRollout\": \"$(uuidgen)\"}}"
```

### Rollout strategies

Rollout strategies control how Materialize transitions from the current generation to a new generation during an upgrade.

The behavior of the new version rollout follows your `rolloutStrategy` setting.

#### *WaitUntilReady* - ***Default***

`WaitUntilReady` creates a new generation of pods and automatically cuts over to them as soon as they catch up to the old generation and become `ReadyToPromote`. This strategy temporarily doubles the required resources to run Materialize.
{{< warning >}} `WaitUntilReady` waits up to 72 hours (configurable by the `with_0dt_deployment_max_wait` flag) for the new pods to become ready. If the promotion has not occurred by then, the new pods are automatically promoted. {{< /warning >}}

#### *ImmediatelyPromoteCausingDowntime*
{{< warning >}} Using the `ImmediatelyPromoteCausingDowntime` rollout flag will cause downtime. {{< /warning >}}

`ImmediatelyPromoteCausingDowntime` tears down the prior generation, and immediately promotes the new generation without waiting for it to hydrate. This causes downtime until the new generation has hydrated. However, it does not require additional resources.

#### *ManuallyPromote*

`ManuallyPromote` allows you to choose when to promote the new generation. This means you can time the promotion for periods when load is low, minimizing the impact of potential downtime for any clients connected to Materialize. This strategy temporarily doubles the required resources to run Materialize.

To minimize downtime, wait until the new generation has fully hydrated and caught up to the prior generation before promoting. To check hydration status, inspect the `UpToDate` condition in the Materialize resource status. When hydration completes, the condition will be `ReadyToPromote`.

To promote, update the `forcePromote` field to match the `requestRollout` field in the Materialize spec. If you need to promote before hydration completes, you can set `forcePromote` immediately, but clients may experience downtime.

{{< warning >}} Leaving a new generation unpromoted for over 6 hours may cause downtime. {{< /warning >}}

**Do not leave new generations unpromoted indefinitely**. They should either be promoted or canceled. New generations open a read hold on the metadata database that prevents compaction. This hold is only released when the generation is promoted or canceled. If left open too long, promoting or canceling can trigger a spike in deletion load on the metadata database, potentially causing downtime. It is not recommended to leave generations unpromoted for over 6 hours.

#### *inPlaceRollout* - ***Deprecated***

The setting is ignored.

## Verifying the Upgrade

After initiating the rollout, you can monitor the status field of the Materialize custom resource to check on the upgrade.

```shell
# Watch the status of your Materialize environment
kubectl get materialize -n materialize-environment -w

# Check the logs of the operator
kubectl logs -l app.kubernetes.io/name=materialize-operator -n materialize
```

## Version Specific Upgrade Notes

### Upgrading to `v26.1` and later versions

{{< include-md file="shared-content/self-managed/upgrade-notes/v26.1.md" >}}

### Upgrading to `v26.0`

{{< include-md file="shared-content/self-managed/upgrade-notes/v26.0.md" >}}

### Upgrading between minor versions less than `v26`
 - Prior to `v26`, you must upgrade at most one minor version at a time. For
   example, upgrading from `v25.1.5` to `v25.2.16` is permitted.

## See also

- [Materialize Operator
  Configuration](/self-managed-deployments/operator-configuration/)

- [Materialize CRD Field
  Descriptions](/self-managed-deployments/materialize-crd-field-descriptions/)

- [Troubleshooting](/self-managed-deployments/troubleshooting/)

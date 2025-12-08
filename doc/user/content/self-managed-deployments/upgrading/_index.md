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

## Upgrading guidelines

{{< include-md file="shared-content/self-managed/general-rules-for-upgrades.md" >}}

## Upgrade guides

The following upgrade guides are available:

{{% include-from-yaml data="self_managed/upgrades"
name="upgrade-landing-guides-helm" %}}

{{% include-from-yaml data="self_managed/upgrades"
name="upgrade-landing-guides-legacy" %}}


## Upgrading the Helm Chart and Kubernetes Operator

{{< important >}}

When upgrading Materialize, always upgrade the Operator first.

{{</ important >}}

The Materialize Kubernetes Operator is deployed via Helm and can be updated through standard Helm upgrade commands.

```shell
helm upgrade my-materialize-operator materialize/misc/helm-charts/operator
```

If you have custom values, make sure to include your values file:

```shell
helm upgrade my-materialize-operator materialize/misc/helm-charts/operator -f my-values.yaml
```

## Upgrading Materialize Instances

To minimize unexpected downtime and avoid connection drops at critical
periods for your application, the upgrade process involves two steps:

- First, stage the changes (`environmentdImageRef` with the new version) to the
  Materialize custom resource. The Operator watches for changes but does not
  automatically roll out the changes.

- Second, roll out the changes by specifying a new UUID for `requestRollout`.


### Updating the `environmentdImageRef`

When upgrading your Materialize instances, you'll first want to update the
`environmentdImageRef` field in the Materialize custom resource spec.

To find a compatible version with your currently deployed Materialize Operator, check the `appVersion` in the Helm repository.

```shell
helm list -n materialize
```

Using the returned version, we can construct an image ref.

```
environmentdImageRef: docker.io/materialize/environmentd:{{< self-managed/versions/get-latest-version >}}
```

The following is an example of how to patch the version.
```shell
# For version updates, first update the image reference
kubectl patch materialize <instance-name> \
  -n <materialize-instance-namespace> \
  --type='merge' \
  -p "{\"spec\": {\"environmentdImageRef\": \"materialize/environmentd:{{< self-managed/versions/get-latest-version >}}\"}}"
```

{{< note >}}
Until you specify a new `requestRollout`, the Operator
watches for updates but does not roll out the changes.
{{< /note >}}


### Applying the changes via `requestRollout`

To apply changes and kick off the Materialize instance upgrade, you must update the `requestRollout` field in the Materialize custom resource spec to a new UUID.
Be sure to consult the [Rollout Configurations](#rollout-configuration) to ensure you've selected the correct rollout behavior.
```shell
# Then trigger the rollout with a new UUID
kubectl patch materialize <instance-name> \
  -n <materialize-instance-namespace> \
  --type='merge' \
  -p "{\"spec\": {\"requestRollout\": \"$(uuidgen)\"}}"
```


It is possible to combine both operations in a single command if preferred:

```shell
kubectl patch materialize <instance-name> \
  -n materialize-environment \
  --type='merge' \
  -p "{\"spec\": {\"environmentdImageRef\": \"materialize/environmentd:{{< self-managed/versions/get-latest-version >}}\", \"requestRollout\": \"$(uuidgen)\"}}"
```

### Using YAML Definition

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

### Forced Rollouts

If you need to force a rollout even when there are no changes to the instance:

```shell
kubectl patch materialize <instance-name> \
  -n materialize-environment \
  --type='merge' \
  -p "{\"spec\": {\"requestRollout\": \"$(uuidgen)\", \"forceRollout\": \"$(uuidgen)\"}}"
```

### Rollout strategies

The behavior of the new version rollout follows your `rolloutStrategy` setting:

| `rolloutStrategy` | Description                        |
| ----------------- | -----------------------------------|
| `WaitUntilReady`  | *Default*. New instances are created and all dataflows are determined to be ready before cutover and terminating the old version, temporarily requiring twice the resources during the transition. |
| `ImmediatelyPromoteCausingDowntime`| Tears down the prior version before creating and promoting the new version. This causes downtime equal to the duration it takes for dataflows to hydrate, but does not require additional resources. |
| `inPlaceRollout`| *Deprecated*.  The setting is ignored. |

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
  Configuration](/self-managed-deployments/appendix/configuration/)

- [Materialize CRD Field
  Descriptions](/self-managed-deployments/appendix/materialize-crd-field-descriptions/)

- [Troubleshooting](/self-managed-deployments/troubleshooting/)

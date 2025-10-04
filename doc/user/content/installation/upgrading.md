---
title: "Upgrading"
description: "Upgrading Helm chart and Materialize."
menu:
  main:
    parent: "installation"
draft: true
---

The following provides steps for upgrading the Materialize operator and
Materialize instances. While the operator and instances can be upgraded
independently, you should ensure version compatibility between them.

When upgrading:

- Ensure version compatibility between the Materialize operator and Materialize
  instance. The operator can manage instances within a certain version range
  version range.

- Upgrade the operator first.

- Always upgrade your Materialize instances after upgrading the operator to
  ensure compatibility.

### Upgrading the Helm Chart

{{< important >}}

Upgrade the operator first.

{{</ important >}}

To upgrade the Materialize operator to a new version:

```shell
helm upgrade my-materialize-operator materialize/misc/helm-charts/operator
```

If you have custom values, make sure to include your values file:

```shell
helm upgrade my-materialize-operator materialize/misc/helm-charts/operator -f my-values.yaml
```

### Upgrading Materialize Instances

{{< important >}}

Always upgrade your Materialize instances after upgrading the operator to
ensure compatibility.

{{</ important >}}

To upgrade your Materialize instances, you'll need to update the Materialize custom resource and trigger a rollout.

By default, the operator performs rolling upgrades (`inPlaceRollout: false`) which minimize downtime but require additional Kubernetes cluster resources during the transition. However, keep in mind that rolling upgrades typically take longer to complete due to the sequential rollout process. For environments where downtime is acceptable, you can opt for in-place upgrades (`inPlaceRollout: true`).

#### Determining the Version

The compatible version for your Materialize instances is specified in the Helm chart's `appVersion`. For the installed chart version, you can run:

```shell
helm list -n materialize
```

Or check the `Chart.yaml` file in the `misc/helm-charts/operator` directory:

```yaml
apiVersion: v2
name: materialize-operator
# ...
version: 25.1.0-beta.1
appVersion: v0.125.2  # Use this version for your Materialize instances
```

Use the `appVersion` (`v0.125.2` in this case) when updating your Materialize instances to ensure compatibility.

#### Using `kubectl` patch

For standard upgrades such as image updates:

```shell
# For version updates, first update the image reference
kubectl patch materialize <instance-name> \
  -n <materialize-instance-namespace> \
  --type='merge' \
  -p "{\"spec\": {\"environmentdImageRef\": \"materialize/environmentd:v0.125.2\"}}"

# Then trigger the rollout with a new UUID
kubectl patch materialize <instance-name> \
  -n <materialize-instance-namespace> \
  --type='merge' \
  -p "{\"spec\": {\"requestRollout\": \"$(uuidgen)\"}}"
```

You can combine both operations in a single command if preferred:

```shell
kubectl patch materialize 12345678-1234-1234-1234-123456789012 \
  -n materialize-environment \
  --type='merge' \
  -p "{\"spec\": {\"environmentdImageRef\": \"materialize/environmentd:v0.125.2\", \"requestRollout\": \"$(uuidgen)\"}}"
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
  environmentdImageRef: materialize/environmentd:v0.125.2 # Update version as needed
  requestRollout: 22222222-2222-2222-2222-222222222222    # Generate new UUID
  forceRollout: 33333333-3333-3333-3333-333333333333      # Optional: for forced rollouts
  inPlaceRollout: false                                   # When false, performs a rolling upgrade rather than in-place
  backendSecretName: materialize-backend
```

Apply the updated definition:

```shell
kubectl apply -f materialize.yaml
```

#### Forced Rollouts

If you need to force a rollout even when there are no changes to the instance:

```shell
kubectl patch materialize <instance-name> \
  -n materialize-environment \
  --type='merge' \
  -p "{\"spec\": {\"requestRollout\": \"$(uuidgen)\", \"forceRollout\": \"$(uuidgen)\"}}"
```

The behavior of a forced rollout follows your `inPlaceRollout` setting:
- With `inPlaceRollout: false` (default): Creates new instances before terminating the old ones, temporarily requiring twice the resources during the transition
- With `inPlaceRollout: true`: Directly replaces the instances, causing downtime but without requiring additional resources

### Verifying the Upgrade

After initiating the rollout, you can monitor the status:

```shell
# Watch the status of your Materialize environment
kubectl get materialize -n materialize-environment -w

# Check the logs of the operator
kubectl logs -l app.kubernetes.io/name=materialize-operator -n materialize
```

### Notes on Rollouts

- `requestRollout` triggers a rollout only if there are actual changes to the instance (like image updates)
- `forceRollout` triggers a rollout regardless of whether there are changes, which can be useful for debugging or when you need to force a rollout for other reasons
- Both fields expect UUID values and each rollout requires a new, unique UUID value
- `inPlaceRollout`:
  - When `false` (default): Performs a rolling upgrade by spawning new instances before terminating old ones. While this minimizes downtime, there may still be a brief interruption during the transition.
  - When `true`: Directly replaces existing instances, which will cause downtime.

## See also

- [Configuration](/installation/configuration/)
- [Installation](/installation/)
- [Troubleshooting](/installation/troubleshooting/)

---
title: "Install/Upgrade (Self-Managed)"
description: "Installation and upgrade guides for Self-Managed Materialize."
disable_list: true
menu:
  main:
    identifier: "installation"
    weight: 5

---

{{< include-md file="shared-content/self-managed/install-landing-page.md" >}}

## Upgrade

{{< include-md file="shared-content/self-managed/general-rules-for-upgrades.md" >}}

### Upgrade guides

The following upgrade guides are available:

|               | Notes  |
| ------------- | -------|
| [Upgrade on kind](/installation/install-on-local-kind/upgrade-on-local-kind/) |
| [Upgrade on AWS](/installation/install-on-aws/upgrade-on-aws/) | Uses Materialize provided Terraform |
| [Upgrade on Azure Kubernetes Service (AKS)](/installation/install-on-azure/upgrade-on-azure/) | Uses Materialize provided Terraform |
| [Upgrade on Google Kubernetes Engine (GKE)](/installation/install-on-gcp/upgrade-on-gcp/) | Uses Materialize provided Terraform |


### General notes for upgrades

The following provides some general notes for upgrades. For specific examples,
see the [Upgrade guides](#upgrade-guides)


#### Upgrading the Helm Chart and Kubernetes Operator

{{< important >}}

When upgrading Materialize, always upgrade the operator first.

{{</ important >}}

The Materialize Kubernetes operator is deployed via Helm and can be updated through standard Helm upgrade commands.

```shell
helm upgrade my-materialize-operator materialize/misc/helm-charts/operator
```

If you have custom values, make sure to include your values file:

```shell
helm upgrade my-materialize-operator materialize/misc/helm-charts/operator -f my-values.yaml
```

#### Upgrading Materialize Instances

In order to minimize unexpected downtime and avoid connection drops at critical
periods for your application, changes are not immediately and automatically
rolled out by the Operator. Instead, the upgrade process involves two steps:
- First, staging spec changes to the Materialize custom resource.
- Second, applying the changes via a `requestRollout`.

When upgrading your Materialize instances, you'll first want to update the `environmentdImageRef` field in the Materialize custom resource spec.

##### Updating the `environmentdImageRef`
To find a compatible version with your currently deployed Materialize operator, check the `appVersion` in the Helm repository.

```shell
helm list -n materialize
```

Using the returned version, we can construct an image ref.
We always recommend using the official Materialize image repository
`docker.io/materialize/environmentd`.

```
environmentdImageRef: docker.io/materialize/environmentd:v26.0.0
```

The following is an example of how to patch the version.
```shell
# For version updates, first update the image reference
kubectl patch materialize <instance-name> \
  -n <materialize-instance-namespace> \
  --type='merge' \
  -p "{\"spec\": {\"environmentdImageRef\": \"materialize/environmentd:v26.0.0\"}}"
```

##### Applying the changes via `requestRollout`

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
  -p "{\"spec\": {\"environmentdImageRef\": \"materialize/environmentd:v26.0.0\", \"requestRollout\": \"$(uuidgen)\"}}"
```

##### Using YAML Definition

Alternatively, you can update your Materialize custom resource definition directly:

```yaml
apiVersion: materialize.cloud/v1alpha1
kind: Materialize
metadata:
  name: 12345678-1234-1234-1234-123456789012
  namespace: materialize-environment
spec:
  environmentdImageRef: materialize/environmentd:v26.0.0 # Update version as needed
  requestRollout: 22222222-2222-2222-2222-222222222222    # Generate new UUID
  forceRollout: 33333333-3333-3333-3333-333333333333      # Optional: for forced rollouts
  inPlaceRollout: false                                   # In Place rollout is deprecated and ignored. Please use rolloutStrategy
  rolloutStrategy: WaitUntilReady                         # The mechanism to use when rolling out the new version. Can be WaitUntilReady or ImmediatelyPromoteCausingDowntime
  backendSecretName: materialize-backend
```

Apply the updated definition:

```shell
kubectl apply -f materialize.yaml
```

#### Rollout Configuration

##### Forced Rollouts

If you need to force a rollout even when there are no changes to the instance:

```shell
kubectl patch materialize <instance-name> \
  -n materialize-environment \
  --type='merge' \
  -p "{\"spec\": {\"requestRollout\": \"$(uuidgen)\", \"forceRollout\": \"$(uuidgen)\"}}"
```

##### Rollout Strategies

The behavior of the new version rollout follows your `rolloutStrategy` setting:

`WaitUntilReady` (default):

New instances are created and all dataflows are determined to be ready before cutover and terminating the old version, temporarily requiring twice the resources during the transition.

`ImmediatelyPromoteCausingDowntime`:

Tears down the prior version before creating and promoting the new version. This causes downtime equal to the duration it takes for dataflows to hydrate, but does not require additional resources.

##### In Place Rollout

The `inPlaceRollout` setting has been deprecated and will be ignored.

### Verifying the Upgrade

After initiating the rollout, you can monitor the status field of the Materialize custom resource to check on the upgrade.

```shell
# Watch the status of your Materialize environment
kubectl get materialize -n materialize-environment -w

# Check the logs of the operator
kubectl logs -l app.kubernetes.io/name=materialize-operator -n materialize
```
### Version Specific Upgrade Notes

#### Upgrading to `v26.0`

{{< include-md file="shared-content/self-managed/upgrade-notes/v26.0.md" >}}

#### Upgrading between minor versions less than `v26`
 - Prior to `v26`, you must upgrade at most one minor version at a time. For
   example, upgrading from `v25.1.5` to `v25.2.15` is permitted.

## See also

- [Materialize Operator Configuration](/installation/configuration/)
- [Troubleshooting](/installation/troubleshooting/)
- [FAQ](/installation/faq/)

---
title: "v1alpha2"
description: "Upgrading Self-Managed Materialize instances using the v1alpha2 CRD API (v26.17+)."
menu:
  main:
    parent: "upgrading-materialize-instances"
    identifier: "upgrading-v1alpha2"
    weight: 10
---

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

#### Using `kubectl patch`

To initiate the Materialize instance version upgrade, you can
use the `kubectl patch` command; for example, if the **App Version** is {{< self-managed/versions/get-latest-version >}}:

```shell
kubectl patch materialize <instance-name> \
  -n <materialize-instance-namespace> \
  --type='merge' \
  -p "{\"spec\": {\"environmentdImageRef\": \"docker.io/materialize/environmentd:{{< self-managed/versions/get-latest-version >}}\"}}"
```


#### Using YAML Definition

Alternatively, you can update your Materialize custom resource definition directly:

```yaml
apiVersion: materialize.cloud/v1alpha2
kind: Materialize
metadata:
  name: 12345678-1234-1234-1234-123456789012
  namespace: materialize-environment
spec:
  environmentdImageRef: materialize/environmentd:{{< self-managed/versions/get-latest-version >}} # Update version as needed
  forceRollout: 33333333-3333-3333-3333-333333333333      # Optional: for forced rollouts
  rolloutStrategy: WaitUntilReady                         # The mechanism to use when rolling out the new version. Can be WaitUntilReady or ImmediatelyPromoteCausingDowntime
  backendSecretName: materialize-backend
```

Apply the updated definition:

```shell
kubectl apply -f materialize.yaml
```

## Rollout Configuration

#### `forceRollout`

Specify a new `UUID` value for `forceRollout` to roll out even when there are
no other changes to the instance.

```shell
kubectl patch materialize <instance-name> \
  -n materialize-environment \
  --type='merge' \
  -p "{\"spec\": {\"forceRollout\": \"$(uuidgen)\"}}"
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

To promote, update the `forcePromote` field to match the `requestedRolloutHash` field in the Materialize status. If you need to promote before hydration completes, you can set `forcePromote` immediately, but clients may experience downtime.

{{< warning >}} Leaving a new generation unpromoted for over 6 hours may cause downtime. {{< /warning >}}

**Do not leave new generations unpromoted indefinitely**. They should either be promoted or canceled. New generations open a read hold on the metadata database that prevents compaction. This hold is only released when the generation is promoted or canceled. If left open too long, promoting or canceling can trigger a spike in deletion load on the metadata database, potentially causing downtime. It is not recommended to leave generations unpromoted for over 6 hours.


## Verifying the Upgrade

After initiating the rollout, you can monitor the status field of the Materialize custom resource to check on the upgrade.

```shell
# Watch the status of your Materialize environment
kubectl get materialize -n materialize-environment -w

# Check the logs of the operator
kubectl logs -l app.kubernetes.io/name=materialize-operator -n materialize
```

## Cancelling the Upgrade

You may want to cancel an in-progress rollout if the upgrade has failed. This may be indicated by new pods not being healthy. Before cancelling, verify that the upgrade has not already completed by checking that the deploy generation (found via `status.activeGeneration`) is still the one from before the upgrade. Once an upgrade has already happened, you cannot revert using this method.

To cancel an in-progress rollout and revert to the last completed rollout state, you must revert the Materialize resource to the contents it had before triggering the rollout:

```shell
kubectl apply -f previous_materialize_configuration.yaml'
```

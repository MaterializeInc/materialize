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

After upgrading the operator, upgrade each Materialize instance to the operator's
**App Version** by updating its `environmentdImageRef`. For step-by-step
instructions, use the [upgrade guide](#upgrade-guides) for your environment
(Kind, AWS, Azure, or GCP). This section describes the rollout behavior those
guides rely on.

### CRD API versions

Starting in v26.30, the Materialize Operator supports two CRD API versions:

- **v1alpha1** (default) uses a two-step rollout: first stage the spec change,
  then trigger a rollout with a new `requestRollout` UUID.
- **v1** simplifies upgrades — rollouts trigger automatically when spec fields
  change, removing the need to manually set a `requestRollout` UUID.

Adopting v1 is **opt-in** for now. Upgrading the operator to v26.30+ does not
change your existing v1alpha1 CRs or their behavior; you can continue using
v1alpha1 and adopt v1 for individual instances at your own pace.

{{< important >}}
In the next major release, all Materialize CRs will be force upgraded to v1. You
will still be able to apply v1alpha1 CRs, but they will be auto-converted to v1
and use the v1 rollout behavior. We recommend opting in to v1 at your convenience
to migrate on your own schedule before the upgrade is mandatory.
{{< /important >}}

## Adopting the v1 CRD

Adopting v1 is opt-in and does not trigger a rollout on its own. Before you
begin, ensure you have completed the prerequisites in the [v26.30 upgrade
notes](#upgrading-to-v2630-and-later-versions) (cert-manager, network/firewall
changes), and have enabled the v1 CRD by setting the Helm value
`operator.args.installV1CRD=true` on the operator. Without this value, the
operator only installs the v1alpha1 CRD version, and the Kubernetes API server
rejects v1 CRs.

### How it works

The v1alpha1 CRD remains the storage version. When you submit a v1 CR, the
operator's conversion webhook automatically converts it to v1alpha1 for storage.
During conversion, the webhook computes a SHA256 hash of a subset of the spec
fields and derives a deterministic `requestRollout` UUID from it. The hash covers
the fields that affect the running `environmentd` (for example,
`environmentdImageRef`, `environmentdExtraArgs`, `environmentdExtraEnv`, resource
requirements, `podAnnotations`, `podLabels`, `authenticatorKind`, `enableRbac`,
`rolloutStrategy`, and `forceRollout`). It excludes fields that do not require a
rollout, such as `balancerd`/`console` resource requirements and replica counts.
The operator triggers a rollout whenever `requestRollout` differs from the last
completed rollout, so:

- **Adopting v1 on an existing v1alpha1 instance typically triggers one
  rollout.** The derived `requestRollout` is computed from the spec hash and will
  not match the `requestRollout` you previously set by hand, so the instance
  rolls out once even if nothing else in the spec changed. Adopt v1 during a
  window where a rollout is acceptable.
- **Once on v1, an unchanged spec does not trigger a rollout.** Reapplying the
  same spec produces the same hash and the same derived `requestRollout`.
  Changing a hashed spec field produces a new value and triggers a rollout
  automatically.

### Using kubectl

To adopt v1 for an existing instance, apply your CR with `apiVersion:
materialize.cloud/v1` and remove the `requestRollout` field:

```shell
kubectl apply -f - <<EOF
apiVersion: materialize.cloud/v1
kind: Materialize
metadata:
  name: <instance-name>
  namespace: <materialize-instance-namespace>
spec:
  environmentdImageRef: <current-image-ref>
  backendSecretName: <backend-secret-name>
  # ... other spec fields (copy from your existing CR, removing requestRollout)
EOF
```

### Using Terraform

If you are managing your Materialize instance with the [Materialize Terraform
modules](https://github.com/MaterializeInc/materialize-terraform-self-managed),
set:

```hcl
crd_version     = "v1"
request_rollout = null
```

### Returning to the v1alpha1 behavior

You can go back to the v1alpha1 rollout behavior at any time by applying your CR
with `apiVersion: materialize.cloud/v1alpha1` and an explicit `requestRollout`
UUID.

## Rollout configuration

How you trigger a rollout depends on your CRD API version.

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

With v1, the operator computes a hash of the spec fields and automatically
triggers a rollout when the hash changes — there is no `requestRollout` field.

Specify a new `UUID` value for `forceRollout` to trigger a rollout even when
there are no other changes to the instance. The `forceRollout` value is included
in the hash, so changing it produces a new hash and triggers a rollout. Set it in
your manifest and reapply:

```yaml
spec:
  forceRollout: <new-uuid>  # e.g. the output of `uuidgen`
```

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

## Version Specific Upgrade Notes

### Upgrading to `v26.30` and later versions

{{< include-md file="shared-content/self-managed/upgrade-notes/v26.30.md" >}}

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

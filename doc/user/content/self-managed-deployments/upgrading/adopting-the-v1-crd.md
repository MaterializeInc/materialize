---
title: "Adopting the v1 CRD"
description: "Adopt the v1 Materialize CRD API version for Self-Managed Materialize."
menu:
  main:
    parent: "upgrading"
    weight: 80
---

This page describes the Materialize CRD API versions and how to adopt `v1` for
your Materialize instances.

## CRD API versions

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

## Prerequisites

Before you begin, ensure you have completed the prerequisites in the [v26.30
upgrade
notes](/self-managed-deployments/upgrading/version-notes/#upgrading-to-v2630-and-later-versions)
(cert-manager, network/firewall changes), and have enabled the v1 CRD by setting
the Helm value `operator.args.installV1CRD=true` on the operator. Enabling the
v1 CRD alone does not roll out your existing instances. Without this value, the
operator only installs the v1alpha1 CRD version, and the Kubernetes API server
rejects v1 CRs.

## How it works

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

## Using kubectl

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

## Using Terraform

If you are managing your Materialize instance with the [Materialize Terraform
modules](https://github.com/MaterializeInc/materialize-terraform-self-managed),
set:

```hcl
crd_version     = "v1"
request_rollout = null
```

## Returning to the v1alpha1 behavior

You can go back to the v1alpha1 rollout behavior at any time by applying your CR
with `apiVersion: materialize.cloud/v1alpha1` and an explicit `requestRollout`
UUID.

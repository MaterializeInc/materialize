---
title: "Upgrading Materialize Instances"
description: "Upgrading Materialize instances for Self-Managed deployments."
disable_list: true
menu:
  main:
    parent: "upgrading"
    identifier: "upgrading-materialize-instances"
    weight: 20
---

{{< important >}}

When upgrading Materialize, always upgrade the Helm Chart and Materialize
Operator first. See [Upgrading the Helm Chart and Materialize Operator](/self-managed-deployments/upgrading/#upgrading-the-helm-chart-and-materialize-operator).

{{</ important >}}

## CRD API Versions

Starting in v26.30, the Materialize Operator supports two CRD API versions:

- **v1** simplifies the upgrade process. Rollouts trigger automatically when spec fields change, removing the need to manually set a `requestRollout` UUID.
- **v1alpha1** uses the original two-step upgrade process: first stage changes, then trigger a rollout with a new `requestRollout` UUID.

Switching to v1 is **opt-in** for now. Upgrading the operator to v26.30+ does not change your existing v1alpha1 CRs or their behavior. You can continue using v1alpha1 until the next major release, switching individual instances to v1 at your own pace.

{{< important >}}
In the next major release, all Materialize CRs will be force upgraded to v1. You will still be able to apply v1alpha1 CRs, but they will be auto-converted to v1 and use the v1 rollout behavior. We recommend opting in to v1 at your convenience to migrate on your own schedule before the upgrade is mandatory.
{{</ important >}}

Select the instructions for your CRD API version:

- [v1 (v26.30+)](/self-managed-deployments/upgrading/materialize-instances/v1/)
- [v1alpha1 (before v26.30)](/self-managed-deployments/upgrading/materialize-instances/v1alpha1/)

## Switching from v1alpha1 to v1

Switching to v1 is opt-in and does not trigger a rollout on its own. Before switching, ensure you have completed the prerequisites in the [v26.30 upgrade notes](/self-managed-deployments/upgrading/#upgrading-to-v2630-and-later-versions) (cert-manager, network/firewall changes), and have enabled the v1 CRD by setting the Helm value `operator.args.installV1CRD=true` on the operator. Without this value, the operator only installs the v1alpha1 CRD version, and the Kubernetes API server rejects v1 CRs.

### How it works

The v1alpha1 CRD remains the storage version. When you submit a v1 CR, the operator's conversion webhook automatically converts it to v1alpha1 for storage. During conversion, the webhook computes a SHA256 hash of the spec and derives a deterministic `requestRollout` UUID from it. This means:

- If the spec hasn't changed, the same UUID is generated, so **no unintended rollout is triggered** by switching API versions alone.
- If the spec has changed, a different UUID is produced, automatically triggering a rollout.

### Using kubectl

To switch an existing instance to v1, apply your CR with the updated `apiVersion` and remove the `requestRollout` field:

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

Or patch the API version on an existing CR:

```shell
kubectl patch materialize <instance-name> \
  -n <materialize-instance-namespace> \
  --type='merge' \
  -p '{"apiVersion":"materialize.cloud/v1"}'
```

### Using Terraform

If you are managing your Materialize instance with the [Materialize Terraform modules](https://github.com/MaterializeInc/materialize-terraform-self-managed), set:

```hcl
crd_version     = "v1"
request_rollout = null
```

### Switching back to v1alpha1

You can switch back to v1alpha1 at any time by reapplying your CR with `apiVersion: materialize.cloud/v1alpha1` and an explicit `requestRollout` UUID.

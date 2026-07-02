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

Starting in v26.30, Materialize introduces support for a new version of the
Materialize CRD, `v1`, which provides simplified rollouts. Previously,
Materialize only supported `v1alpha1`; `v1alpha1` remains the default.

- **v1alpha1** (default) uses a two-step rollout: first stage the spec
  change, then trigger a rollout with a new `requestRollout` UUID.

  ```yaml
  apiVersion: materialize.cloud/v1alpha1
  kind: Materialize
  metadata:
    name: 12345678-1234-1234-1234-123456789012
    namespace: materialize-environment
  spec:
    environmentdImageRef: materialize/environmentd:v26.30.1
    requestRollout: 22222222-2222-2222-2222-222222222222  # ← MUST set a new UUID every upgrade
  # forceRollout: 33333333-3333-3333-3333-333333333333   # ← for forced rollouts
    rolloutStrategy: WaitUntilReady
    backendSecretName: materialize-backend
  ```
- **v1** rolls out automatically when spec fields change, removing the
  need to manually set a `requestRollout` UUID.

  ```yaml
  apiVersion: materialize.cloud/v1
  kind: Materialize
  metadata:
    name: 12345678-1234-1234-1234-123456789012
    namespace: materialize-environment
  spec:
    environmentdImageRef: materialize/environmentd:v26.30.1  # ← just change this
  # forceRollout: 33333333-3333-3333-3333-333333333333       # ← only for forced rollouts
    rolloutStrategy: WaitUntilReady
    backendSecretName: materialize-backend
  ```

Adopting `v1` is **opt-in** for now. Upgrading the operator to v26.30+ does not
change your existing `v1alpha1` CRs or their behavior; you can continue using
`v1alpha1` until the next major release.

{{< important >}}

In the next major release, all Materialize CRs will be force upgraded to `v1`.
You will still be able to apply `v1alpha1` CRs, but they will be auto-converted
to `v1` and use the `v1` rollout behavior. We recommend opting in to `v1` at
your convenience to migrate on your own schedule before the upgrade is
mandatory. With the new change, the `requestRollout` field will be removed,
along with all previously deprecated fields.

{{< /important >}}

## Prerequisites

- First, set up infrastructure requirements (needed by conversion webhooks to
  allow for graceful migration from `v1alpha1` to `v1`)
  - Install `cert-manager` (or provide your own certificate).
  - Allow internal network ingress on port `8001`.

- Next, enable the `v1` CRD by setting the Helm value
  `operator.args.installV1CRD=true`. Enabling the `v1` CRD does not roll out
  your existing instances; they continue to use `v1alpha1`.

For instructions on completing the prerequisites, select the tab that matches
your deployment method:

{{< tabs >}}
{{< tab "Supported Terraform" >}}

If you are using the [supported Terraform
modules](https://github.com/MaterializeInc/materialize-terraform-self-managed),
the required infrastructure changes (cert-manager and network ingress) and
enabling of `v1` CRD will be handled for you automatically starting in TF
modules (**v3.1.1 or greater**).

- If you have already upgraded your TF modules to **v3.1.1 or greater**, the
  prerequisites are handled automatically.

- If you are on earlier TF modules, use the same [procedure to perform version
  upgrades](/self-managed-deployments/upgrading/#upgrade-using-the-new-terraform-modules)
  to upgrade to **v3.1.1 or greater**; i.e., update each module's `source` to
  point to the new release tag (v3.1.1 or greater), then run `terraform init &&
  terraform plan && terraform apply`.

  This will also upgrade your Materialize version to that associated with that
  TF release tag.


  - [Upgrade on AWS](/self-managed-deployments/upgrading/upgrade-on-aws/)
  - [Upgrade on Azure](/self-managed-deployments/upgrading/upgrade-on-azure/)
  - [Upgrade on GCP](/self-managed-deployments/upgrading/upgrade-on-gcp/)

{{< /tab >}}
{{< tab "Legacy Terraform" >}}

If you are using the legacy Terraform modules
([AWS](https://github.com/MaterializeInc/terraform-aws-materialize),
[GCP](https://github.com/MaterializeInc/terraform-gcp-materialize), or
[Azure](https://github.com/MaterializeInc/terraform-azure-materialize)),
we recommend migrating to the [new supported Terraform
modules](https://github.com/MaterializeInc/materialize-terraform-self-managed)
before opting in to the `v1` CRD.

The new modules include built-in support for the conversion webhooks used by
the `v1` CRD, including cert-manager installation and network policy
configuration. The legacy modules do not include these changes, so you would
need to apply them manually (see the **Manual** tab).

For migration guidance, see the documentation for your cloud provider:

- [AWS migration guide](https://github.com/MaterializeInc/materialize-terraform-self-managed/tree/main/aws/examples/migration)
- [GCP migration guide](https://github.com/MaterializeInc/materialize-terraform-self-managed/tree/main/gcp/examples/migration)
- [Azure migration guide](https://github.com/MaterializeInc/materialize-terraform-self-managed/tree/main/azure/examples/migration)

{{< /tab >}}
{{< tab "Manual" >}}

If you are not using our Terraform modules, you **must** complete the following
steps before enabling the `v1` CRD:

**1. Install cert-manager**

The conversion webhook requires a TLS certificate.
The Helm chart defaults to using [cert-manager](https://cert-manager.io/)
to automatically create and manage this certificate. cert-manager must be
installed **before** enabling the `v1` CRD.

If you prefer to provide your own certificate instead of using cert-manager,
set the following Helm values:
- `operator.certificate.source`: `secret`
- `operator.certificate.secretName`: the name of the Kubernetes Secret
  containing `ca.crt`, `tls.crt`, and `tls.key` entries.

**2. Allow network access to the webhook port**

The conversion webhooks require the Kubernetes API server to reach the
`orchestratord` pod on port `8001`. If your cluster enforces network policies
or cloud-level firewall rules, you must allow ingress traffic on TCP port
`8001` from the API server to pods with the label
`app.kubernetes.io/name: materialize-operator`.

**Kubernetes NetworkPolicy:** Add a policy that allows ingress from the
Kubernetes API server on port `8001` to the `materialize-operator` pods in the
namespace where the operator is deployed:

```yaml
apiVersion: networking.k8s.io/v1
kind: NetworkPolicy
metadata:
  name: allow-api-server-ingress-to-conversion-webhook
  namespace: materialize  # the namespace where the operator runs
spec:
  podSelector:
    matchLabels:
      app.kubernetes.io/name: materialize-operator
  policyTypes:
    - Ingress
  ingress:
    - ports:
        - protocol: TCP
          port: 8001
```

**Cloud firewall rules (e.g., AWS security groups, GCP firewall rules):**
Ensure the node security group or firewall allows inbound TCP traffic on
port `8001` from the Kubernetes control plane. For example, on AWS, add an
ingress rule to the EKS node security group allowing port `8001` from the
cluster security group. On GCP with private clusters, add a firewall rule
allowing port `8001` from the GKE control plane CIDR.

For a complete example of the required changes across AWS, Azure, and GCP,
see [this pull request](https://github.com/MaterializeInc/materialize-terraform-self-managed/pull/160).

**3. Enable the v1 CRD**

Once the prerequisites above are in place, set the following Helm value when
installing or upgrading the operator:

```yaml
operator:
  args:
    installV1CRD: true
```

This installs the `v1` version of the Materialize CRD and the conversion
webhook that converts between `v1` and `v1alpha1`.

{{< /tab >}}
{{< /tabs >}}

## Switch to `v1`

After you have fulfilled the pre-requisites (Materialize v26.30+, set up
infrastructure requirements, enabled `v1` CRD), you can submit a `v1` CR to
adopt `v1`.

{{< important >}}

Schedule `v1` adoption during a window where a rollout is acceptable.
{{< /important >}}

### How the switchover works

When you submit a `v1` CR, the operator's conversion webhook automatically
converts it to `v1alpha1` for storage. During conversion, the webhook computes a
SHA256 hash of a subset of the spec fields and derives a deterministic
`requestRollout` UUID from it. The hash covers the fields that affect the
running `environmentd` (for example, `environmentdImageRef`,
`environmentdExtraArgs`, `environmentdExtraEnv`, resource requirements,
`podAnnotations`, `podLabels`, `authenticatorKind`, `enableRbac`,
`rolloutStrategy`, and `forceRollout`). It excludes fields that do not require a
rollout, such as `balancerd`/`console` resource requirements and replica counts.

{{< important >}}

Schedule `v1` adoption during a window where a rollout is acceptable. Adopting
`v1` on an existing `v1alpha1` instance typically triggers a rollout. The
derived `requestRollout` is computed from the spec hash and will not match the
`requestRollout` you previously set by hand, so the instance rolls out once even
if nothing else in the spec changed.

{{< /important >}}

{{< tabs >}}
{{< tab "Supported Terraform" >}}

If you are managing your Materialize instance with the [Materialize Terraform
modules](https://github.com/MaterializeInc/materialize-terraform-self-managed),
set:

```hcl
crd_version     = "v1"
request_rollout = null
```

**Once on v1, an unchanged spec will not trigger a rollout.** Reapplying the
same spec produces the same hash and the same derived `requestRollout`. Changing
a hashed spec field produces a new value and triggers a rollout automatically.
{{< /tab >}}

{{< tab "Manual" >}}

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

**Once on v1, an unchanged spec will not trigger a rollout.** Reapplying the
same spec produces the same hash and the same derived `requestRollout`. Changing
a hashed spec field produces a new value and triggers a rollout automatically.

{{< /tab >}}
{{< /tabs >}}

## Returning to the v1alpha1 behavior

You can go back to the `v1alpha1` rollout behavior at any time by applying your
CR with `apiVersion: materialize.cloud/v1alpha1` and an explicit
`requestRollout` UUID.

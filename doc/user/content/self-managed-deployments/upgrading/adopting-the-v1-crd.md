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

To adopt v1, you must be on Materialize v26.30+.

In addition, you must:

- First, set up infrastructure requirements:
  - Install `cert-manager` (or provide your own certificate).
  - Allow internal network ingress on port `8001`.

- Then, enable the `v1` CRD by setting the Helm value
  `operator.args.installV1CRD=true`.

Materialize uses conversion webhooks to allow you to gracefully migrate from
`v1alpha1` to `v1`. Enabling the `v1` CRD alone does not roll out your existing
instances; they continue to use `v1alpha1`. Without the `v1` CRD, the operator
installs only the `v1alpha1` CRD version and the Kubernetes API server rejects
`v1` CRs.

Choose the tab that matches your deployment method:

<div class="code-tabs">
<ul class="nav-tabs"></ul>
<div class="tab-content">
<div class="tab-pane" title="Supported Terraform" id="upgrade-supported-terraform">

If you are using the [supported Terraform
modules](https://github.com/MaterializeInc/materialize-terraform-self-managed),
the required infrastructure changes (cert-manager and network ingress) will be
handled for you automatically.

Update each module's `source` to point to the new release tag (v3.1.0 or
greater), then run `terraform init && terraform plan && terraform apply`. To
enable the `v1` CRD, set the Helm value `operator.args.installV1CRD=true` in the
values passed to the operator module.

The key modules and their dependency chain are shown in the tabs below. Your
configuration may include additional modules (networking, storage, database,
node pools, etc.); update those to the same release tag as well.

<div class="code-tabs">
<ul class="nav-tabs"></ul>
<div class="tab-content">
<div class="tab-pane" title="AWS" id="upgrade-supported-terraform-aws">

```hcl
module "eks" {
  source = "github.com/MaterializeInc/materialize-terraform-self-managed//aws/modules/eks?ref=<RELEASE_TAG>"
  # ... your existing configuration ...
}

module "cert_manager" {
  source = "github.com/MaterializeInc/materialize-terraform-self-managed//kubernetes/modules/cert-manager?ref=<RELEASE_TAG>"
  # ... your existing configuration ...

  # Your configuration may have additional dependencies here.
  depends_on = [module.eks]
}

module "operator" {
  source = "github.com/MaterializeInc/materialize-terraform-self-managed//aws/modules/operator?ref=<RELEASE_TAG>"
  # ... your existing configuration ...

  # Your configuration may have additional dependencies here.
  depends_on = [module.cert_manager]
}

module "materialize_instance" {
  source = "github.com/MaterializeInc/materialize-terraform-self-managed//kubernetes/modules/materialize-instance?ref=<RELEASE_TAG>"
  # ... your existing configuration ...

  # Your configuration may have additional dependencies here.
  depends_on = [module.operator]
}
```

For a complete example, see
[`aws/examples/simple/main.tf`](https://github.com/MaterializeInc/materialize-terraform-self-managed/blob/main/aws/examples/simple/main.tf).

</div>
<div class="tab-pane" title="GCP" id="upgrade-supported-terraform-gcp">

```hcl
module "gke" {
  source = "github.com/MaterializeInc/materialize-terraform-self-managed//gcp/modules/gke?ref=<RELEASE_TAG>"
  # ... your existing configuration ...
}

module "cert_manager" {
  source = "github.com/MaterializeInc/materialize-terraform-self-managed//kubernetes/modules/cert-manager?ref=<RELEASE_TAG>"
  # ... your existing configuration ...

  # Your configuration may have additional dependencies here.
  depends_on = [module.gke]
}

module "operator" {
  source = "github.com/MaterializeInc/materialize-terraform-self-managed//gcp/modules/operator?ref=<RELEASE_TAG>"
  # ... your existing configuration ...

  # Your configuration may have additional dependencies here.
  depends_on = [module.cert_manager]
}

module "materialize_instance" {
  source = "github.com/MaterializeInc/materialize-terraform-self-managed//kubernetes/modules/materialize-instance?ref=<RELEASE_TAG>"
  # ... your existing configuration ...

  # Your configuration may have additional dependencies here.
  depends_on = [module.operator]
}
```

For a complete example, see
[`gcp/examples/simple/main.tf`](https://github.com/MaterializeInc/materialize-terraform-self-managed/blob/main/gcp/examples/simple/main.tf).

</div>
<div class="tab-pane" title="Azure" id="upgrade-supported-terraform-azure">

```hcl
module "aks" {
  source = "github.com/MaterializeInc/materialize-terraform-self-managed//azure/modules/aks?ref=<RELEASE_TAG>"
  # ... your existing configuration ...
}

module "cert_manager" {
  source = "github.com/MaterializeInc/materialize-terraform-self-managed//kubernetes/modules/cert-manager?ref=<RELEASE_TAG>"
  # ... your existing configuration ...

  # Your configuration may have additional dependencies here.
  depends_on = [module.aks]
}

module "operator" {
  source = "github.com/MaterializeInc/materialize-terraform-self-managed//azure/modules/operator?ref=<RELEASE_TAG>"
  # ... your existing configuration ...

  # Your configuration may have additional dependencies here.
  depends_on = [module.cert_manager]
}

module "materialize_instance" {
  source = "github.com/MaterializeInc/materialize-terraform-self-managed//kubernetes/modules/materialize-instance?ref=<RELEASE_TAG>"
  # ... your existing configuration ...

  # Your configuration may have additional dependencies here.
  depends_on = [module.operator]
}
```

For a complete example, see
[`azure/examples/simple/main.tf`](https://github.com/MaterializeInc/materialize-terraform-self-managed/blob/main/azure/examples/simple/main.tf).

</div>
</div>
</div>
</div>
<div class="tab-pane" title="Legacy Terraform" id="upgrade-legacy-terraform">

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

</div>
<div class="tab-pane" title="Manual" id="upgrade-manual">

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

</div>
</div>
</div>

## Submit a `v1` CR to adopt `v1`

After you have fulfilled the pre-requisites (Materialize v26.30+, set up
infrastructure requirements, enabled `v1` CRD), you can submit a `v1` CR to
adopt `v1`.

{{< important >}}

Schedule `v1` apdoption during a window where a rollout is acceptable.
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

Schedule `v1` apdoption during a window where a rollout is acceptable. Adopting
`v1` on an existing `v1alpha1` instance typically triggers a rollout. The
derived `requestRollout` is computed from the spec hash and will not match the
`requestRollout` you previously set by hand, so the instance rolls out once even
if nothing else in the spec changed.

{{< /important >}}

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

**Once on v1, an unchanged spec will not trigger a rollout.** Reapplying the
same spec produces the same hash and the same derived `requestRollout`. Changing
a hashed spec field produces a new value and triggers a rollout automatically.

### Using Terraform

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

## Returning to the v1alpha1 behavior

You can go back to the v1alpha1 rollout behavior at any time by applying your CR
with `apiVersion: materialize.cloud/v1alpha1` and an explicit `requestRollout`
UUID.

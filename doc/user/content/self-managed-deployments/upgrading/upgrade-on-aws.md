---
title: "Upgrade on AWS"
description: "Upgrade Materialize on AWS using the Terraform module."
menu:
  main:
    parent: "upgrading"
    weight: 20
---

The following tutorial upgrades your Materialize deployment running on AWS
Elastic Kubernetes Service (EKS). The tutorial assumes you have installed the
example on [Install on
AWS](/self-managed-deployments/installation/install-on-aws/).

## Upgrade guidelines

{{% include-from-yaml data="self_managed/upgrades"
name="upgrades-general-rules" %}}

{{< note >}}
{{< include-from-yaml data="self_managed/upgrades"
name="upgrade-major-version-restriction" >}}
{{< /note >}}

{{< note >}}
{{< include-from-yaml data="self_managed/upgrades"
name="downgrade-restriction" >}}
{{< /note >}}

## Prerequisites

### Required Tools

- [Terraform](https://developer.hashicorp.com/terraform/install?product_intent=terraform)
- [AWS CLI](https://docs.aws.amazon.com/cli/latest/userguide/install-cliv2.html)
- [kubectl](https://docs.aws.amazon.com/eks/latest/userguide/install-kubectl.html)

## Upgrade process

{{< important >}}

The following procedure performs a rolling upgrade, where both the old and new Materialize instances are running before the old instances are removed. When performing a rolling upgrade, ensure you have enough resources to support having both the old and new Materialize instances running.

{{</ important >}}

### Step 1: Update TF module source version

Update each module's `source` to point to the desired release tag, substituting
`<RELEASE_TAG>` in the code block below with your tag version:

{{< important >}}

The following code block is not comprehensive. Only the core modules and their
dependency chain are shown below.

If your configuration includes additional modules (networking, storage,
database, node pools, etc.) provided by Materialize, **update those to the same
release tag as well**.

{{< /important >}}

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

# Update the source of any additional Materialize-provided modules to the same release tag
```

### Step 2: Explicitly request rollout if using v1alpha1

{{< self-managed/crd-version-note "v1alpha1" >}}

{{< include-from-yaml data="self_managed/upgrades"
name="upgrade-tf-v4-crd-version-default" >}}

{{< include-from-yaml data="self_managed/crd_version_checks"
name="check-crd-version-tf" >}}

- If you are using `v1`, skip to the [Apply the updated TF
  step](#step-3-apply-the-updated-tf).
- {{< include-from-yaml data="self_managed/upgrades" name="upgrade-request_rollout" >}}

### Step 3: Apply the updated TF

{{% include-from-yaml data="self_managed/upgrades" name="upgrade-tf-apply" %}}

### Step 4: Verify the upgrade

Configure `kubectl` to connect to your EKS cluster, replacing `<your-region>`
with the region of your cluster (found in your `terraform.tfvars`; e.g.,
`us-east-1`):

```bash
# aws eks update-kubeconfig --name <your-eks-cluster-name> --region <your-region>
aws eks update-kubeconfig --name $(terraform output -raw eks_cluster_name) --region <your-region>
```

{{% include-from-yaml data="self_managed/upgrades" name="upgrade-verify-status" %}}

## Enable the monitoring stack

The Terraform modules can install a monitoring stack — Grafana, Thanos, Loki,
Grafana Alloy, and Alertmanager — alongside your deployment, with the
Materialize dashboards pre-installed. You can turn it on during an upgrade, in
the same `terraform apply` as the version bump.

The stack below arrived in **TF v10.0.0**, replacing the earlier single
Prometheus and Grafana. **TF v10.1.0** then added durable state for Grafana and
a load balancer to reach it on.

{{< warning >}}
`kubernetes/modules/prometheus` and `kubernetes/modules/grafana` were **removed**
in v10.0.0, not deprecated in place. If your configuration references either
directly, that reference breaks — pin the previous major until you have
migrated.

If you were running the old stack, upgrading **destroys** its Helm releases and
PersistentVolumeClaims. Up to 15 days of local Prometheus data goes with them,
along with anything hand-created in the old Grafana. There is no backfill. See
[Upgrading from the previous
stack](/manage/monitor/self-managed/grafana/#upgrading-from-the-previous-stack).
{{< /warning >}}

### If you use the example configuration

Set the following in your `terraform.tfvars`:

```hcl
enable_observability = true
```

### If you instantiate the modules yourself

1. Add the `alekc/kubectl` provider to your `versions.tf`. The monitoring module
   uses it for the `TargetGroupBinding` that attaches the Grafana load balancer
   to the Grafana Service:

   ```hcl
   kubectl = {
     source  = "alekc/kubectl"
     version = "2.4.1"
   }
   ```

1. Add the `monitoring` module, using the same release tag as the rest of your
   modules:

   ```hcl
   module "monitoring" {
     source = "github.com/MaterializeInc/materialize-terraform-self-managed//aws/modules/monitoring?ref=<RELEASE_TAG>"

     name_prefix = var.name_prefix
     region      = var.aws_region

     namespace = "monitoring"
     # The operator module already creates this namespace.
     create_namespace = false

     oidc_provider_arn       = module.eks.oidc_provider_arn
     cluster_oidc_issuer_url = module.eks.cluster_oidc_issuer_url

     storage_class = module.ebs_csi_driver.storage_class_name

     materialize_instance_namespace = "materialize-environment"
     materialize_operator_namespace = "materialize"

     # Grafana's own state. Omit to leave Grafana on SQLite.
     grafana_database = {
       vpc_id                    = module.networking.vpc_id
       subnet_ids                = module.networking.private_subnet_ids
       cluster_name              = module.eks.cluster_name
       cluster_security_group_id = module.eks.cluster_security_group_id
       node_security_group_id    = module.eks.node_security_group_id
     }

     # Reach Grafana without port forwarding. Omit to keep it on ClusterIP.
     grafana_load_balancer = {
       vpc_id                 = module.networking.vpc_id
       subnet_ids             = module.networking.private_subnet_ids
       node_security_group_id = module.eks.node_security_group_id
       ingress_cidr_blocks    = var.ingress_cidr_blocks
     }

     depends_on = [module.operator]
   }
   ```

1. Turn on the operator's scrape annotations so its pods are collected:

   ```hcl
   module "operator" {
     # ...
     helm_values = {
       observability = {
         enabled = true
         prometheus = {
           scrapeAnnotations = {
             enabled = true
           }
         }
       }
     }
   }
   ```

### What this creates

Applying the above adds S3 buckets for metrics and logs, and — from TF v10.1.0 —
a `db.t4g.micro` RDS instance for Grafana's own state and an internal NLB to
reach Grafana on. The database and the load balancer are both billable.

{{< warning >}}
The Grafana load balancer terminates no TLS, and Grafana has no identity
provider until you configure one. Keep it internal until both are addressed. A
public load balancer whose allowlist is still `0.0.0.0/0` is refused at plan
time for Grafana specifically.
{{< /warning >}}

{{< note >}}
The monitoring stack runs several components: Loki, Thanos, Grafana,
Alertmanager, kube-state-metrics, and two Alloy roles. Your generic node pool
may need to grow before the apply can schedule all of them.
{{< /note >}}

For accessing Grafana, pointing the stack at a database you already run, sizing
profiles, and retention, see
[Grafana](/manage/monitor/self-managed/grafana/).

## See also

- [Materialize Operator
  Configuration](/self-managed-deployments/operator-configuration/)
- [Materialize CRD Field
  Descriptions](/self-managed-deployments/materialize-crd-field-descriptions/)
- [Troubleshooting](/self-managed-deployments/troubleshooting/)

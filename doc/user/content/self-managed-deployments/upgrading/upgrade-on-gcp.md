---
title: "Upgrade on GCP"
description: "Upgrade Materialize on GCP using the Terraform module."
menu:
  main:
    parent: "upgrading"
    weight: 30
---

The following tutorial upgrades your Materialize deployment running on Google
Kubernetes Engine (GKE). The tutorial assumes you have installed the
example on [Install on
GCP](/self-managed-deployments/installation/install-on-gcp/).

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
- [Google Cloud CLI](https://cloud.google.com/sdk/docs/install)
- [kubectl](https://kubernetes.io/docs/tasks/tools/)

## Upgrade process

{{< important >}}

The following procedure performs a rolling upgrade, where both the old and new Materialize instances are running before the old instances are removed. When performing a rolling upgrade, ensure you have enough resources to support having both the old and new Materialize instances running.

{{</ important >}}

### Step 1: Update the Materialize Terraform Modules source version

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

# Update the source of any additional Materialize-provided modules to the same release tag
```

### Step 2: Explicitly request rollout if using v1alpha1

{{< self-managed/crd-version-note "v1alpha1" >}}

{{< include-from-yaml data="self_managed/upgrades"
name="upgrade-tf-v4-crd-version-default" >}}

{{< include-from-yaml data="self_managed/crd_version_checks"
name="check-crd-version-tf" >}}

- If you are using `v1`, skip to the [Apply the updated Terraform
  step](#step-3-apply-the-updated-terraform).
- {{< include-from-yaml data="self_managed/upgrades" name="upgrade-request_rollout" >}}

### Step 3: Apply the updated Terraform

{{% include-from-yaml data="self_managed/upgrades" name="upgrade-tf-apply" %}}

### Step 4: Verify the upgrade

Configure `kubectl` to connect to your GKE cluster, replacing `<your-project-id>`
with your GCP project ID:

```bash
# gcloud container clusters get-credentials <your-gke-cluster-name> --region <your-region> --project <your-project-id>
gcloud container clusters get-credentials $(terraform output -raw gke_cluster_name) \
 --region $(terraform output -raw gke_cluster_location) \
 --project <your-project-id>
```

{{% include-from-yaml data="self_managed/upgrades" name="upgrade-verify-status" %}}

## Enable the monitoring stack

The Terraform modules can install a monitoring stack — Grafana, Thanos, Loki,
Grafana Alloy, and Alertmanager — alongside your deployment, with the
Materialize dashboards pre-installed. You can turn it on during an upgrade, in
the same `terraform apply` as the version bump.

The stack below arrived in **v10.0.0** of the Materialize Terraform Modules,
replacing the earlier single Prometheus and Grafana. **v10.1.0** then added
durable state for Grafana and a load balancer to reach it on.

{{< warning >}}
Starting with **v11.0.0** of the Materialize Terraform Modules,
`enable_observability` defaults to `true`. Bumping `ref=<RELEASE_TAG>` to
v11.0.0 or later therefore installs the whole stack, and its billable
supporting resources, on a deployment that never set the variable. Set
`enable_observability = false` in the same change if you do not want it.
{{< /warning >}}

{{< warning >}}
`kubernetes/modules/prometheus` and `kubernetes/modules/grafana` were **removed**
in v10.0.0, not deprecated in place. If your configuration references either
directly, that reference breaks — pin the previous major until you have
migrated.

If you were running the old stack, upgrading **destroys** its Helm releases and
PersistentVolumeClaims. Up to 15 days of local Prometheus data goes with them,
along with anything hand-created in the old Grafana. There is no backfill. See
[How to upgrade from previous versions of the Materialize Terraform
Modules](/manage/monitor/self-managed/grafana/#how-to-upgrade-from-previous-versions-of-the-materialize-terraform-modules).
{{< /warning >}}

### If you use the example configuration

Nothing is required starting with v11.0.0 of the Materialize Terraform
Modules, where the variable defaults to `true`. To be explicit, or on an
earlier release, set the following in your `terraform.tfvars`:

```hcl
enable_observability = true
```

### If you instantiate the modules yourself

1. Add the `monitoring` module, using the same release tag as the rest of your
   modules:

   ```hcl
   module "monitoring" {
     source = "github.com/MaterializeInc/materialize-terraform-self-managed//gcp/modules/monitoring?ref=<RELEASE_TAG>"

     prefix     = var.name_prefix
     project_id = var.project_id
     region     = var.region

     namespace = "monitoring"
     # The operator module already creates this namespace.
     create_namespace = false

     materialize_instance_namespace = "materialize-environment"
     materialize_operator_namespace = "materialize"

     # Grafana's own state. Omit to leave Grafana on SQLite.
     grafana_database = {
       network_id = module.networking.network_id
     }

     # Reach Grafana without port forwarding. Omit to keep it on ClusterIP.
     grafana_load_balancer = {
       ingress_cidr_blocks = var.ingress_cidr_blocks
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

Applying the above adds Cloud Storage buckets for metrics and logs, and — from
Materialize Terraform Modules v10.1.0 — a `db-f1-micro` Cloud SQL instance for
Grafana's own state and an internal load balancer to reach Grafana on. The
database and the load balancer are both billable.

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
[Grafana](/manage/monitor/self-managed/grafana/). For what the stack stores and
the backends it can forward to, see [How logs and metrics are
stored](/manage/monitor/self-managed/storage/).

## See also

- [Materialize Operator
  Configuration](/self-managed-deployments/operator-configuration/)
- [Materialize CRD Field
  Descriptions](/self-managed-deployments/materialize-crd-field-descriptions/)
- [Troubleshooting](/self-managed-deployments/troubleshooting/)

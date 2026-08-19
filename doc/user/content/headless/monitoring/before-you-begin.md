---
headless: true
---

Ensure you have:

- A Materialize deployment created with the [Materialize Terraform
  modules](/self-managed-deployments/), with the monitoring stack enabled. See
  [Step 1](#step-1-enable-observability).

- [Terraform ⧉](https://developer.hashicorp.com/terraform/install) installed.

- [kubectl ⧉](https://kubernetes.io/docs/tasks/tools/) installed and configured
  to connect to your cluster.

{{< note >}}
The Terraform steps on this page require **TF v12.0.0** or later, which is where
the monitoring module accepts these destinations. If you install the
`materialize-monitoring` chart with Helm rather than through the Terraform
modules, no Terraform release applies and neither does `enable_observability`.
Follow the Helm instructions at the end of this page instead.
{{< /note >}}

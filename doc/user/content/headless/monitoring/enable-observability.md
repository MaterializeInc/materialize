---
headless: true
---

The Materialize Terraform Modules take an `enable_observability` variable.
Starting with **v11.0.0** it defaults to `true`, so a fresh apply installs the
monitoring stack without any configuration, and bumping `ref=<tag>` to v11.0.0
or later installs it on a deployment that never set the variable.

1. To confirm the setting, or to change it, set it explicitly in your
   `terraform.tfvars`:

   ```hcl
   enable_observability = true    # default starting with Materialize Terraform Modules v11.0.0
   ```

1. Apply the configuration:

   ```bash
   terraform apply
   ```

   The apply creates the object storage and cloud identities for metrics and
   logs, and installs the stack into the `monitoring` namespace.

{{< warning >}}
The stack and its supporting resources are billable, and the `generic` node pool
may need to grow before the first apply can schedule everything. If you do not
want it, set `enable_observability = false` before upgrading to Materialize
Terraform Modules v11.0.0.
{{< /warning >}}

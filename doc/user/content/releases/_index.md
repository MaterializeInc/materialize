---
title: "Releases"
description: "Materialize release notes"
disable_list: true
menu:
  main:
    identifier: "releases"
    weight: 80
aliases:
  - /self-managed/v25.1/release-notes/
---


## Self-Managed v26.0.0

*Released: 2025-11-18*

### Swap support

Starting in v26.0.0, Self-Managed Materialize enables swap by default. Swap
allows for infrequently accessed data to be moved from memory to disk. Enabling
swap reduces the memory required to operate Materialize and improves cost
efficiency.

To facilitate upgrades from v25.2, Self-Managed Materialize added new labels to
the node selectors for `clusterd` pods:

- To upgrade using Materialize-provided Terraforms, upgrade your Terraform
  version to `v0.6.1`:
  - {{< include-md
file="shared-content/self-managed/aws-terraform-v0.6.1-upgrade-notes.md" >}}.
  - {{< include-md
file="shared-content/self-managed/gcp-terraform-v0.6.1-upgrade-notes.md" >}}.
  - {{< include-md
  file="shared-content/self-managed/azure-terraform-v0.6.1-upgrade-notes.md"
  >}}.

- To upgrade if <red>**not**</red> using a Materialize-provided Terraforms,  you
must prepare your nodes by adding the required labels. For detailed
instructions, see [Prepare for swap and upgrade to
v26.0](/installation/upgrade-to-swap/).


### SASL/SCRAM-SHA-256 support

Starting in v26.0.0, Self-Managed Materialize supports SASL/SCRAM-SHA-256
authentication for PostgreSQL wire protocol connections. For more information,
see [Authentication](/security/self-managed/authentication/).

When SASL authentication is enabled:

- **PostgreSQL connections** (e.g., `psql`, client libraries, [connection
  poolers](/integrations/connection-pooling/)) use SCRAM-SHA-256 authentication
- **HTTP/Web Console connections** use standard password authentication

This hybrid approach provides maximum security for SQL connections while maintaining
compatibility with web-based tools.

### License Key

Starting in v26.0.0, Self-Managed Materialize requires a license key.

{{< yaml-table data="self_managed/license_key" >}}

For new deployments, you configure your license key in the Kubernetes Secret
resource during the installation process. For details, see the [installation
guides](/installation/). For existing deployments, you can configure your
license key via:

```bash
kubectl -n materialize-environment patch secret materialize-backend -p '{"stringData":{"license_key":"<your license key goes here>"}}' --type=merge
```

### PostgreSQL: Source versioning

{{< private-preview />}}

For PostgreSQL sources, starting in v26.0.0, Materialize introduces new syntax
for [`CREATE SOURCE`](/sql/create-source/postgres-v2/) and [`CREATE
TABLE`](/sql/create-table/) to allow better handle DDL changes to the upstream
PostgreSQL tables.

{{< note >}}
- This feature is currently supported for PostgreSQL sources, with
additional source types coming soon.

- Changing column types is currently unsupported.
{{< /note >}}

{{% include-example file="examples/create_source/example_postgres_source"
 example="syntax" %}}

{{% include-example file="examples/create_table/example_postgres_table"
 example="syntax" %}}

For more information, see:
- [Guide: Handling upstream schema changes with zero
  downtime](/ingest-data/postgres/source-versioning/)
- [`CREATE SOURCE`](/sql/create-source/postgres-v2/)
- [`CREATE TABLE`](/sql/create-table/)

### Deprecation

The `inPlaceRollout` setting has been deprecated and will be ignored. Instead,
use the new setting `rolloutStrategy` to specify either:

- `WaitUntilReady` (*Default*)
- `ImmediatelyPromoteCausingDowntime`

For more information, see [`rolloutStrategy`](/installation/#rollout-strategies).

### Terraform helpers

Corresponding to the v26.0.0 release, the following versions of the sample
Terraform modules have been released:

{{< yaml-table data="self_managed/terraform_list" >}}

{{< tabs >}} {{< tab "Materialize on AWS" >}}

{{< yaml-table data="self_managed/aws_terraform_versions" >}}

{{% self-managed/aws-terraform-upgrade-notes %}}

Click on the Terraform version link to go to the release-specific Upgrade Notes.

{{</ tab >}}

{{< tab "Materialize on Azure" >}}

{{< yaml-table data="self_managed/azure_terraform_versions" >}}

{{% self-managed/azure-terraform-upgrade-notes %}}

See also Upgrade Notes for release specific notes.

{{</ tab >}}

{{< tab "Materialize on GCP" >}}

{{< yaml-table data="self_managed/gcp_terraform_versions" >}}

{{% self-managed/gcp-terraform-upgrade-notes %}}

See also Upgrade Notes for release specific notes.

{{</ tab >}}

{{< tab "terraform-helm-materialize" >}}

{{< yaml-table data="self_managed/terraform_helm_compatibility" >}}

{{</ tab >}} {{</ tabs >}}

### Upgrade notes for v26.0.0

{{< include-md file="shared-content/self-managed/upgrade-notes/v26.0.md" >}}

See also [General notes for upgrades](/installation/#general-notes-for-upgrades).


## See also

- [Cloud Upgrade Schedule](/releases/cloud-upgrade-schedule/)

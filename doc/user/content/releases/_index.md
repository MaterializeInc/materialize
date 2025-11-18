---
title: "Releases"
description: "Materialize release notes"
disable_list: true
menu:
  main:
    identifier: "releases"
    weight: 80

---


## Self-Managed v26.0.0

### Swap support

Starting in v26.0.0, Self-Managed Materialize enables swap by default. Swap
allows for infrequently accessed data to be moved from memory to disk. Enabling
swap reduces the memory required to operate Materialize and improves cost
efficiency.

To facilitate upgrades, Self-Managed Materialize added new labels to the node
selectors for `clusterd` pods:

- To upgrade from v25.2.13 or later, you can follow the standard upgrade
instructions for your deployment:
  - [Upgrade on Kind (via
    Helm)](/installation/install-on-local-kind/upgrade-on-local-kind/)
  - [Upgrade on AWS (via
    Terraform)](/installation/install-on-aws/legacy-terraform-module/upgrade/)
  - [Upgrade on Azure (via
    Terraform)](/installation/install-on-azure/legacy-terraform-module/upgrade/)
  - [Upgrade on GCP (via
    Terraform)](/installation/install-on-gcp/legacy-terraform-module/upgrade/)

- To upgrade from v25.2.12 or earlier, you must prepare your nodes by adding the
required labels. For detailed instructions, see:

  {{< yaml-table data="self_managed/enable_swap_upgrade_guides" >}}


### SASL/SCRAM-SHA-256 support

Starting in v26.0.0, Self-Managed Materialize supports SASL/SCRAM-SHA-256
authentication for PostgreSQL wire protocol connections. For more information,
see [Authentication](/security/self-managed/authentication/).

### License Key

{{< include-md file="shared-content/license-key-required.md" >}}

The license key should be configured in the Kubernetes Secret resource
created during the installation process. To configure a license key in an
existing installation, run:

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

## See also

- [Cloud Upgrade Schedule](/releases/cloud-upgrade-schedule/)

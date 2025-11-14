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
    Terraform)](/installation/install-on-aws/upgrade-on-aws/)
  - [Upgrade on Azure (via
    Terraform)](/installation/install-on-azure/upgrade-on-azure/)
  - [Upgrade on GCP (via
    Terraform)](/installation/install-on-gcp/upgrade-on-gcp/)

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

## See also

- [Cloud Upgrade Schedule](/releases/cloud-upgrade-schedule/)

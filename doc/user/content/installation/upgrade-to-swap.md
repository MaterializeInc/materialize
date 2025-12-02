---
title: "Appendix: Prepare for swap and upgrade to v26.0"
description: "Upgrade procedure for v26.0 if not using Materialize Terraform."
menu:
  main:
    parent: "installation"
    weight: 95
    identifier: "helm-upgrade-from-v25.2.12-aws"
---

{{< annotation type="Disambiguation" >}}

This page outlines the general steps for upgrading from v25.2 to v26.0 if you
are <red>**not**</red> using Materialize provided Terraforms.

If you are using Materialize-provided Terraforms, `v0.6.1` and higher of the
Terraforms handle the preparation for you.  If using Materialize-provided
Terraforms, upgrade your Terraform version to `v0.6.1` or higher and follow the
Upgrade notes:

- {{< include-md
file="shared-content/self-managed/aws-terraform-v0.6.1-upgrade-notes.md" >}}.

- {{< include-md
file="shared-content/self-managed/gcp-terraform-v0.6.1-upgrade-notes.md" >}}.

- {{< include-md
file="shared-content/self-managed/azure-terraform-v0.6.1-upgrade-notes.md" >}}.

See also [Upgrade Overview](/installation/upgrading/).

{{< /annotation >}}

{{< include-md file="shared-content/self-managed/prepare-nodes-and-upgrade.md" >}}

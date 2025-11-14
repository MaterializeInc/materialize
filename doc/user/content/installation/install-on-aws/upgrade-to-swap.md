---
title: "Upgrade from v25.2.12 or earlier (using Helm)"
description: "Upgrade procedure when upgrading from v25.2.12 or earlier without swap to v26.0 with swap enabled if not using Terraform."
menu:
  main:
    parent: "install-on-aws"
    weight: 15
    identifier: "helm-upgrade-from-v25.2.12-aws"
---

{{< annotation type="Disambiguation: Terraform" >}}

This page is for upgrading via Helm. For upgrading your deployment using
Terraform, see {{< include-md
file="shared-content/self-managed/aws-terraform-v0.6.1-upgrade-notes.md" >}}.

{{< /annotation >}}

{{< include-md file="shared-content/self-managed/prepare-nodes-and-upgrade.md" >}}

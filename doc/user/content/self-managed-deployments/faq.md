---
title: "FAQ"
description: "Frequently asked questions about self-managed deployments."
aliases:
  - /self-hosted/faq/
  - /installation/faq/
menu:
  main:
    parent: "sm-deployments"
    weight: 92
---

## How long do license keys last?

Community edition license keys are valid for one year. Enterprise license
keys will vary based on the terms of your contract.

## How do I get a license key?

{{< yaml-table data="self_managed/license_key" >}}

## How do I add a license key to an existing installation?

The license key should be configured in the Kubernetes Secret resource
created during the installation process. To configure a license key in an
existing installation, run:

```bash
kubectl -n materialize-environment patch secret materialize-backend -p '{"stringData":{"license_key":"<your license key goes here>"}}' --type=merge
```

## How can I downgrade Self-Managed Materialize?

{{< include-from-yaml data="self_managed/upgrades"
name="downgrade-restriction" >}}

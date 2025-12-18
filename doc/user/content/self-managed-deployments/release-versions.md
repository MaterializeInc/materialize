---
title: "Self-managed release versions"
description: ""
menu:
  main:
    parent: "sm-deployments"
    weight: 95
aliases:
  - /installation/release-versions/
---

## V26 releases

{{< yaml-table data="self_managed/self_managed_operator_compatibility" >}}

## Terraform releases

Materialize provides the following [**Terraform
modules**](https://github.com/MaterializeInc/materialize-terraform-self-managed/tree/main?tab=readme-ov-file#materialize-self-managed-terraform-modules)
as examples for deploying Materialize.

{{< important >}}
These modules are intended for evaluation/demonstration purposes and for serving
as a template when building your own production deployment. The modules should
not be directly relied upon for production deployments: **future releases of the
modules will contain breaking changes.** Instead, to use as a starting point for
your own production deployment, either:
- Fork the repo and pin to a specific version; or
- Use the code as a reference when developing your own deployment.
{{</ important >}}


{{< yaml-table data="self_managed/unified_terraform_versions" >}}

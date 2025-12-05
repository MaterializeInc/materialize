
---
title: "Terraform Modules"
description: "List of template Terraform modules that are available as a
starting point."
menu:
  main:
    parent: "installation"
    identifier: "appendix-terraforms"
    weight: 95
---

To help you get started, Materialize provides Terraform modules.

{{< important >}}
These modules are intended for evaluation/demonstration purposes and for serving
as a template when building your own production deployment. The modules should
not be directly relied upon for production deployments: **future releases of the
modules will contain breaking changes.** Instead, to use as a starting point for
your own production deployment, either:

- Fork the repo and pin to a specific version; or

- Use the code as a reference when developing your own deployment.

{{</ important >}}

### **Terraform Modules**

Materialize provides a [**unified Terraform module**](https://github.com/MaterializeInc/materialize-terraform-self-managed/tree/main?tab=readme-ov-file#materialize-self-managed-terraform-modules) in order to provide concrete examples and an opinionated model for deploying materialize.
This module supports deployments for AWS

{{< yaml-table data="self_managed/terraform_list" >}}

### *Legacy Terraform Modules*

{{< yaml-table data="self_managed/legacy_terraform_list" >}}

#### Materialize on AWS Terraform module

{{< yaml-table data="self_managed/aws_terraform_versions" >}}

{{% self-managed/aws-terraform-upgrade-notes %}}

See also [Upgrade Notes](
https://github.com/MaterializeInc/terraform-aws-materialize?tab=readme-ov-file#upgrade-notes)
for release-specific upgrade notes.


#### Materialize on Azure Terraform module

{{< yaml-table data="self_managed/azure_terraform_versions" >}}

{{% self-managed/azure-terraform-upgrade-notes %}}

### Materialize on GCP Terraform module

{{< yaml-table data="self_managed/gcp_terraform_versions" >}}

{{% self-managed/gcp-terraform-upgrade-notes %}}

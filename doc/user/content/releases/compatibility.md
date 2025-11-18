---
title: "Terraform releases"
description: "Terraform releases and compatibility."
menu:
  main:
    name: "Release Compatibility"
    identifier: "release-compatibility"
    parent: "releases"
    weight: 80
---

## Terraform helpers

To help you get started, Materialize also provides some template Terraforms.

{{< important >}} These modules are intended for evaluation/demonstration purposes and for serving as a template when building your own production deployment. The modules should not be directly relied upon for production deployments: future releases of the modules will contain breaking changes. Instead, to use as a starting point for your own production deployment, either:

Fork the repo and pin to a specific version; or

Use the code as a reference when developing your own deployment.

{{</ important >}}

{{< yaml-table data="self_managed/terraform_list" >}}

{{< tabs >}} {{< tab "Materialize on AWS" >}}

{{< yaml-table data="self_managed/aws_terraform_versions" >}}

{{% self-managed/aws-terraform-upgrade-notes %}}

See also Upgrade Notes for release-specific upgrade notes.

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
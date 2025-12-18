---
title: "Use Terraform to manage Materialize"
description: "Create and manage Materialize resources with Terraform"
disable_list: true
menu:
  main:
    parent: manage
    weight: 30
    identifier: "manage-terraform"
    name: "Manage with Terraform"
aliases:
  - /self-managed/v25.1/manage/terraform/
---

[Terraform](https://www.terraform.io/) is an infrastructure-as-code tool that
allows you to manage your resources in a declarative configuration language.
Materialize maintains a [Terraform provider](https://registry.terraform.io/providers/MaterializeInc/materialize/latest/docs)
to help you safely and predictably provision and manage connections, sources,
and other database objects.

Materialize also maintains [several
modules](/manage/terraform/manage-cloud-modules) that make it easier to manage
other cloud resources that Materialize depends on. Modules allow you to bypass
manually configuring cloud resources and are an efficient way of deploying
infrastructure with a single `terraform apply` command.

## Available guides

{{< multilinkbox >}}
{{< linkbox title="To get started" >}}
[Get started with Terraform and Materialize](./get-started/)
{{</ linkbox >}}

{{< linkbox title="Manage resources" >}}

[Manage Materialize resources](./manage-resources)

[Manage cloud resources](./manage-cloud-modules/)

{{</ linkbox >}}
{{< linkbox title="Additional modules" >}}

- [Appendix: Secret stores](./appendix-secret-stores/)

{{</ linkbox >}}
{{</ multilinkbox >}}


## Contributing

If you want to help develop the Materialize provider, check out the [contribution guidelines](https://github.com/MaterializeInc/terraform-provider-materialize/blob/main/CONTRIBUTING.md).

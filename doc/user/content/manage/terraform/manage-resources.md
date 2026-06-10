---
title: "Manage Materialize resources with the Materialize provider"
description: ""
menu:
  main:
    parent: "manage-terraform"
    weight: 15
    name: "Manage Materialize resources"
---

The Materialize provider allows you to create several resource types in your
region. Resources correspond to Materialize objects and are configured
with the `resource` block in your Terraform configuration file.

### Create Materialize cluster

For example, to create a new cluster, you would use the `materialize_cluster`
resource:

```hcl
resource "materialize_cluster" "example_cluster" {
  name = "cluster"
}
```

You can find reference documentation for all the resources available in the
Materialize provider in the [Terraform registry](https://registry.terraform.io/providers/MaterializeInc/materialize/latest/docs).

### Retrieve Materialize data sources

The Materialize provider supports several data source types to retrieve
information about your existing Materialize resources. Data sources can return
information about objects defined outside of Terraform and can be used as
variables in your configuration with the `data` block.

For example, to return information about your current clusters, you would use the
`materialize_cluster` data source:

```hcl
data "materialize_cluster" "all" {}
```

This data source returns all cluster names and IDs which you can use as
variables for new resources.

### Import Materialize objects into Terraform state

Terraform allows you to import infrastructure into your current Terraform state
file. Importing objects allows you to keep track of infrastructure created
outside of the Terraform workflow. The `terraform import` command lets you
specify objects you want Terraform to manage and reduces potential configuration
drift. Importing objects allows you to keep related infrastructure in a
Terraform state file and let Terraform manage the configuration.

For instance, if you created a cluster in Materialize and wanted to manage that
resource with Terraform, you would add create resource block for the resource
you want to import in your Terraform configuration:

```hcl
resource "materialize_cluster.<cluster_name> {
    name = <cluster_name>
}
```

Next, you would use the `terraform import` command with the cluster name and ID
to associate the object with the resource block:

```shell
terraform import materialize_cluster.<cluster_name> <CLUSTER_ID>
```

Terraform will then manage the cluster and you can use Terraform as the source of
truth for your Materialize object.

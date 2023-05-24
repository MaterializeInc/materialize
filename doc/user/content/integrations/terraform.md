---
title: "Terraform Materialize Provider"
description: "Create and manage Materialize objects with the Terraform Materialize provider"
menu:
  main:
    parent: "integrations"
    weight: 11
    name: "Terraform Materialize Provider"
---

Terraform is an infrastructure-as-code tool that allows you to manage your
resources in a declarative configuration language. Materialize maintains a
Terraform provider to help you configure Materialize resources like
connections, sources, and database objects.

## Use cases

Materialize objects can be created through standard SQL queries, but managing
more than a few components can be difficult to maintain. The Terraform
Materialize provider allows users who are not well-versed in SQL to maintain
objects in an organization. A few specific use cases are captured in the
sections below.

### MSK or self-managed Kafka and PrivateLink

To get data into Materialize, you need a connection to allow your data source to
communicate with Materialize. One option to connect securely to Materialize is
AWS PrivateLink.

The [AWS MSK PrivateLink](https://github.com/MaterializeInc/terraform-aws-msk-privatelink) and [AWS Kafka PrivateLink](https://github.com/MaterializeInc/terraform-aws-kafka-privatelink) modules allow you to manage
your connection to Materialize in a single configuration. With these modules,
you can create a secure connection and add Materialize [resource blocks](https://registry.terraform.io/providers/MaterializeInc/materialize/latest/docs/resources/connection_aws_privatelink) to allow
Materialize to communicate with the PrivateLink endpoint.

### EC2 SSH bastion host

Another method for source connection is to use a bastion host to allow SSH
communication to and from Materialize.

The [EC2 SSH bastion module](https://github.com/MaterializeInc/terraform-aws-ec2-ssh-bastion) allows you to configure an EC2 instance with security groups and an SSH keypair. After using the module, you can configure the `materialize_connection_ssh_tunnel` resource, allowing Materialize an end-to-end connection to your source.

### Postgres RDS

You can also create an RDS instance from which you can track and propagate
changes. The [AWS RDS Postgres module](https://github.com/MaterializeInc/terraform-aws-rds-postgres) creates a VPC, security groups, and an RDS instance in AWS. After you run the module, you can create a secret, connection and source with the Materialize Terraform provider for an end-to-end connection to this instance as a new source.

## Using the Terraform provider

To use the Materialize provider, you create a new `main.tf` file and add the
required providers:

```hcl
terraform {
  required_providers {
    materialize = {
      source = "MaterializeInc/materialize"
      version = "0.0.5"
    }
  }
}
```

The Materialize provider is hosted on the [Terraform provider
registry](https://registry.terraform.io/providers/MaterializeInc/materialize/latest).

### Authentication

To configure the provider to communicate with your Materialize organization, you
need to authenticate with a Materialize username, app password, and other
specifics from your account.

Materialize recommends saving sensitive input variables as environment variables
to avoid checking secrets into source control. To create a Terraform environment
variable, export your sensitive variable in the terminal:

```shell
export TF_VAR_MZ_PW=yourMZpassword
```

In the `main.tf` file, add the provider configuration and any variable
references:

```hcl

variable "MZ_PW" {}

provider "materialize" {
  host     = "yourMZhostname"
  username = "yourMZusername"
  password = var.MZ_PW
  port     = 6875
  database = "yourMZdatabase"
}
```

### Creating resources

The Materialize provider allows you to create several resource types in your
organization. Resources correspond to Materialize objects and are configured
with the `resource` block in your Terraform configuration file.

For example, to create a new cluster, you would use the `materialize_cluster`
resource:

```hcl
resource "materialize_cluster" "example_cluster" {
  name = "cluster"
}
```

All the available Materialize resources can be found in the [Terraform provider
registry documentation](https://registry.terraform.io/providers/MaterializeInc/materialize/latest/docs).

## Contributing

If you want to help develop the Materialize provider, check out the [contribution
guidelines](https://registry.terraform.io/providers/MaterializeInc/materialize/latest/docs) in the provider repository.

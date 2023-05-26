---
title: "Terraform"
description: "Create and manage Materialize objects with the Terraform Materialize provider"
menu:
  main:
    parent: "integrations"
    weight: 11
    name: "Terraform"
---

[Terraform](https://www.terraform.io/) is an infrastructure-as-code tool that allows you to manage your
resources in a declarative configuration language. Materialize maintains a
[Terraform provider](https://registry.terraform.io/providers/MaterializeInc/materialize/latest/docs) to help you configure Materialize resources like
connections, sources, and database objects. The Materialize provider is a
Terraform plugin that allows Terraform to interact with the Materialize API.

Materialize also maintains several modules that make it easier to manage your
other cloud resources that Materialize depends on. Modules allow you to bypass
manually configuring cloud resources and are an efficient way of deploying
infrastructure with a single `terraform apply` command.

## Use cases

Materialize objects can be created through standard SQL queries, but managing
more than a few components can be difficult to maintain.  Using Terraform can be
helpful when you manage other cloud resources in collaboration with Materialize.
The Terraform modules below provide the cloud infrastructure foundation
Materialize needs to communicate with components outside of Materialize itself.
The Materialize provider allows users to manage Materialize resources in the
same programmatic way. 

You can use the modules to establish the underlying cloud
resources and then use the Materialize provider to build Materialize-specific
objects. A few use cases are captured in the sections below:

### MSK or self-managed Kafka and PrivateLink

To get data into Materialize, you need a connection to allow your data source to
communicate with Materialize. One option to connect securely to Materialize is
AWS PrivateLink.

The [AWS MSK PrivateLink](https://github.com/MaterializeInc/terraform-aws-msk-privatelink) and [AWS Kafka PrivateLink](https://github.com/MaterializeInc/terraform-aws-kafka-privatelink) modules allow you to manage
your connection to Materialize in a single configuration. The module builds
target groups for brokers, a network load balancer, a TCP listener, and a VPC. These AWS resources are necessary components for creating a PrivateLink connection.

The Materialize provider uses [connection resource blocks](https://registry.terraform.io/providers/MaterializeInc/materialize/latest/docs/resources/connection_aws_privatelink) to allow
Materialize to communicate with the PrivateLink endpoint. After you deploy the
module, you can create a new Materialize connection with the AWS resource
information. The configuration below is an example of the Materialize provider,
performing the same necessary steps as the [`CREATE CONNECTION`](https://materialize.com/docs/sql/create-connection/#aws-privatelink) statement in SQL:


```hcl
resource "materialize_connection_aws_privatelink" "example_privatelink_connection" {
  service_name       = <vpc_endpoint_service_name>
  availability_zones = [<availability_zone_ids>]
}

resource "materialize_connection_kafka" "example_kafka_connection_multiple_brokers" {
  name = "example_kafka_connection_multiple_brokers"
  kafka_broker {
    broker            = "b-1.hostname-1:9096"
    target_group_port = "9001"
    availability_zone = <availability_zone_id>
    privatelink_connection {
      name          = example_aws_privatelink_connection"
      database_name = "materialize"
      schema_name   = "public"
    }
  }
}
```

### EC2 SSH bastion host

Another method for source connection is to use a bastion host to allow SSH
communication to and from Materialize.

The [EC2 SSH bastion module](https://github.com/MaterializeInc/terraform-aws-ec2-ssh-bastion) allows
you to configure an EC2 instance with security groups and an SSH keypair. These
components form the foundation you need to have a secure, centralized access
point between your data source and Materialize.

After using the module, you can configure the [`materialize_connection_ssh_tunnel`](https://registry.terraform.io/providers/MaterializeInc/materialize/latest/docs/resources/connection_ssh_tunnel)
resource with the module output, allowing Materialize an end-to-end connection
to your source. The provider will configure the same Materialize objects as the
[`CREATE CONNECTION`](https://materialize.com/docs/sql/create-connection/#ssh-tunnel) statement


### Postgres RDS

You can also create an RDS instance from which you can track and propagate
changes. The [AWS RDS Postgres module](https://github.com/MaterializeInc/terraform-aws-rds-postgres) creates a
VPC, security groups, and an RDS instance in AWS. You can use these AWS
components to create a database with data you want to process in Materialize.

After you run the module, you
can create a secret, connection, and source with the Materialize
provider for an end-to-end connection to this instance as a new source. The
Materialize provider will create these objects just like the [`CREATE
SECRET`](https://materialize.com/docs/sql/create-secret/), [`CREATE CONNECTION`](https://materialize.com/docs/sql/create-connection/#postgresql), and [`CREATE SOURCE`](https://materialize.com/docs/sql/create-source/postgres/) statements in SQL. The
secret, connection, and source resources would be similar to the example
Terraform configuration below with output from the module:

```hcl
resource "materialize_secret" "example_secret" {
  name  = "secret"
  value = <your_RDS_password>
}

resource "materialize_connection_postgres" "example_postgres_connection" {
  name = "example_postgres_connection"
  host = <your_RDS_hostname>
  port = 5432
  user {
    secret {
      name          = "example"
      database_name = "database"
      schema_name   = "schema"
    }
  }
  password {
    name          = "example"
    database_name = "database"
    schema_name   = "schema"
  }
  database = "example"
}

resource "materialize_source_postgres" "example_source_postgres" {
  name        = "source_postgres"
  schema_name = "schema"
  size        = "3xsmall"
  postgres_connection {
    name = "pg_connection"
    # Optional parameters
    # database_name = "postgres"
    # schema_name = "public"
  }
  publication = "mz_source"
  table = {
    "schema1.table_1" = "s1_table_1"
    "schema2_table_1" = "s2_table_1"
  }
}
```

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

The Materialize provider is hosted on the [Terraform provider registry](https://registry.terraform.io/providers/MaterializeInc/materialize/latest).

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

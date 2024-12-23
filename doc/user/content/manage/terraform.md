---
title: "Use Terraform to manage Materialize"
description: "Create and manage Materialize resources with Terraform"
menu:
  main:
    parent: "manage"
    weight: 11
    name: "Use Terraform to manage Materialize"
---

[Terraform](https://www.terraform.io/) is an infrastructure-as-code tool that
allows you to manage your resources in a declarative configuration language.
Materialize maintains a [Terraform provider](https://registry.terraform.io/providers/MaterializeInc/materialize/latest/docs)
to help you safely and predictably provision and manage connections, sources,
and other database objects.

Materialize also maintains [several modules](#terraform-modules) that make it
easier to manage other cloud resources that Materialize depends on. Modules
allow you to bypass manually configuring cloud resources and are an efficient
way of deploying infrastructure with a single `terraform apply` command.

## Terraform provider

The Materialize provider is hosted on the [Terraform provider registry](https://registry.terraform.io/providers/MaterializeInc/materialize/latest).

To use the Materialize provider, you create a new `main.tf` file and add the
required providers:

```hcl
terraform {
  required_providers {
    materialize = {
      source = "MaterializeInc/materialize"
    }
  }
}
```

### Authentication

To configure the provider to communicate with your Materialize region, you
need to authenticate with a Materialize username, app password, and other
specifics from your account.

We recommend saving sensitive input variables as environment variables to avoid
checking secrets into source control. In Terraform, you can export Materialize
app passwords as a [Terraform environment variable](https://developer.hashicorp.com/terraform/cli/config/environment-variables#tf_var_name)
with the `TF_VAR_<name>` format.

```shell
export TF_VAR_MZ_PASSWORD=<app_password>
```

In the `main.tf` file, add the provider configuration and any variable
references:

```hcl
variable "MZ_PASSWORD" {}

provider "materialize" {
  password       = var.MZ_PASSWORD
  default_region = <region>
  database       = <database>
}
```

#### Creating service accounts

**Minimum requirements:** `terraform-provider-materialize` v0.8.1+

As a best practice, we strongly recommend using [service accounts](/manage/access-control/create-service-accounts)
to connect external applications to Materialize. To create a
service account, create a new [`materialize_role`](https://registry.terraform.io/providers/MaterializeInc/materialize/latest/docs/resources/role)
and associate it with a new [`materialize_app_password`](https://registry.terraform.io/providers/MaterializeInc/materialize/latest/docs/resources/app_password)
of type `service`. More granular permissions for the service account can then
be configured using [role-based access control (RBAC)](/manage/access-control/#role-based-access-control-rbac).

```hcl
# Create a service user in the aws/us-east-1 region.
resource "materialize_role" "production_dashboard" {
  name   = "svc_production_dashboard"
  region = "aws/us-east-1"
}

# Create an app password for the service user.
resource "materialize_app_password" "production_dashboard" {
  name = "production_dashboard_app_password"
  type = "service"
  user = materialize_role.production_dashboard.name
  roles = ["Member"]
}

# Allow the service user to use the "production_analytics" database.
resource "materialize_database_grant" "database_usage" {
  role_name     = materialize_role.production_dashboard.name
  privilege     = "USAGE"
  database_name = "production_analytics"
  region        = "aws/us-east-1"
}

# Export the user and password for use in the external tool.
output "production_dashboard_user" {
  value = materialize_role.production_dashboard.name
}
output "production_dashboard_password" {
  value = materialize_app_password.production_dashboard.password
}
```

### Materialize resources

The Materialize provider allows you to create several resource types in your
region. Resources correspond to Materialize objects and are configured
with the `resource` block in your Terraform configuration file.

For example, to create a new cluster, you would use the `materialize_cluster`
resource:

```hcl
resource "materialize_cluster" "example_cluster" {
  name = "cluster"
}
```

You can find reference documentation for all the resources available in the
Materialize provider in the [Terraform registry](https://registry.terraform.io/providers/MaterializeInc/materialize/latest/docs).

### Create Materialize data sources

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

## Terraform modules

The Terraform modules below provide the cloud infrastructure foundation
Materialize needs to communicate with components outside of Materialize itself.
The Materialize provider allows users to manage Materialize resources in the
same programmatic way.

{{< note >}}
While Materialize offers support for its Terraform provider, Materialize does
not offer support for these modules.
{{</ note >}}

You can use the modules to establish the underlying cloud
resources and then use the Materialize provider to build Materialize-specific
objects. A few use cases are captured in the sections below:

### AWS PrivateLink


To get data into Materialize, you need a connection to allow your data source to
communicate with Materialize. One option to connect securely to Materialize is
AWS PrivateLink.

The [AWS MSK PrivateLink](https://github.com/MaterializeInc/terraform-aws-msk-privatelink), [AWS RDS PrivateLink](https://github.com/MaterializeInc/terraform-aws-rds-privatelink), and [AWS Kafka PrivateLink](https://github.com/MaterializeInc/terraform-aws-kafka-privatelink) modules allow you to manage
your connection to Materialize in a single configuration. The module builds
target groups for brokers, a network load balancer, a TCP listener, and a VPC endpoint within an existing VPC. These AWS resources are necessary components for creating a PrivateLink connection.

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

For a complete example of the Amazon MSK module with the Materialize provider,
check out this [demo](https://github.com/MaterializeInc/demos/tree/main/integrations/terraform/msk-privatelink). The demo adds the Materialize provider configuration to the modules and bundles the entire deployment into one Terraform configuration file.


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
[`CREATE
CONNECTION`](https://materialize.com/docs/sql/create-connection/#ssh-tunnel)
statement.


### Amazon RDS for PostgreSQL

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
  value = <RDSpassword>
}

resource "materialize_connection_postgres" "example_postgres_connection" {
  name = "example_postgres_connection"
  host = <RDShostname>
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
  cluster_name = "quickstart"
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

## External Secret Stores

Materialize does not directly integrate with external secret stores, but it's possible to manage this integration via Terraform.

The [secret stores demo](https://github.com/MaterializeInc/demos/tree/main/integrations/terraform/secret-stores) shows how to handle [secrets](/sql/create-secret) and sensitive data with some popular secret stores. By utilizing Terraform's infrastructure-as-code model, you can automate and simplify both the initial setup and ongoing management of secret stores with Materialize.

A popular secret store is [HashiCorp Vault](https://www.vaultproject.io/). To use Vault with Materialize, you'll need to install the Terraform Vault provider:

```hcl
terraform {
  required_providers {
    vault = {
      source  = "hashicorp/vault"
      version = "~> 3.15"
    }
  }
}

provider "vault" {
  address = "https://vault.example.com"
  token   = "your-vault-token"
}
```

Next, fetch a secret from Vault and use it to create a new Materialize secret:

```hcl
data "vault_generic_secret" "materialize_password" {
  path = "secret/materialize"
}

resource "materialize_secret" "example_secret" {
  name  = "pgpass"
  value = data.vault_generic_secret.materialize_password.data["pgpass"]
}
```

In this example, the `vault_generic_secret` data source retrieves a secret from Vault, which is then used as the value for a new `materialize_secret` resource.

You can find examples of using other popular secret stores providers in the [secret stores demo](https://github.com/MaterializeInc/demos/tree/main/integrations/terraform/secret-stores).

## Contributing

If you want to help develop the Materialize provider, check out the [contribution guidelines](https://github.com/MaterializeInc/terraform-provider-materialize/blob/main/CONTRIBUTING.md).

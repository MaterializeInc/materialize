---
audience: developer
canonical_url: https://materialize.com/docs/manage/terraform/manage-cloud-modules/
complexity: beginner
description: Use Terraform modules to set up cloud resources
doc_type: reference
keywords:
- CREATE CONNECTION
- CREATE AN
- CREATE A
- 'Note:'
- Manage cloud resources
product_area: Operations
status: stable
title: Manage cloud resources
---

# Manage cloud resources

## Purpose
Use Terraform modules to set up cloud resources

If you need to understand the syntax and options for this command, you're in the right place.


Use Terraform modules to set up cloud resources



The Terraform modules below provide the cloud infrastructure foundation
Materialize needs to communicate with components outside of Materialize itself.
The Materialize provider allows users to manage Materialize resources in the
same programmatic way.

You can use the modules to establish the underlying cloud
resources and then use the Materialize provider to build Materialize-specific
objects. A few use cases are captured in the sections below.

> **Note:** 
While Materialize offers support for its Terraform provider, Materialize does
not offer support for these cloud resources modules.


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
performing the same necessary steps as the [`CREATE CONNECTION`](/sql/create-connection/#aws-privatelink) statement in SQL:


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
```text

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
CONNECTION`](/sql/create-connection/#ssh-tunnel)
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
SECRET`](/sql/create-secret/), [`CREATE CONNECTION`](/sql/create-connection/#postgresql), and [`CREATE SOURCE`](/sql/create-source/postgres/) statements in SQL. The
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


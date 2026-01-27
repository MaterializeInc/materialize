# Use Terraform to manage Materialize

Create and manage Materialize resources with Terraform



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

<div class="multilinkbox">
<div class="linkbox ">
  <div class="title">
    To get started
  </div>
  <a href="./get-started/" >Get started with Terraform and Materialize</a>
</div>


<div class="linkbox ">
  <div class="title">
    Manage resources
  </div>
  <p><a href="./manage-resources" >Manage Materialize resources</a></p>
<p><a href="./manage-cloud-modules/" >Manage cloud resources</a></p>

</div>

<div class="linkbox ">
  <div class="title">
    Additional modules
  </div>
  <ul>
<li><a href="./appendix-secret-stores/" >Appendix: Secret stores</a></li>
</ul>

</div>

</div>



## Contributing

If you want to help develop the Materialize provider, check out the [contribution guidelines](https://github.com/MaterializeInc/terraform-provider-materialize/blob/main/CONTRIBUTING.md).



---

## Appendix: External secret stores


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

You can find examples of using other popular secret stores providers in the
[secret stores
demo](https://github.com/MaterializeInc/demos/tree/main/integrations/terraform/secret-stores).


---

## Get started with the Materialize provider


The following guide provides an introduction to the Materialize Terraform
provider and setup.

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

## Authentication and Configuration

The provider auto-detects your deployment type based on the configuration you
provide:
- **Materialize Cloud**: Use `password` and `default_region`
- **Self-managed**: Use `host`, `username`, `password`, and other connection parameters

> **Warning:** Switching between Materialize Cloud and self-managed configuration **breaks your
> Terraform state file**. Ensure that your initial configuration
> matches your intended deployment type, and do not switch to a
> different deployment type afterward.



**Materialize Cloud:**
### Materialize Cloud

Configure the provider with your [app password](/security/cloud/users-service-accounts/create-service-accounts/)
and region. This provides access to all provider resources, including
organization-level resources (users, SSO, SCIM) and database resources.

To avoid checking secrets into source control, use environment variables for authentication. You have two options:

**Option 1: Using provider environment variables (recommended)**

The provider automatically reads the `MZ_PASSWORD` environment variable:

```shell
export MZ_PASSWORD=<app_password>
```

```hcl
provider "materialize" {
  default_region = <region>
  database       = <database>
}
```

**Option 2: Using Terraform input variables**

Use [Terraform environment variables](https://developer.hashicorp.com/terraform/cli/config/environment-variables#tf_var_name) with the `TF_VAR_` prefix:

```shell
export TF_VAR_materialize_password=<app_password>
```

```hcl
variable "materialize_password" {
  type      = string
  sensitive = true
}

provider "materialize" {
  password       = var.materialize_password
  default_region = <region>
  database       = <database>
}
```

#### Creating service accounts

**Minimum requirements:** `terraform-provider-materialize` v0.8.1+

As a best practice, we strongly recommend using [service accounts](/security/users-service-accounts/create-service-accounts/)
to connect external applications to Materialize. To create a
service account, create a new [`materialize_role`](https://registry.terraform.io/providers/MaterializeInc/materialize/latest/docs/resources/role)
and associate it with a new [`materialize_app_password`](https://registry.terraform.io/providers/MaterializeInc/materialize/latest/docs/resources/app_password)
of type `service`. More granular permissions for the service account can then
be configured using [role-based access control (RBAC)](/security/cloud/access-control/#role-based-access-control-rbac).

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



**Self-managed Materialize:**
### Self-managed Materialize

Configure the provider with connection parameters similar to a standard
PostgreSQL connection. Only database resources are available (clusters, sources,
sinks, etc.). Organization-level resources like `materialize_app_password`,
`materialize_user`, and SSO/SCIM resources are not supported.

To avoid checking secrets into source control, use environment variables for authentication. You have two options:

**Option 1: Using provider environment variables (recommended)**

The provider automatically reads configuration from `MZ_*` environment variables:

```shell
export MZ_PASSWORD=<password>
export MZ_HOST=<host>
```

```hcl
provider "materialize" {
  # Configuration will be read from MZ_HOST, MZ_PORT, MZ_USER,
  # MZ_DATABASE, MZ_PASSWORD, MZ_SSLMODE environment variables
}
```

**Option 2: Using Terraform input variables**

Use [Terraform environment variables](https://developer.hashicorp.com/terraform/cli/config/environment-variables#tf_var_name) with the `TF_VAR_` prefix:

```shell
export TF_VAR_mz_password=<password>
```

```hcl
variable "mz_password" {
  type      = string
  sensitive = true
}

provider "materialize" {
  host     = "materialized"
  port     = 6875
  username = "materialize"
  database = "materialize"
  password = var.mz_password
  sslmode  = "disable"
}
```

#### Provider configuration parameters

| Parameter | Description | Environment Variable | Default |
|-----------|-------------|---------------------|---------|
| `host` | Materialize host address | `MZ_HOST` | - |
| `port` | Materialize port | `MZ_PORT` | `6875` |
| `username` | Database username | `MZ_USER` | `materialize` |
| `database` | Database name | `MZ_DATABASE` | `materialize` |
| `password` | Database password | `MZ_PASSWORD` | - |
| `sslmode` | SSL mode (`disable`, `require`, `verify-ca`, `verify-full`) | `MZ_SSLMODE` | `require` |





---

## Manage cloud resources


The Terraform modules below provide the cloud infrastructure foundation
Materialize needs to communicate with components outside of Materialize itself.
The Materialize provider allows users to manage Materialize resources in the
same programmatic way.

You can use the modules to establish the underlying cloud
resources and then use the Materialize provider to build Materialize-specific
objects. A few use cases are captured in the sections below.

> **Note:** While Materialize offers support for its Terraform provider, Materialize does
> not offer support for these cloud resources modules.


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


---

## Manage Materialize resources with the Materialize provider


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


---

## Manage privileges


This tutorial walks you through managing roles in Materialize with [Terraform](https://www.terraform.io/). By the end of this tutorial you will:

* Create two new roles in your Materialize
* Apply privileges to the new roles
* Assign a role to a user
* Modify and remove privileges on roles

In this scenario, you are a DevOps engineer responsible for managing your Materialize account with code. You recently hired a new developer who needs privileges in a non-production cluster. You will create specific privileges for the new role that align with your business needs and restrict the developer role from having access to your production cluster.

## Before you begin

* Make sure you have a [Materialize account](https://materialize.com/register/?utm_campaign=General&utm_source=documentation) and already have a password to connect with.

* You should be familiar with setting up a [Terraform project in Materialize](/manage/terraform/).

* Have an understanding of permissions in Materialize.

## Step 1. Create Role

1. You can create a functional role with a set of object-specific privileges.
   First, we will create a role resource in Terraform.

    ```hcl
    resource "materialize_role" "dev_role" {
      name = "dev_role"
    }
    ```

2. We will run Terraform to create this role.

    ```shell
    terraform apply
    ```

    > **Note:** All of the resources in this tutorial can be run with a single terraform apply but we will add and apply resources incrementally to better illustrate grants.


3. Each role you create has default role attributes that determine how they can interact with Materialize objects. Let’s look at the role attributes of the role you created:

    ```mzsql
    SELECT * FROM mz_roles WHERE name = 'dev_role';
    ```

    <p></p>

    ```nofmt
    -[ RECORD 1 ]--+------
    id             | u8
    oid            | 50991
    name           | dev_role
    inherit        | t
    create_role    | f
    create_db      | f
    create_cluster | f
    ```
    Your `id` and `oid` values will look different.

## Step 2. Create example objects

Your `dev_role` has the default system-level permissions and needs object-level privileges. RBAC allows you to apply granular privileges to objects in the SQL hierarchy. Let's create some example objects in the system and determine what privileges the role needs.

1. In the Terraform project we will add a cluster, cluster replica, database, schema and table.

    ```hcl
    resource "materialize_cluster" "cluster" {
      name = "dev_cluster"
    }

    resource "materialize_cluster_replica" "cluster_replica" {
      name         = "devr1"
      cluster_name = materialize_cluster.cluster.name
      size         = "25cc"
    }

    resource "materialize_database" "database" {
      name = "dev_db"
    }

    resource "materialize_schema" "schema" {
      name          = "schema"
      database_name = materialize_database.database.name
    }

    resource "materialize_table" "table" {
      name          = "dev_table"
      schema_name   = materialize_schema.schema.name
      database_name = materialize_database.database.name

      column {
        name = "a"
        type = "int"
      }
      column {
        name     = "b"
        type     = "text"
        nullable = true
      }
    }
    ```

2. We will apply our Terraform project again to create the object resources.

    ```shell
    terraform apply
    ```

3. Now that our resources exist, we can query their privileges before they have been associated with our role created in step 1.

    ```mzsql
    SELECT name, privileges FROM mz_tables WHERE name = 'dev_table';
    ```

    <p></p>

    ```nofmt
    name|privileges
    dev_table|{u1=arwd/u1,u8=arw/u1}
    (1 row)
    ```

Currently, the `dev_role` has no permissions on the table `dev_table`.

## Step 3. Grant privileges on example objects

In this example, let's say your `dev_role` needs the following permissions:

* Read, write, and append privileges on the table
* Usage privileges on the schema
* All available privileges on the database
* Usage and create privileges on the cluster

1. We will add the grant resources to our Terraform project.

    ```hcl
    resource "materialize_table_grant" "dev_role_table_grant" {
      for_each = toset(["SELECT", "INSERT", "UPDATE"])

      role_name     = materialize_role.dev_role.name
      privilege     = each.value
      database_name = materialize_table.table.database_name
      schema_name   = materialize_table.table.schema_name
      table_name    = materialize_table.table.name
    }
    ```

    > **Note:** All of the grant resources are a 1:1 between a specific role, object and privilege. So adding three privileges to the `dev_role` will require three Terraform resources which can can be accomplished with the `for_each` meta-argument.


2. We will run Terraform to grant these privileges on the `dev_table` table.

    ```shell
    terraform apply
    ```

3. We can now check the privileges on our table again

    ```mzsql
    SELECT name, privileges FROM mz_tables WHERE name = 'dev_table';
    ```

    <p></p>

    ```nofmt
    name|privileges
    dev_table|{u1=arwd/u1,u8=arw/u1}
    (1 row)
    ```

4. Now we will include the additional grants for the schema, database and cluster.

    ```hcl
    resource "materialize_schema_grant" "dev_role_schema_grant_usage" {
      role_name     = materialize_role.dev_role.name
      privilege     = "USAGE"
      database_name = materialize_schema.schema.database_name
      schema_name   = materialize_schema.schema.name
    }

    resource "materialize_database_grant" "dev_role_database_grant" {
      for_each = toset(["USAGE", "CREATE"])

      role_name     = materialize_role.dev_role.name
      privilege     = each.value
      database_name = materialize_database.database.name
    }

    resource "materialize_cluster_grant" "dev_role_cluster_grant" {
      for_each = toset(["USAGE", "CREATE"])

      role_name    = materialize_role.dev_role.name
      privilege    = each.value
      cluster_name = materialize_cluster.cluster.name
    }
    ```

5. Run Terraform again to grant these additional privileges on the database, schema and cluster.

    ```shell
    terraform apply
    ```

## Step 4. Assign the role to a user

The dev_role now has the acceptable privileges it needs. Let’s apply this role to a user in your Materialize organization.

1. Include a Terraform resource that grants the role we have created in our Terraform project to a Materialize user.

    ```hcl
    resource "materialize_role_grant" "dev_role_grant_user" {
      role_name   = materialize_role.dev_role.name
      member_name = "<user>"
    }
    ```

2. Apply our Terraform change.

    ```shell
    terraform apply
    ```

3. To review the permissions a roles, you can view the object data:

    ```mzsql
    SELECT name, privileges FROM mz_tables WHERE name = 'dev_table';
    ```

    The output should return the object ID, the level of permission, and the assigning role ID.


    ```nofmt
    name|privileges
    dev_table|{u1=arwd/u1,u8=arw/u1}
    (1 row)
    ```
    In this example, role ID `u1` has append, read, write, and delete privileges on the table. Object ID `u8` is the `dev_role` and has append, read, and write privileges, which were assigned by the `u1` user.

## Step 5. Create a second role

Next, you will create a new role with different privileges to other objects. Then you will apply those privileges to the dev role and alter or drop privileges as needed.

1. Create a second role your Materialize account:

    ```hcl
    resource "materialize_role" "qa_role" {
      name = "qa_role"
    }
    ```

2. Apply `CREATEDB` privileges to the `qa_role`:

    ```hcl
    resource "materialize_grant_system_privilege" "qa_role_system_createdb" {
      role_name = materialize_role.qa_role.name
      privilege = "CREATEDB"
    }
    ```

3. Create a new `qa_db` database:

    ```hcl
    resource "materialize_database" "database" {
      name = "dev_db"
    }
    ```

4. Apply `USAGE` and `CREATE` privileges to the `qa_role` role for the new database:

    ```hcl
    resource "materialize_database_grant" "qa_role_database_grant" {
      for_each = toset(["USAGE", "CREATE"])

      role_name     = materialize_role.qa_role.name
      privilege     = each.value
      database_name = materialize_database.database.name
    }
    ```

## Step 6. Add inherited privileges

Your `dev_role` also needs access to `qa_db`. You can apply these privileges individually or you can choose to grant the `dev_role` the same permissions as the `qa_role`.

1. Add `dev_role` as a member of `qa_role`:

    ```hcl
    resource "materialize_role_grant" "qa_role_grant_dev_role" {
      role_name   = materialize_role.qa_role.name
      member_name = materialize_role.dev_role.name
    }
    ```

2. We will run Terraform to grant these the inherited privileges.

    ```shell
    terraform apply
    ```

3. Review the privileges of `qa_role` and `dev_role`:

   ```mzsql
   SELECT name, privileges FROM mz_databases WHERE name='qa_db';
   ```

   Your output will be similar to the example below:

   ```nofmt
   name|privileges
   qa_db|{u1=UC/u1,u9=UC/u1}
   (1 row)
   ```

   Both `dev_role` and `qa_role` have usage and create access to the `qa_db`. In the next section, you will edit role attributes for these roles and drop privileges.


## Step 7. Revoke privileges

You can revoke certain privileges for each role, even if they are inherited from another role.

1. Remove the resource `materialize_database_grant.qa_role_database_grant_create` from the Terraform project.

2. We will run Terraform to revoke privileges.

    ```shell
    terraform apply
    ```

    Because Terraform is responsible for maintaining the state of our project, removing this grant resource and running an `apply` is the equivalent of running a revoke statement:

    ```mzsql
    REVOKE CREATE ON DATABASE dev_table FROM dev_role;
    ```

## Next steps

To destroy the roles and objects you created, you can remove all resources from your Terraform project. Running a `terraform apply` will `DROP` all objects.

## Related pages

For more information on RBAC in Materialize, review the reference documentation:

* [`GRANT ROLE`](/sql/grant-role/)
* [`CREATE ROLE`](/sql/create-role/)
* [`GRANT PRIVILEGE`](/sql/grant-privilege/)
* [`ALTER ROLE`](/sql/alter-role/)
* [`REVOKE PRIVILEGE`](/sql/revoke-privilege/)
* [`DROP ROLE`](/sql/drop-role/)

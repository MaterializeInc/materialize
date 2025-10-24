---
title: "Create new"
description: "Create new clusters, sources, and application passwords in the Materialize console"
menu:
  main:
    parent: console
    weight: 6
    identifier: console-create-new
---

From the Console, you can create new [clusters](/concepts/clusters/ "Isolated
pools of compute resources (CPU, memory, and scratch disk space)"),
[sources](/concepts/sources/ "Upstream (i.e., external) systems you want
Materialize to read data from"), and, for Materialize Cloud, application passwords.

### Create new cluster

![Image of the Create New Cluster flow](/images/console/console-create-new/postgresql/create-new-cluster-flow.png "Create New Cluster flow")

From the Materialize Console:

1. Click **+ Create New** and select **Cluster** to open the **New cluster**
   screen.

1. In the **New cluster** screen,

   1. Specify the following cluster information:

      | Field | Description |
      | ----- | ----------- |
      | **Name** | A name for the cluster. | `
      | **Size** | The [size](/sql/create-cluster/#size) of the cluster. |
      | **Replica** | The [replication factor](/sql/create-cluster/#replication-factor) of the cluster. Default: `1` <br>Clusters that contain sources or sinks cannot have a replication factor greater than 1.|

   1. Click **Create cluster** to create the cluster.

1. Upon successful creation, you'll be redirected to the **Overview** page of
    the newly created cluster.


### Create new source

{{< tip >}}

- For PostgreSQL and MySQL, you must configure your upstream database first.
  Refer to the [Ingest data](/ingest-data/) section for your data source.

- For information about the snapshotting process that occurs when a new source
  is created as well as some best practice guidelines, see [Ingest
  data](/ingest-data/).

{{</ tip >}}

![Image of the Create New Source start for
PostgreSQL](/images/console/console-create-new/postgresql/create-new-source-start.png
"Create New Source start for PostgreSQL")

From the Materialize Console:

1. Click **+ Create New** and select **Source** to open the **New source**
   screen.

1. Choose the source type and follow the instructions to configure a new source.

    {{< tip >}}

    For PostgreSQL and MySQL, you must configure your upstream database first. Refer
    to the [Ingest data](/ingest-data/) section for your data source.

    {{</ tip >}}


### Create new app password (Cloud-only)

![Image of the Create application
password](/images/console/console-create-new/create-app-password.png "Create
application password")


1. Click **+ Create New** and select **App Password** to open the **New app
   password** modal.

1. In the  **New app password** modal, specify the **Type** (either **Personal**
   or **Service**) and the associated details:

   {{< note >}}

- Only **Organization admins** can create a service account.
- **Personal** apps are run under your user account.
- **Service** apps are run under a Service account user. If the specified
   Service account user does not exist, it will be automatically created the
   **first time** the app password is used.

   {{</ note >}}


   {{< tabs >}}
   {{< tab "Personal" >}}

   For a personal app that you will run under your user account, specify the
   type and required field(s):

   | Type | Details |
   | ---- | ----------- |
   | **Type** | Select **Personal** |
   | **Name** | Specify a descriptive name. |

   {{</ tab >}}
   {{< tab "Service account" >}}

   For an app that you will run under a Service account, specify the
   type and required field(s):

   {{< yaml-table data="console/service_account_fields" >}}

   See also [Create service
   accounts](/security/users-service-accounts/create-service-accounts/) for creating
   service accounts via Terraform.

   {{</ tab >}}
   {{</ tabs >}}


1. Click **Create password** to generate the app password.

1. Store the new password securely.

   {{< note >}}

   Do not reload or navigate away from the screen before storing the
   password. This information is not displayed again.

   {{</ note >}}

1. **For a new service account only**.

   For a new service account, after creating the new app password, you must
   connect with the service account to complete the account creation. {{<
   include-md file="shared-content/rbac/service-account-creation.md" >}}

   To connect:

   1. Find your new service account in the **App Passwords** table.

   1. Click on the **Connect** button to get details on connecting with the new
      account.

      {{< tabs >}}
      {{< tab "psql" >}}
If you have `psql` installed:

1. Click on the **Terminal** tab.
1. From a terminal, connect using the psql command displayed.
1. When prompted for the password, enter the app's password.

Once connected, the service account creation is complete and you can grant roles
to the new service account.

      {{</ tab >}}
      {{< tab "Other clients" >}}
To use a non-psql client to connect,

1. Click on the **External tools** tab to get the connection details.

1. Update the client to use these details and connect.

Once connected, the service account creation is complete and you can grant roles
to the new service account.
      {{</ tab >}}
      {{</ tabs >}}

To view the created app accounts, go to [Admin > App
Passwords](/console/admin/).

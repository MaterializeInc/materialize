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
Materialize to read data from"), and application passwords.

### Create new cluster

![Image of the Create New Cluster flow](/images/console/console-create-new/postgresql/create-new-cluster-flow.png "Create New Cluster flow")

From the [Materialize console](https://console.materialize.com/):

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

From the [Materialize console](https://console.materialize.com/):

1. Click **+ Create New** and select **Source** to open the **New source**
   screen.

1. Choose the source type and follow the instructions to configure a new source.

    {{< tip >}}

    For PostgreSQL and MySQL, you must configure your upstream database first. Refer
    to the [Ingest data](/ingest-data/) section for your data source.

    {{</ tip >}}


### Create new app password

![Image of the Create application
password](/images/console/console-create-new/create-app-password.png "Create
application password")

1. Click **+ Create New** and select **App Password** to open the **New app
   password** modal.

1. In the  **New app password** modal, specify the **Type** (either **Personal**
   or **Service**) and the associated details:

   {{< note >}}

   - **Personal** apps are run under your user account.
- **Service** apps are run under a Service account.  If the specified Service
     account does not exist, it will be automatically created the **first time**
     the app password is used.

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


   | Field | Details |
   | ----- | ----------- |
   | **Type** | Select **Personal** |
   | **Name** | Specify a descriptive name. |
   | **User** | Specify the service account user name. If the specifie account   does not exist, it will be automatically created the **first time** the application password is used. |
   | **Roles** | Select the roles to associate with the account. <br><br><ul><li><strong>Organization Admin</strong> has _superuser_ privileges in the database.</li><li><strong>Organization Member</strong> has restricted access to the database, depending on the privileges defined via [role-based access control (RBAC)](/manage/access-control/#role-based-access-control-rbac). |

   See also [Create service
   accounts](/manage/access-control/create-service-accounts/) for creating
   service accounts via Terraform.

   {{</ tab >}}
   {{</ tabs >}}

1. Click **Create password** to create the app password.

To view the created application passwords, go to [Admin > App
Passwords](/console/admin/).

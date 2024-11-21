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
   panel.

1. In the **New cluster** panel,

   1. Specify the following cluster information:

      | Field | Description |
      | ----- | ----------- |
      | **Name** | A name for the cluster. | `
      | **Size** | The [size](/sql/create-cluster/#size) of the cluster. Default: `25cc` |
      | **Replica** | The [replication factor](/sql/create-cluster/#replication-factor) of the cluster. Default: `1` <br>Clusters that contain sources or sinks cannot have a replication factor greater than 1.|

   1. Click **Create cluster** to create the cluster.

1. Upon successful creation, you'll be redirected to the **Overview** page of
    the newly created cluster.


### Create new source

{{< tip >}}

For PostgreSQL and MySQL, you must configure your upstream database first. Refer
to the [Ingest data](/ingest-data/) section for your data source.

{{</ tip >}}

From the [Materialize console](https://console.materialize.com/):


1. Click **+ Create New** and select **Source** to open the **New source**
   panel.

1. Choose the source type and follow the instructions to configure a new source.

![Image of the Create New Source start for PostgreSQL](/images/console/console-create-new/postgresql/create-new-source-start.png "Create New Source start for PostgreSQL")

### Create new app password

1. Click **+ Create New** and select **App Password** to open the **New app
   password** modal.

1. In the  **New app password** modal, specify the **Type** of password you want
to create and the associated details.

   | Type | Details |
   | ---- | ----------- |
   | **Personal** | **Name** |
   | **Service** | **Name**, **User**, and **Roles** |

1. Click **Create password** to create the app password.

![Image of the Create application
password](/images/console/console-create-new/create-app-password.png "Create
application password")

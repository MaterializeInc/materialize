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

From the [Materialize Console](https://console.materialize.com/):

1. Click **+ Create New** and select **Cluster** to open the **New Cluster**
   panel.

1. In the **New Cluster** panel,

   1. Specify the following cluster information:

      | Field | Description |
      | ----- | ----------- |
      | **Name** | A name for the cluster. | `
      | [**Size**](/sql/create-cluster/#size) | The size of the cluster.  A cluster of [size](/concepts/clusters/#cluster-sizing) `200cc` should be enough to process the initial snapshot of the tables in your publication. For very large snapshots, consider using a larger size to speed up processing. Once the snapshot is finished, you can readjust the size of the cluster to fit the volume of changes being replicated from your upstream PostgeSQL database. |
      | [**Replica**](/concepts/clusters/#fault-tolerance) | The replication factor for the cluster. Default: `1` <br>Clusters that contain sources can only have a replication factor of 0 or 1.|

   1. Click **Create** to create the cluster.

1. Upon successful creation, the newly created cluster's **Overview** page
   opens.


### Create new source

{{< tip >}}

Before you can create a source, you must configure your upstream database. Refer
to the [Ingest data](/ingest-data/) section for your data source.

{{</ tip >}}

From the [Materialize Console](https://console.materialize.com/):


1. Click **+ Create New** and select **Source** to open the **Create a Source**
   panel.

1. Choose the source and follow the instructions to configure your source.

![Image of the Create New Source start for PostgreSQL](/images/console/console-create-new/postgresql/create-new-source-start.png "Create New Source start for PostgreSQL")

### Create new app password

1. Click **+ Create New** and select **App Password** to open the **New app
   password** model.

1. In the  **New app password** modal, specify the **Type** and the associated
   information for that type

   | Type | Information |
   | ---- | ----------- |
   | **Personal** | **Name** |
   | **Service** | **Name**, **User**, and **Roles** |

1. Click **Create password** to create the app password.

![Image of the Create application
password](/images/console/console-create-new/create-app-password.png "Create
application password")

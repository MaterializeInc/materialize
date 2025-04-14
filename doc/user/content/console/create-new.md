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
pools of compute resources (CPU, memory, and scratch disk space)") and
[sources](/concepts/sources/).

### Create new cluster

![Image of the Create New Cluster flow](/images/console/console-create-new/postgresql/create-new-cluster-flow.png "Create New Cluster flow")

From the Materialize console:

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

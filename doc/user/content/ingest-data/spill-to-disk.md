---
title: "Spill to Disk"
description: "How to run and manage Upsert and Debezium sources with spill to disk enabled"
menu:
  main:
    parent: "kafka"
    name: "Spill to Disk"
    weight: 4
---

{{< public-preview />}}

{{< warning >}}
**Pricing for this feature is likely to change.**

Replicas with disks currently consume credits at the same rate as
replicas without disks. In the future, replicas with disks will likely
consume credits at a faster rate than replicas without disks.
{{< /warning >}}

This is a guide on how and why to run Upsert and Debezium Kafka sources with spill to disk.

## Overview

Kafka sources that use `ENVELOPE UPSERT` or `ENVELOPE DEBEZIUM` require the entire _working set_ to be stored locally within its scheduled cluster to produce retractions when keys are updated.
The working set is sum total of _current_ values for every key.

By default, Materialize uses an in-memory map to store the working set. However, sources scheduled to run on a cluster with an attached disk store this state on that attached disk.
[Clusters with disk-attached replicas](/sql/create-cluster/#disk) can reduce source memory usage, allowing users to leverage smaller cluster sizes.

## Setting up spill to disk

1. #### Create a Kafka Connection object
    Choose an existing guide to setup your Kafka connection objects: [`DEBEZIUM`](/ingest-data/cdc-postgres-kafka-debezium/), [MSK](/ingest-data/amazon-msk/),
    [Confluent Cloud](/ingest-data/confluent-cloud/), [Upstash](/ingest-data/upstash-kafka/), [Redpanda](/ingest-data/redpanda/),
    and [Redpanda Cloud](/ingest-data/redpanda-cloud/) guides are good starts.

2. #### Create a cluster with disk attached

    To use spill to disk, you need a source cluster with disk attached.

    If you already have a disk-attached cluster, you can skip this step.

    ```sql
    CREATE CLUSTER <cluster_name> MANAGED,
      SIZE = '<size>',
      REPLICATION FACTOR = 1,
      DISK = true;
    ```

    You can find more information on creating clusters [here](/get-started/key-concepts/#clusters).

3. #### Create a source in Materialize

    Create a source on your disk-attached cluster.
    Note that sources created on disk-attached clusters will use spill to disk by default.

    ```sql
    CREATE SOURCE <src_name>
      IN CLUSTER <cluster_name>
      ...;
    ```

    You can find more information on creating Kafka sources [here](/sql/create-source/kafka/).

## Migrating an existing source cluster to spill to disk

Existing source clusters can be migrated to use spill to disk.

```sql
ALTER CLUSTER <cluster_name> SET (DISK = true, SIZE = '<size>');
```

{{< note >}}
If using a legacy unmanaged cluster, it is recommended you migrate to a managed cluster and then enable spill to disk.

```sql
ALTER CLUSTER <cluster_name> SET (MANAGED);

ALTER CLUSTER <cluster_name> SET (DISK = true, SIZE = '<size>');
```

You can find more information about migrating to managed clusters [here](/sql/alter-cluster/#converting-unmanaged-to-managed-clusters).
{{< /note >}}


## Resource Usage During Hydration

Hydration is the process by which Materialize reads the complete history of a Kafka topic when creating new sources.
There is currently a limitation in Materialize, where hydration uses more memory and CPU than is strictly necessary
If you observe clusters failing to ingest new sources, temporarily increase the cluster size until hydration completes.

The simplest way to determine if hydration is complete is to observe the sources associated [progress source](/sql/create-source/kafka/#monitoring-source-progress).
It will display the latest offset from each partition consumed into Materialize.
Hydration is complete once the offsets are close to the max offset of each partition in the topic.

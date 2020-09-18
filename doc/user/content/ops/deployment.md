---
title: "Deployment"
description: "Find details about running your Materialize instances"
menu:
  main:
    parent: operations
---

_This page is a work in progress and will have more detail in the coming months.
If you have specific questions, feel free to [file a GitHub
issue](https://github.com/MaterializeInc/materialize/issues/new?labels=C-feature&template=feature.md)._

## Memory

Materialize stores the majority of its state in memory, and works best when the
streamed data can be reduced in some way. For example, if you know that only a
subset of your rows and columns are relevant for your queries, it helps to avoid
materializing sources or views until you've expressed this to the system.
Materialize can then avoid stashing the full set of rows and columns, which can
in some cases dramatically reduce Materialize's memory footprint.

### Compaction

To prevent memory from growing without bound, Materialize periodically
"compacts" data in [arrangements](/overview/api-components#indexes). For
example, if you have a source that tracks product inventory, you might receive
periodic inventory updates throughout the day:

```
(T-shirts, 9:07am, +500)
(T-shirts, 11:32am, -1)
(T-shirts, 3:14pm, -2)
```

Logical compaction will fold historical updates that fall outside the compaction
window into the state at the start of the window.

```
(T-shirts, 3:14pm, +497)
```

Materialize will only perform this compaction on data that falls outside the
logical compaction window. The default compaction window is 60 seconds behind
the current time, but the window can be adjusted via the
[`--logical-compaction-window`](/cli/#compaction-window) option.

Adjusting the compaction window involves making a tradeoff between historical
detail and resource usage. A larger compaction window retains more historical
detail, but requires more memory. A smaller compaction window uses less memory
but also retains less historical detail. Larger compaction windows also increase
CPU usage, as more detailed histories require more compute time to maintain.

{{< warning >}}
Setting the compaction window too small can prevent Materialize from finding a
suitable timestamp at which to execute a `SELECT` query if that query depends
upon multiple sources.
{{< /warning >}}

Note that compaction is triggered in response to updates arriving. As a result,
if updates stop arriving for a source, Materialize may never compact the source
fully. This can result in higher-than-expected memory usage.

This phenomenon is particularly evident when ingesting a source with a large
amount of historical data (e.g, a several gigabyte Kafka topic that is no longer
changing). With the default compaction window of 60 seconds, it is likely that
the source will be fully ingested within the compaction window. By the time the
data is eligible for compaction, the source is fully ingested, no new updates
are arriving, and therefore no compaction is ever triggered.

If the increased memory usage is problematic, consider one of the following
solutions:

  * Decrease the logical compaction window so that compaction is triggered while
    the source is still ingesting.

  * Compact the source upstream of Materialize.

    If you are using the [upsert envelope with a Kafka
    source](/sql/create-source/avro-kafka/#upsert-envelope-details), consider
    setting compaction policy on the Kafka topic to have Kafka perform the
    compaction.

    If you are using a [file source](/sql/create-source/text-file), consider
    rewriting the file to remove irrelevant historical data.

  * Periodically send dummy updates to trigger compaction.

### Swap

To minimize the chances that Materialize runs out of memory in a production
environment, we recommend you make additional memory available to Materialize
via a SSD-backed swap file or swap partition.

This is particularly important in Linux and in Docker, where swap may not be
automatically set up for you.

#### Docker

By default, a container has no [resource
constraints](https://docs.docker.com/config/containers/resource_constraints/)
and can use as much memory and swap as the host allows, unless you have
overridden this with the `--memory` or the `--memory-swap` flags.

#### Linux

Most cloud Linux distributions do not enable swap by default. However, you can
enable it quite easily with the following steps.

1. Create a swapfile using the `fallocate` command.

   The general syntax is: `fallocate -l <swap size> filename`. For a 1GB swap
   file, for example:

    ```shell
    sudo fallocate -l 1G /swapfile
    ```

1. Make the swap file only accessible to `root`:

    ```shell
    chmod 600 /swapfile
    ```

1. Mark the file as swap space:

   ```shell
   mkswap /swapfile
   ```

1. Enable the swap file:

    ```shell
    swapon /swapfile
    ```

1. Verify the swap is available:

    ```shell
    swapon --show
    ```
    ```ignore
    NAME      TYPE SIZE  USED PRIO
    /swapfile file   1G  0M   -2
    ```

1. *Optional.* To make the swap file permanent, add an entry for it to
  `/etc/fstab`:

     ```shell
     cat '/swapfile none swap sw 0 0' >> /etc/fstab
     ```

## Persistence

{{< experimental v0.4.2 >}}

To avoid re-reading data from Kafka on restart, Materialize lets you create
persistent sources, which persist input messages from Kafka topics to files
on the Materialize instance's local hard drive. Persistence will not solve all
restart time related problems, as there are other factors beyond Kafka broker
read performance that contribute to high restart times.

We recommend enabling persistence if you are using Kafka sources, need to relieve
load on upstream Kafka brokers, and are comfortable using
[experimental][exp] features.

[exp]: /cli/#experimental-mode

{{< warning >}}

Materialize currently does not delete persisted records when the source is dropped.
Additionally, Materialize does not currently compact persisted data. If you enable
persistence on sources from compacted Kafka topics, Materialize will store and re-read
all records that have been persisted, even if some of them were compacted by the upstream
source.

{{< /warning >}}

### Details

Any Kafka source can be declared as persistent source, irrespective of its
format ([Avro][avro], [text/bytes][text], [csv][csv], [Protobuf][proto] and
[JSON][json]).

Materialize stores one copy of all input data for each persistent Kafka source.
Materialize stores these files in:

```
{data-directory}/persistence/{source-id}
```

Within this directory, Materialize writes to files named

```
materialize-{source-id}-{partition-id}-{start-offset}-{end-offset}
```

Here, each file stores data for ranges of offsets per `partition-id`. Each file
stores all the data from `start-offset` (inclusive) to `end-offset` (exclusive).
Materialize buffers input records in memory and flushes them every 10 minutes or
immediately if the number of buffered records per source exceeds the
[`--persistence-max-pending-records`][persistence-flag] parameter. Setting this flag to a
higher value helps Materialize achieve higher ingest and disk write throughput,
however this also increases the average latency before records are persisted.

On restart, Materialize reads back all of the records that had been previously
persisted in offset order, and then continues reading from the upstream source
for data after the last persisted record in each partition.

[avro]: /sql/create-source/avro-kafka/#persisted-kafka-sources
[text]: /sql/create-source/text-kafka/#persisted-kafka-sources
[proto]: /sql/create-source/protobuf-kafka/#persisted-kafka-sources
[csv]: /sql/create-source/csv-kafka/#persisted-kafka-sources
[json]: /sql/create-source/json-kafka/#persisted-kafka-sources
[persistence-flag]: /cli/#persistence

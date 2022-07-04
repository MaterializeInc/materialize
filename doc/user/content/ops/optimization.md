---
title: "Optimization"
description: "Recommendations for memory settings"
menu:
  main:
    parent: ops
    weight: 70
aliases:
  - /ops/deployment/
  - /ops/memory/
---

Materialize stores the majority of its state in memory, and works best when the
streamed data can be reduced in some way. For example, if you know that only a
subset of your rows and columns are relevant for your queries, it helps to avoid
materializing sources or views until you've expressed this to the system.
Materialize can then avoid stashing the full set of rows and columns, which can
in some cases dramatically reduce Materialize's memory footprint.

## Compaction

To prevent memory from growing without bound, Materialize periodically
"compacts" data in [arrangements](/overview/arrangements). For
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
logical compaction window. The default compaction window is 1 millisecond behind
the current time, but the window can be adjusted for each index.

Adjusting the compaction window involves making a tradeoff between historical
detail and resource usage. A larger compaction window retains more historical
detail, but requires more memory. A smaller compaction window uses less memory
but also retains less historical detail. Larger compaction windows also increase
CPU usage, as more detailed histories require more compute time to maintain.

Note that compaction is triggered in response to updates arriving. As a result,
if updates stop arriving for a source, Materialize may never compact the source
fully. This can result in higher-than-expected memory usage.

This phenomenon is particularly evident when ingesting a source with a large
amount of historical data (e.g, a several gigabyte Kafka topic that is no longer
changing). With a compaction window of 60 seconds, for example, it is likely that
the source will be fully ingested within the compaction window. By the time the
data is eligible for compaction, the source is fully ingested, no new updates
are arriving, and therefore no compaction is ever triggered.

If the increased memory usage is problematic, consider one of the following
solutions:

  * Decrease the logical compaction window so that compaction is triggered while
    the source is still ingesting.

  * Compact the source upstream of Materialize.

    If you are using the [upsert envelope with a Kafka
    source](/sql/create-source/kafka/#handling-upserts), consider
    setting compaction policy on the Kafka topic to have Kafka perform the
    compaction.

  * Periodically send dummy updates to trigger compaction.

## Swap

To minimize the chances that Materialize runs out of memory in a production
environment, we recommend you make additional memory available to Materialize
via a SSD-backed swap file or swap partition.

This is particularly important in Linux and in Docker, where swap may not be
automatically set up for you.

### Docker

By default, a container has no [resource
constraints](https://docs.docker.com/config/containers/resource_constraints/)
and can use as much memory and swap as the host allows, unless you have
overridden this with the `--memory` or the `--memory-swap` flags.

### Linux

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

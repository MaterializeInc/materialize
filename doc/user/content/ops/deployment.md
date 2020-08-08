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

Materialize stores the majority of its state in-memory, and works best when the streamed data
can be reduced in some way. For example, if you know that only a subset of your rows and columns
are relevant for your queries, it helps to avoid materializing sources or views until you've
expressed this to the system (we can avoid stashing that data, which can in some cases dramatically
reduce the memory footprint).

To minimize the chances that Materialize runs out of memory in a production environment,
we recommend you make additional memory available to Materialize via a SSD-backed
swap file or swap partition.

This is particularly important in Linux and in Docker, where swap may not be automatically
setup for you:

#### Docker
[By default](https://docs.docker.com/config/containers/resource_constraints/), a
container has no resource constraints and can use as much of a given resource as the host’s
kernel scheduler allows, unless you have overridden this with the `--memory` or the
`--memory-swap` flags.

#### Linux
Most Linux distributions do not enable swap by default. However, you can enable it
quite easily:

1. Create a swapfile:
   The general syntax is: `fallocate [-n] [-o offset] -l length filename`

   (ie, for a 1GB swapfile):
    ```shell
    sudo fallocate -l <swap size> /swapfile
    ```

1. Make the swapfile only accessible to `root`:

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

1. We can now verify the swap is available:

     ```shell
     swapon --show
     ```

1. (Optional): To make the swap file permenant, add this to `/etc/fstab`:

     ```shell
     cat '/swapfile none swap sw 0 0' >> /etc/fstab
     ```

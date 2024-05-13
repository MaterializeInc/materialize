# Platform v2: A Modularized/Decoupled Storage Controller

## Context

As part of the platform v2 work (specifically use-case isolation) we want to
develop a scalable and isolated serving layer that is made up of multiple
processes that interact with distributed primitives at the moments where
coordination is required.

The way the compute controller and storage controller work currently will not
work well for that, because they're coupled together inside one process.

## Goals

Overarching/long-term goal: make it possible to run controller code distributed
across different processes. Say, one compute controller per cluster, that is
local to that cluster.

Finer-grained goals:

- Pull out the part of StorageController that ComputeController needs. Let each
  ComputeController have their own "handle". We will call this part
  StorageCollections.
- Pull out the part of StorageController that deals with table
  writes/transactions. Make it so multiple processes can hold a "handle" to
  this. We will call this part TableWriter.
- Make the rest of StorageController (the part that deals with running
  computation on a cluster) more like ComputeController, where each cluster has
  their own piece of StorageController managing its storage-flavored
  computation.

## Non-Goals

- Actually separate things out into different processes.

## Context/Current Architecture

- We want to have Coordinator-like things running in different processes. We
  want each cluster's (compute) controller to be separate from other
  controllers, running in separate processes.
- Currently, the compute controller is given a "mutable reference" to the
  storage controller, when it needs to do any work.
- The storage controller has come to be responsible for 3 things that can and
  should be separated out:
  - StorageCollections: holds on to since handles and tells a compute
    controller at what time collections are readable, when asked. Allows
    installing read holds.
  - TableWriter: facilitates writes to tables. Could also be called TxnWriter.
  - StorageController: manages storage-flavored computation running on a
    cluster. Think sources and sinks. This is the same as ComputeController,
    but for running storage-flavored computation on a cluster.

## Overview

- We need to take apart the naturally grown responsibilities of current
  StorageController to make it fit into a distributed, post-platform-v2 world.
- StorageCollections and TableWriter will become roughly thin-clients that use
  internal "widgets" to do distributed coordination:
  - For StorageCollections, that widget is persist since handles for holding a
    since, and other handles for learning about upper advancement.
  - For TableWriter, that widget is `persist-txn`. But there is more work in
    that area, specifically we need to replace the current process-level write
    lock.

## StorageCollections

- The compute controller only needs a StorageCollections. It has a much reduced
  interface, compared to current StorageController. Greatly reducing surface
  area/coupling between the two.
- We will revive an old idea where we use persist directly to learn about the
  upper of collections: the StorageCollections does not rely on feedback from
  the cluster to learn about uppers. It uses persist, and drives its since
  handles forward based on that.
- Each compute controller is given their own "instance" of a
  StorageCollections, that they own.
- A StorageCollections will not initially acquire since handles for _all_
  collections but only for those in which the compute controller expresses an
  interest.
- In the past, we were hesitant about this approach because we wouldn't want to
  regularly poll persist for uppers. Now, with persist pubsub, that wont't be a
  problem anymore. Persist pubsub will essentially become the fabric that ships
  upper information around a whole environment, between different processes.

Advantages of this approach:

- Reduced coupling/much reduced surface area between ComputeController and
  StorageCollections.
- Makes use-case isolation/distributed processes possible!

Implications:

- In the limit, it can now happen that we hold `num_collection * num_clusters`
  since handles, where before it was only `num_collection` handles.
- More load on persist pubsub.
- One could argue there would be more latency in learning about a new upper,
  but I don't think that's a valid concern: persist pubsub has so far proven to
  be low latency in practice. And there's motivation to invest into fixing
  issues with persist pubsub because it is used in all places where persist
  shards are being read.

## TableWriter

We can move table-related things out into a TableWriter because the
StorageController doesn't do much with/to tables, except:

- Learn about upper updates through a channel: when adapter writes to tables,
  an update get sent through a channel, the StorageController absorbs those
  similarly to how it absorbs upper updates from running ingestions.
- Acquire since holds when sinking tables to an export.

For both of these use cases, the StorageController can be given access to a
StorageCollections, and acquire read holds same as everyone else (same as
compute and the adapter). Upper updates will no longer have to flow through a
special channel, the StorageCollections will be keeping uppers/sinces up to
date same as for other collections: through persist pubsub.

## StorageController (the per-cluster part)

If we want to achieve full _physical_ use-case isolation, where we have the
serving work (and therefore also the controller work) of an environment split
across multiple processes and not one centralized `environmentd`, we also need
StorageController to work in that world. That is, it needs to become more like
ComputeController where there is a per-cluster controller and not one
monolithic controller inside `environmentd`.

## Rollout

1. We can immediately get started on factoring out TableWriter and
   StorageCollections.
2. Remodeling StorageController will come as a next step, but needs to happen
   for fully-realized use-case isolation.

We only need #1 for use-case isolation Milestone 2, where we want better
isolated components and use-case isolation within the single `environmentd`.
For Milestone 3, full physical use-case isolation, we also need #2.

## Alternatives

### Centralized StorageController and RPC

We can keep a centralized StorageController that runs as a singleton in one
process. Whenever other processes want to, for example, acquire or release read
holds they have to talk to this process via RPC.

Arguments against this alternative:

- We need to worry about processes timing out and then, for example, not
  releasing read holds.
- We would introduce a special-case RPC protocol and a new service while we
  already have persist pub sub as a general purpose fabric that works for our
  purposes.
- Using `SinceHandles` (and other resources) for each cluster makes it clearer
  who is holding on to things.

## Open questions

None so far.

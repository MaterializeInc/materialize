# Platform v2: Decouple Personalities of the Storage Controller

> [!NOTE]
> This is more a proto-design doc right now, laying out the context and basics
> of what I want.

## Context

As part of the platform v2 work (specifically use-case isolation) we want to
develop a scalable and isolated serving layer that is made up of multiple
processes that interact with distributed primitives at the moments where
coordination is required.

The way the compute controller and storage controller work currently will not
work well for that, because they're coupled together inside one process.

## Goals

- Make "storage controller" shareable, give each compute controller their own
  instance. This actually means we want to give each compute controller a
  CollectionController, but you have to read below first.
- Tweeze appart responsibilities of the current storage controller.
- Use persist directly (helped by persist pubsub) to learn about uppers of
  collections and drive their since forward (based on the since policy).

## Non-Goals

- Actually separate things out into separate processes.

## Overview

- We want to to have Coordinator-like things running in different processes. We
  want each cluster's (compute) controller to be separate from other
  controllers, running in separate processes.
- Currently, the compute controller is given a "mutable reference" to the
  storage controller, when it needs to do any work.
- The storage controller has come to be responsible for 3 things that (IMO) can
  and should be separated out:
  - CollectionController: holds on to since handles and tells a compute
    controller at what time collections are readable, when asked. Allows
    installing read holds.
  - TablesController: facilitates writes to tables. Could also be called
    TxnController.
  - StorageController: manages storage-flavored computation running on a
    cluster. Think sources and sinks.

Sketch of what I think we need:

- The compute controller really only needs a CollectionController. It would
  have a much reduced interface, compared to current StorageController. Greatly
  reducing surface area/coupling between the two.
- We can revive an old idea where we use persist directly to learn about the
  upper of collections: the CollectionController would not rely on feedback
  from the cluster to learn about uppers. It would use persist, and drive it's
  since handles forward based on that.
- Each compute controller can be given their own "instance" of a
  CollectionController, that they own.
- A CollectionController would not initially acquire since handles for _all_
  collections but only for those in which the compute controller expresses an
  interest.
- In the past, we were hesitant about this approach because we wouldn't want to
  regularly poll persist for uppers. Now, with persist pubsub, that wouldn't be
  a problem anymore. Persist pubsub would essentially become the fabric that
  ships upper information around a whole environment, between different
  processes.

Advantages of this approach:

- Reduced coupling.
- Interface of each newly carved out controller is reduced.

Implications:

- In the limit, it can now happen that we hold `num_collection * num_clusters`
  since handles, where before it was only `num_collection` handles.
- More load on persist pubsub.
- One could argue there would be more latency in learning about a new upper,
  but I don't even think that's a valid concern.

## Rollout

We can immediately get started on the change to use persist to get the upper
instead of feedback from the cluster. Then, we can separate out the
responsibilities.

## Alternatives

TBD!

## Open questions

TBD!

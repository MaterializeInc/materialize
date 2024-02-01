# Platform v2: Physical, Distributed Architecture

> [!WARNING]
> Currently, mostly a draft! For exploration and discussion. I'm explaining the
> difficulties with a _clusterd-only_ architecture, mostly coming up with
> questions that we'd need to answer. And I show how we can easily evolve our
> current architecture to a _multi-envd-clusterd_ architecture.
>
> This has "I think", and "I" phrases, because this is really me (aljoscha)
> exploring questions.

## Context

As part of the platform v2 work (specifically use-case isolation) we want to
develop a scalable and isolated query/control that is made up of multiple
processes that interact with distributed primitives at the moments where
coordination is required.

In [Platform v2: Logical
Architecture](/doc/developer/design/20231127_decoupled_isolated_coordinator.md)
we lay out the logical architecture. Here, we will lay out the physical
architecture: what kinds of processes make up an environment, how are they
connected, how are queries routed, etc.

## Goals

- Specify what processes and how many we run
- Specify how those processes interact to form a distributed query/control layer
- More, TBD!

## Non-Goals

- TBD!

## Overview

TBD!

## Current Architecture

With our current architecture, one environment runs multiple `clusterd` and a
single `environmentd` (`envd`) process.

![current, single-envd architecture](./static/pv2_physical_architecture/single-envd.png)

The single `envd` process is responsible for/does:

- interacts with durable Catalog
- hosts the adapter layer, including the Coordinator
- hosts Controllers, which instruct clusters and process their responses
- has read holds:
  - for storage objects, these are ultimately backed by persist `SinceHandles`,
    which Coordinator holds via a `StorageController`
  - for compute objects, these are ephemeral, the `ComputeController` ensures
    these holds by choosing when to send `AllowCompaction` commands to clusters
  - a compute controller is the single point that is responsible for
    instructing a cluster about when it can do compaction, this is how it
    logically has "holds" on compute objects
- processes user queries: for this, it needs both types of read holds mentioned above
- downgrades read holds based on outstanding queries and policies, and
  responses about write frontiers from clusters

How are queries routed/processed:

1. simple!
2. all queries arrive at single `envd`, in the adapter layer
3. adapter instructs responsible controller to spin up computation on a cluster
4. responsible controller sends commands to all replicas of the cluster
5. responsible controller eventually receives responses from replicas
6. adapter sends results back to client

### Commentary

Pros:

- Empirically works quite well so far!

Cons:

- Single `envd` is single point through which all query/control processing must
  pass, limiting scalability and isolation.
- We run more than one type of process, which cloud might find harder to
  operate. But it's what we've done for an while now.

## Multi-envd, Multi-clusterd Architecture

With this architecture, 

![multi-envd architecture](./static/pv2_physical_architecture/multi-envd-clusterd.png)

### Commentary

Pros:

- Easy to migrate to from our current architecture.
- Easy-ish to think about how information flows in the system, who "holds" read
  holds, how queries are routed through adapter and to replicas.

Cons:

- We run more than one type of process, which cloud might find harder to
  operate. But it's what we've done for an while now.
- We run more than one `envd`-shaped processes (one per cluster), which we
  previously didn't.

## Multi-clusterd, No-envd Architecture

?

### Commentary

Pros:

- We only run one type of process.
- Plus maybe one super-slim `envd` for bootstrapping things?

Cons:

- Hard to see how we can evolve our design to there from our current
  architecture. At least for me, aljoscha.

## Alternatives

TBD!

## Open questions


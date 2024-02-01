# Platform v2: Physical, Distributed Architecture

> [!WARNING]
> Currently, mostly a draft! For getting things out early. I'm explaining the
> difficulties with a clusterd-only architecture, mostly coming up with
> questions that we'd need to answer. And I show how we can easily migrate our
> current architecture to a multi-envd-clusterd architecture.

## Context

As part of the platform v2 work (specifically use-case isolation) we want to
develop a scalable and isolated query/control that is made up of multiple
processes that interact with distributed primitives at the moments where
coordination is required.

In [link TODO]() we lay out the logical architecture. Here, we will lay out the
physical architecture: what kinds of processes make up an environment, how are
they connected, how are queries routed, etc.

## Goals

- Specify what processes and how many we run
- Specify how those processes interact to form a distributed query/control layer
- More, TBD!

## Non-Goals

- TBD!

## Overview

## Current Architecture

![current, single-envd architecture](./static/pv2_physical_architecture/single-envd.png)

## Multi-envd, Multi-clusterd Architecture

![multi-envd architecture](./static/pv2_physical_architecture/multi-envd-clusterd.png)

Previous design documents describe implementations of the newly required components:

 - [Differential CATALOG state](./20230806_durable_catalog_state.md)
 - [TIMESTAMP ORACLE as a service](./20230921_distributed_ts_oracle.md)
 - [persist-txn](./20230705_v2_txn_management.md)

## Multi-clusterd, No-envd Architecture

?

## Alternatives

TBD!

## Open questions


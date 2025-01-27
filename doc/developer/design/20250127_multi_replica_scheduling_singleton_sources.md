# Multi-Replica Scheduling for Singleton Sources (aka hot-standby sources)

## Context

We want to support zero-downtime ALTER on clusters. The plan for this is to
turn on a new replica with the changed parameters and turn off the old replica
when the new one is "sufficiently ready". This in turn requires that we are
able to run sources and sinks on multi-replica clusters. This document concerns
sources, we are punting on sinks for now though we might use a very similar
solution once we have addressed [storage: restarting a Sink should not require
reading a snapshot of the input #8603
](https://github.com/MaterializeInc/database-issues/issues/8603).

For (UPSERT) Kafka sources we already have a solution in [Feedback
UPSERT](https://github.com/MaterializeInc/materialize/pull/29718), which allows
us to run Kafka sources concurrently. Singleton sources (Postgres and MySQL as
of writing) cannot, as of now, run concurrently: they have state (a slot or
similar) at the upstream system that cannot be shared by concurrent ingestions,
running on different replicas. The good thing about these kinds of sources is
that their running ingestion dataflow is virtually stateless, so restarting
them is fast. We will use this latter fact in our approach to how we want to
support running them on multi-replica clusters.

The approach we want to take can be summarized as: change the storage
controller to schedule stateless sources on only a single replica _and_ update
that scheduling decision when the replica that is running a source is going
away. See below for why we think this is a feasible and sufficient approach for
a first implementation of this feature. As an aside, for (UPSERT) Kafka sources
we can allow scheduling them on all replicas of a cluster.

## Goals

- Support running singleton sources on multi-replica clusters
- Support zero-downtime ALTER on clusters

## Non-Goals

- Add a failure detection mechanism for replicas and update source scheduling
  based on that
- Changes to the storage protocol
- Changes to how storage handles things on the `clusterd` side, for example to
  make it handle concurrent singleton sources purely on the `clusterd`s without
  the controller being involved

## Rationale

Initially, the main goal of supporting singleton sources on multi-replica
clusters is to support zero-downtime ALTER. For this, the controller knows which
replicas are being created and which are being retired, it is therefore enough
to base scheduling decisions on that. If the goal were enable HA of sources
running on multi-replica clusters, we would want to pursue a different approach
were we really do figure out a way for these types of singleton sources to run
concurrently, such that they can either a) very quickly take over when another
instance fails, or b) truly run concurrently and write the output shard(s)
collaboratively.

## Grabbag of Questions

Q: What happens when we accidentally try and schedule a concurrent ingestion
dataflow for a singleton source? Say when the replica processes take longer than
expected to shut down.

A: This is caught by the mechanisms we already have today for making sure there
is only one active ingestion dataflow. We need this for correctness in the fact
of upgrades, or failing/zombie `clusterd` pods. Today, source dataflows use the
state in the upstream system (the slot) to fence out other readers.

## Implementation

We're keeping this very light for now, but think that the initial implementation
can be very bare bones. Most (all?) changes need to happen in
[storage-controller/src/instance.rs](https://github.com/MaterializeInc/materialize/blob/2280405a2e1f8a44fa1c8f046a718c013ff7af6b/src/storage-controller/src/instance.rs#L73),
where we have the knowledge about what replicas exist and when they are being
created or destroyed.

## Alternatives

### Handle Collaboration in the source dataflow implementation

We could change the source ingestion dataflows to handle concurrency internally.
They could do something akin to leader election and/or us the state that they
have at the upstream system (the slot or similar) to figure out who should be
reading. They would need some sort of failure detection to figure out when/if to
take over from the "leader" ingestion, which comes with its own collection of
downsides.

## Open Questions

tbd!

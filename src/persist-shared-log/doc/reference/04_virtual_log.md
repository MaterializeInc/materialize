# Persist Shared Log: Virtual Log & Write Scaling

## Problem

A single log shard is served by a single acceptor. The acceptor's flush
throughput (how many `compare_and_append` calls per second it can sustain)
bounds the aggregate write throughput of the system. For workloads that
exceed this bound, we need a way to scale writes across multiple acceptors.

## The Virtual Log

The virtual log is a concept from Delos (Balakrishnan et al., SOSP 2021).
Instead of a single physical log, the system maintains multiple _log
shards_, each with its own acceptor. Client shards are partitioned across
log shards, so proposals for different client shards can be appended
concurrently to different log shards.

From the perspective of a client shard, there is still a single logical log
(the virtual log) that totally orders its proposals. The partitioning is
transparent to clients.

## Metashard

A distinguished persist shard, the _metashard_, records the mapping from
client shards to log shards over time. Specifically, the metashard records
which log shard owns each client shard at each timestamp range.

Learners read the metashard to discover which log shards they need to
subscribe to. When the mapping changes (e.g. a log shard is sealed and its
client shards are reassigned), learners observe the change via the metashard
and adjust their subscriptions.

The metashard is itself a persist shard, so it benefits from the same
durability, read scale-out, and rehydration properties as every other
persist shard in the system.

## Sealing

Retiring a log shard is called _sealing_. To seal a log shard, its write
frontier is advanced to the empty antichain, which is the standard persist
mechanism for indicating that a shard will receive no more writes.

Once sealed, the log shard is immutable. Its data remains readable (learners
can still subscribe to it and rehydrate from it), but no new proposals are
appended.

Client shards previously owned by the sealed log shard are reassigned to a
new log shard via a metashard update. The new log shard's acceptor begins
accepting proposals for those client shards from that point forward.

## Scaling Model

Each log shard is independent: it has its own acceptor, its own persist
shard, and its own batch numbering. Adding log shards adds write throughput
linearly.

Learners subscribe to all log shards that contain client shards they are
responsible for. A learner processing N log shards evaluates proposals from
each log shard independently. Client shard independence
([02_invariants.md](02_invariants.md), property C4) guarantees that
proposals from different log shards for different client shards do not
interact.

## Open Questions

The following aspects of the virtual log are active design questions.

### Multi-acceptor coordination

How do acceptors discover their log shard assignments? Options include
static configuration, reading the metashard, or a control plane service.
The coordination mechanism determines how quickly the system can rebalance
client shards across log shards.

### Scheduling

Acceptor and learner workers are distributable units; the same abstraction
works whether running as threads on one machine or as processes across
machines. The scheduling question is how workers are placed:

- _Thread-per-core on one machine_: Each acceptor and learner is a
  dedicated thread with its own event loop (like timely dataflow workers).
  Low latency, simple deployment, bounded by one machine's resources.
- _Process-per-machine_: Each acceptor and learner is a separate process.
  Scales across machines, requires network transport between components.
- _Hybrid_: Some workers co-located, others distributed. Matches deployment
  constraints (e.g. acceptors near storage, learners near clients).

The architecture supports all of these. The trait abstractions are
transport-agnostic, so the scheduling decision is an operational one.

### Metashard update protocol

Who writes the metashard, when, and with what consistency requirements?

- A control plane component (e.g. environmentd) could manage shard
  assignments.
- The metashard update must be atomic with the sealing of the old log shard
  to avoid a window where proposals are lost or duplicated.
- Learners must observe the metashard update before the new log shard
  receives proposals for the reassigned client shards, to avoid missing
  data.

## Related Reading

- Balakrishnan et al., "Log-structured Protocols in Delos" (SOSP 2021).
  The virtual log concept, log sealing, and reconfiguration protocol.
- [00_overview.md](00_overview.md). Architecture overview and how the
  virtual log fits into the broader system.

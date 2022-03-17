# Supporting linearizable reads

## Summary

Materialize has "real time" sources whose updates are timestamped with a `materialized` local timestamp, used for queries and interaction with other data.
These timestamps are produced as _timestamp bindings_, which are durably-stored associations between `timestamps` (the literal, real-time timestamps that mz uses to drive its dataflows) and `offsets` (consistent, ordered, source-specific integer identifiers that express how _far along_ you are in the source).

These bindings are produced and durably retained for sources, though the behavior depends on whether the source is currently

- "materialized", in that it is actively being consumed in a live dataflow. This process will continually pull data and create bindings.

- "unmaterialized", in that the source is known but not actively consumed. No current process will create new bindings.

The timestamp bindings are the basis of *consistency* in Materialize. By reading from sources, and views over the sources, at a single timestamp, we can ensure that all readers observe consistent results, for potenially varying definitions of "consistent".

How we choose this timestamp influences the type of consistency guarantee we get. In particular, if we want to achieve _linearizable_ reads, those that reflect all data available in upstream sources as of at least the moment `SELECT` was typed, we will need to select a timestamp that whose bindings reflect all available source data. Consequently, STORAGE will need a command to provide such a timestamp for a collection of sources, when prompted.

## Goals

Offer _linearizable_ reads in as many cases as possible. `materialized` is ultimately built to support this!

Avoid performance interference of linearizable reads on other "less consistent" reads, such as sequential or eventual consistency.

## Non-Goals

Alter the default experience of using `materialized` (yet).

Present strong opinions on which consistency levels should be the default.

## Description

### Background

Sources currently consume messages that contain offsets, and produce updates that contain timestamps. Those offsets are written down, associated with a timestamp, as a _timestamp binding_.
When you do a peek (read) against some source. A recent timestamp is chosen, the index is consulted for data that is valid in respect to that timestamp
and the _timestamp bindings_.

### Linearizable reads

In the case of a linearizable read, peeks will be augemented to work as follows

1. Before issuing the peek, we will first ask STORAGE "for all sources on which this query transitively depends, what timestamp reflects all of their current contents?"
2. We will wait until STORAGE responds with such a timestamp, `t`.
3. We then issue the peek at timestamp `t`.

The behavior of other reads does not need to be changed. They still consult `since` for their inputs, and may elect to choose any timestamp that is greater than this, but perhaps one that is not as "current" as for linearizable reads, to be able to return immediately.

This behavior applies equally well to matarialized and unmaterialized sources.

## Alternatives

- Change the default experience of using `materialized` to be `LINEARIZABLE`

## Open questions

- How much of a latency hit will fetching the max offset incur?
- How feasible is it for ALL sources to offer a "max offset" API?
  - Can we degrade the available options for sources that offer no "max offset"?
  - Will we require a heartbeat mechanism for those sources?
- Will linearized reads negatively impact sequentially consistent reads (are they forced to advance similarly)?
- What syntax is best to express linearizable (or non) reads? For example, as part of `BEGIN`?

# Linearizable and eventually consistent reads

## Summary

Real-time sources come in two different flavors:
- `materialized`, which means there is always an _index_ that drives reads for that source, and, critically, drives
_timestamp bindings_, which are durably-stored associations between `timestamps` (the literal, real-time timestamps that mz uses to drive its dataflows)
and `offsets` (consistent, ordered, source-specific integer identifiers that express how _far along_ you are in the source)
- "unmaterialized", which have no such _index_. These sources could be queried, but produce inconsistent results that are not even _eventually consistent_.

_timestamp bindings_ can allow for _linearizable_ reads, that is, reads that are consistent in respect to the offsets we read in real time. For example, a
_linearizable_ read against a source will **always** include data for a recent offset, _and will associate a specific timestamp with that offset_.
Later reads at a timestamp at or later than timestamp will _at least include_ data up to that offset.

We want to fix the lack of any kind of consistency for "unmaterialized sources" AND offer _linearizable_ reads for all types of sources. In short, we want the following
table to represent all possible interactions with sources in `materialized`:


|              | unmaterialized | materialized   |
| ------------ | -------------- | -------------- |
| Enventual    | Not available  | Default        |
| Linearizable | Default        | `LINEARIZABLE` |

Not offering non-_linearizable_ reads for reads interacting with unmaterialized sources simplifies the implementation, and clarifies the user experience of
working with unmaterialized sources

## Goals

1. Offer _linearizable_ reads in as many cases as possible. `materialized` is ultimately built to support this!
2. Offer eventual consistency by default (keeping the same behavior we currently have in `materialized`) for materialized sources

## Non-Goals

Alter the default experience of using `materialized` (yet).

## Description

### Background

Sources currently produce messages that contain offsets. Those offsets are written down, associated with a timestamp, as a _timestamp binding_.
When you do a peek (read) against some source. A recent timestamp is chosen, the index is consulted for data that is valid in respect to that timestamp
and the _timestamp bindings_.

### Linearizable reads
In the case of `LINEARIZABLE` read (see below for how to express this in sql), peeks will be augemented to work as follows

1. Before issuing the peek, we will first ask each source involved in the peek: "what is your max offset"
2. We will wait until a max offset is available at some timestamp `t` (i.e., written down as a timestamp binding)
3. And then issue the peek for timestamp `t`.

In the case of "unmaterialized" sources, the temporary index will be created at the beginning of this process and not removed until the end.

### Eventually consistent reads
This is the default behavior of peeks, and will be augmented as follows:

1. Before issuing the peek, we will look for the latest available timestamp for all involved sources
2. And then issue the peek at that timestamp

This is the standard `since` and `uppser` logic that already exists in `materialized`. Additionally, "unmaterialized"
sources won't have access to do this. A query involving this

In the case of "unmaterialized" sources, there are 2 important notes:
- the temporary index will be created at the beginning of this process and not removed until the end.
- When an unmaterialized source is created, a background special _index_ will be created, which will progress
the source and produce only offsets that are bound to timestamps. This allows EC reads against unmaterialized sources
to consistently obtain new data.

### Platform considerations

In the world of Platform, when performing a _linearizable_ read, we need to be specific about what we mean by
steps 1&2. The way these will operate is:

1. `STORAGE` will offer an api that simply takes in the list of involved sources, and blocks until it returns a timestamp `t`
2. `STORAGE`, upon receiving this request, will ask the sources for their max offets, and then wait for a timestamp binding
that contains all those offsets.
3. And then issue the peek for timestamp `t`.

### Extensions

This document currently describes a situation where querying unmaterialized sources requires waiting for _linearizability_.
A natural extension of this would be to add a _heartbeating_ mechanism that continuously records offsets, but not data. This
would be a special version of a long-living index.

In the Platform world, this point may be unimportant, as sources may always be making incremental progress, regardless of their
materialization status. (TODO(guswynn): confirm this with petros)


## Proposed syntax changes

`SELECT` statements will be altered to add an optional `LINEARIZABLE` after the `SELECT keyword.

```
SELECT (LINEARIZABLE) ...
```

```sql
SELECT LINEARIZABLE count(*) from unmaterialized_source
```

This is only allowed on the top-level `SELECT` in a statement, and it forces _linearizable_ semantics on the entire query.

## Alternatives

- Change the default experience of using `materialized` to be `LINEARIZABLE`
- Change the default experinece of "unmaterialized" sources to be "eventual consistency", using a heartbeat mechanism
- ???


## Open questions

- How much of a latency hit will fetching the max offset incur?
- How feasible is it for ALL sources to offer a "max offset" API?
  - Can we degrade the available options for sources that offer no "max offset"?
  - Will we require a heartbeat mechanism for those sources?
- ???

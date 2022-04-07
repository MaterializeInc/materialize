# Supporting linearizable reads

**Note**
In this document, the term `CONSISTENT` is used as placeholder syntax for a user asking for
a read with "stronger consistency guarantees" than the default, which may or may not
be true linearizability. See [Formalism](#formalism-and-constraints)x) for more details.

Additionally, a previous version of this document included a design that has since been [broken out](https://github.com/MaterializeInc/materialize/pull/11302).

## Summary

Materialize has "real time" sources whose updates are timestamped with a `materialized` local timestamp, used for queries and interaction with other data.
These timestamps are produced as _timestamp bindings_, which are durably-stored associations between `timestamps` (the literal, real-time timestamps that mz uses to drive its dataflows) and `offsets` (consistent, ordered, source-specific integer identifiers that express how _far along_ you are in the source).

These bindings are produced and durably retained for sources, though the behavior depends on whether the source is currently

- "materialized", in that it is actively being consumed in a live dataflow. This process will continually pull data and create bindings.

- "unmaterialized", in that the source is known but not actively consumed. No current process will create new bindings.

The timestamp bindings are the basis of **consistency** in Materialize. By reading from sources, and views over the sources, at a single timestamp, we can ensure that all readers observe consistent results, for potenially varying definitions of "consistent".

How we choose this timestamp influences the type of consistency guarantee we get. In particular, if we want to achieve _linearizable_ reads, those that reflect all data available in upstream sources as of at least the moment `SELECT` was typed, we will need to select a timestamp that whose bindings reflect all available source data. Consequently, STORAGE will need a command to provide such a timestamp for a collection of sources, when prompted.

## Goals

Offer _linearizable_ reads in as many cases as possible. `materialized` is ultimately built to support this! Offer a weaker
form of consistency in other cases.

Avoid performance interference of _linearizable_ reads on other "less consistent" reads.

## Non-Goals

Alter the default experience of using `materialized` (yet).

Present strong opinions on which consistency levels should be the default.

## Description

### Background

Sources currently consume messages that contain offsets, and produce updates that contain timestamps. Those offsets are written down, associated with a timestamp, as a _timestamp binding_.
When you do a peek (read) against some source. A recent timestamp is chosen, the index is consulted for data that is valid in respect to that timestamp
and the _timestamp bindings_.

### Linearizable reads

In the case of a `CONSISTENT` read, peeks will be augmented to work as follows

1. Before issuing the peek, we will first ask STORAGE "for all sources on which this query transitively depends, what timestamp reflects all of their current contents?"
2. We will wait until STORAGE responds with such a timestamp, `t`.
3. We then issue the peek at _at least_ timestamp `t`, while also taking into consideration the `since` and `uppers` of the involved indexes.

The behavior of non-`CONSISTENT` reads does not need to be changed.
They still consult `since` for their inputs, and may elect to choose any timestamp that is greater than this, but perhaps one that is not as "current" as for linearizable reads, to be able to return immediately.

This behavior applies equally well to matarialized and unmaterialized sources.

### Other open questions

- How much of a latency hit will fetching the max offset incur?
- How feasible is it for ALL sources to offer a "max offset" API?
  - Can we degrade the available options for sources that offer no "max offset"?
  - Will we require a heartbeat mechanism for those sources?
- Will linearized reads negatively impact sequentially consistent reads (are they forced to advance similarly)?
- What syntax is best to express linearizable (or non) reads? For example, as part of `BEGIN`?
- Should be eventually change the default experience of using `materialized` to be `CONSISTENT`?

## Formalism and constraints

**Note**
Please refer to [formalism](https://github.com/MaterializeInc/materialize/blob/fe0f4bac40db1afd21606428be14197601bb5a1c/doc/developer/platform/formalism.md)
for more background about some of the terms used here, as well as
this [this jepsen map on consistency](https://jepsen.io/consistency) for details
about specific consistency models.

Materialized offers reads with stronger _real-time_ consistency guarantees than the default.

Reads again some view `V` always happen at a singular `timestamp`, which must be bound
by the `upper` and `since` of that view. That view `V` is composed of many such _source_
pTVC's, which are pTVC's that correspond to the data produced some some _external system_.

True _linearizability_ requires that reads and writes happen in some _total order_ that is
consistent with the _real time ordering_ of those operations. That is, each operation *appears*
to happen atomically at some singular point in time bounded by the beginning and end of that
operation.

A `LINEARIZABLE` read against some view `V` is possible if-and-only-if the following are true:

* All _source_ pTVC's that are at the base of `V` offer a _linearizable_ API for obtaining the
  latest offset of data for that pTVC.
  * "offset" here refers to some monotonically increasing identifier that durably and consistently
  labels data.
  * A given API is _linearizable_ if-and-only-if later (as in, later in _real-time_) calls tho
  that API can never produce lower "offets".
* For all _source_ pTVC's involved, materialized durably associate _real-time_ timestamps
with specific offsets (these are typically referred to as _timestamp bindings_ and are currently
implemented for Kafka sources).
* ...and the responses from those API's can be given some _real-time total order_.
  * The simplest way to obtain this constraint is to involve only one source.

With the above constraints, the mechanism that materialized uses to service the read
against some view `V` is as follows:

1. Fetch the latest offsets for all _source_ pTVC's involved.
2. Wait for timestamps to be durably recorded associated with all offsets, as `source_timestamps`
3. Choose a timestamp that is _at least_
`max(max(source_uppers), max(source_timestamps))`. Note that this may require one of both of
holding back the `since` of `V` or waiting for the `upper` to progress past the chosen timestamp.
4. Service the read for the view at that timestamp. This will durably advance the `upper`s of all involved
_source_ pTVC's to at least that timestamp.

This provides _linearizability_ because:
* materialized guarantees (possible with a timestamp oracle service) that all calls to
`current_real_time()` monotonically increase.
* Durably recoding offsets associated with timestamps means that reads (even non-linearizable ones)
at later times can violate the _real-time_ ordering, as later reads will always have
a _real-time_ timestamp that is at least the chosen timestamp, and materialized, for a read against
a pTVC at some timestamp `t` always includes all data with offsets whose associated timestamps are <= `t`.

The reason the last constraint above is important is because this process can create
an inviolably _real-time_ ordering between writes the upstream source and reads from
materialized views, without that constraint materialized has no information if offsets
from disparate _sources_ have a causal relationship, and could thereforce produce
results

### Caveat: views involving tables
All reads against views that involve tables will force the query to happen at a global
`linearized` timestamp that is shared across tables. This may be greater than the
timestamp chosen in step #3. This is fine, as servicing this query in step #4
will advance the `upper`s of the involved sources such that future queries will
still be ordered at of after this query.

### Caveat: queries against multiple views

The above reasons only about the ordering of writes and reads to a specific view.
If you perform `CONSISTENT` reads against multiple views with overlapping base _source_
pTVC's, then you will not see a _linearizable_ history between those sources. You will
see other, weaker guarantees, described below in [In practice](#in-practice).

### In practice

In practice this means that the following are true:

* Reads against some view that involves a single _linearizable_ external source instance, and any
number of tables can be made _linearizable_.
  * _linearizable_ external sources include at least kafka, and exclude at least pubnub and kinesis
  * In fact, these reads can be considered _strict serializable_
  * Reads against other views that involve those sources, do not participate in these ordering guarantees, but
  may have some guarantees about the ordering of reads against themselves.
  * Non-`LINEARIZABLE` reads do not participate in these ordering guarantees.
* Some sources (like postgres) may extend the above to allow true _linearizability_ for view involving more than
one external source, depending on some contraints (for postgres this may be: the sources being from the same postgres instance)
* `LINEARIZABLE` reads 1. against a view involving many _linearizable_ external source instances and/or 2. across
multiple such views are not _linearizable_ (or _serializabile_), but are:
  * _at least_ **read-your-writes**: that is, any write to an upstream source will be inviolably reflected in reads against materialized
  * **recent**: that is, will reflect the state of all those upstream sources
  * Present writes as if they happened in some partial-order that never changes; that is, results of reads are not eventually consistent, they are
  fixed at each timestamp along the timeline.
* Reads against views that involve non-_linearizable_ external source instances can not be made `CONSISTENT`

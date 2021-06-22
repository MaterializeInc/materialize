# Observe source progress

## Summary

Currently, Materialize does not provide any feedback about the source
progression. This makes it difficult to determine if a source is up-to-date and
queries return fresh data. The goal of this effort is to add a mechanism to
allow clients to query the progress of a source in a provider-independent way.

## Goals

The goals of this design are to enable users to observe the progress of their
sources. It should help answer the following questions:

* Is all (past) data loaded?

## Non-Goals

A non-goal is to load data in a more efficient manner.

While this design proposal is closely related to observing the progress of
sources, it is not intended to answer questions on the steady-state.
Specifically, the following questions are out of scope:

* What was the last time the source was up-to-date?
* How much does a source lag behind?

## Description

Sources have a different notion of what their progress could be. Some sources
read from an external queue and thus are potentially never complete as there is
more data outstanding. Others read from a fixed set of resources and will
eventually consume all inputs. The goal of this effort is to provide a single
abstraction that covers all use cases while providing meaningful information to
the user.

### Terminology

The **Progress** describes what amount of work has been completed out of the
currently known amount of work. For example, when reading a stream, a number of
items have been consumed and more are available but not yet read. The input ca
be partitioned and each partition can have its own progress information.

The **completed work** describes what amount items has been ingested from a
source. Depending on the source, it can describe rows or larger-granularity
collections such as objects.

The **pending work** describes how much work the source knows to exist and
outstanding. Again, the granularity can differ with each type of source.

Inputs either reveal a **bounded** or **unbounded input** collection. Streams in
general never end while reading from bounded resources can only produce a fixed
amount of information.

### Sources

The following table attempts to summarize characteristics for different source
types. It distinguishes sources with bounded and unbounded inputs in rows. The
columns distinguish what kind of pending work estimation is available. Each
field shows what information we can present to the user.

| Pending work estimation: | Approximate | Unavailable | Precise |
| ------------------------ | ----------- | ----------- | ------- |
| Unbounded input          | temporally approximate | None  | N/A |
| Bounded input            | temporally approximate | None | Accurate |

Sources reading from external queues might only know how much data they
consumed, but not necessarily the amount of pending data. Without any additional
information, for such sources the amount of completed work is always the amount
of pending work, or a constant less.

For sources that read unbounded streams of data but have additional metadata
revealing the amount of pending data, we can report an approximation of pending
work.

The same holds for bounded inputs, with the exception that some inputs can
provide an exact number of available work.

The following table shows for each source type whether it produces bounded
amount of work and what kind of pending work approximation it offers.

| Source | Bounded  | Pending work |
| ------ | -------- | ------------ |
| Kafka  | Infinite | High watermark |
| S3 Scan| Bounded  | 1k increments (paginated) |
| S3 Notifications | Infinite | Unknown |
| File   | Bounded  | File size |
| Socket (File) | Unbounded | Unknown |
| Postgres (initial snapshot) | Bounded | Known* |
| Postgres (updates) | Unbounded | Unknown |
| Kinesis| TODO | TODO |
| PubNub | TODO | TODO |

*Initial snapshots for postgres currently capture the complete source relation
in a single `SELECT` statement.

## Alternatives

<!--
// Similar to the Description section. List of alternative approaches considered, pros/cons or why they were not chosen
-->

## Open questions

<!--
// Anything currently unanswered that needs specific focus. This section may be expanded during the doc meeting as
// other unknowns are pointed out.
// These questions may be technical, product, or anything in-between.
-->
